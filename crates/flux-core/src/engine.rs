//! The engine: owns the tokio runtime, task supervision, scheduler, metrics
//! ticker, snapshot broadcasting and the command API used by CLI/GUI.

use crate::config::{EngineConfig, RateWindow};
use crate::download;
use crate::errors::{FluxError, Result};
use crate::limit::TokenBucket;
use crate::security;
use crate::storage::{PartialFile, WriteBudget};
use crate::store::Store;
use crate::task::{
    AddRequest, ConfigSummary, EngineSnapshot, GlobalStats, TaskId, TaskKind, TaskSnapshot,
    TaskStatus,
};
use chrono::Timelike;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Ctrl {
    Run,
    Pause,
    Stop,
}

/// Live mutable state of one task.
pub(crate) struct TaskMutable {
    pub status: TaskStatus,
    pub filename: String,
    pub output_dir: PathBuf,
    pub size: Option<u64>,
    pub protocol: Option<String>,
    pub resume_supported: bool,
    pub error: Option<String>,
    pub expected_sha256: Option<String>,
    pub actual_sha256: Option<String>,
    pub checksum_ok: Option<bool>,
    pub speed_bps: u64,
    pub avg_bps: u64,
    pub eta_sec: Option<u64>,
    pub history: VecDeque<(i64, u64)>,
    /// (unix ms, displayed progress bytes) of the previous speed sample.
    pub last_sample: (i64, u64),
    pub started_at: Option<i64>,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

/// Shared atomics + handles for a task (workers + supervisor + ticker).
pub(crate) struct TaskShared {
    pub id: TaskId,
    /// Bytes received from the network this session.
    pub net_bytes: AtomicU64,
    /// Bytes currently held in RAM write buffers.
    pub buffered: Arc<AtomicU64>,
    /// Durable-resumed base for displayed progress.
    pub progress_base: AtomicU64,
    pub retries: AtomicU32,
    pub ctrl: tokio::sync::watch::Sender<Ctrl>,
    pub storage: Mutex<Option<Arc<PartialFile>>>,
    pub segs: Mutex<Vec<Arc<download::SegStat>>>,
}

pub(crate) struct TaskEntry {
    pub id: TaskId,
    pub req: AddRequest,
    pub kind: TaskKind,
    pub mutable: Arc<Mutex<TaskMutable>>,
    pub shared: Arc<TaskShared>,
    pub join: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    #[cfg(feature = "torrent")]
    pub torrent: Arc<Mutex<Option<crate::torrent::TorrentHandleBox>>>,
}

/// Internal engine context shared with supervisors.
pub(crate) struct EngineCtx {
    pub http: reqwest::Client,
    pub h3: reqwest::Client,
    pub store: Arc<Store>,
    pub cfg: Arc<Mutex<EngineConfig>>,
    pub budget: Arc<WriteBudget>,
    pub global_limiter: Arc<TokenBucket>,
    pub events: mpsc::Sender<EngineEvent>,
    pub active_conns: AtomicU32,
    pub started_at: i64,
    pub sys: Arc<Mutex<sysinfo::System>>,
    #[cfg(feature = "torrent")]
    pub torrent_session: Arc<tokio::sync::Mutex<Option<Arc<librqbit::Session>>>>,
}

impl EngineCtx {
    pub(crate) fn persist_task(&self, entry: &Arc<TaskEntry>) {
        let m = entry.mutable.lock().expect("task");
        let st = crate::task::StoredTask {
            id: entry.id,
            url: entry.req.url.clone(),
            filename: m.filename.clone(),
            output_dir: m.output_dir.clone(),
            kind: entry.kind,
            status: m.status,
            size: m.size,
            connections: entry.req.connections,
            speed_limit_bps: entry.req.speed_limit_bps,
            checksum: entry.req.checksum.clone(),
            headers: entry.req.headers.clone(),
            schedule_at_unix: entry.req.schedule_at_unix,
            created_at: m.created_at,
            finished_at: m.finished_at,
            error: m.error.clone(),
            actual_sha256: m.actual_sha256.clone(),
            checksum_ok: m.checksum_ok,
            origin: entry.req.origin.clone(),
        };
        drop(m);
        self.store.upsert(&st);
    }
}

pub enum EngineEvent {
    Snapshot(Arc<EngineSnapshot>),
    TaskAdded(TaskId),
    TaskFinished(TaskId, TaskStatus, Option<String>),
    Info(String),
    Stopped,
}

pub enum Cmd {
    Add(AddRequest, TaskId),
    Pause(TaskId),
    Resume(TaskId),
    Cancel(TaskId),
    Remove(TaskId, bool),
    Retry(TaskId),
    SetConfig(EngineConfig),
    Shutdown,
}

/// Public engine handle. Cheap to clone; communicates with the engine thread.
#[derive(Clone)]
pub struct Engine {
    cmd: tokio::sync::mpsc::UnboundedSender<Cmd>,
    snapshot_slot: Arc<RwLock<Arc<EngineSnapshot>>>,
    events_rx: Arc<Mutex<Option<mpsc::Receiver<EngineEvent>>>>,
    thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
}

fn unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Engine {
    /// Start the engine on a dedicated thread (single tokio current-thread runtime).
    pub fn start(cfg: EngineConfig) -> Engine {
        if let Err(e) = std::fs::create_dir_all(&cfg.data_dir) {
            tracing::warn!("cannot create data dir {}: {e}", cfg.data_dir.display());
        }
        if !cfg.download_dir.exists() {
            let _ = std::fs::create_dir_all(&cfg.download_dir);
        }
        let (event_tx, event_rx) = mpsc::channel();
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel::<Cmd>();
        let snapshot_slot: Arc<RwLock<Arc<EngineSnapshot>>> =
            Arc::new(RwLock::new(Arc::new(EngineSnapshot {
                tasks: vec![],
                global: GlobalStats {
                    speed_bps: 0,
                    total_session_bytes: 0,
                    history: vec![],
                    active_tasks: 0,
                    queued_tasks: 0,
                    total_conns: 0,
                    cache_in_use_bytes: 0,
                    cache_budget_bytes: 0,
                },
                config: summary_of(&cfg, 0),
                license: crate::license::load_license(&cfg.data_dir),
                version: crate::VERSION.to_string(),
                started_at_unix: unix_now(),
            })));

        let license = crate::license::load_license(&cfg.data_dir);

        let engine = Engine {
            cmd: cmd_tx,
            snapshot_slot: snapshot_slot.clone(),
            events_rx: Arc::new(Mutex::new(Some(event_rx))),
            thread: Arc::new(Mutex::new(None)),
        };

        let handle = std::thread::Builder::new()
            .name("flux-engine".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build engine runtime");
                rt.block_on(async move {
                    // Clients are built inside the runtime context (reqwest
                    // requires a tokio handle at build time).
                    let store = Arc::new(Store::open(&cfg.data_dir).expect("open task store"));
                    let budget = WriteBudget::new();
                    let global_limiter = Arc::new(TokenBucket::new(
                        cfg.global_speed_limit_bps.unwrap_or(u64::MAX / 2),
                    ));
                    let http = build_http_client(&cfg);
                    let h3 = build_h3_client(&cfg);
                    let ctx = Arc::new(EngineCtx {
                        http,
                        h3,
                        store,
                        cfg: Arc::new(Mutex::new(cfg.clone())),
                        budget: budget.clone(),
                        global_limiter,
                        events: event_tx,
                        active_conns: AtomicU32::new(0),
                        started_at: unix_now(),
                        sys: Arc::new(Mutex::new(sysinfo::System::new())),
                        #[cfg(feature = "torrent")]
                        torrent_session: Arc::new(tokio::sync::Mutex::new(None)),
                    });

                    // Seed the RAM cache budget from current memory state.
                    {
                        let mut sys = ctx.sys.lock().expect("sys");
                        sys.refresh_memory();
                        let avail = sys.available_memory();
                        let b = cfg.ram_cache_budget_bytes(avail);
                        budget.set_budget(b);
                        budget.flush_hint_bytes.store(
                            (b / 16).clamp(256 * 1024, 8 * 1024 * 1024),
                            Ordering::Relaxed,
                        );
                    }

                    engine_main(ctx, snapshot_slot, license, &mut cmd_rx).await;
                });
            })
            .expect("spawn engine thread");
        *engine.thread.lock().expect("thread slot") = Some(handle);

        engine
    }

    pub fn snapshot(&self) -> Arc<EngineSnapshot> {
        self.snapshot_slot.read().expect("snapshot slot").clone()
    }

    pub fn events(&self) -> mpsc::Receiver<EngineEvent> {
        self.events_rx
            .lock()
            .expect("events slot")
            .take()
            .expect("engine events can only be consumed once")
    }

    /// Register a repaint callback (the GUI passes an egui repaint request).
    pub fn set_repaint_callback(&self, f: Box<dyn Fn() + Send + Sync + 'static>) {
        let _ = self.cmd;
        // Delivered via a global slot the engine ticks call.
        REPAINT_CALLBACK
            .lock()
            .expect("repaint callback")
            .replace(f);
    }

    /// Add a download. Returns after URL validation (no network I/O).
    pub fn add(&self, req: AddRequest) -> Result<TaskId> {
        let id = TaskId::new_v4();
        if crate::security::is_torrent_source(&req.url) {
            if !cfg!(feature = "torrent") {
                return Err(FluxError::Unsupported(
                    "this build has no BitTorrent support (rebuild with the `torrent` feature)"
                        .into(),
                ));
            }
        } else {
            security::parse_download_url(&req.url)?;
        }
        self.cmd
            .send(Cmd::Add(req, id))
            .map_err(|_| FluxError::Shutdown)?;
        Ok(id)
    }

    pub fn pause(&self, id: TaskId) -> Result<()> {
        self.cmd
            .send(Cmd::Pause(id))
            .map_err(|_| FluxError::Shutdown)
    }

    pub fn resume(&self, id: TaskId) -> Result<()> {
        self.cmd
            .send(Cmd::Resume(id))
            .map_err(|_| FluxError::Shutdown)
    }

    pub fn cancel(&self, id: TaskId) -> Result<()> {
        self.cmd
            .send(Cmd::Cancel(id))
            .map_err(|_| FluxError::Shutdown)
    }

    /// Remove from the queue; optionally delete the output files.
    pub fn remove(&self, id: TaskId, delete_files: bool) -> Result<()> {
        self.cmd
            .send(Cmd::Remove(id, delete_files))
            .map_err(|_| FluxError::Shutdown)
    }

    pub fn retry(&self, id: TaskId) -> Result<()> {
        self.cmd
            .send(Cmd::Retry(id))
            .map_err(|_| FluxError::Shutdown)
    }

    pub fn set_config(&self, cfg: EngineConfig) -> Result<()> {
        self.cmd
            .send(Cmd::SetConfig(cfg))
            .map_err(|_| FluxError::Shutdown)
    }

    pub fn shutdown(&self) {
        let _ = self.cmd.send(Cmd::Shutdown);
    }

    /// Block until the engine thread exits (for CLI clean shutdown).
    pub fn join(&self) {
        if let Some(h) = self.thread.lock().expect("thread slot").take() {
            let _ = h.join();
        }
    }
}

static REPAINT_CALLBACK: Mutex<Option<Box<dyn Fn() + Send + Sync + 'static>>> = Mutex::new(None);

fn build_http_client(cfg: &EngineConfig) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .user_agent(cfg.user_agent.clone())
        .connect_timeout(Duration::from_secs(cfg.connect_timeout_sec))
        .read_timeout(Duration::from_secs(cfg.read_timeout_sec))
        .redirect(security::redirect_policy(10))
        .pool_idle_timeout(Duration::from_secs(120))
        .pool_max_idle_per_host((cfg.max_total_connections as usize).clamp(8, 256))
        .tcp_nodelay(true);
    if cfg.tls_insecure {
        b = b.danger_accept_invalid_certs(true);
    }
    if let Some(proxy) = &cfg.proxy {
        if let Ok(mut p) = reqwest::Proxy::all(proxy.url.as_str()) {
            if let (Some(u), Some(pw)) = (&proxy.username, &proxy.password) {
                p = p.basic_auth(u, pw);
            }
            b = b.proxy(p);
        } else {
            tracing::warn!("invalid proxy URL {}; ignoring", proxy.url);
        }
    }
    b.build().expect("build HTTP client")
}

fn build_h3_client(cfg: &EngineConfig) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .user_agent(cfg.user_agent.clone())
        .connect_timeout(Duration::from_secs(cfg.connect_timeout_sec))
        .read_timeout(Duration::from_secs(cfg.read_timeout_sec))
        .redirect(security::redirect_policy(10))
        .http3_prior_knowledge()
        .tcp_nodelay(true);
    if cfg.tls_insecure {
        b = b.danger_accept_invalid_certs(true);
    }
    b.build().expect("build HTTP/3 client")
}

fn summary_of(cfg: &EngineConfig, ram_cache_mb: u64) -> ConfigSummary {
    ConfigSummary {
        max_active_tasks: cfg.max_active_tasks,
        global_limit_bps: cfg.global_speed_limit_bps,
        http3_enabled: cfg.http3_enabled,
        torrent_enabled: cfg.torrent_enabled && cfg!(feature = "torrent"),
        torrent_compiled: cfg!(feature = "torrent"),
        proxy: cfg.proxy.as_ref().map(|p| p.url.clone()),
        ram_cache_mb,
        schedule_windows: cfg.schedule.len(),
    }
}

struct EngineLoop {
    ctx: Arc<EngineCtx>,
    tasks: HashMap<TaskId, Arc<TaskEntry>>,
    snapshot_slot: Arc<RwLock<Arc<EngineSnapshot>>>,
    global_prev: (i64, u64),
    global_history: VecDeque<(i64, u64)>,
    global_speed: u64,
    session_bytes: u64,
    last_store_scan: Instant,
    ram_cache_mb: u64,
    license: Option<crate::license::LicenseInfo>,
}

async fn engine_main(
    ctx: Arc<EngineCtx>,
    snapshot_slot: Arc<RwLock<Arc<EngineSnapshot>>>,
    license: Option<crate::license::LicenseInfo>,
    cmd_rx: &mut tokio::sync::mpsc::UnboundedReceiver<Cmd>,
) {
    let mut engine = EngineLoop {
        ctx: ctx.clone(),
        tasks: HashMap::new(),
        snapshot_slot,
        global_prev: (unix_ms(), 0),
        global_history: VecDeque::new(),
        global_speed: 0,
        session_bytes: 0,
        last_store_scan: Instant::now(),
        ram_cache_mb: 0,
        license,
    };

    // Import persisted queue.
    engine.import_persisted_tasks();

    let mut tick = tokio::time::interval(Duration::from_millis(250));
    let mut tick_count: u64 = 0;
    loop {
        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                match cmd {
                    None | Some(Cmd::Shutdown) => break,
                    Some(c) => engine.handle_cmd(c),
                }
            }
            _ = tick.tick() => {
                tick_count += 1;
                engine.tick(tick_count);
                if tick_count % 4 == 0 {
                    engine.slow_tick();
                }
                if tick_count % 20 == 0 {
                    engine.refresh_ram_budget();
                }
            }
        }
    }

    // Graceful shutdown: stop workers, persist state.
    for entry in engine.tasks.values() {
        entry.shared.ctrl.send_replace(Ctrl::Stop);
        if let Some(h) = entry.join.lock().expect("join").take() {
            std::mem::drop(h);
        }
        ctx.persist_task(entry);
    }
    let _ = ctx.events.send(EngineEvent::Stopped);
}

impl EngineLoop {
    fn handle_cmd(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Add(req, id) => self.add_task(req, id),
            Cmd::Pause(id) => self.pause_task(&id),
            Cmd::Resume(id) => self.resume_task(&id),
            Cmd::Cancel(id) => self.cancel_task(&id),
            Cmd::Remove(id, delete_files) => self.remove_task(&id, delete_files),
            Cmd::Retry(id) => self.retry_task(&id),
            Cmd::SetConfig(cfg) => {
                *self.ctx.cfg.lock().expect("config") = cfg;
                let rate = {
                    let c = self.ctx.cfg.lock().expect("config");
                    c.global_speed_limit_bps.unwrap_or(u64::MAX / 2)
                };
                self.ctx.global_limiter.set_rate_bps(rate);
                if let Err(e) = self.ctx.cfg.lock().expect("config").save() {
                    tracing::warn!("failed to persist settings: {e}");
                }
                let _ = self
                    .ctx
                    .events
                    .send(EngineEvent::Info("settings updated".into()));
            }
            Cmd::Shutdown => {}
        }
    }

    fn add_task(&mut self, req: AddRequest, id: TaskId) {
        let output_dir = req
            .output_dir
            .clone()
            .unwrap_or_else(|| self.ctx.cfg.lock().expect("config").download_dir.clone());
        let kind = if crate::security::is_torrent_source(&req.url) {
            TaskKind::Torrent
        } else {
            TaskKind::Http
        };
        let entry = Arc::new(TaskEntry {
            id,
            req,
            kind,
            mutable: Arc::new(Mutex::new(TaskMutable {
                status: TaskStatus::Queued,
                filename: String::new(),
                output_dir,
                size: None,
                protocol: None,
                resume_supported: false,
                error: None,
                expected_sha256: None,
                actual_sha256: None,
                checksum_ok: None,
                speed_bps: 0,
                avg_bps: 0,
                eta_sec: None,
                history: VecDeque::new(),
                last_sample: (unix_ms(), 0),
                started_at: None,
                created_at: unix_now(),
                finished_at: None,
            })),
            shared: Arc::new(TaskShared {
                id,
                net_bytes: AtomicU64::new(0),
                buffered: Arc::new(AtomicU64::new(0)),
                progress_base: AtomicU64::new(0),
                retries: AtomicU32::new(0),
                ctrl: tokio::sync::watch::channel(Ctrl::Run).0,
                storage: Mutex::new(None),
                segs: Mutex::new(Vec::new()),
            }),
            join: Arc::new(Mutex::new(None)),
            #[cfg(feature = "torrent")]
            torrent: Arc::new(Mutex::new(None)),
        });
        self.ctx.persist_task(&entry);
        self.tasks.insert(id, entry.clone());
        let _ = self.ctx.events.send(EngineEvent::TaskAdded(id));
        if entry.req.auto_start && entry.req.schedule_at_unix.is_none() {
            self.try_start(&id);
        }
    }

    fn import_persisted_tasks(&mut self) {
        let stored = self.ctx.store.list();
        let cfg = self.ctx.cfg.lock().expect("config").clone();
        let mut count = 0;
        for st in stored {
            let entry = self.entry_from_stored(&st, cfg.auto_resume_on_start);
            self.tasks.insert(st.id, entry);
            count += 1;
        }
        if count > 0 {
            let _ = self.ctx.events.send(EngineEvent::Info(format!(
                "restored {count} task(s) from the previous session"
            )));
        }
    }

    fn entry_from_stored(&self, st: &crate::task::StoredTask, auto_resume: bool) -> Arc<TaskEntry> {
        let status = if st.status.is_terminal() {
            st.status
        } else if auto_resume {
            TaskStatus::Queued
        } else {
            TaskStatus::Paused
        };
        let req = AddRequest {
            url: st.url.clone(),
            output_dir: Some(st.output_dir.clone()),
            filename: if st.filename.is_empty() {
                None
            } else {
                Some(st.filename.clone())
            },
            connections: st.connections,
            speed_limit_bps: st.speed_limit_bps,
            checksum: st.checksum.clone(),
            headers: st.headers.clone(),
            schedule_at_unix: st.schedule_at_unix,
            auto_start: true,
            origin: st.origin.clone(),
        };
        let entry = Arc::new(TaskEntry {
            id: st.id,
            req,
            kind: st.kind,
            mutable: Arc::new(Mutex::new(TaskMutable {
                status,
                filename: st.filename.clone(),
                output_dir: st.output_dir.clone(),
                size: st.size,
                protocol: None,
                resume_supported: false,
                error: st.error.clone(),
                expected_sha256: st.checksum.clone(),
                actual_sha256: st.actual_sha256.clone(),
                checksum_ok: st.checksum_ok,
                speed_bps: 0,
                avg_bps: 0,
                eta_sec: None,
                history: VecDeque::new(),
                last_sample: (unix_ms(), 0),
                started_at: None,
                created_at: st.created_at,
                finished_at: st.finished_at,
            })),
            shared: Arc::new(TaskShared {
                id: st.id,
                net_bytes: AtomicU64::new(0),
                buffered: Arc::new(AtomicU64::new(0)),
                progress_base: AtomicU64::new(0),
                retries: AtomicU32::new(0),
                ctrl: tokio::sync::watch::channel(Ctrl::Run).0,
                storage: Mutex::new(None),
                segs: Mutex::new(Vec::new()),
            }),
            join: Arc::new(Mutex::new(None)),
            #[cfg(feature = "torrent")]
            torrent: Arc::new(Mutex::new(None)),
        });
        if !status.is_terminal() {
            self.ctx.persist_task(&entry);
        }
        entry
    }

    fn active_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|e| {
                let m = e.mutable.lock().expect("task");
                m.status.is_active() && e.join.lock().expect("join").is_some()
            })
            .count()
    }

    fn try_start(&mut self, id: &TaskId) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        if entry.join.lock().expect("join").is_some() {
            return;
        }
        let status = entry.mutable.lock().expect("task").status;
        if !matches!(status, TaskStatus::Queued) {
            return;
        }
        let max_active = self.ctx.cfg.lock().expect("config").max_active_tasks;
        if self.active_count() >= max_active {
            return;
        }
        // Schedule gate.
        if let Some(at) = entry.req.schedule_at_unix {
            if unix_now() < at {
                return;
            }
        }
        {
            let mut m = entry.mutable.lock().expect("task");
            // Claim an active status immediately so active_count() sees this
            // task before the supervisor's first status update lands.
            if m.status == TaskStatus::Queued {
                m.status = TaskStatus::Probing;
            }
            m.started_at.replace(unix_now());
        }
        let ectx = self.ctx.clone();
        let entry2 = entry.clone();
        let join = match entry.kind {
            TaskKind::Http => tokio::spawn(async move {
                download::run_http_task(ectx, entry2).await;
            }),
            #[cfg(feature = "torrent")]
            TaskKind::Torrent => tokio::spawn(async move {
                crate::torrent::run_torrent_task(ectx, entry2).await;
            }),
            #[cfg(not(feature = "torrent"))]
            TaskKind::Torrent => tokio::spawn(async move {
                download::set_status(
                    &entry2,
                    TaskStatus::Failed,
                    Some("BitTorrent support not compiled in this build".into()),
                );
                download::notify_finished(&ectx, &entry2, TaskStatus::Failed);
            }),
        };
        *entry.join.lock().expect("join") = Some(join);
    }

    fn pause_task(&mut self, id: &TaskId) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        let mut m = entry.mutable.lock().expect("task");
        match m.status {
            TaskStatus::Queued => {
                m.status = TaskStatus::Paused;
                drop(m);
                self.ctx.persist_task(entry);
            }
            s if s.is_active() => {
                drop(m);
                entry.shared.ctrl.send_replace(Ctrl::Pause);
                // Supervisor will mark Paused once workers drain.
            }
            _ => {}
        }
    }

    fn resume_task(&mut self, id: &TaskId) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        {
            let mut m = entry.mutable.lock().expect("task");
            match m.status {
                TaskStatus::Paused | TaskStatus::Queued => {
                    m.status = TaskStatus::Queued;
                    m.error = None;
                }
                TaskStatus::Cancelled => {
                    m.status = TaskStatus::Queued;
                    m.error = None;
                }
                _ => return,
            }
            drop(m);
        }
        entry.shared.ctrl.send_replace(Ctrl::Run);
        *entry.join.lock().expect("join") = None;
        self.ctx.persist_task(entry);
        self.try_start(id);
    }

    fn cancel_task(&mut self, id: &TaskId) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        {
            let m = entry.mutable.lock().expect("task");
            if m.status.is_terminal() {
                return;
            }
        }
        entry.shared.ctrl.send_replace(Ctrl::Stop);
        // If it was queued (not running), finalize immediately.
        let running = entry.join.lock().expect("join").is_some();
        if !running {
            let mut m = entry.mutable.lock().expect("task");
            m.status = TaskStatus::Cancelled;
            m.finished_at = Some(unix_now());
            drop(m);
            self.ctx.persist_task(entry);
            let _ =
                self.ctx
                    .events
                    .send(EngineEvent::TaskFinished(*id, TaskStatus::Cancelled, None));
        }
    }

    fn remove_task(&mut self, id: &TaskId, delete_files: bool) {
        let Some(entry) = self.tasks.remove(id) else {
            return;
        };
        entry.shared.ctrl.send_replace(Ctrl::Stop);
        self.ctx.store.remove(*id);
        if delete_files {
            let m = entry.mutable.lock().expect("task");
            let final_path = m.output_dir.join(&m.filename);
            let _ = std::fs::remove_file(&final_path);
            let _ = std::fs::remove_file(crate::storage::part_path_for(&final_path));
            let _ = std::fs::remove_file(crate::storage::meta_path_for(&final_path));
        }
    }

    fn retry_task(&mut self, id: &TaskId) {
        let Some(entry) = self.tasks.get(id) else {
            return;
        };
        {
            let mut m = entry.mutable.lock().expect("task");
            if !matches!(m.status, TaskStatus::Failed | TaskStatus::Cancelled) {
                return;
            }
            m.status = TaskStatus::Queued;
            m.error = None;
            m.finished_at = None;
            m.checksum_ok = None;
        }
        entry.shared.net_bytes.store(0, Ordering::Relaxed);
        entry.shared.progress_base.store(0, Ordering::Relaxed);
        *entry.join.lock().expect("join") = None;
        self.ctx.persist_task(entry);
        self.try_start(id);
    }

    fn tick(&mut self, _n: u64) {
        let now = unix_ms();
        let entries: Vec<Arc<TaskEntry>> = self.tasks.values().cloned().collect();
        let mut global_progress: u64 = 0;
        let mut active = 0;
        let mut queued = 0;
        let mut snapshots = Vec::with_capacity(entries.len());

        for entry in &entries {
            let progress = self.sample_task_speed(entry, now);
            global_progress = global_progress.saturating_add(progress);
            let m = entry.mutable.lock().expect("task");
            match m.status {
                s if s.is_active() && entry.join.lock().expect("join").is_some() => active += 1,
                TaskStatus::Queued => queued += 1,
                _ => {}
            }
            snapshots.push(self.build_snapshot(entry, &m));
        }

        // Global speed: sample on displayed progress.
        let prev_sample = self.global_prev;
        let dt = ((now - prev_sample.0).max(1)) as f64 / 1000.0;
        let dbytes = global_progress.saturating_sub(prev_sample.1);
        self.global_speed = if dt > 0.0 {
            (dbytes as f64 / dt) as u64
        } else {
            0
        };
        self.session_bytes = self.session_bytes.saturating_add(dbytes);
        if (now / 1000) != (prev_sample.0 / 1000) || self.global_history.is_empty() {
            download::push_history(&mut self.global_history, now, self.global_speed);
            while self.global_history.len() > 240 {
                self.global_history.pop_front();
            }
        }
        self.global_prev = (now, global_progress);

        let snapshot = Arc::new(EngineSnapshot {
            tasks: snapshots,
            global: GlobalStats {
                speed_bps: self.global_speed,
                total_session_bytes: self.session_bytes,
                history: self.global_history.iter().copied().collect(),
                active_tasks: active,
                queued_tasks: queued,
                total_conns: self.ctx.active_conns.load(Ordering::Relaxed),
                cache_in_use_bytes: self.ctx.budget.in_use(),
                cache_budget_bytes: self.ctx.budget.budget(),
            },
            config: summary_of(&self.ctx.cfg.lock().expect("config"), self.ram_cache_mb),
            license: self.license.clone(),
            version: crate::VERSION.to_string(),
            started_at_unix: self.ctx.started_at,
        });
        *self.snapshot_slot.write().expect("snapshot slot") = snapshot.clone();
        let _ = self.ctx.events.send(EngineEvent::Snapshot(snapshot));
        if let Some(f) = REPAINT_CALLBACK.lock().expect("repaint").as_ref() {
            f();
        }
    }

    fn sample_task_speed(&mut self, entry: &Arc<TaskEntry>, now: i64) -> u64 {
        let progress = task_progress(entry);
        if entry.kind == crate::task::TaskKind::Torrent {
            // Speed for torrent tasks comes from librqbit live stats (set by the
            // torrent supervision loop); the ticker only records history/ETA.
            let mut m = entry.mutable.lock().expect("task");
            let prev_sample = m.last_sample;
            if (now / 1000) != (prev_sample.0 / 1000) || m.history.is_empty() {
                let sp = m.speed_bps;
                download::push_history(&mut m.history, now, sp);
            }
            m.last_sample = (now, progress);
            if let (Some(start), Some(size)) = (m.started_at, m.size) {
                let elapsed = (now / 1000 - start).max(1) as u64;
                m.avg_bps = progress / elapsed;
                let remaining = size.saturating_sub(progress);
                m.eta_sec = if m.speed_bps > 1024 {
                    Some(remaining / m.speed_bps)
                } else {
                    None
                };
            }
            return progress;
        }
        download::sample_seg_speeds(entry, 0.25);
        let mut m = entry.mutable.lock().expect("task");
        let prev_sample = m.last_sample;
        let dt = ((now - prev_sample.0).max(1)) as f64 / 1000.0;
        let dbytes = progress.saturating_sub(prev_sample.1);
        let inst = if dt > 0.0 {
            (dbytes as f64 / dt) as u64
        } else {
            0
        };
        // EMA smoothing for display.
        m.speed_bps = if m.speed_bps == 0 {
            inst
        } else {
            ((m.speed_bps as f64 * 0.7) + (inst as f64 * 0.3)) as u64
        };
        if (now / 1000) != (prev_sample.0 / 1000) || m.history.is_empty() {
            let sp = m.speed_bps;
            download::push_history(&mut m.history, now, sp);
        }
        m.last_sample = (now, progress);
        if let (Some(start), Some(size)) = (m.started_at, m.size) {
            let elapsed = (now / 1000 - start).max(1) as u64;
            m.avg_bps = progress / elapsed;
            let remaining = size.saturating_sub(progress);
            m.eta_sec = if m.speed_bps > 1024 {
                Some(remaining / m.speed_bps)
            } else {
                None
            };
        }
        progress
    }

    fn build_snapshot(&self, entry: &Arc<TaskEntry>, m: &TaskMutable) -> TaskSnapshot {
        // NOTE: guards must be dropped before the struct literal below;
        // field temporaries live for the whole expression and re-locking
        // a non-reentrant Mutex from another field would self-deadlock.
        let done_ranges = {
            let slot = entry.shared.storage.lock().expect("storage");
            slot.as_ref().map(|s| s.done_ranges()).unwrap_or_default()
        };
        let active_conns = entry.shared.segs.lock().expect("segs").len() as u32;
        let segments = download::segment_snaps(entry);
        TaskSnapshot {
            id: entry.id,
            url: entry.req.url.clone(),
            filename: m.filename.clone(),
            output_dir: m.output_dir.clone(),
            kind: entry.kind,
            status: m.status,
            size: m.size,
            downloaded: task_progress(entry),
            net_bytes: entry.shared.net_bytes.load(Ordering::Relaxed),
            speed_bps: m.speed_bps,
            avg_bps: m.avg_bps,
            eta_sec: m.eta_sec,
            error: m.error.clone(),
            protocol: m.protocol.clone(),
            resume_supported: m.resume_supported,
            expected_sha256: m.expected_sha256.clone(),
            actual_sha256: m.actual_sha256.clone(),
            checksum_ok: m.checksum_ok,
            retries: entry.shared.retries.load(Ordering::Relaxed),
            active_conns,
            segments,
            speed_history: m.history.iter().copied().collect(),
            done_ranges,
            created_at: m.created_at,
            finished_at: m.finished_at,
        }
    }

    fn slow_tick(&mut self) {
        // Start queued tasks as slots free up.
        let ids: Vec<TaskId> = {
            let mut v = Vec::new();
            for (id, entry) in &self.tasks {
                let m = entry.mutable.lock().expect("task");
                if matches!(m.status, TaskStatus::Queued) {
                    v.push(*id);
                }
            }
            v.sort_by_key(|id| {
                let e = &self.tasks[id];
                e.mutable.lock().expect("task").created_at
            });
            v
        };
        for id in ids {
            self.try_start(&id);
        }

        // Bandwidth schedule (local-time windows).
        let rate = self.schedule_rate_now().unwrap_or_else(|| {
            self.ctx
                .cfg
                .lock()
                .expect("config")
                .global_speed_limit_bps
                .unwrap_or(u64::MAX / 2)
        });
        self.ctx.global_limiter.set_rate_bps(rate);

        // Import tasks added by other processes (browser bridge).
        if self.last_store_scan.elapsed() >= Duration::from_secs(2) {
            self.last_store_scan = Instant::now();
            let known: Vec<TaskId> = self.tasks.keys().copied().collect();
            let added = self.ctx.store.import_external(&known);
            for st in added {
                let auto = self.ctx.cfg.lock().expect("config").auto_resume_on_start;
                let entry = self.entry_from_stored(&st, auto);
                self.tasks.insert(st.id, entry.clone());
                let _ = self.ctx.events.send(EngineEvent::TaskAdded(st.id));
                let _ = self.ctx.events.send(EngineEvent::Info(format!(
                    "task received from {}: {}",
                    st.origin.as_deref().unwrap_or("external source"),
                    st.filename
                )));
                let id = st.id;
                self.try_start(&id);
            }
        }
    }

    /// Active scheduled rate limit for the current local time, if any window matches.
    fn schedule_rate_now(&self) -> Option<u64> {
        let cfg = self.ctx.cfg.lock().expect("config");
        if cfg.schedule.is_empty() {
            return None;
        }
        let now = chrono::Local::now();
        let minutes = now.hour() as i32 * 60 + now.minute() as i32;
        for w in &cfg.schedule {
            if window_active(w, minutes) {
                return Some(w.limit_bps.unwrap_or(u64::MAX / 2));
            }
        }
        None
    }

    fn refresh_ram_budget(&mut self) {
        let cfg = self.ctx.cfg.lock().expect("config").clone();
        let mut sys = self.ctx.sys.lock().expect("sys");
        sys.refresh_memory();
        let avail = sys.available_memory();
        let budget = cfg.ram_cache_budget_bytes(avail);
        let budget_mb = budget / (1024 * 1024);
        self.ram_cache_mb = budget_mb;
        self.ctx.budget.set_budget(budget);
        let workers = (self.active_count().max(1) as u64)
            * (self.ctx.cfg.lock().expect("config").default_connections as u64).max(1);
        let hint = (budget / workers.max(1)).clamp(256 * 1024, 8 * 1024 * 1024);
        self.ctx
            .budget
            .flush_hint_bytes
            .store(hint, Ordering::Relaxed);
    }
}

fn window_active(w: &RateWindow, now_minutes: i32) -> bool {
    let parse = |s: &str| -> Option<i32> {
        let (h, m) = s.split_once(':')?;
        Some(h.trim().parse::<i32>().ok()? * 60 + m.trim().parse::<i32>().ok()?)
    };
    let (Some(start), Some(end)) = (parse(&w.start), parse(&w.end)) else {
        return false;
    };
    if start <= end {
        now_minutes >= start && now_minutes < end
    } else {
        now_minutes >= start || now_minutes < end
    }
}

fn task_progress(entry: &Arc<TaskEntry>) -> u64 {
    if entry.kind == crate::task::TaskKind::Torrent {
        #[cfg(feature = "torrent")]
        if let Some(p) = crate::torrent::torrent_progress(entry) {
            return p;
        }
        #[cfg(not(feature = "torrent"))]
        return 0;
    }
    // `bytes_done` (durable ranges) already includes bytes resumed from disk.
    let flushed = entry
        .shared
        .storage
        .lock()
        .expect("storage")
        .as_ref()
        .map(|s| s.bytes_done())
        .unwrap_or(0);
    let buffered = entry.shared.buffered.load(Ordering::Relaxed);
    flushed.saturating_add(buffered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_window_matching() {
        let w = RateWindow {
            start: "22:00".into(),
            end: "06:00".into(),
            limit_bps: Some(1024),
        };
        assert!(window_active(&w, 23 * 60));
        assert!(window_active(&w, 3 * 60));
        assert!(!window_active(&w, 12 * 60));
        let w2 = RateWindow {
            start: "09:00".into(),
            end: "17:00".into(),
            limit_bps: None,
        };
        assert!(window_active(&w2, 12 * 60));
        assert!(!window_active(&w2, 20 * 60));
    }
}
