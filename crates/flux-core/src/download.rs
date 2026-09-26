//! Multi-segment download execution: supervisor + connection workers.

use crate::engine::{Ctrl, EngineCtx, TaskEntry, TaskShared};
use crate::errors::Result;
use crate::probe::{self, ProbeInfo};
use crate::security;
use crate::storage::{dedupe_filename, Expected, PartialFile, WriteBudget, WriteBuffer};
use crate::task::{SegState, SegmentSnap, TaskStatus};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Per-connection live state (drives the connections detail view).
pub(crate) struct SegStat {
    pub start: AtomicU64,
    pub end: AtomicU64,
    pub done: AtomicU64,
    pub state: AtomicU8,
    pub speed_bps: AtomicU64,
    pub retries: std::sync::atomic::AtomicU32,
    /// (last sample time, done bytes) for speed computation.
    pub last: Mutex<(Instant, u64)>,
}

impl SegStat {
    fn new(start: u64, end: u64) -> Arc<Self> {
        Arc::new(Self {
            start: AtomicU64::new(start),
            end: AtomicU64::new(end),
            done: AtomicU64::new(0),
            state: AtomicU8::new(SegState::Connecting as u8),
            speed_bps: AtomicU64::new(0),
            retries: std::sync::atomic::AtomicU32::new(0),
            last: Mutex::new((Instant::now(), 0)),
        })
    }

    pub fn seg_state(&self) -> SegState {
        match self.state.load(Ordering::Relaxed) {
            1 => SegState::Connecting,
            2 => SegState::Receiving,
            3 => SegState::Retrying,
            4 => SegState::Done,
            _ => SegState::Pending,
        }
    }
}

pub(crate) fn set_seg_state(stat: &SegStat, s: SegState) {
    stat.state.store(s as u8, Ordering::Relaxed);
}

/// Work queue over the remaining byte gaps. Workers lease chunks from the
/// largest gap (its tail), which yields IDM-style coverage spreading.
pub(crate) struct GapQueue {
    gaps: Mutex<Vec<(u64, u64)>>,
    lease_hint: u64,
}

impl GapQueue {
    pub fn new(mut gaps: Vec<(u64, u64)>, lease_hint: u64) -> Self {
        gaps.sort_unstable();
        Self {
            gaps: Mutex::new(gaps),
            lease_hint: lease_hint.max(256 * 1024),
        }
    }

    pub fn pop_lease(&self) -> Option<(u64, u64)> {
        let mut gaps = self.gaps.lock().expect("gap queue");
        let idx = gaps
            .iter()
            .enumerate()
            .max_by_key(|(_, &(s, e))| e - s)
            .map(|(i, _)| i)?;
        let (gs, ge) = gaps[idx];
        let len = ge - gs;
        if len <= self.lease_hint {
            gaps.remove(idx);
            Some((gs, ge))
        } else {
            let lease = (ge - self.lease_hint, ge);
            gaps[idx] = (gs, ge - self.lease_hint);
            Some(lease)
        }
    }

    pub fn requeue(&self, range: (u64, u64)) {
        if range.1 <= range.0 {
            return;
        }
        self.gaps.lock().expect("gap queue").push(range);
    }

    pub fn remaining_bytes(&self) -> u64 {
        self.gaps
            .lock()
            .expect("gap queue")
            .iter()
            .map(|(s, e)| e - s)
            .sum()
    }

    pub fn is_empty(&self) -> bool {
        self.gaps.lock().expect("gap queue").is_empty()
    }
}

pub(crate) enum WorkerOut {
    /// No leases left; worker exits normally.
    Idle,
    Paused,
    Stopped,
    Failed(String),
    ServerChanged(String),
    /// Single-stream mode completed with this many bytes.
    SingleStreamDone(u64),
}

struct WorkerCtx {
    shared: Arc<TaskShared>,
    client: reqwest::Client,
    url: String,
    headers: Vec<(String, String)>,
    etag: Option<String>,
    gap_queue: Arc<GapQueue>,
    storage: Arc<PartialFile>,
    global_limiter: Arc<crate::limit::TokenBucket>,
    task_limiter: Option<Arc<crate::limit::TokenBucket>>,
    budget: Arc<WriteBudget>,
    range_mode: bool,
    single_total: Option<u64>,
    max_retries: u32,
    backoff_ms: u64,
    /// Route requests through the QUIC pool (per-request HTTP/3 marking).
    h3: bool,
}

/// Sleep with exponential backoff; returns Some(out) if paused/stopped meanwhile.
async fn backoff(
    ctrl: &mut tokio::sync::watch::Receiver<Ctrl>,
    retry: u32,
    base_ms: u64,
) -> Option<WorkerOut> {
    let exp = base_ms.saturating_mul(1u64 << retry.min(5).max(1));
    let ms = exp.min(30_000).max(50);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(ms)) => None,
        _ = ctrl.changed() => match *ctrl.borrow() {
            Ctrl::Pause => Some(WorkerOut::Paused),
            _ => Some(WorkerOut::Stopped),
        }
    }
}

async fn acquire_limits(ctx: &WorkerCtx, n: usize) {
    ctx.global_limiter.acquire(n).await;
    if let Some(task_limiter) = &ctx.task_limiter {
        task_limiter.acquire(n).await;
    }
}

fn flush_storage(storage: &Arc<PartialFile>, buf: &mut WriteBuffer) -> Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let (off, data) = buf.take();
    storage.flush_batch(off, &data)
}

/// One connection worker: leases byte ranges and streams them to storage.
async fn run_worker(ctx: WorkerCtx) -> WorkerOut {
    let mut ctrl = ctx.shared.ctrl.subscribe();
    let buffered_counter = ctx.shared.buffered.clone();
    let mut buf = WriteBuffer::new(Some(ctx.budget.clone()), Some(buffered_counter));

    'outer: loop {
        match *ctrl.borrow() {
            Ctrl::Run => {}
            Ctrl::Pause => return WorkerOut::Paused,
            Ctrl::Stop => return WorkerOut::Stopped,
        }
        if !ctx.range_mode {
            return run_single_stream(ctx, ctrl, buf).await;
        }
        let lease = match ctx.gap_queue.pop_lease() {
            None => return WorkerOut::Idle,
            Some(l) => l,
        };

        let stat = SegStat::new(lease.0, lease.1);
        ctx.shared.register_seg(stat.clone());
        let mut cur = lease.0;
        let mut retries: u32 = 0;
        let _last_flush = Instant::now();

        'lease: loop {
            match *ctrl.borrow() {
                Ctrl::Run => {}
                Ctrl::Pause | Ctrl::Stop => {
                    let _ = flush_storage(&ctx.storage, &mut buf);
                    ctx.shared.unregister_seg(&stat);
                    return match *ctrl.borrow() {
                        Ctrl::Pause => WorkerOut::Paused,
                        _ => WorkerOut::Stopped,
                    };
                }
            }
            set_seg_state(&stat, SegState::Connecting);

            let mut req = ctx.client.get(ctx.url.as_str());
            req = req.header("Range", format!("bytes={}-{}", cur, lease.1 - 1));
            if ctx.h3 {
                req = req.version(reqwest::Version::HTTP_3);
            }
            if let Some(etag) = &ctx.etag {
                req = req.header("If-Range", etag.clone());
            }
            for (k, v) in &ctx.headers {
                req = req.header(k.as_str(), v.as_str());
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    retries += 1;
                    stat.retries.store(retries, Ordering::Relaxed);
                    if retries > ctx.max_retries {
                        ctx.shared.unregister_seg(&stat);
                        return WorkerOut::Failed(format!("connection failed: {e}"));
                    }
                    set_seg_state(&stat, SegState::Retrying);
                    if let Some(early) = backoff(&mut ctrl, retries, ctx.backoff_ms).await {
                        ctx.shared.unregister_seg(&stat);
                        return early;
                    }
                    continue 'lease;
                }
            };

            let status = resp.status();
            if status.as_u16() == 200 {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::ServerChanged(
                    "server returned the full body instead of the requested range".into(),
                );
            }
            if status.as_u16() == 416 {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::ServerChanged("range not satisfiable (content changed)".into());
            }
            if status.as_u16() == 404 {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::Failed("404 not found".into());
            }
            if status.is_server_error() || status.is_client_error() {
                retries += 1;
                stat.retries.store(retries, Ordering::Relaxed);
                if retries > ctx.max_retries {
                    ctx.shared.unregister_seg(&stat);
                    return WorkerOut::Failed(format!("HTTP {}", status.as_u16()));
                }
                set_seg_state(&stat, SegState::Retrying);
                if let Some(early) = backoff(&mut ctrl, retries, ctx.backoff_ms).await {
                    ctx.shared.unregister_seg(&stat);
                    return early;
                }
                continue 'lease;
            }
            // Validate the returned range actually starts at `cur`.
            let range_ok = resp
                .headers()
                .get("Content-Range")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("bytes "))
                .and_then(|v| v.split('-').next())
                .and_then(|v| v.parse::<u64>().ok())
                .map(|start| start == cur)
                .unwrap_or(true);
            if !range_ok {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::ServerChanged("server resumed at a different offset".into());
            }

            set_seg_state(&stat, SegState::Receiving);
            let mut stream = resp.bytes_stream();
            use futures_util::StreamExt;
            let mut stream_error = false;
            let mut last_flush = Instant::now();

            'stream: loop {
                tokio::select! {
                    biased;
                    _ = ctrl.changed() => {
                        let _ = flush_storage(&ctx.storage, &mut buf);
                        ctx.shared.unregister_seg(&stat);
                        return match *ctrl.borrow() {
                            Ctrl::Pause => WorkerOut::Paused,
                            _ => WorkerOut::Stopped,
                        };
                    }
                    chunk = stream.next() => {
                        match chunk {
                            Some(Ok(bytes)) => {
                                let n = bytes.len();
                                acquire_limits(&ctx, n).await;
                                buf.append(cur, &bytes);
                                cur += n as u64;
                                ctx.shared.net_bytes.fetch_add(n as u64, Ordering::Relaxed);
                                stat.done.store(cur - lease.0, Ordering::Relaxed);
                                let hint = ctx.budget.flush_hint_bytes.load(Ordering::Relaxed).max(256 * 1024);
                                // Flush on size threshold OR every 2 s so slow
                                // streams still reach durable storage quickly.
                                if buf.should_flush(hint) || (!buf.is_empty() && last_flush.elapsed() > Duration::from_secs(2)) {
                                    if let Err(e) = flush_storage(&ctx.storage, &mut buf) {
                                        ctx.shared.unregister_seg(&stat);
                                        return WorkerOut::Failed(e.to_string());
                                    }
                                    last_flush = Instant::now();
                                }
                                if cur >= lease.1 {
                                    break 'stream;
                                }
                            }
                            Some(Err(e)) => {
                                tracing::debug!(task = %ctx.shared.id, "stream error: {e}");
                                stream_error = true;
                                break 'stream;
                            }
                            None => break 'stream,
                        }
                    }
                }
            }

            if let Err(e) = flush_storage(&ctx.storage, &mut buf) {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::Failed(e.to_string());
            }

            if !stream_error && cur >= lease.1 {
                set_seg_state(&stat, SegState::Done);
                ctx.shared.unregister_seg(&stat);
                continue 'outer; // next lease
            }

            // Incomplete lease (stream error / early EOF): requeue the rest.
            retries += 1;
            stat.retries.store(retries, Ordering::Relaxed);
            if retries > ctx.max_retries {
                ctx.shared.unregister_seg(&stat);
                return WorkerOut::Failed("connection dropped too many times".into());
            }
            set_seg_state(&stat, SegState::Retrying);
            ctx.gap_queue.requeue((cur, lease.1));
            if let Some(early) = backoff(&mut ctrl, retries, ctx.backoff_ms).await {
                ctx.shared.unregister_seg(&stat);
                return early;
            }
        }
    }
}

/// Single-connection mode for servers without range support.
async fn run_single_stream(
    ctx: WorkerCtx,
    mut ctrl: tokio::sync::watch::Receiver<Ctrl>,
    mut buf: WriteBuffer,
) -> WorkerOut {
    let stat = SegStat::new(0, ctx.single_total.unwrap_or(u64::MAX));
    ctx.shared.register_seg(stat.clone());
    let mut cur = 0u64;
    let mut retries = 0u32;
    let mut last_flush = Instant::now();
    loop {
        match *ctrl.borrow() {
            Ctrl::Run => {}
            Ctrl::Pause | Ctrl::Stop => {
                let _ = flush_storage(&ctx.storage, &mut buf);
                ctx.shared.unregister_seg(&stat);
                return match *ctrl.borrow() {
                    Ctrl::Pause => WorkerOut::Paused,
                    _ => WorkerOut::Stopped,
                };
            }
        }
        set_seg_state(&stat, SegState::Connecting);
        let mut req = ctx.client.get(ctx.url.as_str());
        if ctx.h3 {
            req = req.version(reqwest::Version::HTTP_3);
        }
        for (k, v) in &ctx.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                retries += 1;
                if retries > ctx.max_retries {
                    ctx.shared.unregister_seg(&stat);
                    return WorkerOut::Failed(format!("connection failed: {e}"));
                }
                if let Some(early) = backoff(&mut ctrl, retries, ctx.backoff_ms).await {
                    ctx.shared.unregister_seg(&stat);
                    return early;
                }
                continue;
            }
        };
        if !resp.status().is_success() {
            ctx.shared.unregister_seg(&stat);
            return WorkerOut::Failed(format!("HTTP {}", resp.status().as_u16()));
        }
        set_seg_state(&stat, SegState::Receiving);
        let mut stream = resp.bytes_stream();
        use futures_util::StreamExt;
        let mut stream_error = false;

        'stream: loop {
            tokio::select! {
                biased;
                _ = ctrl.changed() => {
                    let _ = flush_storage(&ctx.storage, &mut buf);
                    ctx.shared.unregister_seg(&stat);
                    return match *ctrl.borrow() {
                        Ctrl::Pause => WorkerOut::Paused,
                        _ => WorkerOut::Stopped,
                    };
                }
                chunk = stream.next() => {
                    match chunk {
                        Some(Ok(bytes)) => {
                            let n = bytes.len();
                            acquire_limits(&ctx, n).await;
                            buf.append(cur, &bytes);
                            cur += n as u64;
                            ctx.shared.net_bytes.fetch_add(n as u64, Ordering::Relaxed);
                            stat.done.store(cur, Ordering::Relaxed);
                            let hint = ctx.budget.flush_hint_bytes.load(Ordering::Relaxed).max(256 * 1024);
                            if buf.should_flush(hint) || (!buf.is_empty() && last_flush.elapsed() > Duration::from_secs(2)) {
                                if let Err(e) = flush_storage(&ctx.storage, &mut buf) {
                                    ctx.shared.unregister_seg(&stat);
                                    return WorkerOut::Failed(e.to_string());
                                }
                                last_flush = Instant::now();
                            }
                        }
                        Some(Err(e)) => {
                            tracing::debug!(task = %ctx.shared.id, "stream error: {e}");
                            stream_error = true;
                            break 'stream;
                        }
                        None => break 'stream,
                    }
                }
            }
        }

        if let Err(e) = flush_storage(&ctx.storage, &mut buf) {
            ctx.shared.unregister_seg(&stat);
            return WorkerOut::Failed(e.to_string());
        }
        if !stream_error {
            set_seg_state(&stat, SegState::Done);
            ctx.shared.unregister_seg(&stat);
            return WorkerOut::SingleStreamDone(cur);
        }
        retries += 1;
        if retries > ctx.max_retries {
            ctx.shared.unregister_seg(&stat);
            return WorkerOut::Failed("connection dropped too many times".into());
        }
        set_seg_state(&stat, SegState::Retrying);
        if let Some(early) = backoff(&mut ctrl, retries, ctx.backoff_ms).await {
            ctx.shared.unregister_seg(&stat);
            return early;
        }
    }
}

/// The supervisor for one HTTP task: probe -> plan -> workers -> verify -> finalize.
pub(crate) async fn run_http_task(ectx: Arc<EngineCtx>, entry: Arc<TaskEntry>) {
    let mut restarts: u32 = 0;
    loop {
        match run_http_task_once(ectx.clone(), entry.clone()).await {
            HttpRunEnd::Restart(reason) => {
                restarts += 1;
                if restarts > 2 {
                    set_status(&entry, TaskStatus::Failed, Some(reason));
                    notify_finished(&ectx, &entry, TaskStatus::Failed);
                    return;
                }
                tracing::info!(task = %entry.id, "restarting task cleanly: {reason}");
                if let Some(storage) = entry.shared.storage.lock().expect("storage").take() {
                    storage.discard();
                }
                entry.shared.progress_base.store(0, Ordering::Relaxed);
                entry.shared.net_bytes.store(0, Ordering::Relaxed);
            }
            HttpRunEnd::Done => return,
        }
    }
}

enum HttpRunEnd {
    Done,
    Restart(String),
}

async fn run_http_task_once(ectx: Arc<EngineCtx>, entry: Arc<TaskEntry>) -> HttpRunEnd {
    // Scope the config lock: a MutexGuard must never live across an await
    // (it would make the supervisor future non-Send).
    let (
        connect_timeout,
        verify_on_finish,
        default_conns,
        max_conns,
        min_seg,
        max_retries,
        backoff_ms,
        h3_enabled,
        default_task_limit,
    ) = {
        let g = ectx.cfg.lock().expect("config");
        (
            Duration::from_secs(g.connect_timeout_sec),
            g.verify_sha256_on_finish,
            g.default_connections,
            g.max_connections_per_task,
            g.min_segment_mb.max(1) * 1024 * 1024,
            g.max_retries_per_segment,
            g.retry_backoff_ms,
            g.http3_enabled,
            g.per_task_limit_default_bps,
        )
    };

    set_status(&entry, TaskStatus::Probing, None);

    // --- Probe (opportunistic HTTP/3, fall back to HTTP/1.1/2) ---
    let url: reqwest::Url = match entry.req.url.parse() {
        Ok(u) => u,
        Err(e) => {
            set_status(
                &entry,
                TaskStatus::Failed,
                Some(format!("invalid URL: {e}")),
            );
            notify_finished(&ectx, &entry, TaskStatus::Failed);
            return HttpRunEnd::Done;
        }
    };
    let mut probe_info: Option<ProbeInfo> = None;
    let mut use_h3 = false;
    if h3_enabled && url.scheme() == "https" {
        if let Ok(Ok(info)) = tokio::time::timeout(
            connect_timeout,
            probe::probe(&ectx.h3, &url, &entry.req.headers, true),
        )
        .await
        {
            if info.protocol == "HTTP/3" {
                probe_info = Some(info);
                use_h3 = true;
            }
        }
    }
    let client = if use_h3 {
        ectx.h3.clone()
    } else {
        ectx.http.clone()
    };
    if probe_info.is_none() {
        match probe::probe(&client, &url, &entry.req.headers, use_h3).await {
            Ok(info) => probe_info = Some(info),
            Err(e) => {
                set_status(&entry, TaskStatus::Failed, Some(e.to_string()));
                notify_finished(&ectx, &entry, TaskStatus::Failed);
                return HttpRunEnd::Done;
            }
        }
    }
    let info = probe_info.expect("probe info");

    {
        let mut m = entry.mutable.lock().expect("task");
        m.size = info.size;
        m.protocol = Some(info.protocol.clone());
        m.resume_supported = info.accept_ranges;
        if m.filename.is_empty() {
            let dir = m.output_dir.clone();
            let candidate = security::sanitize_filename(
                entry
                    .req
                    .filename
                    .as_deref()
                    .or(info.filename_hint.as_deref())
                    .or(Some("download")),
                "download",
            );
            m.filename = dedupe_filename(&dir, &candidate);
        }
    }
    ectx.persist_task(&entry);

    let final_path = {
        let m = entry.mutable.lock().expect("task");
        m.output_dir.join(&m.filename)
    };

    // --- Open partial storage (resume validation) ---
    let expected = Expected {
        url: info.final_url.to_string(),
        size: info.size,
        etag: info.etag.clone(),
        last_modified: info.last_modified.clone(),
    };
    let (storage, outcome) = match PartialFile::open(&final_path, &expected) {
        Ok(v) => v,
        Err(e) => {
            set_status(&entry, TaskStatus::Failed, Some(e.to_string()));
            notify_finished(&ectx, &entry, TaskStatus::Failed);
            return HttpRunEnd::Done;
        }
    };
    let resumed = match outcome {
        crate::storage::OpenOutcome::Resumed(n) => n,
        crate::storage::OpenOutcome::Fresh => 0,
    };
    entry.shared.progress_base.store(resumed, Ordering::Relaxed);
    *entry.shared.storage.lock().expect("storage") = Some(storage.clone());
    entry.shared.net_bytes.store(0, Ordering::Relaxed);

    let size = info.size;
    let range_mode = info.accept_ranges && size.unwrap_or(0) > 0;

    // --- Plan ---
    let mut planned_conns = entry
        .req
        .connections
        .unwrap_or(default_conns)
        .clamp(1, max_conns) as u64;
    if !range_mode {
        planned_conns = 1;
    } else if let Some(sz) = size {
        let by_size = (sz / min_seg).max(1);
        planned_conns = planned_conns.min(by_size);
    }

    if resumed > 0 {
        tracing::info!(
            task = %entry.id,
            "resuming {} of {} bytes",
            resumed,
            size.map(|s| s.to_string()).unwrap_or_else(|| "?".into())
        );
    }

    let lease_hint = if range_mode {
        let remaining: u64 = storage.gaps().iter().map(|(s, e)| e - s).sum();
        (remaining / (planned_conns * 2))
            .clamp(min_seg, 32 * 1024 * 1024)
            .max(min_seg)
    } else {
        u64::MAX / 4
    };
    let gap_queue = Arc::new(GapQueue::new(storage.gaps(), lease_hint));
    if range_mode && gap_queue.is_empty() && storage.is_complete() {
        // Everything already on disk (a resumed task that was actually complete).
        return finish_task(ectx, entry, storage, verify_on_finish).await;
    }

    let task_limiter = entry
        .req
        .speed_limit_bps
        .or(default_task_limit)
        .filter(|r| *r > 0)
        .map(crate::limit::TokenBucket::new)
        .map(Arc::new);

    set_status(&entry, TaskStatus::Downloading, None);

    let workers_total = if range_mode { planned_conns } else { 1 };
    ectx.active_conns
        .fetch_add(workers_total as u32, Ordering::Relaxed);

    let mut join_set: tokio::task::JoinSet<WorkerOut> = tokio::task::JoinSet::new();
    for _ in 0..workers_total {
        let wctx = WorkerCtx {
            shared: entry.shared.clone(),
            client: client.clone(),
            url: info.final_url.to_string(),
            headers: entry.req.headers.clone(),
            etag: info.etag.clone(),
            gap_queue: gap_queue.clone(),
            storage: storage.clone(),
            global_limiter: ectx.global_limiter.clone(),
            task_limiter: task_limiter.clone(),
            budget: ectx.budget.clone(),
            range_mode,
            single_total: if range_mode { None } else { size },
            max_retries,
            backoff_ms,
            h3: use_h3,
        };
        join_set.spawn(run_worker(wctx));
    }

    let mut final_status = TaskStatus::Done;
    let mut error: Option<String> = None;
    let mut single_stream_bytes: Option<u64> = None;

    while let Some(joined) = join_set.join_next().await {
        let out = match joined {
            Ok(o) => o,
            Err(join_err) => WorkerOut::Failed(format!("worker crashed: {join_err}")),
        };
        match out {
            WorkerOut::Idle => {}
            WorkerOut::Paused => {
                final_status = TaskStatus::Paused;
                entry.shared.ctrl.send_replace(Ctrl::Pause);
            }
            WorkerOut::Stopped => {
                final_status = TaskStatus::Cancelled;
                entry.shared.ctrl.send_replace(Ctrl::Stop);
            }
            WorkerOut::Failed(msg) => {
                final_status = TaskStatus::Failed;
                error = Some(msg);
                entry.shared.ctrl.send_replace(Ctrl::Stop);
            }
            WorkerOut::ServerChanged(reason) => {
                ectx.active_conns
                    .fetch_sub(workers_total as u32, Ordering::Relaxed);
                return HttpRunEnd::Restart(reason);
            }
            WorkerOut::SingleStreamDone(n) => {
                single_stream_bytes = Some(n);
            }
        }
    }
    ectx.active_conns
        .fetch_sub(workers_total as u32, Ordering::Relaxed);

    match final_status {
        TaskStatus::Paused => {
            set_status(&entry, TaskStatus::Paused, None);
            ectx.persist_task(&entry);
            return HttpRunEnd::Done;
        }
        TaskStatus::Cancelled => {
            set_status(&entry, TaskStatus::Cancelled, None);
            notify_finished(&ectx, &entry, TaskStatus::Cancelled);
            return HttpRunEnd::Done;
        }
        TaskStatus::Failed => {
            set_status(&entry, TaskStatus::Failed, error);
            notify_finished(&ectx, &entry, TaskStatus::Failed);
            return HttpRunEnd::Done;
        }
        _ => {}
    }

    // Completion check.
    let complete = if let Some(n) = single_stream_bytes {
        n > 0 && storage.bytes_done() >= n
    } else {
        storage.is_complete()
            || (range_mode && gap_queue.is_empty() && storage.bytes_done() >= size.unwrap_or(0))
    };
    if !complete {
        set_status(
            &entry,
            TaskStatus::Failed,
            Some("download ended before completion".into()),
        );
        notify_finished(&ectx, &entry, TaskStatus::Failed);
        return HttpRunEnd::Done;
    }
    finish_task(ectx, entry, storage, verify_on_finish).await
}

async fn finish_task(
    ectx: Arc<EngineCtx>,
    entry: Arc<TaskEntry>,
    storage: Arc<PartialFile>,
    verify_on_finish: bool,
) -> HttpRunEnd {
    // SHA-256 verification (streamed, memory-light).
    let expected = entry
        .req
        .checksum
        .as_deref()
        .and_then(crate::format::normalize_sha256);
    let should_hash = verify_on_finish || expected.is_some();
    if should_hash {
        set_status(&entry, TaskStatus::Verifying, None);
        let actual = match tokio::task::spawn_blocking({
            let storage = storage.clone();
            move || storage.compute_sha256()
        })
        .await
        {
            Ok(Ok(h)) => Some(h),
            Ok(Err(e)) => {
                set_status(&entry, TaskStatus::Failed, Some(e.to_string()));
                notify_finished(&ectx, &entry, TaskStatus::Failed);
                return HttpRunEnd::Done;
            }
            Err(e) => {
                set_status(
                    &entry,
                    TaskStatus::Failed,
                    Some(format!("hash worker failed: {e}")),
                );
                notify_finished(&ectx, &entry, TaskStatus::Failed);
                return HttpRunEnd::Done;
            }
        };
        let mut m = entry.mutable.lock().expect("task");
        m.actual_sha256 = actual.clone();
        m.expected_sha256 = expected.clone();
        match (&expected, &actual) {
            (Some(exp), Some(act)) if exp != act => {
                m.checksum_ok = Some(false);
                drop(m);
                let msg = format!("checksum mismatch: expected {exp}, computed {act}");
                set_status(&entry, TaskStatus::Failed, Some(msg));
                notify_finished(&ectx, &entry, TaskStatus::Failed);
                return HttpRunEnd::Done;
            }
            (_, Some(_)) => {
                // Hash computed (and matches when an expectation existed).
                m.checksum_ok = Some(true);
            }
            _ => {}
        }
    }

    if let Err(e) = storage.finalize() {
        set_status(&entry, TaskStatus::Failed, Some(e.to_string()));
        notify_finished(&ectx, &entry, TaskStatus::Failed);
        return HttpRunEnd::Done;
    }
    *entry.shared.storage.lock().expect("storage") = Some(storage);

    set_status(&entry, TaskStatus::Done, None);
    notify_finished(&ectx, &entry, TaskStatus::Done);
    HttpRunEnd::Done
}

pub(crate) fn set_status(entry: &Arc<TaskEntry>, status: TaskStatus, error: Option<String>) {
    let mut m = entry.mutable.lock().expect("task");
    m.status = status;
    m.error = error;
    if status.is_terminal() {
        m.finished_at = Some(crate::store::unix_now());
    }
}

pub(crate) fn notify_finished(ectx: &Arc<EngineCtx>, entry: &Arc<TaskEntry>, status: TaskStatus) {
    ectx.persist_task(entry);
    let _ = ectx.events.send(crate::engine::EngineEvent::TaskFinished(
        entry.id, status, None,
    ));
}

/// Snapshot helper: build the segment list for a task.
pub(crate) fn segment_snaps(entry: &Arc<TaskEntry>) -> Vec<SegmentSnap> {
    let segs = entry.shared.segs.lock().expect("segs");
    segs.iter()
        .map(|s| SegmentSnap {
            start: s.start.load(Ordering::Relaxed),
            end: s.end.load(Ordering::Relaxed).min(u64::MAX / 2),
            done: s.done.load(Ordering::Relaxed),
            speed_bps: s.speed_bps.load(Ordering::Relaxed),
            state: s.seg_state(),
            retries: s.retries.load(Ordering::Relaxed),
        })
        .collect()
}

/// Speed sampling for segments, called from the engine ticker.
pub(crate) fn sample_seg_speeds(entry: &Arc<TaskEntry>, dt_secs: f64) {
    let segs = entry.shared.segs.lock().expect("segs");
    for s in segs.iter() {
        let done = s.done.load(Ordering::Relaxed);
        let mut last = s.last.lock().expect("seg last");
        let delta = done.saturating_sub(last.1);
        let speed = if dt_secs > 0.0 {
            (delta as f64 / dt_secs) as u64
        } else {
            0
        };
        s.speed_bps.store(speed, Ordering::Relaxed);
        *last = (Instant::now(), done);
    }
}

impl TaskShared {
    pub(crate) fn register_seg(&self, stat: Arc<SegStat>) {
        self.segs.lock().expect("segs").push(stat);
    }

    pub(crate) fn unregister_seg(&self, stat: &Arc<SegStat>) {
        self.segs
            .lock()
            .expect("segs")
            .retain(|s| !Arc::ptr_eq(s, stat));
    }
}

/// History bookkeeping used by the ticker.
pub(crate) fn push_history(history: &mut VecDeque<(i64, u64)>, now_ms: i64, speed: u64) {
    history.push_back((now_ms, speed));
    while history.len() > 240 {
        history.pop_front();
    }
}
