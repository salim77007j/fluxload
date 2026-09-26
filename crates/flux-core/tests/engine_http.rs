//! Engine integration tests against the local test server.
//! Every assertion exercises the real engine: probing, segmentation,
//! crash-safe storage, throttling, limits, scheduling and integrity.

use flux_core::config::EngineConfig;
use flux_core::engine::{Engine, EngineEvent};
use flux_core::task::{AddRequest, TaskStatus};
use std::path::PathBuf;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("flux-it-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn make_config(tag: &str, tls_insecure: bool) -> (EngineConfig, PathBuf, PathBuf) {
    let dir = temp_dir(tag);
    let dl = dir.join("downloads");
    std::fs::create_dir_all(&dl).unwrap();
    let mut cfg = EngineConfig::default();
    cfg.data_dir = dir.join("data");
    cfg.download_dir = dl.clone();
    cfg.tls_insecure = tls_insecure;
    cfg.max_active_tasks = 3;
    cfg.default_connections = 8;
    (cfg, dir, dl)
}

/// Wait until the task reaches a terminal state; returns its final snapshot.
fn wait_terminal(
    engine: &Engine,
    events: &mut Receiver<EngineEvent>,
    id: flux_core::task::TaskId,
    timeout: Duration,
) -> flux_core::task::TaskSnapshot {
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < timeout,
            "task did not finish within {timeout:?}"
        );
        match events.recv_timeout(Duration::from_millis(500)) {
            Ok(EngineEvent::TaskFinished(finished, status, _))
                if finished == id && status.is_terminal() =>
            {
                // The event can race the next snapshot publish; wait for the
                // published state to agree (bounded).
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline {
                    if let Some(t) = engine.snapshot().tasks.iter().find(|t| t.id == id).cloned() {
                        if t.status.is_terminal() {
                            return t;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                return engine
                    .snapshot()
                    .tasks
                    .iter()
                    .find(|t| t.id == id)
                    .cloned()
                    .expect("task snapshot");
            }
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                panic!("engine event channel closed");
            }
        }
    }
}

fn sha256_of(path: &std::path::Path) -> String {
    use sha2::Digest;
    let mut f = std::fs::File::open(path).unwrap();
    let mut h = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf).unwrap();
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    let out: [u8; 32] = h.finalize().into();
    out.iter().map(|b| format!("{b:02x}")).collect()
}

fn add_and_wait(
    engine: &Engine,
    events: &mut Receiver<EngineEvent>,
    req: AddRequest,
    timeout: Duration,
) -> flux_core::task::TaskSnapshot {
    let id = engine.add(req).expect("add task");
    wait_terminal(engine, events, id, timeout)
}

fn default_req(url: String) -> AddRequest {
    AddRequest {
        url,
        output_dir: None,
        filename: None,
        connections: None,
        speed_limit_bps: None,
        checksum: None,
        headers: vec![],
        schedule_at_unix: None,
        auto_start: true,
        origin: None,
    }
}

// ---------------------------------------------------------------------------
// Basic segmented download + integrity
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn download_small_file_multisegment() {
    let root = temp_dir("srv-small");
    let hash = flux_testsrv::generate_file(&root.join("small.bin"), 4 * 1024 * 1024).unwrap();
    let server = flux_testsrv::start(root).await.unwrap();
    let (cfg, dir, dl) = make_config("small", true);
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req(format!("http://{}/files/small.bin", server.addr));
    req.connections = Some(8);
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(60));
    engine.shutdown();

    assert_eq!(
        snap.status,
        TaskStatus::Done,
        "status was {:?}",
        snap.status
    );
    assert_eq!(snap.size, Some(4 * 1024 * 1024));
    assert!(snap.resume_supported, "test server must advertise ranges");
    let out = dl.join("small.bin");
    assert!(out.exists(), "final file must exist");
    assert_eq!(sha256_of(&out), hash, "content integrity");
    assert_eq!(snap.actual_sha256.as_deref(), Some(hash.as_str()));
    assert_eq!(snap.checksum_ok, Some(true));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Pause / resume
// ---------------------------------------------------------------------------

fn wait_for_status(
    engine: &Engine,
    id: flux_core::task::TaskId,
    status: TaskStatus,
    timeout: Duration,
) -> flux_core::task::TaskSnapshot {
    let start = Instant::now();
    loop {
        assert!(start.elapsed() < timeout, "never reached {status:?}");
        let snap = engine.snapshot();
        let t = snap.tasks.iter().find(|t| t.id == id).unwrap().clone();
        if t.status == status {
            return t;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_and_resume_completes_with_integrity() {
    let root = temp_dir("srv-pause");
    // 24 MB behind a per-connection 4 MB/s throttle: pause has time to hit.
    flux_testsrv::generate_file(&root.join("pause.bin"), 24 * 1024 * 1024).unwrap();
    let hash = sha256_of(&root.join("pause.bin"));
    let server = flux_testsrv::start(root).await.unwrap();
    let (cfg, dir, dl) = make_config("pause", true);
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req(format!("http://{}/throttle/4096/pause.bin", server.addr));
    req.connections = Some(4);
    let id = engine.add(req).unwrap();

    // Wait until it is actively downloading with progress.
    let start = Instant::now();
    loop {
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "never started downloading"
        );
        let snap = engine.snapshot();
        let t = match snap.tasks.iter().find(|t| t.id == id) {
            Some(t) => t.clone(),
            None => {
                eprintln!("PAUSEDBG missing from snapshot, tasks={}", snap.tasks.len());
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        eprintln!(
            "PAUSEDBG {:?}",
            (t.status, t.downloaded, t.active_conns, &t.error)
        );
        if t.status == TaskStatus::Downloading && t.downloaded > 2 * 1024 * 1024 {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    engine.pause(id).unwrap();
    let paused = wait_for_status(&engine, id, TaskStatus::Paused, Duration::from_secs(20));
    assert!(paused.downloaded < 24 * 1024 * 1024, "paused mid-flight");
    assert!(paused.downloaded > 0, "made progress before pause");
    let durable_before = paused.downloaded;

    std::thread::sleep(Duration::from_millis(300));
    engine.resume(id).unwrap();
    let snap = wait_terminal(&engine, &mut events, id, Duration::from_secs(120));
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Done);
    assert!(
        snap.downloaded >= durable_before,
        "resume never loses durable bytes"
    );
    let out = dl.join("pause.bin");
    assert_eq!(sha256_of(&out), hash, "post-resume integrity");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Multi-connection speedup on a throttled server (the IDM reason to exist)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_connection_speedup_on_throttled_server() {
    let root = temp_dir("srv-speed");
    flux_testsrv::generate_file(&root.join("speed.bin"), 16 * 1024 * 1024).unwrap();
    let hash = sha256_of(&root.join("speed.bin"));
    let server = flux_testsrv::start(root).await.unwrap();

    // Single connection: 1 MB/s per conn -> ~16 s.
    let (cfg1, dir1, _dl) = make_config("speed-1", true);
    let engine1 = Engine::start(cfg1);
    let mut ev1 = engine1.events();
    let t0 = Instant::now();
    let mut req = default_req(format!("http://{}/throttle/1024/speed.bin", server.addr));
    req.connections = Some(1);
    let snap1 = add_and_wait(&engine1, &mut ev1, req, Duration::from_secs(90));
    let single_elapsed = t0.elapsed();
    engine1.shutdown();

    // 8 connections over the same per-connection throttle -> ~8x.
    let (cfg8, dir8, dl8) = make_config("speed-8", true);
    let engine8 = Engine::start(cfg8);
    let mut ev8 = engine8.events();
    let t1 = Instant::now();
    let mut req8 = default_req(format!("http://{}/throttle/1024/speed.bin", server.addr));
    req8.connections = Some(8);
    let snap8 = add_and_wait(&engine8, &mut ev8, req8, Duration::from_secs(90));
    let multi_elapsed = t1.elapsed();
    engine8.shutdown();

    assert_eq!(snap1.status, TaskStatus::Done);
    assert_eq!(snap8.status, TaskStatus::Done);
    assert!(
        multi_elapsed.as_secs_f64() < single_elapsed.as_secs_f64() * 0.5,
        "8 connections ({multi_elapsed:?}) should be at least 2x faster than 1 ({single_elapsed:?})"
    );
    assert!(
        single_elapsed.as_secs_f64() > 5.0,
        "throttle must actually bite (single: {single_elapsed:?})"
    );
    let out = dl8.join("speed.bin");
    assert_eq!(sha256_of(&out), hash);
    let _ = std::fs::remove_dir_all(dir1);
    let _ = std::fs::remove_dir_all(dir8);
}

// ---------------------------------------------------------------------------
// Global speed limit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn global_speed_limit_is_enforced() {
    let root = temp_dir("srv-limit");
    flux_testsrv::generate_file(&root.join("limit.bin"), 8 * 1024 * 1024).unwrap();
    let server = flux_testsrv::start(root).await.unwrap();
    let (mut cfg, dir, _dl) = make_config("limit", true);
    cfg.global_speed_limit_bps = Some(2 * 1024 * 1024); // 2 MB/s
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let t0 = Instant::now();
    let mut req = default_req(format!("http://{}/files/limit.bin", server.addr));
    req.connections = Some(8);
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(60));
    let elapsed = t0.elapsed();
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Done);
    assert!(
        elapsed >= Duration::from_secs(3),
        "8 MB at 2 MB/s must take at least ~4 s (took {elapsed:?})"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Checksum mismatch detection
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checksum_mismatch_is_detected() {
    let root = temp_dir("srv-csum");
    flux_testsrv::generate_file(&root.join("csum.bin"), 2 * 1024 * 1024).unwrap();
    let server = flux_testsrv::start(root).await.unwrap();
    let (cfg, dir, _dl) = make_config("csum", true);
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req(format!("http://{}/files/csum.bin", server.addr));
    req.checksum = Some(format!("sha256:{}", "0".repeat(64)));
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(60));
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Failed);
    let err = snap.error.unwrap_or_default();
    assert!(
        err.contains("checksum"),
        "error should mention checksum: {err}"
    );
    assert_eq!(snap.checksum_ok, Some(false));
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Single-stream mode (server without range support)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_range_server_downloads_single_stream() {
    let root = temp_dir("srv-norange");
    let hash = flux_testsrv::generate_file(&root.join("nr.bin"), 4 * 1024 * 1024).unwrap();
    let server = flux_testsrv::start(root).await.unwrap();
    let (cfg, dir, dl) = make_config("norange", true);
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req(format!("http://{}/norange/nr.bin", server.addr));
    req.connections = Some(8); // server cannot honor this
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(60));
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Done, "err={:?}", snap.error);
    assert!(!snap.resume_supported, "no-range server cannot resume");
    let out = dl.join("nr.bin");
    assert_eq!(sha256_of(&out), hash);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Queue scheduling: max_active_tasks respected
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_respects_max_active_tasks() {
    let root = temp_dir("srv-queue");
    for i in 0..4 {
        flux_testsrv::generate_file(&root.join(format!("q{i}.bin")), 6 * 1024 * 1024).unwrap();
    }
    let server = flux_testsrv::start(root).await.unwrap();
    let (mut cfg, dir, _dl) = make_config("queue", true);
    cfg.max_active_tasks = 2;
    let engine = Engine::start(cfg);
    let mut _events = engine.events();

    for i in 0..4 {
        let mut req = default_req(format!("http://{}/throttle/4096/q{i}.bin", server.addr));
        req.connections = Some(2);
        engine.add(req).unwrap();
    }

    // Sample engine state for a few seconds: never more than 2 active.
    let mut violations = 0;
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        let snap = engine.snapshot();
        let active = snap.tasks.iter().filter(|t| t.status.is_active()).count();
        if active > 2 {
            violations += 1;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    engine.shutdown();
    assert_eq!(violations, 0, "max_active_tasks was exceeded");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// HTTP/3 over local QUIC
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http3_download_over_local_quic() {
    let root = temp_dir("srv-h3");
    let hash = flux_testsrv::generate_file(&root.join("h3.bin"), 8 * 1024 * 1024).unwrap();
    let state = flux_testsrv::ServerState {
        root: root.clone(),
        etags: std::sync::Arc::new(std::collections::HashMap::new()),
        sizes: std::sync::Arc::new(std::collections::HashMap::new()),
        tracker: Default::default(),
    };
    let h3 = flux_testsrv::h3_server::start(state).await.unwrap();

    let (mut cfg, dir, dl) = make_config("h3", true);
    cfg.tls_insecure = true; // self-signed test certificate
    cfg.http3_enabled = true;
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req(format!("https://localhost:{}/files/h3.bin", h3.addr.port()));
    req.connections = Some(8);
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(90));
    engine.shutdown();

    assert_eq!(
        snap.status,
        TaskStatus::Done,
        "h3 download failed: {:?}",
        snap.error
    );
    assert_eq!(
        snap.protocol.as_deref(),
        Some("HTTP/3"),
        "expected HTTP/3 negotiation, got {:?} (QUIC may be blocked in this environment)",
        snap.protocol
    );
    let out = dl.join("h3.bin");
    assert_eq!(sha256_of(&out), hash, "HTTP/3 content integrity");
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// Real-world HTTPS download (github.com) — network permitting
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_https_download() {
    // Skip gracefully when GitHub is unreachable (offline environments).
    use std::net::ToSocketAddrs;
    let reachable = ("github.com", 443)
        .to_socket_addrs()
        .map(|mut a| a.next().is_some())
        .unwrap_or(false);
    if !reachable || std::env::var("FLUX_SKIP_NET_TESTS").is_ok() {
        eprintln!("skipping real_https_download: github.com unreachable");
        return;
    }

    let (mut cfg, dir, dl) = make_config("real-https", false);
    cfg.tls_insecure = false; // REAL certificate validation
    let engine = Engine::start(cfg);
    let mut events = engine.events();

    let mut req = default_req("https://github.com/robots.txt".into());
    req.connections = Some(4);
    let snap = add_and_wait(&engine, &mut events, req, Duration::from_secs(60));
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Done, "error: {:?}", snap.error);
    let out = dl.join("robots.txt");
    let size = std::fs::metadata(&out).map(|m| m.len()).unwrap_or(0);
    assert!(
        size > 100,
        "robots.txt should have real content, got {size} bytes"
    );
    assert!(snap.protocol.is_some(), "protocol must be reported");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// External task import (the browser native-messaging bridge path)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_tasks_are_imported_from_store() {
    let root = temp_dir("srv-bridge");
    let hash = flux_testsrv::generate_file(&root.join("bridge.bin"), 2 * 1024 * 1024).unwrap();
    let server = flux_testsrv::start(root).await.unwrap();
    let (cfg, dir, dl) = make_config("bridge", true);

    // The engine starts first...
    let engine = Engine::start(cfg);
    std::thread::sleep(Duration::from_millis(300));

    // ...then the "browser bridge" process writes a task into tasks.json.
    let id = uuid::Uuid::new_v4();
    let stored = vec![flux_core::task::StoredTask {
        id,
        url: format!("http://{}/files/bridge.bin", server.addr),
        filename: String::new(),
        output_dir: dl.clone(),
        kind: flux_core::task::TaskKind::Http,
        status: TaskStatus::Queued,
        size: None,
        connections: None,
        speed_limit_bps: None,
        checksum: None,
        headers: vec![],
        schedule_at_unix: None,
        created_at: 1,
        finished_at: None,
        error: None,
        actual_sha256: None,
        checksum_ok: None,
        origin: Some("browser-extension".into()),
    }];
    std::fs::create_dir_all(dir.join("data")).unwrap();
    std::fs::write(
        dir.join("data/tasks.json"),
        serde_json::to_vec_pretty(&stored).unwrap(),
    )
    .unwrap();

    // The engine picks it up within ~2 s and completes it.
    let mut events = engine.events();
    let snap = wait_terminal(&engine, &mut events, id, Duration::from_secs(60));
    engine.shutdown();

    assert_eq!(snap.status, TaskStatus::Done, "error: {:?}", snap.error);
    assert_eq!(sha256_of(&dl.join("bridge.bin")), hash);
    let _ = std::fs::remove_dir_all(&dir);
}
