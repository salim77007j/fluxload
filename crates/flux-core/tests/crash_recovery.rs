//! Power-loss crash recovery: the download process is SIGKILLed mid-transfer,
//! then a fresh engine resumes from the durable partial state and must finish
//! with byte-exact integrity.

use flux_core::config::EngineConfig;
use flux_core::engine::{Engine, EngineEvent};
use flux_core::task::{AddRequest, TaskStatus};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "FLUX_CRASH_CHILD_DIR";

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("flux-it-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
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

/// Child mode: run a throttled download that never finishes on its own within
/// the kill window. Invoked as `current_exe --exact crash_child_download`.
#[test]
fn crash_child_download() {
    let Ok(data_root) = std::env::var(CHILD_ENV) else {
        return; // normal test runs skip this immediately
    };
    let dir = PathBuf::from(&data_root);
    let dl = dir.join("downloads");
    std::fs::create_dir_all(&dl).unwrap();

    let server_url = std::env::var("FLUX_CRASH_SERVER").expect("server url");
    let mut cfg = EngineConfig::default();
    cfg.data_dir = dir.join("data");
    cfg.download_dir = dl.clone();
    cfg.tls_insecure = true;
    let engine = Engine::start(cfg);
    let events = engine.events();

    let mut req = default_req(server_url);
    req.connections = Some(4);
    let _id = engine.add(req).expect("add");
    // Block until the task finishes or the parent kills us (SIGKILL).
    loop {
        if let Ok(EngineEvent::TaskFinished(_, status, _)) =
            events.recv_timeout(Duration::from_millis(200))
        {
            if status.is_terminal() {
                break;
            }
        }
    }
    std::process::exit(0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kill9_mid_download_then_resume_with_integrity() {
    let root = temp_dir("srv-crash");
    // 64 MB at ~1 MB/s aggregate (4 conns x 256 KB/s): long enough to kill mid-flight.
    flux_testsrv::generate_file(&root.join("crash.bin"), 64 * 1024 * 1024).unwrap();
    let expected = {
        use sha2::Digest;
        let mut f = std::fs::File::open(root.join("crash.bin")).unwrap();
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
        out.iter().map(|b| format!("{b:02x}")).collect::<String>()
    };
    let server = flux_testsrv::start(root).await.unwrap();

    let dir = temp_dir("crash");
    let dl = dir.join("downloads");
    std::fs::create_dir_all(&dl).unwrap();

    // Spawn the child (self-exec) which downloads the throttled file.
    let exe = std::env::current_exe().expect("current exe");
    let mut child = Command::new(exe)
        .arg("--exact")
        .arg("crash_child_download")
        .arg("--nocapture")
        .env(CHILD_ENV, &dir)
        .env(
            "FLUX_CRASH_SERVER",
            format!("http://{}/throttle/256/crash.bin", server.addr),
        )
        .spawn()
        .expect("spawn crash child");

    // Wait for durable partial progress on disk, then SIGKILL.
    let part = dl.join("crash.bin.fluxpart");
    let meta = dl.join("crash.bin.fluxpart.json");
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut durable = 0u64;
    loop {
        assert!(Instant::now() < deadline, "child never wrote partial data");
        if meta.exists() {
            if let Ok(text) = std::fs::read_to_string(&meta) {
                if let Ok(m) = serde_json::from_str::<serde_json::Value>(&text) {
                    let ranges = m["done_ranges"].as_array().cloned().unwrap_or_default();
                    let sum: u64 = ranges
                        .iter()
                        .filter_map(|r| {
                            let s = r[0].as_u64()?;
                            let e = r[1].as_u64()?;
                            Some(e - s)
                        })
                        .sum();
                    if sum > 2 * 1024 * 1024 {
                        durable = sum;
                        break;
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    // Hard kill: no graceful shutdown, no cleanup, no meta flush beyond what
    // was already fsynced. This simulates power loss.
    child.kill().expect("SIGKILL child");
    let _ = child.wait();
    assert!(part.exists(), "partial file must survive the kill");

    // Recover the task id the child persisted.
    let import_id: uuid::Uuid = {
        let text = std::fs::read_to_string(dir.join("data/tasks.json")).expect("child tasks.json");
        let tasks: serde_json::Value = serde_json::from_str(&text).expect("parse tasks.json");
        uuid::Uuid::parse_str(tasks[0]["id"].as_str().expect("task id in store")).expect("uuid")
    };

    // A brand-new engine (fresh process state) resumes and must complete.
    let mut cfg = EngineConfig::default();
    cfg.data_dir = dir.join("data");
    cfg.download_dir = dl.clone();
    cfg.tls_insecure = true;
    let engine = Engine::start(cfg);
    let events = engine.events();

    // Wait for the imported task to be picked up and finished.
    let start = Instant::now();
    let mut final_snap = None;
    'wait: loop {
        assert!(
            start.elapsed() < Duration::from_secs(180),
            "resume did not finish"
        );
        match events.recv_timeout(Duration::from_millis(500)) {
            Ok(EngineEvent::TaskFinished(task_id, status, _)) => {
                if task_id == import_id && status.is_terminal() {
                    // Wait for the published snapshot to agree (bounded).
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while Instant::now() < deadline {
                        if let Some(t) = engine
                            .snapshot()
                            .tasks
                            .iter()
                            .find(|t| t.id == task_id)
                            .cloned()
                        {
                            if t.status.is_terminal() {
                                final_snap = Some(t);
                                break 'wait;
                            }
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            }
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!("engine died"),
        }
    }
    engine.shutdown();

    let snap = final_snap.unwrap();
    assert_eq!(snap.status, TaskStatus::Done, "error: {:?}", snap.error);
    assert!(
        snap.downloaded > durable,
        "resume must build on durable bytes: {} > {}",
        snap.downloaded,
        durable
    );
    let out = dl.join("crash.bin");
    assert!(out.exists(), "final file exists after resume");

    // The proof: byte-exact content after an unclean kill.
    use sha2::Digest;
    let mut f = std::fs::File::open(&out).unwrap();
    let mut h = sha2::Sha256::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = std::io::Read::read(&mut f, &mut buf).unwrap();
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    let actual: [u8; 32] = h.finalize().into();
    let actual_hex: String = actual.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        actual_hex, expected,
        "CRASH RECOVERY INTEGRITY: content must be byte-exact"
    );
    assert_eq!(snap.actual_sha256.as_deref(), Some(expected.as_str()));
    let _ = std::fs::remove_dir_all(&dir);
}
