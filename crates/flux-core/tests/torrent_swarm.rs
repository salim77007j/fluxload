//! Hybrid BitTorrent: a real local swarm.
//!
//! 1. A seed librqbit session shares a generated file, announcing to the
//!    test server's HTTP tracker.
//! 2. The Fluxload engine receives the magnet link (tracker embedded).
//! 3. The engine joins the swarm, downloads from the seeder over the real
//!    BitTorrent peer protocol, and completes with byte-exact integrity.
//!
//! Requires the `torrent` feature (default) and the test server tracker.

#![cfg(feature = "torrent")]

use flux_core::config::EngineConfig;
use flux_core::engine::{Engine, EngineEvent};
use flux_core::task::{AddRequest, TaskKind, TaskStatus};
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("flux-tt-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
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

fn hex20(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magnet_download_from_local_swarm() {
    // --- Seed material -----------------------------------------------------
    let root = temp_dir("seed-src");
    let payload_path = root.join("payload.bin");
    let size = 8 * 1024 * 1024;
    flux_testsrv::generate_file(&payload_path, size).unwrap();
    let expected = sha256_of(&payload_path);

    // --- Tracker (test server) ---------------------------------------------
    let tracker_root = temp_dir("tracker");
    let server = flux_testsrv::start(tracker_root).await.unwrap();
    let tracker_url = format!("http://{}/announce", server.addr);

    // --- Seeder: librqbit session that already has the file ----------------
    let seed_dir = temp_dir("seed-out");
    std::fs::copy(&payload_path, seed_dir.join("payload.bin")).unwrap();

    let seed_session = librqbit::Session::new_with_opts(
        seed_dir.clone(),
        librqbit::SessionOptions {
            persistence: None,
            listen: Some(librqbit::ListenerOptions {
                listen_addr: "0.0.0.0:0".parse().unwrap(),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .expect("seed session");

    // Create the torrent metadata pointing at our tracker, then add it to the
    // seed session (files already complete -> it verifies and seeds).
    let create_result = librqbit::create_torrent(
        &payload_path,
        librqbit::CreateTorrentOptions {
            name: Some("payload.bin"),
            trackers: vec![tracker_url.clone()],
            piece_length: Some(256 * 1024),
        },
        &librqbit::spawn_utils::BlockingSpawner::new(2),
    )
    .await
    .expect("create torrent");

    let info_hash_hex = create_result.info_hash().as_string();
    let torrent_bytes = create_result.as_bytes().expect("serialize torrent");

    let add = seed_session
        .add_torrent(
            librqbit::AddTorrent::from_bytes(torrent_bytes.to_vec()),
            Some(librqbit::AddTorrentOptions {
                output_folder: Some(seed_dir.to_string_lossy().to_string()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .expect("seed add torrent");
    let _seed_handle = add.into_handle().expect("seed handle");
    tracing::info!("seeder ready: info hash {info_hash_hex}");

    // Give the seeder a moment to verify + announce.
    tokio::time::sleep(Duration::from_secs(2));

    // --- Engine (leecher) receives the magnet -------------------------------
    let dir = temp_dir("leech");
    let dl = dir.join("downloads");
    std::fs::create_dir_all(&dl).unwrap();
    let mut cfg = EngineConfig::default();
    cfg.data_dir = dir.join("data");
    cfg.download_dir = dl.clone();
    cfg.tls_insecure = true;
    cfg.torrent_enabled = true;
    let engine = Engine::start(cfg);
    let events = engine.events();

    let magnet = format!("magnet:?xt=urn:btih:{info_hash_hex}&dn=payload.bin&tr={tracker_url}");
    let id = engine
        .add(AddRequest {
            url: magnet,
            output_dir: Some(dl.clone()),
            ..AddRequest::new(String::new())
        })
        .expect("add magnet");

    // Wait for completion (real peer transfer).
    let start = Instant::now();
    let mut final_snap = None;
    'wait: loop {
        assert!(
            start.elapsed() < Duration::from_secs(240),
            "swarm transfer did not finish"
        );
        match events.recv_timeout(Duration::from_millis(500)) {
            Ok(EngineEvent::TaskFinished(finished, status, _))
                if finished == id && status.is_terminal() =>
            {
                let deadline = Instant::now() + Duration::from_secs(10);
                while Instant::now() < deadline {
                    if let Some(t) = engine.snapshot().tasks.iter().find(|t| t.id == id).cloned() {
                        if t.status.is_terminal() {
                            final_snap = Some(t);
                            break 'wait;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!("engine died"),
        }
    }
    engine.shutdown();

    let snap = final_snap.expect("snapshot");
    assert_eq!(snap.kind, TaskKind::Torrent);
    assert_eq!(
        snap.status,
        TaskStatus::Done,
        "torrent failed: {:?}",
        snap.error
    );
    let out = dl.join("payload.bin");
    assert!(out.exists(), "torrent output exists");
    assert_eq!(
        sha256_of(&out),
        expected,
        "swarm-downloaded content integrity"
    );
    seed_session.cancellation_token().clone().cancel();
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&seed_dir);
}
