//! Real benchmarking: single- vs multi-connection transfers through the full
//! engine (probe, segmentation, storage, integrity), measuring wall-clock time.

use crate::engine::{Engine, EngineEvent};
use crate::errors::{FluxError, Result};
use crate::task::{AddRequest, TaskSnapshot, TaskStatus};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, serde::Serialize)]
pub struct BenchRow {
    pub connections: u32,
    pub wall_ms: u128,
    pub bytes: u64,
    pub throughput_bps: u64,
    pub protocol: Option<String>,
    pub resumed_bytes: u64,
    pub sha256: Option<String>,
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct BenchReport {
    pub url: String,
    pub rows: Vec<BenchRow>,
}

/// Run the benchmark for each connection count. Each run downloads the file
/// into a fresh temp directory through a fresh engine instance.
pub fn run_bench(url: &str, connections: &[u32], per_run_timeout: Duration) -> Result<BenchReport> {
    let mut rows = Vec::new();
    for &n in connections {
        let dir =
            std::env::temp_dir().join(format!("flux-bench-{}-c{n}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).map_err(|e| FluxError::Disk(e.to_string()))?;
        let row = bench_once(url, n, &dir, per_run_timeout);
        let _ = std::fs::remove_dir_all(&dir);
        rows.push(row);
    }
    Ok(BenchReport {
        url: url.to_string(),
        rows,
    })
}

#[allow(clippy::field_reassign_with_default)] // staged config setup reads top-down
fn bench_once(url: &str, n: u32, dir: &std::path::Path, timeout: Duration) -> BenchRow {
    let mut cfg = crate::config::EngineConfig::default();
    cfg.data_dir = dir.join("data");
    cfg.download_dir = dir.to_path_buf();
    // Full speed benchmark: no artificial limits.
    cfg.global_speed_limit_bps = None;
    cfg.per_task_limit_default_bps = None;
    cfg.max_active_tasks = 1;
    cfg.auto_resume_on_start = false;

    let engine = Engine::start(cfg);
    let events = engine.events();
    let id = match engine.add(AddRequest {
        url: url.to_string(),
        output_dir: Some(dir.to_path_buf()),
        filename: None,
        connections: Some(n),
        speed_limit_bps: None,
        checksum: None,
        headers: vec![],
        schedule_at_unix: None,
        auto_start: true,
        origin: Some("bench".into()),
    }) {
        Ok(id) => id,
        Err(e) => {
            engine.shutdown();
            return BenchRow {
                connections: n,
                wall_ms: 0,
                bytes: 0,
                throughput_bps: 0,
                protocol: None,
                resumed_bytes: 0,
                sha256: None,
                ok: false,
                error: Some(e.to_string()),
            };
        }
    };

    let start = Instant::now();
    let mut final_snapshot: Option<TaskSnapshot> = None;
    let deadline = std::time::SystemTime::now() + timeout;
    'wait: loop {
        if start.elapsed() > timeout {
            break;
        }
        let remaining = deadline
            .duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::from_millis(1));
        match events.recv_timeout(remaining) {
            Ok(EngineEvent::TaskFinished(finished_id, status, _msg)) if finished_id == id => {
                let snap = engine.snapshot();
                let ts = snap.tasks.iter().find(|t| t.id == id).cloned();
                if status == TaskStatus::Done {
                    final_snapshot = ts;
                    break 'wait;
                }
                let err = ts
                    .as_ref()
                    .and_then(|t| t.error.clone())
                    .unwrap_or_else(|| format!("task ended with status {}", status.label()));
                engine.shutdown();
                return BenchRow {
                    connections: n,
                    wall_ms: start.elapsed().as_millis(),
                    bytes: ts.as_ref().map(|t| t.downloaded).unwrap_or(0),
                    throughput_bps: 0,
                    protocol: ts.as_ref().and_then(|t| t.protocol.clone()),
                    resumed_bytes: 0,
                    sha256: None,
                    ok: false,
                    error: Some(err),
                };
            }
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    engine.shutdown();

    let elapsed = start.elapsed();
    let snap =
        final_snapshot.or_else(|| engine.snapshot().tasks.iter().find(|t| t.id == id).cloned());
    let bytes = snap.as_ref().map(|t| t.downloaded).unwrap_or(0);
    let throughput = if elapsed.as_millis() > 0 {
        (bytes as u128 * 1000 / elapsed.as_millis()) as u64
    } else {
        0
    };
    BenchRow {
        connections: n,
        wall_ms: elapsed.as_millis(),
        bytes,
        throughput_bps: throughput,
        protocol: snap.as_ref().and_then(|t| t.protocol.clone()),
        resumed_bytes: 0,
        sha256: snap.as_ref().and_then(|t| t.actual_sha256.clone()),
        ok: bytes > 0,
        error: if bytes == 0 {
            Some("no data downloaded (timeout?)".into())
        } else {
            None
        },
    }
}
