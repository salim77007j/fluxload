//! Hybrid BitTorrent transfers (librqbit integration).
//!
//! Magnet links and .torrent URLs join the same queue as HTTP downloads.
//! Real DHT, tracker, and peer protocol handling comes from librqbit; pause and
//! resume map to session pause/unpause. Torrent tasks are piece-verified by
//! the BitTorrent protocol itself.

use crate::engine::{Ctrl, EngineCtx, TaskEntry, TaskShared};
use crate::task::{AddRequest, TaskStatus};
use librqbit::{AddTorrent, AddTorrentOptions, ManagedTorrent, Session, SessionOptions};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

pub type ManagedTorrentHandle = Arc<ManagedTorrent>;

/// Opaque handle wrapper stored on the task entry.
pub struct TorrentHandleBox {
    pub handle: ManagedTorrentHandle,
}

async fn ensure_session(ectx: &Arc<EngineCtx>) -> std::result::Result<Arc<Session>, String> {
    let mut guard = ectx.torrent_session.lock().await;
    if let Some(s) = guard.as_ref() {
        return Ok(s.clone());
    }
    let default_output = ectx.cfg.lock().expect("config").download_dir.clone();
    let session = Session::new_with_opts(
        default_output,
        SessionOptions {
            // In-memory session state (no background persistence files).
            persistence: None,
            // Listen on an ephemeral IPv4 TCP port so we can seed back.
            listen: Some(librqbit::ListenerOptions {
                listen_addr: SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    *guard = Some(session.clone());
    Ok(session)
}

/// Resolve the torrent source into AddTorrent (magnet URL, .torrent URL, or
/// data fetched over HTTP by the engine's own HTTP client).
async fn resolve_source(
    ectx: &Arc<EngineCtx>,
    req: &AddRequest,
) -> std::result::Result<AddTorrent<'static>, String> {
    let url = req.url.trim();
    if url.starts_with("magnet:") {
        // Hand the magnet over; librqbit fetches metadata via DHT/peers.
        return Ok(AddTorrent::from_url(url.to_string()));
    }
    // .torrent over HTTP(S): download it ourselves (with our redirect policy).
    let resp = ectx
        .http
        .get(url)
        .send()
        .await
        .map_err(|e| format!("failed to fetch .torrent file: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!(
            "failed to fetch .torrent file: HTTP {}",
            resp.status().as_u16()
        ));
    }
    let limit = 32 * 1024 * 1024;
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("failed to read .torrent file: {e}"))?;
    if bytes.len() > limit {
        return Err(".torrent file too large".into());
    }
    Ok(AddTorrent::from_bytes(bytes.to_vec()))
}

pub(crate) async fn run_torrent_task(ectx: Arc<EngineCtx>, entry: Arc<TaskEntry>) {
    if !ectx.cfg.lock().expect("config").torrent_enabled {
        set_failed(
            &ectx,
            &entry,
            "BitTorrent transfers are disabled in settings",
        );
        return;
    }
    let session = match ensure_session(&ectx).await {
        Ok(s) => s,
        Err(e) => {
            set_failed(&ectx, &entry, &format!("torrent session init failed: {e}"));
            return;
        }
    };

    // Pause an already-managed torrent if we are resuming a paused task entry.
    let source = match resolve_source(&ectx, &entry.req).await {
        Ok(s) => s,
        Err(e) => {
            set_failed(&ectx, &entry, &e.to_string());
            return;
        }
    };

    let output_folder = {
        let m = entry.mutable.lock().expect("task");
        m.output_dir.to_string_lossy().to_string()
    };
    let opts = AddTorrentOptions {
        output_folder: Some(output_folder),
        // Allow resuming on top of existing files.
        overwrite: true,
        ..Default::default()
    };

    let response = match session.add_torrent(source, Some(opts)).await {
        Ok(r) => r,
        Err(e) => {
            set_failed(&ectx, &entry, &format!("failed to add torrent: {e}"));
            return;
        }
    };
    let handle = match response.into_handle() {
        Some(h) => h,
        None => {
            set_failed(&ectx, &entry, "torrent source produced no handle");
            return;
        }
    };
    *entry.torrent.lock().expect("torrent") = Some(TorrentHandleBox {
        handle: handle.clone(),
    });

    // If the torrent was previously paused (task resume path), unpause it.
    if handle.is_paused() {
        let _ = session.unpause(&handle).await;
    }

    // Adopt the torrent name as the task filename.
    if let Some(name) = handle.name() {
        let mut m = entry.mutable.lock().expect("task");
        if m.filename.is_empty() {
            m.filename = crate::security::sanitize_filename(Some(&name), "torrent");
        }
        m.protocol = Some("BitTorrent".into());
        m.resume_supported = true;
    }
    ectx.persist_task(&entry);
    set_status_downloading(&entry);

    // Supervision loop: poll stats, react to control changes.
    let mut ctrl = entry.shared.ctrl.subscribe();
    let mut prev_progress: u64 = 0;
    loop {
        let stats = handle.stats();
        {
            let mut m = entry.mutable.lock().expect("task");
            m.size = Some(stats.total_bytes);
            if stats.finished && !matches!(m.status, TaskStatus::Done) {
                m.status = TaskStatus::Done;
                m.finished_at = Some(crate::store::unix_now());
                m.checksum_ok = Some(true); // piece-verified by protocol
                m.speed_bps = 0;
                drop(m);
                ectx.persist_task(&entry);
                let _ = ectx.events.send(crate::engine::EngineEvent::TaskFinished(
                    entry.id,
                    TaskStatus::Done,
                    None,
                ));
                entry.torrent.lock().expect("torrent").take();
                return;
            }
            if let Some(live) = stats.live.as_ref() {
                m.speed_bps = (live.download_speed.mbps * 125_000.0) as u64;
            } else if m.status == TaskStatus::Downloading {
                m.speed_bps = 0;
            }
            if stats.progress_bytes >= prev_progress {
                prev_progress = stats.progress_bytes;
            }
            m.protocol = Some("BitTorrent".into());
        }

        // Ctrl poll (copy the state first: guards must not live across awaits).
        let ctrl_state = *ctrl.borrow();
        match ctrl_state {
            Ctrl::Run => {}
            Ctrl::Pause => {
                let _ = session.pause(&handle).await;
                entry.mutable.lock().expect("task").status = TaskStatus::Paused;
                ectx.persist_task(&entry);
                entry.torrent.lock().expect("torrent").take();
                // Keep the entry joinable-again state clean.
                entry.join.lock().expect("join").take();
                return;
            }
            Ctrl::Stop => {
                let _ = session.pause(&handle).await;
                let mut m = entry.mutable.lock().expect("task");
                m.status = TaskStatus::Cancelled;
                m.finished_at = Some(crate::store::unix_now());
                drop(m);
                ectx.persist_task(&entry);
                let _ = ectx.events.send(crate::engine::EngineEvent::TaskFinished(
                    entry.id,
                    TaskStatus::Cancelled,
                    None,
                ));
                entry.torrent.lock().expect("torrent").take();
                entry.join.lock().expect("join").take();
                return;
            }
        }

        if stats.error.is_some() {
            let msg = stats.error.clone().unwrap_or_default();
            // librqbit keeps retrying; only fail hard on repeated zero progress.
            if prev_progress == 0 {
                let m = entry.mutable.lock().expect("task");
                let created = m.created_at;
                drop(m);
                if std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0)
                    - created
                    > 600
                {
                    set_failed(&ectx, &entry, &format!("torrent error: {msg}"));
                    return;
                }
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(400)) => {}
            _ = ctrl.changed() => {}
        }
    }
}

fn set_status_downloading(entry: &Arc<TaskEntry>) {
    let mut m = entry.mutable.lock().expect("task");
    if !m.status.is_terminal() {
        m.status = TaskStatus::Downloading;
        if m.started_at.is_none() {
            m.started_at = Some(crate::store::unix_now());
        }
    }
}

fn set_failed(ectx: &Arc<EngineCtx>, entry: &Arc<TaskEntry>, msg: &str) {
    let mut m = entry.mutable.lock().expect("task");
    m.status = TaskStatus::Failed;
    m.error = Some(msg.to_string());
    m.finished_at = Some(crate::store::unix_now());
    drop(m);
    ectx.persist_task(entry);
    let _ = ectx.events.send(crate::engine::EngineEvent::TaskFinished(
        entry.id,
        TaskStatus::Failed,
        Some(msg.to_string()),
    ));
}

/// Progress for torrent tasks is read from librqbit stats by the engine ticker.
pub(crate) fn torrent_progress(entry: &Arc<TaskEntry>) -> Option<u64> {
    let guard = entry.torrent.lock().expect("torrent");
    let h = guard.as_ref()?;
    let stats = h.handle.stats();
    Some(stats.progress_bytes)
}

impl TaskShared {
    /// Keep net accounting consistent for torrent tasks.
    pub fn note_torrent_bytes(&self, n: u64) {
        self.net_bytes.store(n, Ordering::Relaxed);
    }
}
