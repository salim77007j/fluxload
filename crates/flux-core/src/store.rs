//! Persisted task queue (`tasks.json`) — survives restarts, shared with the
//! browser native-messaging host which appends new tasks to the same file.

use crate::task::{StoredTask, TaskId};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::SystemTime;

pub struct Store {
    path: PathBuf,
    tasks: Mutex<Vec<StoredTask>>,
}

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Store {
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(data_dir)?;
        let path = data_dir.join("tasks.json");
        let tasks = Self::read_from(&path).unwrap_or_default();
        Ok(Self {
            path,
            tasks: Mutex::new(tasks),
        })
    }

    fn read_from(path: &Path) -> Option<Vec<StoredTask>> {
        let text = std::fs::read_to_string(path).ok()?;
        match serde_json::from_str(&text) {
            Ok(v) => Some(v),
            Err(e) => {
                tracing::warn!("tasks.json unreadable ({e}); starting with an empty queue");
                None
            }
        }
    }

    pub fn mtime(&self) -> Option<SystemTime> {
        std::fs::metadata(&self.path)
            .ok()
            .and_then(|m| m.modified().ok())
    }

    /// Reload from disk and return tasks that were added by other processes
    /// (e.g. the browser bridge). Existing ids are refreshed in place.
    pub fn import_external(&self, known: &[TaskId]) -> Vec<StoredTask> {
        let fresh = Self::read_from(&self.path).unwrap_or_default();
        let mut added = Vec::new();
        let mut tasks = self.tasks.lock().expect("store lock");
        for t in fresh {
            if known.contains(&t.id) {
                // refresh persisted fields of known tasks
                if let Some(existing) = tasks.iter_mut().find(|e| e.id == t.id) {
                    *existing = t;
                }
            } else {
                added.push(t.clone());
                tasks.push(t);
            }
        }
        added
    }

    pub fn upsert(&self, task: &StoredTask) {
        let mut tasks = self.tasks.lock().expect("store lock");
        match tasks.iter_mut().find(|t| t.id == task.id) {
            Some(t) => *t = task.clone(),
            None => tasks.push(task.clone()),
        }
        self.persist_locked(&tasks);
    }

    pub fn remove(&self, id: TaskId) {
        let mut tasks = self.tasks.lock().expect("store lock");
        tasks.retain(|t| t.id != id);
        self.persist_locked(&tasks);
    }

    pub fn list(&self) -> Vec<StoredTask> {
        self.tasks.lock().expect("store lock").clone()
    }

    fn persist_locked(&self, tasks: &[StoredTask]) {
        match serde_json::to_vec_pretty(tasks) {
            Ok(data) => {
                if let Err(e) = crate::config::atomic_write(self.path.clone(), data) {
                    tracing::error!("failed to persist tasks.json: {e}");
                }
            }
            Err(e) => tracing::error!("failed to serialize tasks.json: {e}"),
        }
    }
}

pub fn unix_now() -> i64 {
    now_unix()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{AddRequest, TaskKind, TaskStatus};

    fn stored(id: TaskId) -> StoredTask {
        StoredTask {
            id,
            url: "https://example.com/f.bin".into(),
            filename: "f.bin".into(),
            output_dir: std::env::temp_dir(),
            kind: TaskKind::Http,
            status: TaskStatus::Queued,
            size: Some(1),
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
            origin: None,
        }
    }

    #[test]
    fn store_roundtrip_and_import() {
        let dir = std::env::temp_dir().join(format!("flux-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let s = Store::open(&dir).unwrap();
        let id = TaskId::new_v4();
        s.upsert(&stored(id));
        assert_eq!(s.list().len(), 1);

        // Simulate an external (browser bridge) write to the same file.
        let mut external = s.list();
        let ext_id = TaskId::new_v4();
        external.push(stored(ext_id));
        std::fs::write(
            dir.join("tasks.json"),
            serde_json::to_vec_pretty(&external).unwrap(),
        )
        .unwrap();

        let added = s.import_external(&[id]);
        assert_eq!(added.len(), 1);
        assert_eq!(added[0].id, ext_id);
        assert_eq!(s.list().len(), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn add_request_serializes() {
        let r = AddRequest::new("magnet:?xt=urn:btih:abc");
        let s = serde_json::to_string(&r).unwrap();
        let back: AddRequest = serde_json::from_str(&s).unwrap();
        assert_eq!(back.url, r.url);
    }
}
