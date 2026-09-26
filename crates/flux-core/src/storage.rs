//! Crash-safe partial-file storage.
//!
//! Design (power-loss safe):
//! 1. Downloads write to `<final>.fluxpart` with positional writes.
//! 2. Each flush batch: pwrite -> fsync(file) -> update done-ranges -> atomically
//!    persist metadata (`<final>.fluxpart.json` via tmp+rename). The metadata
//!    never claims more than what is fsynced.
//! 3. On resume, done-ranges describe exactly which byte ranges are durable;
//!    the engine re-plans leases over the gaps.
//! 4. Finalize: fsync + rename part -> final name + remove metadata.
//!
//! The RAM write cache lives in the workers (`WriteBuffer`); `in_use` accounting
//! is centralized here so the engine can report real cache usage.

use crate::config::atomic_write;
use crate::errors::{FluxError, Result};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// Half-open byte range [start, end).
pub type ByteRange = (u64, u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartialMeta {
    pub version: u32,
    pub url: String,
    /// Total size if known (0 = unknown / streaming).
    pub size: u64,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub last_modified: Option<String>,
    /// Sorted, merged durable byte ranges.
    #[serde(default)]
    pub done_ranges: Vec<ByteRange>,
}

impl PartialMeta {
    /// Load metadata from disk (None if missing or unreadable).
    pub fn load(path: &Path) -> Option<PartialMeta> {
        let text = std::fs::read_to_string(path).ok()?;
        match serde_json::from_str(&text) {
            Ok(m) => Some(m),
            Err(e) => {
                tracing::warn!("corrupt partial metadata {}: {e}", path.display());
                None
            }
        }
    }

    pub fn bytes_done(&self) -> u64 {
        self.done_ranges.iter().map(|(s, e)| e - s).sum()
    }

    /// Gaps within [0, size) not covered by done_ranges.
    pub fn gaps(&self) -> Vec<ByteRange> {
        let mut gaps = Vec::new();
        if self.size == 0 {
            return gaps;
        }
        let mut cursor = 0u64;
        for &(s, e) in &self.done_ranges {
            if s > cursor {
                gaps.push((cursor, s.min(self.size)));
            }
            cursor = cursor.max(e);
            if cursor >= self.size {
                break;
            }
        }
        if cursor < self.size {
            gaps.push((cursor, self.size));
        }
        gaps
    }

    /// Insert a durable range and re-merge.
    pub fn add_range(&mut self, start: u64, end: u64) {
        if end <= start {
            return;
        }
        self.done_ranges.push((start, end));
        self.done_ranges.sort_unstable();
        let mut merged: Vec<ByteRange> = Vec::with_capacity(self.done_ranges.len());
        for &(s, e) in &self.done_ranges {
            match merged.last_mut() {
                Some((_, le)) if s <= *le => {
                    *le = (*le).max(e);
                }
                _ => merged.push((s, e)),
            }
        }
        self.done_ranges = merged;
    }

    pub fn is_complete(&self) -> bool {
        self.size > 0 && self.gaps().is_empty()
    }
}

/// Server validators used to decide whether a partial file is still resumable.
#[derive(Clone, Debug)]
pub struct Expected {
    pub url: String,
    pub size: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub enum OpenOutcome {
    Fresh,
    Resumed(u64),
}

/// Shared write cache accounting for the whole engine.
#[derive(Default)]
pub struct WriteBudget {
    budget_bytes: AtomicI64,
    in_use_bytes: AtomicI64,
    /// Per-worker flush threshold hint in bytes.
    pub flush_hint_bytes: std::sync::atomic::AtomicU64,
}

impl WriteBudget {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn set_budget(&self, bytes: u64) {
        self.budget_bytes.store(bytes as i64, Ordering::Relaxed);
    }

    pub fn budget(&self) -> u64 {
        self.budget_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn in_use(&self) -> u64 {
        self.in_use_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn add_in_use(&self, n: usize) {
        self.in_use_bytes.fetch_add(n as i64, Ordering::Relaxed);
    }

    pub fn sub_in_use(&self, n: usize) {
        self.in_use_bytes.fetch_sub(n as i64, Ordering::Relaxed);
    }
}

/// Worker-local RAM buffer that coalesces network chunks before hitting the disk.
/// Accounts buffered bytes against both the engine-wide budget and the task counter.
pub struct WriteBuffer {
    base_off: u64,
    buf: Vec<u8>,
    budget: Option<Arc<WriteBudget>>,
    task_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
}

impl WriteBuffer {
    pub fn new(
        budget: Option<Arc<WriteBudget>>,
        task_counter: Option<Arc<std::sync::atomic::AtomicU64>>,
    ) -> Self {
        Self {
            base_off: 0,
            buf: Vec::with_capacity(256 * 1024),
            budget,
            task_counter,
        }
    }

    /// Append a contiguous chunk. The first append fixes the base offset.
    pub fn append(&mut self, off: u64, data: &[u8]) {
        if self.buf.is_empty() {
            self.base_off = off;
        }
        debug_assert_eq!(
            self.base_off + self.buf.len() as u64,
            off,
            "non-contiguous write"
        );
        if self.base_off + self.buf.len() as u64 != off {
            // Safety net: flush what we have and rebase (must not normally happen).
            self.buf.clear();
            self.base_off = off;
        }
        self.buf.extend_from_slice(data);
        if let Some(b) = &self.budget {
            b.add_in_use(data.len());
        }
        if let Some(t) = &self.task_counter {
            t.fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn should_flush(&self, hint: u64) -> bool {
        self.buf.len() >= hint as usize
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Take the buffered bytes and the absolute offset they belong to.
    pub fn take(&mut self) -> (u64, Vec<u8>) {
        let data = std::mem::take(&mut self.buf);
        if let Some(b) = &self.budget {
            b.sub_in_use(data.len());
        }
        if let Some(t) = &self.task_counter {
            t.fetch_sub(data.len() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        (self.base_off, data)
    }
}

/// Positional write that works on Unix and Windows.
fn pwrite(file: &File, buf: &[u8], off: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, off)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut pos = off;
        for chunk in buf.chunks(1 << 30) {
            let mut written = 0usize;
            while written < chunk.len() {
                let n = file.seek_write(&chunk[written..], pos)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    ));
                }
                written += n;
                pos += n as u64;
            }
        }
        Ok(())
    }
}

/// An open partial download. Cloning is cheap; the underlying file is shared.
pub struct PartialFile {
    pub final_path: PathBuf,
    pub part_path: PathBuf,
    meta_path: PathBuf,
    file: Mutex<File>,
    pub meta: Mutex<PartialMeta>,
}

impl PartialFile {
    /// Open (or create) the partial file for `final_path`.
    pub fn open(final_path: &Path, expected: &Expected) -> Result<(Arc<Self>, OpenOutcome)> {
        let part_path = part_path_for(final_path);
        let meta_path = meta_path_for(final_path);
        let mut outcome = OpenOutcome::Fresh;

        let meta = match PartialMeta::load(&meta_path) {
            Some(m) if part_path.exists() => {
                let size_ok = expected.size.is_none_or(|s| m.size == s);
                let etag_ok = match (&m.etag, &expected.etag) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
                let lm_ok = match (&m.last_modified, &expected.last_modified) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                };
                if size_ok && etag_ok && lm_ok {
                    let resumed = m.bytes_done();
                    outcome = OpenOutcome::Resumed(resumed);
                    m
                } else {
                    tracing::info!(
                        "partial file {} no longer matches the server (size/etag/last-modified changed); restarting cleanly",
                        part_path.display()
                    );
                    let _ = std::fs::remove_file(&part_path);
                    let _ = std::fs::remove_file(&meta_path);
                    PartialMeta {
                        version: 1,
                        url: expected.url.clone(),
                        size: expected.size.unwrap_or(0),
                        etag: expected.etag.clone(),
                        last_modified: expected.last_modified.clone(),
                        done_ranges: Vec::new(),
                    }
                }
            }
            _ => {
                let _ = std::fs::remove_file(&part_path);
                PartialMeta {
                    version: 1,
                    url: expected.url.clone(),
                    size: expected.size.unwrap_or(0),
                    etag: expected.etag.clone(),
                    last_modified: expected.last_modified.clone(),
                    done_ranges: Vec::new(),
                }
            }
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // Never truncate: a partial file's existing bytes ARE the resume
            // state. Setting truncate(true) would destroy crash recovery.
            .truncate(false)
            .open(&part_path)
            .map_err(|e| {
                FluxError::Disk(format!(
                    "cannot open partial file {}: {e}",
                    part_path.display()
                ))
            })?;

        if let Some(size) = expected.size {
            if file.metadata().map(|m| m.len()).unwrap_or(0) != size {
                file.set_len(size).map_err(|e| {
                    FluxError::Disk(format!("cannot preallocate {}: {e}", part_path.display()))
                })?;
            }
        }

        let pf = Arc::new(Self {
            final_path: final_path.to_path_buf(),
            part_path,
            meta_path,
            file: Mutex::new(file),
            meta: Mutex::new(meta),
        });
        pf.persist_meta()?;
        Ok((pf, outcome))
    }

    /// Flush one batch: pwrite + fsync + metadata advance + atomic meta save.
    pub fn flush_batch(&self, off: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        {
            let file = self.file.lock().expect("partial file lock");
            pwrite(&file, data, off).map_err(|e| {
                FluxError::Disk(format!("write to {}: {e}", self.part_path.display()))
            })?;
            file.sync_all()
                .map_err(|e| FluxError::Disk(format!("fsync {}: {e}", self.part_path.display())))?;
        }
        {
            let mut meta = self.meta.lock().expect("meta lock");
            meta.add_range(off, off + data.len() as u64);
        }
        self.persist_meta()?;
        Ok(())
    }

    fn persist_meta(&self) -> Result<()> {
        let meta = self.meta.lock().expect("meta lock");
        let data = serde_json::to_vec(&*meta)
            .map_err(|e| FluxError::Task(format!("meta serialize: {e}")))?;
        drop(meta);
        atomic_write(self.meta_path.clone(), data)
            .map_err(|e| FluxError::Disk(format!("persist meta: {e}")))
    }

    pub fn bytes_done(&self) -> u64 {
        self.meta.lock().expect("meta lock").bytes_done()
    }

    pub fn done_ranges(&self) -> Vec<ByteRange> {
        self.meta.lock().expect("meta lock").done_ranges.clone()
    }

    pub fn gaps(&self) -> Vec<ByteRange> {
        self.meta.lock().expect("meta lock").gaps()
    }

    pub fn size(&self) -> u64 {
        self.meta.lock().expect("meta lock").size
    }

    pub fn is_complete(&self) -> bool {
        self.meta.lock().expect("meta lock").is_complete()
    }

    /// Rename the part file into place and remove metadata.
    pub fn finalize(self: &Arc<Self>) -> Result<()> {
        {
            let file = self.file.lock().expect("partial file lock");
            file.sync_all()
                .map_err(|e| FluxError::Disk(format!("final fsync: {e}")))?;
        }
        if let Some(parent) = self.final_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| FluxError::Disk(format!("create output dir: {e}")))?;
        }
        if self.final_path.exists() {
            std::fs::remove_file(&self.final_path)
                .map_err(|e| FluxError::Disk(format!("replace existing file: {e}")))?;
        }
        std::fs::rename(&self.part_path, &self.final_path)
            .map_err(|e| FluxError::Disk(format!("finalize {}: {e}", self.final_path.display())))?;
        let _ = std::fs::remove_file(&self.meta_path);
        Ok(())
    }

    /// Remove the partial data entirely.
    pub fn discard(self: &Arc<Self>) {
        let _ = std::fs::remove_file(&self.part_path);
        let _ = std::fs::remove_file(&self.meta_path);
    }

    /// Stream the partial file through SHA-256 (call before `finalize`).
    pub fn compute_sha256(&self) -> Result<String> {
        use sha2::Digest;
        let mut f = File::open(&self.part_path).map_err(FluxError::Io)?;
        let mut hasher = sha2::Sha256::new();
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = std::io::Read::read(&mut f, &mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hex::encode(hasher.finalize()))
    }
}

pub fn part_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".fluxpart");
    PathBuf::from(s)
}

pub fn meta_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".fluxpart.json");
    PathBuf::from(s)
}

/// Choose a non-colliding output filename: `movie.bin` -> `movie (2).bin`.
pub fn dedupe_filename(dir: &Path, name: &str) -> String {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return name.to_string();
    }
    let (stem, ext) = match name.rsplit_once('.') {
        Some((s, e)) => (s.to_string(), format!(".{e}")),
        None => (name.to_string(), String::new()),
    };
    for i in 2..1000 {
        let alt = format!("{stem} ({i}){ext}");
        if !dir.join(&alt).exists() {
            return alt;
        }
    }
    format!("{stem}-{}{ext}", uuid::Uuid::new_v4().simple())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("flux-storage-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn meta_range_math() {
        let mut m = PartialMeta {
            version: 1,
            url: "u".into(),
            size: 100,
            etag: None,
            last_modified: None,
            done_ranges: vec![],
        };
        m.add_range(10, 20);
        m.add_range(20, 30);
        m.add_range(50, 60);
        assert_eq!(m.done_ranges, vec![(10, 30), (50, 60)]);
        assert_eq!(m.bytes_done(), 30);
        assert_eq!(m.gaps(), vec![(0, 10), (30, 50), (60, 100)]);
        m.add_range(0, 10);
        m.add_range(30, 50);
        m.add_range(60, 100);
        assert!(m.is_complete());
        assert_eq!(m.bytes_done(), 100);
    }

    #[test]
    fn flush_and_resume() {
        let dir = tmpdir("flush");
        let final_path = dir.join("data.bin");
        let expected = Expected {
            url: "http://x/f".into(),
            size: Some(1024),
            etag: Some("\"abc\"".into()),
            last_modified: None,
        };
        let (pf, outcome) = PartialFile::open(&final_path, &expected).unwrap();
        assert!(matches!(outcome, OpenOutcome::Fresh));
        pf.flush_batch(0, &[1u8; 100]).unwrap();
        pf.flush_batch(100, &[2u8; 100]).unwrap();
        assert_eq!(pf.bytes_done(), 200);
        drop(pf);

        // Simulate restart: reopen and check resume + gaps.
        let (pf2, outcome) = PartialFile::open(&final_path, &expected).unwrap();
        assert!(matches!(outcome, OpenOutcome::Resumed(200)));
        assert_eq!(pf2.gaps(), vec![(200, 1024)]);

        // Data integrity of the persisted bytes.
        let written = std::fs::read(part_path_for(&final_path)).unwrap();
        assert_eq!(&written[..100], &vec![1u8; 100][..]);
        assert_eq!(&written[100..200], &vec![2u8; 100][..]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn etag_mismatch_forces_clean_restart() {
        let dir = tmpdir("etag");
        let final_path = dir.join("f.bin");
        let e1 = Expected {
            url: "u".into(),
            size: Some(10),
            etag: Some("\"v1\"".into()),
            last_modified: None,
        };
        let (pf, _) = PartialFile::open(&final_path, &e1).unwrap();
        pf.flush_batch(0, &[0u8; 10]).unwrap();
        drop(pf);

        let e2 = Expected {
            url: "u".into(),
            size: Some(10),
            etag: Some("\"v2\"".into()),
            last_modified: None,
        };
        let (pf2, outcome) = PartialFile::open(&final_path, &e2).unwrap();
        assert!(matches!(outcome, OpenOutcome::Fresh));
        assert_eq!(pf2.bytes_done(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn finalize_renames() {
        let dir = tmpdir("fin");
        let final_path = dir.join("out.bin");
        let expected = Expected {
            url: "u".into(),
            size: Some(16),
            etag: None,
            last_modified: None,
        };
        let (pf, _) = PartialFile::open(&final_path, &expected).unwrap();
        pf.flush_batch(0, &[9u8; 16]).unwrap();
        assert!(pf.is_complete());
        let hash = pf.compute_sha256().unwrap();
        assert_eq!(hash.len(), 64);
        pf.finalize().unwrap();
        assert!(final_path.exists());
        assert!(!part_path_for(&final_path).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn filename_dedupe() {
        let dir = tmpdir("dedupe");
        std::fs::write(dir.join("a.bin"), b"x").unwrap();
        assert_eq!(dedupe_filename(&dir, "a.bin"), "a (2).bin");
        assert_eq!(dedupe_filename(&dir, "b.bin"), "b.bin");
        std::fs::remove_dir_all(&dir).ok();
    }
}
