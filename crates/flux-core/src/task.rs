//! Task model: what the queue, CLI and GUI operate on.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub type TaskId = Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    Probing,
    Downloading,
    Paused,
    Verifying,
    Done,
    Failed,
    Cancelled,
}

impl TaskStatus {
    pub fn label(self) -> &'static str {
        match self {
            TaskStatus::Queued => "Queued",
            TaskStatus::Probing => "Probing",
            TaskStatus::Downloading => "Downloading",
            TaskStatus::Paused => "Paused",
            TaskStatus::Verifying => "Verifying",
            TaskStatus::Done => "Completed",
            TaskStatus::Failed => "Failed",
            TaskStatus::Cancelled => "Cancelled",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(
            self,
            TaskStatus::Probing | TaskStatus::Downloading | TaskStatus::Verifying
        )
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Done | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TaskKind {
    #[default]
    Http,
    Torrent,
}

impl TaskKind {
    pub fn label(self) -> &'static str {
        match self {
            TaskKind::Http => "HTTP",
            TaskKind::Torrent => "BitTorrent",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SegmentSnap {
    /// Inclusive start byte.
    pub start: u64,
    /// Exclusive end byte.
    pub end: u64,
    pub done: u64,
    pub speed_bps: u64,
    pub state: SegState,
    pub retries: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum SegState {
    Pending,
    Connecting,
    Receiving,
    Retrying,
    Done,
}

impl SegState {
    pub fn label(self) -> &'static str {
        match self {
            SegState::Pending => "pending",
            SegState::Connecting => "connecting",
            SegState::Receiving => "receiving",
            SegState::Retrying => "retrying",
            SegState::Done => "done",
        }
    }
}

/// A live view of one download, rebuilt on every engine tick.
#[derive(Clone, Debug, Serialize)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub url: String,
    pub filename: String,
    pub output_dir: PathBuf,
    pub kind: TaskKind,
    pub status: TaskStatus,
    pub size: Option<u64>,
    /// Durable + buffered bytes (resumed base + flushed + in RAM).
    pub downloaded: u64,
    /// Bytes received from the network this session (used for wire speed).
    pub net_bytes: u64,
    pub speed_bps: u64,
    pub avg_bps: u64,
    pub eta_sec: Option<u64>,
    pub error: Option<String>,
    /// Negotiated protocol label, e.g. "HTTP/1.1", "HTTP/2", "HTTP/3", "BitTorrent".
    pub protocol: Option<String>,
    pub resume_supported: bool,
    pub expected_sha256: Option<String>,
    pub actual_sha256: Option<String>,
    pub checksum_ok: Option<bool>,
    pub retries: u32,
    pub active_conns: u32,
    /// Active segment leases (the connections detail view).
    pub segments: Vec<SegmentSnap>,
    /// (unix_ms, bytes_per_sec) samples, newest last, up to 240.
    pub speed_history: Vec<(i64, u64)>,
    /// Byte coverage of completed ranges for the segment grid.
    pub done_ranges: Vec<(u64, u64)>,
    pub created_at: i64,
    pub finished_at: Option<i64>,
}

impl TaskSnapshot {
    pub fn progress_frac(&self) -> Option<f32> {
        self.size.map(|s| {
            if s == 0 {
                1.0
            } else {
                (self.downloaded as f32 / s as f32).clamp(0.0, 1.0)
            }
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct GlobalStats {
    pub speed_bps: u64,
    pub total_session_bytes: u64,
    pub history: Vec<(i64, u64)>,
    pub active_tasks: usize,
    pub queued_tasks: usize,
    pub total_conns: u32,
    pub cache_in_use_bytes: u64,
    pub cache_budget_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct ConfigSummary {
    pub max_active_tasks: usize,
    pub global_limit_bps: Option<u64>,
    pub http3_enabled: bool,
    pub torrent_enabled: bool,
    pub torrent_compiled: bool,
    pub proxy: Option<String>,
    pub ram_cache_mb: u64,
    pub schedule_windows: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct EngineSnapshot {
    pub tasks: Vec<TaskSnapshot>,
    pub global: GlobalStats,
    pub config: ConfigSummary,
    pub license: Option<crate::license::LicenseInfo>,
    pub version: String,
    pub started_at_unix: i64,
}

/// Request to add a new download. Produced by CLI, GUI, or browser bridge.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AddRequest {
    pub url: String,
    #[serde(default)]
    pub output_dir: Option<PathBuf>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub connections: Option<u32>,
    #[serde(default)]
    pub speed_limit_bps: Option<u64>,
    /// Expected SHA-256 (64 hex chars). Verified on completion.
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    /// Unix seconds; task stays queued until this time.
    #[serde(default)]
    pub schedule_at_unix: Option<i64>,
    #[serde(default = "default_true")]
    pub auto_start: bool,
    /// Origin hint for diagnostics (e.g. "browser-extension").
    #[serde(default)]
    pub origin: Option<String>,
}

fn default_true() -> bool {
    true
}

impl AddRequest {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            output_dir: None,
            filename: None,
            connections: None,
            speed_limit_bps: None,
            checksum: None,
            headers: Vec::new(),
            schedule_at_unix: None,
            auto_start: true,
            origin: None,
        }
    }
}

/// Persisted task record (cross-restart queue).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoredTask {
    pub id: TaskId,
    pub url: String,
    pub filename: String,
    pub output_dir: PathBuf,
    #[serde(default)]
    pub kind: TaskKind,
    pub status: TaskStatus,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub connections: Option<u32>,
    #[serde(default)]
    pub speed_limit_bps: Option<u64>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub schedule_at_unix: Option<i64>,
    pub created_at: i64,
    #[serde(default)]
    pub finished_at: Option<i64>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub actual_sha256: Option<String>,
    #[serde(default)]
    pub checksum_ok: Option<bool>,
    #[serde(default)]
    pub origin: Option<String>,
}
