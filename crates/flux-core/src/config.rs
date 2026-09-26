//! Engine configuration: persisted as `settings.json` in the Fluxload data dir.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "mode")]
#[derive(Default)]
pub enum CacheMode {
    /// Budget derived from live free RAM (auto-adaptive).
    #[default]
    Auto,
    /// Fixed budget in MB set by the user.
    Manual { budget_mb: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct RamCacheConfig {
    pub mode: CacheMode,
    /// Fraction of available RAM usable in Auto mode.
    pub auto_fraction: f32,
    /// Lower/upper bounds for the Auto budget (MB).
    pub auto_min_mb: u64,
    pub auto_max_mb: u64,
}

impl Default for RamCacheConfig {
    fn default() -> Self {
        Self {
            mode: CacheMode::Auto,
            auto_fraction: 0.25,
            auto_min_mb: 16,
            auto_max_mb: 512,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ProxyConfig {
    /// e.g. `socks5://127.0.0.1:1080` or `http://proxy.corp:3128`
    pub url: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

/// A daily bandwidth window. Applies to local time. `limit_bps: None` = unlimited.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RateWindow {
    /// "HH:MM" local time, inclusive start.
    pub start: String,
    /// "HH:MM" local time, exclusive end (may wrap midnight).
    pub end: String,
    /// None = unlimited during this window.
    pub limit_bps: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct EngineConfig {
    /// Directory for tasks.json / settings.json / logs.
    pub data_dir: PathBuf,
    /// Default download destination.
    pub download_dir: PathBuf,
    /// Concurrent actively downloading tasks.
    pub max_active_tasks: usize,
    /// Default segment connections per task.
    pub default_connections: u32,
    /// Hard cap on connections per task.
    pub max_connections_per_task: u32,
    /// Minimum segment size before splitting further.
    pub min_segment_mb: u64,
    /// Global speed cap (bytes/sec), None = unlimited.
    pub global_speed_limit_bps: Option<u64>,
    /// Default per-task cap (bytes/sec), None = unlimited.
    pub per_task_limit_default_bps: Option<u64>,
    pub connect_timeout_sec: u64,
    pub read_timeout_sec: u64,
    pub max_retries_per_segment: u32,
    /// Base backoff for failed segments (exponential, capped at 30 s).
    pub retry_backoff_ms: u64,
    pub user_agent: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy: Option<ProxyConfig>,
    /// Always compute SHA-256 on finish (and compare when an expected hash was given).
    pub verify_sha256_on_finish: bool,
    pub ram_cache: RamCacheConfig,
    /// Opportunistic HTTP/3 with HTTP/1.1/2 fallback.
    pub http3_enabled: bool,
    /// BitTorrent transfers enabled (requires `torrent` build feature).
    pub torrent_enabled: bool,
    /// Resume incomplete tasks from the persisted queue on startup.
    pub auto_resume_on_start: bool,
    /// Bandwidth schedule windows (local time).
    pub schedule: Vec<RateWindow>,
    /// Hard cap on total concurrent TCP/QUIC connections across all tasks.
    pub max_total_connections: u32,
    /// TEST-ONLY: accept self-signed certificates (used by the local test
    /// server). Never enable in production builds.
    #[serde(default)]
    pub tls_insecure: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        let download_dir = dirs::download_dir()
            .or_else(dirs::home_dir)
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            data_dir: default_data_dir(),
            download_dir,
            max_active_tasks: 3,
            default_connections: 8,
            max_connections_per_task: 64,
            min_segment_mb: 2,
            global_speed_limit_bps: None,
            per_task_limit_default_bps: None,
            connect_timeout_sec: 20,
            read_timeout_sec: 60,
            max_retries_per_segment: 15,
            retry_backoff_ms: 500,
            user_agent: format!(
                "Fluxload/{} (https://github.com/salim77007j/fluxload)",
                crate::VERSION
            ),
            proxy: None,
            verify_sha256_on_finish: true,
            ram_cache: RamCacheConfig::default(),
            http3_enabled: true,
            torrent_enabled: true,
            auto_resume_on_start: true,
            schedule: Vec::new(),
            max_total_connections: 128,
            tls_insecure: false,
        }
    }
}

/// Standard Fluxload data dir: Linux `~/.config/fluxload`, Windows `%APPDATA%\fluxload`.
pub fn default_data_dir() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| dirs::home_dir().unwrap_or_else(|| PathBuf::from(".")))
        .join(crate::PRODUCT)
}

impl EngineConfig {
    pub fn load_or_default() -> Self {
        let dir = default_data_dir();
        let path = dir.join("settings.json");
        Self::load_from(&path).unwrap_or_default()
    }

    pub fn load_from(path: &Path) -> Option<Self> {
        let text = std::fs::read_to_string(path).ok()?;
        match serde_json::from_str(&text) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                tracing::warn!("failed to parse {}: {e}; using defaults", path.display());
                None
            }
        }
    }

    pub fn save(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.data_dir)?;
        let path = self.data_dir.join("settings.json");
        atomic_write(
            path,
            serde_json::to_vec_pretty(self).expect("config serializes"),
        )
    }

    /// Compute the current RAM write-cache budget in bytes given available RAM.
    pub fn ram_cache_budget_bytes(&self, available_ram: u64) -> u64 {
        let cfg = &self.ram_cache;
        let budget = match cfg.mode {
            CacheMode::Manual { budget_mb } => budget_mb * 1024 * 1024,
            CacheMode::Auto => {
                let frac = cfg.auto_fraction.clamp(0.05, 0.5) as f64;
                let b = (available_ram as f64 * frac) as u64;
                b.clamp(cfg.auto_min_mb * 1024 * 1024, cfg.auto_max_mb * 1024 * 1024)
            }
        };
        budget.max(4 * 1024 * 1024)
    }
}

/// Write a file atomically: temp file in the same directory + rename.
pub fn atomic_write(path: PathBuf, data: Vec<u8>) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)] // staged config setup reads top-down
mod tests {
    use super::*;

    #[test]
    fn config_roundtrip() {
        let dir = std::env::temp_dir().join(format!("flux-test-cfg-{}", std::process::id()));
        let mut cfg = EngineConfig::default();
        cfg.data_dir = dir.clone();
        cfg.save().expect("save config");
        let loaded = EngineConfig::load_from(&dir.join("settings.json")).expect("load config");
        assert_eq!(cfg, loaded);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn ram_budget_bounds() {
        let mut cfg = EngineConfig::default();
        // 4 GB available -> 25% = 1 GB, clamped to 512 MB.
        assert_eq!(cfg.ram_cache_budget_bytes(4 << 30), 512 * 1024 * 1024);
        // 64 MB available -> 16 MB, clamped to 16 MB min.
        assert_eq!(cfg.ram_cache_budget_bytes(64 << 20), 16 * 1024 * 1024);
        cfg.ram_cache.mode = CacheMode::Manual { budget_mb: 64 };
        assert_eq!(cfg.ram_cache_budget_bytes(u64::MAX / 4), 64 * 1024 * 1024);
    }
}
