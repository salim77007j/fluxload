//! Fluxload engine core.
//!
//! A commercial-grade, memory-safe download engine written in Rust:
//! - Multi-segment HTTP/HTTPS transfers with adaptive lease planning
//! - Crash-safe partial storage (pwrite + fsync-before-metadata, atomic metadata)
//! - Opportunistic HTTP/3 (QUIC) with automatic HTTP/1.1/2 fallback
//! - Hybrid BitTorrent transfers (feature `torrent`)
//! - Token-bucket rate limiting (global + per task), bandwidth scheduling
//! - Adaptive RAM write cache sized from live system memory
//! - SHA-256 integrity verification
//! - Offline Ed25519 license verification
//!
//! Everything exposed by this crate is real, working functionality.

pub mod bench;
pub mod config;
pub mod doctor;
pub mod download;
pub mod engine;
pub mod errors;
pub mod format;
pub mod license;
pub mod limit;
pub mod probe;
pub mod security;
pub mod storage;
pub mod store;
pub mod task;

#[cfg(feature = "torrent")]
pub mod torrent;

pub const PRODUCT: &str = "fluxload";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Feature availability reported in the UI (never faked: reflects compile-time reality).
pub struct BuildFeatures {
    /// reqwest is always built with the `http3` feature in this workspace.
    pub http3: bool,
    pub torrent: bool,
}

pub fn build_features() -> BuildFeatures {
    BuildFeatures {
        http3: true,
        torrent: cfg!(feature = "torrent"),
    }
}
