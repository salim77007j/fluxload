//! Error types for the Fluxload engine.

/// All engine errors. Strings are user-presentable; details are preserved via `source`.
#[derive(thiserror::Error, Debug)]
pub enum FluxError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),

    #[error("network error: {0}")]
    Network(String),

    #[error("server returned {status} for {context}")]
    HttpStatus { status: u16, context: String },

    #[error("server content changed mid-download ({reason}); a clean restart is required")]
    ServerChanged { reason: String },

    #[error("checksum mismatch: expected {expected}, computed {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    #[error("disk error: {0}")]
    Disk(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Task(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("operation cancelled")]
    Cancelled,

    #[error("shutting down")]
    Shutdown,
}

pub type Result<T, E = FluxError> = std::result::Result<T, E>;

impl FluxError {
    /// Short machine tag used in JSON events for CLI/tests.
    pub fn tag(&self) -> &'static str {
        match self {
            FluxError::InvalidUrl(_) => "invalid_url",
            FluxError::Network(_) => "network",
            FluxError::HttpStatus { .. } => "http_status",
            FluxError::ServerChanged { .. } => "server_changed",
            FluxError::ChecksumMismatch { .. } => "checksum_mismatch",
            FluxError::Disk(_) => "disk",
            FluxError::Io(_) => "io",
            FluxError::Task(_) => "task",
            FluxError::Unsupported(_) => "unsupported",
            FluxError::Cancelled => "cancelled",
            FluxError::Shutdown => "shutdown",
        }
    }
}
