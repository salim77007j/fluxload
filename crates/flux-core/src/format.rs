//! Human-readable formatting helpers shared by the CLI and GUI.

/// Format byte counts: `12.5 MB`, `1.4 GB`, `512 B`.
pub fn fmt_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    let b = bytes as f64;
    if b < KB {
        format!("{bytes} B")
    } else if b < KB * KB {
        format!("{:.1} KB", b / KB)
    } else if b < KB * KB * KB {
        format!("{:.1} MB", b / (KB * KB))
    } else if b < KB * KB * KB * KB {
        format!("{:.2} GB", b / (KB * KB * KB))
    } else {
        format!("{:.2} TB", b / (KB * KB * KB * KB))
    }
}

/// Format transfer speed: `12.5 MB/s`.
pub fn fmt_speed(bps: u64) -> String {
    if bps == 0 {
        "0 B/s".into()
    } else {
        format!("{}/s", fmt_bytes(bps))
    }
}

/// Format a duration as `1h 02m 05s` / `02m 05s` / `05s`.
pub fn fmt_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h {m:02}m {s:02}s")
    } else if m > 0 {
        format!("{m:02}m {s:02}s")
    } else {
        format!("{s:02}s")
    }
}

pub fn fmt_eta(secs: Option<u64>) -> String {
    match secs {
        None => "—".into(),
        Some(s) if s > 86_400 * 7 => "—".into(),
        Some(s) => fmt_duration(s),
    }
}

/// Parse a human speed limit like `10MB`, `500K`, `1.5G`, `1048576` into bytes/sec.
pub fn parse_speed_limit(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    let (num_part, mult) = if let Some(x) = lower.strip_suffix("gb") {
        (x, 1 << 30)
    } else if let Some(x) = lower.strip_suffix("g") {
        (x, 1 << 30)
    } else if let Some(x) = lower.strip_suffix("mb") {
        (x, 1 << 20)
    } else if let Some(x) = lower.strip_suffix("m") {
        (x, 1 << 20)
    } else if let Some(x) = lower.strip_suffix("kb") {
        (x, 1 << 10)
    } else if let Some(x) = lower.strip_suffix("k") {
        (x, 1 << 10)
    } else if let Some(x) = lower.strip_suffix("b") {
        (x, 1)
    } else {
        (lower.as_str(), 1u64)
    };
    let n: f64 = num_part.trim().parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    Some((n * mult as f64) as u64)
}

/// Parse a checksum argument: accepts `sha256:<hex>` or bare 64-char hex.
pub fn normalize_sha256(s: &str) -> Option<String> {
    let hex_part = s.trim().strip_prefix("sha256:").unwrap_or(s.trim());
    let h = hex_part.to_ascii_lowercase();
    if h.len() == 64 && h.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(h)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_formatting() {
        assert_eq!(fmt_bytes(0), "0 B");
        assert_eq!(fmt_bytes(512), "512 B");
        assert_eq!(fmt_bytes(1024), "1.0 KB");
        assert_eq!(fmt_bytes(5 * 1024 * 1024), "5.0 MB");
        assert_eq!(fmt_bytes(3 * 1024u64.pow(3)), "3.00 GB");
    }

    #[test]
    fn speed_parsing() {
        assert_eq!(parse_speed_limit("10MB"), Some(10 << 20));
        assert_eq!(parse_speed_limit("500K"), Some(500 << 10));
        assert_eq!(
            parse_speed_limit("1.5G"),
            Some((1.5 * (1u64 << 30) as f64) as u64)
        );
        assert_eq!(parse_speed_limit("1048576"), Some(1 << 20));
        assert_eq!(parse_speed_limit("abc"), None);
    }

    #[test]
    fn checksum_normalization() {
        assert!(normalize_sha256(&"A".repeat(64)).is_some());
        assert!(normalize_sha256(&format!("sha256:{}", "b".repeat(64))).is_some());
        assert!(normalize_sha256("abc").is_none());
    }
}
