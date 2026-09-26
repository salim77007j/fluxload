//! Security hardening: URL validation, filename sanitization, redirect policy.

use crate::errors::{FluxError, Result};
use reqwest::Url;

/// Validate a download URL. Only `http`, `https`, and `magnet` (BitTorrent)
/// schemes are accepted. `file://`, `ftp://`, data URIs and exotic schemes are
/// rejected before any network activity.
pub fn parse_download_url(raw: &str) -> Result<Url> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(FluxError::InvalidUrl("empty URL".into()));
    }
    if raw.starts_with("magnet:") {
        Url::parse(raw).map_err(|e| FluxError::InvalidUrl(e.to_string()))?;
        // Re-parse leniently; magnet URLs are passed to the torrent engine.
        return Url::parse(raw).map_err(|e| FluxError::InvalidUrl(e.to_string()));
    }
    if raw.ends_with(".torrent") && !raw.contains("://") {
        // Local .torrent file path is handled by the caller (torrent engine).
        return Err(FluxError::Unsupported(
            "local .torrent files must be passed as file paths to the torrent engine".into(),
        ));
    }
    let url = Url::parse(raw).map_err(|e| FluxError::InvalidUrl(e.to_string()))?;
    match url.scheme() {
        "http" | "https" => {
            if url.host_str().is_none() {
                return Err(FluxError::InvalidUrl("URL has no host".into()));
            }
            if url.port() > Some(65535) {
                return Err(FluxError::InvalidUrl("invalid port".into()));
            }
            Ok(url)
        }
        other => Err(FluxError::InvalidUrl(format!(
            "scheme '{other}' is not supported (allowed: http, https, magnet)"
        ))),
    }
}

/// Detect a BitTorrent source (magnet link or .torrent URL).
pub fn is_torrent_source(url: &str) -> bool {
    let url = url.trim();
    url.starts_with("magnet:")
        || (url.starts_with("http") && url.split('?').next().unwrap_or("").ends_with(".torrent"))
}

/// Sanitize a candidate filename: strip path separators, control characters and
/// Windows-reserved device names; cap length. Prevents path traversal from
/// malicious `Content-Disposition` headers or crafted URLs.
pub fn sanitize_filename(candidate: Option<&str>, fallback: &str) -> String {
    let raw = candidate.unwrap_or("");
    let mut cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | '"' | '<' | '>' | '|' | '?' | '*') {
                ' '
            } else {
                c
            }
        })
        .collect();
    cleaned = cleaned.trim().trim_end_matches('.').to_string();
    // Collapse runs of whitespace.
    while cleaned.contains("  ") {
        cleaned = cleaned.replace("  ", " ");
    }
    // Remove ".." components entirely.
    while cleaned.contains("..") {
        cleaned = cleaned.replace("..", "_");
    }
    // Cap at 120 characters.
    if cleaned.chars().count() > 120 {
        cleaned = cleaned.chars().take(120).collect();
    }
    if cleaned.is_empty() {
        cleaned = fallback.to_string();
    }
    // Windows reserved device names (CON, NUL, COM1..9, LPT1..9).
    const RESERVED: [&str; 22] = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    let stem = cleaned.split('.').next().unwrap_or("").to_uppercase();
    if RESERVED.contains(&stem.as_str()) {
        cleaned = format!("_{cleaned}");
    }
    cleaned
}

/// Extract a filename hint from a URL path (only if the path does not end
/// with a directory slash).
pub fn filename_from_url(url: &Url) -> Option<String> {
    let path = url.path();
    if path.ends_with('/') {
        return None;
    }
    let last = path.rsplit('/').next().unwrap_or("");
    if last.is_empty() {
        None
    } else {
        Some(last.to_string())
    }
}

/// Percent-decode an RFC 5987 `filename*=UTF-8''...` value.
fn decode_rfc5987(value: &str) -> Option<String> {
    let encoded = value.split("''").nth(1)?;
    let mut out = Vec::with_capacity(encoded.len());
    let bytes = encoded.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
                let v = u8::from_str_radix(hex, 16).ok()?;
                out.push(v);
                i += 3;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

/// Parse a `Content-Disposition` header, preferring RFC 5987 (`filename*=`)
/// over the legacy quoted `filename=` form.
pub fn parse_content_disposition(value: &str) -> Option<String> {
    let lower = value.to_ascii_lowercase();
    // Prefer filename*=UTF-8''...
    if let Some(idx) = lower.find("filename*=") {
        let rest = &value[idx + "filename*=".len()..];
        let end = rest.find(';').map(|e| &rest[..e]).unwrap_or(rest);
        if let Some(decoded) = decode_rfc5987(end) {
            if !decoded.is_empty() {
                return Some(decoded);
            }
        }
    }
    if let Some(idx) = lower.find("filename=") {
        let rest = value[idx + "filename=".len()..].trim();
        let end = rest.find(';').map(|e| &rest[..e]).unwrap_or(rest);
        let mut name = end.trim();
        if name.starts_with('"') && name.ends_with('"') && name.len() >= 2 {
            name = &name[1..name.len() - 1];
        }
        let name = name.replace("\\\"", "\"").replace("\\\\", "\\");
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// A hardened redirect policy:
/// - caps the hop count (default 10),
/// - blocks HTTPS -> HTTP downgrades,
/// - blocks redirects to non-HTTP(S) schemes (e.g. `file://`).
pub fn redirect_policy(max_hops: usize) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        // Collect borrowed data before consuming `attempt`.
        let hops = attempt.previous().len();
        let first_scheme_https = attempt
            .previous()
            .first()
            .map(|u| u.scheme() == "https")
            .unwrap_or(false);
        let next_scheme = attempt.url().scheme().to_string();
        if hops >= max_hops {
            return attempt.error("too many redirects");
        }
        if !matches!(next_scheme.as_str(), "http" | "https") {
            return attempt.error(format!("blocked redirect to scheme '{next_scheme}'"));
        }
        if first_scheme_https && next_scheme == "http" {
            return attempt.error("blocked insecure HTTPS to HTTP downgrade redirect");
        }
        attempt.follow()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_validation() {
        assert!(parse_download_url("https://example.com/file.zip").is_ok());
        assert!(parse_download_url("http://example.com:8080/file.zip").is_ok());
        assert!(parse_download_url("magnet:?xt=urn:btih:abcdef").is_ok());
        assert!(parse_download_url("file:///etc/passwd").is_err());
        assert!(parse_download_url("ftp://example.com/file").is_err());
        assert!(parse_download_url("javascript:alert(1)").is_err());
        assert!(parse_download_url("not a url").is_err());
    }

    #[test]
    fn filename_sanitization() {
        // Path traversal attempts
        assert!(!sanitize_filename(Some("../../etc/passwd"), "x").contains(".."));
        assert!(!sanitize_filename(Some("..\\..\\windows\\system32"), "x").contains(".."));
        // Control characters and separators
        assert!(!sanitize_filename(Some("a/b\\c\u{0007}d"), "x").contains('/'));
        // Windows reserved names
        let n = sanitize_filename(Some("CON.mp4"), "x");
        assert!(n.starts_with('_'));
        // Empty falls back
        assert_eq!(sanitize_filename(Some(""), "download.bin"), "download.bin");
        assert_eq!(sanitize_filename(None, "download.bin"), "download.bin");
        // Length cap
        assert!(
            sanitize_filename(Some(&"a".repeat(500)), "x")
                .chars()
                .count()
                <= 120
        );
    }

    #[test]
    fn content_disposition_parsing() {
        assert_eq!(
            parse_content_disposition("attachment; filename=\"report.pdf\""),
            Some("report.pdf".into())
        );
        assert_eq!(
            parse_content_disposition("attachment; filename*=UTF-8''%E6%8A%A5%E5%91%8A.pdf"),
            Some("报告.pdf".into())
        );
        assert_eq!(parse_content_disposition("inline"), None);
    }

    #[test]
    fn filename_extraction_from_url() {
        let url = Url::parse("https://cdn.example.com/files/FLUX-logo.png?sig=1").unwrap();
        assert_eq!(filename_from_url(&url).as_deref(), Some("FLUX-logo.png"));
        let url = Url::parse("https://cdn.example.com/files/").unwrap();
        assert_eq!(filename_from_url(&url), None);
    }
}
