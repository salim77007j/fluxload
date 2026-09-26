//! Server probe: discover size, range support, validators, filename and
//! negotiated protocol (HTTP/1.1, HTTP/2, HTTP/3) before starting workers.

use crate::errors::{FluxError, Result};
use crate::security;
use reqwest::{header, Client, StatusCode, Url};

#[derive(Clone, Debug)]
pub struct ProbeInfo {
    /// Final URL after redirects.
    pub final_url: Url,
    pub size: Option<u64>,
    pub accept_ranges: bool,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub filename_hint: Option<String>,
    /// "HTTP/1.1" | "HTTP/2" | "HTTP/3"
    pub protocol: String,
    pub content_type: Option<String>,
}

fn version_label(v: reqwest::Version) -> String {
    match v {
        reqwest::Version::HTTP_09 => "HTTP/0.9".into(),
        reqwest::Version::HTTP_10 => "HTTP/1.0".into(),
        reqwest::Version::HTTP_11 => "HTTP/1.1".into(),
        reqwest::Version::HTTP_2 => "HTTP/2".into(),
        reqwest::Version::HTTP_3 => "HTTP/3".into(),
        _ => "HTTP".into(),
    }
}

/// Parse `bytes 0-0/12345` (or `bytes 0-0/*`) into the total size.
fn parse_total_from_content_range(v: &str) -> Option<u64> {
    let total = v.rsplit('/').next()?.trim();
    if total == "*" {
        return None;
    }
    total.parse().ok()
}

/// Probe a server with a 1-byte range request. A 206 response proves range
/// support and reveals the total size; a 200 means single-stream mode.
/// When `h3` is true the request is routed through the client's HTTP/3 (QUIC)
/// pool (reqwest requires per-request `HTTP_3` version marking).
pub async fn probe(
    client: &Client,
    url: &Url,
    extra_headers: &[(String, String)],
    h3: bool,
) -> Result<ProbeInfo> {
    let mut req = client.get(url.clone()).header(header::RANGE, "bytes=0-0");
    if h3 {
        req = req.version(reqwest::Version::HTTP_3);
    }
    for (k, v) in extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = req.send().await.map_err(|e| {
        FluxError::Network(format!("probe {}: {e}", url.host_str().unwrap_or("host")))
    })?;

    let status = resp.status();
    let final_url = resp.url().clone();
    let version = resp.version();
    let headers = resp.headers().clone();

    // Drain the 0-0 body so the connection can be reused.
    let _ = resp.bytes().await;

    match status {
        StatusCode::OK | StatusCode::PARTIAL_CONTENT => {}
        StatusCode::NOT_FOUND => {
            return Err(FluxError::HttpStatus {
                status: 404,
                context: url.to_string(),
            })
        }
        s if s.is_server_error() => {
            return Err(FluxError::HttpStatus {
                status: s.as_u16(),
                context: url.to_string(),
            })
        }
        s if s.is_client_error() => {
            return Err(FluxError::HttpStatus {
                status: s.as_u16(),
                context: format!("{} (authentication required?)", url),
            })
        }
        _ => {}
    }

    let accept_ranges = status == StatusCode::PARTIAL_CONTENT
        || headers
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_ascii_lowercase().contains("bytes"))
            .unwrap_or(false);

    let size = if status == StatusCode::PARTIAL_CONTENT {
        headers
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_total_from_content_range)
    } else {
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
    };

    // Strong etags only; weak validators cannot drive If-Range safely.
    let etag = headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty() && !s.starts_with("W/"));

    let last_modified = headers
        .get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string());

    let filename_hint = headers
        .get(header::CONTENT_DISPOSITION)
        .and_then(|v| v.to_str().ok())
        .and_then(security::parse_content_disposition)
        .or_else(|| security::filename_from_url(&final_url));

    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Ok(ProbeInfo {
        final_url,
        size,
        accept_ranges,
        etag,
        last_modified,
        filename_hint,
        protocol: version_label(version),
        content_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_range_parsing() {
        assert_eq!(
            parse_total_from_content_range("bytes 0-0/12345"),
            Some(12345)
        );
        assert_eq!(parse_total_from_content_range("bytes 0-99/*"), None);
        assert_eq!(parse_total_from_content_range("garbage"), None);
    }
}
