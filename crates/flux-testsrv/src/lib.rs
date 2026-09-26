//! Local test server for Fluxload integration tests.
//!
//! Real behaviors (no mocks):
//! - HTTP/1.1 range requests with strong ETags and If-Range semantics
//! - per-connection bandwidth throttling
//! - a no-range endpoint (single-stream mode)
//! - redirects (including loops and non-HTTP schemes)
//! - an HTTP BitTorrent tracker (compact bencode peers)
//! - [feature `h3`] an HTTP/3 (QUIC) server with a self-signed cert

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Clone)]
pub struct ServerState {
    pub root: PathBuf,
    /// filename -> strong etag (sha256 hex)
    pub etags: Arc<HashMap<String, String>>,
    /// filename -> total size
    pub sizes: Arc<HashMap<String, u64>>,
    pub tracker: Arc<Tracker>,
}

#[derive(Default)]
pub struct Tracker {
    /// info_hash -> peers
    pub peers: Mutex<HashMap<Vec<u8>, Vec<TrackerPeer>>>,
}

#[derive(Clone, Debug)]
pub struct TrackerPeer {
    pub peer_id: Vec<u8>,
    pub ip: [u8; 4],
    pub port: u16,
}

pub struct TestServer {
    pub addr: SocketAddr,
    pub handle: tokio::task::JoinHandle<()>,
}

/// Start the plain-HTTP test server on an ephemeral port.
pub async fn start(root: PathBuf) -> anyhow::Result<TestServer> {
    let (etags, sizes) = index_dir(&root);
    let state = ServerState {
        root,
        etags: Arc::new(etags),
        sizes: Arc::new(sizes),
        tracker: Arc::new(Tracker::default()),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Ok(TestServer { addr, handle })
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/files/{name}", get(file_handler).head(file_handler))
        .route("/throttle/{kbps}/{name}", get(throttle_handler))
        .route("/norange/{name}", get(norange_handler))
        .route("/auth/{token}/{name}", get(auth_handler))
        .route("/redirect", get(redirect_handler))
        .route("/announce", get(announce_handler))
        .route("/garbage", get(|| async { StatusCode::NOT_FOUND }))
        .with_state(state)
}

fn index_dir(root: &FsPath) -> (HashMap<String, String>, HashMap<String, u64>) {
    let mut etags = HashMap::new();
    let mut sizes = HashMap::new();
    if let Ok(rd) = std::fs::read_dir(root) {
        for entry in rd.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    let meta = std::fs::metadata(&path).ok();
                    let size = meta.map(|m| m.len()).unwrap_or(0);
                    sizes.insert(name.to_string(), size);
                    if let Ok(bytes) = std::fs::read(&path) {
                        use sha2::Digest;
                        let mut h = sha2::Sha256::new();
                        h.update(&bytes);
                        etags.insert(name.to_string(), hex::format_hex(h.finalize()));
                    }
                }
            }
        }
    }
    (etags, sizes)
}

fn resolve(state: &ServerState, name: &str) -> Option<PathBuf> {
    // Path traversal protection: reject any separators.
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return None;
    }
    let p = state.root.join(name);
    p.is_file().then_some(p)
}

#[derive(serde::Deserialize, Default)]
pub struct RangeQuery {
    /// Force ignoring the Range header (for testing).
    #[serde(default)]
    pub ignore_range: Option<bool>,
}

async fn file_handler(
    State(state): State<ServerState>,
    Path(name): Path<String>,
    Query(q): Query<RangeQuery>,
    req_method: Method,
    uri: Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    serve_file(state, name, q, req_method, uri, headers, None).await
}

async fn throttle_handler(
    State(state): State<ServerState>,
    Path((kbps, name)): Path<(u32, String)>,
    req_method: Method,
    uri: Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    serve_file(
        state,
        name,
        RangeQuery::default(),
        req_method,
        uri,
        headers,
        Some(kbps),
    )
    .await
}

async fn norange_handler(
    State(state): State<ServerState>,
    Path(name): Path<String>,
    req_method: Method,
    uri: Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    serve_file(
        state,
        name,
        RangeQuery {
            ignore_range: Some(true),
        },
        req_method,
        uri,
        headers,
        None,
    )
    .await
}

async fn auth_handler(
    State(state): State<ServerState>,
    Path((token, name)): Path<(String, String)>,
    req_method: Method,
    uri: Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    let provided = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match provided {
        Some(t) if t == token => {
            serve_file(
                state,
                name,
                RangeQuery::default(),
                req_method,
                uri,
                headers,
                None,
            )
            .await
        }
        _ => (StatusCode::UNAUTHORIZED, "bad token").into_response(),
    }
}

#[derive(serde::Deserialize)]
pub struct RedirectQuery {
    pub to: String,
    #[serde(default)]
    pub code: Option<u16>,
}

async fn redirect_handler(Query(q): Query<RedirectQuery>) -> Response {
    let status = match q.code.unwrap_or(302) {
        301 => StatusCode::MOVED_PERMANENTLY,
        307 => StatusCode::TEMPORARY_REDIRECT,
        _ => StatusCode::FOUND,
    };
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = status;
    if let Ok(v) = HeaderValue::from_str(&q.to) {
        resp.headers_mut().insert(header::LOCATION, v);
    }
    resp
}

/// Core file serving with real Range + If-Range semantics.
async fn serve_file(
    state: ServerState,
    name: String,
    q: RangeQuery,
    method: Method,
    _uri: Uri,
    headers: axum::http::HeaderMap,
    throttle_kbps: Option<u32>,
) -> Response {
    let Some(path) = resolve(&state, &name) else {
        return (StatusCode::NOT_FOUND, "no such file").into_response();
    };
    let size = match std::fs::metadata(&path) {
        Ok(m) => m.len(),
        Err(_) => return (StatusCode::NOT_FOUND, "metadata").into_response(),
    };
    let etag = state
        .etags
        .get(&name)
        .cloned()
        .unwrap_or_else(|| format!("\"size-{size}\""));

    let ignore_range = q.ignore_range.unwrap_or(false);
    let range_header = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let if_range = headers
        .get("if-range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string());

    // If-Range: only honor Range when the validator still matches.
    let range_valid = if let (Some(ir), Some(_rh)) = (&if_range, &range_header) {
        ir == &etag || ir == &size.to_string()
    } else {
        true
    };

    let mut range: Option<(u64, u64)> = None;
    if !ignore_range && range_valid {
        if let Some(rh) = &range_header {
            range = parse_range(rh, size);
        }
    }

    let (status, (start, end)) = match (range_header.is_some() && !ignore_range, range, range_valid)
    {
        (true, Some(r), true) => (StatusCode::PARTIAL_CONTENT, r),
        // Content changed or ranges unsupported: 200 with the full body.
        _ => (StatusCode::OK, (0, size.saturating_sub(1))),
    };

    let length = if size == 0 { 0 } else { end - start + 1 };
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(
            header::ACCEPT_RANGES,
            if ignore_range { "none" } else { "bytes" },
        )
        .header(header::ETAG, etag.clone())
        .header(header::LAST_MODIFIED, "Mon, 01 Jan 2024 00:00:00 GMT")
        .header(header::CONTENT_LENGTH, length.to_string());
    if status == StatusCode::PARTIAL_CONTENT {
        builder = builder.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{size}"));
    }

    if method == Method::HEAD {
        return builder.body(Body::empty()).unwrap();
    }

    let path_inner = path.clone();
    let stream = async_stream_body(path_inner, start, length, throttle_kbps);
    builder.body(Body::from_stream(stream)).unwrap()
}

fn parse_range(rh: &str, size: u64) -> Option<(u64, u64)> {
    let spec = rh.strip_prefix("bytes=")?;
    if spec.contains(',') {
        // Multiple ranges: serve only the first.
        let first = spec.split(',').next()?;
        return parse_one(first, size);
    }
    parse_one(spec, size)
}

fn parse_one(spec: &str, size: u64) -> Option<(u64, u64)> {
    let spec = spec.trim();
    if let Some((a, b)) = spec.split_once('-') {
        if a.is_empty() {
            // suffix: last N bytes
            let n: u64 = b.parse().ok()?;
            let len = n.min(size);
            if len == 0 {
                return None;
            }
            return Some((size - len, size - 1));
        }
        let start: u64 = a.parse().ok()?;
        if start >= size {
            return None;
        }
        let end = if b.is_empty() {
            size - 1
        } else {
            let e: u64 = b.parse().ok()?;
            e.min(size - 1)
        };
        if end < start {
            return None;
        }
        return Some((start, end));
    }
    None
}

fn async_stream_body(
    path: PathBuf,
    start: u64,
    length: u64,
    throttle_kbps: Option<u32>,
) -> impl futures_util::Stream<Item = Result<bytes::Bytes, std::io::Error>> {
    let chunk = 128 * 1024;
    let delay_per_chunk = throttle_kbps.map(|kbps| {
        let rate = (kbps as u64).max(1) * 1024;
        std::time::Duration::from_micros((chunk as u64) * 1_000_000 / rate)
    });
    futures_util::stream::unfold(
        (path, start, length, delay_per_chunk, 0u64),
        move |(path, pos, remaining, delay, emitted)| async move {
            if remaining == 0 {
                return None;
            }
            if let Some(d) = delay {
                if emitted > 0 {
                    tokio::time::sleep(d).await;
                }
            }
            let mut file = tokio::fs::File::open(&path).await.ok()?;
            file.seek(std::io::SeekFrom::Start(pos)).await.ok()?;
            let want = chunk.min(remaining as usize);
            let mut buf = vec![0u8; want];
            let n = file.read(&mut buf).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.truncate(n);
            let item = Ok(bytes::Bytes::from(buf));
            Some((
                item,
                (
                    path,
                    pos + n as u64,
                    remaining - n as u64,
                    delay,
                    emitted + 1,
                ),
            ))
        },
    )
}

// ---------------- HTTP BitTorrent tracker ----------------

fn percent_decode(s: &str) -> Vec<u8> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(
                std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("zz"),
                16,
            ) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == key).then(|| v.to_string())
    })
}

async fn announce_handler(
    State(state): State<ServerState>,
    uri: Uri,
    headers: axum::http::HeaderMap,
) -> Response {
    let query = uri.query().unwrap_or("");
    let Some(info_hash_raw) = query_param(query, "info_hash") else {
        return (StatusCode::BAD_REQUEST, "missing info_hash").into_response();
    };
    let info_hash = percent_decode(&info_hash_raw.replace('+', "%20"));
    if info_hash.len() != 20 {
        return (StatusCode::BAD_REQUEST, "info_hash must be 20 bytes").into_response();
    }
    let peer_id = query_param(query, "peer_id")
        .map(|p| percent_decode(&p.replace('+', "%20")))
        .unwrap_or_default();
    let port: u16 = query_param(query, "port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    let ip: [u8; 4] = headers
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.split(',').next().map(|s| s.trim().to_string()))
        .and_then(|s| s.parse::<std::net::Ipv4Addr>().ok())
        .map(|a| a.octets())
        .unwrap_or([127, 0, 0, 1]);

    let self_id = peer_id.clone();
    let peer = TrackerPeer { peer_id, ip, port };
    {
        let mut peers = state.tracker.peers.lock().expect("tracker");
        let entry = peers.entry(info_hash.clone()).or_default();
        entry.retain(|p| p.peer_id != self_id);
        if port > 0 {
            entry.push(peer);
        }
    }

    // Compact peers response (everyone else with the same info hash).
    let mut compact: Vec<u8> = Vec::new();
    {
        let peers = state.tracker.peers.lock().expect("tracker");
        if let Some(list) = peers.get(&info_hash) {
            for p in list {
                if p.port == 0 || p.peer_id == self_id {
                    continue;
                }
                compact.extend_from_slice(&p.ip);
                compact.extend_from_slice(&p.port.to_be_bytes());
            }
        }
    }

    // bencode: d8:completei1e10:incompletei1e8:intervali1e5:peersN:...e
    let mut body = Vec::new();
    body.extend_from_slice(b"d8:completei1e10:incompletei1e8:intervali1e5:peers");
    body.extend_from_slice(format!("{}:", compact.len()).as_bytes());
    body.extend_from_slice(&compact);
    body.push(b'e');

    let mut resp = Response::new(Body::from(body));
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
    resp
}

#[cfg(feature = "h3")]
pub mod h3_server;

/// Generate a deterministic-but-random test file with a known sha256.
pub fn generate_file(path: &FsPath, size: u64) -> std::io::Result<String> {
    use sha2::Digest;
    use std::io::Write;
    // Deterministic PRNG (xorshift) so hashes are reproducible per size.
    let mut state: u64 = size | 1;
    let mut buf = vec![0u8; 1 << 20];
    let mut hasher = sha2::Sha256::new();
    let mut f = std::fs::File::create(path)?;
    let mut remaining = size;
    while remaining > 0 {
        let n = remaining.min(buf.len() as u64) as usize;
        for b in buf[..n].iter_mut() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *b = (state >> 24) as u8;
        }
        f.write_all(&buf[..n])?;
        hasher.update(&buf[..n]);
        remaining -= n as u64;
    }
    f.sync_all()?;
    Ok(hex::format_hex(hasher.finalize()))
}

/// Hex-encode a digest into a lowercase string.
pub fn hex_hash(bytes: impl AsRef<[u8]>) -> String {
    let mut s = String::with_capacity(bytes.as_ref().len() * 2);
    for b in bytes.as_ref() {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

#[allow(dead_code)]
mod hex {
    pub fn format_hex(bytes: impl AsRef<[u8]>) -> String {
        let mut s = String::with_capacity(bytes.as_ref().len() * 2);
        for b in bytes.as_ref() {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}
