//! HTTP/3 (QUIC) test server with a self-signed certificate.
//!
//! Serves `/files/{name}` over real QUIC + HTTP/3 semantics, including
//! Range support, so multi-segment downloads over HTTP/3 can be tested
//! locally. The engine connects with `tls_insecure` (test-only mode).

use crate::ServerState;
use bytes::Bytes;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct H3TestServer {
    pub addr: SocketAddr,
    pub handle: tokio::task::JoinHandle<()>,
}

pub async fn start(state: ServerState) -> anyhow::Result<H3TestServer> {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])?;
    let cert = ck.cert.der().clone();
    let key = quinn::rustls::pki_types::PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der());

    let provider = Arc::new(quinn::rustls::crypto::ring::default_provider());
    let rustls_cfg = quinn::rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&quinn::rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key.into())?;
    let mut rustls_cfg = rustls_cfg;
    rustls_cfg.alpn_protocols = vec![b"h3".to_vec()];

    let quinn_cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(rustls_cfg)?,
    ));

    let endpoint = quinn::Endpoint::server(quinn_cfg, "127.0.0.1:0".parse()?)?;
    let addr = endpoint.local_addr()?;
    tracing::info!("h3 test server listening on {addr}");

    let handle = tokio::spawn(async move {
        while let Some(incoming) = endpoint.accept().await {
            let Ok(conn) = incoming.await else { continue };
            let state = state.clone();
            tokio::spawn(async move {
                let mut h3conn: h3::server::Connection<h3_quinn::Connection, Bytes> =
                    match h3::server::Connection::new(h3_quinn::Connection::new(conn)).await {
                        Ok(c) => c,
                        Err(_) => return,
                    };
                loop {
                    match h3conn.accept().await {
                        Ok(None) => break,
                        Ok(Some(resolver)) => {
                            let Ok((req, mut stream)) = resolver.resolve_request().await else {
                                continue;
                            };
                            if let Err(e) = serve_h3_request(state.clone(), req, &mut stream).await
                            {
                                tracing::debug!("h3 request error: {e:?}");
                            }
                        }
                        Err(_) => break,
                    }
                }
            });
        }
    });

    Ok(H3TestServer { addr, handle })
}

async fn serve_h3_request(
    state: ServerState,
    req: http::Request<()>,
    stream: &mut h3::server::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>,
) -> Result<(), h3::error::StreamError> {
    let path = req.uri().path().to_string();
    let name = path
        .strip_prefix("/files/")
        .unwrap_or_else(|| path.trim_start_matches('/'))
        .to_string();

    let Some(path_on_disk) = (if name.contains('/') || name.contains("..") {
        None
    } else {
        let p = state.root.join(&name);
        p.is_file().then_some(p)
    }) else {
        let resp = http::Response::builder().status(404).body(()).unwrap();
        stream.send_response(resp).await?;
        stream.finish().await?;
        return Ok(());
    };

    let size = std::fs::metadata(&path_on_disk)
        .map(|m| m.len())
        .unwrap_or(0);

    // Range support over H3.
    let range_header = req
        .headers()
        .get("range")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let (status, start, length) = if let Some(rh) = &range_header {
        match parse_simple_range(rh, size) {
            Some((s, e)) => (206u16, s, e - s + 1),
            None => (200u16, 0, size),
        }
    } else {
        (200u16, 0, size)
    };

    let mut builder = http::Response::builder()
        .status(status)
        .header("content-type", "application/octet-stream")
        .header("accept-ranges", "bytes")
        .header("etag", format!("\"h3-{size}\""))
        .header("content-length", length.to_string());
    if status == 206 {
        let (s, e) = (start, start + length - 1);
        builder = builder.header("content-range", format!("bytes {s}-{e}/{size}"));
    }
    let resp = builder.body(()).unwrap();
    stream.send_response(resp).await?;

    // Stream the body in 128 KB chunks.
    let mut file = match tokio::fs::File::open(&path_on_disk).await {
        Ok(f) => f,
        Err(_) => {
            stream.finish().await?;
            return Ok(());
        }
    };
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let _ = file.seek(std::io::SeekFrom::Start(start)).await;
    let mut remaining = length;
    let mut buf = vec![0u8; 128 * 1024];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = file.read(&mut buf[..want]).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        stream.send_data(Bytes::copy_from_slice(&buf[..n])).await?;
        remaining -= n as u64;
    }
    stream.finish().await?;
    Ok(())
}

fn parse_simple_range(rh: &str, size: u64) -> Option<(u64, u64)> {
    let spec = rh.strip_prefix("bytes=")?;
    let (a, b) = spec.split_once('-')?;
    let start: u64 = a.parse().ok()?;
    if start >= size {
        return None;
    }
    let end = if b.is_empty() {
        size - 1
    } else {
        b.parse::<u64>().ok()?.min(size - 1)
    };
    (end >= start).then_some((start, end))
}
