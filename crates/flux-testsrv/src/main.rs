//! flux-testsrv: standalone local download test server.
//!
//! Usage:
//!   flux-testsrv --dir <path> [--gen name:MB ...] [--port N] [--h3] [--json]
//!
//! Serves range-capable files from --dir, optional HTTP/3 on a separate port,
//! throttled endpoints, redirects and an HTTP BitTorrent tracker.

use flux_testsrv::{generate_file, start as start_http};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let mut dir: Option<PathBuf> = None;
    let mut port: Option<u16> = None;
    let mut gen: Vec<(String, u64)> = Vec::new();
    let mut want_h3 = false;
    let mut json = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => dir = args.next().map(PathBuf::from),
            "--port" => port = args.next().and_then(|p| p.parse().ok()),
            "--gen" => {
                if let Some(spec) = args.next() {
                    if let Some((name, mb)) = spec.rsplit_once(':') {
                        gen.push((name.to_string(), mb.parse().unwrap_or(1)));
                    }
                }
            }
            "--h3" => want_h3 = true,
            "--json" => json = true,
            _ => {}
        }
    }

    let dir = dir.unwrap_or_else(|| {
        let d = std::env::temp_dir().join(format!("flux-testsrv-{}", std::process::id()));
        std::fs::create_dir_all(&d).ok();
        d
    });
    std::fs::create_dir_all(&dir)?;

    for (name, mb) in gen {
        let path = dir.join(&name);
        let hash = generate_file(&path, mb * 1024 * 1024)?;
        if json {
            println!(
                "{{\"event\":\"generated\",\"file\":\"{name}\",\"size\":{},\"sha256\":\"{hash}\"}}",
                mb * 1024 * 1024
            );
        } else {
            println!("generated {name} ({} MB) sha256={hash}", mb);
        }
    }

    let _ = port; // informational only; the server always reports its actual port
    let server = start_http(dir.clone()).await?;

    let mut h3_addr = None;
    if want_h3 {
        #[cfg(feature = "h3")]
        {
            let (etags, sizes) = index(&dir);
            let state = flux_testsrv::ServerState {
                root: dir.clone(),
                etags: std::sync::Arc::new(etags),
                sizes: std::sync::Arc::new(sizes),
                tracker: Default::default(),
            };
            let h3 = flux_testsrv::h3_server::start(state).await?;
            h3_addr = Some(h3.addr);
        }
    }

    if json {
        println!(
            "{{\"event\":\"listening\",\"addr\":\"{}\",\"h3\":{}}}",
            server.addr,
            h3_addr
                .map(|a| format!("\"{a}\""))
                .unwrap_or_else(|| "null".into())
        );
    } else {
        println!("http listening on {}", server.addr);
        if let Some(a) = h3_addr {
            println!("h3   listening on {a}");
        }
    }
    std::io::Write::flush(&mut std::io::stdout()).ok();

    // Serve until killed.
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn index(
    dir: &std::path::Path,
) -> (
    std::collections::HashMap<String, String>,
    std::collections::HashMap<String, u64>,
) {
    let mut etags = std::collections::HashMap::new();
    let mut sizes = std::collections::HashMap::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_file() {
                if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                    sizes.insert(
                        name.to_string(),
                        std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0),
                    );
                    if let Ok(b) = std::fs::read(&p) {
                        use sha2::Digest;
                        let mut h = sha2::Sha256::new();
                        h.update(&b);
                        etags.insert(name.to_string(), flux_testsrv::hex_hash(h.finalize()));
                    }
                }
            }
        }
    }
    (etags, sizes)
}
