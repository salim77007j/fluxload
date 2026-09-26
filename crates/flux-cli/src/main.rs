//! flux — the Fluxload command-line interface.
//!
//! Subcommands:
//!   download  — segmented download with resume + integrity verification
//!   bench     — single vs multi-connection performance comparison
//!   list      — show the persisted queue
//!   doctor    — environment diagnostics
//!   update    — check for a newer release
//!   license   — activate / show license status
//!   native-host — browser extension bridge (Chrome native messaging)

use clap::{Parser, Subcommand};
use flux_core::config::EngineConfig;
use flux_core::engine::{Engine, EngineEvent};
use flux_core::format::{fmt_bytes, fmt_duration, fmt_eta, fmt_speed, parse_speed_limit};
use flux_core::task::{AddRequest, TaskSnapshot, TaskStatus};
use std::io::{Read, Write};
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(
    name = "flux",
    version,
    about = "Fluxload download engine — fast, crash-safe, hybrid HTTP + BitTorrent",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Download a file (HTTP/HTTPS/magnet)
    Download {
        /// URL to download
        url: String,
        /// Output file path
        #[arg(short, long)]
        output: Option<String>,
        /// Output directory
        #[arg(short, long)]
        dir: Option<String>,
        /// Number of connections (default from settings)
        #[arg(short = 'n', long)]
        connections: Option<u32>,
        /// Speed limit, e.g. 10MB or 500K
        #[arg(long)]
        limit: Option<String>,
        /// Expected SHA-256 (hex or sha256:hex) — verified on completion
        #[arg(long)]
        verify: Option<String>,
        /// Extra header "Name: Value" (repeatable)
        #[arg(long = "header")]
        headers: Vec<String>,
        /// Custom User-Agent
        #[arg(long)]
        user_agent: Option<String>,
        /// Stop after N seconds (test tool; leaves a resumable partial)
        #[arg(long)]
        max_time: Option<u64>,
        /// Emit JSON progress events on stdout (machine-readable)
        #[arg(long)]
        json: bool,
        /// Quiet mode
        #[arg(short, long)]
        quiet: bool,
    },
    /// Benchmark connection counts against a URL
    Bench {
        url: String,
        /// Comma-separated connection counts
        #[arg(short, long, default_value = "1,2,4,8,16")]
        connections: String,
        /// Per-run timeout seconds
        #[arg(long, default_value = "120")]
        timeout: u64,
        /// Save the report as JSON
        #[arg(long)]
        out: Option<String>,
    },
    /// Show the persisted download queue
    List,
    /// Run environment diagnostics
    Doctor,
    /// Check for a newer release
    Update {
        /// Only check, do not print instructions
        #[arg(long)]
        check: bool,
    },
    /// License management
    License {
        #[command(subcommand)]
        action: LicenseAction,
    },
    /// Browser native messaging host (stdin/stdout JSON protocol)
    NativeHost,
}

#[derive(Subcommand)]
enum LicenseAction {
    /// Show current license status
    Show,
    /// Activate with a license key
    Activate { key: String },
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let code = match cli.command {
        Command::Download {
            url,
            output,
            dir,
            connections,
            limit,
            verify,
            headers,
            user_agent,
            max_time,
            json,
            quiet,
        } => cmd_download(
            &url,
            output.as_deref(),
            dir.as_deref(),
            connections,
            limit.as_deref(),
            verify.as_deref(),
            &headers,
            user_agent.as_deref(),
            max_time,
            json,
            quiet,
        ),
        Command::Bench {
            url,
            connections,
            timeout,
            out,
        } => cmd_bench(&url, &connections, timeout, out.as_deref()),
        Command::List => cmd_list(),
        Command::Doctor => cmd_doctor(),
        Command::Update { check } => cmd_update(check),
        Command::License { action } => cmd_license(action),
        Command::NativeHost => cmd_native_host(),
    };
    std::process::exit(code);
}

fn base_config() -> EngineConfig {
    let mut cfg = EngineConfig::load_or_default();
    if let Ok(dir) = std::env::var("FLUX_DATA_DIR") {
        cfg.data_dir = dir.into();
    }
    if let Ok(dir) = std::env::var("FLUX_DOWNLOAD_DIR") {
        cfg.download_dir = dir.into();
    }
    cfg
}

fn header_args_to_pairs(raw: &[String]) -> Vec<(String, String)> {
    raw.iter()
        .filter_map(|h| {
            let (k, v) = h.split_once(':')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

fn progress_line(snap: &TaskSnapshot) -> String {
    let pct = snap
        .progress_frac()
        .map(|p| format!("{:5.1}%", p * 100.0))
        .unwrap_or_else(|| "  n/a".into());
    let size = snap.size.map(fmt_bytes).unwrap_or_else(|| "?".into());
    format!(
        "{}  {}/{}  {:>10}  ETA {}  {} conn",
        pct,
        fmt_bytes(snap.downloaded),
        size,
        fmt_speed(snap.speed_bps),
        fmt_eta(snap.eta_sec),
        snap.active_conns
    )
}

fn draw_bar(frac: f32, width: usize) -> String {
    let filled = (frac.clamp(0.0, 1.0) * width as f32) as usize;
    let mut out = String::with_capacity(width + 2);
    out.push('[');
    out.push_str(&"#".repeat(filled));
    if filled < width {
        out.push('>');
        out.push_str(&".".repeat(width - filled - 1));
    }
    out.push(']');
    out
}

#[allow(clippy::too_many_arguments)] // CLI flag surface; grouped struct would obscure usage
fn cmd_download(
    url: &str,
    output: Option<&str>,
    dir: Option<&str>,
    connections: Option<u32>,
    limit: Option<&str>,
    verify: Option<&str>,
    headers: &[String],
    user_agent: Option<&str>,
    max_time: Option<u64>,
    json: bool,
    quiet: bool,
) -> i32 {
    let mut cfg = base_config();
    if let Some(ua) = user_agent {
        cfg.user_agent = ua.to_string();
    }
    let engine = Engine::start(cfg);
    let events = engine.events();

    let req = AddRequest {
        url: url.to_string(),
        output_dir: dir.map(std::path::PathBuf::from),
        filename: output.map(|s| s.to_string()),
        connections,
        speed_limit_bps: limit.and_then(parse_speed_limit),
        checksum: verify.and_then(flux_core::format::normalize_sha256),
        headers: header_args_to_pairs(headers),
        schedule_at_unix: None,
        auto_start: true,
        origin: Some("cli".into()),
    };

    let id = match engine.add(req) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };

    let started = Instant::now();
    let mut last_line_len = 0;
    let mut final_snap: Option<TaskSnapshot> = None;

    // Ctrl+C: pause gracefully so the partial state stays resumable.
    let engine_for_signal = engine.clone();
    let signal_id = id;
    let ctrlc_installed = ctrlc_flag_setup(move || {
        let _ = engine_for_signal.pause(signal_id);
    });
    let _ = ctrlc_installed;

    let deadline = max_time.map(|t| Instant::now() + Duration::from_secs(t));
    let mut json_id_printed = false;

    loop {
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                engine.pause(id).ok();
                // Drain until paused.
                let wait_start = Instant::now();
                while wait_start.elapsed() < Duration::from_secs(10) {
                    let paused = engine
                        .snapshot()
                        .tasks
                        .iter()
                        .find(|t| t.id == id)
                        .map(|t| t.status == TaskStatus::Paused)
                        .unwrap_or(true);
                    if paused {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                final_snap = engine.snapshot().tasks.iter().find(|t| t.id == id).cloned();
                break;
            }
        }
        let timeout = Duration::from_millis(200);
        match events.recv_timeout(timeout) {
            Ok(EngineEvent::Snapshot(snap)) => {
                let Some(t) = snap.tasks.iter().find(|t| t.id == id) else {
                    continue;
                };
                if json {
                    if !json_id_printed {
                        println!(
                            "{{\"ev\":\"task\",\"id\":\"{id}\",\"url\":\"{}\"}}",
                            url.replace('\\', "\\\\").replace('"', "\\\"")
                        );
                        json_id_printed = true;
                    }
                    let out = format!(
                        "{{\"ev\":\"progress\",\"id\":\"{id}\",\"status\":\"{}\",\"downloaded\":{},\"size\":{},\"speed\":{},\"conns\":{},\"protocol\":{}}}",
                        t.status.label().to_lowercase(),
                        t.downloaded,
                        t.size.unwrap_or(0),
                        t.speed_bps,
                        t.active_conns,
                        t.protocol
                            .as_deref()
                            .map(|p| format!("\"{p}\""))
                            .unwrap_or_else(|| "null".into())
                    );
                    println!("{out}");
                } else if !quiet && t.status.is_active() {
                    let line = format!(
                        "  {} {}",
                        draw_bar(t.progress_frac().unwrap_or(0.0), 30),
                        progress_line(t)
                    );
                    eprint!("\r{}", " ".repeat(last_line_len));
                    eprint!("\r{line}");
                    last_line_len = line.len();
                    let _ = std::io::stderr().flush();
                }
            }
            Ok(EngineEvent::TaskFinished(finished, status, _msg)) if finished == id => {
                // Bounded wait for the published snapshot to agree.
                let wait_start = Instant::now();
                while wait_start.elapsed() < Duration::from_secs(10) {
                    let t = engine.snapshot().tasks.iter().find(|t| t.id == id).cloned();
                    if let Some(t) = t {
                        if t.status.is_terminal() {
                            final_snap = Some(t);
                            break;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                let _ = status;
                if final_snap.is_none() {
                    final_snap = engine.snapshot().tasks.iter().find(|t| t.id == id).cloned();
                }
                break;
            }
            Ok(_) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    engine.shutdown();
    if !quiet && !json {
        eprintln!();
    }

    let Some(snap) = final_snap else {
        eprintln!("error: task vanished");
        return 1;
    };

    match snap.status {
        TaskStatus::Done => {
            let path = snap.output_dir.join(&snap.filename);
            if json {
                println!(
                    "{{\"ev\":\"done\",\"id\":\"{id}\",\"path\":{:?},\"size\":{},\"sha256\":{},\"protocol\":{},\"elapsed_ms\":{}}}",
                    path.display().to_string(),
                    snap.size.unwrap_or(snap.downloaded),
                    snap.actual_sha256
                        .as_deref()
                        .map(|h| format!("\"{h}\""))
                        .unwrap_or_else(|| "null".into()),
                    snap.protocol
                        .as_deref()
                        .map(|p| format!("\"{p}\""))
                        .unwrap_or_else(|| "null".into()),
                    started.elapsed().as_millis()
                );
            } else {
                println!("Completed: {}", path.display());
                println!(
                    "  size: {}  sha256: {}  time: {}  avg: {}",
                    fmt_bytes(snap.downloaded),
                    snap.actual_sha256.as_deref().unwrap_or("-"),
                    fmt_duration(started.elapsed().as_secs()),
                    fmt_speed(snap.avg_bps),
                );
            }
            0
        }
        TaskStatus::Paused => {
            if json {
                println!(
                    "{{\"ev\":\"paused\",\"id\":\"{id}\",\"downloaded\":{}}}",
                    snap.downloaded
                );
            } else {
                println!(
                    "Paused at {} — rerun the same command to resume.",
                    fmt_bytes(snap.downloaded)
                );
            }
            2
        }
        TaskStatus::Failed => {
            eprintln!(
                "error: {}",
                snap.error.as_deref().unwrap_or("download failed")
            );
            1
        }
        s => {
            eprintln!("stopped with status {}", s.label());
            2
        }
    }
}

/// Sets a Ctrl+C handler that flips a flag and runs the callback once.
fn ctrlc_flag_setup(f: impl Fn() + Send + 'static) -> bool {
    // Unix: SIGINT handler thread.
    #[cfg(unix)]
    {
        use signal_hook::iterator::Signals;
        let Ok(mut signals) = Signals::new([signal_hook::consts::SIGINT]) else {
            return false;
        };
        std::thread::spawn(move || {
            if let Some(sig) = signals.forever().next() {
                if sig == signal_hook::consts::SIGINT {
                    f();
                }
            }
        });
        true
    }
    #[cfg(not(unix))]
    {
        let _ = f;
        false
    }
}

fn cmd_bench(url: &str, connections: &str, timeout: u64, out: Option<&str>) -> i32 {
    let counts: Vec<u32> = connections
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .filter(|&c| c > 0)
        .collect();
    if counts.is_empty() {
        eprintln!("error: no valid connection counts in {connections:?}");
        return 1;
    }
    println!("Fluxload benchmark against {url}");
    println!(
        "{:<12} {:>12} {:>12} {:>10}  integrity",
        "connections", "wall time", "throughput", "protocol"
    );
    let report = match flux_core::bench::run_bench(url, &counts, Duration::from_secs(timeout)) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: {e}");
            return 1;
        }
    };
    let mut ok = true;
    for row in &report.rows {
        println!(
            "{:<12} {:>12} {:>12} {:>10}  {}",
            row.connections,
            format!("{:.2}s", row.wall_ms as f64 / 1000.0),
            fmt_speed(row.throughput_bps),
            row.protocol.as_deref().unwrap_or("-"),
            if row.ok {
                row.sha256.as_deref().map(|s| &s[..16]).unwrap_or("ok")
            } else {
                "FAILED"
            },
        );
        if !row.ok {
            ok = false;
        }
    }
    if let Some(path) = out {
        match serde_json::to_vec_pretty(&report) {
            Ok(data) => {
                if let Err(e) = std::fs::write(path, data) {
                    eprintln!("warning: could not write report: {e}");
                } else {
                    println!("report saved to {path}");
                }
            }
            Err(e) => eprintln!("warning: report serialize: {e}"),
        }
    }
    if ok {
        0
    } else {
        1
    }
}

fn cmd_list() -> i32 {
    let cfg = base_config();
    let store = flux_core::store::Store::open(&cfg.data_dir);
    match store {
        Ok(store) => {
            let tasks = store.list();
            if tasks.is_empty() {
                println!("queue is empty");
                return 0;
            }
            println!(
                "{:<38} {:<10} {:<12} {:<10} url",
                "id", "status", "size", "kind"
            );
            for t in tasks {
                println!(
                    "{:<38} {:<10} {:<12} {:<10} {}",
                    t.id,
                    t.status.label(),
                    t.size.map(fmt_bytes).unwrap_or_else(|| "?".into()),
                    t.kind.label(),
                    t.url
                );
            }
            0
        }
        Err(e) => {
            eprintln!("error opening store: {e}");
            1
        }
    }
}

fn cmd_doctor() -> i32 {
    let cfg = base_config();
    println!("Fluxload {} diagnostics", flux_core::VERSION);
    println!();
    let mut diag = flux_core::doctor::run_diagnostics(&cfg);

    // Async network checks.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let net_checks = rt.block_on(flux_core::doctor::network_checks(&cfg));
    diag.checks.extend(net_checks);
    rt.shutdown_timeout(Duration::from_secs(2));

    let mut failures = 0;
    let mut warnings = 0;
    for c in &diag.checks {
        let icon = match c.status {
            flux_core::doctor::CheckStatus::Ok => "[ ok ]",
            flux_core::doctor::CheckStatus::Warn => "[warn]",
            flux_core::doctor::CheckStatus::Fail => "[fail]",
        };
        println!("{icon} {:<34} {}", c.label, c.detail);
        match c.status {
            flux_core::doctor::CheckStatus::Ok => {}
            flux_core::doctor::CheckStatus::Warn => warnings += 1,
            flux_core::doctor::CheckStatus::Fail => failures += 1,
        }
    }
    println!();
    println!(
        "engine: {} | os: {} | ram cache budget: {}",
        flux_core::VERSION,
        diag.os,
        fmt_bytes(diag.ram_cache_budget_bytes)
    );
    println!(
        "memory: {} total / {} available",
        fmt_bytes(diag.total_memory_bytes),
        fmt_bytes(diag.available_memory_bytes)
    );
    if failures > 0 {
        println!("{failures} check(s) failed");
        1
    } else if warnings > 0 {
        println!("{warnings} warning(s)");
        0
    } else {
        println!("all checks passed");
        0
    }
}

fn cmd_update(check_only: bool) -> i32 {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let result = rt.block_on(check_latest());
    rt.shutdown_timeout(Duration::from_secs(2));
    match result {
        Ok(latest) => {
            let current = flux_core::VERSION;
            let newer = match latest.as_deref() {
                Some(l) => version_gt(l.trim_start_matches('v'), current),
                None => false,
            };
            match latest {
                Some(l) if newer => {
                    println!("update available: {current} -> {l}");
                    println!("download: https://github.com/salim77007j/fluxload/releases/latest");
                    0
                }
                Some(l) => {
                    println!("up to date ({current}; latest release: {l})");
                    0
                }
                None => {
                    println!("no releases published yet (current: {current})");
                    0
                }
            }
        }
        Err(e) => {
            let _ = check_only;
            eprintln!("update check failed: {e}");
            1
        }
    }
}

async fn check_latest() -> Result<Option<String>, String> {
    let client = reqwest::Client::builder()
        .user_agent(format!("Fluxload/{} update-check", flux_core::VERSION))
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| e.to_string())?;
    // The releases/latest page redirects to the tag; avoids API rate limits.
    let resp = client
        .get("https://github.com/salim77007j/fluxload/releases/latest")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let final_url = resp.url().to_string();
    let tag = final_url.rsplit('/').next().unwrap_or("").to_string();
    if tag.starts_with('v') || tag.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        if tag.is_empty() || tag == "latest" {
            return Ok(None);
        }
        Ok(Some(tag))
    } else {
        Ok(None)
    }
}

fn version_gt(a: &str, b: &str) -> bool {
    let parse = |v: &str| -> Vec<u64> {
        v.split('-')
            .next()
            .unwrap_or(v)
            .split('.')
            .map(|s| s.parse().unwrap_or(0))
            .collect()
    };
    let (av, bv) = (parse(a), parse(b));
    for i in 0..av.len().max(bv.len()) {
        let x = av.get(i).copied().unwrap_or(0);
        let y = bv.get(i).copied().unwrap_or(0);
        if x != y {
            return x > y;
        }
    }
    false
}

fn cmd_license(action: LicenseAction) -> i32 {
    let cfg = base_config();
    match action {
        LicenseAction::Show => match flux_core::license::load_license(&cfg.data_dir) {
            Some(info) => {
                println!("licensed to: {}", info.u);
                let features = if info.f.is_empty() {
                    "-".to_string()
                } else {
                    info.f.join(", ")
                };
                println!("features:   {features}");
                let expires = match info.e {
                    Some(e) => chrono::DateTime::from_timestamp(e as i64, 0)
                        .map(|d| d.to_rfc3339())
                        .unwrap_or_else(|| e.to_string()),
                    None => "never".to_string(),
                };
                println!("expires:    {expires}");
                0
            }
            None => {
                println!("no active license (running unlicensed)");
                0
            }
        },
        LicenseAction::Activate { key } => match flux_core::license::verify_license_key(&key) {
            Ok(info) => {
                if let Err(e) = std::fs::create_dir_all(&cfg.data_dir) {
                    eprintln!("error: cannot create data dir: {e}");
                    return 1;
                }
                if let Err(e) = std::fs::write(cfg.data_dir.join("license.key"), key.trim()) {
                    eprintln!("error: cannot store license: {e}");
                    return 1;
                }
                println!(
                    "license activated: {} (features: {})",
                    info.u,
                    info.f.join(",")
                );
                0
            }
            Err(e) => {
                eprintln!("license rejected: {e}");
                1
            }
        },
    }
}

fn cmd_native_host() -> i32 {
    // Chrome native messaging: 4-byte little-endian length prefix + UTF-8 JSON.
    let cfg = base_config();
    let store = match flux_core::store::Store::open(&cfg.data_dir) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("native-host: cannot open store: {e}");
            return 1;
        }
    };

    let mut stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    loop {
        let mut len_buf = [0u8; 4];
        if stdin.read_exact(&mut len_buf).is_err() {
            break; // EOF: browser closed the port
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 || len > 16 * 1024 * 1024 {
            continue;
        }
        let mut msg = vec![0u8; len];
        if stdin.read_exact(&mut msg).is_err() {
            break;
        }
        let parsed: serde_json::Value = match serde_json::from_slice(&msg) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let url = parsed["url"].as_str().unwrap_or("").to_string();
        if url.is_empty() {
            continue;
        }
        let filename = parsed["filename"].as_str().map(|s| s.to_string());
        let mut headers: Vec<(String, String)> = Vec::new();
        if let Some(hs) = parsed["headers"].as_array() {
            for h in hs {
                if let (Some(k), Some(v)) = (h["name"].as_str(), h["value"].as_str()) {
                    headers.push((k.to_string(), v.to_string()));
                }
            }
        }
        let id = uuid::Uuid::new_v4();
        let stored = flux_core::task::StoredTask {
            id,
            url: url.clone(),
            filename: filename.unwrap_or_default(),
            output_dir: cfg.download_dir.clone(),
            kind: if flux_core::security::is_torrent_source(&url) {
                flux_core::task::TaskKind::Torrent
            } else {
                flux_core::task::TaskKind::Http
            },
            status: flux_core::task::TaskStatus::Queued,
            size: None,
            connections: None,
            speed_limit_bps: None,
            checksum: None,
            headers,
            schedule_at_unix: None,
            created_at: flux_core::store::unix_now(),
            finished_at: None,
            error: None,
            actual_sha256: None,
            checksum_ok: None,
            origin: Some("browser-extension".into()),
        };
        store.upsert(&stored);

        let reply = serde_json::json!({
            "status": "accepted",
            "id": id.to_string(),
            "url": url,
        });
        let bytes = serde_json::to_vec(&reply).unwrap_or_default();
        let _ = stdout.write_all(&(bytes.len() as u32).to_le_bytes());
        let _ = stdout.write_all(&bytes);
        let _ = stdout.flush();
    }
    0
}
