//! Diagnostics: real environment checks (disk, RAM, DNS, TCP, TLS, write
//! throughput, config sanity). Used by `flux doctor` and the GUI settings page.

use serde::Serialize;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum CheckStatus {
    Ok,
    Warn,
    Fail,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckResult {
    pub label: String,
    pub status: CheckStatus,
    pub detail: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct Diagnostics {
    pub checks: Vec<CheckResult>,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub os: String,
    pub ram_cache_budget_bytes: u64,
}

pub fn run_diagnostics(cfg: &crate::config::EngineConfig) -> Diagnostics {
    let mut checks: Vec<CheckResult> = Vec::new();

    // OS + memory
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let total_mem = sys.total_memory();
    let avail_mem = sys.available_memory();
    let os = format!("{} {}", std::env::consts::OS, std::env::consts::ARCH);
    checks.push(CheckResult {
        label: "Operating system".into(),
        status: CheckStatus::Ok,
        detail: os.clone(),
    });
    checks.push(CheckResult {
        label: "Memory".into(),
        status: if avail_mem > 512 * 1024 * 1024 {
            CheckStatus::Ok
        } else {
            CheckStatus::Warn
        },
        detail: format!(
            "{} total / {} available",
            crate::format::fmt_bytes(total_mem),
            crate::format::fmt_bytes(avail_mem)
        ),
    });
    let ram_budget = cfg.ram_cache_budget_bytes(avail_mem);
    checks.push(CheckResult {
        label: "RAM write cache budget".into(),
        status: CheckStatus::Ok,
        detail: crate::format::fmt_bytes(ram_budget).to_string(),
    });

    // Data dir + download dir writable
    for (label, dir) in [
        ("Data directory", cfg.data_dir.clone()),
        ("Download directory", cfg.download_dir.clone()),
    ] {
        let status = match probe_write(&dir) {
            Ok(bytes_per_sec) => {
                checks.push(CheckResult {
                    label: format!("{label} ({})", dir.display()),
                    status: CheckStatus::Ok,
                    detail: format!(
                        "writable, write throughput {}",
                        crate::format::fmt_speed(bytes_per_sec)
                    ),
                });
                CheckStatus::Ok
            }
            Err(e) => {
                checks.push(CheckResult {
                    label: format!("{label} ({})", dir.display()),
                    status: CheckStatus::Fail,
                    detail: format!("not writable: {e}"),
                });
                CheckStatus::Fail
            }
        };
        let _ = status;
    }

    // Disk free space
    {
        let disks = sysinfo::Disks::new_with_refreshed_list();
        let probe = cfg
            .download_dir
            .canonicalize()
            .unwrap_or(cfg.download_dir.clone());
        let mut best_free: Option<u64> = None;
        for d in disks.list() {
            if let Some(mount) = d.mount_point().to_str() {
                if probe.starts_with(mount) {
                    best_free = Some(best_free.unwrap_or(0).max(d.available_space()));
                }
            }
        }
        if let Some(free) = best_free {
            checks.push(CheckResult {
                label: "Disk space (download volume)".into(),
                status: if free > 1024 * 1024 * 1024 {
                    CheckStatus::Ok
                } else if free > 128 * 1024 * 1024 {
                    CheckStatus::Warn
                } else {
                    CheckStatus::Fail
                },
                detail: format!("{} free", crate::format::fmt_bytes(free)),
            });
        } else {
            checks.push(CheckResult {
                label: "Disk space (download volume)".into(),
                status: CheckStatus::Warn,
                detail: "unable to determine".into(),
            });
        }
    }

    // DNS
    let dns_status = "github.com:443".to_socket_addrs();
    match dns_status {
        Ok(addrs) => {
            let n = addrs.count();
            checks.push(CheckResult {
                label: "DNS resolution".into(),
                status: if n > 0 {
                    CheckStatus::Ok
                } else {
                    CheckStatus::Fail
                },
                detail: format!("github.com resolves ({n} address(es))"),
            });
        }
        Err(e) => checks.push(CheckResult {
            label: "DNS resolution".into(),
            status: CheckStatus::Fail,
            detail: format!("github.com: {e}"),
        }),
    }

    // TLS / HTTPS is verified asynchronously by `network_checks()` (called by
    // the CLI/GUI, which own a runtime). The synchronous section above covers
    // only local system checks.

    // Proxy
    match &cfg.proxy {
        Some(p) => checks.push(CheckResult {
            label: "Proxy".into(),
            status: CheckStatus::Ok,
            detail: p.url.clone(),
        }),
        None => checks.push(CheckResult {
            label: "Proxy".into(),
            status: CheckStatus::Ok,
            detail: "direct connection (no proxy configured)".into(),
        }),
    }

    Diagnostics {
        checks,
        total_memory_bytes: total_mem,
        available_memory_bytes: avail_mem,
        os,
        ram_cache_budget_bytes: ram_budget,
    }
}

/// Async network checks (TLS round-trip with latency), appended to diagnostics.
pub async fn network_checks(cfg: &crate::config::EngineConfig) -> Vec<CheckResult> {
    let mut out = Vec::new();
    let mut builder = reqwest::Client::builder()
        .user_agent(&cfg.user_agent)
        .connect_timeout(std::time::Duration::from_secs(10));
    if let Some(p) = &cfg.proxy {
        if let Ok(proxy) = reqwest::Proxy::all(p.url.as_str()) {
            builder = builder.proxy(proxy);
        }
    }
    let client = match builder.build() {
        Ok(c) => c,
        Err(e) => {
            out.push(CheckResult {
                label: "HTTPS round-trip".into(),
                status: CheckStatus::Fail,
                detail: format!("client build failed: {e}"),
            });
            return out;
        }
    };
    let t0 = Instant::now();
    match client.get("https://github.com/robots.txt").send().await {
        Ok(resp) if resp.status().is_success() => {
            out.push(CheckResult {
                label: "HTTPS round-trip".into(),
                status: CheckStatus::Ok,
                detail: format!(
                    "github.com reachable in {} ms (TLS trust roots OK)",
                    t0.elapsed().as_millis()
                ),
            });
        }
        Ok(resp) => out.push(CheckResult {
            label: "HTTPS round-trip".into(),
            status: CheckStatus::Warn,
            detail: format!("github.com returned HTTP {}", resp.status().as_u16()),
        }),
        Err(e) => out.push(CheckResult {
            label: "HTTPS round-trip".into(),
            status: CheckStatus::Fail,
            detail: format!("{e}"),
        }),
    }
    out
}

fn probe_write(dir: &PathBuf) -> std::result::Result<u64, String> {
    std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    let probe = dir.join(format!(".flux-write-probe-{}", std::process::id()));
    let data = vec![0u8; 32 * 1024 * 1024];
    let t0 = Instant::now();
    std::fs::write(&probe, &data).map_err(|e| e.to_string())?;
    let elapsed = t0.elapsed().as_secs_f64();
    std::fs::remove_file(&probe).ok();
    Ok((data.len() as f64 / elapsed.max(1e-6)) as u64)
}
