//! Settings window: every control maps to a real EngineConfig field.

use crate::theme::{self, Palette};
use egui::Ui;
use flux_core::config::{CacheMode, EngineConfig, ProxyConfig, RateWindow};
use flux_core::format::parse_speed_limit;

pub struct SettingsState {
    pub cfg: EngineConfig,
    /// Dirty flag: apply button enabled.
    pub dirty: bool,
    pub global_limit_text: String,
    pub per_task_limit_text: String,
    pub proxy_url: String,
    pub proxy_user: String,
    pub proxy_pass: String,
    pub manual_cache_mb: u64,
    pub auto_fraction: f32,
    pub diagnostics: Option<String>,
    pub diag_running: bool,
}

impl SettingsState {
    pub fn from_cfg(cfg: &EngineConfig) -> Self {
        Self {
            global_limit_text: cfg
                .global_speed_limit_bps
                .map(flux_core::format::fmt_bytes)
                .unwrap_or_default(),
            per_task_limit_text: cfg
                .per_task_limit_default_bps
                .map(flux_core::format::fmt_bytes)
                .unwrap_or_default(),
            proxy_url: cfg
                .proxy
                .as_ref()
                .map(|p| p.url.clone())
                .unwrap_or_default(),
            proxy_user: cfg
                .proxy
                .as_ref()
                .and_then(|p| p.username.clone())
                .unwrap_or_default(),
            proxy_pass: cfg
                .proxy
                .as_ref()
                .and_then(|p| p.password.clone())
                .unwrap_or_default(),
            manual_cache_mb: match cfg.ram_cache.mode {
                CacheMode::Manual { budget_mb } => budget_mb,
                CacheMode::Auto => 128,
            },
            auto_fraction: cfg.ram_cache.auto_fraction,
            diagnostics: None,
            diag_running: false,
            cfg: cfg.clone(),
            dirty: false,
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Section {
    General,
    Engine,
    Network,
    Bandwidth,
    Privacy,
    Diagnostics,
}

pub fn settings_window(
    ui: &mut Ui,
    st: &mut SettingsState,
    section: &mut Section,
    theme_mode: &mut crate::theme::ThemeMode,
    p: Palette,
    on_apply: &mut dyn FnMut(EngineConfig, crate::theme::ThemeMode),
    on_run_diagnostics: &mut dyn FnMut(),
) {
    ui.set_min_width(520.0);
    ui.heading(
        egui::RichText::new("Settings")
            .font(theme::font_h1())
            .color(p.text)
            .strong(),
    );
    ui.add_space(8.0);

    ui.horizontal(|ui| {
        ui.selectable_value(section, Section::General, "General");
        ui.selectable_value(section, Section::Engine, "Engine");
        ui.selectable_value(section, Section::Network, "Network");
        ui.selectable_value(section, Section::Bandwidth, "Bandwidth");
        ui.selectable_value(section, Section::Privacy, "Privacy");
        ui.selectable_value(section, Section::Diagnostics, "Diagnostics");
    });
    ui.separator();

    let cfg = &mut st.cfg;
    match section {
        Section::General => {
            ui.label(section_title("Appearance", p));
            ui.horizontal(|ui| {
                ui.label("Theme");
                if ui
                    .selectable_value(theme_mode, crate::theme::ThemeMode::Light, "Light")
                    .clicked()
                {
                    st.dirty = true;
                }
                if ui
                    .selectable_value(theme_mode, crate::theme::ThemeMode::Dark, "Dark")
                    .clicked()
                {
                    st.dirty = true;
                }
            });
            ui.add_space(6.0);
            ui.label(section_title("Downloads", p));
            ui.horizontal(|ui| {
                ui.label("Save to");
                let mut dir = cfg.download_dir.display().to_string();
                if ui.text_edit_singleline(&mut dir).changed() {
                    cfg.download_dir = dir.into();
                    st.dirty = true;
                }
            });
            ui.checkbox(
                &mut cfg.auto_resume_on_start,
                "Resume incomplete tasks on startup",
            )
            .changed()
            .then(|| st.dirty = true);
        }
        Section::Engine => {
            ui.label(section_title("Connections", p));
            drag_u32(
                ui,
                "Default connections per task",
                &mut cfg.default_connections,
                1,
                64,
            );
            drag_u32(
                ui,
                "Max connections per task",
                &mut cfg.max_connections_per_task,
                1,
                128,
            );
            let mut tasks = cfg.max_active_tasks as u32;
            if drag_u32(ui, "Max simultaneous tasks", &mut tasks, 1, 16) {
                cfg.max_active_tasks = tasks as usize;
                st.dirty = true;
            }
            drag_u64(ui, "Min segment size (MB)", &mut cfg.min_segment_mb, 1, 256);
            ui.add_space(6.0);
            ui.label(section_title("Reliability", p));
            drag_u32(
                ui,
                "Max retries per segment",
                &mut cfg.max_retries_per_segment,
                1,
                100,
            );
            drag_u64(
                ui,
                "Retry backoff base (ms)",
                &mut cfg.retry_backoff_ms,
                50,
                10_000,
            );
            drag_u64(
                ui,
                "Connect timeout (s)",
                &mut cfg.connect_timeout_sec,
                3,
                300,
            );
            drag_u64(ui, "Read timeout (s)", &mut cfg.read_timeout_sec, 10, 600);
            ui.checkbox(
                &mut cfg.verify_sha256_on_finish,
                "Always compute SHA-256 on completion",
            )
            .changed()
            .then(|| st.dirty = true);
            ui.add_space(6.0);
            ui.label(section_title("RAM cache (write coalescing)", p));
            ui.horizontal(|ui| {
                ui.label("Mode");
                if ui
                    .selectable_value(&mut cfg.ram_cache.mode, CacheMode::Auto, "Adaptive (auto)")
                    .clicked()
                {
                    st.dirty = true;
                }
                if ui
                    .selectable_value(
                        &mut cfg.ram_cache.mode,
                        CacheMode::Manual {
                            budget_mb: st.manual_cache_mb,
                        },
                        "Manual",
                    )
                    .clicked()
                {
                    st.dirty = true;
                }
            });
            match cfg.ram_cache.mode {
                CacheMode::Auto => {
                    ui.add(
                        egui::Slider::new(&mut st.auto_fraction, 0.05..=0.5)
                            .text("Fraction of available RAM"),
                    );
                    if cfg.ram_cache.auto_fraction != st.auto_fraction {
                        cfg.ram_cache.auto_fraction = st.auto_fraction;
                        st.dirty = true;
                    }
                    ui.label(
                        egui::RichText::new(format!(
                            "current budget: {} (limits: {} – {})",
                            flux_core::format::fmt_bytes(0),
                            cfg.ram_cache.auto_min_mb,
                            cfg.ram_cache.auto_max_mb
                        ))
                        .font(theme::font_small())
                        .color(p.sub),
                    );
                }
                CacheMode::Manual { budget_mb } => {
                    if drag_u64(ui, "Cache budget (MB)", &mut st.manual_cache_mb, 16, 4096) {
                        cfg.ram_cache.mode = CacheMode::Manual {
                            budget_mb: st.manual_cache_mb,
                        };
                        st.dirty = true;
                    }
                    let _ = budget_mb;
                }
            }
        }
        Section::Network => {
            ui.label(section_title("Transport", p));
            ui.checkbox(
                &mut cfg.http3_enabled,
                "HTTP/3 (QUIC) with automatic fallback",
            )
            .changed()
            .then(|| st.dirty = true);
            ui.checkbox(
                &mut cfg.torrent_enabled,
                "BitTorrent transfers (magnet / .torrent)",
            )
            .changed()
            .then(|| st.dirty = true);
            ui.add_space(6.0);
            ui.label(section_title("Client", p));
            ui.horizontal(|ui| {
                ui.label("User agent");
                if ui.text_edit_singleline(&mut cfg.user_agent).changed() {
                    st.dirty = true;
                }
            });
            ui.add_space(6.0);
            ui.label(section_title("Proxy", p));
            ui.horizontal(|ui| {
                ui.label("URL");
                if ui.text_edit_singleline(&mut st.proxy_url).changed() {
                    st.dirty = true;
                }
            });
            ui.horizontal(|ui| {
                ui.label("User");
                if ui.text_edit_singleline(&mut st.proxy_user).changed() {
                    st.dirty = true;
                }
            });
            ui.horizontal(|ui| {
                ui.label("Pass");
                if ui.text_edit_singleline(&mut st.proxy_pass).changed() {
                    st.dirty = true;
                }
            });
            ui.label(
                egui::RichText::new("Supports http://, https:// and socks5:// proxies.")
                    .font(theme::font_small())
                    .color(p.sub),
            );
        }
        Section::Bandwidth => {
            ui.label(section_title("Limits", p));
            ui.horizontal(|ui| {
                ui.label("Global limit");
                if ui.text_edit_singleline(&mut st.global_limit_text).changed() {
                    st.dirty = true;
                }
                ui.label("(empty = unlimited, e.g. 10MB)");
            });
            ui.horizontal(|ui| {
                ui.label("Default per-task");
                if ui
                    .text_edit_singleline(&mut st.per_task_limit_text)
                    .changed()
                {
                    st.dirty = true;
                }
                ui.label("(empty = unlimited)");
            });
            ui.add_space(6.0);
            ui.label(section_title("Schedule (local time)", p));
            let mut remove: Option<usize> = None;
            for (i, w) in cfg.schedule.iter_mut().enumerate() {
                ui.horizontal(|ui| {
                    let mut start = w.start.clone();
                    let mut end = w.end.clone();
                    let mut limit = w
                        .limit_bps
                        .map(flux_core::format::fmt_bytes)
                        .unwrap_or_default();
                    ui.label("from");
                    ui.text_edit_singleline(&mut start);
                    ui.label("to");
                    ui.text_edit_singleline(&mut end);
                    ui.label("limit");
                    ui.text_edit_singleline(&mut limit);
                    if ui.button("✕").clicked() {
                        remove = Some(i);
                    }
                    w.start = start;
                    w.end = end;
                    w.limit_bps = parse_speed_limit(&limit);
                    st.dirty = true;
                });
            }
            if let Some(i) = remove {
                cfg.schedule.remove(i);
                st.dirty = true;
            }
            if ui.button("+ Add window").clicked() {
                cfg.schedule.push(RateWindow {
                    start: "22:00".into(),
                    end: "06:00".into(),
                    limit_bps: None,
                });
                st.dirty = true;
            }
            ui.label(
                egui::RichText::new("During a window the global limit is replaced by the window limit (empty = unlimited).")
                    .font(theme::font_small())
                    .color(p.sub),
            );
        }
        Section::Privacy => {
            ui.label(section_title("Privacy", p));
            ui.label(
                egui::RichText::new(
                    "Fluxload collects nothing. No telemetry, no analytics, no crash uploads.\n\
                     Update checks contact github.com only when you run them explicitly.",
                )
                .font(theme::font_body())
                .color(p.sub),
            );
            ui.add_space(6.0);
            ui.label(section_title("License", p));
            if let Some(lic) = crate::app::license_summary() {
                ui.label(egui::RichText::new(format!("Licensed to: {}", lic.u)).color(p.text));
                if let Some(e) = lic.e {
                    let dt = chrono::DateTime::from_timestamp(e as i64, 0)
                        .map(|d| d.to_rfc3339())
                        .unwrap_or_else(|| e.to_string());
                    ui.label(egui::RichText::new(format!("Expires: {dt}")).color(p.sub));
                }
            } else {
                ui.label(
                    egui::RichText::new(
                        "Unlicensed (development build). Use `flux license activate <key>`.",
                    )
                    .color(p.warn),
                );
            }
        }
        Section::Diagnostics => {
            ui.label(section_title("Environment checks", p));
            if ui
                .button(if st.diag_running {
                    "Running…"
                } else {
                    "Run diagnostics"
                })
                .clicked()
                && !st.diag_running
            {
                st.diag_running = true;
                on_run_diagnostics();
            }
            if let Some(report) = &st.diagnostics {
                egui::ScrollArea::vertical().show(ui, |ui| {
                    ui.label(
                        egui::RichText::new(report)
                            .font(egui::FontId::monospace(12.0))
                            .color(p.text),
                    );
                });
            }
            ui.add_space(6.0);
            ui.label(section_title("Logs", p));
            if ui.button("Open data folder").clicked() {
                let dir = cfg.data_dir.clone();
                open_path(&dir);
            }
            ui.label(
                egui::RichText::new(format!("data dir: {}", cfg.data_dir.display()))
                    .font(theme::font_small())
                    .color(p.sub),
            );
        }
    }

    ui.add_space(10.0);
    ui.separator();
    ui.horizontal(|ui| {
        if ui
            .add_enabled(st.dirty, egui::Button::new("Apply & save"))
            .clicked()
        {
            // Parse limit text fields.
            cfg.global_speed_limit_bps = parse_speed_limit(&st.global_limit_text);
            cfg.per_task_limit_default_bps = parse_speed_limit(&st.per_task_limit_text);
            if st.proxy_url.trim().is_empty() {
                cfg.proxy = None;
            } else {
                cfg.proxy = Some(ProxyConfig {
                    url: st.proxy_url.trim().to_string(),
                    username: if st.proxy_user.is_empty() {
                        None
                    } else {
                        Some(st.proxy_user.clone())
                    },
                    password: if st.proxy_pass.is_empty() {
                        None
                    } else {
                        Some(st.proxy_pass.clone())
                    },
                });
            }
            on_apply(cfg.clone(), *theme_mode);
            st.dirty = false;
        }
        ui.label(
            egui::RichText::new(if st.dirty {
                "unsaved changes"
            } else {
                "all saved"
            })
            .font(theme::font_small())
            .color(p.sub),
        );
    });
}

fn section_title(text: &str, p: Palette) -> egui::RichText {
    egui::RichText::new(text.to_uppercase())
        .font(theme::font_small())
        .color(p.sub)
        .strong()
}

fn drag_u32(ui: &mut Ui, label: &str, value: &mut u32, min: u32, max: u32) -> bool {
    let mut changed = false;
    ui.horizontal(|ui| {
        ui.label(label);
        if ui
            .add(egui::DragValue::new(value).range(min..=max).speed(0.5))
            .changed()
        {
            changed = true;
        }
    });
    changed
}

fn drag_u64(ui: &mut Ui, label: &str, value: &mut u64, min: u64, max: u64) -> bool {
    let mut changed = false;
    ui.horizontal(|ui| {
        ui.label(label);
        if ui
            .add(egui::DragValue::new(value).range(min..=max).speed(0.5))
            .changed()
        {
            changed = true;
        }
    });
    changed
}

pub fn open_path(path: &std::path::Path) {
    #[cfg(target_os = "windows")]
    let cmd = "explorer";
    #[cfg(target_os = "linux")]
    let cmd = "xdg-open";
    #[cfg(target_os = "macos")]
    let cmd = "open";
    let _ = std::process::Command::new(cmd)
        .arg(path)
        .spawn()
        .map(|_| ());
}
