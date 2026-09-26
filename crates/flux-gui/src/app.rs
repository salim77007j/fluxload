//! The Fluxload application state and UI composition.

use crate::settings::{Section, SettingsState};
use crate::theme::{self, ThemeMode};
use crate::views;
use crate::widgets;
use flux_core::engine::{Engine, EngineEvent};
use flux_core::task::{TaskId, TaskSnapshot, TaskStatus};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, TryRecvError};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct GuiOptions {
    pub adds: Vec<String>,
    pub screenshots: Vec<(PathBuf, u64)>,
    pub exit_after_screenshots: bool,
    pub theme: ThemeMode,
    pub fps_report: Option<PathBuf>,
}

pub struct FluxloadApp {
    pub engine: Engine,
    pub snapshot: Arc<flux_core::task::EngineSnapshot>,
    events: Option<Receiver<EngineEvent>>,
    pub theme: ThemeMode,
    pub selected: Option<TaskId>,
    pub url_input: String,
    filter: Filter,
    settings_open: bool,
    settings: Option<SettingsState>,
    settings_section: Section,
    toasts: VecDeque<(String, Instant, ToastKind)>,
    screenshots: Vec<(PathBuf, u64)>,
    saved_shots: std::collections::HashSet<PathBuf>,
    exit_after: bool,
    started: Instant,
    frames: u64,
    fps: f32,
    diag_rx: Option<std::sync::mpsc::Receiver<String>>,
    last_theme_applied: Option<ThemeMode>,
    /// Keep the selected task across snapshots.
    #[allow(dead_code)] // read by the settings view via license_summary()
    pub license: Option<flux_core::license::LicenseInfo>,
    fps_report: Option<PathBuf>,
}

#[derive(Clone, Copy, PartialEq)]
enum Filter {
    All,
    Active,
    Done,
    Failed,
}

#[derive(Clone, Copy, PartialEq)]
enum ToastKind {
    Info,
    Ok,
    Err,
}

impl FluxloadApp {
    pub fn new(engine: Engine, opts: &GuiOptions) -> Self {
        let events = engine.events();
        let snapshot = engine.snapshot();
        let license = snapshot.license.clone();
        let mut app = Self {
            engine,
            snapshot,
            events: Some(events),
            theme: opts.theme,
            selected: None,
            url_input: String::new(),
            filter: Filter::All,
            settings_open: false,
            settings: None,
            settings_section: Section::Engine,
            toasts: VecDeque::new(),
            screenshots: opts.screenshots.clone(),
            saved_shots: Default::default(),
            exit_after: opts.exit_after_screenshots,
            started: Instant::now(),
            frames: 0,
            fps: 0.0,
            diag_rx: None,
            last_theme_applied: None,
            license,
            fps_report: opts.fps_report.clone(),
        };
        let mut first_added: Option<TaskId> = None;
        for url in &opts.adds {
            let req = flux_core::task::AddRequest::new(url.clone());
            match app.engine.add(req) {
                Ok(id) => {
                    if first_added.is_none() {
                        first_added = Some(id);
                    }
                }
                Err(e) => app.toast(format!("could not add {url}: {e}"), ToastKind::Err),
            }
        }
        // Show details for the first startup add so the side panel is live.
        app.selected = first_added.or_else(|| app.snapshot.tasks.first().map(|t| t.id));
        app
    }

    fn toast(&mut self, msg: impl Into<String>, kind: ToastKind) {
        self.toasts.push_back((msg.into(), Instant::now(), kind));
        if self.toasts.len() > 6 {
            self.toasts.pop_front();
        }
    }

    fn pull_events(&mut self) {
        let events = match self.events.take() {
            Some(e) => e,
            None => return,
        };
        let mut new_snapshot = None;
        let mut channel_alive = true;
        loop {
            match events.try_recv() {
                Ok(EngineEvent::Snapshot(s)) => new_snapshot = Some(s),
                Ok(EngineEvent::TaskAdded(id)) => {
                    if self.selected.is_none() {
                        self.selected = Some(id);
                    }
                }
                Ok(EngineEvent::TaskFinished(_id, status, _msg)) => match status {
                    TaskStatus::Done => {
                        self.toast("download completed", ToastKind::Ok);
                    }
                    TaskStatus::Failed => {
                        self.toast("download failed — see task details", ToastKind::Err);
                    }
                    _ => {}
                },
                Ok(EngineEvent::Info(msg)) => self.toast(msg, ToastKind::Info),
                Ok(EngineEvent::Stopped) | Err(TryRecvError::Disconnected) => {
                    channel_alive = false;
                    break;
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        if channel_alive {
            self.events = Some(events);
        }
        if let Some(s) = new_snapshot {
            self.snapshot = s;
        }
        // Drop selection for removed tasks.
        if let Some(sel) = self.selected {
            if !self.snapshot.tasks.iter().any(|t| t.id == sel) {
                self.selected = self.snapshot.tasks.first().map(|t| t.id);
            }
        }
        if self.selected.is_none() {
            self.selected = self.snapshot.tasks.first().map(|t| t.id);
        }
    }

    fn handle_screenshots(&mut self, ctx: &egui::Context) {
        let elapsed_ms = self.started.elapsed().as_millis() as u64;
        // Request due screenshots (requested tracking via saved_shots + a flag vec is overkill:
        // re-requesting is harmless, egui coalesces).
        for (path, at) in &self.screenshots {
            if elapsed_ms >= *at && !self.saved_shots.contains(path) {
                let user_data = egui::UserData::new(path.display().to_string());
                ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(user_data));
            }
        }
        // Collect screenshot images from input events.
        let events: Vec<egui::Event> = ctx.input(|i| i.events.clone());
        for ev in events {
            if let egui::Event::Screenshot {
                viewport_id: _,
                user_data,
                image,
            } = ev
            {
                let path = user_data
                    .data
                    .as_ref()
                    .and_then(|d| d.downcast_ref::<String>())
                    .cloned()
                    .unwrap_or_else(|| "fluxload-screenshot.png".into());
                let saved = save_png(&image, &path);
                self.saved_shots.insert(PathBuf::from(&path));
                match saved {
                    Ok(()) => self.toast(format!("screenshot saved: {path}"), ToastKind::Ok),
                    Err(e) => self.toast(format!("screenshot failed: {e}"), ToastKind::Err),
                }
            }
        }
        // Exit only after every requested image has actually been captured.
        if self.exit_after
            && !self.screenshots.is_empty()
            && self
                .screenshots
                .iter()
                .all(|(p, _)| self.saved_shots.contains(p))
        {
            self.exit_after = false; // only close once
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
    }

    fn measure_fps(&mut self) {
        self.frames += 1;
        let elapsed = self.started.elapsed().as_secs_f32();
        if elapsed >= 1.0 {
            self.fps = self.frames as f32 / elapsed;
        }
    }
}

impl eframe::App for FluxloadApp {
    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if let Some(path) = &self.fps_report {
            let report = serde_json::json!({
                "frames": self.frames,
                "runtime_secs": self.started.elapsed().as_secs_f32(),
                "avg_fps": self.fps,
            });
            let _ = std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap_or_default());
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        self.pull_events();
        self.measure_fps();
        let ctx = ui.ctx().clone();
        self.handle_screenshots(&ctx);

        // Apply theme once (and on change).
        if self.last_theme_applied != Some(self.theme) {
            theme::apply(&ctx, self.theme);
            self.last_theme_applied = Some(self.theme);
        }

        let p = self.theme.palette();

        // Layout: toolbar (top), status bar (bottom), details (right), queue (center).
        let toolbar = egui::Panel::top("toolbar")
            .frame(
                egui::Frame::new()
                    .fill(p.surface)
                    .inner_margin(egui::Margin::symmetric(16, 10)),
            )
            .show(ui, |ui| self.toolbar(ui, p));
        let statusbar = egui::Panel::bottom("statusbar")
            .frame(
                egui::Frame::new()
                    .fill(p.surface)
                    .inner_margin(egui::Margin::symmetric(16, 9)),
            )
            .show(ui, |ui| self.statusbar(ui, p));
        let details_width = 400.0;
        let details = egui::Panel::right("details")
            .resizable(true)
            .default_size(details_width)
            .frame(
                egui::Frame::new()
                    .fill(p.surface)
                    .inner_margin(egui::Margin::same(16)),
            )
            .show(ui, |ui| self.details(ui, p));
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(p.surface)
                    .inner_margin(egui::Margin::same(14)),
            )
            .show(ui, |ui| self.queue(ui, p));

        // Chrome borders, painted last so they sit on top of panel edges.
        {
            let painter = ui.painter();
            let tr = toolbar.response.rect;
            painter.hline(
                tr.left()..=tr.right(),
                tr.bottom(),
                egui::Stroke::new(1.0, p.border),
            );
            let sr = statusbar.response.rect;
            painter.hline(
                sr.left()..=sr.right(),
                sr.top(),
                egui::Stroke::new(1.0, p.border),
            );
            let dr = details.response.rect;
            painter.vline(
                dr.left(),
                dr.top()..=dr.bottom(),
                egui::Stroke::new(1.0, p.border),
            );
        }

        self.windows(ui, p);
        self.draw_toast(&ctx, p);
    }
}

impl FluxloadApp {
    fn toolbar(&mut self, ui: &mut egui::Ui, p: theme::Palette) {
        ui.horizontal_centered(|ui| {
            widgets::logo(ui, 28.0, p.accent, p.accent_soft);
            ui.add_space(8.0);
            ui.vertical(|ui| {
                ui.label(
                    egui::RichText::new("fluxload")
                        .font(egui::FontId::proportional(17.0))
                        .color(p.text)
                        .strong(),
                );
                ui.label(
                    egui::RichText::new("download engine")
                        .font(theme::font_small())
                        .color(p.sub),
                );
            });
            ui.add_space(16.0);

            // Right cluster (right_to_left): add button, settings, theme, then
            // the URL field stretches across all remaining space next to the
            // brand. Returns true when Enter should trigger an add.
            let theme_label = match self.theme {
                ThemeMode::Light => "🌙",
                ThemeMode::Dark => "☀",
            };
            let enter_add = ui
                .with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    let can_add = !self.url_input.trim().is_empty();
                    let add_btn = egui::Button::new(
                        egui::RichText::new("＋ Add download")
                            .font(theme::font_body())
                            .strong()
                            .color(if can_add { egui::Color32::WHITE } else { p.sub }),
                    )
                    .fill(if can_add { p.accent } else { p.surface_alt });
                    if ui.add_enabled(can_add, add_btn).clicked() {
                        self.add_from_input();
                    }
                    ui.add_space(6.0);
                    if ui
                        .button(egui::RichText::new("⚙").font(theme::font_body()))
                        .on_hover_text("Settings")
                        .clicked()
                    {
                        self.settings_open = !self.settings_open;
                        if self.settings_open && self.settings.is_none() {
                            let cfg = self.current_cfg();
                            self.settings = Some(SettingsState::from_cfg(&cfg));
                        }
                    }
                    if ui
                        .button(egui::RichText::new(theme_label).font(theme::font_body()))
                        .on_hover_text("Toggle light/dark theme")
                        .clicked()
                    {
                        self.theme = self.theme.toggle();
                    }
                    ui.add_space(12.0);

                    // URL field: the last widget in right_to_left takes the
                    // whole remaining left region.
                    let width = ui.available_width().max(220.0);
                    let resp = ui.add(
                        egui::TextEdit::singleline(&mut self.url_input)
                            .hint_text("Paste a URL or magnet link, then press Enter…")
                            .desired_width(width)
                            .margin(egui::Margin::symmetric(10, 6))
                            .font(theme::font_body()),
                    );
                    let enter = ui.input(|i| i.key_pressed(egui::Key::Enter));
                    if enter && can_add {
                        resp.request_focus();
                    }
                    enter && can_add
                })
                .inner;
            if enter_add {
                self.add_from_input();
            }
        });
    }

    fn add_from_input(&mut self) {
        let url = self.url_input.trim().to_string();
        let req = flux_core::task::AddRequest::new(url);
        match self.engine.add(req) {
            Ok(id) => {
                self.selected = Some(id);
                self.url_input.clear();
            }
            Err(e) => self.toast(e.to_string(), ToastKind::Err),
        }
    }

    fn current_cfg(&self) -> flux_core::config::EngineConfig {
        // The engine persists settings.json on every apply; read the latest.
        flux_core::config::EngineConfig::load_or_default()
    }

    fn counts(&self) -> (usize, usize, usize, usize) {
        let mut all = 0;
        let mut active = 0;
        let mut done = 0;
        let mut failed = 0;
        for t in &self.snapshot.tasks {
            all += 1;
            match t.status {
                s if s.is_active() => active += 1,
                TaskStatus::Done => done += 1,
                TaskStatus::Failed => failed += 1,
                _ => {}
            }
        }
        (all, active, done, failed)
    }

    fn queue(&mut self, ui: &mut egui::Ui, p: theme::Palette) {
        // Header row: "Downloads" title on the left, filter chips right.
        let (all, active, done, failed) = self.counts();
        ui.horizontal_centered(|ui| {
            ui.label(
                egui::RichText::new("Downloads")
                    .font(theme::font_h2())
                    .color(p.text)
                    .strong(),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                for (f, label, n) in [
                    (Filter::Failed, "Failed", failed),
                    (Filter::Done, "Done", done),
                    (Filter::Active, "Active", active),
                    (Filter::All, "All", all),
                ] {
                    let selected = self.filter == f;
                    let text = format!("{label} · {n}");
                    let btn = egui::Button::new(if selected {
                        egui::RichText::new(text)
                            .font(theme::font_small())
                            .color(egui::Color32::WHITE)
                            .strong()
                    } else {
                        egui::RichText::new(text)
                            .font(theme::font_small())
                            .color(p.sub)
                    })
                    .fill(if selected { p.accent } else { p.bg });
                    if ui.add(btn).clicked() {
                        self.filter = f;
                    }
                    ui.add_space(4.0);
                }
            });
        });
        ui.add_space(8.0);

        let tasks: Vec<TaskSnapshot> = self
            .snapshot
            .tasks
            .iter()
            .filter(|t| match self.filter {
                Filter::All => true,
                Filter::Active => t.status.is_active() || t.status == TaskStatus::Queued,
                Filter::Done => t.status == TaskStatus::Done,
                Filter::Failed => t.status == TaskStatus::Failed,
            })
            .cloned()
            .collect();

        if tasks.is_empty() {
            views::empty_state(ui, p, self.snapshot.tasks.is_empty());
            return;
        }

        views::queue_header(ui, p);
        ui.add_space(8.0);
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .scroll_bar_visibility(egui::containers::scroll_area::ScrollBarVisibility::AlwaysHidden)
            .show(ui, |ui| {
                ui.set_min_width(ui.available_size().x);
                for t in &tasks {
                    let selected = self.selected == Some(t.id);
                    if views::task_row(ui, t, selected, p) {
                        self.selected = Some(t.id);
                    }
                    ui.add_space(6.0);
                }
            });
    }

    fn details(&mut self, ui: &mut egui::Ui, p: theme::Palette) {
        let sel = self.selected;
        let Some(t) = self
            .snapshot
            .tasks
            .iter()
            .find(|t| Some(t.id) == sel)
            .cloned()
        else {
            ui.centered_and_justified(|ui| {
                ui.label(
                    egui::RichText::new("Select a download to see live details,\nspeed graph, segments and integrity.")
                        .font(theme::font_body())
                        .color(p.sub),
                );
            });
            return;
        };
        let id = t.id;
        let engine = self.engine.clone();
        let mut on_pause = move || {
            let _ = engine.pause(id);
        };
        let engine = self.engine.clone();
        let mut on_resume = move || {
            let _ = engine.resume(id);
        };
        let engine = self.engine.clone();
        let mut on_retry = move || {
            let _ = engine.retry(id);
        };
        let engine = self.engine.clone();
        let mut on_cancel = move || {
            let _ = engine.cancel(id);
        };
        let engine = self.engine.clone();
        let mut on_remove = move || {
            let _ = engine.remove(id, false);
        };
        let url = t.url.clone();
        let mut on_copy_url = move || {
            ui_output::set_clipboard(&url);
        };
        let dir = t.output_dir.clone();
        let mut on_open_folder = move || {
            crate::settings::open_path(&dir);
        };
        egui::ScrollArea::vertical()
            .auto_shrink([false, false])
            .show(ui, |ui| {
                views::task_details(
                    ui,
                    &t,
                    p,
                    self.theme,
                    &mut on_pause,
                    &mut on_resume,
                    &mut on_retry,
                    &mut on_cancel,
                    &mut on_remove,
                    &mut on_copy_url,
                    &mut on_open_folder,
                );
            });
    }

    fn statusbar(&mut self, ui: &mut egui::Ui, p: theme::Palette) {
        let g = self.snapshot.global.clone();
        let cfg = self.snapshot.config.clone();
        let fps = self.fps;
        ui.horizontal_centered(|ui| {
            ui.set_min_height(34.0);
            // Global speed cluster.
            widgets::sparkline(ui, &g.history, 88.0, 18.0, p);
            ui.label(
                egui::RichText::new(flux_core::format::fmt_speed(g.speed_bps))
                    .font(egui::FontId::monospace(13.0))
                    .color(p.text)
                    .strong(),
            );
            widgets::vsep(ui, p);
            ui.label(
                egui::RichText::new(format!(
                    "session {} · {} active · {} queued",
                    flux_core::format::fmt_bytes(g.total_session_bytes),
                    g.active_tasks,
                    g.queued_tasks
                ))
                .font(theme::font_small())
                .color(p.sub),
            );
            widgets::vsep(ui, p);
            // RAM cache cluster.
            let cache_pct = if g.cache_budget_bytes > 0 {
                (g.cache_in_use_bytes as f32 / g.cache_budget_bytes as f32 * 100.0).min(100.0)
            } else {
                0.0
            };
            ui.label(
                egui::RichText::new("RAM cache")
                    .font(theme::font_small())
                    .color(p.sub),
            );
            widgets::mini_bar(ui, 64.0, 5.0, cache_pct / 100.0, p);
            ui.label(
                egui::RichText::new(format!("{:.0}%", cache_pct))
                    .font(theme::font_small())
                    .color(p.sub),
            );
            // Engine feature chips (real build/runtime facts).
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(
                    egui::RichText::new(format!(
                        "fluxload {} · {:.0} fps",
                        flux_core::VERSION,
                        fps
                    ))
                    .font(theme::font_small())
                    .color(p.sub),
                );
                widgets::vsep(ui, p);
                if cfg.http3_enabled {
                    widgets::chip(ui, "HTTP/3", p, p.ok, p.ok_soft);
                }
                if cfg.torrent_enabled {
                    widgets::chip(ui, "BitTorrent", p, p.ok, p.ok_soft);
                } else if !cfg.torrent_compiled {
                    widgets::chip(ui, "BT not compiled", p, p.warn, p.warn_soft);
                }
                match &cfg.proxy {
                    Some(proxy) => {
                        widgets::chip(ui, &format!("proxy {proxy}"), p, p.accent, p.accent_soft)
                    }
                    None => widgets::chip(ui, "direct", p, p.sub, p.bg),
                }
            });
        });
    }

    /// Floating toast (auto-expiring), anchored above the status bar so it
    /// never disturbs the layout of any panel. Long messages are truncated
    /// so the chip can never overflow the window edge.
    fn draw_toast(&mut self, ctx: &egui::Context, p: theme::Palette) {
        self.toasts
            .retain(|(_, at, _)| at.elapsed() < Duration::from_secs(5));
        if let Some((msg, _, kind)) = self.toasts.back().cloned() {
            let (fg, bg) = match kind {
                ToastKind::Info => (p.sub, p.surface_alt),
                ToastKind::Ok => (p.ok, p.ok_soft),
                ToastKind::Err => (p.err, p.err_soft),
            };
            // Keep the chip on-screen: head of message + ellipsis.
            let msg: String = if msg.chars().count() > 64 {
                let head: String = msg.chars().take(61).collect();
                format!("{head}…")
            } else {
                msg
            };
            egui::Area::new(egui::Id::new("toast"))
                .anchor(egui::Align2::RIGHT_BOTTOM, egui::vec2(-16.0, -68.0))
                .order(egui::Order::Foreground)
                .interactable(false)
                .show(ctx, |ui| {
                    widgets::chip(ui, &msg, p, fg, bg);
                });
        }
    }

    fn windows(&mut self, ui: &mut egui::Ui, p: theme::Palette) {
        let ctx = ui.ctx().clone();
        // Settings window.
        if self.settings_open {
            let mut open = self.settings_open;
            let theme_mode = self.theme;
            let _engine = self.engine.clone();
            let mut apply_cfg: Option<(flux_core::config::EngineConfig, ThemeMode)> = None;
            egui::Window::new("Settings")
                .open(&mut open)
                .default_width(600.0)
                .show(&ctx, |ui| {
                    let Some(st) = &mut self.settings else { return };
                    let mut section = self.settings_section;
                    let mut run_diag = false;
                    crate::settings::settings_window(
                        ui,
                        st,
                        &mut section,
                        &mut { theme_mode },
                        p,
                        &mut |cfg, theme| apply_cfg = Some((cfg, theme)),
                        &mut || run_diag = true,
                    );
                    self.settings_section = section;
                    if run_diag {
                        self.start_diagnostics();
                    }
                });
            self.settings_open = open;
            if let Some((cfg, theme)) = apply_cfg {
                if let Err(e) = self.engine.set_config(cfg) {
                    self.toast(format!("could not apply settings: {e}"), ToastKind::Err);
                } else {
                    self.toast("settings applied", ToastKind::Ok);
                }
                self.theme = theme;
            }
        }

        // Diagnostics result polling.
        if let Some(rx) = &self.diag_rx {
            if let Ok(report) = rx.try_recv() {
                if let Some(st) = &mut self.settings {
                    st.diagnostics = Some(report);
                    st.diag_running = false;
                }
                self.diag_rx = None;
            }
        }
    }

    fn start_diagnostics(&mut self) {
        if self.diag_rx.is_some() {
            return;
        }
        let cfg = self.current_cfg();
        let (tx, rx) = std::sync::mpsc::channel();
        self.diag_rx = Some(rx);
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("diag runtime");
            let mut diag = flux_core::doctor::run_diagnostics(&cfg);
            let net = rt.block_on(flux_core::doctor::network_checks(&cfg));
            rt.shutdown_timeout(Duration::from_secs(2));
            diag.checks.extend(net);
            let mut out = String::new();
            for c in &diag.checks {
                let icon = match c.status {
                    flux_core::doctor::CheckStatus::Ok => "[ ok ]",
                    flux_core::doctor::CheckStatus::Warn => "[warn]",
                    flux_core::doctor::CheckStatus::Fail => "[fail]",
                };
                out.push_str(&format!("{icon} {}\n     {}\n", c.label, c.detail));
            }
            out.push_str(&format!(
                "\nram cache budget: {}\nengine: fluxload {}\n",
                flux_core::format::fmt_bytes(diag.ram_cache_budget_bytes),
                flux_core::VERSION
            ));
            let _ = tx.send(out);
        });
    }
}

mod ui_output {
    pub fn set_clipboard(text: &str) {
        // egui exposes the clipboard through arboard internally; reuse Context.
        // Fallback: we cannot access ctx here, so use arboard via egui's clipboard
        // through the global egui instance is not available — store minimal dep.
        let _ = arboard_set(text);
    }
    fn arboard_set(text: &str) -> Result<(), String> {
        // Keep this dependency-free: try wl-copy/xclip/clip via command.
        #[cfg(target_os = "windows")]
        {
            use std::io::Write;
            if let Ok(mut child) = std::process::Command::new("clip")
                .stdin(std::process::Stdio::piped())
                .spawn()
            {
                if let Some(stdin) = &mut child.stdin {
                    let _ = stdin.write_all(text.as_bytes());
                }
                let _ = child.wait();
                return Ok(());
            }
        }
        #[cfg(not(target_os = "windows"))]
        {
            for cmd in ["wl-copy", "xclip", "xsel"] {
                let args: &[&str] = if cmd == "xclip" {
                    &["-selection", "clipboard"]
                } else if cmd == "xsel" {
                    &["--clipboard", "--input"]
                } else {
                    &[]
                };
                if let Ok(mut child) = std::process::Command::new(cmd)
                    .args(args)
                    .stdin(std::process::Stdio::piped())
                    .spawn()
                {
                    use std::io::Write;
                    if let Some(stdin) = &mut child.stdin {
                        let _ = stdin.write_all(text.as_bytes());
                    }
                    let _ = child.wait();
                    return Ok(());
                }
            }
        }
        Err("no clipboard tool available".into())
    }
}

fn save_png(image: &egui::ColorImage, path: &str) -> Result<(), String> {
    let (w, h) = (image.size[0] as u32, image.size[1] as u32);
    let img = image::RgbaImage::from_fn(w, h, |x, y| {
        let idx = (y as usize) * image.size[0] + (x as usize);
        let c = image.pixels[idx];
        image::Rgba([c.r(), c.g(), c.b(), c.a()])
    });
    img.save(path).map_err(|e| e.to_string())
}

pub fn license_summary() -> Option<flux_core::license::LicenseInfo> {
    // Loaded at app start from the snapshot.
    LICENSE_SUMMARY.with(|l| l.borrow().clone())
}

thread_local! {
    static LICENSE_SUMMARY: std::cell::RefCell<Option<flux_core::license::LicenseInfo>> =
        const { std::cell::RefCell::new(None) };
}

/// Store license info for the settings view (called from `new`).
pub fn set_license_summary(info: Option<flux_core::license::LicenseInfo>) {
    LICENSE_SUMMARY.with(|l| *l.borrow_mut() = info);
}
