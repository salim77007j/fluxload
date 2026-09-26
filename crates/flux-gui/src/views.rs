//! The download queue list (custom-painted rows), column geometry, and the
//! task details panel.

use crate::theme::{self, Palette, ThemeMode};
use crate::widgets;
use egui::{Color32, Painter, Pos2, Rect, Sense, Stroke, Ui, Vec2};
use flux_core::task::{TaskSnapshot, TaskStatus};
use flux_core::{format as ffmt, PRODUCT};

/// Column geometry shared by the header strip and every queue row, so the
/// two can never drift apart.
pub struct QueueCols {
    pub name_w: f32,
    pub prog_x: f32,
    pub prog_w: f32,
    pub status_x: f32,
}

/// Compute the column layout for a queue of the given width.
pub fn queue_cols(width: f32) -> QueueCols {
    let width = width.max(320.0);
    let status_w = (width * 0.26).clamp(132.0, 240.0);
    let name_max = (width - status_w - 170.0).max(170.0);
    let name_w = (width * 0.42).clamp(150.0, name_max);
    let prog_w = (width - name_w - status_w - 28.0).max(90.0);
    let prog_x = name_w + 14.0;
    QueueCols {
        name_w,
        prog_x,
        prog_w,
        status_x: prog_x + prog_w + 14.0,
    }
}

/// The column header strip above the queue: NAME · PROGRESS · STATUS.
pub fn queue_header(ui: &mut Ui, p: Palette) {
    let width = ui.available_size().x;
    let cols = queue_cols(width);
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, 24.0), Sense::hover());
    let painter = ui.painter_at(rect);
    let font = theme::font_small();

    let label = |text: &str, x: f32, align_right: bool| {
        let g = painter.layout_no_wrap(text.to_owned(), font.clone(), p.sub);
        let px = if align_right { x - g.size().x } else { x };
        painter.galley(Pos2::new(px, rect.top() + 5.0), g, p.sub);
    };
    label("NAME", rect.left() + 14.0, false);
    label("PROGRESS", rect.left() + cols.prog_x, false);
    label("STATUS", rect.right() - 14.0, true);

    painter.hline(
        rect.left()..=rect.right(),
        rect.bottom() - 0.5,
        Stroke::new(1.0, p.border),
    );
}

/// One queue row, fully custom-painted. Returns true when clicked.
///
/// Layout follows [`queue_cols`]: name (with dot + meta) on the left, progress
/// bar with live stats in the middle, status chip and connection/error info on
/// the right. All text is truncated to its column so nothing can overlap.
pub fn task_row(ui: &mut Ui, t: &TaskSnapshot, selected: bool, p: Palette) -> bool {
    let width = ui.available_size().x;
    let cols = queue_cols(width);
    let height = 66.0;
    let (rect, resp) = ui.allocate_exact_size(Vec2::new(width, height), Sense::click());
    let painter = ui.painter_at(rect);
    let hovered = resp.hovered();

    // Card background, hover and selection states.
    let card_bg = if selected || hovered {
        p.surface_alt
    } else {
        p.bg
    };
    painter.rect_filled(rect, 8.0, card_bg);
    painter.rect_stroke(
        rect,
        8.0,
        Stroke::new(1.0, if selected { p.accent } else { p.border }),
        egui::StrokeKind::Middle,
    );
    if selected {
        let bar = Rect::from_min_max(
            Pos2::new(rect.left() + 3.0, rect.top() + 10.0),
            Pos2::new(rect.left() + 6.0, rect.bottom() - 10.0),
        );
        painter.rect_filled(bar, 1.5, p.accent);
    }

    let left = rect.left() + 14.0;
    let right = rect.right() - 14.0;
    let top = rect.top();

    // --- Name column: status dot, filename, host/kind/protocol meta ---
    let (_, dot_color, _) = widgets::status_colors(t.status, p);
    painter.circle_filled(Pos2::new(left + 5.0, top + 16.0), 4.0, dot_color);

    let name = if t.filename.is_empty() {
        "(resolving…)"
    } else {
        &t.filename
    };
    let name_font = theme::font_h2();
    let name_w = (cols.name_w - 26.0).max(60.0);
    let name_text = truncate(&painter, name, name_font.clone(), name_w);
    let name_galley = painter.layout_no_wrap(name_text, name_font, p.text);
    painter.galley(Pos2::new(left + 16.0, top + 8.0), name_galley, p.text);

    let host = t
        .url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("magnet")
        .to_string();
    let meta = format!(
        "{} · {} · {}",
        host,
        t.kind.label(),
        t.protocol.as_deref().unwrap_or("…")
    );
    let meta_font = theme::font_small();
    let meta_text = truncate(&painter, &meta, meta_font.clone(), name_w);
    let meta_galley = painter.layout_no_wrap(meta_text, meta_font, p.sub);
    painter.galley(Pos2::new(left + 16.0, top + 32.0), meta_galley, p.sub);

    // --- Progress column: pct + speed, bar, bytes + ETA ---
    let px = rect.left() + cols.prog_x;
    let pw = cols.prog_w;
    let pct = t
        .progress_frac()
        .map(|f| format!("{:5.1}%", f * 100.0))
        .unwrap_or_else(|| "  n/a".into());
    let pct_galley = painter.layout_no_wrap(pct, theme::font_mono(), p.text);
    painter.galley(Pos2::new(px, top + 7.0), pct_galley, p.text);
    if t.status.is_active() {
        let speed_txt = ffmt::fmt_speed(t.speed_bps);
        let sg = painter.layout_no_wrap(speed_txt, theme::font_mono(), p.accent);
        painter.galley(Pos2::new(px + pw - sg.size().x, top + 7.0), sg, p.accent);
    }

    let bar_rect = Rect::from_min_size(Pos2::new(px, top + 30.0), Vec2::new(pw, 6.0));
    widgets::paint_progress(
        &painter,
        bar_rect,
        t.progress_frac().unwrap_or(0.0),
        p,
        Some(bar_color(t.status, p)),
    );

    let bytes_txt = format!(
        "{} / {}",
        ffmt::fmt_bytes(t.downloaded),
        t.size.map(ffmt::fmt_bytes).unwrap_or_else(|| "?".into()),
    );
    let bytes_galley = painter.layout_no_wrap(bytes_txt, theme::font_small(), p.sub);
    painter.galley(Pos2::new(px, top + 43.0), bytes_galley, p.sub);
    if t.status.is_active() {
        if let Some(eta) = t.eta_sec {
            let eta_txt = format!("ETA {}", ffmt::fmt_eta(Some(eta)));
            let eg = painter.layout_no_wrap(eta_txt, theme::font_small(), p.sub);
            painter.galley(Pos2::new(px + pw - eg.size().x, top + 43.0), eg, p.sub);
        }
    }

    // --- Status column: chip + connections or error (truncated) ---
    let sx = rect.left() + cols.status_x;
    let sw = (right - sx).max(72.0);
    let (label, fg, chip_bg) = widgets::status_colors(t.status, p);
    let chip_font = theme::font_small();
    let label_txt = truncate(&painter, label, chip_font.clone(), (sw - 18.0).max(40.0));
    let chip_w = widgets::chip_size(&label_txt, chip_font.clone(), &painter)
        .x
        .min(sw);
    let chip_rect = Rect::from_min_size(
        Pos2::new(right - chip_w, top + 9.0),
        Vec2::new(chip_w, 20.0),
    );
    widgets::paint_chip(&painter, chip_rect, &label_txt, chip_font, fg, chip_bg);

    if t.status.is_active() {
        let conn = format!("{} connections", t.active_conns);
        let conn_txt = truncate(&painter, &conn, theme::font_small(), sw);
        let cg = painter.layout_no_wrap(conn_txt, theme::font_small(), p.accent);
        painter.galley(Pos2::new(right - cg.size().x, top + 37.0), cg, p.accent);
    } else if let Some(err) = &t.error {
        let err_full = format!("⚠ {err}");
        let err_txt = truncate(&painter, &err_full, theme::font_small(), sw);
        let eg = painter.layout_no_wrap(err_txt, theme::font_small(), p.err);
        painter.galley(Pos2::new(right - eg.size().x, top + 37.0), eg, p.err);
    }

    let _ = PRODUCT;
    resp.clicked()
}

/// Truncate `text` (appending an ellipsis) so it fits `max_w` at `font`.
/// UTF-8 safe: works on chars, and binary-searches the longest fitting prefix.
fn truncate(painter: &Painter, text: &str, font: egui::FontId, max_w: f32) -> String {
    let full = painter.layout_no_wrap(text.to_owned(), font.clone(), Color32::WHITE);
    if full.size().x <= max_w || text.is_empty() {
        return text.to_owned();
    }
    let chars: Vec<char> = text.chars().collect();
    let fits = |n: usize| -> bool {
        let prefix: String = chars[..n].iter().collect();
        painter
            .layout_no_wrap(format!("{prefix}…"), font.clone(), Color32::WHITE)
            .size()
            .x
            <= max_w
    };
    let mut lo = 0usize; // fits(lo) is true ("…" alone always fits).
    let mut hi = chars.len();
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if fits(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    let prefix: String = chars[..lo].iter().collect();
    format!("{prefix}…")
}

/// Centered empty state: a download glyph plus a call to action.
pub fn empty_state(ui: &mut Ui, p: Palette, no_tasks: bool) {
    let rect = ui.available_rect_before_wrap();
    let painter = ui.painter_at(rect);
    let center = rect.center();

    // Glyph: circle with a download arrow and tray.
    let gc = Pos2::new(center.x, center.y - 36.0);
    painter.circle_filled(gc, 34.0, p.surface_alt);
    painter.line_segment(
        [Pos2::new(gc.x, gc.y - 15.0), Pos2::new(gc.x, gc.y + 8.0)],
        Stroke::new(3.5, p.sub),
    );
    let head = [
        Pos2::new(gc.x - 10.0, gc.y - 1.0),
        Pos2::new(gc.x, gc.y + 10.0),
        Pos2::new(gc.x + 10.0, gc.y - 1.0),
    ];
    painter.line_segment([head[0], head[1]], Stroke::new(3.5, p.sub));
    painter.line_segment([head[1], head[2]], Stroke::new(3.5, p.sub));
    painter.line_segment(
        [
            Pos2::new(gc.x - 15.0, gc.y + 19.0),
            Pos2::new(gc.x + 15.0, gc.y + 19.0),
        ],
        Stroke::new(3.5, p.sub),
    );

    let title = if no_tasks {
        "No downloads yet"
    } else {
        "Nothing matches this filter"
    };
    let sub = if no_tasks {
        "Paste a URL or magnet link in the field above and press Enter."
    } else {
        "Try another filter to see your other downloads."
    };
    let tg = painter.layout_no_wrap(title.to_owned(), theme::font_h2(), p.text);
    painter.galley(
        Pos2::new(center.x - tg.size().x / 2.0, center.y + 8.0),
        tg,
        p.text,
    );
    let sg = painter.layout_no_wrap(sub.to_owned(), theme::font_small(), p.sub);
    painter.galley(
        Pos2::new(center.x - sg.size().x / 2.0, center.y + 34.0),
        sg,
        p.sub,
    );
}

fn bar_color(s: TaskStatus, p: Palette) -> Color32 {
    match s {
        TaskStatus::Done => p.ok,
        TaskStatus::Failed => p.err,
        TaskStatus::Paused | TaskStatus::Cancelled => p.sub,
        _ => p.accent,
    }
}

/// The details panel content for the selected task.
#[allow(clippy::too_many_arguments)]
pub fn task_details(
    ui: &mut Ui,
    t: &TaskSnapshot,
    p: Palette,
    _theme: ThemeMode,
    on_pause: &mut dyn FnMut(),
    on_resume: &mut dyn FnMut(),
    on_retry: &mut dyn FnMut(),
    on_cancel: &mut dyn FnMut(),
    on_remove: &mut dyn FnMut(),
    on_copy_url: &mut dyn FnMut(),
    on_open_folder: &mut dyn FnMut(),
) {
    ui.add_space(4.0);
    ui.label(
        egui::RichText::new(if t.filename.is_empty() {
            "(resolving…)"
        } else {
            &t.filename
        })
        .font(theme::font_h1())
        .color(p.text)
        .strong(),
    );
    ui.horizontal(|ui| {
        widgets::status_chip(ui, t.status, p);
        if let Some(proto) = &t.protocol {
            widgets::chip(ui, proto, p, p.sub, p.surface_alt);
        }
        if t.resume_supported {
            widgets::chip(ui, "resumable", p, p.ok, p.ok_soft);
        } else {
            widgets::chip(ui, "no resume", p, p.warn, p.warn_soft);
        }
    });
    ui.add_space(6.0);

    ui.label(
        egui::RichText::new(&t.url)
            .font(theme::font_mono())
            .color(p.sub)
            .weak(),
    );

    ui.add_space(10.0);
    ui.columns(3, |cols| {
        stat_block(&mut cols[0], "Progress", &progress_text(t), p);
        stat_block(&mut cols[1], "Speed", &ffmt::fmt_speed(t.speed_bps), p);
        stat_block(&mut cols[2], "ETA", &ffmt::fmt_eta(t.eta_sec), p);
    });
    ui.add_space(6.0);
    ui.columns(3, |cols| {
        stat_block(
            &mut cols[0],
            "Downloaded",
            &ffmt::fmt_bytes(t.downloaded),
            p,
        );
        stat_block(
            &mut cols[1],
            "Size",
            &t.size
                .map(ffmt::fmt_bytes)
                .unwrap_or_else(|| "unknown".into()),
            p,
        );
        stat_block(&mut cols[2], "Avg speed", &ffmt::fmt_speed(t.avg_bps), p);
    });

    ui.add_space(8.0);
    let frac = t.progress_frac().unwrap_or(0.0);
    widgets::progress_bar(
        ui,
        frac,
        ui.available_size().x,
        8.0,
        p,
        Some(bar_color(t.status, p)),
    );
    if t.size.unwrap_or(0) > 0 {
        ui.add_space(6.0);
        widgets::segment_grid(ui, &t.done_ranges, &t.segments, t.size.unwrap_or(0), p);
    }

    ui.add_space(12.0);
    widgets::speed_chart(ui, &t.speed_history, 118.0, p, "Transfer speed — last 60 s");

    ui.add_space(12.0);
    ui.label(
        egui::RichText::new("Integrity")
            .font(theme::font_h2())
            .color(p.text)
            .strong(),
    );
    ui.horizontal(|ui| {
        let (label, fg, bg) = match t.checksum_ok {
            Some(true) => ("SHA-256 verified", p.ok, p.ok_soft),
            Some(false) => ("SHA-256 MISMATCH", p.err, p.err_soft),
            None => ("not computed", p.sub, p.surface_alt),
        };
        widgets::chip(ui, label, p, fg, bg);
        if t.kind == flux_core::task::TaskKind::Torrent {
            widgets::chip(ui, "piece-verified (BitTorrent)", p, p.ok, p.ok_soft);
        }
    });
    if let Some(h) = &t.actual_sha256 {
        // Full hash on hover; on screen show head…tail so the row stays
        // inside the panel at any width.
        let short = if h.len() > 36 {
            format!("{}…{}", &h[..16], &h[h.len() - 12..])
        } else {
            h.clone()
        };
        ui.label(
            egui::RichText::new(format!("sha256  {short}"))
                .font(theme::font_mono())
                .color(p.sub),
        )
        .on_hover_text(format!("sha256 (actual)\n{h}"));
    }
    if let Some(h) = &t.expected_sha256 {
        let short = if h.len() > 36 {
            format!("{}…{}", &h[..16], &h[h.len() - 12..])
        } else {
            h.clone()
        };
        ui.label(
            egui::RichText::new(format!("expect  {short}"))
                .font(theme::font_mono())
                .color(p.sub),
        )
        .on_hover_text(format!("sha256 (expected)\n{h}"));
    }

    if !t.segments.is_empty() {
        ui.add_space(8.0);
        ui.label(
            egui::RichText::new(format!("Connections ({})", t.segments.len()))
                .font(theme::font_h2())
                .color(p.text)
                .strong(),
        );
        egui::ScrollArea::vertical()
            .max_height(140.0)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                for seg in &t.segments {
                    ui.horizontal(|ui| {
                        let span = if seg.end < u64::MAX / 2 {
                            format!("{} – {}", seg.start, seg.end)
                        } else {
                            "stream".into()
                        };
                        let frac = if seg.end > seg.start && seg.end < u64::MAX / 2 {
                            (seg.done as f32 / (seg.end - seg.start) as f32).min(1.0)
                        } else {
                            0.0
                        };
                        ui.label(
                            egui::RichText::new(seg.state.label())
                                .font(theme::font_small())
                                .color(p.sub),
                        );
                        ui.add_space(4.0);
                        widgets::progress_bar(ui, frac, 90.0, 5.0, p, Some(p.accent));
                        ui.label(
                            egui::RichText::new(format!("{:>10}", ffmt::fmt_speed(seg.speed_bps)))
                                .font(theme::font_mono())
                                .color(p.text),
                        );
                        if seg.retries > 0 {
                            ui.label(
                                egui::RichText::new(format!("{} retries", seg.retries))
                                    .font(theme::font_small())
                                    .color(p.warn),
                            );
                        }
                        ui.label(
                            egui::RichText::new(span)
                                .font(theme::font_small())
                                .color(p.sub),
                        );
                    });
                }
            });
    }

    if let Some(err) = &t.error {
        ui.add_space(8.0);
        ui.colored_label(
            p.err,
            egui::RichText::new(format!("Error: {err}"))
                .font(theme::font_body())
                .strong(),
        );
    }

    ui.add_space(10.0);
    ui.horizontal(|ui| {
        let status = t.status;
        if status.is_active() && ui.button("⏸ Pause").clicked() {
            on_pause();
        }
        if (status == TaskStatus::Paused || status == TaskStatus::Queued)
            && ui.button("▶ Resume").clicked()
        {
            on_resume();
        }
        if matches!(status, TaskStatus::Failed | TaskStatus::Cancelled)
            && ui.button("↻ Retry").clicked()
        {
            on_retry();
        }
        if !status.is_terminal() && ui.button("✕ Cancel").clicked() {
            on_cancel();
        }
        if ui.button("Copy URL").clicked() {
            on_copy_url();
        }
        if ui.button("Open folder").clicked() {
            on_open_folder();
        }
        if ui
            .button(egui::RichText::new("Remove").color(p.err))
            .clicked()
        {
            on_remove();
        }
    });
}

fn progress_text(t: &TaskSnapshot) -> String {
    t.progress_frac()
        .map(|f| format!("{:.1}%", f * 100.0))
        .unwrap_or_else(|| "-".into())
}

fn stat_block(ui: &mut Ui, label: &str, value: &str, p: Palette) {
    ui.vertical(|ui| {
        ui.label(
            egui::RichText::new(label)
                .font(theme::font_small())
                .color(p.sub),
        );
        ui.label(
            egui::RichText::new(value)
                .font(egui::FontId::proportional(15.0))
                .color(p.text)
                .strong(),
        );
    });
}
