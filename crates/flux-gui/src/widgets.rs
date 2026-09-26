//! Custom-painted widgets: logo, progress bars, status chips, speed charts,
//! the segment grid — all drawn from real engine data.

use crate::theme::Palette;
use egui::{Color32, Mesh, Painter, Pos2, Rect, Sense, Shape, Stroke, Ui, Vec2};

/// The Fluxload mark: a rounded square with a lightning bolt.
pub fn logo(ui: &mut Ui, size: f32, accent: Color32, bg: Color32) -> egui::Response {
    let desired = Vec2::splat(size);
    let (rect, resp) = ui.allocate_exact_size(desired, Sense::hover());
    let painter = ui.painter_at(rect);
    draw_logo(&painter, rect, accent, bg);
    resp
}

pub fn draw_logo(painter: &Painter, rect: Rect, accent: Color32, bg: Color32) {
    painter.rect_filled(rect, 6.0, bg);
    // Bolt polygon (normalized 0..1 coordinates).
    let pts = [
        (0.58, 0.10),
        (0.24, 0.56),
        (0.46, 0.56),
        (0.38, 0.90),
        (0.76, 0.42),
        (0.52, 0.42),
        (0.62, 0.10),
    ];
    let scale = rect.size();
    let pos: Vec<Pos2> = pts
        .iter()
        .map(|&(x, y)| Pos2::new(rect.left() + x * scale.x, rect.top() + y * scale.y))
        .collect();
    painter.add(Shape::convex_polygon(pos, accent, Stroke::NONE));
}

/// Rounded progress bar, custom-painted.
pub fn progress_bar(
    ui: &mut Ui,
    frac: f32,
    width: f32,
    height: f32,
    p: Palette,
    color: Option<Color32>,
) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, height / 2.0, p.surface_alt);
    let fill = color.unwrap_or(p.accent);
    let w = (frac.clamp(0.0, 1.0) * rect.width()).max(if frac > 0.0 { height } else { 0.0 });
    if w > 0.0 {
        let mut fill_rect = rect;
        fill_rect.set_width(w);
        painter.rect_filled(fill_rect, height / 2.0, fill);
    }
}

pub fn paint_progress(
    painter: &Painter,
    rect: Rect,
    frac: f32,
    p: Palette,
    color: Option<Color32>,
) {
    painter.rect_filled(rect, rect.height() / 2.0, p.surface_alt);
    let fill = color.unwrap_or(p.accent);
    let w = (frac.clamp(0.0, 1.0) * rect.width()).max(if frac > 0.0 { rect.height() } else { 0.0 });
    if w > 0.0 {
        let mut fill_rect = rect;
        fill_rect.set_width(w);
        painter.rect_filled(fill_rect, rect.height() / 2.0, fill);
    }
}

/// Pill-shaped status chip with a colored dot (layout widget).
pub fn chip(ui: &mut Ui, label: &str, _p: Palette, fg: Color32, bg: Color32) {
    let font = crate::theme::font_small();
    let galley = ui
        .painter()
        .layout_no_wrap(label.to_owned(), font.clone(), fg);
    let pad = 6.0;
    let dot = 5.0;
    let size = Vec2::new(
        galley.size().x + pad * 2.0 + dot + 4.0,
        galley.size().y + 4.0,
    );
    let (rect, _) = ui.allocate_exact_size(size, Sense::hover());
    let painter = ui.painter_at(rect);
    paint_chip(&painter, rect, label, font, fg, bg);
}

/// Painter-based chip (used inside custom-drawn rows).
pub fn paint_chip(
    painter: &Painter,
    rect: Rect,
    label: &str,
    font: egui::FontId,
    fg: Color32,
    bg: Color32,
) {
    painter.rect_filled(rect, rect.height() / 2.0, bg);
    let galley = painter.layout_no_wrap(label.to_owned(), font.clone(), fg);
    let pad = 6.0;
    let dot = 5.0;
    let dot_center = Pos2::new(rect.left() + pad + dot / 2.0, rect.center().y);
    painter.circle_filled(dot_center, dot / 2.0, fg);
    let text_pos = Pos2::new(
        rect.left() + pad + dot + 4.0,
        rect.top() + (rect.height() - galley.size().y) / 2.0,
    );
    painter.galley(text_pos, galley, fg);
}

/// Measure a chip's size without allocating.
pub fn chip_size(label: &str, font: egui::FontId, painter: &Painter) -> Vec2 {
    let galley = painter.layout_no_wrap(label.to_owned(), font, Color32::WHITE);
    Vec2::new(galley.size().x + 12.0 + 5.0 + 4.0, galley.size().y + 4.0)
}

pub fn status_chip(ui: &mut Ui, status: flux_core::task::TaskStatus, p: Palette) {
    let (label, fg, bg) = status_colors(status, p);
    chip(ui, label, p, fg, bg);
}

pub fn status_colors(
    status: flux_core::task::TaskStatus,
    p: Palette,
) -> (&'static str, Color32, Color32) {
    use flux_core::task::TaskStatus::*;
    match status {
        Queued => ("Queued", p.sub, p.surface_alt),
        Probing => ("Probing", p.accent, p.accent_soft),
        Downloading => ("Downloading", p.accent, p.accent_soft),
        Paused => ("Paused", p.warn, p.warn_soft),
        Verifying => ("Verifying", p.warn, p.warn_soft),
        Done => ("Completed", p.ok, p.ok_soft),
        Failed => ("Failed", p.err, p.err_soft),
        Cancelled => ("Cancelled", p.sub, p.surface_alt),
    }
}

/// Real-time speed area chart (bytes/sec), drawn from engine history samples.
pub fn speed_chart(
    ui: &mut Ui,
    history: &[(i64, u64)],
    wanted_height: f32,
    p: Palette,
    title: &str,
) {
    let width = ui.available_size().x;
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, wanted_height), Sense::hover());
    let painter = ui.painter_at(rect);

    let font = crate::theme::font_small();
    let title_galley = painter.layout_no_wrap(title.to_owned(), font.clone(), p.sub);
    painter.galley(rect.left_top(), title_galley, p.sub);

    // Layout: title strip on top, right gutter for Y labels, plot inside.
    // Nothing is painted within `pad` of the outer rect, so labels can
    // never clip against the panel boundary.
    let gutter = 46.0;
    let pad = 6.0;
    let plot = Rect::from_min_max(
        Pos2::new(rect.left() + pad, rect.top() + 18.0),
        Pos2::new(rect.right() - gutter - pad, rect.bottom() - pad),
    );
    painter.rect_filled(plot, 4.0, p.bg);

    let max_speed = history.iter().map(|s| s.1).max().unwrap_or(0).max(1024);
    for i in 1..=4 {
        let y = plot.bottom() - plot.height() * i as f32 / 4.0;
        let pts = [Pos2::new(plot.left(), y), Pos2::new(plot.right(), y)];
        painter.line_segment(pts, Stroke::new(1.0, p.border));
        let label = flux_core::format::fmt_speed((max_speed as f32 * i as f32 / 4.0) as u64);
        let galley = painter.layout_no_wrap(label, font.clone(), p.sub);
        // Centered vertically on its gridline, left-aligned in the gutter:
        // two points of clearance on every side.
        let pos = Pos2::new(
            (plot.right() + 5.0).min(rect.right() - galley.size().x - 2.0),
            y - galley.size().y / 2.0,
        );
        painter.galley(pos, galley, p.sub);
    }

    if history.len() < 2 {
        let galley = painter.layout_no_wrap("waiting for data…".into(), font.clone(), p.sub);
        let pos = Pos2::new(
            plot.center().x - galley.size().x / 2.0,
            plot.center().y - galley.size().y / 2.0,
        );
        painter.galley(pos, galley, p.sub);
        return;
    }

    let now_ms = history.last().map(|s| s.0).unwrap_or(0);
    let window_ms = 60_000i64;
    let to_x = |t: i64| {
        let age = (now_ms - t).clamp(0, window_ms) as f32;
        plot.right() - age / window_ms as f32 * plot.width()
    };
    let to_y = |v: u64| plot.bottom() - (v as f32 / max_speed as f32).min(1.0) * plot.height();

    let pts: Vec<Pos2> = history
        .iter()
        .filter(|(t, _)| now_ms - *t <= window_ms)
        .map(|(t, v)| Pos2::new(to_x(*t), to_y(*v)))
        .collect();

    if pts.len() >= 2 {
        let mut mesh = Mesh::default();
        let fill = Color32::from_rgba_unmultiplied(p.accent.r(), p.accent.g(), p.accent.b(), 48);
        let base = plot.bottom();
        let mut prev = *pts.first().unwrap();
        for &cur in pts.iter().skip(1) {
            let i0 = mesh.vertices.len() as u32;
            mesh.colored_vertex(Pos2::new(prev.x, base), fill);
            mesh.colored_vertex(Pos2::new(cur.x, base), fill);
            mesh.colored_vertex(prev, fill);
            mesh.colored_vertex(cur, fill);
            mesh.add_triangle(i0, i0 + 1, i0 + 2);
            mesh.add_triangle(i0 + 1, i0 + 3, i0 + 2);
            prev = cur;
        }
        painter.add(Shape::Mesh(std::sync::Arc::new(mesh)));
        painter.add(Shape::line(pts.clone(), Stroke::new(2.0, p.accent)));
        let last = *pts.last().unwrap();
        painter.circle_filled(last, 3.0, p.accent);
    }
}

/// The IDM-style segment grid: 64 cells showing byte coverage.
pub fn segment_grid(
    ui: &mut Ui,
    done_ranges: &[(u64, u64)],
    segments: &[flux_core::task::SegmentSnap],
    size: u64,
    p: Palette,
) {
    let width = ui.available_size().x;
    let cols = 32usize;
    let rows = 2usize;
    let gap = 2.0;
    let cell_w = (width - gap * (cols as f32 - 1.0)) / cols as f32;
    let cell_h = 10.0;
    let height = rows as f32 * (cell_h + gap) - gap;
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), Sense::hover());
    let painter = ui.painter_at(rect);
    let origin = rect.left_top();

    let coverage = |start: u64, end: u64| -> f32 {
        let mut covered = 0u64;
        for &(s, e) in done_ranges {
            let lo = s.max(start);
            let hi = e.min(end);
            if hi > lo {
                covered += hi - lo;
            }
        }
        for seg in segments {
            let lo = seg.start.max(start);
            let hi = seg.end.min(end);
            if hi > lo {
                let seg_frac = if seg.end > seg.start {
                    (seg.done as f32 / (seg.end - seg.start) as f32).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                covered += ((hi - lo) as f32 * seg_frac) as u64;
            }
        }
        let len = end.saturating_sub(start).max(1);
        (covered as f32 / len as f32).clamp(0.0, 1.0)
    };

    for row in 0..rows {
        for col in 0..cols {
            let idx = row * cols + col;
            let cell_size = size.max(1);
            let c_start = idx as u64 * cell_size / (cols * rows) as u64;
            let c_end = (idx as u64 + 1) * cell_size / (cols * rows) as u64;
            let frac = coverage(c_start, c_end);
            let pos = Pos2::new(
                origin.x + col as f32 * (cell_w + gap),
                origin.y + row as f32 * (cell_h + gap),
            );
            let cell = Rect::from_min_size(pos, Vec2::new(cell_w, cell_h));
            let color = if frac >= 0.999 {
                p.ok
            } else if frac > 0.0 {
                p.accent
            } else {
                p.surface_alt
            };
            painter.rect_filled(cell, 2.0, color);
        }
    }
}

/// Small sparkline for the status bar, drawn inside a subtle container.
pub fn sparkline(ui: &mut Ui, history: &[(i64, u64)], width: f32, height: f32, p: Palette) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, height / 2.0, p.bg);
    let max = history.iter().map(|s| s.1).max().unwrap_or(1).max(1024);
    if history.len() < 2 {
        return;
    }
    let pts: Vec<Pos2> = history
        .iter()
        .enumerate()
        .map(|(i, (_, v))| {
            let x = rect.left() + 3.0
                + i as f32 / (history.len() as f32 - 1.0) * (rect.width() - 6.0);
            let y = rect.bottom() - 3.0 - (*v as f32 / max as f32).min(1.0) * (rect.height() - 6.0);
            Pos2::new(x, y)
        })
        .collect();
    painter.add(Shape::line(pts, Stroke::new(1.5, p.accent)));
}

/// Thin vertical divider used to group status-bar clusters.
pub fn vsep(ui: &mut Ui, p: Palette) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(1.0, 16.0), Sense::hover());
    ui.painter_at(rect).rect_filled(rect, 0.5, p.border);
}

/// Tiny horizontal meter (e.g. RAM cache pressure).
pub fn mini_bar(ui: &mut Ui, width: f32, height: f32, frac: f32, p: Palette) {
    let (rect, _) = ui.allocate_exact_size(Vec2::new(width, height), Sense::hover());
    let painter = ui.painter_at(rect);
    painter.rect_filled(rect, height / 2.0, p.surface_alt);
    let w = (frac.clamp(0.0, 1.0) * width).max(if frac > 0.0 { height } else { 0.0 });
    if w > 0.0 {
        let mut fill_rect = rect;
        fill_rect.set_width(w);
        painter.rect_filled(fill_rect, height / 2.0, p.accent);
    }
}
