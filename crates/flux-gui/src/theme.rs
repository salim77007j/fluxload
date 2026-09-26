//! Fluxload visual identity: palettes, typography (Inter), spacing.

use egui::epaint::text::{FontInsert, FontPriority, InsertFontFamily};
use egui::{Color32, Context, FontFamily, Style, Visuals};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ThemeMode {
    Light,
    Dark,
}

impl ThemeMode {
    pub fn toggle(self) -> Self {
        match self {
            ThemeMode::Light => ThemeMode::Dark,
            ThemeMode::Dark => ThemeMode::Light,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Palette {
    pub bg: Color32,
    pub surface: Color32,
    pub surface_alt: Color32,
    pub border: Color32,
    pub text: Color32,
    pub sub: Color32,
    pub accent: Color32,
    pub accent_soft: Color32,
    pub ok: Color32,
    pub ok_soft: Color32,
    pub warn: Color32,
    pub warn_soft: Color32,
    pub err: Color32,
    pub err_soft: Color32,
}

pub const LIGHT: Palette = Palette {
    bg: Color32::from_rgb(0xFF, 0xFF, 0xFF),
    surface: Color32::from_rgb(0xF8, 0xFA, 0xFC),
    surface_alt: Color32::from_rgb(0xEF, 0xF3, 0xF8),
    border: Color32::from_rgb(0xE2, 0xE8, 0xF0),
    text: Color32::from_rgb(0x0F, 0x17, 0x2A),
    sub: Color32::from_rgb(0x64, 0x74, 0x8B),
    accent: Color32::from_rgb(0x25, 0x63, 0xEB),
    accent_soft: Color32::from_rgb(0xDB, 0xEA, 0xFE),
    ok: Color32::from_rgb(0x0D, 0x94, 0x82),
    ok_soft: Color32::from_rgb(0xCC, 0xF0, 0xEA),
    warn: Color32::from_rgb(0xD9, 0x77, 0x06),
    warn_soft: Color32::from_rgb(0xFE, 0xF3, 0xC7),
    err: Color32::from_rgb(0xDC, 0x26, 0x26),
    err_soft: Color32::from_rgb(0xFE, 0xE2, 0xE2),
};

pub const DARK: Palette = Palette {
    bg: Color32::from_rgb(0x0B, 0x12, 0x20),
    surface: Color32::from_rgb(0x11, 0x1A, 0x2C),
    surface_alt: Color32::from_rgb(0x1B, 0x27, 0x3E),
    border: Color32::from_rgb(0x24, 0x32, 0x4C),
    text: Color32::from_rgb(0xE2, 0xE8, 0xF0),
    sub: Color32::from_rgb(0xA9, 0xB7, 0xCB),
    accent: Color32::from_rgb(0x3B, 0x82, 0xF6),
    accent_soft: Color32::from_rgb(0x1E, 0x2E, 0x52),
    ok: Color32::from_rgb(0x10, 0xB9, 0x81),
    ok_soft: Color32::from_rgb(0x0C, 0x2A, 0x26),
    warn: Color32::from_rgb(0xF5, 0x9E, 0x0B),
    warn_soft: Color32::from_rgb(0x33, 0x27, 0x08),
    err: Color32::from_rgb(0xF8, 0x71, 0x71),
    err_soft: Color32::from_rgb(0x3A, 0x14, 0x17),
};

impl ThemeMode {
    pub fn palette(self) -> Palette {
        match self {
            ThemeMode::Light => LIGHT,
            ThemeMode::Dark => DARK,
        }
    }
}

const INTER_REGULAR: &[u8] = include_bytes!("../assets/fonts/Inter-Regular.ttf");
const INTER_MEDIUM: &[u8] = include_bytes!("../assets/fonts/Inter-Medium.ttf");
const INTER_SEMIBOLD: &[u8] = include_bytes!("../assets/fonts/Inter-SemiBold.ttf");

/// Register the Inter font family at highest priority (per-weight faces).
pub fn install_fonts(ctx: &Context) {
    for (name, bytes) in [
        ("Inter-Regular", INTER_REGULAR),
        ("Inter-Medium", INTER_MEDIUM),
        ("Inter-SemiBold", INTER_SEMIBOLD),
    ] {
        ctx.add_font(FontInsert {
            name: name.into(),
            data: egui::FontData::from_static(bytes),
            families: vec![
                InsertFontFamily {
                    family: FontFamily::Proportional,
                    priority: FontPriority::Highest,
                },
                InsertFontFamily {
                    family: FontFamily::Monospace,
                    priority: FontPriority::Lowest,
                },
            ],
        });
    }
}

/// Build a complete egui Style for the given theme with Fluxload spacing.
pub fn build_style(mode: ThemeMode) -> Style {
    let p = mode.palette();
    let mut style = Style::default();
    let is_dark = matches!(mode, ThemeMode::Dark);
    let mut v = if is_dark {
        Visuals::dark()
    } else {
        Visuals::light()
    };

    v.panel_fill = p.bg;
    v.window_fill = p.surface;
    v.extreme_bg_color = p.surface_alt;
    v.faint_bg_color = p.surface;
    v.code_bg_color = p.surface_alt;
    v.hyperlink_color = p.accent;
    v.override_text_color = Some(p.text);
    v.weak_text_color = Some(p.sub);

    // Widgets: flat, rounded, subtle strokes.
    let w = &mut v.widgets;
    w.noninteractive.bg_fill = p.surface;
    w.noninteractive.fg_stroke.color = p.text;
    w.noninteractive.bg_stroke.color = p.border;
    w.inactive.bg_fill = p.surface;
    w.inactive.fg_stroke.color = p.text;
    w.inactive.bg_stroke.color = p.border;
    w.hovered.bg_fill = p.surface_alt;
    w.hovered.fg_stroke.color = p.text;
    w.hovered.bg_stroke.color = p.border;
    w.active.bg_fill = p.accent;
    w.active.fg_stroke.color = Color32::WHITE;
    w.active.bg_stroke.color = p.accent;
    w.open.bg_fill = p.surface_alt;
    w.open.bg_stroke.color = p.border;
    w.open.fg_stroke.color = p.text;

    v.selection.bg_fill = p.accent;
    v.selection.stroke.color = Color32::WHITE;

    style.visuals = v;
    style.spacing.item_spacing = egui::vec2(8.0, 8.0);
    style.spacing.button_padding = egui::vec2(10.0, 5.0);
    style.spacing.window_margin = egui::Margin::same(14);
    style
}

/// Apply the theme (fonts installed once at startup).
pub fn apply(ctx: &Context, mode: ThemeMode) {
    let style = build_style(mode);
    let arc: Arc<Style> = Arc::new(style);
    ctx.set_style_of(egui::Theme::Light, arc.clone());
    ctx.set_style_of(egui::Theme::Dark, arc);
}

/// Heading font sizes used across the app.
pub fn font_h1() -> egui::FontId {
    egui::FontId::proportional(20.0)
}
pub fn font_h2() -> egui::FontId {
    egui::FontId::proportional(15.0)
}
pub fn font_body() -> egui::FontId {
    egui::FontId::proportional(13.5)
}
pub fn font_small() -> egui::FontId {
    egui::FontId::proportional(11.5)
}
pub fn font_mono() -> egui::FontId {
    egui::FontId::monospace(12.0)
}
