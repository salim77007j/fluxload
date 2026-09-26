//! fluxload — the native GUI for the Fluxload download engine.
//!
//! CLI flags (used by CI and power users):
//!   --add URL                  enqueue a download at startup (repeatable)
//!   --screenshot PATH:MS       save a PNG after MS milliseconds (repeatable)
//!   --exit-after-screenshots   exit once all screenshots are captured
//!   --theme light|dark         start theme (default: light)

use clap::Parser;
use flux_core::config::EngineConfig;

mod app;
mod settings;
mod theme;
mod views;
mod widgets;

#[derive(Parser, Debug)]
#[command(name = "fluxload", version, about = "Fluxload — download manager")]
struct Args {
    /// Enqueue this URL at startup (repeatable)
    #[arg(long = "add")]
    adds: Vec<String>,
    /// Save a screenshot to PATH after MS milliseconds ("path:ms", repeatable)
    #[arg(long = "screenshot", value_name = "PATH:MS")]
    screenshots: Vec<String>,
    /// Exit after all screenshots are taken
    #[arg(long)]
    exit_after_screenshots: bool,
    /// Start theme
    #[arg(long, default_value = "light")]
    theme: String,
    /// Data directory override
    #[arg(long)]
    data_dir: Option<String>,
    /// Default download directory override
    #[arg(long)]
    download_dir: Option<String>,
    /// Write measured UI FPS stats to this file on exit
    #[arg(long)]
    fps_report: Option<String>,
}

fn main() -> eframe::Result {
    let args = Args::parse();

    // Logging: file + stderr.
    init_logging();

    let mut cfg = EngineConfig::load_or_default();
    if let Some(d) = &args.data_dir {
        cfg.data_dir = d.into();
    }
    if let Some(d) = &args.download_dir {
        cfg.download_dir = d.into();
    }

    let engine = flux_core::engine::Engine::start(cfg);

    // Pre-create the egui context so the engine can request repaints.
    let ctx = egui::Context::default();
    let repaint_ctx = ctx.clone();
    engine.set_repaint_callback(Box::new(move || {
        repaint_ctx.request_repaint();
    }));

    app::set_license_summary(engine.snapshot().license.clone());

    let screenshots: Vec<(std::path::PathBuf, u64)> = args
        .screenshots
        .iter()
        .filter_map(|spec| {
            let (path, ms) = spec.rsplit_once(':')?;
            Some((std::path::PathBuf::from(path), ms.parse().ok()?))
        })
        .collect();

    let opts = app::GuiOptions {
        adds: args.adds,
        screenshots,
        exit_after_screenshots: args.exit_after_screenshots,
        theme: match args.theme.as_str() {
            "dark" => theme::ThemeMode::Dark,
            _ => theme::ThemeMode::Light,
        },
        fps_report: args.fps_report.map(std::path::PathBuf::from),
    };

    let app_name = "fluxload";
    let native_options = eframe::NativeOptions {
        renderer: eframe::Renderer::Glow,
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1180.0, 760.0])
            .with_min_inner_size([860.0, 560.0])
            .with_title("fluxload"),
        ..Default::default()
    };

    let app = app::FluxloadApp::new(engine, &opts);
    // Install fonts on the context before the first frame.
    theme::install_fonts(&ctx);
    theme::apply(&ctx, opts.theme);
    eframe::run_native_ext(
        app_name,
        native_options,
        Some(ctx),
        Box::new(|_cc| Ok(Box::new(app))),
    )
}

fn init_logging() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let dir = flux_core::config::default_data_dir().join("logs");
    if std::fs::create_dir_all(&dir).is_ok() {
        let file_appender = tracing_appender::rolling::daily(&dir, "fluxload.log");
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(file_appender)
            .with_ansi(false)
            .with_target(false);
        let stderr_layer = tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_target(false);
        let filter =
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(stderr_layer)
            .with(file_layer)
            .try_init();
    } else {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .try_init();
    }
}
