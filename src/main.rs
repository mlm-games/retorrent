mod bencode;
mod config;
mod engine;
mod error;
mod metainfo;
mod network;
mod peer;
mod piece;
mod storage;
mod tracker;
mod tray;
mod types;
mod ui;

use anyhow::Result;
use config::Config;
use engine::TorrentEngine;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::load_or_default();
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(2);
    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(num_threads)
            .build()?,
    );

    let engine = rt.block_on(async { Arc::new(TorrentEngine::new(config).await) });

    let engine_for_shutdown = engine.clone();

    {
        let hashes = engine.get_all_info_hashes();
        for hash in hashes {
            engine.start_torrent(&hash, &rt);
        }
    }

    let (tray_cmd_tx, tray_cmd_rx) = mpsc::channel();
    let tray = Arc::new(tray::AppTray::new(tray_cmd_tx));
    let tray_cmd_rx = Arc::new(Mutex::new(tray_cmd_rx));

    // On Wayland, set_visible is a no-op so the close button would do nothing.
    if std::env::var("WAYLAND_DISPLAY").is_err() {
        repose_platform::set_close_to_tray(true);
    }

    repose_platform::run_desktop_app(move |sched, _rc| {
        ui::app::app(
            sched,
            engine.clone(),
            rt.clone(),
            tray.clone(),
            tray_cmd_rx.clone(),
        )
    })?;

    tracing::info!("Shutting down, saving resume data...");
    engine_for_shutdown.save_all_resume();
    tracing::info!("Done.");

    Ok(())
}
