mod bencode;
mod config;
mod dht;
mod nat;
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
mod webseed;

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

    let args: Vec<String> = std::env::args().collect();
    let headless = args.iter().any(|a| a == "--headless");
    let torrent_files: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|a| !a.starts_with("--"))
        .cloned()
        .collect();

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

    for path in &torrent_files {
        match std::fs::read(path) {
            Ok(data) => match engine.add_torrent_from_bytes(data) {
                Ok(hash) => {
                    tracing::info!("Added torrent from {} -> {}", path, hash);
                }
                Err(e) => {
                    tracing::error!("Failed to add {}: {}", path, e);
                }
            },
            Err(e) => tracing::error!("Failed to read {}: {}", path, e),
        }
    }

    {
        let hashes = engine.get_all_info_hashes();
        for hash in hashes {
            engine.start_torrent(&hash, &rt);
        }
    }

    if headless {
        return run_headless(engine, rt);
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

fn run_headless(engine: Arc<TorrentEngine>, rt: Arc<tokio::runtime::Runtime>) -> Result<()> {
    tracing::info!("Headless mode: Ctrl-C to stop. Stats every 5s.");
    let engine_for_loop = engine.clone();
    rt.block_on(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => break,
                _ = tick.tick() => {
                    for hash in engine.get_all_info_hashes() {
                        if let Some(session) = engine.get_session(&hash) {
                            let s = session.stats.lock().clone();
                            tracing::info!(
                                "{} progress={:5.2}% down={:>10}/s up={:>10}/s peers={:>3} seeders={} leechers={} state={:?}",
                                hash,
                                s.progress * 100.0,
                                human_bytes(s.download_rate),
                                human_bytes(s.upload_rate),
                                s.connected_peers,
                                s.seeders,
                                s.leechers,
                                s.state,
                            );
                        }
                    }
                }
            }
        }
    });
    tracing::info!("Shutting down, saving resume data...");
    engine_for_loop.save_all_resume();
    Ok(())
}

fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{:.1} {}", v, UNITS[u])
}
