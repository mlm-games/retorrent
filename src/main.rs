mod bencode;
mod config;
mod dht;
mod engine;
mod error;
mod metainfo;
mod nat;
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
use metainfo::MetaInfo;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Clone)]
pub struct PendingTorrent {
    pub name: String,
    pub total_size: u64,
    pub files: Vec<crate::metainfo::FileInfo>,
    pub data: Vec<u8>,
    pub suggested_dir: PathBuf,
}

fn main() -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let headless = args.iter().any(|a| a == "--headless");

    let mut download_dir_arg: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--download-dir" && i + 1 < args.len() {
            download_dir_arg = Some(args[i + 1].clone());
            i += 2;
            continue;
        }
        i += 1;
    }

    let mut skip_next = false;
    let torrent_files: Vec<String> = args
        .iter()
        .skip(1)
        .filter(|a| {
            if skip_next {
                skip_next = false;
                return false;
            }
            if *a == "--download-dir" {
                skip_next = true;
                return false;
            }
            !a.starts_with("--")
        })
        .cloned()
        .collect();

    let mut config = Config::load_or_default();
    if let Some(dir) = download_dir_arg {
        config.download_dir = std::path::PathBuf::from(dir);
    }
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
    let pending: Arc<Mutex<Vec<PendingTorrent>>> = Arc::new(Mutex::new(Vec::new()));
    let suggested_dir = dirs::download_dir().unwrap_or_else(|| engine.config.download_dir.clone());
    for path in &torrent_files {
        match std::fs::read(path) {
            Ok(data) => match MetaInfo::from_bytes(&data) {
                Ok(meta) => {
                    pending.lock().unwrap().push(PendingTorrent {
                        name: meta.name.clone(),
                        total_size: meta.total_size,
                        files: meta.files.clone(),
                        data,
                        suggested_dir: suggested_dir.clone(),
                    });
                }
                Err(e) => tracing::error!("Failed to parse {}: {}", path, e),
            },
            Err(e) => tracing::error!("Failed to read {}: {}", path, e),
        }
    }

    if headless {
        // No UI: auto-add and start everything from the intent.
        let pending = Arc::try_unwrap(pending)
            .map(|m| m.into_inner().unwrap_or_default())
            .unwrap_or_default();
        for p in pending {
            if let Ok(hash) = engine.add_torrent_from_bytes(p.data, Some(p.suggested_dir), None) {
                engine.start_torrent(&hash, &rt);
            }
        }
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
            pending.clone(),
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
