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
mod types;
mod webseed;

#[cfg(any(
    target_os = "linux",
    target_os = "windows",
    target_os = "macos",
    target_os = "android"
))]
mod ui;

#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
mod tray;

#[cfg(target_os = "android")]
mod android_service;
#[cfg(target_os = "android")]
use jni::{jni_sig, jni_str};

use anyhow::Result;
use config::Config;
use engine::TorrentEngine;
use metainfo::MetaInfo;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct PendingTorrent {
    pub name: String,
    pub total_size: u64,
    pub files: Vec<crate::metainfo::FileInfo>,
    pub data: Vec<u8>,
    pub suggested_dir: PathBuf,
}

pub fn human_bytes(n: u64) -> String {
    const UNITS: &[&str] = &["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    format!("{:.1} {}", v, UNITS[u])
}

pub fn run_headless(engine: Arc<TorrentEngine>, rt: Arc<tokio::runtime::Runtime>) -> Result<()> {
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

#[cfg(feature = "desktop-bin")]
pub fn run_desktop_main() -> Result<()> {
    use std::sync::mpsc;
    use tracing_subscriber::{EnvFilter, fmt};

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

#[cfg(target_os = "android")]
#[unsafe(no_mangle)]
pub extern "C" fn android_main(android_app: winit::platform::android::activity::AndroidApp) {
    use jni::objects::{JObject, JString, JValue};

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    tracing::info!("Retorrent starting on Android");

    rlobkit_dialogs::init();

    if jni_min_helper::android_api_level() >= 33 {
        let _ = jni_min_helper::PermissionRequest::request(
            "Downloads",
            ["android.permission.POST_NOTIFICATIONS"],
        );
    }

    let android_data_dir = jni_min_helper::jni_with_env(|env| -> Result<PathBuf, jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let file_obj = env
            .call_method(
                ctx,
                jni_str!("getFilesDir"),
                jni_sig!("()Ljava/io/File;"),
                &[],
            )?
            .l()?;
        let jpath = env
            .call_method(
                &file_obj,
                jni_str!("getAbsolutePath"),
                jni_sig!("()Ljava/lang/String;"),
                &[],
            )?
            .l()?;
        let s: String = JString::cast_local(env, jpath)?.try_to_string(env)?;
        Ok(PathBuf::from(s))
    })
    .unwrap_or_else(|_| PathBuf::from("/data/data/dev.mlm.retorrent"));
    config::ANDROID_DATA_DIR.set(android_data_dir).ok();

    let download_dir = {
        let dir = jni_min_helper::jni_with_env(|env| -> Result<PathBuf, jni::errors::Error> {
            let ctx = jni_min_helper::android_context();
            let file_obj = env
                .call_method(
                    ctx,
                    jni_str!("getExternalFilesDir"),
                    jni_sig!("(Ljava/lang/String;)Ljava/io/File;"),
                    &[JValue::Object(&JObject::null())],
                )?
                .l()?;
            let jpath = env
                .call_method(
                    &file_obj,
                    jni_str!("getAbsolutePath"),
                    jni_sig!("()Ljava/lang/String;"),
                    &[],
                )?
                .l()?;
            let s: String = JString::cast_local(env, jpath)?.try_to_string(env)?;
            Ok(PathBuf::from(s))
        })
        .ok()
        .unwrap_or_else(|| PathBuf::from("/sdcard"))
        .join("Retorrent/downloads");
        let _ = std::fs::create_dir_all(&dir);
        dir
    };

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("Failed to build Tokio runtime"),
    );
    let mut config = Config::load_or_default();
    config.download_dir = download_dir;
    let engine = rt.block_on(async { Arc::new(TorrentEngine::new(config).await) });
    android_service::ENGINE.set(engine.clone()).ok();
    android_service::RUNTIME.set(rt.clone()).ok();

    let _ = jni_min_helper::jni_with_env(|env| -> Result<(), jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let intent = env.new_object(jni_str!("android/content/Intent"), jni_sig!("()V"), &[])?;
        let svc_name = JString::new(env, "dev.mlm.retorrent.TorrentService")?;
        env.call_method(
            &intent,
            jni_str!("setClassName"),
            jni_sig!("(Landroid/content/Context;Ljava/lang/String;)Landroid/content/Intent;"),
            &[JValue::from(ctx), JValue::from(&svc_name)],
        )?;
        env.call_method(
            ctx,
            jni_str!("startService"),
            jni_sig!("(Landroid/content/Intent;)Landroid/content/ComponentName;"),
            &[JValue::from(&intent)],
        )?;
        Ok(())
    });

    let engine_for_shutdown = engine.clone();
    let pending: Arc<Mutex<Vec<PendingTorrent>>> = Arc::new(Mutex::new(Vec::new()));

    use repose_platform::android::run_android_app;

    let _ = run_android_app(android_app, move |sched, _rc| {
        ui::app::app(sched, engine.clone(), rt.clone(), pending.clone())
    });

    engine_for_shutdown.save_all_resume();

    let _ = jni_min_helper::jni_with_env(|env| -> Result<(), jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let intent = env.new_object(jni_str!("android/content/Intent"), jni_sig!("()V"), &[])?;
        let svc_name = JString::new(env, "dev.mlm.retorrent.TorrentService")?;
        env.call_method(
            &intent,
            jni_str!("setClassName"),
            jni_sig!("(Landroid/content/Context;Ljava/lang/String;)Landroid/content/Intent;"),
            &[JValue::from(ctx), JValue::from(&svc_name)],
        )?;
        env.call_method(
            ctx,
            jni_str!("stopService"),
            jni_sig!("(Landroid/content/Intent;)Z"),
            &[JValue::from(&intent)],
        )?;
        Ok(())
    });
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(start)]
pub fn wasm_start() -> Result<(), wasm_bindgen::prelude::JsValue> {
    console_error_panic_hook::set_once();
    console_log::init_with_level(log::Level::Info).expect("Failed to init console_log");

    log::info!("Retorrent starting on WASM");
    Ok(())
}
