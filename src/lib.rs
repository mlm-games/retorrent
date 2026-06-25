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
#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
use tray::TrayCommand;

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
mod single_instance {
    use std::path::PathBuf;

    pub fn socket_path() -> PathBuf {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("retorrent")
            .join("retorrent.sock")
    }
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
    let mut torrent_files: Vec<String> = Vec::new();
    let mut magnet_uris: Vec<String> = Vec::new();
    for a in args.iter().skip(1) {
        if skip_next {
            skip_next = false;
            continue;
        }
        if *a == "--download-dir" {
            skip_next = true;
            continue;
        }
        if a.starts_with("--") {
            continue;
        }
        if a.starts_with("magnet:?") || a.starts_with("magnet:") {
            magnet_uris.push(a.clone());
        } else {
            let path = a.strip_prefix("file://").unwrap_or(a).to_string();
            torrent_files.push(path);
        }
    }

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

    let minimize_to_tray = config.minimize_to_tray;
    let (engine, auto_start) = rt.block_on(async { TorrentEngine::new(config).await });
    let engine = Arc::new(engine);

    let engine_for_shutdown = engine.clone();

    // Single-instance IPC: forward CLI args to an already-running instance.
    if !headless {
        let socket_path = single_instance::socket_path();
        let forwarded = rt.block_on(async {
            use tokio::io::AsyncWriteExt;

            if let Ok(mut stream) = tokio::net::UnixStream::connect(&socket_path).await {
                let mut skip_next = false;
                for a in args.iter().skip(1) {
                    if skip_next { skip_next = false; continue; }
                    if *a == "--download-dir" { skip_next = true; continue; }
                    if a.starts_with("--") { continue; }
                    let _ = stream.write_all(format!("{}\n", a).as_bytes()).await;
                }
                return true;
            }

            // No existing instance — bind socket for future instances.
            let _ = std::fs::remove_file(&socket_path);
            if let Ok(listener) = tokio::net::UnixListener::bind(&socket_path) {
                let eng = engine.clone();
                let rt_clone = rt.clone();
                tokio::spawn(async move {
                    loop {
                        match listener.accept().await {
                            Ok((stream, _)) => {
                                let eng = eng.clone();
                                let rt = rt_clone.clone();
                                tokio::spawn(async move {
                                    use tokio::io::AsyncBufReadExt;
                                    let mut reader = tokio::io::BufReader::new(stream);
                                    let mut line = String::new();
                                    loop {
                                        match reader.read_line(&mut line).await {
                                            Ok(0) => break,
                                            Ok(_) => {
                                                let arg = line.trim().to_string();
                                                if arg.starts_with("magnet:?") || arg.starts_with("magnet:") {
                                                    match eng.add_torrent_from_magnet(&arg, None) {
                                                        Ok(hash) => eng.start_torrent(&hash, &rt),
                                                        Err(e) => tracing::error!("IPC magnet: {}", e),
                                                    }
                                                } else {
                                                    let path = arg.strip_prefix("file://").unwrap_or(&arg);
                                                    match std::fs::read(path) {
                                                        Ok(data) => match MetaInfo::from_bytes(&data) {
                                                            Ok(_) => match eng.add_torrent_from_bytes(data, None, None) {
                                                                Ok(hash) => eng.start_torrent(&hash, &rt),
                                                                Err(e) => tracing::error!("IPC torrent: {}", e),
                                                            },
                                                            Err(e) => tracing::error!("IPC torrent parse: {}", e),
                                                        },
                                                        Err(e) => tracing::error!("IPC torrent read: {}", e),
                                                    }
                                                }
                                                line.clear();
                                            }
                                            Err(e) => {
                                                tracing::error!("IPC read error: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                tracing::error!("IPC accept error: {}", e);
                                break;
                            }
                        }
                    }
                });
                tracing::info!("IPC socket bound at {:?}", socket_path);
            } else {
                // Race: another instance bound between our connect and bind.
                // Try again — but if we reach here we just run without IPC.
                tracing::debug!("IPC socket bind failed at {:?}", socket_path);
            }
            false
        });
        if forwarded {
            return Ok(());
        }
    }

    let pending: Arc<Mutex<Vec<PendingTorrent>>> = Arc::new(Mutex::new(Vec::new()));
    let suggested_dir =
        dirs::download_dir().unwrap_or_else(|| engine.config_read().download_dir.clone());
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

    for magnet in &magnet_uris {
        match engine.add_torrent_from_magnet(magnet, None) {
            Ok(hash) => {
                engine.start_torrent(&hash, &rt);
            }
            Err(e) => tracing::error!("Failed to add magnet {}: {}", magnet, e),
        }
    }

    for hash in &auto_start {
        engine.start_torrent(hash, &rt);
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

    repose_platform::set_close_to_tray(true);

    // hide/show toggles work even when the window is hidden
    let hide_on_start = minimize_to_tray;
    {
        let rx = tray_cmd_rx.clone();
        let eng = engine.clone();
        repose_platform::set_about_to_wait_callback(Box::new(move || {
            use std::sync::atomic::{AtomicBool, Ordering};
            static FIRST_FRAME: AtomicBool = AtomicBool::new(true);
            if hide_on_start && FIRST_FRAME.swap(false, Ordering::Relaxed) {
                repose_platform::hide_app_window();
            }
            if let Ok(guard) = rx.lock() {
                while let Ok(cmd) = guard.try_recv() {
                    match cmd {
                        TrayCommand::ToggleWindow => {
                            let vis = repose_platform::window_is_visible();
                            tracing::info!("TrayCommand::ToggleWindow: window_is_visible={vis}");
                            if vis {
                                repose_platform::hide_app_window();
                            } else {
                                repose_platform::show_app_window();
                            }
                        }
                        TrayCommand::Quit => {
                            tracing::info!("TrayCommand::Quit: saving and exiting");
                            eng.save_all_resume();
                            std::process::exit(0);
                        }
                    }
                }
            }
        }));
    }

    repose_platform::run_desktop_app(move |sched, _rc| {
        ui::app::app(
            sched,
            engine.clone(),
            rt.clone(),
            tray.clone(),
            pending.clone(),
        )
    })?;

    let _ = std::fs::remove_file(single_instance::socket_path());
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

    jni_min_helper::jni_with_env(|env| -> Result<(), jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let jobj = unsafe { jni::objects::JObject::from_raw(env, ctx.as_raw()) };
        rustls_platform_verifier::android::init_with_env(env, jobj)?;
        Ok(())
    })
    .expect("rustls-platform-verifier Android init failed");

    if jni_min_helper::android_api_level() >= 33 {
        let _ = jni_min_helper::PermissionRequest::request(
            "Downloads",
            ["android.permission.POST_NOTIFICATIONS"],
        );
    }

    let android_data_dir =
        jni_min_helper::jni_with_env(|env| -> Result<PathBuf, jni::errors::Error> {
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
        .unwrap_or_else(|_| PathBuf::from("/data/data/org.mlm.retorrent"));
    config::ANDROID_DATA_DIR.set(android_data_dir.clone()).ok();

    // Read the launch intent saved by RetorrentActivity.onCreate to a file.
    let pending_intent_path = android_data_dir.join("pending_intent");
    if pending_intent_path.exists() {
        match std::fs::read(&pending_intent_path) {
            Ok(bytes) => {
                repose_platform::push_deeplink(bytes);
                let _ = std::fs::remove_file(&pending_intent_path);
                tracing::info!("init_deeplink: pushed launch intent from file");
            }
            Err(e) => tracing::error!("init_deeplink: failed to read file: {e}"),
        }
    }

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
        .unwrap_or_else(|| android_data_dir.clone())
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
    let download_dir = config.download_dir.clone();
    let (engine, auto_start) = rt.block_on(async { TorrentEngine::new(config).await });
    let engine = Arc::new(engine);
    android_service::ENGINE.set(engine.clone()).ok();
    android_service::RUNTIME.set(rt.clone()).ok();

    for hash in &auto_start {
        engine.start_torrent(hash, &rt);
    }

    let _ = jni_min_helper::jni_with_env(|env| -> Result<(), jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let intent = env.new_object(jni_str!("android/content/Intent"), jni_sig!("()V"), &[])?;
        let svc_name = JString::new(env, "org.mlm.retorrent.TorrentService")?;
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

    // Register deeplink callback — processes magnets and .torrent files from
    // both the initial launch intent and any subsequent onNewIntent calls.
    let engine_clone = engine.clone();
    let rt_clone = rt.clone();
    repose_platform::set_on_deeplink(Box::new(move |data: Vec<u8>| {
        if let Ok(text) = String::from_utf8(data.clone()) {
            if text.starts_with("magnet:?") {
                tracing::info!("deeplink: magnet {}", &text[..80.min(text.len())]);
                match engine_clone.add_torrent_from_magnet(&text, None) {
                    Ok(hash) => {
                        engine_clone.start_torrent(&hash, &rt_clone);
                    }
                    Err(e) => tracing::error!("deeplink: add magnet failed: {e}"),
                }
                return;
            }
        }
        tracing::info!("deeplink: {} bytes as .torrent", data.len());
        android_service::queue_torrent_bytes(data);
    }));

    // Background notification updater
    {
        let engine_notif = engine.clone();
        rt.spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let hashes = engine_notif.get_all_info_hashes();
                let mut total_progress = 0.0f64;
                let mut active = 0usize;
                let mut global_dl = 0u64;
                let mut global_ul = 0u64;
                for hash in &hashes {
                    if let Some(session) = engine_notif.get_session(hash) {
                        let s = session.stats.lock();
                        total_progress += s.progress as f64;
                        if matches!(
                            s.state,
                            crate::types::TorrentState::Downloading
                                | crate::types::TorrentState::FetchingMetadata
                        ) {
                            active += 1;
                        }
                        global_dl += s.download_rate;
                        global_ul += s.upload_rate;
                    }
                }
                let avg_progress = if hashes.is_empty() {
                    0.0
                } else {
                    total_progress / hashes.len() as f64
                };
                if !hashes.is_empty() {
                    android_service::acquire_wake_lock_if_needed();
                } else {
                    android_service::release_wake_lock_if_held();
                }
                android_service::update_notification(
                    &format!(
                        "\u{2B07} {}  \u{2B06} {}",
                        crate::human_bytes(global_dl),
                        crate::human_bytes(global_ul)
                    ),
                    &format!("{} active / {} torrents", active, hashes.len()),
                    avg_progress,
                );
            }
        });
    }

    let engine_for_shutdown = engine.clone();
    let pending: Arc<Mutex<Vec<PendingTorrent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let mut pending_guard = pending.lock().unwrap();
        for mut p in android_service::drain_pending_intents() {
            p.suggested_dir = download_dir.clone();
            pending_guard.push(p);
        }
    }

    use repose_platform::android::run_android_app;

    let _ = run_android_app(android_app, move |sched, _rc| {
        ui::app::app(sched, engine.clone(), rt.clone(), pending.clone())
    });

    engine_for_shutdown.save_all_resume();

    let _ = jni_min_helper::jni_with_env(|env| -> Result<(), jni::errors::Error> {
        let ctx = jni_min_helper::android_context();
        let intent = env.new_object(jni_str!("android/content/Intent"), jni_sig!("()V"), &[])?;
        let svc_name = JString::new(env, "org.mlm.retorrent.TorrentService")?;
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
