use crate::PendingTorrent;
use crate::engine::TorrentEngine;
use crate::metainfo::MetaInfo;
use jni::Env;
use jni::errors;
use jni::errors::ThrowRuntimeExAndDefault;
use jni::objects::{Global, JByteArray, JObject, JString, JValue};
use jni::sys::{jbyteArray, jint};
use jni::{jni_sig, jni_str};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

pub static ENGINE: OnceLock<Arc<TorrentEngine>> = OnceLock::new();
pub static RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();

static SERVICE_GLOBAL: OnceLock<Global<JObject<'static>>> = OnceLock::new();
static WAKE_LOCK: OnceLock<Mutex<Option<Global<JObject<'static>>>>> = OnceLock::new();

static PENDING_FROM_INTENT: OnceLock<Mutex<Vec<PendingTorrent>>> = OnceLock::new();

pub const STAT_SYS_DOWNLOAD: jint = 17301637;

fn jstr<'local>(env: &mut Env<'local>, s: &str) -> errors::Result<JString<'local>> {
    JString::new(env, s)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_mlm_retorrent_TorrentService_nativeOnCreate<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: jni::sys::jclass,
    context: JObject<'local>,
) {
    env.with_env(|env| -> errors::Result<()> {
        let ctx_global = env.new_global_ref(&context)?;
        SERVICE_GLOBAL
            .set(ctx_global)
            .map_err(|_| errors::Error::JniCall(errors::JniError::Unknown))?;

        let s_downloads = jstr(env, "retorrent_downloads")?;
        let s_downloads_label = jstr(env, "Downloads")?;
        let s_notification = jstr(env, "notification")?;
        let s_retorrent = jstr(env, "Retorrent")?;
        let s_starting = jstr(env, "Starting...")?;
        let s_power = jstr(env, "power")?;
        let s_wakelock = jstr(env, "retorrent:wakelock")?;

        let channel = env.new_object(
            jni_str!("android/app/NotificationChannel"),
            jni_sig!("(Ljava/lang/String;Ljava/lang/CharSequence;I)V"),
            &[
                JValue::from(&s_downloads),
                JValue::from(&s_downloads_label),
                JValue::Int(2),
            ],
        )?;
        let manager = env
            .call_method(
                &context,
                jni_str!("getSystemService"),
                jni_sig!("(Ljava/lang/String;)Ljava/lang/Object;"),
                &[JValue::from(&s_notification)],
            )?
            .l()?;
        env.call_method(
            &manager,
            jni_str!("createNotificationChannel"),
            jni_sig!("(Landroid/app/NotificationChannel;)V"),
            &[JValue::from(&channel)],
        )?;

        let builder = env.new_object(
            jni_str!("android/app/Notification$Builder"),
            jni_sig!("(Landroid/content/Context;Ljava/lang/String;)V"),
            &[JValue::from(&context), JValue::from(&s_downloads)],
        )?;
        env.call_method(
            &builder,
            jni_str!("setContentTitle"),
            jni_sig!("(Ljava/lang/CharSequence;)Landroid/app/Notification$Builder;"),
            &[JValue::from(&s_retorrent)],
        )?;
        env.call_method(
            &builder,
            jni_str!("setContentText"),
            jni_sig!("(Ljava/lang/CharSequence;)Landroid/app/Notification$Builder;"),
            &[JValue::from(&s_starting)],
        )?;
        env.call_method(
            &builder,
            jni_str!("setSmallIcon"),
            jni_sig!("(I)Landroid/app/Notification$Builder;"),
            &[JValue::Int(STAT_SYS_DOWNLOAD)],
        )?;
        env.call_method(
            &builder,
            jni_str!("setOngoing"),
            jni_sig!("(Z)Landroid/app/Notification$Builder;"),
            &[JValue::Bool(true)],
        )?;

        // Initial indeterminate progress bar (need to poll)
        env.call_method(
            &builder,
            jni_str!("setProgress"),
            jni_sig!("(IIZ)Landroid/app/Notification$Builder;"),
            &[JValue::Int(0), JValue::Int(0), JValue::Bool(true)],
        )?;

        let notif = env
            .call_method(
                &builder,
                jni_str!("build"),
                jni_sig!("()Landroid/app/Notification;"),
                &[],
            )?
            .l()?;

        env.call_method(
            &context,
            jni_str!("startForeground"),
            jni_sig!("(ILandroid/app/Notification;)V"),
            &[JValue::Int(1), JValue::from(&notif)],
        )?;

        let power = env
            .call_method(
                &context,
                jni_str!("getSystemService"),
                jni_sig!("(Ljava/lang/String;)Ljava/lang/Object;"),
                &[JValue::from(&s_power)],
            )?
            .l()?;
        let wl = env
            .call_method(
                &power,
                jni_str!("newWakeLock"),
                jni_sig!("(ILjava/lang/String;)Landroid/os/PowerManager$WakeLock;"),
                &[JValue::Int(1), JValue::from(&s_wakelock)],
            )?
            .l()?;
        const TEN_MINUTES: i64 = 10 * 60 * 1000;
        env.call_method(
            &wl,
            jni_str!("acquire"),
            jni_sig!("(J)V"),
            &[JValue::Long(TEN_MINUTES)],
        )?;
        let wl_global = env.new_global_ref(&wl)?;
        WAKE_LOCK
            .get_or_init(|| Mutex::new(None))
            .lock()
            .unwrap()
            .replace(wl_global);

        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_mlm_retorrent_TorrentService_nativeOnDestroy<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: jni::sys::jclass,
) {
    env.with_env(|_env| -> errors::Result<()> {
        if let Some(e) = ENGINE.get() {
            e.save_all_resume();
        }
        if let Some(lock) = WAKE_LOCK.get() {
            if let Some(wl) = lock.lock().unwrap().take() {
                let _ = jni_min_helper::jni_with_env(|inner_env| {
                    let wl = unsafe { JObject::from_raw(inner_env, wl.as_raw()) };
                    inner_env.call_method(&wl, jni_str!("release"), jni_sig!("()V"), &[])?;
                    Ok::<_, errors::Error>(())
                });
            }
        }
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

pub fn update_notification(title: &str, text: &str, progress: f64) {
    let ctx_global = match SERVICE_GLOBAL.get() {
        Some(r) => r,
        None => return,
    };

    let _ = jni_min_helper::jni_with_env(|env| -> errors::Result<()> {
        let svc = unsafe { JObject::from_raw(env, ctx_global.as_raw()) };

        env.with_local_frame(16, |env| {
            let s_channel_id = jstr(env, "retorrent_downloads")?;
            let s_title = jstr(env, title)?;
            let s_text = jstr(env, text)?;

            let builder = env.new_object(
                jni_str!("android/app/Notification$Builder"),
                jni_sig!("(Landroid/content/Context;Ljava/lang/String;)V"),
                &[JValue::from(&svc), JValue::from(&s_channel_id)],
            )?;
            env.call_method(
                &builder,
                jni_str!("setContentTitle"),
                jni_sig!("(Ljava/lang/CharSequence;)Landroid/app/Notification$Builder;"),
                &[JValue::from(&s_title)],
            )?;
            env.call_method(
                &builder,
                jni_str!("setContentText"),
                jni_sig!("(Ljava/lang/CharSequence;)Landroid/app/Notification$Builder;"),
                &[JValue::from(&s_text)],
            )?;
            env.call_method(
                &builder,
                jni_str!("setSmallIcon"),
                jni_sig!("(I)Landroid/app/Notification$Builder;"),
                &[JValue::Int(STAT_SYS_DOWNLOAD)],
            )?;
            env.call_method(
                &builder,
                jni_str!("setOngoing"),
                jni_sig!("(Z)Landroid/app/Notification$Builder;"),
                &[JValue::Bool(true)],
            )?;

            let progress_int = (progress.clamp(0.0, 1.0) * 1000.0) as i32;

            if jni_min_helper::android_api_level() >= 36 {
                if let Ok(progress_style) = env.new_object(
                    jni_str!("android/app/Notification$ProgressStyle"),
                    jni_sig!("()V"),
                    &[],
                ) {
                    if progress_int > 0 {
                        if let Ok(segment) = env.new_object(
                            jni_str!("android/app/Notification$ProgressStyle$Segment"),
                            jni_sig!("(I)V"),
                            &[JValue::Int(progress_int)],
                        ) {
                            let _ = env.call_method(
                                &progress_style,
                                jni_str!("addProgressSegment"),
                                jni_sig!("(Landroid/app/Notification$ProgressStyle$Segment;)Landroid/app/Notification$ProgressStyle;"),
                                &[JValue::from(&segment)],
                            );
                        }
                    }
                    if progress_int < 1000 {
                        let remaining = 1000 - progress_int;
                        if let Ok(segment) = env.new_object(
                            jni_str!("android/app/Notification$ProgressStyle$Segment"),
                            jni_sig!("(I)V"),
                            &[JValue::Int(remaining)],
                        ) {
                            let _ = env.call_method(
                                &progress_style,
                                jni_str!("addProgressSegment"),
                                jni_sig!("(Landroid/app/Notification$ProgressStyle$Segment;)Landroid/app/Notification$ProgressStyle;"),
                                &[JValue::from(&segment)],
                            );
                        }
                    }
                    let _ = env.call_method(
                        &progress_style,
                        jni_str!("setProgress"),
                        jni_sig!("(I)Landroid/app/Notification$ProgressStyle;"),
                        &[JValue::Int(progress_int)],
                    );
                    let _ = env.call_method(
                        &builder,
                        jni_str!("setStyle"),
                        jni_sig!(
                            "(Landroid/app/Notification$Style;)Landroid/app/Notification$Builder;"
                        ),
                        &[JValue::from(&progress_style)],
                    );
                }
            } else {
                env.call_method(
                    &builder,
                    jni_str!("setProgress"),
                    jni_sig!("(IIZ)Landroid/app/Notification$Builder;"),
                    &[
                        JValue::Int(1000),
                        JValue::Int(progress_int),
                        JValue::Bool(false),
                    ],
                )?;
            }

            let notif = env
                .call_method(
                    &builder,
                    jni_str!("build"),
                    jni_sig!("()Landroid/app/Notification;"),
                    &[],
                )?
                .l()?;

            env.call_method(
                &svc,
                jni_str!("startForeground"),
                jni_sig!("(ILandroid/app/Notification;)V"),
                &[JValue::Int(1), JValue::from(&notif)],
            )?;
            Ok(())
        })
    });
}

pub fn acquire_wake_lock_if_needed() {
    let lock = match WAKE_LOCK.get() {
        Some(l) => l,
        None => return,
    };
    let mut guard = lock.lock().unwrap();
    if guard.is_some() {
        return;
    }
    let ctx_global = match SERVICE_GLOBAL.get() {
        Some(r) => r,
        None => return,
    };

    let _ = jni_min_helper::jni_with_env(|env| -> errors::Result<()> {
        let svc = unsafe { JObject::from_raw(env, ctx_global.as_raw()) };
        let s_power = jstr(env, "power")?;
        let s_wakelock = jstr(env, "retorrent:wakelock")?;

        let power = env
            .call_method(
                &svc,
                jni_str!("getSystemService"),
                jni_sig!("(Ljava/lang/String;)Ljava/lang/Object;"),
                &[JValue::from(&s_power)],
            )?
            .l()?;
        let wl = env
            .call_method(
                &power,
                jni_str!("newWakeLock"),
                jni_sig!("(ILjava/lang/String;)Landroid/os/PowerManager$WakeLock;"),
                &[JValue::Int(1), JValue::from(&s_wakelock)],
            )?
            .l()?;
        const TEN_MINUTES: i64 = 10 * 60 * 1000;
        env.call_method(
            &wl,
            jni_str!("acquire"),
            jni_sig!("(J)V"),
            &[JValue::Long(TEN_MINUTES)],
        )?;
        let wl_global = env.new_global_ref(&wl)?;
        guard.replace(wl_global);
        Ok(())
    });
}

pub fn release_wake_lock_if_held() {
    let lock = match WAKE_LOCK.get() {
        Some(l) => l,
        None => return,
    };
    let mut guard = lock.lock().unwrap();
    if let Some(wl) = guard.take() {
        let _ = jni_min_helper::jni_with_env(|env| {
            let wl = unsafe { JObject::from_raw(env, wl.as_raw()) };
            env.call_method(&wl, jni_str!("release"), jni_sig!("()V"), &[])?;
            Ok::<_, errors::Error>(())
        });
    }
}

pub fn queue_torrent_bytes(bytes: Vec<u8>) {
    let meta = match MetaInfo::from_bytes(&bytes) {
        Ok(m) => m,
        Err(_) => return,
    };
    let suggested_dir = PathBuf::from("/sdcard/Retorrent/downloads");
    let pending = PendingTorrent {
        name: meta.name,
        total_size: meta.total_size,
        files: meta.files,
        data: bytes,
        suggested_dir,
    };
    let storage = PENDING_FROM_INTENT.get_or_init(|| Mutex::new(Vec::new()));
    storage.lock().unwrap().push(pending);
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_mlm_retorrent_TorrentService_nativeOnTorrentData<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: jni::sys::jclass,
    data: jbyteArray,
) {
    env.with_env(|env| -> errors::Result<()> {
        let array = unsafe { JByteArray::from_raw(env, data) };
        let bytes = env.convert_byte_array(&array)?;
        queue_torrent_bytes(bytes);
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

pub fn drain_pending_intents() -> Vec<PendingTorrent> {
    let storage = PENDING_FROM_INTENT.get_or_init(|| Mutex::new(Vec::new()));
    std::mem::take(&mut *storage.lock().unwrap())
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_mlm_retorrent_RetorrentActivity_nativeOnNewIntent<'local>(
    mut env: jni::EnvUnowned<'local>,
    _class: jni::sys::jclass,
    data: jbyteArray,
) {
    env.with_env(|env| -> errors::Result<()> {
        let array = unsafe { JByteArray::from_raw(env, data) };
        let bytes = env.convert_byte_array(&array)?;
        tracing::info!(
            "nativeOnNewIntent: forwarding {} bytes to repose deeplink API",
            bytes.len()
        );
        repose_platform::push_deeplink(bytes);
        Ok(())
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_org_mlm_retorrent_RetorrentActivity_nativeOnWindowInsets<'local>(
    _env: jni::EnvUnowned<'local>,
    _class: jni::sys::jclass,
    top_px: jni::sys::jfloat,
    bottom_px: jni::sys::jfloat,
    left_px: jni::sys::jfloat,
    right_px: jni::sys::jfloat,
    ime_bottom_px: jni::sys::jfloat,
) {
    let insets = repose_core::locals::WindowInsets {
        top: top_px,
        bottom: bottom_px,
        left: left_px,
        right: right_px,
        ime_bottom: ime_bottom_px,
    };
    repose_core::locals::set_window_insets_default(insets);
}
