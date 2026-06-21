use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

#[cfg(target_os = "android")]
use crate::android_service;

use repose_core::locals::set_theme_default;
use repose_core::modifier::{PaddingValues, StateColors};
use repose_core::prelude::*;
use repose_material::material3::dialog::{Dialog, DialogState};
use repose_material::material3::{
    self, Checkbox, FilledButton, FilledTonalButton, IconButton, Switch, TabRow, TextButton,
};
use repose_ui::overlay::OverlayHandle;
use repose_ui::scroll::{ScrollArea, remember_scroll_state};
use repose_ui::{textfield::TextField, *};

use crate::PendingTorrent;
use crate::config::Config;
use crate::engine::TorrentEngine;
use crate::metainfo::{FileInfo, MetaInfo};
use crate::network::TorrentStats;
#[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
use crate::tray::AppTray;
use crate::types::*;
use crate::ui::components;
use crate::ui::icons::{Symbols, icon};
use crate::ui::theme;
use crate::ui::utils::*;
use repose_material::Symbol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tab {
    General,
    Files,
    Peers,
    Trackers,
    Pieces,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterState {
    All,
    Downloading,
    Seeding,
    Paused,
    Complete,
}

#[derive(Clone)]
struct TorrentRow {
    info_hash: InfoHash,
    name: String,
    stats: TorrentStats,
    total_size: u64,
    have_pieces: Vec<bool>,
    num_pieces: u32,
    piece_length: u64,
    files: Vec<crate::metainfo::FileInfo>,
    trackers: Vec<String>,
    file_priorities: Vec<FilePriority>,
    display_progress: f32,
}

fn matches_filter(t: &TorrentRow, filter: FilterState) -> bool {
    match filter {
        FilterState::All => true,
        FilterState::Downloading => {
            matches!(
                t.stats.state,
                TorrentState::Downloading | TorrentState::FetchingMetadata
            )
        }
        FilterState::Seeding => t.stats.state == TorrentState::Seeding,
        FilterState::Paused => t.stats.state == TorrentState::Paused,
        FilterState::Complete => {
            matches!(
                t.stats.state,
                TorrentState::Complete | TorrentState::Seeding
            )
        }
    }
}

fn torrent_state_color(state: TorrentState) -> Color {
    match state {
        TorrentState::Downloading => theme::downloading(),
        TorrentState::Seeding => theme::seeding(),
        TorrentState::Paused => theme::paused(),
        TorrentState::Complete => theme::success(),
        TorrentState::Error => theme::error(),
        TorrentState::FetchingMetadata => theme::metadata(),
        _ => theme::accent(),
    }
}

fn torrent_state_symbol(state: TorrentState) -> Symbol {
    match state {
        TorrentState::Downloading => Symbols::DOWNLOAD,
        TorrentState::Seeding => Symbols::UPLOAD,
        TorrentState::Paused => Symbols::PAUSE,
        TorrentState::Complete => Symbols::CHECK_CIRCLE,
        TorrentState::Error => Symbols::ERROR,
        TorrentState::FetchingMetadata => Symbols::PUBLIC,
        _ => Symbols::SCHEDULE,
    }
}

pub fn app(
    _sched: &mut Scheduler,
    engine: Arc<TorrentEngine>,
    rt: Arc<tokio::runtime::Runtime>,
    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))] tray: Arc<AppTray>,
    pending_torrents: Arc<Mutex<Vec<PendingTorrent>>>,
) -> View {
    set_theme_default(theme::dark_theme());

    let selected: Rc<Signal<Option<InfoHash>>> = remember(|| signal(None));
    let active_tab: Rc<Signal<Tab>> = remember(|| signal(Tab::General));
    let filter_state: Rc<Signal<FilterState>> = remember(|| signal(FilterState::All));
    let search_query: Rc<Signal<String>> = remember(|| signal(String::new()));
    let magnet_input: Rc<Signal<String>> = remember(|| signal(String::new()));
    let url_input: Rc<Signal<String>> = remember(|| signal(String::new()));
    let torrents: Rc<Signal<Vec<TorrentRow>>> = remember(|| signal(Vec::new()));
    let global_dl: Rc<Signal<u64>> = remember(|| signal(0));
    let global_ul: Rc<Signal<u64>> = remember(|| signal(0));
    let last_refresh: Rc<Signal<web_time::Instant>> = remember(|| signal(web_time::Instant::now()));

    let overlay = remember(|| OverlayHandle::new());
    let magnet_state = remember(|| DialogState::new());
    let url_state = remember(|| DialogState::new());
    let remove_state = remember(|| DialogState::new());
    let settings_state = remember(|| DialogState::new());
    let add_state = remember(|| DialogState::new());

    let pending: Rc<Signal<Vec<PendingTorrent>>> = remember(|| {
        let v = std::mem::take(&mut *pending_torrents.lock().unwrap());
        signal(v)
    });
    let current_add: Rc<Signal<Option<PendingTorrent>>> = remember(|| signal(None));
    let file_checks: Rc<Signal<Vec<bool>>> = remember(|| signal(Vec::new()));
    let download_path_input: Rc<Signal<String>> = remember(|| signal(String::new()));
    let add_taken = remember(|| signal(false));

    if !add_taken.get() {
        if let Some(next) = pending.get().first().cloned() {
            file_checks.set(vec![true; next.files.len()]);
            download_path_input.set(next.suggested_dir.to_string_lossy().to_string());
            current_add.set(Some(next));
            add_state.show();
        }
        add_taken.set(true);
    }

    let pending_from_button: Rc<Arc<Mutex<Vec<PendingTorrent>>>> =
        remember(|| Arc::new(Mutex::new(Vec::new())));

    if let Ok(mut p) = pending_from_button.lock() {
        if !p.is_empty() {
            let mut q = pending.get();
            q.extend(p.drain(..));
            pending.set(q);
            if current_add.get().is_none() {
                if let Some(next) = pending.get().first().cloned() {
                    file_checks.set(vec![true; next.files.len()]);
                    download_path_input.set(next.suggested_dir.to_string_lossy().to_string());
                    current_add.set(Some(next));
                    add_state.show();
                }
            }
        }
    }

    #[cfg(target_os = "android")]
    {
        let intent_pending = android_service::drain_pending_intents();
        if !intent_pending.is_empty() {
            if let Ok(mut p) = pending_from_button.lock() {
                p.extend(intent_pending);
            }
        }
    }

    // Periodic refresh
    let now = web_time::Instant::now();
    if now.duration_since(last_refresh.get()) > web_time::Duration::from_millis(500) {
        let mut rows = Vec::new();
        let mut dl_total = 0u64;
        let mut ul_total = 0u64;
        for hash in engine.get_all_info_hashes() {
            if let Some(session) = engine.get_session(&hash) {
                let stats = session.get_stats();
                dl_total += stats.download_rate;
                ul_total += stats.upload_rate;
                let meta = session.meta.read();
                let pm = session.piece_manager.read();
                let have = pm.get_have_vec();
                let files = meta.files.clone();
                let mut trackers = Vec::new();
                if let Some(ref a) = meta.announce {
                    trackers.push(a.clone());
                }
                for tier in &meta.announce_list {
                    trackers.extend(tier.iter().cloned());
                }
                let pl = meta.piece_length;
                let priorities = session.get_file_priorities();
                let display_progress = {
                    let mut total = 0u32;
                    let mut done = 0u32;
                    for (i, fp) in priorities.iter().enumerate() {
                        if *fp == FilePriority::Skip {
                            continue;
                        }
                        if i >= files.len() {
                            break;
                        }
                        let f = &files[i];
                        let first = (f.offset / pl) as usize;
                        let last = ((f.offset + f.length - 1) / pl) as usize;
                        for p in first..=last {
                            total += 1;
                            if p < have.len() && have[p] {
                                done += 1;
                            }
                        }
                    }
                    if total > 0 {
                        done as f32 / total as f32
                    } else {
                        0.0
                    }
                };

                rows.push(TorrentRow {
                    info_hash: hash,
                    name: meta.name.clone(),
                    stats,
                    total_size: meta.total_size,
                    have_pieces: have,
                    num_pieces: meta.num_pieces(),
                    piece_length: pl,
                    files,
                    trackers,
                    file_priorities: priorities,
                    display_progress,
                });
            }
        }
        torrents.set(rows);
        global_dl.set(dl_total);
        global_ul.set(ul_total);
        last_refresh.set(now);
    }

    #[cfg(any(target_os = "linux", target_os = "windows", target_os = "macos"))]
    {
        let rows = torrents.get();
        let active = rows
            .iter()
            .filter(|t| {
                matches!(
                    t.stats.state,
                    TorrentState::Downloading | TorrentState::Seeding
                )
            })
            .count();
        tray.set_tooltip(&format!(
            "Retorrent\n\u{2B07} {}  \u{2B06} {}\n{}/{} active",
            format_speed(global_dl.get()),
            format_speed(global_ul.get()),
            active,
            rows.len()
        ));
    }

    let all_torrents = torrents.get();

    // Filter and search
    let filter = filter_state.get();
    let query = search_query.get();
    let filtered_indices: Vec<usize> = all_torrents
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            let state_match = matches_filter(t, filter);
            let query_match =
                query.is_empty() || t.name.to_lowercase().contains(&query.to_lowercase());
            state_match && query_match
        })
        .map(|(i, _)| i)
        .collect();

    let selected_hash = selected.get();
    let selected_torrent =
        selected_hash.and_then(|hash| all_torrents.iter().find(|t| t.info_hash == hash).cloned());

    // Main content
    let content = Column(
        Modifier::new().fill_max_size(), // .system_bars_padding(), // .ime_padding(),
    )
    .child((
        top_bar_view(
            engine.clone(),
            rt.clone(),
            all_torrents.clone(),
            selected.clone(),
            magnet_state.clone(),
            url_state.clone(),
            remove_state.clone(),
            settings_state.clone(),
            global_dl.get(),
            global_ul.get(),
            (*pending_from_button).clone(),
        ),
        main_shell_view(
            &all_torrents,
            &filtered_indices,
            selected.clone(),
            active_tab.clone(),
            filter_state.clone(),
            search_query.clone(),
            selected_torrent,
            engine.clone(),
        ),
    ));

    // Wrap in overlay host so overlay entries (dialogs) are rendered
    overlay.host(
        Modifier::new().fill_max_size(),
        ZStack(Modifier::new().fill_max_size()).child((
            Box(Modifier::new()
                .fill_max_size()
                .background(theme().background))
            .child(content),
            magnet_dialog_view(
                magnet_state.clone(),
                (*overlay).clone(),
                magnet_input.clone(),
                engine.clone(),
                rt.clone(),
            ),
            url_dialog_view(
                url_state.clone(),
                (*overlay).clone(),
                url_input.clone(),
                engine.clone(),
                rt.clone(),
                (*pending_from_button).clone(),
            ),
            remove_dialog_view(
                remove_state.clone(),
                (*overlay).clone(),
                selected.clone(),
                engine.clone(),
            ),
            settings_dialog_view(settings_state.clone(), (*overlay).clone(), engine.clone()),
            add_torrent_dialog_view(
                add_state.clone(),
                (*overlay).clone(),
                current_add.clone(),
                pending.clone(),
                file_checks.clone(),
                download_path_input.clone(),
                engine.clone(),
                rt.clone(),
            ),
        )),
    )
}

fn main_shell_view(
    torrents: &[TorrentRow],
    filtered_indices: &[usize],
    selected: Rc<Signal<Option<InfoHash>>>,
    active_tab: Rc<Signal<Tab>>,
    filter_state: Rc<Signal<FilterState>>,
    search_query: Rc<Signal<String>>,
    selected_torrent: Option<TorrentRow>,
    engine: Arc<TorrentEngine>,
) -> View {
    let th = theme();

    Row(Modifier::new()
        .fill_max_size()
        .padding(12.0)
        .background(th.background))
    .child((
        Box(Modifier::new()
            .width(440.0)
            .fill_max_height()
            .background(th.surface_container_low)
            .border(1.0, th.outline_variant, 18.0)
            .clip_rounded(18.0))
        .child(Column(Modifier::new().fill_max_size()).child((
            filter_search_panel(filter_state, search_query),
            torrent_list_view(torrents, filtered_indices, selected),
        ))),
        Box(Modifier::new().width(12.0)),
        Box(Modifier::new()
            .flex_grow(1.0)
            .fill_max_height()
            .background(th.surface_container)
            .border(1.0, th.outline_variant, 18.0)
            .clip_rounded(18.0))
        .child(details_panel_view_v2(selected_torrent, active_tab, engine)),
    ))
}

fn top_bar_view(
    engine: Arc<TorrentEngine>,
    rt: Arc<tokio::runtime::Runtime>,
    torrents: Vec<TorrentRow>,
    selected: Rc<Signal<Option<InfoHash>>>,
    magnet_state: Rc<DialogState>,
    url_state: Rc<DialogState>,
    remove_state: Rc<DialogState>,
    settings_state: Rc<DialogState>,
    global_dl: u64,
    global_ul: u64,
    pending_from_button: Arc<Mutex<Vec<PendingTorrent>>>,
) -> View {
    let th = theme();

    Row(Modifier::new()
        .fill_max_width()
        .height(72.0)
        .padding(12.0)
        .background(th.surface)
        .align_items(AlignItems::Center))
    .child({
        let mut children: Vec<View> = Vec::new();

        #[cfg(not(target_os = "android"))]
        children.push(
            Row(Modifier::new().align_items(AlignItems::Center)).child((
                Box(Modifier::new()
                    .size(44.0, 44.0)
                    .background(th.primary_container)
                    .clip_rounded(14.0))
                .child(
                    Box(Modifier::new()
                        .fill_max_size()
                        .align_items(AlignItems::Center)
                        .justify_content(JustifyContent::Center))
                    .child(icon(Symbols::CLOUD_DOWNLOAD, 24.0, th.on_primary_container)),
                ),
                Box(Modifier::new().width(12.0)),
                Column(Modifier::new()).child((
                    Text("Retorrent").size(18.0).color(th.on_surface),
                    Text(format!("{} torrents", torrents.len()))
                        .size(12.0)
                        .color(th.on_surface_variant),
                )),
            )),
        );

        children.push(Box(Modifier::new().width(24.0)));

        #[cfg(not(target_os = "android"))]
        children.push(stat_pill(
            Symbols::DOWNLOAD,
            format_speed(global_dl),
            theme::downloading(),
        ));
        #[cfg(not(target_os = "android"))]
        children.push(Box(Modifier::new().width(8.0)));
        #[cfg(not(target_os = "android"))]
        children.push(stat_pill(
            Symbols::UPLOAD,
            format_speed(global_ul),
            theme::seeding(),
        ));

        children.push(Spacer());

        children.push(FilledButton(
            Modifier::new().height(40.0),
            {
                let pending_from_button = pending_from_button.clone();
                let engine = engine.clone();
                let rt = rt.clone();
                move || {
                    let pending_from_button = pending_from_button.clone();
                    let engine = engine.clone();
                    let rt = rt.clone();
                    std::thread::spawn(move || {
                        #[cfg(any(
                            target_os = "linux",
                            target_os = "windows",
                            target_os = "macos"
                        ))]
                        {
                            if let Some(path) = rlobkit_dialogs::blocking_open_file(
                                "Select Torrent File",
                                &["torrent"],
                            ) {
                                match std::fs::read(&path) {
                                    Ok(data) => match MetaInfo::from_bytes(&data) {
                                        Ok(meta) => {
                                            let suggested_dir =
                                                engine.config_read().download_dir.clone();
                                            if let Ok(mut p) = pending_from_button.lock() {
                                                p.push(PendingTorrent {
                                                    name: meta.name,
                                                    total_size: meta.total_size,
                                                    files: meta.files,
                                                    data,
                                                    suggested_dir,
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to parse torrent: {}", e)
                                        }
                                    },
                                    Err(e) => tracing::error!("Failed to read file: {}", e),
                                }
                            }
                        }
                        #[cfg(target_os = "android")]
                        rt.block_on(async {
                            use rlobkit_dialogs::RlobKitMode;
                            use rlobkit_dialogs::picker::{OpenFileOptions, RlobKit};
                            match RlobKit::open_file_picker(OpenFileOptions {
                                title: Some("Select Torrent File".into()),
                                mode: RlobKitMode::Single,
                                ..Default::default()
                            })
                            .await
                            {
                                Ok(Some(files)) => {
                                    for file in files {
                                        let temp_dir = std::env::temp_dir();
                                        let temp_path =
                                            temp_dir.join(format!("torrent_{}", file.name()));
                                        if RlobKit::read_file_to_path(&file, &temp_path).is_ok() {
                                            match std::fs::read(&temp_path) {
                                                Ok(data) => match MetaInfo::from_bytes(&data) {
                                                    Ok(meta) => {
                                                        if let Ok(mut p) =
                                                            pending_from_button.lock()
                                                        {
                                                            p.push(PendingTorrent {
                                                                name: meta.name,
                                                                total_size: meta.total_size,
                                                                files: meta.files,
                                                                data,
                                                                suggested_dir: engine
                                                                    .config_read()
                                                                    .download_dir
                                                                    .clone(),
                                                            });
                                                        }
                                                    }
                                                    Err(e) => tracing::error!(
                                                        "Failed to parse torrent: {}",
                                                        e
                                                    ),
                                                },
                                                Err(e) => tracing::error!(
                                                    "Failed to read temp file: {}",
                                                    e
                                                ),
                                            }
                                            let _ = std::fs::remove_file(&temp_path);
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => tracing::error!("File picker error: {}", e),
                            }
                        });
                    });
                }
            },
            || {
                Row(Modifier::new().align_items(AlignItems::Center)).child((
                    icon(Symbols::FOLDER_OPEN, 18.0, th.on_primary),
                    Box(Modifier::new().width(6.0)),
                    Text("Torrent").size(13.0),
                ))
            },
        ));

        children.push(Box(Modifier::new().width(8.0)));

        children.push(FilledTonalButton(
            Modifier::new().height(40.0),
            {
                let s = magnet_state.clone();
                move || s.show()
            },
            || {
                Row(Modifier::new().align_items(AlignItems::Center)).child((
                    icon(Symbols::LINK, 18.0, th.on_surface),
                    Box(Modifier::new().width(6.0)),
                    Text("Magnet").size(13.0),
                ))
            },
        ));

        children.push(Box(Modifier::new().width(8.0)));

        children.push(FilledTonalButton(
            Modifier::new().height(40.0),
            {
                let s = url_state.clone();
                move || {
                    s.show();
                }
            },
            || {
                Row(Modifier::new().align_items(AlignItems::Center)).child((
                    icon(Symbols::PUBLIC, 18.0, th.on_surface),
                    Box(Modifier::new().width(6.0)),
                    Text("URL").size(13.0),
                ))
            },
        ));

        children.push(Box(Modifier::new().width(8.0)));

        children.push(IconButton(
            icon(Symbols::PAUSE, 20.0, th.on_surface_variant),
            {
                let selected = selected.clone();
                let engine = engine.clone();
                move || {
                    if let Some(hash) = selected.get() {
                        engine.pause_torrent(&hash);
                    }
                }
            },
        ));

        children.push(IconButton(
            icon(Symbols::PLAY_ARROW, 20.0, th.on_surface_variant),
            {
                let selected = selected.clone();
                let engine = engine.clone();
                let rt = rt.clone();
                move || {
                    if let Some(hash) = selected.get() {
                        engine.resume_torrent(&hash, &rt);
                    }
                }
            },
        ));

        children.push(IconButton(icon(Symbols::DELETE, 20.0, th.error), {
            let selected = selected.clone();
            let s = remove_state.clone();
            move || {
                if selected.get().is_some() {
                    s.show();
                }
            }
        }));

        children.push(Box(Modifier::new().width(4.0)));

        children.push(IconButton(
            icon(Symbols::SETTINGS, 20.0, th.on_surface_variant),
            {
                let s = settings_state.clone();
                move || s.show()
            },
        ));

        children
    })
}

fn stat_pill(symbol: Symbol, value: String, color: Color) -> View {
    let th = theme();

    Box(Modifier::new()
        .height(34.0)
        .background(th.surface_container_high)
        .border(1.0, th.outline_variant, 17.0)
        .clip_rounded(17.0)
        .padding_values(PaddingValues {
            left: 12.0,
            right: 12.0,
            top: 6.0,
            bottom: 6.0,
        }))
    .child(Row(Modifier::new().align_items(AlignItems::Center)).child((
        icon(symbol, 17.0, color),
        Box(Modifier::new().width(6.0)),
        Text(value).size(12.0).color(th.on_surface),
    )))
}

fn filter_search_panel(
    filter_state: Rc<Signal<FilterState>>,
    search_query: Rc<Signal<String>>,
) -> View {
    let th = theme();
    let selected = filter_state.get();

    let filters = [
        (FilterState::All, "All", Symbols::FILTER_LIST),
        (FilterState::Downloading, "Downloading", Symbols::DOWNLOAD),
        (FilterState::Seeding, "Seeding", Symbols::UPLOAD),
        (FilterState::Paused, "Paused", Symbols::PAUSE),
        (FilterState::Complete, "Complete", Symbols::CHECK_CIRCLE),
    ];

    Column(
        Modifier::new()
            .fill_max_width()
            .padding(12.0)
            .background(th.surface_container_low),
    )
    .child((
        TextField(
            "Search torrents",
            search_query.get(),
            Modifier::new().fill_max_width().height(42.0),
            Some({
                let q = search_query.clone();
                move |v| q.set(v)
            }),
            None::<fn(String)>,
        ),
        Box(Modifier::new().height(10.0)),
        FlowRow(
            Modifier::new()
                .fill_max_width()
                .align_items(AlignItems::Center),
        )
        .child(
            filters
                .into_iter()
                .map(|(state, label, sym)| {
                    material3::FilterChip(
                        selected == state,
                        {
                            let f = filter_state.clone();
                            move || f.set(state)
                        },
                        Text(label).size(12.0),
                        Some(icon(sym, 16.0, th.on_surface_variant)),
                        None,
                    )
                })
                .collect::<Vec<_>>(),
        ),
    ))
}

fn torrent_list_view(
    torrents: &[TorrentRow],
    filtered_indices: &[usize],
    selected: Rc<Signal<Option<InfoHash>>>,
) -> View {
    let th = theme();
    let scroll_state = remember_scroll_state("torrent_list");
    let selected_hash = selected.get();

    if filtered_indices.is_empty() {
        return Box(Modifier::new()
            .fill_max_size()
            .align_items(AlignItems::Center)
            .justify_content(JustifyContent::Center))
        .child(
            Column(Modifier::new().align_items(AlignItems::Center)).child((
                Text("No torrents found").size(15.0).color(th.on_surface),
                Box(Modifier::new().height(4.0)),
                Text("Add a .torrent file or magnet link to get started.")
                    .size(12.0)
                    .color(th.on_surface_variant),
            )),
        );
    }

    ScrollArea(
        Modifier::new().fill_max_size(),
        scroll_state,
        Column(
            Modifier::new()
                .fill_max_width()
                .padding_values(PaddingValues {
                    left: 8.0,
                    right: 8.0,
                    top: 0.0,
                    bottom: 8.0,
                }),
        )
        .child(
            filtered_indices
                .iter()
                .copied()
                .map(|orig_idx| {
                    let torrent = &torrents[orig_idx];
                    torrent_card_view(
                        torrent,
                        selected_hash == Some(torrent.info_hash),
                        selected.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        ),
    )
}

fn torrent_card_view(
    torrent: &TorrentRow,
    is_selected: bool,
    selected: Rc<Signal<Option<InfoHash>>>,
) -> View {
    let th = theme();
    let state_color = torrent_state_color(torrent.stats.state);
    let state_symbol = torrent_state_symbol(torrent.stats.state);

    let bg = if is_selected {
        th.primary_container.with_alpha(90)
    } else {
        th.surface_container
    };

    let hash = torrent.info_hash;

    Box(Modifier::new()
        .fill_max_width()
        .padding_values(PaddingValues {
            left: 4.0,
            right: 4.0,
            top: 4.0,
            bottom: 8.0,
        })
        .background(bg)
        .border(
            1.0,
            if is_selected {
                th.primary
            } else {
                th.outline_variant
            },
            16.0,
        )
        .clip_rounded(16.0)
        .state_colors(StateColors {
            default: bg,
            hovered: th.surface_container_high,
            pressed: th.primary_container.with_alpha(120),
            disabled: bg,
        })
        .clickable()
        .on_pointer_down({
            let selected = selected.clone();
            move |_| selected.set(Some(hash))
        }))
    .child(
        Row(Modifier::new().fill_max_width()).child((
            Box(Modifier::new()
                .width(4.0)
                .fill_max_height()
                .background(state_color)),
            Column(Modifier::new().fill_max_width().padding(12.0)).child((
                Row(Modifier::new()
                    .fill_max_width()
                    .align_items(AlignItems::Center))
                .child((
                    icon(state_symbol, 20.0, state_color),
                    Box(Modifier::new().width(8.0)),
                    Text(&torrent.name).size(14.0).color(th.on_surface),
                    Spacer(),
                    Text(format_bytes(torrent.total_size))
                        .size(11.0)
                        .color(th.on_surface_variant),
                )),
                Box(Modifier::new().height(8.0)),
                components::progress_bar_view(torrent.display_progress, torrent.stats.state),
                Box(Modifier::new().height(8.0)),
                Row(Modifier::new()
                    .fill_max_width()
                    .align_items(AlignItems::Center))
                .child({
                    let mut m: Vec<View> = Vec::new();
                    m.push(metric_compact(
                        Symbols::DOWNLOAD,
                        format_speed(torrent.stats.download_rate),
                        theme::downloading(),
                    ));
                    m.push(Box(Modifier::new().width(8.0)));
                    m.push(metric_compact(
                        Symbols::UPLOAD,
                        format_speed(torrent.stats.upload_rate),
                        theme::seeding(),
                    ));
                    m.push(Box(Modifier::new().width(8.0)));
                    m.push(metric_text(format!("{} seeds", torrent.stats.seeders)));
                    m.push(Box(Modifier::new().width(8.0)));
                    m.push(metric_text(format!(
                        "{} peers",
                        torrent.stats.connected_peers
                    )));
                    m.push(Spacer());
                    m.push(
                        Text(format_eta(torrent.stats.eta_seconds))
                            .size(11.0)
                            .color(th.on_surface_variant),
                    );
                    m
                }),
            )),
        )),
    )
}

fn metric_compact(symbol: Symbol, value: String, color: Color) -> View {
    let th = theme();

    Row(Modifier::new().align_items(AlignItems::Center)).child((
        icon(symbol, 14.0, color),
        Box(Modifier::new().width(3.0)),
        Text(value).size(10.5).color(th.on_surface_variant),
    ))
}

fn metric_text(value: String) -> View {
    Text(value).size(10.5).color(theme().on_surface_variant)
}

fn details_panel_view_v2(
    torrent: Option<TorrentRow>,
    active_tab: Rc<Signal<Tab>>,
    engine: Arc<TorrentEngine>,
) -> View {
    let th = theme();

    let torrent = match torrent {
        Some(t) => t,
        None => {
            return Box(Modifier::new()
                .fill_max_size()
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::Center))
            .child(
                Column(Modifier::new().align_items(AlignItems::Center)).child((
                    Text("Select a torrent").size(18.0).color(th.on_surface),
                    Box(Modifier::new().height(6.0)),
                    Text("Torrent details, files, peers, trackers, and pieces will appear here.")
                        .size(12.0)
                        .color(th.on_surface_variant),
                )),
            );
        }
    };

    let active = active_tab.get();

    Column(Modifier::new().fill_max_size()).child((
        details_header(&torrent),
        details_tabs(active_tab.clone()),
        match active {
            Tab::General => general_tab_view_v2(&torrent),
            Tab::Files => files_tab_view(&torrent, torrent.info_hash, engine),
            Tab::Peers => peers_tab_view(&torrent),
            Tab::Trackers => trackers_tab_view(&torrent),
            Tab::Pieces => pieces_tab_view(&torrent),
        },
    ))
}

fn details_header(torrent: &TorrentRow) -> View {
    let th = theme();
    let state_color = torrent_state_color(torrent.stats.state);

    Column(
        Modifier::new()
            .fill_max_width()
            .padding(18.0)
            .background(th.surface_container),
    )
    .child((
        Row(Modifier::new()
            .fill_max_width()
            .align_items(AlignItems::Center))
        .child((
            Box(Modifier::new()
                .size(46.0, 46.0)
                .background(state_color.with_alpha(45))
                .clip_rounded(14.0))
            .child(
                Box(Modifier::new()
                    .fill_max_size()
                    .align_items(AlignItems::Center)
                    .justify_content(JustifyContent::Center))
                .child(icon(
                    torrent_state_symbol(torrent.stats.state),
                    25.0,
                    state_color,
                )),
            ),
            Box(Modifier::new().width(12.0)),
            Column(Modifier::new().flex_grow(1.0)).child((
                Text(&torrent.name).size(18.0).color(th.on_surface),
                Text(torrent.stats.state.to_string())
                    .size(12.0)
                    .color(th.on_surface_variant),
            )),
            stat_pill(
                Symbols::DOWNLOAD,
                format_speed(torrent.stats.download_rate),
                theme::downloading(),
            ),
            Box(Modifier::new().width(8.0)),
            stat_pill(
                Symbols::UPLOAD,
                format_speed(torrent.stats.upload_rate),
                theme::seeding(),
            ),
        )),
        Box(Modifier::new().height(14.0)),
        components::progress_bar_view(torrent.display_progress, torrent.stats.state),
    ))
}

fn details_tabs(active_tab: Rc<Signal<Tab>>) -> View {
    let active = active_tab.get();

    let tab_specs = [
        (Tab::General, "General"),
        (Tab::Files, "Files"),
        (Tab::Peers, "Peers"),
        (Tab::Trackers, "Trackers"),
        (Tab::Pieces, "Pieces"),
    ];

    let tabs: Vec<material3::Tab> = tab_specs
        .into_iter()
        .map(|(tab, label)| material3::Tab {
            label: label.to_string(),
            icon: None,
            on_click: Rc::new({
                let active_tab = active_tab.clone();
                move || active_tab.set(tab)
            }),
        })
        .collect();

    let active_idx = match active {
        Tab::General => 0usize,
        Tab::Files => 1,
        Tab::Peers => 2,
        Tab::Trackers => 3,
        Tab::Pieces => 4,
    };

    TabRow(active_idx, tabs)
}

fn general_tab_view_v2(torrent: &TorrentRow) -> View {
    let th = theme();

    ScrollArea(
        Modifier::new().fill_max_size(),
        remember_scroll_state("general_tab_v2"),
        Column(Modifier::new().fill_max_width().padding(16.0)).child((
            Row(Modifier::new().fill_max_width()).child((
                stat_card(
                    Symbols::CLOUD_DOWNLOAD,
                    "Downloaded",
                    format_bytes(torrent.stats.downloaded),
                    theme::downloading(),
                ),
                Box(Modifier::new().width(12.0)),
                stat_card(
                    Symbols::CLOUD_UPLOAD,
                    "Uploaded",
                    format_bytes(torrent.stats.uploaded),
                    theme::seeding(),
                ),
                Box(Modifier::new().width(12.0)),
                stat_card(
                    Symbols::SCHEDULE,
                    "ETA",
                    format_eta(torrent.stats.eta_seconds),
                    theme::warning(),
                ),
            )),
            Box(Modifier::new().height(12.0)),
            Row(Modifier::new().fill_max_width()).child((
                stat_card(
                    Symbols::GROUP,
                    "Peers",
                    torrent.stats.connected_peers.to_string(),
                    th.primary,
                ),
                Box(Modifier::new().width(12.0)),
                stat_card(
                    Symbols::UPLOAD,
                    "Seeds",
                    torrent.stats.seeders.to_string(),
                    theme::seeding(),
                ),
                Box(Modifier::new().width(12.0)),
                stat_card(
                    Symbols::MEMORY,
                    "Pieces",
                    format!(
                        "{} / {}",
                        torrent.have_pieces.iter().filter(|&&v| v).count(),
                        torrent.num_pieces
                    ),
                    theme::accent(),
                ),
            )),
            Box(Modifier::new().height(18.0)),
            info_section(
                "Torrent",
                vec![
                    ("Name", torrent.name.clone()),
                    ("Info Hash", torrent.info_hash.to_string()),
                    ("Total Size", format_bytes(torrent.total_size)),
                    (
                        "Progress",
                        format!("{:.2}%", torrent.display_progress * 100.0),
                    ),
                    (
                        "Ratio",
                        format_ratio(torrent.stats.uploaded, torrent.stats.downloaded),
                    ),
                    ("Status", torrent.stats.state.to_string()),
                ],
            ),
        )),
    )
}

fn stat_card(
    symbol: Symbol,
    label: impl Into<String>,
    value: impl Into<String>,
    color: Color,
) -> View {
    let th = theme();

    Box(Modifier::new()
        .flex_grow(1.0)
        .height(92.0)
        .background(th.surface_container_high)
        .border(1.0, th.outline_variant, 16.0)
        .clip_rounded(16.0)
        .padding(14.0))
    .child(Column(Modifier::new().fill_max_size()).child((
        Row(Modifier::new().align_items(AlignItems::Center)).child((
            icon(symbol, 18.0, color),
            Box(Modifier::new().width(6.0)),
            Text(label.into()).size(11.0).color(th.on_surface_variant),
        )),
        Spacer(),
        Text(value.into()).size(17.0).color(th.on_surface),
    )))
}

fn info_section(title: &str, rows: Vec<(&str, String)>) -> View {
    let th = theme();

    Box(Modifier::new()
        .fill_max_width()
        .background(th.surface_container_high)
        .border(1.0, th.outline_variant, 16.0)
        .clip_rounded(16.0)
        .padding(16.0))
    .child(Column(Modifier::new().fill_max_width()).child({
        let mut views: Vec<View> = vec![
            Text(title).size(15.0).color(th.on_surface),
            Box(Modifier::new().height(10.0)),
        ];

        for (label, value) in rows {
            views.push(
                Row(Modifier::new()
                    .fill_max_width()
                    .padding_values(PaddingValues {
                        left: 0.0,
                        right: 0.0,
                        top: 4.0,
                        bottom: 4.0,
                    }))
                .child((
                    Text(label)
                        .size(12.0)
                        .color(th.on_surface_variant)
                        .modifier(Modifier::new().width(130.0)),
                    Text(value).size(12.0).color(th.on_surface),
                )),
            );
        }

        views
    }))
}

fn file_progress_view(progress: f32, state: TorrentState) -> View {
    components::colored_progress_bar(progress, components::state_color(state))
}

fn files_tab_view(torrent: &TorrentRow, _info_hash: InfoHash, _engine: Arc<TorrentEngine>) -> View {
    let th = theme();
    let files = torrent.files.clone();
    let file_priorities = torrent.file_priorities.clone();
    let state = torrent.stats.state;

    ScrollArea(
        Modifier::new().fill_max_width().min_height(200.0),
        remember_scroll_state("files_tab"),
        Column(Modifier::new().fill_max_width().padding(4.0)).child({
            let mut views: Vec<View> = Vec::new();

            views.push(
                Row(Modifier::new()
                    .fill_max_width()
                    .padding(4.0)
                    .column_gap(8.0))
                .child((
                    Text("File")
                        .size(12.0)
                        .color(th.on_surface)
                        .modifier(Modifier::new().flex_grow(1.0)),
                    Text("Size")
                        .size(12.0)
                        .color(th.on_surface)
                        .modifier(Modifier::new().width(80.0)),
                    Text("Progress")
                        .size(12.0)
                        .color(th.on_surface)
                        .modifier(Modifier::new().flex_grow(2.0)),
                    Text("Priority")
                        .size(12.0)
                        .color(th.on_surface)
                        .modifier(Modifier::new().width(70.0)),
                )),
            );

            views.push(Box(Modifier::new()
                .fill_max_width()
                .height(1.0)
                .background(th.outline_variant)));

            for (fi, file) in files.iter().enumerate() {
                let display_name = file.path.rsplit('/').next().unwrap_or(&file.path);
                let current_prio = file_priorities
                    .get(fi)
                    .copied()
                    .unwrap_or(FilePriority::Normal);
                let file_progress = if torrent.piece_length > 0 {
                    let first = (file.offset / torrent.piece_length) as usize;
                    let last = ((file.offset + file.length - 1) / torrent.piece_length) as usize;
                    let total = last.saturating_sub(first) + 1;
                    let done = (first..=last)
                        .filter(|&i| i < torrent.have_pieces.len() && torrent.have_pieces[i])
                        .count();
                    done as f32 / total as f32
                } else {
                    0.0
                };

                views.push(
                    Row(Modifier::new()
                        .fill_max_width()
                        .padding(2.0)
                        .column_gap(8.0))
                    .child((
                        Text(display_name)
                            .size(11.0)
                            .color(th.on_surface)
                            .modifier(Modifier::new().flex_grow(1.0)),
                        Text(format_bytes(file.length))
                            .size(11.0)
                            .color(th.on_surface)
                            .modifier(Modifier::new().width(80.0)),
                        file_progress_view(file_progress, state),
                        Text(current_prio.to_string())
                            .size(11.0)
                            .color(th.on_surface)
                            .modifier(Modifier::new().width(70.0)),
                    )),
                );
            }
            views
        }),
    )
}

fn peers_tab_view(torrent: &TorrentRow) -> View {
    let th = theme();
    Column(
        Modifier::new()
            .fill_max_width()
            .min_height(200.0)
            .padding(8.0),
    )
    .child((
        Text(format!(
            "Connected Peers: {}",
            torrent.stats.connected_peers
        ))
        .size(12.0)
        .color(th.on_surface),
        Box(Modifier::new().height(8.0)),
        Text("(Peer details shown during active connections)")
            .size(11.0)
            .color(th.on_surface_variant),
    ))
}

fn trackers_tab_view(torrent: &TorrentRow) -> View {
    let th = theme();
    ScrollArea(
        Modifier::new().fill_max_width().min_height(200.0),
        remember_scroll_state("trackers_tab"),
        Column(Modifier::new().fill_max_width().padding(8.0)).child(
            torrent
                .trackers
                .iter()
                .enumerate()
                .map(|(idx, tracker)| {
                    Row(Modifier::new().fill_max_width()).child((
                        Text(format!("{}.", idx + 1))
                            .size(11.0)
                            .color(th.on_surface_variant),
                        Box(Modifier::new().width(4.0)),
                        Text(tracker).size(11.0).color(theme::accent()),
                    ))
                })
                .collect::<Vec<_>>(),
        ),
    )
}

fn pieces_tab_view(torrent: &TorrentRow) -> View {
    let th = theme();
    let completed = torrent.have_pieces.iter().filter(|&&v| v).count();

    Column(
        Modifier::new()
            .fill_max_width()
            .min_height(200.0)
            .padding(8.0),
    )
    .child((
        Text(format!(
            "Pieces: {} / {} completed",
            completed, torrent.num_pieces
        ))
        .size(12.0)
        .color(th.on_surface),
        Box(Modifier::new().height(8.0)),
        ScrollArea(
            Modifier::new().fill_max_width(),
            remember_scroll_state("pieces_tab"),
            components::piece_map_view(&torrent.have_pieces, 600.0),
        ),
    ))
}

fn magnet_dialog_view(
    state: Rc<DialogState>,
    overlay: OverlayHandle,
    magnet_input: Rc<Signal<String>>,
    engine: Arc<TorrentEngine>,
    rt: Arc<tokio::runtime::Runtime>,
) -> View {
    let th = theme();

    Dialog(
        state.clone(),
        overlay,
        Modifier::new(),
        Column(Modifier::new().padding(24.0).min_width(400.0)).child((
            Text("Add Magnet Link").size(18.0).color(th.on_surface),
            Box(Modifier::new().height(12.0)),
            TextField(
                "magnet:?xt=urn:btih:...",
                magnet_input.get(),
                Modifier::new().fill_max_width().height(60.0),
                Some({
                    let m = magnet_input.clone();
                    move |v| m.set(v)
                }),
                None::<fn(String)>,
            ),
            Box(Modifier::new().height(16.0)),
            Row(Modifier::new()
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::End))
            .child((
                TextButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let m = magnet_input.clone();
                        move || {
                            m.set(String::new());
                            s.dismiss();
                        }
                    },
                    || Text("Cancel"),
                ),
                Box(Modifier::new().width(8.0)),
                FilledButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let m = magnet_input.clone();
                        move || {
                            let uri = m.get().trim().to_string();
                            if !uri.is_empty() {
                                match engine.add_torrent_from_magnet(&uri, None) {
                                    Ok(hash) => {
                                        engine.start_torrent(&hash, &rt);
                                        tracing::info!("Added magnet: {}", hash);
                                    }
                                    Err(e) => tracing::error!("Failed to add magnet: {}", e),
                                }
                            }
                            m.set(String::new());
                            s.dismiss();
                        }
                    },
                    || Text("Add"),
                ),
            )),
        )),
    )
}

fn url_dialog_view(
    state: Rc<DialogState>,
    overlay: OverlayHandle,
    url_input: Rc<Signal<String>>,
    engine: Arc<TorrentEngine>,
    rt: Arc<tokio::runtime::Runtime>,
    pending_from_button: Arc<Mutex<Vec<PendingTorrent>>>,
) -> View {
    let th = theme();

    Dialog(
        state.clone(),
        overlay,
        Modifier::new(),
        Column(Modifier::new().padding(24.0).min_width(400.0)).child((
            Text("Add Torrent from URL").size(18.0).color(th.on_surface),
            Box(Modifier::new().height(12.0)),
            TextField(
                "https://example.com/file.torrent",
                url_input.get(),
                Modifier::new().fill_max_width().height(60.0),
                Some({
                    let u = url_input.clone();
                    move |v| u.set(v)
                }),
                None::<fn(String)>,
            ),
            Box(Modifier::new().height(16.0)),
            Row(Modifier::new()
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::End))
            .child((
                TextButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let u = url_input.clone();
                        move || {
                            u.set(String::new());
                            s.dismiss();
                        }
                    },
                    || Text("Cancel"),
                ),
                Box(Modifier::new().width(8.0)),
                FilledButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let u = url_input.clone();
                        let engine = engine.clone();
                        let pending_from_button = pending_from_button.clone();
                        move || {
                            let url = u.get().trim().to_string();
                            if !url.is_empty() {
                                let engine = engine.clone();
                                let pfb = pending_from_button.clone();
                                std::thread::spawn(move || match reqwest::blocking::get(&url) {
                                    Ok(resp) => match resp.bytes() {
                                        Ok(bytes) => {
                                            let data = bytes.to_vec();
                                            match MetaInfo::from_bytes(&data) {
                                                Ok(meta) => {
                                                    if let Ok(mut p) = pfb.lock() {
                                                        p.push(PendingTorrent {
                                                            name: meta.name,
                                                            total_size: meta.total_size,
                                                            files: meta.files,
                                                            data,
                                                            suggested_dir: engine
                                                                .config
                                                                .read()
                                                                .unwrap()
                                                                .download_dir
                                                                .clone(),
                                                        });
                                                    }
                                                }
                                                Err(e) => tracing::error!(
                                                    "Failed to parse torrent from URL: {}",
                                                    e
                                                ),
                                            }
                                        }
                                        Err(e) => {
                                            tracing::error!("Failed to read response body: {}", e)
                                        }
                                    },
                                    Err(e) => tracing::error!("Failed to fetch URL: {}", e),
                                });
                            }
                            u.set(String::new());
                            s.dismiss();
                        }
                    },
                    || Text("Fetch"),
                ),
            )),
        )),
    )
}

fn remove_dialog_view(
    state: Rc<DialogState>,
    overlay: OverlayHandle,
    selected: Rc<Signal<Option<InfoHash>>>,
    engine: Arc<TorrentEngine>,
) -> View {
    let th = theme();
    let remove_delete_files = remember(|| signal(false));

    Dialog(
        state.clone(),
        overlay,
        Modifier::new(),
        Column(Modifier::new().padding(24.0).min_width(360.0)).child((
            Text("Remove Torrent").size(18.0).color(th.on_surface),
            Box(Modifier::new().height(12.0)),
            Text("Are you sure you want to remove this torrent?")
                .size(14.0)
                .color(th.on_surface_variant),
            Box(Modifier::new().height(12.0)),
            Row(Modifier::new().align_items(AlignItems::Center)).child((
                Checkbox(remove_delete_files.get(), {
                    let d = remove_delete_files.clone();
                    move |v| d.set(v)
                }),
                Text("  Also delete downloaded files")
                    .size(13.0)
                    .color(th.on_surface),
            )),
            Box(Modifier::new().height(16.0)),
            Row(Modifier::new()
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::End))
            .child((
                TextButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let d = remove_delete_files.clone();
                        move || {
                            d.set(false);
                            s.dismiss();
                        }
                    },
                    || Text("Cancel"),
                ),
                Box(Modifier::new().width(8.0)),
                FilledButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let d = remove_delete_files.clone();
                        let sel = selected.clone();
                        let e = engine.clone();
                        move || {
                            if let Some(hash) = sel.get() {
                                e.remove_torrent(&hash, d.get());
                                sel.set(None);
                            }
                            d.set(false);
                            s.dismiss();
                        }
                    },
                    || Text("Remove"),
                ),
            )),
        )),
    )
}

fn settings_dialog_view(
    state: Rc<DialogState>,
    overlay: OverlayHandle,
    engine: Arc<TorrentEngine>,
) -> View {
    let th = theme();
    let config: Rc<Signal<Config>> =
        remember_with_key(state.key("cfg"), || signal(engine.config_read().clone()));
    let last_visible = remember_with_key(state.key("lv"), || signal(false));

    let listen_port_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_port"), || signal(String::new()));
    let max_conn_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_mc"), || signal(String::new()));
    let max_pt_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_mpt"), || signal(String::new()));
    let pipeline_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_pd"), || signal(String::new()));
    let upload_slots_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_us"), || signal(String::new()));
    let max_dl_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_mdl"), || signal(String::new()));
    let max_ul_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_mul"), || signal(String::new()));
    let cache_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_cache"), || signal(String::new()));
    let choke_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_choke"), || signal(String::new()));
    let unchoke_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_unchoke"), || signal(String::new()));
    let seed_ratio_input: Rc<Signal<String>> =
        remember_with_key(state.key("si_sr"), || signal(String::new()));

    let vis = state.is_visible();
    if vis && !last_visible.get() {
        let engine_cfg = engine.config_read().clone();
        config.set(engine_cfg.clone());
        listen_port_input.set(engine_cfg.listen_port.to_string());
        max_conn_input.set(engine_cfg.max_connections.to_string());
        max_pt_input.set(engine_cfg.max_connections_per_torrent.to_string());
        pipeline_input.set(engine_cfg.pipeline_depth.to_string());
        upload_slots_input.set(engine_cfg.upload_slots.to_string());
        max_dl_input.set(engine_cfg.max_download_rate.to_string());
        max_ul_input.set(engine_cfg.max_upload_rate.to_string());
        cache_input.set(engine_cfg.cache_size_mb.to_string());
        choke_input.set(engine_cfg.choke_interval.to_string());
        unchoke_input.set(engine_cfg.optimistic_unchoke_interval.to_string());
        seed_ratio_input.set(engine_cfg.seed_ratio_limit.to_string());
        last_visible.set(true);
    } else if !vis {
        last_visible.set(false);
    }
    let cfg = config.get();

    Dialog(
        state.clone(),
        overlay,
        Modifier::new().max_width(540.0),
        Column(Modifier::new().padding(24.0)).child((
            Text("\u{2699} Settings").size(18.0).color(th.on_surface),
            Box(Modifier::new().height(12.0)),
            ScrollArea(
                Modifier::new().fill_max_width().max_height(400.0),
                {
                    let s = remember_scroll_state("settings_scroll");
                    s.set_show_scrollbar(false);
                    s
                },
                Column(Modifier::new().fill_max_width()).child({
                    let mut views: Vec<View> = Vec::new();

                    let field_row = |label: &str, input: &Rc<Signal<String>>| {
                        Row(Modifier::new()
                            .fill_max_width()
                            .align_items(AlignItems::Center))
                        .child((
                            Text(label)
                                .size(12.0)
                                .color(th.on_surface_variant)
                                .modifier(Modifier::new().width(150.0)),
                            TextField(
                                "",
                                input.get(),
                                Modifier::new().flex_grow(1.0).height(28.0),
                                Some({
                                    let s = input.clone();
                                    move |v| s.set(v)
                                }),
                                None::<fn(String)>,
                            ),
                        ))
                    };
                    let switch_row = |label: &str, val: bool, on_toggle: Rc<dyn Fn(bool)>| {
                        Row(Modifier::new()
                            .fill_max_width()
                            .align_items(AlignItems::Center))
                        .child((
                            Text(label)
                                .size(12.0)
                                .color(th.on_surface_variant)
                                .modifier(Modifier::new().flex_grow(1.0)),
                            Switch(val, move |v| on_toggle(v)),
                        ))
                    };

                    // Network
                    views.push(Text("Network").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Listen Port:", &listen_port_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Max Connections:", &max_conn_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Max Per Torrent:", &max_pt_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Pipeline Depth:", &pipeline_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Upload Slots:", &upload_slots_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(switch_row("Accept Incoming", cfg.accept_incoming, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.accept_incoming = v;
                            c.set(nc);
                        })
                    }));

                    // Bandwidth
                    views.push(Box(Modifier::new().height(12.0)));
                    views.push(Text("Bandwidth").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Max DL Rate (0=\u{221E}):", &max_dl_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Max UL Rate (0=\u{221E}):", &max_ul_input));

                    // Storage
                    views.push(Box(Modifier::new().height(12.0)));
                    views.push(Text("Storage").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(
                        Row(Modifier::new().fill_max_width()).child((
                            Text("Directory:").size(12.0).color(th.on_surface_variant),
                            Text(cfg.download_dir.to_string_lossy())
                                .size(12.0)
                                .color(th.on_surface),
                        )),
                    );
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Cache Size (MB):", &cache_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(switch_row("Preallocate Files", cfg.prealloc_files, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.prealloc_files = v;
                            c.set(nc);
                        })
                    }));

                    // Features
                    views.push(Box(Modifier::new().height(12.0)));
                    views.push(Text("Features").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(switch_row("DHT", cfg.dht_enabled, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.dht_enabled = v;
                            c.set(nc);
                        })
                    }));
                    views.push(switch_row("UPnP", cfg.upnp_enabled, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.upnp_enabled = v;
                            c.set(nc);
                        })
                    }));
                    views.push(switch_row("PEX", cfg.pex_enabled, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.pex_enabled = v;
                            c.set(nc);
                        })
                    }));
                    views.push(switch_row("Webseed", cfg.webseed_enabled, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.webseed_enabled = v;
                            c.set(nc);
                        })
                    }));
                    views.push(switch_row("Endgame Mode", cfg.endgame_mode, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.endgame_mode = v;
                            c.set(nc);
                        })
                    }));
                    views.push(switch_row("Auto Resume", cfg.auto_resume, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.auto_resume = v;
                            c.set(nc);
                        })
                    }));
                    #[cfg(not(target_os = "android"))]
                    views.push(switch_row("Minimize on launch", cfg.minimize_to_tray, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.minimize_to_tray = v;
                            c.set(nc);
                        })
                    }));

                    // Seeding
                    views.push(Box(Modifier::new().height(12.0)));
                    views.push(Text("Seeding").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(switch_row("Seed Ratio Limit", cfg.seed_ratio_enabled, {
                        let c = config.clone();
                        Rc::new(move |v| {
                            let mut nc = c.get();
                            nc.seed_ratio_enabled = v;
                            c.set(nc);
                        })
                    }));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Ratio:", &seed_ratio_input));

                    // Advanced
                    views.push(Box(Modifier::new().height(12.0)));
                    views.push(Text("Advanced").size(16.0).color(th.on_surface));
                    views.push(Box(Modifier::new().height(8.0)));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Choke Interval (s):", &choke_input));
                    views.push(Box(Modifier::new().height(4.0)));
                    views.push(field_row("Opt. Unchoke Interval (s):", &unchoke_input));

                    views
                }),
            ),
            Box(Modifier::new().height(16.0)),
            Row(Modifier::new()
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::End))
            .child((
                TextButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let c = config.clone();
                        let e = engine.clone();
                        let lp = listen_port_input.clone();
                        let mc = max_conn_input.clone();
                        let mpt = max_pt_input.clone();
                        let pd = pipeline_input.clone();
                        let us = upload_slots_input.clone();
                        let mdl = max_dl_input.clone();
                        let mul = max_ul_input.clone();
                        let cache = cache_input.clone();
                        let choke = choke_input.clone();
                        let unchoke = unchoke_input.clone();
                        let sr = seed_ratio_input.clone();
                        move || {
                            let engine_cfg = e.config_read().clone();
                            c.set(engine_cfg.clone());
                            lp.set(engine_cfg.listen_port.to_string());
                            mc.set(engine_cfg.max_connections.to_string());
                            mpt.set(engine_cfg.max_connections_per_torrent.to_string());
                            pd.set(engine_cfg.pipeline_depth.to_string());
                            us.set(engine_cfg.upload_slots.to_string());
                            mdl.set(engine_cfg.max_download_rate.to_string());
                            mul.set(engine_cfg.max_upload_rate.to_string());
                            cache.set(engine_cfg.cache_size_mb.to_string());
                            choke.set(engine_cfg.choke_interval.to_string());
                            unchoke.set(engine_cfg.optimistic_unchoke_interval.to_string());
                            sr.set(engine_cfg.seed_ratio_limit.to_string());
                            s.dismiss();
                        }
                    },
                    || Text("Cancel"),
                ),
                Box(Modifier::new().width(8.0)),
                FilledButton(
                    Modifier::new(),
                    {
                        let s = state.clone();
                        let e = engine.clone();
                        let c = config.clone();
                        let lp = listen_port_input.clone();
                        let mc = max_conn_input.clone();
                        let mpt = max_pt_input.clone();
                        let pd = pipeline_input.clone();
                        let us = upload_slots_input.clone();
                        let mdl = max_dl_input.clone();
                        let mul = max_ul_input.clone();
                        let cache = cache_input.clone();
                        let choke = choke_input.clone();
                        let unchoke = unchoke_input.clone();
                        let sr = seed_ratio_input.clone();
                        move || {
                            let mut cfg = c.get();
                            if let Ok(v) = lp.get().parse::<u16>() {
                                cfg.listen_port = v;
                            }
                            if let Ok(v) = mc.get().parse::<usize>() {
                                cfg.max_connections = v;
                            }
                            if let Ok(v) = mpt.get().parse::<usize>() {
                                cfg.max_connections_per_torrent = v;
                            }
                            if let Ok(v) = pd.get().parse::<u32>() {
                                cfg.pipeline_depth = v;
                            }
                            if let Ok(v) = us.get().parse::<usize>() {
                                cfg.upload_slots = v;
                            }
                            if let Ok(v) = mdl.get().parse::<u64>() {
                                cfg.max_download_rate = v;
                            }
                            if let Ok(v) = mul.get().parse::<u64>() {
                                cfg.max_upload_rate = v;
                            }
                            if let Ok(v) = cache.get().parse::<usize>() {
                                cfg.cache_size_mb = v;
                            }
                            if let Ok(v) = choke.get().parse::<u64>() {
                                cfg.choke_interval = v;
                            }
                            if let Ok(v) = unchoke.get().parse::<u64>() {
                                cfg.optimistic_unchoke_interval = v;
                            }
                            if let Ok(v) = sr.get().parse::<f64>() {
                                cfg.seed_ratio_limit = v;
                            }
                            let _ = cfg.save();
                            e.apply_config(&cfg);
                            s.dismiss();
                        }
                    },
                    || Text("Save"),
                ),
            )),
        )),
    )
}

fn add_torrent_dialog_view(
    state: Rc<DialogState>,
    overlay: OverlayHandle,
    current_add: Rc<Signal<Option<PendingTorrent>>>,
    pending: Rc<Signal<Vec<PendingTorrent>>>,
    file_checks: Rc<Signal<Vec<bool>>>,
    download_path_input: Rc<Signal<String>>,
    engine: Arc<TorrentEngine>,
    rt: Arc<tokio::runtime::Runtime>,
) -> View {
    let th = theme();
    let visible = state.is_visible();
    let torrent = current_add.get();

    // Thread-safe channel for the folder-pick result.
    let browse_pending: Rc<std::sync::Arc<std::sync::Mutex<Option<PathBuf>>>> =
        remember(|| std::sync::Arc::new(std::sync::Mutex::new(None)));
    if let Ok(mut p) = browse_pending.lock() {
        if let Some(folder) = p.take() {
            download_path_input.set(folder.to_string_lossy().to_string());
        }
    }

    // Drain the next pending torrent (if any) into `current_add`.
    let advance = {
        let pending = pending.clone();
        let current_add = current_add.clone();
        let file_checks = file_checks.clone();
        let download_path_input = download_path_input.clone();
        move || {
            let mut q = pending.get();
            if !q.is_empty() {
                q.remove(0);
                pending.set(q.clone());
                if let Some(next) = q.first().cloned() {
                    file_checks.set(vec![true; next.files.len()]);
                    download_path_input.set(next.suggested_dir.to_string_lossy().to_string());
                    current_add.set(Some(next));
                    return true;
                }
            }
            current_add.set(None);
            file_checks.set(Vec::new());
            download_path_input.set(String::new());
            false
        }
    };

    let content: View = if visible {
        if let Some(ref torrent) = torrent {
            let files = torrent.files.clone();
            let total_size = torrent.total_size;
            let name = torrent.name.clone();
            let checks = file_checks.get();

            let select_all = {
                let file_checks = file_checks.clone();
                let n = files.len();
                move || {
                    file_checks.set(vec![true; n]);
                }
            };
            let select_none = {
                let file_checks = file_checks.clone();
                let n = files.len();
                move || {
                    file_checks.set(vec![false; n]);
                }
            };

            let pick_folder = {
                let pending: std::sync::Arc<std::sync::Mutex<Option<PathBuf>>> =
                    (*browse_pending).clone();
                move || {
                    let pending = pending.clone();
                    std::thread::spawn(move || {
                        #[cfg(not(target_os = "android"))]
                        if let Some(folder) =
                            rlobkit_dialogs::blocking_pick_directory("Select Download Directory")
                        {
                            if let Ok(mut p) = pending.lock() {
                                *p = Some(folder);
                            }
                        }
                        #[cfg(target_os = "android")]
                        {
                            use rlobkit_dialogs::picker::{OpenDirectoryOptions, RlobKit};
                            let local_rt = tokio::runtime::Builder::new_current_thread()
                                .enable_all()
                                .build()
                                .expect("Failed to build local runtime");
                            local_rt.block_on(async {
                                match RlobKit::open_directory_picker(OpenDirectoryOptions {
                                    title: Some("Select Download Directory".into()),
                                    initial_directory: None,
                                })
                                .await
                                {
                                    Ok(Some(dir)) => {
                                        if let Ok(mut p) = pending.lock() {
                                            *p = Some(dir.path().to_path_buf());
                                        }
                                    }
                                    Ok(None) => {}
                                    Err(e) => tracing::error!("Directory picker error: {}", e),
                                }
                            });
                        }
                    });
                }
            };

            let on_confirm = {
                let advance = advance.clone();
                let state = state.clone();
                let engine = engine.clone();
                let rt = rt.clone();
                let current_add = current_add.clone();
                let download_path_input = download_path_input.clone();
                let file_checks = file_checks.clone();
                move || {
                    if let Some(pt) = current_add.get() {
                        let data = pt.data.clone();
                        let checks = file_checks.get();
                        let dir_str = download_path_input.get().trim().to_string();
                        let dir = if dir_str.is_empty() {
                            None
                        } else {
                            Some(PathBuf::from(dir_str))
                        };
                        let priorities: Vec<FilePriority> = checks
                            .iter()
                            .map(|&c| {
                                if c {
                                    FilePriority::Normal
                                } else {
                                    FilePriority::Skip
                                }
                            })
                            .collect();
                        match engine.add_torrent_from_bytes(data, dir, Some(&priorities)) {
                            Ok(info_hash) => {
                                engine.start_torrent(&info_hash, &rt);
                                tracing::info!("Added torrent: {}", info_hash);
                            }
                            Err(e) => tracing::error!("Failed to add torrent: {}", e),
                        }
                    }
                    let has_more = advance();
                    if !has_more {
                        state.dismiss();
                    }
                }
            };
            let on_cancel = {
                let advance = advance.clone();
                let state = state.clone();
                move || {
                    let has_more = advance();
                    if !has_more {
                        state.dismiss();
                    }
                }
            };

            let file_rows: Vec<View> = files
                .iter()
                .enumerate()
                .map(|(i, file)| {
                    let checked = checks.get(i).copied().unwrap_or(true);
                    let fi: FileInfo = file.clone();
                    let file_checks = file_checks.clone();
                    add_file_row_view(i, fi, checked, file_checks)
                })
                .collect();

            let mut body: Vec<View> = Vec::new();
            body.push(
                Row(Modifier::new()
                    .fill_max_width()
                    .align_items(AlignItems::Center))
                .child((
                    icon(Symbols::CLOUD_DOWNLOAD, 22.0, th.primary),
                    Box(Modifier::new().width(10.0)),
                    Text("Add Torrent").size(18.0).color(th.on_surface),
                )),
            );
            body.push(Box(Modifier::new().height(10.0)));
            body.push(
                Text(&name)
                    .size(13.0)
                    .color(th.on_surface_variant)
                    .overflow_ellipsize(),
            );
            body.push(
                Text(format!(
                    "{} \u{00B7} {}",
                    crate::ui::utils::format_bytes(total_size),
                    format!(
                        "{} file{}",
                        files.len(),
                        if files.len() == 1 { "" } else { "s" }
                    )
                ))
                .size(12.0)
                .color(th.on_surface_variant),
            );
            body.push(Box(Modifier::new().height(14.0)));
            body.push(
                Row(Modifier::new()
                    .fill_max_width()
                    .align_items(AlignItems::Center))
                .child((
                    Text("Download to:")
                        .size(12.0)
                        .color(th.on_surface_variant)
                        .modifier(Modifier::new().width(110.0)),
                    TextField(
                        "Path",
                        download_path_input.get(),
                        Modifier::new().flex_grow(1.0).height(36.0),
                        Some({
                            let p = download_path_input.clone();
                            move |v| p.set(v)
                        }),
                        None::<fn(String)>,
                    ),
                    Box(Modifier::new().width(6.0)),
                    FilledTonalButton(
                        Modifier::new().height(36.0),
                        move || pick_folder(),
                        || Text("Browse"),
                    ),
                )),
            );
            body.push(Box(Modifier::new().height(14.0)));
            body.push(
                Row(Modifier::new()
                    .fill_max_width()
                    .align_items(AlignItems::Center))
                .child((
                    Text("Files:").size(12.0).color(th.on_surface_variant),
                    Spacer(),
                    TextButton(Modifier::new(), move || select_all(), || Text("Select All")),
                    Box(Modifier::new().width(4.0)),
                    TextButton(
                        Modifier::new(),
                        move || select_none(),
                        || Text("Select None"),
                    ),
                )),
            );
            body.push(
                Box(Modifier::new()
                    .fill_max_width()
                    .height(220.0)
                    .background(th.surface_container_high)
                    .border(1.0, th.outline_variant, 10.0)
                    .clip_rounded(10.0))
                .child(ScrollArea(
                    Modifier::new().fill_max_size(),
                    remember_scroll_state("add_torrent_files"),
                    Column(Modifier::new().fill_max_width().padding(6.0)).child(file_rows),
                )),
            );
            body.push(Box(Modifier::new().height(16.0)));
            body.push(
                Row(Modifier::new()
                    .align_items(AlignItems::Center)
                    .justify_content(JustifyContent::End))
                .child((
                    TextButton(Modifier::new(), move || on_cancel(), || Text("Cancel")),
                    Box(Modifier::new().width(8.0)),
                    FilledButton(Modifier::new(), move || on_confirm(), || Text("Add")),
                )),
            );

            Column(Modifier::new().padding(20.0).min_width(360.0)).child(body)
        } else {
            Box(Modifier::new().size(0.0, 0.0))
        }
    } else {
        Box(Modifier::new().size(0.0, 0.0))
    };

    Dialog(
        state.clone(),
        overlay,
        Modifier::new().max_width(500.0).max_height(560.0),
        content,
    )
}

fn add_file_row_view(
    index: usize,
    file: FileInfo,
    checked: bool,
    file_checks: Rc<Signal<Vec<bool>>>,
) -> View {
    let th = theme();
    let display_name = file
        .path
        .rsplit('/')
        .next()
        .unwrap_or(&file.path)
        .to_string();
    let full_path = file.path.clone();
    let size_text = crate::ui::utils::format_bytes(file.length);

    Row(Modifier::new()
        .fill_max_width()
        .padding_values(PaddingValues {
            left: 6.0,
            right: 6.0,
            top: 4.0,
            bottom: 4.0,
        })
        .column_gap(8.0)
        .align_items(AlignItems::Center))
    .child((
        Checkbox(checked, {
            let file_checks = file_checks.clone();
            move |v| {
                let mut cur = file_checks.get();
                if index < cur.len() {
                    cur[index] = v;
                    file_checks.set(cur);
                }
            }
        }),
        Column(Modifier::new().flex_grow(1.0)).child((
            Text(display_name)
                .size(12.0)
                .color(th.on_surface)
                .overflow_ellipsize(),
            Text(full_path)
                .size(10.0)
                .color(th.on_surface_variant)
                .overflow_ellipsize(),
        )),
        Text(size_text).size(11.0).color(th.on_surface_variant),
    ))
}
