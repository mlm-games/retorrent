use crate::types::TorrentState;
use crate::ui::theme;
use repose_core::prelude::*;
use repose_ui::{Box, Column, Row, Text, TextStyle, ViewExt, ZStack};

pub fn progress_bar_view(progress: f32, state: TorrentState, width: f32) -> View {
    let fill_color = match state {
        TorrentState::Downloading => theme::downloading(),
        TorrentState::Seeding => theme::seeding(),
        TorrentState::Paused => theme::paused(),
        TorrentState::Complete => theme::success(),
        TorrentState::Error => theme::error(),
        TorrentState::FetchingMetadata => theme::warning(),
        _ => theme::accent(),
    };

    let th = theme();
    let label = if state == TorrentState::FetchingMetadata {
        "Fetching metadata".to_string()
    } else {
        format!("{:.1}%", progress * 100.0)
    };

    let fill_pct = progress.clamp(0.0, 1.0);

    ZStack(Modifier::new().width(width).height(10.0)).child((
        Box(Modifier::new()
            .fill_max_size()
            .background(th.surface_container_highest)
            .clip_rounded(5.0)),
        Box(Modifier::new()
            .width(width * fill_pct)
            .height(10.0)
            .background(fill_color)
            .clip_rounded(5.0)),
        Box(Modifier::new()
            .fill_max_size()
            .align_items(AlignItems::Center)
            .justify_content(JustifyContent::Center)
            .hit_passthrough())
        .child(Text(label).size(9.0).color(Color::WHITE.with_alpha(210))),
    ))
}

pub fn piece_map_view(have: &[bool], available_width: f32) -> View {
    let piece_size = 6.0;
    let spacing = 1.0;
    let step = piece_size + spacing;
    let columns = ((available_width / step) as usize).max(1);
    let rows = have.len().div_ceil(columns);

    let success_color = theme::success();
    let empty_color = Color::from_rgb(50, 50, 65);

    let mut row_views: Vec<View> = Vec::with_capacity(rows);

    for row_idx in 0..rows {
        let mut cells: Vec<View> = Vec::with_capacity(columns);
        for col in 0..columns {
            let idx = row_idx * columns + col;
            if idx < have.len() {
                let color = if have[idx] {
                    success_color
                } else {
                    empty_color
                };
                cells.push(Box(Modifier::new()
                    .size(piece_size, piece_size)
                    .background(color)
                    .clip_rounded(1.0)));
            }
        }
        row_views.push(Row(Modifier::new().height(step)).child(cells));
    }

    Column(Modifier::new().width(available_width)).child(row_views)
}
