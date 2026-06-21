use crate::types::TorrentState;
use crate::ui::theme;
use repose_core::prelude::*;
use repose_material::material3::{
    LinearProgressIndicator, LinearProgressIndicatorConfig,
};
use repose_ui::{
    box_with_constraints_with_key, Box, Column, Row, Text, TextStyle, ViewExt,
};

pub fn state_color(state: TorrentState) -> Color {
    match state {
        TorrentState::Downloading => theme::downloading(),
        TorrentState::Seeding => theme::seeding(),
        TorrentState::Paused => theme::paused(),
        TorrentState::Complete => theme::success(),
        TorrentState::Error => theme::error(),
        TorrentState::FetchingMetadata => theme::warning(),
        _ => theme::accent(),
    }
}

pub fn progress_bar_view(progress: f32, state: TorrentState) -> View {
    let th = theme();
    let label = if state == TorrentState::FetchingMetadata {
        "Fetching metadata".to_string()
    } else {
        format!("{:.1}%", progress * 100.0)
    };

    Column(Modifier::new().fill_max_width()).child((
        Row(Modifier::new().fill_max_width().height(16.0).align_items(AlignItems::Center))
            .child(Text(label).size(11.0).color(th.on_surface_variant)),
        LinearProgressIndicator(
            Some(progress.clamp(0.0, 1.0)),
            LinearProgressIndicatorConfig {
                color: state_color(state),
                ..Default::default()
            },
        ),
    ))
}

pub fn colored_progress_bar(progress: f32, color: Color) -> View {
    LinearProgressIndicator(
        Some(progress.clamp(0.0, 1.0)),
        LinearProgressIndicatorConfig {
            color,
            ..Default::default()
        },
    )
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
