use crate::types::TorrentState;
use crate::ui::theme;
use repose_core::{PaddingValues, prelude::*};
use repose_material::material3::{LinearProgressIndicator, LinearProgressIndicatorConfig};
use repose_ui::{Box, Column, Row, Text, TextStyle, ViewExt, box_with_constraints_with_key};

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
        Row(Modifier::new()
            .fill_max_width()
            .height(16.0)
            .align_items(AlignItems::Center))
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

pub fn piece_map_view(have: &[bool]) -> View {
    let success_color = theme::success();
    let empty_color = Color::from_rgb(50, 50, 65);

    let have_data = have.to_vec();

    box_with_constraints_with_key(
        have_data.clone(),
        Modifier::new().fill_max_size(),
        move |scope| {
            let n = have_data.len();

            let edge_padding = 8.0;
            let spacing = 1.0;
            let min_piece_size = 3.0;

            if n == 0 {
                return Box(Modifier::new().fill_max_size());
            }

            let available_w = scope.max_width.max(0.0);
            let available_h = (scope.max_height / 2.0).max(0.0); // HACK: goes offscreen to the bottom without this

            if available_w < min_piece_size || available_h < min_piece_size {
                return Box(Modifier::new().fill_max_size());
            }

            let max_cols_to_test = (((available_w + spacing) / (min_piece_size + spacing)).floor()
                as usize)
                .max(1)
                .min(n);

            let mut best_columns = 1usize;
            let mut best_piece_size = min_piece_size;

            for columns in 1..=max_cols_to_test {
                let rows = n.div_ceil(columns);

                let total_spacing_w = columns.saturating_sub(1) as f32 * spacing;
                let total_spacing_h = rows.saturating_sub(1) as f32 * spacing;

                let piece_w = (available_w - total_spacing_w) / columns as f32;
                let piece_h = (available_h - total_spacing_h) / rows as f32;

                let piece_size = piece_w.min(piece_h).floor();

                if piece_size >= min_piece_size && piece_size > best_piece_size {
                    best_piece_size = piece_size;
                    best_columns = columns;
                }
            }

            let piece_size = best_piece_size;
            let columns = best_columns;
            let rows = n.div_ceil(columns);

            let grid_w = columns as f32 * piece_size + columns.saturating_sub(1) as f32 * spacing;
            let grid_h = rows as f32 * piece_size + rows.saturating_sub(1) as f32 * spacing;

            let mut row_views: Vec<View> = Vec::with_capacity(rows * 2);

            for row_idx in 0..rows {
                let mut cells: Vec<View> = Vec::with_capacity(columns * 2);

                for col in 0..columns {
                    let idx = row_idx * columns + col;

                    if idx < n {
                        let color = if have_data[idx] {
                            success_color
                        } else {
                            empty_color
                        };

                        cells.push(Box(Modifier::new()
                            .size(piece_size, piece_size)
                            .background(color)
                            .clip_rounded(1.0)));

                        if col + 1 < columns && idx + 1 < n {
                            cells.push(Box(Modifier::new().width(spacing)));
                        }
                    }
                }

                row_views.push(Row(Modifier::new().height(piece_size)).child(cells));

                if row_idx + 1 < rows {
                    row_views.push(Box(Modifier::new().height(spacing)));
                }
            }

            Box(Modifier::new()
                .fill_max_size()
                .padding(edge_padding)
                .align_items(AlignItems::Center)
                .justify_content(JustifyContent::Center))
            .child(Column(Modifier::new().size(grid_w, grid_h)).child(row_views))
        },
    )
}
