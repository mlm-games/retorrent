use repose_core::{Color, ColorScheme, Theme};

pub fn dark_theme() -> Theme {
    let mut cs = ColorScheme::dark();

    cs.background = Color::from_rgb(9, 11, 18);
    cs.surface = Color::from_rgb(15, 18, 28);
    cs.surface_container_lowest = Color::from_rgb(8, 10, 16);
    cs.surface_container_low = Color::from_rgb(13, 16, 24);
    cs.surface_container = Color::from_rgb(18, 22, 34);
    cs.surface_container_high = Color::from_rgb(24, 29, 44);
    cs.surface_container_highest = Color::from_rgb(31, 37, 55);

    cs.on_surface = Color::from_rgb(232, 236, 246);
    cs.on_surface_variant = Color::from_rgb(156, 166, 190);

    cs.primary = Color::from_rgb(125, 161, 255);
    cs.on_primary = Color::from_rgb(9, 20, 45);
    cs.primary_container = Color::from_rgb(36, 63, 140);
    cs.on_primary_container = Color::from_rgb(218, 226, 255);

    cs.secondary = Color::from_rgb(103, 220, 160);
    cs.outline = Color::from_rgb(58, 68, 92);
    cs.outline_variant = Color::from_rgb(37, 45, 64);

    cs.error = Color::from_rgb(255, 117, 117);
    cs.scrim = Color::from_rgba(0, 0, 0, 180);

    let mut t = Theme::default().with_colors(cs);
    t.scrollbar_track = Color::from_rgba(232, 236, 246, 18);
    t.scrollbar_thumb = Color::from_rgba(232, 236, 246, 90);
    t
}

pub fn accent() -> Color {
    Color::from_rgb(125, 161, 255)
}

pub fn success() -> Color {
    Color::from_rgb(103, 220, 160)
}

pub fn warning() -> Color {
    Color::from_rgb(255, 190, 90)
}

pub fn error() -> Color {
    Color::from_rgb(255, 117, 117)
}

pub fn downloading() -> Color {
    Color::from_rgb(91, 176, 255)
}

pub fn seeding() -> Color {
    Color::from_rgb(103, 220, 160)
}

pub fn paused() -> Color {
    Color::from_rgb(150, 158, 178)
}

pub fn metadata() -> Color {
    Color::from_rgb(194, 151, 255)
}
