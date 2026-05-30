use repose_core::prelude::*;
use repose_material::{Icon, Symbol};
use repose_ui::TextStyle;

repose_material::material_symbols! {
    ADD: '\u{e145}',
    FOLDER_OPEN: '\u{e2c8}',
    LINK: '\u{e157}',
    PLAY_ARROW: '\u{e037}',
    PAUSE: '\u{e034}',
    DELETE: '\u{e872}',
    SETTINGS: '\u{e8b8}',
    SEARCH: '\u{e8b6}',
    DOWNLOAD: '\u{f090}',
    UPLOAD: '\u{f09b}',
    CLOUD_DOWNLOAD: '\u{e2c0}',
    CLOUD_UPLOAD: '\u{e2c3}',
    CHECK_CIRCLE: '\u{e86c}',
    ERROR: '\u{e000}',
    SCHEDULE: '\u{e8b5}',
    INFO: '\u{e88e}',
    FOLDER: '\u{e2c7}',
    GROUP: '\u{e7ef}',
    PUBLIC: '\u{e80b}',
    EXTENSION: '\u{e87b}',
    FILTER_LIST: '\u{e152}',
    MEMORY: '\u{e322}',
    ROUTER: '\u{e328}',
}

pub fn icon(symbol: Symbol, size: f32, color: Color) -> View {
    Icon(symbol).size(size).color(color)
}
