pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TiB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

pub fn format_speed(bytes_per_sec: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_sec))
}

pub fn format_eta(seconds: Option<u64>) -> String {
    match seconds {
        None => "\u{221e}".to_string(),
        Some(0) => "done".to_string(),
        Some(s) => {
            let hours = s / 3600;
            let minutes = (s % 3600) / 60;
            let secs = s % 60;
            if hours > 0 {
                format!("{}h {:02}m {:02}s", hours, minutes, secs)
            } else if minutes > 0 {
                format!("{}m {:02}s", minutes, secs)
            } else {
                format!("{}s", secs)
            }
        }
    }
}

pub fn format_ratio(uploaded: u64, downloaded: u64) -> String {
    if downloaded == 0 {
        if uploaded == 0 {
            "0.000".to_string()
        } else {
            "\u{221e}".to_string()
        }
    } else {
        format!("{:.3}", uploaded as f64 / downloaded as f64)
    }
}
