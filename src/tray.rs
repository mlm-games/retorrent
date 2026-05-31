use std::sync::mpsc;
use std::time::Duration;

use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem, PredefinedMenuItem};
use tray_icon::{Icon, TrayIconBuilder};

#[derive(Debug)]
pub enum TrayCommand {
    ToggleWindow,
    Quit,
}

const TOGGLE_ID: &str = "toggle";
const QUIT_ID: &str = "quit";

pub struct AppTray {
    #[cfg(target_os = "linux")]
    tooltip_tx: mpsc::Sender<String>,
    #[cfg(not(target_os = "linux"))]
    icon: tray_icon::TrayIcon,
}

impl AppTray {
    pub fn new(command_tx: mpsc::Sender<TrayCommand>) -> Self {
        let icon = create_icon();

        #[cfg(target_os = "linux")]
        {
            let (tooltip_tx, tooltip_rx) = mpsc::channel();

            std::thread::spawn(move || {
                gtk::init().expect("GTK init failed");

                let menu = build_menu();

                let tray = TrayIconBuilder::new()
                    .with_menu(Box::new(menu))
                    .with_tooltip("Retorrent")
                    .with_icon(icon)
                    .build()
                    .unwrap();

                MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
                    let id = event.id();
                    if id.as_ref() == TOGGLE_ID {
                        let _ = command_tx.send(TrayCommand::ToggleWindow);
                        repose_platform::wake_event_loop();
                    } else if id.as_ref() == QUIT_ID {
                        let _ = command_tx.send(TrayCommand::Quit);
                        repose_platform::wake_event_loop();
                    }
                }));

                gtk::glib::timeout_add_local(Duration::from_secs(1), move || {
                    while let Ok(tip) = tooltip_rx.try_recv() {
                        tray.set_tooltip(Some(&tip)).ok();
                    }
                    gtk::glib::ControlFlow::Continue
                });

                gtk::main();
            });

            Self { tooltip_tx }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let menu = build_menu();

            MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
                let id = event.id();
                if id.as_ref() == TOGGLE_ID {
                    let _ = command_tx.send(TrayCommand::ToggleWindow);
                } else if id.as_ref() == QUIT_ID {
                    let _ = command_tx.send(TrayCommand::Quit);
                }
            }));

            let tray = TrayIconBuilder::new()
                .with_menu(Box::new(menu))
                .with_tooltip("Rust Torrent")
                .with_icon(icon)
                .build()
                .unwrap();

            Self { icon: tray }
        }
    }

    pub fn set_tooltip(&self, text: &str) {
        #[cfg(target_os = "linux")]
        {
            let _ = self.tooltip_tx.send(text.to_string());
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.icon.set_tooltip(Some(text)).ok();
        }
    }
}

fn build_menu() -> Menu {
    let menu = Menu::new();

    let toggle_item = MenuItem::with_id(MenuId::new(TOGGLE_ID), "Show/Hide", true, None);
    let quit_item = MenuItem::with_id(MenuId::new(QUIT_ID), "Quit", true, None);

    menu.append_items(&[&toggle_item, &PredefinedMenuItem::separator(), &quit_item])
        .ok();

    menu
}

fn create_icon() -> Icon {
    let size = 32u32;
    let mut rgba = Vec::with_capacity((size * size * 4) as usize);

    for y in 0..size {
        for x in 0..size {
            let cx = (size / 2) as i32;
            let cy = (size / 2) as i32;
            let r = cx - 1;

            let dx = (x as i32) - cx;
            let dy = (y as i32) - cy;

            let in_circle = dx * dx + dy * dy <= r * r;

            if in_circle {
                let dist = ((dx as f32).powi(2) + (dy as f32).powi(2)).sqrt() / (r as f32);

                let red = (200.0 * (1.0 - dist * 0.3)) as u8;
                let green = (120.0 + 80.0 * (1.0 - dist)) as u8;
                let blue = (40.0 + 60.0 * (1.0 - dist)) as u8;

                let t_thickness = 4i32;
                let t_x = cx - 6;
                let t_y = cy - 6;
                let t_w = 12i32;
                let t_h = 12i32;

                let hbar = (x as i32) >= t_x && (x as i32) < t_x + t_w && (y as i32) == t_y;

                let vbar = (x as i32) >= (t_x + t_w / 2 - t_thickness / 2)
                    && (x as i32) < (t_x + t_w / 2 + t_thickness / 2)
                    && (y as i32) >= t_y
                    && (y as i32) < t_y + t_h;

                if hbar || vbar {
                    rgba.extend_from_slice(&[255, 255, 255, 255]);
                } else {
                    rgba.extend_from_slice(&[red, green, blue, 255]);
                }
            } else {
                rgba.extend_from_slice(&[0, 0, 0, 0]);
            }
        }
    }

    Icon::from_rgba(rgba, size, size).unwrap()
}
