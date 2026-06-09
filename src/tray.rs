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
    let img_bytes = include_bytes!("../others/packaging/icon.png");
    let img = image::load_from_memory(img_bytes)
        .expect("Failed to load tray icon")
        .resize_exact(64, 64, image::imageops::FilterType::Lanczos3)
        .to_rgba8();
    let (width, height) = img.dimensions();
    Icon::from_rgba(img.into_raw(), width, height).unwrap()
}
