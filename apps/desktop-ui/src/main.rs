// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX Download Manager — Professional Desktop UI                         ║
// ║  Main Entry Point with System Tray and Backend Integration.             ║
// ╚══════════════════════════════════════════════════════════════════════════╝

#![recursion_limit = "256"]

pub mod contracts;
pub mod adapters;
pub mod models;
pub mod state;
pub mod runtime;
pub mod bridge;
pub mod ui;

use tray_icon::{
    menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem},
    TrayIconBuilder,
};

// Generate Rust bindings from the Slint UI description.
slint::include_modules!();

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Setup Logging ─────────────────────────────────────────────────────
    tracing_subscriber::fmt::init();
    tracing::info!("Starting APEX Download Manager Desktop UI...");

    // ── System Tray Setup ─────────────────────────────────────────────────
    let tray_menu = Menu::new();
    let show_item = MenuItem::new("Open ADM", true, None);
    let quit_item = MenuItem::new("Quit", true, None);

    tray_menu.append_items(&[
        &show_item,
        &PredefinedMenuItem::separator(),
        &quit_item,
    ])?;

    let _tray_icon = TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu))
        .with_tooltip("APEX Download Manager")
        .build()?;

    let menu_channel = MenuEvent::receiver();
    let quit_id = quit_item.id().clone();
    
    // Spawn task to handle tray events
    tokio::spawn(async move {
        while let Ok(event) = menu_channel.recv() {
            if event.id == quit_id {
                std::process::exit(0);
            }
            // Logic for showing window is handled via Slint's event loop
        }
    });

    // ── Run Window ────────────────────────────────────────────────────────
    // Note: Slint needs to run on the main thread, so we call it here.
    // The tokio::main attribute allows us to use async for setup.
    
    if let Err(e) = ui::app_window::run() {
        tracing::error!("UI Error: {}", e);
        return Err(e.into());
    }

    Ok(())
}
