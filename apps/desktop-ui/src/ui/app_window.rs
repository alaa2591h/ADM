// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — ui/app_window.rs                                              ║
// ║  Window creation helpers.                                                ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use slint::ComponentHandle;

use crate::MainWindow;
use crate::runtime::fake_runtime::FakeRuntime;

/// Create the main window, start the runtime, and run the event loop.
pub fn run() -> Result<(), slint::PlatformError> {
    let window = MainWindow::new()?;

    // Use "real" backend if specified, or auto-detect.
    let backend = std::env::var("ADM_BACKEND").unwrap_or_else(|_| "real".to_string());
    let _runtime = FakeRuntime::start_with_adapter(&window, &backend);

    window.run()
}
