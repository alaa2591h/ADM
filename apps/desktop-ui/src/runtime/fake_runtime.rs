// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — runtime/fake_runtime.rs                                       ║
// ║  The central fake runtime. Owns the simulation timer and all shared      ║
// ║  state. Keeps running as long as the returned FakeRuntime is alive.      ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use slint::ComponentHandle;

use crate::adapters::{MockAdapter, RealAdapter};
use crate::bridge::callbacks::wire_callbacks;
use crate::bridge::event_bus::EventBus;
use crate::bridge::ui_bridge::sync_to_ui;
use crate::contracts::{RuntimeAdapter, RuntimeConfig};
use crate::state::app_state::AppState;
use crate::MainWindow;

/// Simulation tick interval: 100 ms.
const TICK_MS: u64 = 100;

/// Owns the shared state + timer. Drop this to stop the runtime.
/// Keep it alive (e.g. `let _rt = FakeRuntime::start(...)`) for the
/// lifetime of the main event loop.
pub struct FakeRuntime {
    // These are kept alive so their Rc strong-counts stay > 0.
    _state: Arc<Mutex<AppState>>,
    _adapter: Rc<RefCell<Box<dyn RuntimeAdapter>>>,
    /// The simulation timer handle — dropping it cancels the timer.
    _timer: slint::Timer,
    /// The pulse timer drives blinking animations in the UI every 700 ms.
    _pulse_timer: slint::Timer,
}

impl FakeRuntime {
    /// Wire callbacks, perform the initial UI sync, and start the simulation
    /// timer. Equivalent to `start_with_adapter(window, "mock")`.
    #[allow(dead_code)]
    pub fn start(window: &MainWindow) -> Self {
        Self::start_with_adapter(window, "mock")
    }

    /// Start the runtime with a specific adapter backend.
    pub fn start_with_adapter(window: &MainWindow, adapter_type: &str) -> Self {
        let state = Arc::new(Mutex::new(AppState::new()));
        let adapter: Box<dyn RuntimeAdapter> = match adapter_type {
            "mock" => Box::new(MockAdapter::new(state.clone())),
            "real" => {
                let daemon_url = std::env::var("ADM_DAEMON_URL")
                    .unwrap_or_else(|_| "http://127.0.0.1:57423".to_string());
                tracing::info!("[runtime] connecting to real daemon at {}", daemon_url);
                Box::new(RealAdapter::new(state.clone(), &daemon_url))
            }
            _ => {
                eprintln!("[runtime] unknown adapter type '{}', falling back to mock", adapter_type);
                Box::new(MockAdapter::new(state.clone()))
            }
        };
        let adapter = Rc::new(RefCell::new(adapter));
        let bus   = Rc::new(RefCell::new(EventBus::new()));

        wire_callbacks(window, bus.clone());
        sync_to_ui(&state.lock().unwrap(), window);

        if let Err(err) = adapter.borrow_mut().initialize(RuntimeConfig::default()) {
            eprintln!("[runtime] adapter init failed: {:?}", err);
        }

        let timer = slint::Timer::default();
        let state_t = state.clone();
        let bus_t = bus.clone();
        let wh = window.as_weak();
        let adapter_t = adapter.clone();

        // Secondary timer: toggles PulseClock.tick every 700 ms to drive
        // the blinking animations in the UI (status dots, chunk highlights).
        let pulse_timer = slint::Timer::default();
        let wh_pulse = window.as_weak();
        pulse_timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(700),
            move || {
                if let Some(win) = wh_pulse.upgrade() {
                    let current = win.global::<crate::PulseClock>().get_tick();
                    win.global::<crate::PulseClock>().set_tick(!current);
                }
            },
        );

        timer.start(
            slint::TimerMode::Repeated,
            Duration::from_millis(TICK_MS),
            move || {
                let win = match wh.upgrade() {
                    Some(w) => w,
                    None => return,
                };

                let commands = bus_t.borrow_mut().drain();
                for command in commands {
                    let _ = adapter_t.borrow_mut().execute_command(command);
                }

                let _ = adapter_t.borrow_mut().tick();

                sync_to_ui(&state_t.lock().unwrap(), &win);
            },
        );

        FakeRuntime {
            _state: state,
            _adapter: adapter,
            _timer: timer,
            _pulse_timer: pulse_timer,
        }
    }
}
