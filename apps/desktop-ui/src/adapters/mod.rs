// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — adapters/mod.rs                                               ║
// ║  Adapter implementations — bridges between UI and runtime backends       ║
// ║  Allows swapping between mock and real implementations                   ║
// ╚══════════════════════════════════════════════════════════════════════════╝

pub mod mock_adapter;
pub mod real_adapter;
pub mod runtime_adapter;

pub use mock_adapter::{MockAdapter, MockAdapterFactory};
pub use real_adapter::RealAdapter;
pub use runtime_adapter::{AdapterRegistry, CommandDispatcher, EventAggregator};
