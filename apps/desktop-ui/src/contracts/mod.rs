// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — contracts/mod.rs                                              ║
// ║  Backend-agnostic contracts module                                        ║
// ║  Defines the stable interfaces for runtime integration                   ║
// ╚══════════════════════════════════════════════════════════════════════════╝

pub mod dto;
pub mod events;
pub mod commands;
pub mod runtime_api;

// Re-export commonly used types at the contracts level
pub use dto::{DownloadDTO, DownloadStatus, ChunkDTO, StatisticsDTO, RuntimeConfig};
pub use events::RuntimeEvent;
pub use commands::RuntimeCommand;
pub use runtime_api::{
    RuntimeAdapter,
    RuntimeAdapterFactory,
    RuntimeAdapterBuilder,
    RuntimeError,
    RuntimeResult,
};
