// ╔══════════════════════════════════════════════════════════════════════════╗
// ║  APEX DM — contracts/runtime_api.rs                                      ║
// ║  Runtime API trait — defines the contract that any runtime must fulfill  ║
// ║  This allows switching between mock and real implementations              ║
// ╚══════════════════════════════════════════════════════════════════════════╝

use crate::contracts::dto::{DownloadDTO, StatisticsDTO, RuntimeConfig};
use crate::contracts::RuntimeEvent;
use crate::contracts::RuntimeCommand;

/// Result type for runtime operations
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Runtime errors
#[derive(Debug, Clone)]
pub enum RuntimeError {
    InvalidDownloadId(String),
    DownloadAlreadyExists(String),
    DownloadNotFound(String),
    InvalidUrl(String),
    ConfigurationError(String),
    InternalError(String),
    NotInitialized,
}

impl RuntimeError {
    pub fn message(&self) -> String {
        match self {
            RuntimeError::InvalidDownloadId(id) => format!("Invalid download ID: {}", id),
            RuntimeError::DownloadAlreadyExists(id) => format!("Download already exists: {}", id),
            RuntimeError::DownloadNotFound(id) => format!("Download not found: {}", id),
            RuntimeError::InvalidUrl(url) => format!("Invalid URL: {}", url),
            RuntimeError::ConfigurationError(msg) => format!("Configuration error: {}", msg),
            RuntimeError::InternalError(msg) => format!("Internal error: {}", msg),
            RuntimeError::NotInitialized => "Runtime not initialized".to_string(),
        }
    }
}

/// RuntimeAdapter trait — defines the contract that any runtime implementation must satisfy
/// This is the core abstraction that allows swapping mock and real backends
pub trait RuntimeAdapter: Send + Sync {
    // ────────────────────────────────────────────────────────────────────────
    // Lifecycle
    // ────────────────────────────────────────────────────────────────────────
    
    /// Initialize the runtime with configuration
    fn initialize(&mut self, config: RuntimeConfig) -> RuntimeResult<()>;
    
    /// Perform a single tick of the runtime (called by the main loop)
    /// Returns true if there's more work to do
    fn tick(&mut self) -> RuntimeResult<bool>;
    
    /// Shut down the runtime gracefully
    fn shutdown(&mut self) -> RuntimeResult<()>;
    
    /// Check if runtime is initialized
    fn is_initialized(&self) -> bool;
    
    // ────────────────────────────────────────────────────────────────────────
    // Command Processing
    // ────────────────────────────────────────────────────────────────────────
    
    /// Process a command from the UI layer
    fn execute_command(&mut self, command: RuntimeCommand) -> RuntimeResult<()>;
    
    // ────────────────────────────────────────────────────────────────────────
    // State Access
    // ────────────────────────────────────────────────────────────────────────
    
    /// Get all downloads
    fn get_downloads(&self) -> RuntimeResult<Vec<DownloadDTO>>;
    
    /// Get a specific download by ID
    fn get_download(&self, id: &str) -> RuntimeResult<DownloadDTO>;
    
    /// Get current statistics
    fn get_statistics(&self) -> RuntimeResult<StatisticsDTO>;
    
    // ────────────────────────────────────────────────────────────────────────
    // Event Handling
    // ────────────────────────────────────────────────────────────────────────
    
    /// Drain all pending events (FIFO order)
    fn drain_events(&mut self) -> Vec<RuntimeEvent>;
    
    /// Check if there are pending events
    fn has_pending_events(&self) -> bool;
}

/// RuntimeAdapterFactory — creates runtime adapters
/// This pattern allows flexible instantiation of different implementations
pub trait RuntimeAdapterFactory {
    /// Create a new runtime adapter instance
    fn create(&self) -> Box<dyn RuntimeAdapter>;
}

/// Default implementation marker for builder pattern
pub struct RuntimeAdapterBuilder {
    adapter_type: String,
}

impl RuntimeAdapterBuilder {
    pub fn new(adapter_type: &str) -> Self {
        RuntimeAdapterBuilder {
            adapter_type: adapter_type.to_string(),
        }
    }
    
    pub fn build(&self) -> RuntimeResult<Box<dyn RuntimeAdapter>> {
        match self.adapter_type.as_str() {
            "mock" => {
                // Creates a standalone MockAdapter with its own fresh AppState.
                // For the main application, FakeRuntime::start_with_adapter is used
                // directly so that the shared Arc<Mutex<AppState>> is wired correctly.
                let state = std::sync::Arc::new(std::sync::Mutex::new(
                    crate::state::app_state::AppState::new()
                ));
                Ok(Box::new(crate::adapters::MockAdapter::new(state)))
            }
            _ => Err(RuntimeError::ConfigurationError(
                format!("Unknown adapter type: {}", self.adapter_type),
            )),
        }
    }
}
