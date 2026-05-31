//! Comprehensive error types for the APEX Download Manager (ADM) download engine.

use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Storage operation failed: {0}")]
    Storage(#[from] StorageError),

    #[error("Network request failed: {0}")]
    Network(String),

    #[error("Task {0} not found")]
    TaskNotFound(Uuid),

    #[error("Chunk {task_id}/{index} not found")]
    ChunkNotFound { task_id: Uuid, index: u32 },

    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition { from: String, to: String },

    #[error("Download cancelled by user")]
    Cancelled,

    #[error("File system error: {0}")]
    FileSystem(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Internal engine error: {0}")]
    Internal(String),
}

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Database locked after {0} retries")]
    DatabaseLocked(u32),

    #[error("Record not found")]
    NotFound,

    #[error("Constraint violation: {0}")]
    Constraint(String),
}
