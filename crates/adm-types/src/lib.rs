pub mod config;
pub mod download;
pub mod event;
pub mod models;
pub mod policy;
pub mod runtime;
pub mod utils;
pub mod validation;

pub use config::AppConfig;
pub use download::{
    ChunkDescriptor, ChunkState, ChunkUpdate, DownloadChunk, DownloadState, DownloadTask,
    WorkerHandle,
};
pub use event::Event;
pub use policy::{
    CertificatePinning, HostnameVerification, HttpsMode, HttpsPolicy, SelfSignedPolicy,
};
pub use runtime::{DownloadSnapshot, SystemStats};
pub use utils::{derive_filename_from_url, unix_millis, unix_secs};
pub use validation::{FilePathValidator, UrlValidator};
