use adm_storage::Storage;
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
pub use tokio_util::sync::CancellationToken;

pub mod file;
pub mod ftp;
pub mod http;
#[cfg(feature = "rtmp")]
pub mod rtmp;
pub mod s3;
pub mod sftp;
pub mod smb;
pub mod torrent;
pub mod webdav;

pub use crate::http::HttpNetworkClient;

#[async_trait]
pub trait ResponseStream: Send + Sync {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError>;
    fn total_bytes(&self) -> Option<u64>;
    async fn cancel(&mut self) -> Result<(), NetworkError>;
}

#[async_trait]
pub trait NetworkClient: Send + Sync {
    async fn execute(
        &self,
        req: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError>;

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError>;
}

#[derive(Debug, Clone, Default)]
pub struct NetworkRequest {
    pub url: String,
    pub range: Option<(u64, u64)>,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl NetworkRequest {
    #[must_use]
    pub fn new(url: impl Into<String>, range: Option<(u64, u64)>) -> Self {
        Self {
            url: url.into(),
            range,
            headers: vec![],
            body: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("Network error: {0}")]
    Io(String),
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("TLS error: {0}")]
    Tls(String),
    #[error("Timeout error")]
    Timeout,
    #[error("Other error: {0}")]
    Other(String),
}

impl From<std::io::Error> for NetworkError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

pub struct HeadInfo {
    pub content_length: Option<u64>,
    pub accept_ranges: bool,
    pub final_url: String,
}

pub struct Downloader {
    storage: Arc<Storage>,
    client: Arc<dyn NetworkClient>,
}

impl Downloader {
    #[must_use]
    pub fn new(storage: Arc<Storage>, client: Arc<dyn NetworkClient>) -> Self {
        Self { storage, client }
    }

    /// Downloads a single chunk of a file.
    pub async fn download_chunk(
        &self,
        task: &adm_types::DownloadTask,
        chunk: &mut adm_types::DownloadChunk,
    ) -> Result<()> {
        tracing::info!(chunk_index = chunk.index, "Downloading chunk");

        let req = NetworkRequest::new(
            task.url.clone(),
            Some((chunk.offset, chunk.offset + chunk.length - 1)),
        );
        let mut stream = self
            .client
            .execute(req)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        while let Some(_bytes) = stream.next_chunk().await.map_err(|e| anyhow::anyhow!(e))? {
            // In a real app, write to storage
        }

        chunk.set_state(adm_types::ChunkState::Completed);
        Ok(())
    }
}

#[derive(Clone)]
pub struct BandwidthLimiter {
    pub bytes_per_sec: u64,
}

impl BandwidthLimiter {
    pub fn new(kbps: u64) -> Self {
        Self {
            bytes_per_sec: kbps * 1024,
        }
    }

    pub async fn allow_bytes(&self, _amount: u64) -> bool {
        // Simple stub for now
        true
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Default)]
pub struct MockNetworkClient {
    pub data: Vec<u8>,
    pub chunk_size: usize,
    pub fail_at: Option<usize>,
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl NetworkClient for MockNetworkClient {
    async fn execute(
        &self,
        _req: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        Ok(Box::new(MockResponseStream {
            data: self.data.clone(),
            pos: 0,
        }))
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        Ok(HeadInfo {
            content_length: Some(self.data.len() as u64),
            accept_ranges: true,
            final_url: url.to_string(),
        })
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub struct MockResponseStream {
    pub data: Vec<u8>,
    pub pos: usize,
}

#[cfg(any(test, feature = "test-utils"))]
#[async_trait]
impl ResponseStream for MockResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        if self.pos >= self.data.len() {
            Ok(None)
        } else {
            let end = std::cmp::min(self.pos + 4096, self.data.len());
            let chunk = self.data[self.pos..end].to_vec();
            self.pos = end;
            Ok(Some(chunk))
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        Some(self.data.len() as u64)
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }
}
