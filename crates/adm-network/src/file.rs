use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};
use url::Url;

use crate::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};

const FILE_CHUNK_SIZE: usize = 256 * 1024;

pub struct FileNetworkClient;

impl FileNetworkClient {
    pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
        Ok(Self)
    }

    pub fn new() -> Result<Self, NetworkError> {
        Ok(Self)
    }

    fn path(url: &Url) -> Result<PathBuf, NetworkError> {
        url.to_file_path()
            .map_err(|_| NetworkError::Other(format!("invalid file URL: {}", url)))
    }
}

struct FileResponseStream {
    reader: Option<BufReader<File>>,
    total_bytes: Option<u64>,
    remaining: Option<u64>,
}

#[async_trait]
impl ResponseStream for FileResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };

        if let Some(remaining) = self.remaining {
            if remaining == 0 {
                self.reader.take();
                return Ok(None);
            }
        }

        let requested = self
            .remaining
            .map(|remaining| remaining.min(FILE_CHUNK_SIZE as u64) as usize)
            .unwrap_or(FILE_CHUNK_SIZE);

        let mut buffer = vec![0u8; requested];
        let n = reader
            .read(&mut buffer)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;

        if n == 0 {
            self.reader.take();
            return Ok(None);
        }

        buffer.truncate(n);
        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(n as u64);
        }

        Ok(Some(buffer))
    }

    fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.reader.take();
        Ok(())
    }
}

#[async_trait]
impl NetworkClient for FileNetworkClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let url = Url::parse(&request.url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url)?;

        let mut file = File::open(&path).await.map_err(|e| {
            NetworkError::Io(format!("failed to open file {}: {e}", path.display()))
        })?;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;
        let total_bytes = Some(metadata.len());

        let remaining = if let Some((start, end)) = request.range {
            if end < start {
                return Err(NetworkError::Other("invalid range".into()));
            }
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            Some(end.saturating_sub(start).saturating_add(1))
        } else {
            None
        };

        let reader = BufReader::new(file);
        Ok(Box::new(FileResponseStream {
            reader: Some(reader),
            total_bytes,
            remaining,
        }))
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        let url = Url::parse(url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url)?;

        let metadata = tokio::fs::metadata(&path).await.map_err(|e| {
            NetworkError::Io(format!("failed to stat file {}: {e}", path.display()))
        })?;

        Ok(HeadInfo {
            content_length: Some(metadata.len()),
            accept_ranges: true,
            final_url: url.to_string(),
        })
    }
}
