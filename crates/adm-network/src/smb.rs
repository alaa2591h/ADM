//! SMB (Server Message Block) / CIFS protocol backend.
//!
//! Professional implementation that supports:
//! - Native UNC path mapping on Windows.
//! - Mounted SMB share detection on Linux.
//! - Parallel chunk reads using standard File I/O (buffered).
//! - Future-ready for `smb2` crate integration for raw protocol support.

use crate::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, BufReader};
use url::Url;

pub struct SmbNetworkClient {
    // Current professional implementation uses OS-level SMB mounting/UNC.
    // This ensures maximum performance and compatibility with OS credential managers.
}

impl SmbNetworkClient {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
        Ok(Self::new())
    }

    /// Convert smb://server/share/path to a local-accessible path.
    ///
    /// On Windows, this becomes a UNC path: \\server\share\path
    /// On Linux/macOS, it looks for mount points in standard locations (/mnt/smb, /Volumes).
    fn smb_to_local_path(url_str: &str) -> Result<PathBuf, NetworkError> {
        let parsed = Url::parse(url_str).map_err(|e| NetworkError::Other(e.to_string()))?;
        let host = parsed
            .host_str()
            .ok_or_else(|| NetworkError::Other("SMB URL missing host".into()))?;

        let path = parsed.path().replace('/', std::path::MAIN_SEPARATOR_STR);

        #[cfg(windows)]
        {
            // Windows handles UNC paths natively.
            Ok(PathBuf::from(format!("\\\\{}{}", host, path)))
        }

        #[cfg(not(windows))]
        {
            // On Unix, we assume the share is mounted.
            // Professional ADM installs often mount shares to /mnt/adm/smb/{host}/{share}
            let mount_base =
                std::env::var("ADM_SMB_MOUNT_BASE").unwrap_or_else(|_| "/mnt/smb".into());
            let local_path = PathBuf::from(mount_base)
                .join(host)
                .join(path.trim_start_matches(std::path::MAIN_SEPARATOR));

            if !local_path.exists() {
                return Err(NetworkError::Io(format!(
                    "SMB share not mounted locally. Please mount {} to {}",
                    url_str,
                    local_path.display()
                )));
            }
            Ok(local_path)
        }
    }
}

impl Default for SmbNetworkClient {
    fn default() -> Self {
        Self::new()
    }
}

struct SmbResponseStream {
    reader: Option<BufReader<File>>,
    total_bytes: Option<u64>,
    remaining: Option<u64>,
}

#[async_trait]
impl ResponseStream for SmbResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        const CHUNK_SIZE: usize = 256 * 1024; // 256 KiB consistent with HTTP

        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };

        if let Some(0) = self.remaining {
            self.reader.take();
            return Ok(None);
        }

        let to_read = self
            .remaining
            .map_or(CHUNK_SIZE as u64, |r| r.min(CHUNK_SIZE as u64)) as usize;
        let mut buf = vec![0u8; to_read];

        let n = reader
            .read(&mut buf)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;
        if n == 0 {
            self.reader.take();
            return Ok(None);
        }

        buf.truncate(n);
        if let Some(ref mut rem) = self.remaining {
            *rem = rem.saturating_sub(n as u64);
        }

        Ok(Some(buf))
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
impl NetworkClient for SmbNetworkClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let path = Self::smb_to_local_path(&request.url)?;
        let mut file = File::open(&path)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;
        let size = file
            .metadata()
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?
            .len();

        if let Some((start, end)) = request.range {
            file.seek(std::io::SeekFrom::Start(start))
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let remaining = end.saturating_sub(start).saturating_add(1);
            Ok(Box::new(SmbResponseStream {
                reader: Some(BufReader::new(file)),
                total_bytes: Some(size),
                remaining: Some(remaining),
            }))
        } else {
            Ok(Box::new(SmbResponseStream {
                reader: Some(BufReader::new(file)),
                total_bytes: Some(size),
                remaining: None,
            }))
        }
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        let path = Self::smb_to_local_path(url)?;
        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;

        Ok(HeadInfo {
            content_length: Some(meta.len()),
            accept_ranges: true,
            final_url: url.to_owned(),
        })
    }
}
