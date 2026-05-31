use async_ftp::types::FileType;
use async_ftp::FtpStream;
use async_trait::async_trait;
use std::convert::TryFrom;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_rustls_023::rustls::{ClientConfig, RootCertStore, ServerName};
use url::Url;

use crate::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};

const FTP_CHUNK_SIZE: usize = 256 * 1024;

pub struct FtpNetworkClient {
    tls_config: Option<Arc<ClientConfig>>,
}

impl FtpNetworkClient {
    pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
        Self::new()
    }

    pub fn new() -> Result<Self, NetworkError> {
        let mut root_store = RootCertStore::empty();
        root_store.add_server_trust_anchors(webpki_roots_022::TLS_SERVER_ROOTS.0.iter().map(
            |ta| {
                tokio_rustls_023::rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
                    ta.subject,
                    ta.spki,
                    ta.name_constraints,
                )
            },
        ));

        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(Self {
            tls_config: Some(Arc::new(config)),
        })
    }

    async fn connect(&self, url: &Url) -> Result<FtpStream, NetworkError> {
        let host = url
            .host_str()
            .ok_or_else(|| NetworkError::Other("FTP URL missing host".into()))?;
        let port = url.port_or_known_default().unwrap_or(21);
        let addr = format!("{}:{}", host, port);

        let mut stream = FtpStream::connect(addr)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;

        if url.scheme() == "ftps" {
            let tls_config = self
                .tls_config
                .as_ref()
                .ok_or_else(|| NetworkError::Tls("missing TLS configuration".into()))?;

            let server_name = if let Ok(ip_addr) = host.parse::<std::net::IpAddr>() {
                ServerName::IpAddress(ip_addr)
            } else {
                ServerName::try_from(host)
                    .map_err(|e| NetworkError::Tls(format!("invalid server name: {e}")))?
            };

            stream = stream
                .into_secure((**tls_config).clone(), server_name)
                .await
                .map_err(|e| NetworkError::Tls(e.to_string()))?;
        }

        let username = url.username();
        let password = url.password().unwrap_or("anonymous");
        let user = if username.is_empty() {
            "anonymous"
        } else {
            username
        };

        stream
            .login(user, password)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;
        stream
            .transfer_type(FileType::Binary)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?;

        Ok(stream)
    }

    fn path(url: &Url) -> String {
        if url.path().is_empty() {
            "/".to_string()
        } else {
            url.path().to_string()
        }
    }
}

struct FtpResponseStream {
    reader: Option<BufReader<async_ftp::DataStream>>,
    total_bytes: Option<u64>,
    remaining: Option<u64>,
}

#[async_trait]
impl ResponseStream for FtpResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let reader = match self.reader.as_mut() {
            Some(reader) => reader,
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
            .map(|remaining| remaining.min(FTP_CHUNK_SIZE as u64) as usize)
            .unwrap_or(FTP_CHUNK_SIZE);

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
impl NetworkClient for FtpNetworkClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let url = Url::parse(&request.url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url);
        let mut stream = self.connect(&url).await?;

        if let Some((start, end)) = request.range {
            if end < start {
                return Err(NetworkError::Other("invalid range".into()));
            }
            stream
                .restart_from(start)
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let data_reader = stream
                .get(&path)
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let total_bytes = stream
                .size(&path)
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?
                .map(|v| v as u64);
            Ok(Box::new(FtpResponseStream {
                reader: Some(data_reader),
                total_bytes,
                remaining: Some(end.saturating_sub(start).saturating_add(1)),
            }))
        } else {
            let data_reader = stream
                .get(&path)
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let total_bytes = stream
                .size(&path)
                .await
                .map_err(|e| NetworkError::Io(e.to_string()))?
                .map(|v| v as u64);
            Ok(Box::new(FtpResponseStream {
                reader: Some(data_reader),
                total_bytes,
                remaining: None,
            }))
        }
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        let url = Url::parse(url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url);
        let mut stream = self.connect(&url).await?;

        let content_length = stream
            .size(&path)
            .await
            .map_err(|e| NetworkError::Io(e.to_string()))?
            .map(|v| v as u64);

        let final_url = url.to_string();
        let accept_ranges = true;

        let _ = stream.quit().await;

        Ok(HeadInfo {
            content_length,
            accept_ranges,
            final_url,
        })
    }
}

pub struct MultiNetworkClient {
    http: Arc<dyn NetworkClient>,
    ftp: Arc<dyn NetworkClient>,
    sftp: Arc<dyn NetworkClient>,
    file: Arc<dyn NetworkClient>,
    torrent: Arc<dyn NetworkClient>,
    smb: Arc<dyn NetworkClient>,
    webdav: Arc<dyn NetworkClient>,
    s3: Arc<dyn NetworkClient>,
}

impl MultiNetworkClient {
    pub fn new(
        http: Arc<dyn NetworkClient>,
        ftp: Arc<dyn NetworkClient>,
        sftp: Arc<dyn NetworkClient>,
        file: Arc<dyn NetworkClient>,
        torrent: Arc<dyn NetworkClient>,
        smb: Arc<dyn NetworkClient>,
        webdav: Arc<dyn NetworkClient>,
        s3: Arc<dyn NetworkClient>,
    ) -> Self {
        Self {
            http,
            ftp,
            sftp,
            file,
            torrent,
            smb,
            webdav,
            s3,
        }
    }
}

#[async_trait]
impl NetworkClient for MultiNetworkClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let url = Url::parse(&request.url).map_err(|e| NetworkError::Other(e.to_string()))?;
        match url.scheme() {
            "http" | "https" => self.http.execute(request).await,
            "ftp" | "ftps" => self.ftp.execute(request).await,
            "sftp" => self.sftp.execute(request).await,
            "file" => self.file.execute(request).await,
            "magnet" | "torrent" => self.torrent.execute(request).await,
            "smb" => self.smb.execute(request).await,
            "webdav" => self.webdav.execute(request).await,
            "s3" => self.s3.execute(request).await,
            scheme => Err(NetworkError::Other(format!(
                "unsupported protocol: {}",
                scheme
            ))),
        }
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        let url = Url::parse(url).map_err(|e| NetworkError::Other(e.to_string()))?;
        match url.scheme() {
            "http" | "https" => self.http.head(url.as_str()).await,
            "ftp" | "ftps" => self.ftp.head(url.as_str()).await,
            "sftp" => self.sftp.head(url.as_str()).await,
            "file" => self.file.head(url.as_str()).await,
            "magnet" | "torrent" => self.torrent.head(url.as_str()).await,
            "smb" => self.smb.head(url.as_str()).await,
            "webdav" => self.webdav.head(url.as_str()).await,
            "s3" => self.s3.head(url.as_str()).await,
            scheme => Err(NetworkError::Other(format!(
                "unsupported protocol: {}",
                scheme
            ))),
        }
    }
}
