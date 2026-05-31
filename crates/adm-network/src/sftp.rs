use async_trait::async_trait;
use ssh2::{OpenFlags, OpenType, Session};
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpStream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::mpsc;
use tokio::task;
use url::Url;

use crate::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};

const SFTP_CHUNK_SIZE: usize = 256 * 1024;

pub struct SftpNetworkClient;

impl SftpNetworkClient {
    pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
        Ok(Self)
    }

    pub fn new() -> Result<Self, NetworkError> {
        Ok(Self)
    }

    fn path(url: &Url) -> Result<String, NetworkError> {
        let path = url.path();
        if path.is_empty() {
            return Err(NetworkError::Other("SFTP URL missing path".into()));
        }
        Ok(path.to_string())
    }

    fn username(url: &Url) -> String {
        let user = url.username();
        if !user.is_empty() {
            return user.to_string();
        }

        std::env::var("USER")
            .or_else(|_| std::env::var("USERNAME"))
            .unwrap_or_else(|_| "anonymous".into())
    }

    fn password(url: &Url) -> Option<String> {
        url.password().map(|p| p.to_string())
    }

    fn host_addr(url: &Url) -> Result<(String, u16), NetworkError> {
        let host = url
            .host_str()
            .ok_or_else(|| NetworkError::Other("SFTP URL missing host".into()))?;
        let port = url.port_or_known_default().unwrap_or(22);
        Ok((host.to_string(), port))
    }

    fn connect(url: &Url) -> Result<Session, NetworkError> {
        let (host, port) = Self::host_addr(url)?;
        let address = format!("{}:{}", host, port);
        let tcp = TcpStream::connect(address).map_err(|e| NetworkError::Io(e.to_string()))?;

        let mut session = Session::new()
            .map_err(|e| NetworkError::Other(format!("failed to create SSH session: {e}")))?;
        session.set_tcp_stream(tcp);
        session
            .handshake()
            .map_err(|e| NetworkError::Io(e.to_string()))?;

        let username = Self::username(url);
        match Self::password(url) {
            Some(password) => {
                session
                    .userauth_password(&username, &password)
                    .map_err(|e| NetworkError::Io(e.to_string()))?;
            }
            None => {
                if session.userauth_agent(&username).is_err() {
                    return Err(NetworkError::Other(
                        "SFTP auth failed: password missing and SSH agent unavailable".into(),
                    ));
                }
            }
        }

        if !session.authenticated() {
            return Err(NetworkError::Other("SFTP authentication failed".into()));
        }

        Ok(session)
    }
}

struct SftpResponseStream {
    receiver: mpsc::Receiver<Result<Vec<u8>, NetworkError>>,
    total_bytes: Option<u64>,
    cancelled: Arc<AtomicBool>,
}

#[async_trait]
impl ResponseStream for SftpResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        match self.receiver.recv().await {
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.total_bytes
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.cancelled.store(true, Ordering::SeqCst);
        self.receiver.close();
        Ok(())
    }
}

#[async_trait]
impl NetworkClient for SftpNetworkClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let url = Url::parse(&request.url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url)?;
        let range = request.range;
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_read = cancelled.clone();

        let (tx, rx) = mpsc::channel(2);
        let url_string = request.url.clone();

        let path_for_stat = path.clone();
        let total_bytes: Option<u64> = task::spawn_blocking(move || {
            let url = Url::parse(&url_string).map_err(|e| NetworkError::Other(e.to_string()))?;
            let session = Self::connect(&url)?;
            let sftp = session
                .sftp()
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let file_stat = sftp
                .stat(std::path::Path::new(&path_for_stat))
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            Ok::<Option<u64>, NetworkError>(file_stat.size)
        })
        .await
        .map_err(|e| NetworkError::Other(e.to_string()))??;

        let total_bytes =
            total_bytes.ok_or_else(|| NetworkError::Other("could not get file size".into()))?;
        let url_string = request.url.clone();
        let tx_task = tx.clone();

        task::spawn_blocking(move || {
            let url = match Url::parse(&url_string) {
                Ok(value) => value,
                Err(e) => {
                    let _ = tx_task.blocking_send(Err(NetworkError::Other(e.to_string())));
                    return;
                }
            };

            let session = match Self::connect(&url) {
                Ok(value) => value,
                Err(error) => {
                    let _ = tx_task.blocking_send(Err(error));
                    return;
                }
            };

            let sftp = match session.sftp() {
                Ok(value) => value,
                Err(e) => {
                    let _ = tx_task.blocking_send(Err(NetworkError::Io(e.to_string())));
                    return;
                }
            };

            let mut file = match sftp.open_mode(&path, OpenFlags::READ, 0, OpenType::File) {
                Ok(value) => value,
                Err(e) => {
                    let _ = tx_task.blocking_send(Err(NetworkError::Io(e.to_string())));
                    return;
                }
            };

            if let Some((start, _end)) = range {
                if let Err(e) = file.seek(SeekFrom::Start(start)) {
                    let _ = tx_task.blocking_send(Err(NetworkError::Io(e.to_string())));
                    return;
                }
            }

            let mut remaining =
                range.map(|(start, end)| end.saturating_sub(start).saturating_add(1));
            let mut buffer = vec![0u8; SFTP_CHUNK_SIZE];

            loop {
                if cancelled_read.load(Ordering::SeqCst) {
                    break;
                }

                let chunk_size = remaining
                    .map(|rem| rem.min(SFTP_CHUNK_SIZE as u64) as usize)
                    .unwrap_or(SFTP_CHUNK_SIZE);
                let read_buf = &mut buffer[..chunk_size];
                match file.read(read_buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Some(rem) = remaining.as_mut() {
                            *rem = rem.saturating_sub(n as u64);
                        }
                        let chunk = read_buf[..n].to_vec();
                        if tx_task.blocking_send(Ok(chunk)).is_err() {
                            break;
                        }
                        if let Some(0) = remaining {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx_task.blocking_send(Err(NetworkError::Io(e.to_string())));
                        break;
                    }
                }
            }
        });

        Ok(Box::new(SftpResponseStream {
            receiver: rx,
            total_bytes: Some(total_bytes),
            cancelled,
        }))
    }

    async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
        let final_url = url.to_string();
        let url = Url::parse(url).map_err(|e| NetworkError::Other(e.to_string()))?;
        let path = Self::path(&url)?;

        let total_bytes: Option<u64> = task::spawn_blocking(move || {
            let session = Self::connect(&url)?;
            let sftp = session
                .sftp()
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            let file_stat = sftp
                .stat(std::path::Path::new(&path))
                .map_err(|e| NetworkError::Io(e.to_string()))?;
            Ok::<Option<u64>, NetworkError>(file_stat.size)
        })
        .await
        .map_err(|e| NetworkError::Other(e.to_string()))??;

        Ok(HeadInfo {
            content_length: total_bytes,
            accept_ranges: true,
            final_url,
        })
    }
}
