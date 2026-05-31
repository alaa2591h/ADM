#![allow(clippy::unused_async)]

use crate::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};
use async_trait::async_trait;

#[cfg(not(feature = "torrent"))]
mod stub {
    use super::*;
    use std::path::PathBuf;

    pub struct TorrentNetworkClient {
        cache_dir: PathBuf,
    }

    impl TorrentNetworkClient {
        pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
            let cache_dir = std::env::temp_dir().join("apex-torrent-cache");
            if !cache_dir.exists() {
                std::fs::create_dir_all(&cache_dir).map_err(|e| NetworkError::Io(e.to_string()))?;
            }
            Ok(Self { cache_dir })
        }
    }

    #[async_trait]
    impl NetworkClient for TorrentNetworkClient {
        async fn execute(
            &self,
            _request: NetworkRequest,
        ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
            Err(NetworkError::Other("torrent feature not enabled".into()))
        }

        async fn head(&self, _url: &str) -> Result<HeadInfo, NetworkError> {
            Err(NetworkError::Other("torrent feature not enabled".into()))
        }
    }
}

#[cfg(not(feature = "torrent"))]
pub use stub::TorrentNetworkClient;

#[cfg(feature = "torrent")]
mod real {
    use super::*;
    use crate::http::stream_adapters::AsyncReadResponseStream;
    use librqbit::api::TorrentIdOrHash;
    use librqbit::Session;
    use librqbit::{AddTorrent, Api};
    use librqbit_core::hash_id::Id20;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;
    use tokio::sync::{Mutex, Notify};

    struct Inner {
        session: Option<Arc<Session>>,
        api: Option<Api>,
        // map infohash string -> numeric torrent id
        ids: HashMap<String, usize>,
        #[allow(dead_code)]
        cache_dir: PathBuf,
    }

    pub struct TorrentNetworkClient {
        inner: Arc<Mutex<Inner>>,
        ready: Arc<Notify>,
    }

    impl TorrentNetworkClient {
        pub fn from_config(_cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
            let cache_dir = std::env::temp_dir().join("apex-torrent-cache");
            if !cache_dir.exists() {
                std::fs::create_dir_all(&cache_dir).map_err(|e| NetworkError::Io(e.to_string()))?;
            }

            let inner = Inner {
                session: None,
                api: None,
                ids: HashMap::new(),
                cache_dir: cache_dir.clone(),
            };
            let inner = Arc::new(Mutex::new(inner));
            let ready = Arc::new(Notify::new());

            // Spawn background task to initialize librqbit session asynchronously.
            let bg_inner = inner.clone();
            let bg_ready = ready.clone();
            tokio::spawn(async move {
                match Session::new(cache_dir).await {
                    Ok(s) => {
                        let api = Api::new(s.clone(), None);
                        let mut lock = bg_inner.lock().await;
                        lock.session = Some(s);
                        lock.api = Some(api);
                        bg_ready.notify_waiters();
                    }
                    Err(e) => {
                        tracing::error!(error=?e, "failed to initialize librqbit session");
                    }
                }
            });

            Ok(Self { inner, ready })
        }

        async fn ensure_ready(&self) -> Result<Arc<Session>, NetworkError> {
            // Fast path
            {
                let lock = self.inner.lock().await;
                if let Some(s) = lock.session.as_ref() {
                    return Ok(s.clone());
                }
            }
            // Wait for background init
            self.ready.notified().await;
            let lock = self.inner.lock().await;
            lock.session
                .as_ref()
                .cloned()
                .ok_or_else(|| NetworkError::Other("failed to init torrent session".into()))
        }

        pub async fn add_magnet_simple(&self, magnet: &str) -> Result<String, NetworkError> {
            let session = self.ensure_ready().await?;
            let add = AddTorrent::from_url(magnet.to_owned());
            let res = session
                .add_torrent(add, None)
                .await
                .map_err(|e| NetworkError::Other(e.to_string()))?;
            let handle = res.into_handle().ok_or_else(|| {
                NetworkError::Other("magnet resolved to list-only or not added".into())
            })?;
            let info_hash = handle.info_hash().as_string();
            // store numeric id if available
            let id = handle.id();
            let mut lock = self.inner.lock().await;
            lock.ids.insert(info_hash.clone(), id);
            Ok(info_hash)
        }

        async fn lookup_handle_by_hash(
            &self,
            hash: &str,
        ) -> Result<Arc<librqbit::ManagedTorrent>, NetworkError> {
            let session = self.ensure_ready().await?;
            let id20 = Id20::from_str(hash).map_err(|e| NetworkError::Other(e.to_string()))?;
            session
                .get(TorrentIdOrHash::Hash(id20))
                .ok_or_else(|| NetworkError::Other("torrent not found".into()))
        }

        async fn stream_from_handle(
            &self,
            handle: Arc<librqbit::ManagedTorrent>,
            file_index: usize,
            _range: Option<(u64, u64)>,
        ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
            // Use Api::api_stream to get a FileStream which implements AsyncRead
            let api = {
                let lock = self.inner.lock().await;
                lock.api
                    .clone()
                    .ok_or_else(|| NetworkError::Other("api missing".into()))?
            };
            // need to get torrent id or hash to call api_stream; prefer id
            let id = handle.id();
            let fs = api
                .api_stream(TorrentIdOrHash::Id(id), file_index)
                .map_err(|e| NetworkError::Other(format!("api stream error: {e:?}")))?;
            // `fs` implements AsyncRead; wrap it in an adapter implementing our ResponseStream
            let stream = AsyncReadResponseStream::new(fs);
            Ok(Box::new(stream))
        }
    }

    #[async_trait]
    impl NetworkClient for TorrentNetworkClient {
        async fn execute(
            &self,
            request: NetworkRequest,
        ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
            let url =
                url::Url::parse(&request.url).map_err(|e| NetworkError::Other(e.to_string()))?;
            match url.scheme() {
                "magnet" => {
                    // add or lookup
                    let _hash = if url.path().len() == 40 {
                        url.path().to_string()
                    } else {
                        request.url.clone()
                    };
                    let info_hash = self.add_magnet_simple(&request.url).await?;
                    // try first file (0)
                    let handle = self.lookup_handle_by_hash(&info_hash).await?;
                    self.stream_from_handle(handle, 0, request.range).await
                }
                "torrent" => {
                    // url points to .torrent resource — let session fetch and add
                    let session = self.ensure_ready().await?;
                    let add = AddTorrent::from_url(request.url.clone());
                    let res = session
                        .add_torrent(add, None)
                        .await
                        .map_err(|e| NetworkError::Other(e.to_string()))?;
                    let handle = res.into_handle().ok_or_else(|| {
                        NetworkError::Other("torrent add resulted in list-only".into())
                    })?;
                    self.stream_from_handle(handle, 0, request.range).await
                }
                _ => Err(NetworkError::Other(
                    "unsupported scheme for torrent client".into(),
                )),
            }
        }

        async fn head(&self, url: &str) -> Result<HeadInfo, NetworkError> {
            let parsed = url::Url::parse(url).map_err(|e| NetworkError::Other(e.to_string()))?;
            match parsed.scheme() {
                "magnet" => Ok(HeadInfo {
                    content_length: None,
                    accept_ranges: true,
                    final_url: url.to_owned(),
                }),
                "torrent" => {
                    // try fetch metadata and compute total
                    let session = self.ensure_ready().await?;
                    let add = AddTorrent::from_url(url.to_owned());
                    let res = session
                        .add_torrent(add, None)
                        .await
                        .map_err(|e| NetworkError::Other(e.to_string()))?;
                    if let Some(handle) = res.into_handle() {
                        // metadata available; total bytes could be read from handle.stats() if needed
                        let _stats = handle.stats();
                    }
                    Ok(HeadInfo {
                        content_length: None,
                        accept_ranges: true,
                        final_url: url.to_owned(),
                    })
                }
                _ => Err(NetworkError::Other("unsupported scheme for head".into())),
            }
        }
    }
}

#[cfg(feature = "torrent")]
pub use real::TorrentNetworkClient;
