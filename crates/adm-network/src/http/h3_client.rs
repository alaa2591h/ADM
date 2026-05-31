//! HTTP/3 (QUIC) transport implementation.
//!
//! Provides [`H3Transport`]: a reusable, connection-pooling HTTP/3 client built
//! directly on top of [`quinn`] (QUIC) and [`h3`] (HTTP/3 framing).
//!
//! # Architecture
//!
//! ```text
//! HttpNetworkClient
//!   ├── h1h2_client: reqwest::Client      ← always-available fallback
//!   └── h3_transport: Option<H3Transport>  ← H3 when available
//!         ├── quinn::Endpoint              ← one per process
//!         └── conn_pool: HashMap<origin>   ← reuse QUIC connections
//!               └── H3ConnectionHandle
//!                     ├── send_request: h3::client::SendRequest (cloneable)
//!                     └── driver_task: JoinHandle
//! ```
//!
//! # Connection lifecycle
//!
//! 1. First request to an origin → dial QUIC, start h3 driver task, cache handle.
//! 2. Subsequent requests → clone `SendRequest` from cached handle.
//! 3. When the QUIC connection closes (idle timeout / error) → remove from pool,
//!    next request re-dials transparently.
//! 4. On graceful shutdown → `H3Transport::shutdown()` closes the endpoint.
//!
//! # 0-RTT support
//!
//! When reconnecting to a previously-seen server, Quinn automatically attempts
//! QUIC 0-RTT, eliminating the round-trip handshake for subsequent connections.
//! This is enabled by setting `enable_early_data = true` on the TLS config.

use crate::{NetworkError, ResponseStream};
use bytes::Bytes;
use h3::client::SendRequest;
use h3_quinn::Connection as H3QuinnConn;
use http::{Method, Request};
use parking_lot::Mutex;
use quinn::{ClientConfig as QuinnClientConfig, Endpoint, TransportConfig, VarInt};
use rustls::{ClientConfig as RustlsClientConfig, RootCertStore};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

// ── Type aliases ─────────────────────────────────────────────────────────────

type H3SendRequest = SendRequest<h3_quinn::OpenStreams, Bytes>;

// ── H3ConnectionHandle ────────────────────────────────────────────────────────

/// A live H3 connection to a single origin.
struct H3ConnectionHandle {
    /// Cloneable request sender — used to open new streams on the QUIC connection.
    send_request: H3SendRequest,
    /// Drive the H3 session to completion (GO_AWAY, SETTINGS, etc.).
    /// Stored so we can abort it when we evict the connection from the pool.
    _driver: JoinHandle<()>,
}

// ── ConnectionPool ────────────────────────────────────────────────────────────

/// Per-origin connection pool.  `origin_key` = `"host:port"`.
#[derive(Default)]
struct ConnectionPool {
    map: HashMap<String, H3ConnectionHandle>,
}

impl ConnectionPool {
    fn get_sender(&self, key: &str) -> Option<H3SendRequest> {
        self.map.get(key).map(|h| h.send_request.clone())
    }

    fn insert(&mut self, key: String, handle: H3ConnectionHandle) {
        self.map.insert(key, handle);
    }

    fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }
}

// ── H3Transport ───────────────────────────────────────────────────────────────

/// Reusable HTTP/3 transport layer.  One instance is shared across the
/// [`super::HttpNetworkClient`] via `Arc`.
pub struct H3Transport {
    endpoint: Endpoint,
    pool: Arc<Mutex<ConnectionPool>>,
    connect_timeout: Duration,
    request_timeout: Option<Duration>,
    user_agent: String,
}

impl H3Transport {
    /// Build an `H3Transport` with Mozilla root certificates and sensible QUIC
    /// transport defaults.
    ///
    /// # Errors
    ///
    /// Returns `NetworkError::Tls` if the QUIC crypto configuration is invalid.
    pub fn build(
        user_agent: &str,
        connect_timeout: Duration,
        request_timeout: Option<Duration>,
    ) -> Result<Self, NetworkError> {
        // ── TLS root store ──────────────────────────────────────────────────
        let mut root_store = RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

        let mut tls_config = RustlsClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        // Enable TLS early data so Quinn can attempt 0-RTT on reconnect.
        tls_config.enable_early_data = true;

        // Advertise HTTP/3 as the ALPN protocol.
        tls_config.alpn_protocols = vec![b"h3".to_vec()];

        // ── QUIC transport config ───────────────────────────────────────────
        let quic_tls = quinn::crypto::rustls::QuicClientConfig::try_from(tls_config)
            .map_err(|e| NetworkError::Tls(format!("quic tls config: {e}")))?;

        let mut transport = TransportConfig::default();
        // 30-second idle timeout; server will keep QUIC alive with PING frames.
        transport.max_idle_timeout(Some(
            VarInt::from_u32(30_000)
                .try_into()
                .map_err(|e| NetworkError::Tls(format!("idle timeout: {e}")))?,
        ));
        // Send QUIC PING every 15 s to keep NAT mappings alive.
        transport.keep_alive_interval(Some(Duration::from_secs(15)));
        // Allow up to 16 concurrent bidirectional streams (one per chunk worker).
        transport.max_concurrent_bidi_streams(VarInt::from_u32(16));

        let mut client_cfg = QuinnClientConfig::new(Arc::new(quic_tls));
        client_cfg.transport_config(Arc::new(transport));

        // ── QUIC endpoint (bound to any local UDP port) ─────────────────────
        let mut endpoint = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))
            .map_err(|e| NetworkError::Io(format!("quic endpoint: {e}")))?;
        endpoint.set_default_client_config(client_cfg);

        Ok(Self {
            endpoint,
            pool: Arc::new(Mutex::new(ConnectionPool::default())),
            connect_timeout,
            request_timeout,
            user_agent: user_agent.to_owned(),
        })
    }

    /// Acquire a `SendRequest` for `origin_key`, either from the pool or by
    /// dialing a new QUIC connection.
    ///
    /// Returns `Err` if the connection cannot be established within
    /// `connect_timeout`.
    async fn acquire_sender(
        &self,
        host: &str,
        port: u16,
        origin_key: &str,
    ) -> Result<H3SendRequest, NetworkError> {
        // Fast path — reuse an existing connection.
        if let Some(sender) = self.pool.lock().get_sender(origin_key) {
            debug!(origin = %origin_key, "h3: reusing cached connection");
            return Ok(sender);
        }

        // Slow path — dial a new QUIC connection.
        let sender = self.dial(host, port, origin_key).await?;
        Ok(sender)
    }

    /// Dial a fresh QUIC connection to `host:port` and register it in the pool.
    async fn dial(
        &self,
        host: &str,
        port: u16,
        origin_key: &str,
    ) -> Result<H3SendRequest, NetworkError> {
        info!(origin = %origin_key, "h3: dialing new QUIC connection");

        // DNS resolution.
        let addr = tokio::net::lookup_host(format!("{host}:{port}"))
            .await
            .map_err(|e| NetworkError::Io(format!("dns: {e}")))?
            .next()
            .ok_or_else(|| NetworkError::Io(format!("dns: no address for {host}:{port}")))?;

        // QUIC connection with connect-timeout and optional 0-RTT.
        let quinn_conn = tokio::time::timeout(self.connect_timeout, async {
            let connecting = self
                .endpoint
                .connect(addr, host)
                .map_err(|e| NetworkError::Io(format!("quic connect: {e}")))?;

            // Attempt 0-RTT when resuming a known session.  On first connect
            // (no session ticket) `into_0rtt` returns Err and we fall back to
            // the regular 1-RTT handshake.
            match connecting.into_0rtt() {
                Ok((conn, zero_rtt_ok)) => {
                    let key = origin_key.to_owned();
                    tokio::spawn(async move {
                        match zero_rtt_ok.await {
                            true => debug!(origin = %key, "h3: 0-RTT accepted by server"),
                            false => {
                                warn!(origin = %key, "h3: 0-RTT rejected, data replayed on 1-RTT")
                            }
                        }
                    });
                    Ok(conn)
                }
                Err(connecting) => {
                    debug!(origin = %origin_key, "h3: no session ticket, using 1-RTT");
                    connecting
                        .await
                        .map_err(|e| NetworkError::Tls(format!("quic handshake: {e}")))
                }
            }
        })
        .await
        .map_err(|_| NetworkError::Timeout)??;

        // Wrap the QUIC connection in the h3-quinn adapter.
        let h3_conn = H3QuinnConn::new(quinn_conn);

        // Build the h3 session.  The driver must be polled continuously.
        let (mut driver, send_request) = h3::client::new(h3_conn)
            .await
            .map_err(|e| NetworkError::Other(format!("h3 handshake: {e}")))?;

        // Spawn the h3 driver.
        let key_for_driver = origin_key.to_owned();
        let pool_ref = self.pool.clone();
        let driver_handle = tokio::spawn(async move {
            let e = futures_util::future::poll_fn(|cx| driver.poll_close(cx)).await;
            warn!(origin = %key_for_driver, "h3 driver closed: {e}");
            // Evict from pool when the connection ends so subsequent requests
            // trigger a fresh dial.
            pool_ref.lock().remove(&key_for_driver);
        });

        let handle = H3ConnectionHandle {
            send_request: send_request.clone(),
            _driver: driver_handle,
        };
        self.pool.lock().insert(origin_key.to_owned(), handle);

        Ok(send_request)
    }

    /// Execute an HTTP/3 GET (or HEAD) request.
    ///
    /// Returns the response status, headers, and a boxed [`ResponseStream`].
    /// The stream lazily reads QUIC DATA frames on demand.
    pub async fn request(
        &self,
        url: &str,
        method: Method,
        range: Option<(u64, u64)>,
        extra_headers: &[(String, String)],
    ) -> Result<H3Response, NetworkError> {
        let uri: http::Uri = url
            .parse()
            .map_err(|e| NetworkError::Other(format!("invalid url: {e}")))?;

        let host = uri
            .host()
            .ok_or_else(|| NetworkError::Other("url missing host".into()))?;
        let port = uri.port_u16().unwrap_or(443);
        let origin_key = format!("{host}:{port}");

        let mut sender = self.acquire_sender(host, port, &origin_key).await?;

        // Build the request.
        let authority = uri
            .authority()
            .map(|a| a.as_str().to_owned())
            .unwrap_or_else(|| format!("{host}:{port}"));
        let path_and_query = uri
            .path_and_query()
            .map(|pq| pq.as_str().to_owned())
            .unwrap_or_else(|| "/".to_owned());

        let mut req_builder = Request::builder()
            .method(method)
            .uri(&path_and_query)
            .header(http::header::HOST, &authority)
            .header(http::header::USER_AGENT, &self.user_agent)
            .header(http::header::ACCEPT_ENCODING, "gzip, br, deflate");

        if let Some((start, end)) = range {
            req_builder = req_builder.header(http::header::RANGE, format!("bytes={start}-{end}"));
        }

        for (k, v) in extra_headers {
            req_builder = req_builder.header(k.as_str(), v.as_str());
        }

        let request = req_builder
            .body(())
            .map_err(|e| NetworkError::Other(format!("build request: {e}")))?;

        // Send request and receive response headers.
        let maybe_stream = tokio::time::timeout(
            self.connect_timeout, // use connect_timeout for initial stream open
            sender.send_request(request),
        )
        .await
        .map_err(|_| NetworkError::Timeout)?
        .map_err(|e| NetworkError::Other(format!("h3 send_request: {e}")))?;

        let mut request_stream = maybe_stream;

        // Signal end of request body (we have no request body for GET/HEAD).
        request_stream
            .finish()
            .await
            .map_err(|e| NetworkError::Other(format!("h3 finish: {e}")))?;

        // Receive response headers.
        let timeout = self.request_timeout.unwrap_or(Duration::from_secs(60));
        let response = tokio::time::timeout(timeout, request_stream.recv_response())
            .await
            .map_err(|_| NetworkError::Timeout)?
            .map_err(|e| NetworkError::Other(format!("h3 recv_response: {e}")))?;

        let status = response.status();
        let content_length = response
            .headers()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let alt_svc = response
            .headers()
            .get("alt-svc")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        Ok(H3Response {
            status,
            content_length,
            alt_svc,
            stream: Box::new(H3BodyStream {
                request_stream: Some(request_stream),
                buf: Vec::new(),
                content_length,
            }),
        })
    }

    /// Close the QUIC endpoint gracefully, waiting up to `timeout` for in-flight
    /// connections to finish.
    pub async fn shutdown(&self, timeout: Duration) {
        self.endpoint.close(VarInt::from_u32(0), b"shutdown");
        tokio::time::timeout(timeout, self.endpoint.wait_idle())
            .await
            .ok();
    }
}

// ── H3Response ────────────────────────────────────────────────────────────────

/// Parsed H3 response: status, headers, and body stream.
pub struct H3Response {
    pub status: http::StatusCode,
    pub content_length: Option<u64>,
    /// Raw `Alt-Svc` header value, if present.
    pub alt_svc: Option<String>,
    pub stream: Box<dyn ResponseStream + Send + Sync>,
}

// ── H3BodyStream ─────────────────────────────────────────────────────────────

/// Adapts an h3 `RequestStream` into the generic [`ResponseStream`] trait,
/// streaming QUIC DATA frames through the 256 KiB coalescing buffer that all
/// other backends use.
struct H3BodyStream {
    request_stream: Option<h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>>,
    buf: Vec<u8>,
    content_length: Option<u64>,
}

const CHUNK_SIZE: usize = 256 * 1024; // 256 KiB — matches http.rs buffer

#[async_trait::async_trait]
impl ResponseStream for H3BodyStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        loop {
            // Flush the internal buffer first.
            if self.buf.len() >= CHUNK_SIZE {
                let out = std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK_SIZE));
                return Ok(Some(out));
            }

            let stream = match self.request_stream.as_mut() {
                Some(s) => s,
                None => {
                    // Stream was cancelled; flush remaining.
                    if self.buf.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(std::mem::take(&mut self.buf)));
                }
            };

            match stream.recv_data().await {
                Ok(Some(mut chunk)) => {
                    use bytes::Buf;
                    while chunk.has_remaining() {
                        let n = chunk.remaining().min(CHUNK_SIZE - self.buf.len());
                        let slice = chunk.copy_to_bytes(n);
                        self.buf.extend_from_slice(&slice);

                        if self.buf.len() >= CHUNK_SIZE {
                            let out =
                                std::mem::replace(&mut self.buf, Vec::with_capacity(CHUNK_SIZE));
                            return Ok(Some(out));
                        }
                    }
                    // Keep reading for more data.
                }
                Ok(None) => {
                    // EOF — flush remaining buffer.
                    self.request_stream.take();
                    if self.buf.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(std::mem::take(&mut self.buf)));
                }
                Err(e) => {
                    self.request_stream.take();
                    return Err(NetworkError::Other(format!("h3 recv_data: {e}")));
                }
            }
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.content_length
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.buf.clear();
        // Dropping the request_stream resets the QUIC stream with STOP_SENDING.
        self.request_stream.take();
        Ok(())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify `H3Transport::build()` succeeds (TLS + QUIC endpoint setup).
    /// This is a smoke test that doesn't require a live server.
    #[test]
    fn build_succeeds_with_default_config() {
        let result = H3Transport::build(
            "APEX/1.0",
            Duration::from_secs(10),
            Some(Duration::from_secs(60)),
        );
        assert!(
            result.is_ok(),
            "H3Transport::build should succeed: {:?}",
            result.err()
        );
    }

    /// Verify that 0-RTT branch doesn't panic when there's no session ticket.
    /// (Falls back to 1-RTT handshake which times out on localhost:1.)
    #[tokio::test]
    async fn connect_to_invalid_addr_returns_error() {
        let transport = H3Transport::build(
            "APEX/1.0",
            Duration::from_millis(200), // very short timeout
            None,
        )
        .expect("build should succeed");

        let result = transport
            .request("https://127.0.0.1:1/", Method::GET, None, &[])
            .await;
        assert!(result.is_err(), "connecting to a closed port should fail");
    }

    /// Verify that `H3BodyStream` flushes the internal buffer correctly at EOF.
    #[tokio::test]
    async fn body_stream_small_payload_flushes_at_eof() {
        // We can't easily unit-test H3BodyStream without a live QUIC connection,
        // but we can verify the no-op state (None stream → returns Ok(None)).
        let mut stream = H3BodyStream {
            request_stream: None,
            buf: Vec::new(),
            content_length: None,
        };
        let result = stream.next_chunk().await;
        assert_eq!(result.unwrap(), None);
    }

    /// Verify that cancel() clears the buffer.
    #[tokio::test]
    async fn cancel_clears_buffer() {
        let mut stream = H3BodyStream {
            request_stream: None,
            buf: vec![0u8; 1024],
            content_length: Some(1024),
        };
        stream.cancel().await.unwrap();
        assert!(stream.buf.is_empty());
    }
}
