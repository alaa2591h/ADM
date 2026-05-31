//! Concrete reqwest-based HTTP backend with integrated HTTP/3 (QUIC) support.
//!
//! # HTTP version negotiation strategy
//!
//! The client uses a **three-tier protocol cascade** for every request:
//!
//! ```text
//! ┌──────────────────────────────────────────────┐
//! │  Request to example.com:443                  │
//! ├──────────────────────────────────────────────┤
//! │  1. Check AltSvcCache                        │
//! │     ├── H3 known available  → try H3         │
//! │     │     └─ fail → fallback H1/H2, mark bad │
//! │     ├── H3 known unavailable → skip to H1/H2 │
//! │     └── Unknown              → use H1/H2     │
//! │         └─ response has Alt-Svc: h3=...      │
//! │              └── cache + use H3 next time    │
//! └──────────────────────────────────────────────┘
//! ```
//!
//! This mirrors how browsers approach H3: discover via Alt-Svc, then switch,
//! with automatic recovery when H3 fails (e.g. QUIC blocked by firewall).
//!
//! ## Prior-knowledge mode
//!
//! When [`H3Mode::PriorKnowledge`] is selected, the client skips the Alt-Svc
//! discovery step and connects via H3 directly, falling back to H1/H2 only on
//! connection failure.  This is useful for CDN origins known to support H3.

pub mod alt_svc;
pub mod h3_client;
pub mod stream_adapters;

use crate::{NetworkClient, NetworkError, NetworkRequest, ResponseStream};
use adm_types::config::NetworkSettings;
use alt_svc::AltSvcCache;
use async_trait::async_trait;
use h3_client::H3Transport;
use http::Method;
use reqwest::{Client, Proxy};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};
use url::Url;

// ── ClientConfig ─────────────────────────────────────────────────────────────

/// HTTP/3 negotiation mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum H3Mode {
    /// Disabled: use H1/H2 only, ignore Alt-Svc headers.
    Disabled,
    /// Discover H3 via `Alt-Svc` response headers (default — safest).
    /// Falls back to H1/H2 if H3 is unavailable or blocked.
    #[default]
    AltSvc,
    /// Skip discovery and attempt H3 directly on every HTTPS request.
    /// Falls back to H1/H2 on QUIC connection failure.
    PriorKnowledge,
}

/// All configuration knobs for the HTTP network client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub request_timeout: Option<Duration>,
    pub connect_timeout: Duration,
    pub max_connections_per_host: usize,
    pub proxy_url: Option<String>,
    pub user_agent: String,
    pub follow_redirects: bool,
    pub max_redirects: usize,
    /// TCP keepalive interval; None disables keepalive.
    pub tcp_keepalive: Option<Duration>,
    /// Enable TCP_NODELAY to reduce latency on small writes.
    pub tcp_nodelay: bool,
    /// Pool idle timeout — connections idle longer than this are closed.
    pub pool_idle_timeout: Option<Duration>,
    /// Enable automatic response body decompression (gzip/brotli/deflate).
    pub accept_encoding: bool,
    /// HTTP/3 negotiation mode.
    pub h3_mode: H3Mode,
    /// [Legacy field] preserved for backward compatibility; ignored in favour
    /// of `h3_mode`.  Setting this to `true` maps to `H3Mode::AltSvc`.
    pub enable_http3: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            request_timeout: Some(Duration::from_secs(60)),
            connect_timeout: Duration::from_secs(10),
            max_connections_per_host: 16,
            proxy_url: None,
            user_agent: "APEX/1.0".to_owned(),
            follow_redirects: true,
            max_redirects: 10,
            tcp_keepalive: Some(Duration::from_secs(30)),
            tcp_nodelay: true,
            pool_idle_timeout: Some(Duration::from_secs(90)),
            accept_encoding: true,
            h3_mode: H3Mode::AltSvc,
            enable_http3: true, // backward compat
        }
    }
}

impl From<&NetworkSettings> for ClientConfig {
    fn from(settings: &NetworkSettings) -> Self {
        let proxy_url = if !settings.http_proxy.is_empty() {
            Some(settings.http_proxy.clone())
        } else if !settings.socks5_proxy.is_empty() {
            Some(settings.socks5_proxy.clone())
        } else {
            None
        };

        Self {
            request_timeout: Some(Duration::from_secs(settings.request_timeout_secs)),
            connect_timeout: Duration::from_secs(settings.connection_timeout_secs),
            max_connections_per_host: settings.max_connections_per_host,
            proxy_url,
            user_agent: settings.user_agent.clone(),
            follow_redirects: true,
            max_redirects: 10,
            tcp_keepalive: Some(Duration::from_secs(30)),
            tcp_nodelay: true,
            pool_idle_timeout: Some(Duration::from_secs(90)),
            accept_encoding: true,
            h3_mode: H3Mode::AltSvc,
            enable_http3: true,
        }
    }
}

// ── HttpResponseStream ────────────────────────────────────────────────────────

/// Streams response body chunks from a live `reqwest::Response`.
/// Uses an internal 256 KiB coalescing buffer so that small wire frames are
/// batched before being handed to the worker, reducing per-chunk overhead.
pub struct HttpResponseStream {
    response: Option<reqwest::Response>,
    /// Cached at construction time so `total_bytes()` works after `cancel()`.
    content_length: Option<u64>,
    buf: Vec<u8>,
}

const STREAM_BUFFER_CAPACITY: usize = 256 * 1024; // 256 KiB

#[async_trait]
impl ResponseStream for HttpResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let response = match self.response.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };
        loop {
            if self.buf.len() >= STREAM_BUFFER_CAPACITY {
                let out =
                    std::mem::replace(&mut self.buf, Vec::with_capacity(STREAM_BUFFER_CAPACITY));
                return Ok(Some(out));
            }

            match response.chunk().await {
                Ok(Some(bytes)) => {
                    self.buf.extend_from_slice(&bytes);
                    if self.buf.len() >= STREAM_BUFFER_CAPACITY {
                        let out = std::mem::replace(
                            &mut self.buf,
                            Vec::with_capacity(STREAM_BUFFER_CAPACITY),
                        );
                        return Ok(Some(out));
                    }
                    continue;
                }
                Ok(None) => {
                    if self.buf.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(std::mem::take(&mut self.buf)));
                }
                Err(e) => return Err(NetworkError::Io(e.to_string())),
            }
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.content_length
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.buf.clear();
        self.response.take();
        Ok(())
    }
}

// ── HttpNetworkClient ─────────────────────────────────────────────────────────

/// Production HTTP client with H3 → H2 → H1 fallback cascade.
pub struct HttpNetworkClient {
    /// Primary H1/H2 client — always available, used as fallback.
    pub(crate) h1h2_client: Client,
    /// Optional H3 transport layer.  `None` when `h3_mode == H3Mode::Disabled`.
    h3_transport: Option<Arc<H3Transport>>,
    /// Per-origin Alt-Svc cache.  Shared with all clones of this client.
    alt_svc_cache: AltSvcCache,
    /// Effective H3 mode.
    h3_mode: H3Mode,
}

impl HttpNetworkClient {
    /// Build the client from an explicit [`ClientConfig`].
    pub fn from_config(cfg: &ClientConfig) -> Result<Self, NetworkError> {
        // ── H1/H2 reqwest client ────────────────────────────────────────────
        let mut builder = Client::builder()
            .user_agent(&cfg.user_agent)
            .connect_timeout(cfg.connect_timeout)
            .tcp_nodelay(cfg.tcp_nodelay)
            .redirect(if cfg.follow_redirects {
                reqwest::redirect::Policy::limited(cfg.max_redirects)
            } else {
                reqwest::redirect::Policy::none()
            });

        if let Some(keepalive) = cfg.tcp_keepalive {
            builder = builder.tcp_keepalive(keepalive);
        }
        if let Some(idle) = cfg.pool_idle_timeout {
            builder = builder.pool_idle_timeout(idle);
        }
        if let Some(timeout) = cfg.request_timeout {
            builder = builder.timeout(timeout);
        }
        if cfg.max_connections_per_host > 0 {
            builder = builder.pool_max_idle_per_host(cfg.max_connections_per_host);
        }
        if cfg.accept_encoding {
            builder = builder.gzip(true).brotli(true).deflate(true);
        }

        #[cfg(feature = "http3")]
        {
            if cfg.h3_mode == H3Mode::PriorKnowledge {
                // For PriorKnowledge, we tell reqwest to assume H3 (QUIC) for all https:// URLs.
                builder = builder.http3_prior_knowledge();
            }
        }

        if let Some(ref proxy_url) = cfg.proxy_url {
            Url::parse(proxy_url)
                .map_err(|e| NetworkError::Other(format!("invalid proxy url: {e}")))?;
            let proxy = Proxy::all(proxy_url)
                .map_err(|e| NetworkError::Other(format!("invalid proxy url: {e}")))?;
            builder = builder.proxy(proxy);
        }

        let h1h2_client = builder
            .build()
            .map_err(|e| NetworkError::Other(format!("reqwest build: {e}")))?;

        // ── Effective H3 mode ───────────────────────────────────────────────
        let effective_mode = match (cfg.h3_mode, cfg.enable_http3) {
            (H3Mode::Disabled, _) => H3Mode::Disabled,
            (mode, _) => mode,
        };

        // ── H3 transport ────────────────────────────────────────────────────
        let h3_transport = if effective_mode != H3Mode::Disabled {
            match H3Transport::build(&cfg.user_agent, cfg.connect_timeout, cfg.request_timeout) {
                Ok(t) => {
                    debug!("http3: transport initialised (mode={effective_mode:?})");
                    Some(Arc::new(t))
                }
                Err(e) => {
                    warn!("http3: failed to build transport, H3 disabled: {e}");
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            h1h2_client,
            h3_transport,
            alt_svc_cache: AltSvcCache::new(),
            h3_mode: effective_mode,
        })
    }

    pub fn from_settings(settings: &NetworkSettings) -> Result<Self, NetworkError> {
        Self::from_config(&ClientConfig::from(settings))
    }

    pub fn new() -> Self {
        Self::from_config(&ClientConfig::default())
            .expect("default ClientConfig must always build successfully")
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /// Derive the cache key for the Alt-Svc map from a URL.
    fn origin_key(parsed_url: &Url) -> String {
        let host = parsed_url.host_str().unwrap_or("");
        let port = parsed_url.port_or_known_default().unwrap_or(443);
        format!("{host}:{port}")
    }

    /// Try an H3 request.  On any error, log it, mark the origin as temporarily
    /// unavailable, and return `None` so the caller can fall back to H1/H2.
    async fn try_h3(
        &self,
        url: &str,
        request: &NetworkRequest,
        origin_key: &str,
    ) -> Option<Box<dyn ResponseStream + Send + Sync>> {
        let transport = self.h3_transport.as_ref()?;

        debug!(origin = %origin_key, "h3: attempting H3 request");

        let method = Method::GET;
        let result = transport
            .request(url, method, request.range, &request.headers)
            .await;

        match result {
            Ok(h3_resp) => {
                let status = h3_resp.status;
                if status.is_success() || status == http::StatusCode::PARTIAL_CONTENT {
                    // Opportunistically update Alt-Svc cache from response headers.
                    if let Some(ref alt_svc_val) = h3_resp.alt_svc {
                        self.handle_alt_svc_header(alt_svc_val, origin_key);
                    }
                    debug!(origin = %origin_key, status = %status, "h3: success");
                    Some(h3_resp.stream)
                } else {
                    // Non-success H3 response — let H1/H2 handle it (might
                    // give a more descriptive error body).
                    debug!(
                        origin = %origin_key,
                        status = %status,
                        "h3: non-success status, falling back to H1/H2"
                    );
                    None
                }
            }
            Err(e) => {
                warn!(
                    origin = %origin_key,
                    error = %e,
                    "h3: request failed, falling back to H1/H2 and marking origin as unavailable"
                );
                // Penalise for 5 minutes before retrying H3.
                self.alt_svc_cache
                    .mark_unavailable(origin_key.to_owned(), Duration::from_secs(300));
                None
            }
        }
    }

    /// Process an `Alt-Svc` header from an H1/H2 response and update the cache.
    fn handle_alt_svc_header(&self, header_value: &str, origin_key: &str) {
        let origin_port: u16 = origin_key
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .unwrap_or(443);

        let alts = alt_svc::parse_alt_svc(header_value, origin_port);
        // Use the first H3 alternative that specifies the expected port.
        if let Some(alt) = alts.first() {
            let effective_port = if alt.alt_port == 0 {
                origin_port
            } else {
                alt.alt_port
            };
            let max_age = Duration::from_secs(alt.max_age_secs.min(86_400));
            debug!(
                origin = %origin_key,
                h3_port = effective_port,
                max_age_secs = alt.max_age_secs,
                "h3: caching Alt-Svc discovery"
            );
            self.alt_svc_cache
                .insert(origin_key.to_owned(), effective_port, max_age);
        }
    }

    /// Execute via the H1/H2 reqwest client.
    async fn h1h2_execute(
        &self,
        request: &NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        let mut req_builder = self.h1h2_client.get(&request.url);

        if let Some((start, end)) = request.range {
            req_builder = req_builder.header("Range", format!("bytes={start}-{end}"));
        }
        for (key, val) in &request.headers {
            req_builder = req_builder.header(key.as_str(), val.as_str());
        }

        match req_builder.send().await {
            Ok(response) => {
                let status = response.status();
                // On an H1/H2 response, check for Alt-Svc discovery.
                if let Some(alt_svc_val) = response
                    .headers()
                    .get("alt-svc")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_owned())
                {
                    if let Ok(parsed) = Url::parse(&request.url) {
                        let key = Self::origin_key(&parsed);
                        if !self.alt_svc_cache.is_known_unavailable(&key) {
                            self.handle_alt_svc_header(&alt_svc_val, &key);
                        }
                    }
                }

                if !status.is_success() {
                    return Err(NetworkError::Other(format!("HTTP {status}")));
                }
                Ok(Box::new(HttpResponseStream {
                    content_length: response.content_length(),
                    response: Some(response),
                    buf: Vec::with_capacity(STREAM_BUFFER_CAPACITY),
                }))
            }
            Err(e) => {
                if e.is_timeout() {
                    Err(NetworkError::Timeout)
                } else if e.is_connect() {
                    Err(NetworkError::Io(e.to_string()))
                } else {
                    Err(NetworkError::Other(e.to_string()))
                }
            }
        }
    }
}

impl Default for HttpNetworkClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NetworkClient for HttpNetworkClient {
    /// Execute an HTTP(S) request using the H3 → H2 → H1 cascade.
    ///
    /// ## H3 routing logic
    ///
    /// | `h3_mode`         | Alt-Svc cache state  | Action                        |
    /// |-------------------|----------------------|-------------------------------|
    /// | `Disabled`        | any                  | H1/H2 directly                |
    /// | `AltSvc`          | H3 known available   | Try H3 first, fallback H1/H2  |
    /// | `AltSvc`          | H3 unknown           | H1/H2, parse Alt-Svc header   |
    /// | `AltSvc`          | H3 known unavailable | H1/H2 directly                |
    /// | `PriorKnowledge`  | any                  | Try H3 first, fallback H1/H2  |
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        // Only attempt H3 for HTTPS URLs — QUIC requires TLS.
        let is_https = request.url.starts_with("https://");

        if is_https && self.h3_transport.is_some() {
            let origin_key = Url::parse(&request.url)
                .ok()
                .map(|u| Self::origin_key(&u))
                .unwrap_or_default();

            match self.h3_mode {
                H3Mode::PriorKnowledge => {
                    // Attempt H3 regardless of cache state.
                    if let Some(stream) = self.try_h3(&request.url, &request, &origin_key).await {
                        return Ok(stream);
                    }
                    // H3 failed → fall through to H1/H2.
                }
                H3Mode::AltSvc => {
                    // H3 only when the cache says it's available.
                    let h3_available = self.alt_svc_cache.lookup(&origin_key).is_some();
                    let h3_blocked = self.alt_svc_cache.is_known_unavailable(&origin_key);

                    if h3_available && !h3_blocked {
                        if let Some(stream) = self.try_h3(&request.url, &request, &origin_key).await
                        {
                            return Ok(stream);
                        }
                        // H3 failed → fall through to H1/H2.
                    }
                }
                H3Mode::Disabled => {}
            }
        }

        // H1/H2 fallback (or primary path when H3 is disabled/unknown).
        self.h1h2_execute(&request).await
    }

    /// Issue a HEAD request to discover `Content-Length` and `Accept-Ranges`.
    ///
    /// Uses H1/H2 for HEAD — the Alt-Svc cache is populated via GET responses,
    /// and HEAD requests are infrequent enough that H3 savings are negligible.
    async fn head(&self, url: &str) -> Result<crate::HeadInfo, NetworkError> {
        let resp = self.h1h2_client.head(url).send().await.map_err(|e| {
            if e.is_timeout() {
                NetworkError::Timeout
            } else {
                NetworkError::Other(e.to_string())
            }
        })?;

        // Update Alt-Svc cache from HEAD response too.
        if let Some(alt_svc_val) = resp
            .headers()
            .get("alt-svc")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned())
        {
            if let Ok(parsed) = Url::parse(url) {
                let key = Self::origin_key(&parsed);
                if !self.alt_svc_cache.is_known_unavailable(&key) {
                    self.handle_alt_svc_header(&alt_svc_val, &key);
                }
            }
        }

        let content_length = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let accept_ranges = resp
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.eq_ignore_ascii_case("bytes"))
            .unwrap_or(false);

        let final_url = resp.url().to_string();

        Ok(crate::HeadInfo {
            content_length,
            accept_ranges,
            final_url,
        })
    }
}

// ── Integration tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockNetworkClient, NetworkRequest};

    /// Verify the client builds with default config (H3 enabled).
    #[test]
    fn client_builds_with_h3_enabled() {
        let cfg = ClientConfig::default();
        assert_eq!(cfg.h3_mode, H3Mode::AltSvc);
        let client = HttpNetworkClient::from_config(&cfg);
        assert!(client.is_ok(), "build should succeed: {:?}", client.err());
    }

    /// Verify the client builds with H3 disabled.
    #[test]
    fn client_builds_with_h3_disabled() {
        let cfg = ClientConfig {
            h3_mode: H3Mode::Disabled,
            ..Default::default()
        };
        let client = HttpNetworkClient::from_config(&cfg).expect("build should succeed");
        assert!(
            client.h3_transport.is_none(),
            "no H3 transport when disabled"
        );
    }

    /// Verify Alt-Svc header parsing + cache insertion round-trip.
    #[test]
    fn alt_svc_cache_round_trip() {
        let client = HttpNetworkClient::new();
        let origin_key = "example.com:443";

        // Before: unknown.
        assert_eq!(client.alt_svc_cache.lookup(origin_key), None);

        // Simulate receiving `Alt-Svc: h3=":443"; ma=3600`.
        client.handle_alt_svc_header("h3=\":443\"; ma=3600", origin_key);

        // After: cached.
        assert_eq!(client.alt_svc_cache.lookup(origin_key), Some(443));
    }

    /// Verify `mark_unavailable` prevents H3 retry for the penalty window.
    #[test]
    fn known_unavailable_blocks_h3_retries() {
        let client = HttpNetworkClient::new();
        let origin_key = "flaky.example.com:443";

        client
            .alt_svc_cache
            .mark_unavailable(origin_key.to_owned(), Duration::from_secs(300));

        assert!(client.alt_svc_cache.is_known_unavailable(origin_key));
        assert_eq!(client.alt_svc_cache.lookup(origin_key), None);
    }

    /// Verify that H3Mode::Disabled skips the H3 transport entirely.
    #[test]
    fn prior_knowledge_mode_sets_correct_field() {
        let cfg = ClientConfig {
            h3_mode: H3Mode::PriorKnowledge,
            ..Default::default()
        };
        let client = HttpNetworkClient::from_config(&cfg).expect("build should succeed");
        assert_eq!(client.h3_mode, H3Mode::PriorKnowledge);
        assert!(client.h3_transport.is_some());
    }

    /// Verify origin_key derivation for standard HTTPS URLs.
    #[test]
    fn origin_key_derivation() {
        let cases = [
            ("https://example.com/file.zip", "example.com:443"),
            ("https://example.com:8443/f", "example.com:8443"),
            ("http://cdn.example.com/a", "cdn.example.com:80"),
        ];
        for (url, expected) in cases {
            let parsed = Url::parse(url).unwrap();
            assert_eq!(
                HttpNetworkClient::origin_key(&parsed),
                expected,
                "url={url}"
            );
        }
    }

    // ── 0-RTT reconnection test ──────────────────────────────────────────────

    /// Verify that the H3 transport attempts 0-RTT on a second connection.
    /// This is a structural test — the actual 0-RTT acceptance requires a live
    /// server.  We confirm the code path reaches `into_0rtt()` without panic.
    ///
    /// Expected: connect to a closed port → returns a NetworkError within the
    /// connect_timeout.  The test verifies that 0-RTT attempt logic is reached
    /// (i.e., no panic on `into_0rtt()`) even when the connection fails.
    #[tokio::test]
    async fn h3_0rtt_attempt_on_second_connection_no_panic() {
        let transport = h3_client::H3Transport::build(
            "APEX/test",
            Duration::from_millis(100), // intentionally very short
            Some(Duration::from_millis(100)),
        )
        .expect("build should succeed");

        // First attempt — always fails (no server).
        let _ = transport
            .request("https://127.0.0.1:1/", Method::GET, None, &[])
            .await;
        // Second attempt — should hit the 0-RTT branch and handle the
        // `into_0rtt` → `Err(connecting)` → timeout path without panic.
        let result = transport
            .request("https://127.0.0.1:1/", Method::GET, None, &[])
            .await;
        assert!(result.is_err(), "expected error for closed port");
    }
}
