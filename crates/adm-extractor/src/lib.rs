//! `extractor` — media-stream extraction layer.
//!
//! ## Responsibilities
//! - Parse playlist / manifest formats (HLS, DASH, …) into a `StreamGraph`
//! - Produce `ExtractedStream` descriptors that the **engine** turns into
//!   `DownloadTask` instances
//! - Know **nothing** about downloading — the extractor never fetches content
//!   bytes; it only reads manifests/metadata over HTTP via the `NetworkClient`
//!   abstraction.
//!
//! ## Module layout
//! ```text
//! extractor/
//!   src/
//!     lib.rs          ← this file: public API, traits, domain types
//!     error.rs        ← ExtractorError
//!     stream_graph.rs ← StreamGraph + helpers
//!     extractors/
//!       mod.rs
//!       hls.rs        ← GenericHlsExtractor  (HLS / M3U8)
//!       dash.rs       ← GenericDashExtractor (DASH / MPD)  [skeleton]
//! ```

pub mod error;
pub mod extractors;
pub mod stream_graph;

pub use error::ExtractorError;
pub use extractors::dash::GenericDashExtractor;
pub use extractors::hls::GenericHlsExtractor;
pub use stream_graph::{MediaTrack, SegmentInfo, StreamGraph, StreamKind, StreamVariant};

use adm_network::NetworkClient;
use async_trait::async_trait;
use std::sync::Arc;

// ── Extractor trait ───────────────────────────────────────────────────────────

/// A `MediaExtractor` inspects a URL and, when it matches, returns a
/// `StreamGraph` describing every downloadable stream variant the URL exposes.
///
/// Extractors are **pure metadata readers**:
/// - They may perform HTTP requests to fetch manifests.
/// - They must **never** buffer media content bytes.
/// - They are stateless — all mutable state lives in the engine.
#[async_trait]
pub trait MediaExtractor: Send + Sync {
    /// Human-readable name ("`GenericHLS`", "`YouTube`", …).
    fn name(&self) -> &'static str;

    /// Return `true` if this extractor can handle `url`.
    /// This check must be **synchronous and cheap** (regex / string match only).
    fn can_handle(&self, url: &str) -> bool;

    /// Extract stream metadata from `url`.  May perform one or more HTTP GET
    /// requests via `client`.
    ///
    /// # Errors
    ///
    /// Returns an [`ExtractorError`] if metadata cannot be fetched or parsed.
    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError>;
}

// ── ExtractorRegistry ─────────────────────────────────────────────────────────

/// Ordered list of registered extractors.  The first extractor whose
/// `can_handle` returns `true` wins.
#[derive(Default)]
pub struct ExtractorRegistry {
    extractors: Vec<Arc<dyn MediaExtractor>>,
}

impl ExtractorRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append `extractor` to the registry (later = lower priority).
    pub fn register(&mut self, extractor: Arc<dyn MediaExtractor>) {
        self.extractors.push(extractor);
    }

    /// Find the first matching extractor and run it.
    ///
    /// # Errors
    ///
    /// Returns [`ExtractorError::Unsupported`] if no registered extractor
    /// accepts the URL, or returns the matching extractor's failure.
    pub async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        for extractor in &self.extractors {
            if extractor.can_handle(url) {
                tracing::debug!(extractor = extractor.name(), url, "running extractor");
                return extractor.extract(url, client).await;
            }
        }
        Err(ExtractorError::Unsupported(url.to_string()))
    }

    /// Return the number of extractors currently registered.
    #[must_use]
    pub fn extractor_count(&self) -> usize {
        self.extractors.len()
    }

    /// Return `true` if any registered extractor claims the URL (cheap check,
    /// no network I/O).
    #[must_use]
    pub fn can_handle(&self, url: &str) -> bool {
        self.extractors.iter().any(|e| e.can_handle(url))
    }

    /// Build the default registry with all built-in extractors registered in
    /// priority order.
    #[must_use]
    pub fn with_defaults() -> Self {
        let mut reg = Self::new();
        // Register custom extractors first so they take precedence over generic parsers
        reg.register(Arc::new(extractors::youtube::YouTubeExtractor::new()));
        reg.register(Arc::new(extractors::vimeo::VimeoExtractor::new()));
        reg.register(Arc::new(
            extractors::dailymotion::DailymotionExtractor::new(),
        ));
        reg.register(Arc::new(extractors::twitch::TwitchExtractor::new()));
        reg.register(Arc::new(extractors::twitter::TwitterExtractor::new()));
        reg.register(Arc::new(extractors::facebook::FacebookExtractor::new()));
        reg.register(Arc::new(extractors::instagram::InstagramExtractor::new()));
        reg.register(Arc::new(extractors::soundcloud::SoundCloudExtractor::new()));
        reg.register(Arc::new(GenericHlsExtractor::new()));
        reg.register(Arc::new(extractors::dash::GenericDashExtractor::new()));
        reg
    }
}
