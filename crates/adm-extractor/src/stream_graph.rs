//! Domain types produced by every extractor.
//!
//! A `StreamGraph` is a **pure data** description of what the engine should
//! download.  It is serialisable so it can cross IPC boundaries and be
//! presented in the UI.

use serde::{Deserialize, Serialize};

// ── StreamKind ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    /// Adaptive bitrate video (HLS / DASH variant stream)
    Video,
    /// Audio-only track
    Audio,
    /// Subtitle / closed-caption track
    Subtitle,
    /// Unknown — extractor could not determine kind
    Unknown,
}

// ── MediaTrack ────────────────────────────────────────────────────────────────

/// A single rendition within a multi-track stream (e.g. an HLS
/// `#EXT-X-MEDIA` record for an audio track).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaTrack {
    pub kind: StreamKind,
    pub language: Option<String>,
    pub label: Option<String>,
    /// Absolute URL of the track's sub-playlist or media file.
    pub url: String,
    pub default_track: bool,
}

// ── SegmentInfo ───────────────────────────────────────────────────────────────

/// One media segment that must be downloaded and assembled in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentInfo {
    /// Absolute URL of the segment.
    pub url: String,
    /// Sequence number within the stream (0-based).
    pub sequence: u64,
    /// Duration in seconds as declared in the manifest.
    pub duration_secs: f64,
    /// Expected byte range within the segment file, if declared.
    pub byte_range: Option<(u64, u64)>,
    /// AES-128 / SAMPLE-AES encryption key URL, if the segment is encrypted.
    pub encryption_key_url: Option<String>,
    /// IV for AES decryption (hex string), if present.
    pub encryption_iv: Option<String>,
    /// Pre-fetched AES-128 key as a lowercase hex string (32 hex chars = 16 bytes).
    ///
    /// When set by the extractor, the engine worker skips the runtime key-fetch
    /// request and uses this value directly, eliminating one extra round-trip per
    /// encrypted segment and reducing key-server load.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption_key_hex: Option<String>,
    /// True when this segment immediately follows an `#EXT-X-DISCONTINUITY` tag.
    ///
    /// A discontinuity signals a sudden change in encoding parameters, byte-stream
    /// timeline, or both (e.g. ad-insertion splice points, live-to-VOD transitions).
    /// The post-processor / muxer **must** insert a presentation-time reset at this
    /// boundary to avoid audio/video sync drift.
    #[serde(default)]
    pub discontinuity: bool,
}

// ── StreamVariant ─────────────────────────────────────────────────────────────

/// One selectable quality / bitrate option offered by the source.
///
/// Each `StreamVariant` maps directly to a set of `DownloadTask`s in the
/// engine — one per segment (or one for a progressive download).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamVariant {
    pub kind: StreamKind,
    /// Human-readable label, e.g. `"1080p"`, `"360p"`, `"audio-en"`.
    pub label: String,
    /// Bandwidth in bits/s as declared in the manifest (0 = unknown).
    pub bandwidth_bps: u64,
    /// Pixel dimensions, if known.
    pub resolution: Option<(u32, u32)>,
    /// Codec strings (e.g. `["avc1.64001f", "mp4a.40.2"]`).
    pub codecs: Vec<String>,
    /// Playlist / manifest URL for this variant (absolute).
    pub playlist_url: String,
    /// All segments in playback order.  Empty for adaptive streams where
    /// a second extraction pass is needed.
    pub segments: Vec<SegmentInfo>,
    /// Associated audio/subtitle tracks that travel with this variant.
    pub associated_tracks: Vec<MediaTrack>,
    /// Whether this is the default / best variant recommended by the extractor.
    pub is_default: bool,
}

impl StreamVariant {
    /// Total declared duration of all segments (seconds).
    #[must_use]
    pub fn total_duration_secs(&self) -> f64 {
        self.segments.iter().map(|s| s.duration_secs).sum()
    }

    /// Number of segments in this variant.
    #[must_use]
    pub const fn segment_count(&self) -> usize {
        self.segments.len()
    }
}

// ── StreamGraph ───────────────────────────────────────────────────────────────

/// Everything an extractor learned about a URL — the complete set of
/// downloadable streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamGraph {
    /// Original URL that was extracted.
    pub source_url: String,
    /// Human-readable title if the extractor could determine one.
    pub title: Option<String>,
    /// All stream variants, sorted best-first by bandwidth.
    pub variants: Vec<StreamVariant>,
    /// Top-level media tracks not tied to a variant (e.g. external subtitles).
    pub standalone_tracks: Vec<MediaTrack>,
    /// Format name used to produce this graph (e.g. `"hls"`, `"dash"`).
    pub format: String,
    /// `true` for live / event streams that lack an `#EXT-X-ENDLIST` tag.
    ///
    /// When `true` the engine must poll the playlist URL for new segments at
    /// an interval of approximately `target_duration_secs` seconds until the
    /// playlist gains `#EXT-X-ENDLIST` or the user cancels.
    #[serde(default)]
    pub is_live: bool,
    /// Declared segment duration from `#EXT-X-TARGETDURATION` (seconds).
    ///
    /// Used as the base live-polling interval.  `None` if not specified (treat
    /// as 6 s, the HLS spec minimum for live streams).
    #[serde(default)]
    pub target_duration_secs: Option<u32>,
}

impl StreamGraph {
    pub fn new(source_url: impl Into<String>, format: impl Into<String>) -> Self {
        Self {
            source_url: source_url.into(),
            title: None,
            variants: Vec::new(),
            standalone_tracks: Vec::new(),
            format: format.into(),
            is_live: false,
            target_duration_secs: None,
        }
    }

    /// Return the recommended default variant (highest bandwidth marked default,
    /// or simply the first variant).
    #[must_use]
    pub fn default_variant(&self) -> Option<&StreamVariant> {
        self.variants
            .iter()
            .find(|v| v.is_default)
            .or_else(|| self.variants.first())
    }

    /// Return all video variants sorted by bandwidth descending.
    #[must_use]
    pub fn video_variants(&self) -> Vec<&StreamVariant> {
        let mut v: Vec<&StreamVariant> = self
            .variants
            .iter()
            .filter(|v| v.kind == StreamKind::Video)
            .collect();
        v.sort_unstable_by_key(|b| std::cmp::Reverse(b.bandwidth_bps));
        v
    }
}
