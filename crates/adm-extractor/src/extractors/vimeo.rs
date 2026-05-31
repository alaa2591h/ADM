//! `VimeoExtractor` — stream extraction for vimeo.com
//!
//! ## Strategy
//! 1. Parse `video_id` (and optional `hash` for unlisted/private videos)
//!    directly from the URL — no round-trip needed.
//! 2. Fetch `https://player.vimeo.com/video/{id}/config[?h={hash}]`.
//!    This is the same JSON the Vimeo web player loads; it returns every
//!    available rendition without authentication for public videos, and
//!    supports unlisted videos when the hash is present.
//! 3. Parse **progressive** streams (`request.files.progressive[]`) — direct
//!    MP4 files, one per quality level, immediately downloadable.
//! 4. Parse **HLS** master playlist (`request.files.hls.cdns`) — returned as
//!    an M3U8 URL that the `GenericHlsExtractor` can expand into per-segment
//!    `SegmentInfo`s in a second pass.
//! 5. Fall back to OEmbed metadata only when the config endpoint fails, so
//!    the extractor always returns *something* useful.
//!
//! ## Supported URL forms
//! - `https://vimeo.com/123456789`
//! - `https://vimeo.com/channels/staffpicks/123456789`
//! - `https://vimeo.com/groups/name/videos/123456789`
//! - `https://vimeo.com/123456789/abcdef012345`        (unlisted — hash in path)
//! - `https://player.vimeo.com/video/123456789?h=…`    (embed player)
//! - `https://vimeo.com/album/123/video/456789`
//!
//! ## Not supported (yet)
//! - Videos behind a password paywall (requires `POST /password`)
//! - DRM-protected content (Widevine / PlayReady)

use crate::{
    stream_graph::{MediaTrack, StreamGraph, StreamKind, StreamVariant},
    ExtractorError, MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Browser-like User-Agent.  Vimeo rejects requests with suspicious UA strings.
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// CDN preference order for the HLS stream.  We try the fastest CDN first.
const HLS_CDN_PREFERENCE: &[&str] = &[
    "akfire_interconnect_quic",
    "fastly_skyfire",
    "fastly",
    "akamai",
];

// ── VimeoVideoRef ─────────────────────────────────────────────────────────────

/// Parsed reference extracted from a Vimeo URL.
#[derive(Debug, Clone)]
struct VimeoVideoRef {
    video_id: String,
    /// Unlisted / private hash (the hex string after the numeric ID).
    hash: Option<String>,
}

impl VimeoVideoRef {
    /// Build the player-config URL for this reference.
    fn player_config_url(&self) -> String {
        match &self.hash {
            Some(h) => format!(
                "https://player.vimeo.com/video/{}/config?h={}",
                self.video_id, h
            ),
            None => format!("https://player.vimeo.com/video/{}/config", self.video_id),
        }
    }
}

// ── URL parsing ───────────────────────────────────────────────────────────────

/// Extract video ID and optional hash from a Vimeo URL.
///
/// Handles all documented URL patterns; returns `None` for non-Vimeo or
/// unrecognisable paths.
fn parse_vimeo_url(url: &str) -> Option<VimeoVideoRef> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;

    if !host.ends_with("vimeo.com") {
        return None;
    }

    // player.vimeo.com/video/{id}[?h={hash}]
    if host == "player.vimeo.com" {
        let path = parsed.path();
        let video_id = path
            .trim_start_matches('/')
            .strip_prefix("video/")?
            .split('/')
            .find(|s| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))?
            .to_string();
        let hash = parsed
            .query_pairs()
            .find(|(k, _)| k == "h")
            .map(|(_, v)| v.into_owned());
        return Some(VimeoVideoRef { video_id, hash });
    }

    // Regular vimeo.com paths — scan segments for numeric ID and optional hash.
    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();

    let mut video_id: Option<String> = None;
    let mut hash: Option<String> = None;

    for (i, seg) in segments.iter().enumerate() {
        if seg.chars().all(|c| c.is_ascii_digit()) && seg.len() > 4 {
            // Looks like a numeric video ID
            video_id = Some((*seg).to_string());
            // The segment immediately after a numeric ID (if it looks like a
            // hex hash, not "videos" / "likes" / …) is the unlisted hash.
            if let Some(next) = segments.get(i + 1) {
                if is_vimeo_hash(next) {
                    hash = Some((*next).to_string());
                }
            }
            break;
        }
    }

    video_id.map(|id| VimeoVideoRef { video_id: id, hash })
}

/// Heuristic: Vimeo unlisted hashes are 8–32 hex characters.
fn is_vimeo_hash(s: &str) -> bool {
    (8..=32).contains(&s.len()) && s.chars().all(|c| c.is_ascii_hexdigit())
}

// ── HTTP helper ───────────────────────────────────────────────────────────────

/// Fetch a URL and return the response body as a `String`.
async fn fetch_text(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    referer: Option<&str>,
) -> Result<String, ExtractorError> {
    let mut req = NetworkRequest::new(url, None);
    req.headers
        .push(("User-Agent".to_string(), USER_AGENT.to_string()));
    req.headers
        .push(("Accept".to_string(), "application/json, */*".to_string()));
    if let Some(ref_url) = referer {
        req.headers
            .push(("Referer".to_string(), ref_url.to_string()));
    }

    let mut stream = client
        .execute(req)
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?;

    let mut buf = Vec::new();
    while let Some(chunk) = stream
        .next_chunk()
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?
    {
        buf.extend_from_slice(&chunk);
    }

    String::from_utf8(buf).map_err(|e| ExtractorError::Parse {
        format: "vimeo",
        reason: format!("utf-8 decode error: {e}"),
    })
}

// ── Progressive stream parsing ────────────────────────────────────────────────

/// Parse a single progressive MP4 entry from `request.files.progressive[]`.
///
/// Returns `None` if the entry lacks a download URL.
fn parse_progressive(entry: &Value) -> Option<StreamVariant> {
    let url = entry.get("url").and_then(Value::as_str)?;
    if url.is_empty() {
        return None;
    }

    let quality = entry
        .get("quality")
        .and_then(Value::as_str)
        .unwrap_or("unknown");

    let width = entry.get("width").and_then(Value::as_u64).map(|v| v as u32);
    let height = entry
        .get("height")
        .and_then(Value::as_u64)
        .map(|v| v as u32);
    let resolution = width.zip(height);

    // bitrate field is in bits/s
    let bandwidth_bps = entry.get("bitrate").and_then(Value::as_u64).unwrap_or(0);

    let codec = entry.get("codec").and_then(Value::as_str).unwrap_or("H264");
    let mime = entry
        .get("mime")
        .and_then(Value::as_str)
        .unwrap_or("video/mp4");

    let codecs = if codec.eq_ignore_ascii_case("H264") {
        vec!["avc1".to_string()]
    } else if codec.eq_ignore_ascii_case("H265") || codec.eq_ignore_ascii_case("HEVC") {
        vec!["hvc1".to_string()]
    } else {
        vec![codec.to_ascii_lowercase()]
    };

    let label = match (quality, resolution) {
        (q, Some((w, h))) => format!("{q} ({w}×{h}) [mp4]"),
        (q, None) => format!("{q} [mp4]"),
    };

    let _ = mime; // retained for future Content-Type validation

    Some(StreamVariant {
        kind: StreamKind::Video,
        label,
        bandwidth_bps,
        resolution,
        codecs,
        playlist_url: url.to_string(),
        segments: vec![],
        associated_tracks: vec![],
        is_default: false,
    })
}

// ── HLS / DASH stream parsing ─────────────────────────────────────────────────

/// Resolve the best HLS CDN URL from `request.files.hls`.
///
/// Tries `HLS_CDN_PREFERENCE` first, then falls back to `default_cdn`,
/// then to the first available CDN.
fn best_hls_url(hls: &Value) -> Option<String> {
    let cdns = hls.get("cdns").and_then(Value::as_object)?;
    let default_cdn = hls.get("default_cdn").and_then(Value::as_str).unwrap_or("");

    // 1. Try preferred CDNs in order
    for preferred in HLS_CDN_PREFERENCE {
        if let Some(url) = cdns
            .get(*preferred)
            .and_then(|c| c.get("url"))
            .and_then(Value::as_str)
        {
            if !url.is_empty() {
                debug!(cdn = *preferred, "selected HLS CDN");
                return Some(url.to_string());
            }
        }
    }

    // 2. Fall back to declared default CDN
    if !default_cdn.is_empty() {
        if let Some(url) = cdns
            .get(default_cdn)
            .and_then(|c| c.get("url"))
            .and_then(Value::as_str)
        {
            if !url.is_empty() {
                debug!(cdn = default_cdn, "falling back to default CDN");
                return Some(url.to_string());
            }
        }
    }

    // 3. Take first available CDN
    cdns.values()
        .find_map(|c| c.get("url").and_then(Value::as_str))
        .filter(|u| !u.is_empty())
        .map(str::to_string)
}

/// Build a `StreamVariant` representing the HLS adaptive stream.
fn parse_hls_stream(hls: &Value, separate_av: bool) -> Option<StreamVariant> {
    let url = best_hls_url(hls)?;

    let label = if separate_av {
        "HLS (adaptive, separate A/V)".to_string()
    } else {
        "HLS (adaptive)".to_string()
    };

    Some(StreamVariant {
        kind: StreamKind::Video,
        label,
        bandwidth_bps: 0, // bandwidth is per-rendition inside the M3U8
        resolution: None,
        codecs: vec![],
        playlist_url: url,
        segments: vec![],
        associated_tracks: vec![],
        is_default: false,
    })
}

/// Build a `StreamVariant` representing the DASH adaptive stream.
fn parse_dash_stream(dash: &Value) -> Option<StreamVariant> {
    let cdns = dash.get("cdns").and_then(Value::as_object)?;
    let default_cdn = dash
        .get("default_cdn")
        .and_then(Value::as_str)
        .unwrap_or("");

    let url = cdns
        .get(default_cdn)
        .and_then(|c| c.get("url"))
        .and_then(Value::as_str)
        .or_else(|| {
            cdns.values()
                .find_map(|c| c.get("url").and_then(Value::as_str))
        })?;

    if url.is_empty() {
        return None;
    }

    Some(StreamVariant {
        kind: StreamKind::Video,
        label: "DASH (adaptive)".to_string(),
        bandwidth_bps: 0,
        resolution: None,
        codecs: vec![],
        playlist_url: url.to_string(),
        segments: vec![],
        associated_tracks: vec![],
        is_default: false,
    })
}

// ── Thumbnail helper ──────────────────────────────────────────────────────────

/// Extract the best available thumbnail URL from `video.thumbs`.
fn best_thumbnail(thumbs: &Value) -> Option<String> {
    let obj = thumbs.as_object()?;
    // Prefer 1280 → 960 → 640 → base
    for key in &["1280", "960", "640", "base"] {
        if let Some(url) = obj.get(*key).and_then(Value::as_str) {
            if !url.is_empty() {
                return Some(url.to_string());
            }
        }
    }
    obj.values()
        .find_map(Value::as_str)
        .filter(|u| !u.is_empty())
        .map(str::to_string)
}

// ── VimeoExtractor ────────────────────────────────────────────────────────────

pub struct VimeoExtractor;

impl Default for VimeoExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl VimeoExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    // ── player/config fetch ──────────────────────────────────────────────────

    async fn fetch_player_config(
        &self,
        vref: &VimeoVideoRef,
        source_url: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<Value, ExtractorError> {
        let config_url = vref.player_config_url();
        debug!(
            video_id = %vref.video_id,
            config_url = %config_url,
            "fetching Vimeo player config"
        );

        let text = fetch_text(&config_url, client, Some(source_url)).await?;

        serde_json::from_str(&text).map_err(|e| ExtractorError::Parse {
            format: "vimeo",
            reason: format!("player config JSON error: {e}"),
        })
    }

    // ── OEmbed fallback ───────────────────────────────────────────────────────

    async fn fetch_oembed_title(
        &self,
        url: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Option<String> {
        let oembed_url = format!(
            "https://vimeo.com/api/oembed.json?url={}",
            url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>()
        );
        match fetch_text(&oembed_url, client, None).await {
            Ok(text) => serde_json::from_str::<Value>(&text)
                .ok()
                .and_then(|v| v.get("title").and_then(Value::as_str).map(str::to_string)),
            Err(e) => {
                warn!("OEmbed fallback failed: {e}");
                None
            }
        }
    }

    // ── StreamGraph assembly ─────────────────────────────────────────────────

    fn build_stream_graph(
        &self,
        source_url: &str,
        config: &Value,
    ) -> Result<StreamGraph, ExtractorError> {
        // ── metadata ──────────────────────────────────────────────────────────
        let video_node = config.get("video");
        let title = video_node
            .and_then(|v| v.get("title"))
            .and_then(Value::as_str)
            .map(str::to_string);

        let thumbnail_url = video_node
            .and_then(|v| v.get("thumbs"))
            .and_then(|t| best_thumbnail(t));

        // ── files node ───────────────────────────────────────────────────────
        let files = config
            .get("request")
            .and_then(|r| r.get("files"))
            .ok_or_else(|| ExtractorError::Parse {
                format: "vimeo",
                reason: "request.files missing from player config".to_string(),
            })?;

        let mut variants: Vec<StreamVariant> = Vec::new();

        // ── progressive streams ───────────────────────────────────────────────
        if let Some(progressive) = files.get("progressive").and_then(Value::as_array) {
            let before = variants.len();
            for entry in progressive {
                if let Some(v) = parse_progressive(entry) {
                    variants.push(v);
                }
            }
            debug!(
                count = variants.len() - before,
                "parsed progressive (MP4) variants"
            );
        }

        // ── HLS adaptive stream ───────────────────────────────────────────────
        if let Some(hls) = files.get("hls") {
            let separate_av = hls
                .get("separate_av")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if let Some(hls_variant) = parse_hls_stream(hls, separate_av) {
                debug!(url = %hls_variant.playlist_url, "parsed HLS adaptive stream");
                variants.push(hls_variant);
            }
        }

        // ── DASH adaptive stream ──────────────────────────────────────────────
        if let Some(dash) = files.get("dash") {
            if let Some(dash_variant) = parse_dash_stream(dash) {
                debug!(url = %dash_variant.playlist_url, "parsed DASH adaptive stream");
                variants.push(dash_variant);
            }
        }

        if variants.is_empty() {
            return Err(ExtractorError::NoStreams);
        }

        // ── sort + mark default ───────────────────────────────────────────────
        // Sort progressive variants by bandwidth descending; leave adaptive
        // (HLS / DASH) streams at the end.
        variants.sort_unstable_by(|a, b| {
            let a_adaptive = a.bandwidth_bps == 0;
            let b_adaptive = b.bandwidth_bps == 0;
            match (a_adaptive, b_adaptive) {
                (false, true) => std::cmp::Ordering::Less,
                (true, false) => std::cmp::Ordering::Greater,
                _ => b.bandwidth_bps.cmp(&a.bandwidth_bps),
            }
        });

        // The highest-bitrate progressive MP4 is the default pick.
        if let Some(first) = variants.first_mut() {
            first.is_default = true;
        }

        // ── standalone thumbnail track ────────────────────────────────────────
        let mut standalone_tracks: Vec<MediaTrack> = Vec::new();
        if let Some(thumb) = thumbnail_url {
            standalone_tracks.push(MediaTrack {
                kind: StreamKind::Unknown,
                language: None,
                label: Some("thumbnail".to_string()),
                url: thumb,
                default_track: false,
            });
        }

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: source_url.to_string(),
            title,
            variants,
            standalone_tracks,
            format: "vimeo".to_string(),
        })
    }
}

// ── MediaExtractor impl ───────────────────────────────────────────────────────

#[async_trait]
impl MediaExtractor for VimeoExtractor {
    fn name(&self) -> &'static str {
        "Vimeo"
    }

    fn can_handle(&self, url: &str) -> bool {
        url.contains("vimeo.com/")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        // 1. Parse video reference from URL
        let vref =
            parse_vimeo_url(url).ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;
        debug!(
            video_id = %vref.video_id,
            hash = ?vref.hash,
            "extracted Vimeo video reference"
        );

        // 2. Fetch player config (primary path)
        let config = match self.fetch_player_config(&vref, url, &client).await {
            Ok(c) => c,
            Err(e) => {
                // Config endpoint failed — try OEmbed for at least a title,
                // then propagate the original error (no streams).
                warn!("player config fetch failed ({e}), attempting OEmbed fallback");
                let title = self.fetch_oembed_title(url, &client).await;
                return if let Some(t) = title {
                    // Partial result — one stub variant so the engine has
                    // something to show the user (no usable URL though).
                    Err(ExtractorError::Parse {
                        format: "vimeo",
                        reason: format!("player config unavailable for \"{t}\": {e}"),
                    })
                } else {
                    Err(e)
                };
            }
        };

        // 3. Build graph from config; if that fails, try OEmbed title then bail.
        match self.build_stream_graph(url, &config) {
            Ok(mut graph) => {
                // Backfill title from OEmbed if the config didn't include one.
                if graph.title.is_none() {
                    graph.title = self.fetch_oembed_title(url, &client).await;
                }
                debug!(
                    title = ?graph.title,
                    variants = graph.variants.len(),
                    "Vimeo extraction complete"
                );
                Ok(graph)
            }
            Err(e) => Err(e),
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── URL parsing ──────────────────────────────────────────────────────────

    #[test]
    fn parses_standard_vimeo_url() {
        let vref = parse_vimeo_url("https://vimeo.com/123456789").unwrap();
        assert_eq!(vref.video_id, "123456789");
        assert!(vref.hash.is_none());
    }

    #[test]
    fn parses_unlisted_url_with_hash() {
        let vref = parse_vimeo_url("https://vimeo.com/123456789/abcdef012345").unwrap();
        assert_eq!(vref.video_id, "123456789");
        assert_eq!(vref.hash.as_deref(), Some("abcdef012345"));
    }

    #[test]
    fn parses_channel_url() {
        let vref = parse_vimeo_url("https://vimeo.com/channels/staffpicks/987654321").unwrap();
        assert_eq!(vref.video_id, "987654321");
        assert!(vref.hash.is_none());
    }

    #[test]
    fn parses_groups_url() {
        let vref = parse_vimeo_url("https://vimeo.com/groups/shortfilms/videos/111222333").unwrap();
        assert_eq!(vref.video_id, "111222333");
        assert!(vref.hash.is_none());
    }

    #[test]
    fn parses_player_embed_url_with_hash() {
        let vref =
            parse_vimeo_url("https://player.vimeo.com/video/555666777?h=deadbeef1234").unwrap();
        assert_eq!(vref.video_id, "555666777");
        assert_eq!(vref.hash.as_deref(), Some("deadbeef1234"));
    }

    #[test]
    fn parses_album_url() {
        let vref = parse_vimeo_url("https://vimeo.com/album/1234/video/444555666").unwrap();
        assert_eq!(vref.video_id, "444555666");
        assert!(vref.hash.is_none());
    }

    #[test]
    fn rejects_non_vimeo_url() {
        assert!(parse_vimeo_url("https://youtube.com/watch?v=abc").is_none());
    }

    #[test]
    fn config_url_no_hash() {
        let vref = VimeoVideoRef {
            video_id: "123".to_string(),
            hash: None,
        };
        assert_eq!(
            vref.player_config_url(),
            "https://player.vimeo.com/video/123/config"
        );
    }

    #[test]
    fn config_url_with_hash() {
        let vref = VimeoVideoRef {
            video_id: "123".to_string(),
            hash: Some("abc123".to_string()),
        };
        assert_eq!(
            vref.player_config_url(),
            "https://player.vimeo.com/video/123/config?h=abc123"
        );
    }

    // ── Progressive parsing ───────────────────────────────────────────────────

    #[test]
    fn parses_progressive_entry() {
        let entry = json!({
            "url": "https://cdn.vimeo.com/video/1080p.mp4?token=xxx",
            "quality": "1080p",
            "width": 1920,
            "height": 1080,
            "bitrate": 4_000_000,
            "codec": "H264",
            "mime": "video/mp4"
        });
        let v = parse_progressive(&entry).unwrap();
        assert_eq!(v.bandwidth_bps, 4_000_000);
        assert_eq!(v.resolution, Some((1920, 1080)));
        assert!(v.label.contains("1080p"));
        assert!(v.label.contains("mp4"));
        assert_eq!(v.codecs, vec!["avc1"]);
        assert_eq!(v.kind, StreamKind::Video);
    }

    #[test]
    fn skips_progressive_entry_with_empty_url() {
        let entry = json!({ "url": "", "quality": "720p" });
        assert!(parse_progressive(&entry).is_none());
    }

    #[test]
    fn skips_progressive_entry_without_url() {
        let entry = json!({ "quality": "360p", "bitrate": 800_000 });
        assert!(parse_progressive(&entry).is_none());
    }

    // ── HLS parsing ──────────────────────────────────────────────────────────

    #[test]
    fn picks_preferred_hls_cdn() {
        let hls = json!({
            "cdns": {
                "akfire_interconnect_quic": { "url": "https://preferred.cdn/master.m3u8" },
                "fastly_skyfire": { "url": "https://fallback.cdn/master.m3u8" }
            },
            "default_cdn": "fastly_skyfire",
            "separate_av": false
        });
        let url = best_hls_url(&hls).unwrap();
        assert_eq!(url, "https://preferred.cdn/master.m3u8");
    }

    #[test]
    fn falls_back_to_default_cdn() {
        let hls = json!({
            "cdns": {
                "some_cdn": { "url": "https://some.cdn/master.m3u8" }
            },
            "default_cdn": "some_cdn"
        });
        let url = best_hls_url(&hls).unwrap();
        assert_eq!(url, "https://some.cdn/master.m3u8");
    }

    #[test]
    fn hls_variant_has_correct_label_for_separate_av() {
        let hls = json!({
            "cdns": {
                "akfire_interconnect_quic": { "url": "https://cdn/master.m3u8" }
            },
            "separate_av": true
        });
        let variant = parse_hls_stream(&hls, true).unwrap();
        assert!(variant.label.contains("separate A/V"));
        assert_eq!(variant.bandwidth_bps, 0);
        assert_eq!(variant.kind, StreamKind::Video);
    }

    // ── StreamGraph assembly ──────────────────────────────────────────────────

    fn make_config() -> Value {
        json!({
            "video": {
                "title": "Test Video",
                "duration": 120,
                "thumbs": {
                    "640": "https://i.vimeocdn.com/video/thumb_640.jpg",
                    "1280": "https://i.vimeocdn.com/video/thumb_1280.jpg"
                }
            },
            "request": {
                "files": {
                    "progressive": [
                        {
                            "url": "https://cdn.vimeo.com/360p.mp4",
                            "quality": "360p",
                            "width": 640,
                            "height": 360,
                            "bitrate": 800_000,
                            "codec": "H264",
                            "mime": "video/mp4"
                        },
                        {
                            "url": "https://cdn.vimeo.com/1080p.mp4",
                            "quality": "1080p",
                            "width": 1920,
                            "height": 1080,
                            "bitrate": 4_000_000,
                            "codec": "H264",
                            "mime": "video/mp4"
                        },
                        {
                            "url": "https://cdn.vimeo.com/720p.mp4",
                            "quality": "720p",
                            "width": 1280,
                            "height": 720,
                            "bitrate": 2_000_000,
                            "codec": "H264",
                            "mime": "video/mp4"
                        }
                    ],
                    "hls": {
                        "cdns": {
                            "akfire_interconnect_quic": {
                                "url": "https://skyfire.cdn/master.m3u8"
                            }
                        },
                        "default_cdn": "akfire_interconnect_quic",
                        "separate_av": false
                    }
                }
            }
        })
    }

    #[test]
    fn builds_graph_with_title_and_four_variants() {
        let extractor = VimeoExtractor::new();
        let graph = extractor
            .build_stream_graph("https://vimeo.com/123", &make_config())
            .unwrap();

        assert_eq!(graph.title.as_deref(), Some("Test Video"));
        assert_eq!(graph.format, "vimeo");
        // 3 progressive + 1 HLS
        assert_eq!(graph.variants.len(), 4);
    }

    #[test]
    fn highest_bitrate_is_default_and_sorted_first() {
        let extractor = VimeoExtractor::new();
        let graph = extractor
            .build_stream_graph("https://vimeo.com/123", &make_config())
            .unwrap();

        let first = &graph.variants[0];
        assert!(first.is_default);
        assert_eq!(first.bandwidth_bps, 4_000_000);
        assert!(first.label.contains("1080p"));
    }

    #[test]
    fn progressive_sorted_before_adaptive() {
        let extractor = VimeoExtractor::new();
        let graph = extractor
            .build_stream_graph("https://vimeo.com/123", &make_config())
            .unwrap();

        // All progressive variants must appear before the HLS variant
        let adaptive_pos = graph
            .variants
            .iter()
            .position(|v| v.label.contains("HLS"))
            .unwrap();
        let max_progressive_pos = graph
            .variants
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.label.contains("HLS"))
            .map(|(i, _)| i)
            .max()
            .unwrap();
        assert!(adaptive_pos > max_progressive_pos);
    }

    #[test]
    fn thumbnail_in_standalone_tracks() {
        let extractor = VimeoExtractor::new();
        let graph = extractor
            .build_stream_graph("https://vimeo.com/123", &make_config())
            .unwrap();

        let thumb = graph
            .standalone_tracks
            .iter()
            .find(|t| t.label.as_deref() == Some("thumbnail"));
        assert!(thumb.is_some());
        // Should prefer 1280px thumbnail
        assert!(thumb.unwrap().url.contains("1280"));
    }

    #[test]
    fn no_streams_error_when_files_empty() {
        let config = json!({
            "video": { "title": "Empty" },
            "request": {
                "files": {}
            }
        });
        let extractor = VimeoExtractor::new();
        let result = extractor.build_stream_graph("https://vimeo.com/99", &config);
        assert!(matches!(result, Err(ExtractorError::NoStreams)));
    }

    #[test]
    fn error_when_files_node_missing() {
        let config = json!({ "request": {} });
        let extractor = VimeoExtractor::new();
        let result = extractor.build_stream_graph("https://vimeo.com/99", &config);
        assert!(matches!(result, Err(ExtractorError::Parse { .. })));
    }

    // ── is_vimeo_hash ─────────────────────────────────────────────────────────

    #[test]
    fn hash_detection_edge_cases() {
        assert!(is_vimeo_hash("abcdef012345")); // 12 hex chars — valid
        assert!(is_vimeo_hash("deadbeef")); // 8 hex chars — min valid
        assert!(!is_vimeo_hash("videos")); // word segment — not a hash
        assert!(!is_vimeo_hash("likes")); // word segment
        assert!(!is_vimeo_hash("abc")); // too short
        assert!(!is_vimeo_hash("xyz")); // too short + non-hex
        assert!(!is_vimeo_hash("abcdefg012345")); // 'g' is not hex
    }

    // ── can_handle ────────────────────────────────────────────────────────────

    #[test]
    fn handles_all_vimeo_url_forms() {
        let extractor = VimeoExtractor::new();
        assert!(extractor.can_handle("https://vimeo.com/123456"));
        assert!(extractor.can_handle("https://player.vimeo.com/video/123?h=abc"));
        assert!(extractor.can_handle("https://vimeo.com/channels/x/123"));
        assert!(!extractor.can_handle("https://youtube.com/watch?v=abc"));
    }
}
