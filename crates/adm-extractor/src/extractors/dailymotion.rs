//! `DailymotionExtractor` — stream extraction for dailymotion.com
//!
//! ## Strategy
//! 1. Parse the video ID from the URL (standard `/video/{id}` and `dai.ly/{id}` short links).
//! 2. Fetch the official Dailymotion player metadata endpoint:
//!    `https://www.dailymotion.com/player/metadata/video/{id}?…`
//!    This returns JSON used by the embedded player; it includes every quality level and
//!    both progressive MP4 and HLS (M3U8) URLs without authentication for public videos.
//! 3. Parse `qualities` → progressive MP4 entries per quality label (1080, 720, 480, 380, 240).
//! 4. Parse `qualities.auto` → M3U8 adaptive stream URL when present.
//! 5. Build a `StreamGraph` with all variants sorted best-first.
//!
//! ## Supported URL forms
//! - `https://www.dailymotion.com/video/x7tgad0`
//! - `https://www.dailymotion.com/video/x7tgad0_some-title`
//! - `https://dai.ly/x7tgad0`
//! - `https://www.dailymotion.com/embed/video/x7tgad0`
//!
//! ## Not supported (yet)
//! - Private / password-protected videos
//! - Paid content / DRM (Widevine)
//! - Playlists / channels (only single video extraction)

use crate::{
    stream_graph::{StreamGraph, StreamKind, StreamVariant},
    ExtractorError, MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Preferred quality labels in descending order.
const QUALITY_ORDER: &[(&str, u64, Option<(u32, u32)>)] = &[
    ("2160", 20_000_000, Some((3840, 2160))),
    ("1440", 12_000_000, Some((2560, 1440))),
    ("1080", 6_000_000, Some((1920, 1080))),
    ("720", 3_000_000, Some((1280, 720))),
    ("480", 1_500_000, Some((854, 480))),
    ("380", 900_000, Some((676, 380))),
    ("240", 500_000, Some((426, 240))),
];

// ── HTTP helper ───────────────────────────────────────────────────────────────

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
    req.headers
        .push(("Accept-Language".to_string(), "en-US,en;q=0.9".to_string()));
    if let Some(r) = referer {
        req.headers.push(("Referer".to_string(), r.to_string()));
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
        format: "dailymotion",
        reason: format!("utf-8 decode error: {e}"),
    })
}

// ── URL parsing ───────────────────────────────────────────────────────────────

/// Extract the Dailymotion video ID from a URL.
///
/// Handles:
/// - `https://www.dailymotion.com/video/x7tgad0`
/// - `https://www.dailymotion.com/video/x7tgad0_some-title-slug`
/// - `https://dai.ly/x7tgad0`
/// - `https://www.dailymotion.com/embed/video/x7tgad0`
fn extract_video_id(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;

    // Short link: dai.ly/{id}
    if host == "dai.ly" {
        return parsed
            .path_segments()?
            .find(|s| !s.is_empty())
            .map(|s| s.split('_').next().unwrap_or(s).to_string());
    }

    if !host.contains("dailymotion.com") {
        return None;
    }

    // /video/{id}[_slug] or /embed/video/{id}[_slug]
    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();

    // Find segment after "video"
    let video_pos = segments.iter().position(|&s| s == "video")?;
    let raw_id = segments.get(video_pos + 1)?;
    // Strip optional _slug suffix
    let id = raw_id.split('_').next().unwrap_or(raw_id);
    if id.is_empty() {
        return None;
    }
    Some(id.to_string())
}

// ── Metadata parsing ──────────────────────────────────────────────────────────

/// Parse quality entries from `metadata.qualities`.
///
/// The `qualities` object has keys like `"1080"`, `"720"`, `"480"`, `"380"`,
/// `"240"`, and `"auto"`.  Each key maps to an array of rendition objects:
/// ```json
/// { "type": "video/mp4", "url": "https://…" }
/// ```
/// or for adaptive:
/// ```json
/// { "type": "application/x-mpegURL", "url": "https://…" }
/// ```
fn parse_qualities(
    qualities: &Value,
    title: Option<&str>,
    source_url: &str,
) -> Result<StreamGraph, ExtractorError> {
    let obj = qualities.as_object().ok_or_else(|| ExtractorError::Parse {
        format: "dailymotion",
        reason: "qualities field is not an object".to_string(),
    })?;

    let mut variants: Vec<StreamVariant> = Vec::new();

    // ── Progressive MP4 variants ──────────────────────────────────────────────
    for &(label, approx_bps, resolution) in QUALITY_ORDER {
        let Some(renditions) = obj.get(label).and_then(Value::as_array) else {
            continue;
        };

        // Prefer MP4; fall back to first available rendition.
        let mp4_entry = renditions
            .iter()
            .find(|r| {
                r.get("type")
                    .and_then(Value::as_str)
                    .map_or(false, |t| t.contains("mp4"))
            })
            .or_else(|| renditions.first());

        let Some(entry) = mp4_entry else { continue };

        let Some(url) = entry.get("url").and_then(Value::as_str) else {
            continue;
        };
        if url.is_empty() {
            continue;
        }

        let mime = entry
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("video/mp4");

        // Skip HLS renditions here — handled in the `auto` block below.
        if mime.contains("mpegURL") || mime.contains("m3u8") {
            continue;
        }

        debug!(label, url, "parsed Dailymotion MP4 variant");

        variants.push(StreamVariant {
            kind: StreamKind::Video,
            label: format!("{label}p [mp4]"),
            bandwidth_bps: approx_bps,
            resolution,
            codecs: vec!["avc1".to_string()],
            playlist_url: url.to_string(),
            segments: vec![],
            associated_tracks: vec![],
            is_default: false,
        });
    }

    // ── HLS adaptive stream (`auto` key) ─────────────────────────────────────
    if let Some(auto_renditions) = obj.get("auto").and_then(Value::as_array) {
        let hls_entry = auto_renditions.iter().find(|r| {
            r.get("type")
                .and_then(Value::as_str)
                .map_or(false, |t| t.contains("mpegURL") || t.contains("m3u8"))
        });

        if let Some(entry) = hls_entry {
            if let Some(url) = entry.get("url").and_then(Value::as_str) {
                if !url.is_empty() {
                    debug!(url, "parsed Dailymotion HLS adaptive stream");
                    variants.push(StreamVariant {
                        kind: StreamKind::Video,
                        label: "HLS (adaptive)".to_string(),
                        bandwidth_bps: 0,
                        resolution: None,
                        codecs: vec![],
                        playlist_url: url.to_string(),
                        segments: vec![],
                        associated_tracks: vec![],
                        is_default: false,
                    });
                }
            }
        }
    }

    if variants.is_empty() {
        return Err(ExtractorError::NoStreams);
    }

    // Sort: progressive (higher bps) before adaptive (bps==0), then desc bandwidth.
    variants.sort_unstable_by(|a, b| {
        let a_adaptive = a.bandwidth_bps == 0;
        let b_adaptive = b.bandwidth_bps == 0;
        match (a_adaptive, b_adaptive) {
            (false, true) => std::cmp::Ordering::Less,
            (true, false) => std::cmp::Ordering::Greater,
            _ => b.bandwidth_bps.cmp(&a.bandwidth_bps),
        }
    });

    if let Some(first) = variants.first_mut() {
        first.is_default = true;
    }

    Ok(StreamGraph {
        is_live: false,
        target_duration_secs: None,
        source_url: source_url.to_string(),
        title: title.map(str::to_string),
        variants,
        standalone_tracks: vec![],
        format: "dailymotion".to_string(),
    })
}

// ── DailymotionExtractor ──────────────────────────────────────────────────────

pub struct DailymotionExtractor;

impl Default for DailymotionExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl DailymotionExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    /// Fetch the player metadata JSON for `video_id`.
    async fn fetch_metadata(
        &self,
        video_id: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<Value, ExtractorError> {
        // The endpoint used by the Dailymotion embedded player — public, no auth required.
        let url = format!(
            "https://www.dailymotion.com/player/metadata/video/{video_id}\
             ?embedder=https%3A%2F%2Fwww.dailymotion.com\
             &locale=en_US\
             &dmV1st=00000000-0000-0000-0000-000000000000\
             &dmTs=0\
             &is_age_gate_disabled=true"
        );

        debug!(video_id, "fetching Dailymotion player metadata");

        let referer = format!("https://www.dailymotion.com/video/{video_id}");
        let text = fetch_text(&url, client, Some(&referer)).await?;

        serde_json::from_str(&text).map_err(|e| ExtractorError::Parse {
            format: "dailymotion",
            reason: format!("metadata JSON parse error: {e}"),
        })
    }
}

#[async_trait]
impl MediaExtractor for DailymotionExtractor {
    fn name(&self) -> &'static str {
        "Dailymotion"
    }

    fn can_handle(&self, url: &str) -> bool {
        url.contains("dailymotion.com/") || url.contains("dai.ly/")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        // 1. Parse video ID
        let video_id =
            extract_video_id(url).ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;
        debug!(video_id, "resolved Dailymotion video ID");

        // 2. Fetch metadata
        let metadata = self.fetch_metadata(&video_id, &client).await?;

        // 3. Check for error responses (e.g. geo-blocked, deleted)
        if let Some(err) = metadata.get("error") {
            let msg = err
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown error");
            return Err(ExtractorError::Network(format!(
                "Dailymotion API error: {msg}"
            )));
        }

        // 4. Extract title
        let title = metadata
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_string);

        // 5. Parse qualities
        let qualities = metadata
            .get("qualities")
            .ok_or_else(|| ExtractorError::Parse {
                format: "dailymotion",
                reason: "no 'qualities' field in metadata response".to_string(),
            })?;

        let mut graph = parse_qualities(qualities, title.as_deref(), url)?;

        debug!(
            title = ?graph.title,
            variants = graph.variants.len(),
            "Dailymotion extraction complete"
        );

        // 6. Attach thumbnail if available
        if let Some(thumb_url) = metadata
            .get("thumbnail_720_url")
            .or_else(|| metadata.get("thumbnail_480_url"))
            .or_else(|| metadata.get("thumbnail_url"))
            .and_then(Value::as_str)
        {
            if !thumb_url.is_empty() {
                use crate::stream_graph::MediaTrack;
                graph.standalone_tracks.push(MediaTrack {
                    kind: StreamKind::Unknown,
                    language: None,
                    label: Some("thumbnail".to_string()),
                    url: thumb_url.to_string(),
                    default_track: false,
                });
            }
        }

        Ok(graph)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── URL parsing ──────────────────────────────────────────────────────────

    #[test]
    fn parses_standard_video_url() {
        assert_eq!(
            extract_video_id("https://www.dailymotion.com/video/x7tgad0"),
            Some("x7tgad0".to_string())
        );
    }

    #[test]
    fn parses_video_url_with_slug() {
        assert_eq!(
            extract_video_id("https://www.dailymotion.com/video/x7tgad0_some-cool-title"),
            Some("x7tgad0".to_string())
        );
    }

    #[test]
    fn parses_short_link() {
        assert_eq!(
            extract_video_id("https://dai.ly/x7tgad0"),
            Some("x7tgad0".to_string())
        );
    }

    #[test]
    fn parses_embed_url() {
        assert_eq!(
            extract_video_id("https://www.dailymotion.com/embed/video/x7tgad0"),
            Some("x7tgad0".to_string())
        );
    }

    #[test]
    fn rejects_non_dailymotion_url() {
        assert!(extract_video_id("https://youtube.com/watch?v=abc").is_none());
    }

    // ── Quality parsing ──────────────────────────────────────────────────────

    fn make_qualities() -> Value {
        json!({
            "1080": [
                { "type": "video/mp4", "url": "https://cdn.dm.com/1080.mp4?sig=abc" }
            ],
            "720": [
                { "type": "video/mp4", "url": "https://cdn.dm.com/720.mp4?sig=abc" }
            ],
            "480": [
                { "type": "video/mp4", "url": "https://cdn.dm.com/480.mp4?sig=abc" }
            ],
            "auto": [
                { "type": "application/x-mpegURL", "url": "https://cdn.dm.com/master.m3u8" }
            ]
        })
    }

    #[test]
    fn parses_all_qualities() {
        let qualities = make_qualities();
        let graph = parse_qualities(
            &qualities,
            Some("Test Video"),
            "https://www.dailymotion.com/video/x1",
        )
        .unwrap();
        // 3 MP4 + 1 HLS
        assert_eq!(graph.variants.len(), 4);
        assert_eq!(graph.title.as_deref(), Some("Test Video"));
        assert_eq!(graph.format, "dailymotion");
    }

    #[test]
    fn highest_quality_is_default_and_first() {
        let qualities = make_qualities();
        let graph =
            parse_qualities(&qualities, None, "https://www.dailymotion.com/video/x1").unwrap();
        let first = &graph.variants[0];
        assert!(first.is_default);
        assert!(first.label.contains("1080"));
    }

    #[test]
    fn hls_sorted_after_progressive() {
        let qualities = make_qualities();
        let graph =
            parse_qualities(&qualities, None, "https://www.dailymotion.com/video/x1").unwrap();
        let hls_pos = graph
            .variants
            .iter()
            .position(|v| v.label.contains("HLS"))
            .unwrap();
        let last_mp4_pos = graph
            .variants
            .iter()
            .enumerate()
            .filter(|(_, v)| !v.label.contains("HLS"))
            .map(|(i, _)| i)
            .max()
            .unwrap();
        assert!(hls_pos > last_mp4_pos);
    }

    #[test]
    fn error_when_qualities_empty() {
        let qualities = json!({});
        let result = parse_qualities(&qualities, None, "https://www.dailymotion.com/video/x1");
        assert!(matches!(result, Err(ExtractorError::NoStreams)));
    }

    // ── can_handle ────────────────────────────────────────────────────────────

    #[test]
    fn handles_all_url_forms() {
        let e = DailymotionExtractor::new();
        assert!(e.can_handle("https://www.dailymotion.com/video/x7tgad0"));
        assert!(e.can_handle("https://dai.ly/x7tgad0"));
        assert!(e.can_handle("https://www.dailymotion.com/embed/video/x7tgad0"));
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
        assert!(!e.can_handle("https://vimeo.com/123"));
    }
}
