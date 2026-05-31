//! `InstagramExtractor` — stream extraction for instagram.com
//!
//! ## Strategy
//! Instagram progressively restricts unauthenticated API access.  The
//! extractor implements three extraction passes in order of reliability:
//!
//! ### Pass A — Media endpoint (`?__a=1&__d=dis`)
//! The legacy `?__a=1` parameter causes Instagram to return a JSON
//! representation of the page instead of HTML.  This works without session
//! cookies for many public posts and reels.
//! ```
//! GET https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis
//! ```
//! Parses: `graphql.shortcode_media.video_url` (progressive MP4)
//!
//! ### Pass B — oEmbed (title + thumbnail only, no stream)
//! ```
//! GET https://www.instagram.com/api/v1/oembed/?url={url}
//! ```
//! Used as a title/thumbnail source when Pass A returns stream data.
//!
//! ### Pass C — GraphQL API (`/api/v1/media/{media_id}/info/`)
//! Requires a valid `sessionid` cookie supplied via the `cookie` header.
//! If `sessionid` is not available this pass is skipped gracefully.
//! Handles Reels, Carousel (multi-image/video posts), and Stories.
//!
//! ### Pass D — HTML scraping
//! Last resort: scrape the page HTML for `"video_url":` patterns.
//! Least reliable; used only when A/C both fail.
//!
//! ## Supported URL forms
//! - `https://www.instagram.com/p/{shortcode}/`
//! - `https://www.instagram.com/reel/{shortcode}/`
//! - `https://www.instagram.com/tv/{shortcode}/` (IGTV)
//! - `https://www.instagram.com/stories/{user}/{media_id}/`
//!
//! ## Limitations
//! - Private accounts always require a valid `sessionid`.
//! - Rate limiting: Instagram heavily throttles unauthenticated requests.
//!   Consumers should implement back-off between requests.
//! - Carousel posts only yield the first video item without a session.

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

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// Instagram mobile app UA — accepted by some API endpoints.
const MOBILE_UA: &str = "Instagram 269.0.0.18.75 Android (26/8.0.0; 480dpi; 1080x1920; \
     OnePlus; 6T Dev; devitron; qcom; en_US; 314665256)";

// ── URL parsing ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum InstagramTarget {
    /// Post (`/p/{shortcode}/`), Reel (`/reel/{shortcode}/`), IGTV (`/tv/{shortcode}/`)
    Post { shortcode: String },
    /// Story (`/stories/{user}/{media_id}/`)
    Story { user: String, media_id: String },
}

fn parse_instagram_url(url: &str) -> Option<InstagramTarget> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;
    if !host.contains("instagram.com") {
        return None;
    }

    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();

    match segments.as_slice() {
        // /p/{shortcode}[/…]  |  /reel/{shortcode}[/…]  |  /tv/{shortcode}[/…]
        [kind, shortcode, ..] if matches!(*kind, "p" | "reel" | "tv") => {
            Some(InstagramTarget::Post {
                shortcode: (*shortcode).to_string(),
            })
        }
        // /stories/{user}/{media_id}
        ["stories", user, media_id, ..] => Some(InstagramTarget::Story {
            user: (*user).to_string(),
            media_id: (*media_id).to_string(),
        }),
        _ => None,
    }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

/// Session cookie that the caller may inject via the `INSTAGRAM_SESSION_ID`
/// environment variable.  Enables authenticated extraction.
fn session_id_from_env() -> Option<String> {
    std::env::var("INSTAGRAM_SESSION_ID")
        .ok()
        .filter(|s| !s.is_empty())
}

async fn fetch_bytes(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    cookies: Option<&str>,
    mobile: bool,
) -> Result<Vec<u8>, ExtractorError> {
    let mut req = NetworkRequest::new(url, None);
    let ua = if mobile { MOBILE_UA } else { USER_AGENT };
    req.headers.push(("User-Agent".to_string(), ua.to_string()));
    req.headers
        .push(("Accept".to_string(), "application/json, */*".to_string()));
    req.headers
        .push(("Accept-Language".to_string(), "en-US,en;q=0.9".to_string()));
    req.headers
        .push(("X-IG-App-ID".to_string(), "936619743392459".to_string()));
    req.headers
        .push(("X-Requested-With".to_string(), "XMLHttpRequest".to_string()));
    req.headers.push((
        "Referer".to_string(),
        "https://www.instagram.com/".to_string(),
    ));
    if let Some(c) = cookies {
        req.headers.push(("Cookie".to_string(), c.to_string()));
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
    Ok(buf)
}

// ── Pass A — `?__a=1` endpoint ────────────────────────────────────────────────

async fn pass_a_json_endpoint(
    shortcode: &str,
    client: &Arc<dyn NetworkClient>,
    session_id: Option<&str>,
) -> Result<Option<Vec<StreamVariant>>, ExtractorError> {
    let url = format!("https://www.instagram.com/p/{shortcode}/?__a=1&__d=dis");

    let cookies = session_id.map(|sid| format!("sessionid={sid}; ig_did=0"));

    let bytes = fetch_bytes(&url, client, cookies.as_deref(), false).await?;

    // Instagram sometimes returns HTML instead of JSON when rate-limited
    if bytes.starts_with(b"<") {
        debug!("Pass A: got HTML instead of JSON — likely rate limited");
        return Ok(None);
    }

    let json: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    // Navigate: graphql.shortcode_media or items[0] depending on API version
    let media = json
        .pointer("/graphql/shortcode_media")
        .or_else(|| json.pointer("/items/0"))
        .or_else(|| json.get("media"));

    let Some(media) = media else {
        debug!("Pass A: no media node in JSON response");
        return Ok(None);
    };

    let variants = extract_variants_from_media_node(media);
    if variants.is_empty() {
        Ok(None)
    } else {
        Ok(Some(variants))
    }
}

/// Extract stream variants from an Instagram media node.
///
/// Handles:
/// - `video_url` (single progressive MP4)
/// - `video_versions[]` (array of per-quality MP4s, used in newer API responses)
/// - `carousel_media[]` (carousel — yields first video item)
fn extract_variants_from_media_node(media: &Value) -> Vec<StreamVariant> {
    let mut variants = Vec::new();

    // Carousel: descend into first video item
    if let Some(carousel) = media.get("carousel_media").and_then(Value::as_array) {
        for item in carousel {
            let sub_variants = extract_variants_from_media_node(item);
            if !sub_variants.is_empty() {
                return sub_variants; // return variants from first video item
            }
        }
    }

    // video_versions[] — per-quality renditions (Graph API / v1 API)
    if let Some(versions) = media.get("video_versions").and_then(Value::as_array) {
        for (i, v) in versions.iter().enumerate() {
            let url = match v.get("url").and_then(Value::as_str) {
                Some(u) if !u.is_empty() => u,
                _ => continue,
            };
            let width = v.get("width").and_then(Value::as_u64).unwrap_or(0) as u32;
            let height = v.get("height").and_then(Value::as_u64).unwrap_or(0) as u32;
            let resolution = if width > 0 && height > 0 {
                Some((width, height))
            } else {
                None
            };

            // Instagram video_versions are typically ordered best-first
            let bps = match i {
                0 => 2_000_000u64,
                1 => 1_200_000,
                _ => 600_000,
            };

            variants.push(StreamVariant {
                kind: StreamKind::Video,
                label: resolution
                    .map(|(_, h)| format!("{h}p [mp4]"))
                    .unwrap_or_else(|| format!("Quality {} [mp4]", i + 1)),
                bandwidth_bps: bps,
                resolution,
                codecs: vec!["avc1".to_string()],
                playlist_url: url.to_string(),
                segments: vec![],
                associated_tracks: vec![],
                is_default: i == 0,
            });
        }
    }

    if !variants.is_empty() {
        return variants;
    }

    // Fallback: single video_url
    if let Some(video_url) = media.get("video_url").and_then(Value::as_str) {
        if !video_url.is_empty() {
            variants.push(StreamVariant {
                kind: StreamKind::Video,
                label: "MP4 [mp4]".to_string(),
                bandwidth_bps: 1_200_000,
                resolution: None,
                codecs: vec!["avc1".to_string()],
                playlist_url: video_url.to_string(),
                segments: vec![],
                associated_tracks: vec![],
                is_default: true,
            });
        }
    }

    variants
}

// ── Pass C — `/api/v1/media/{media_id}/info/` ────────────────────────────────

async fn pass_c_media_api(
    media_id: &str,
    client: &Arc<dyn NetworkClient>,
    session_id: &str,
) -> Result<Option<Vec<StreamVariant>>, ExtractorError> {
    let url = format!("https://www.instagram.com/api/v1/media/{media_id}/info/");
    let cookies = format!("sessionid={session_id}; ig_did=0");

    debug!(media_id, "Pass C: fetching media info from Instagram API");

    let bytes = fetch_bytes(&url, client, Some(&cookies), true).await?;

    if bytes.starts_with(b"<") || bytes.is_empty() {
        return Ok(None);
    }

    let json: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    let items = json.get("items").and_then(Value::as_array);
    let Some(items) = items else { return Ok(None) };
    let Some(first) = items.first() else {
        return Ok(None);
    };

    let variants = extract_variants_from_media_node(first);
    if variants.is_empty() {
        Ok(None)
    } else {
        Ok(Some(variants))
    }
}

// ── Pass D — HTML scraping fallback ──────────────────────────────────────────

async fn pass_d_html_scrape(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    session_id: Option<&str>,
) -> Result<Option<Vec<StreamVariant>>, ExtractorError> {
    let cookies = session_id.map(|sid| format!("sessionid={sid}; ig_did=0"));

    let bytes = fetch_bytes(url, client, cookies.as_deref(), false).await?;
    let html = String::from_utf8_lossy(&bytes);

    // Look for `"video_url":"https://…"` patterns in page HTML
    let needle = "\"video_url\":\"";
    let mut variants = Vec::new();

    let mut search_start = 0;
    while let Some(pos) = html[search_start..].find(needle) {
        let abs_pos = search_start + pos + needle.len();
        let slice = &html[abs_pos..];
        // Find end of URL string (unescaped closing quote)
        let mut end = 0;
        let mut chars = slice.char_indices();
        while let Some((i, c)) = chars.next() {
            if c == '\\' {
                chars.next(); // skip escaped char
                continue;
            }
            if c == '"' {
                end = i;
                break;
            }
        }
        if end > 0 {
            let raw_url = &slice[..end];
            let url = raw_url.replace("\\/", "/");
            if url.starts_with("http")
                && !variants
                    .iter()
                    .any(|v: &StreamVariant| v.playlist_url == url)
            {
                debug!("Pass D: found video_url in HTML");
                variants.push(StreamVariant {
                    kind: StreamKind::Video,
                    label: format!("MP4 [mp4] ({})", variants.len() + 1),
                    bandwidth_bps: 1_000_000,
                    resolution: None,
                    codecs: vec!["avc1".to_string()],
                    playlist_url: url,
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: variants.is_empty(),
                });
            }
            search_start = abs_pos + end + 1;
        } else {
            break;
        }
    }

    Ok(if variants.is_empty() {
        None
    } else {
        Some(variants)
    })
}

// ── OEmbed title/thumbnail fetch ──────────────────────────────────────────────

async fn fetch_oembed_meta(
    url: &str,
    client: &Arc<dyn NetworkClient>,
) -> (Option<String>, Option<String>) {
    let oembed_url = format!(
        "https://www.instagram.com/api/v1/oembed/?url={}",
        url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>()
    );

    let bytes = match fetch_bytes(&oembed_url, client, None, false).await {
        Ok(b) => b,
        Err(_) => return (None, None),
    };

    let json: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => return (None, None),
    };

    let title = json
        .get("title")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(str::to_string);

    let thumbnail = json
        .get("thumbnail_url")
        .and_then(Value::as_str)
        .filter(|u| !u.is_empty())
        .map(str::to_string);

    (title, thumbnail)
}

// ── InstagramExtractor ────────────────────────────────────────────────────────

pub struct InstagramExtractor;

impl Default for InstagramExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl InstagramExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MediaExtractor for InstagramExtractor {
    fn name(&self) -> &'static str {
        "Instagram"
    }

    fn can_handle(&self, url: &str) -> bool {
        url.contains("instagram.com/")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        let target =
            parse_instagram_url(url).ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;

        let session_id = session_id_from_env();

        let (shortcode, story_media_id) = match &target {
            InstagramTarget::Post { shortcode } => (shortcode.as_str(), None),
            InstagramTarget::Story { media_id, .. } => ("", Some(media_id.as_str())),
        };

        debug!(shortcode, ?story_media_id, "extracting Instagram media");

        let mut variants: Option<Vec<StreamVariant>> = None;

        // ── Pass A — JSON endpoint ──────────────────────────────────────────
        if !shortcode.is_empty() {
            match pass_a_json_endpoint(shortcode, &client, session_id.as_deref()).await {
                Ok(Some(v)) => {
                    debug!(count = v.len(), "Pass A succeeded");
                    variants = Some(v);
                }
                Ok(None) => debug!("Pass A: no streams found"),
                Err(e) => warn!("Pass A failed: {e}"),
            }
        }

        // ── Pass C — authenticated media API ───────────────────────────────
        if variants.is_none() {
            if let Some(sid) = &session_id {
                let media_id = story_media_id.unwrap_or(shortcode);
                if !media_id.is_empty() {
                    match pass_c_media_api(media_id, &client, sid).await {
                        Ok(Some(v)) => {
                            debug!(count = v.len(), "Pass C succeeded");
                            variants = Some(v);
                        }
                        Ok(None) => debug!("Pass C: no streams found"),
                        Err(e) => warn!("Pass C failed: {e}"),
                    }
                }
            } else {
                debug!("Pass C skipped — no INSTAGRAM_SESSION_ID");
            }
        }

        // ── Pass D — HTML scraping ─────────────────────────────────────────
        if variants.is_none() {
            match pass_d_html_scrape(url, &client, session_id.as_deref()).await {
                Ok(Some(v)) => {
                    debug!(count = v.len(), "Pass D (HTML scrape) succeeded");
                    variants = Some(v);
                }
                Ok(None) => debug!("Pass D: no streams found in HTML"),
                Err(e) => warn!("Pass D failed: {e}"),
            }
        }

        let Some(mut stream_variants) = variants else {
            return Err(ExtractorError::NoStreams);
        };

        // Ensure exactly one default
        let has_default = stream_variants.iter().any(|v| v.is_default);
        if !has_default {
            if let Some(first) = stream_variants.first_mut() {
                first.is_default = true;
            }
        }

        // ── OEmbed: title + thumbnail (best-effort, non-blocking) ──────────
        let (title, thumbnail_url) = fetch_oembed_meta(url, &client).await;

        let mut standalone_tracks = vec![];
        if let Some(thumb) = thumbnail_url {
            standalone_tracks.push(MediaTrack {
                kind: StreamKind::Unknown,
                language: None,
                label: Some("thumbnail".to_string()),
                url: thumb,
                default_track: false,
            });
        }

        debug!(
            title = ?title,
            variants = stream_variants.len(),
            "Instagram extraction complete"
        );

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: url.to_string(),
            title,
            variants: stream_variants,
            standalone_tracks,
            format: "instagram".to_string(),
        })
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── URL parsing ──────────────────────────────────────────────────────────

    #[test]
    fn parses_post_url() {
        match parse_instagram_url("https://www.instagram.com/p/CpABC123de5/") {
            Some(InstagramTarget::Post { shortcode }) => assert_eq!(shortcode, "CpABC123de5"),
            other => panic!("expected Post, got {other:?}"),
        }
    }

    #[test]
    fn parses_reel_url() {
        match parse_instagram_url("https://www.instagram.com/reel/CxYZ789fg0/") {
            Some(InstagramTarget::Post { shortcode }) => assert_eq!(shortcode, "CxYZ789fg0"),
            other => panic!("expected Post (reel), got {other:?}"),
        }
    }

    #[test]
    fn parses_igtv_url() {
        match parse_instagram_url("https://www.instagram.com/tv/Cabc123xyz/") {
            Some(InstagramTarget::Post { shortcode }) => assert_eq!(shortcode, "Cabc123xyz"),
            other => panic!("expected Post (tv), got {other:?}"),
        }
    }

    #[test]
    fn parses_story_url() {
        match parse_instagram_url("https://www.instagram.com/stories/someuser/123456789012345678/")
        {
            Some(InstagramTarget::Story { user, media_id }) => {
                assert_eq!(user, "someuser");
                assert_eq!(media_id, "123456789012345678");
            }
            other => panic!("expected Story, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_instagram_url() {
        assert!(parse_instagram_url("https://twitter.com/user").is_none());
    }

    // ── Media node parsing ───────────────────────────────────────────────────

    #[test]
    fn extracts_single_video_url() {
        let media = json!({
            "video_url": "https://scontent.cdninstagram.com/v/video.mp4"
        });
        let variants = extract_variants_from_media_node(&media);
        assert_eq!(variants.len(), 1);
        assert!(variants[0].is_default);
        assert!(variants[0].playlist_url.contains("video.mp4"));
    }

    #[test]
    fn extracts_video_versions_array() {
        let media = json!({
            "video_versions": [
                { "url": "https://cdn.ig.com/hd.mp4", "width": 1080, "height": 1920 },
                { "url": "https://cdn.ig.com/sd.mp4", "width": 720,  "height": 1280 }
            ]
        });
        let variants = extract_variants_from_media_node(&media);
        assert_eq!(variants.len(), 2);
        assert!(variants[0].is_default);
        assert_eq!(variants[0].resolution, Some((1080, 1920)));
        assert_eq!(variants[1].resolution, Some((720, 1280)));
        assert!(!variants[1].is_default);
    }

    #[test]
    fn carousel_yields_first_video_item() {
        let media = json!({
            "carousel_media": [
                { "image_url": "https://cdn.ig.com/img.jpg" }, // image item — skip
                { "video_url": "https://cdn.ig.com/reel.mp4" } // video item
            ]
        });
        let variants = extract_variants_from_media_node(&media);
        assert_eq!(variants.len(), 1);
        assert!(variants[0].playlist_url.contains("reel.mp4"));
    }

    #[test]
    fn returns_empty_for_non_video_media() {
        let media = json!({ "image_url": "https://cdn.ig.com/photo.jpg" });
        assert!(extract_variants_from_media_node(&media).is_empty());
    }

    // ── can_handle ────────────────────────────────────────────────────────────

    #[test]
    fn handles_all_instagram_url_forms() {
        let e = InstagramExtractor::new();
        assert!(e.can_handle("https://www.instagram.com/p/ABC123/"));
        assert!(e.can_handle("https://www.instagram.com/reel/XYZ789/"));
        assert!(e.can_handle("https://www.instagram.com/tv/DEF456/"));
        assert!(e.can_handle("https://www.instagram.com/stories/user/123/"));
        assert!(!e.can_handle("https://twitter.com/user"));
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
    }
}
