//! `TwitterExtractor` — stream extraction for twitter.com / x.com
//!
//! ## Strategy
//! Twitter/X requires a bearer token and a guest token to call its API without
//! an authenticated session.  The flow is:
//!
//! 1. **Acquire a guest token** — POST to
//!    `https://api.twitter.com/1.1/guest/activate.json` with the public bearer
//!    token that the Twitter web app embeds in its JavaScript bundle.
//! 2. **Fetch tweet data** — GET
//!    `https://api.twitter.com/1.1/statuses/show/{tweet_id}.json?tweet_mode=extended`
//!    supplying both `Authorization: Bearer …` and `x-guest-token: …` headers.
//! 3. **Parse `extended_entities.media[]`** — each item whose `type` is
//!    `"video"` or `"animated_gif"` contains a `video_info.variants[]` array
//!    with per-quality MP4 URLs and bitrates.
//! 4. Sort variants best-first, mark default, return `StreamGraph`.
//!
//! ## Supported URL forms
//! - `https://twitter.com/{user}/status/{id}`
//! - `https://x.com/{user}/status/{id}`
//! - `https://mobile.twitter.com/{user}/status/{id}`
//!
//! ## Limitations
//! - Guest-token sessions are rate-limited; repeated rapid extractions may
//!   trigger 429s.  The extractor does not cache tokens across invocations.
//! - Protected / private accounts are not accessible without user OAuth.
//! - Spaces and live streams are not covered by this extractor.

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

/// Public bearer token embedded in the Twitter/X web application.
/// This token is intentionally public; Twitter uses it for unauthenticated
/// API access from the browser before a user logs in.
const TWITTER_BEARER: &str = "AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs\
     %3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA";

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// ── URL parsing ───────────────────────────────────────────────────────────────

/// Extract the numeric tweet ID from a Twitter/X URL.
///
/// Handles:
/// - `https://twitter.com/{user}/status/{id}`
/// - `https://x.com/{user}/status/{id}`
/// - `https://mobile.twitter.com/{user}/status/{id}`
fn extract_tweet_id(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;

    if !host.contains("twitter.com") && !host.contains("x.com") {
        return None;
    }

    // Path: /{user}/status/{numeric_id}[/…]
    let mut segments = parsed.path_segments()?.filter(|s| !s.is_empty());

    // Skip user segment
    segments.next()?;
    // Must be "status"
    let marker = segments.next()?;
    if marker != "status" {
        return None;
    }
    // Tweet ID — must be numeric
    let id = segments.next()?;
    if id.chars().all(|c| c.is_ascii_digit()) && !id.is_empty() {
        Some(id.to_string())
    } else {
        None
    }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

async fn fetch_bytes(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    extra_headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<Vec<u8>, ExtractorError> {
    let mut req = NetworkRequest::new(url, None);
    req.headers
        .push(("User-Agent".to_string(), USER_AGENT.to_string()));
    req.headers.push((
        "Authorization".to_string(),
        format!("Bearer {TWITTER_BEARER}"),
    ));
    for (k, v) in extra_headers {
        req.headers.push((k.to_string(), v.to_string()));
    }
    if let Some(b) = body {
        req.headers
            .push(("Content-Length".to_string(), b.len().to_string()));
        req.body = Some(b);
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

// ── Guest token ───────────────────────────────────────────────────────────────

/// Obtain a fresh guest token from the Twitter API.
///
/// The token is short-lived (typically hours); we acquire one per extraction.
async fn acquire_guest_token(client: &Arc<dyn NetworkClient>) -> Result<String, ExtractorError> {
    debug!("acquiring Twitter guest token");

    let bytes = fetch_bytes(
        "https://api.twitter.com/1.1/guest/activate.json",
        client,
        &[],
        Some(Vec::new()), // POST with empty body triggers token generation
    )
    .await?;

    let json: Value = serde_json::from_slice(&bytes).map_err(|e| ExtractorError::Parse {
        format: "twitter",
        reason: format!("guest activate JSON error: {e}"),
    })?;

    json.get("guest_token")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitter",
            reason: "guest_token field missing from activate response".to_string(),
        })
}

// ── Tweet fetch ───────────────────────────────────────────────────────────────

/// Fetch the full tweet JSON (extended mode) using the v1.1 statuses endpoint.
async fn fetch_tweet(
    tweet_id: &str,
    guest_token: &str,
    client: &Arc<dyn NetworkClient>,
) -> Result<Value, ExtractorError> {
    let url = format!(
        "https://api.twitter.com/1.1/statuses/show/{tweet_id}.json\
         ?tweet_mode=extended\
         &include_entities=true\
         &include_ext_media_availability=true"
    );

    debug!(tweet_id, "fetching tweet data");

    let bytes = fetch_bytes(
        &url,
        client,
        &[
            ("x-guest-token", guest_token),
            ("x-twitter-active-user", "yes"),
            ("x-twitter-client-language", "en"),
        ],
        None,
    )
    .await?;

    serde_json::from_slice(&bytes).map_err(|e| ExtractorError::Parse {
        format: "twitter",
        reason: format!("tweet JSON parse error: {e}"),
    })
}

// ── Stream parsing ────────────────────────────────────────────────────────────

/// Parse `extended_entities.media[]` into `StreamVariant`s.
///
/// Each media entity of type `"video"` or `"animated_gif"` contains
/// `video_info.variants[]`, each with a `content_type` and `url`.
/// MP4 variants have a `bitrate` field; M3U8 variants (`content_type:
/// "application/x-mpeg-URL"`) do not.
fn parse_media_variants(tweet: &Value, source_url: &str) -> Result<StreamGraph, ExtractorError> {
    // Prefer extended_entities; fall back to entities
    let media_array = tweet
        .pointer("/extended_entities/media")
        .or_else(|| tweet.pointer("/entities/media"))
        .and_then(Value::as_array)
        .ok_or(ExtractorError::NoStreams)?;

    // Find first video media entity
    let video_media = media_array
        .iter()
        .find(|m| {
            matches!(
                m.get("type").and_then(Value::as_str),
                Some("video") | Some("animated_gif")
            )
        })
        .ok_or(ExtractorError::NoStreams)?;

    let is_gif = video_media
        .get("type")
        .and_then(Value::as_str)
        .map_or(false, |t| t == "animated_gif");

    let variants = video_media
        .pointer("/video_info/variants")
        .and_then(Value::as_array)
        .ok_or(ExtractorError::NoStreams)?;

    // Title from tweet text
    let title = tweet
        .get("full_text")
        .or_else(|| tweet.get("text"))
        .and_then(Value::as_str)
        .map(|t| {
            // Truncate long tweet text for display
            let trimmed = t.trim();
            if trimmed.len() > 100 {
                format!("{}…", &trimmed[..97])
            } else {
                trimmed.to_string()
            }
        });

    let mut stream_variants: Vec<StreamVariant> = Vec::new();

    for variant in variants {
        let content_type = variant
            .get("content_type")
            .and_then(Value::as_str)
            .unwrap_or("video/mp4");
        let url = match variant.get("url").and_then(Value::as_str) {
            Some(u) if !u.is_empty() => u,
            _ => continue,
        };

        if content_type.contains("mpegURL") || content_type.contains("m3u8") {
            // HLS adaptive stream
            stream_variants.push(StreamVariant {
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
            continue;
        }

        // MP4 variant — has bitrate
        let bitrate = variant.get("bitrate").and_then(Value::as_u64).unwrap_or(0);

        // Try to infer resolution from the URL (Twitter CDN embeds WxH in path).
        let resolution = infer_resolution_from_url(url);

        let label = if is_gif {
            format!("GIF-MP4 {}kbps", bitrate / 1000)
        } else {
            match resolution {
                Some((w, h)) => format!("{h}p [mp4]"),
                None => format!("{}kbps [mp4]", bitrate / 1000),
            }
        };

        stream_variants.push(StreamVariant {
            kind: StreamKind::Video,
            label,
            bandwidth_bps: bitrate,
            resolution,
            codecs: vec!["avc1".to_string(), "mp4a.40.2".to_string()],
            playlist_url: url.to_string(),
            segments: vec![],
            associated_tracks: vec![],
            is_default: false,
        });
    }

    if stream_variants.is_empty() {
        return Err(ExtractorError::NoStreams);
    }

    // Sort: MP4 by descending bitrate first, HLS last
    stream_variants.sort_unstable_by(|a, b| {
        let a_hls = a.bandwidth_bps == 0 && a.label.contains("HLS");
        let b_hls = b.bandwidth_bps == 0 && b.label.contains("HLS");
        match (a_hls, b_hls) {
            (false, true) => std::cmp::Ordering::Less,
            (true, false) => std::cmp::Ordering::Greater,
            _ => b.bandwidth_bps.cmp(&a.bandwidth_bps),
        }
    });

    if let Some(first) = stream_variants.first_mut() {
        first.is_default = true;
    }

    // Thumbnail from media entity
    let mut standalone_tracks = vec![];
    if let Some(thumb_url) = video_media
        .pointer("/media_url_https")
        .and_then(Value::as_str)
        .filter(|u| !u.is_empty())
    {
        use crate::stream_graph::MediaTrack;
        standalone_tracks.push(MediaTrack {
            kind: StreamKind::Unknown,
            language: None,
            label: Some("thumbnail".to_string()),
            url: thumb_url.to_string(),
            default_track: false,
        });
    }

    Ok(StreamGraph {
        is_live: false,
        target_duration_secs: None,
        source_url: source_url.to_string(),
        title,
        variants: stream_variants,
        standalone_tracks,
        format: "twitter".to_string(),
    })
}

/// Try to infer resolution from a Twitter CDN URL.
///
/// Twitter CDN paths typically contain `/{width}x{height}/` segments, e.g.:
/// `https://video.twimg.com/amplify_video/12345/vid/1280x720/video.mp4`
fn infer_resolution_from_url(url: &str) -> Option<(u32, u32)> {
    // Look for a path component matching `{digits}x{digits}`
    url.split('/').find_map(|seg| {
        let (w_str, h_str) = seg.split_once('x')?;
        let w: u32 = w_str.parse().ok()?;
        let h: u32 = h_str
            .split(|c: char| !c.is_ascii_digit())
            .next()?
            .parse()
            .ok()?;
        if w > 0 && h > 0 {
            Some((w, h))
        } else {
            None
        }
    })
}

// ── TwitterExtractor ──────────────────────────────────────────────────────────

pub struct TwitterExtractor;

impl Default for TwitterExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl TwitterExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MediaExtractor for TwitterExtractor {
    fn name(&self) -> &'static str {
        "Twitter/X"
    }

    fn can_handle(&self, url: &str) -> bool {
        url.contains("twitter.com/") || url.contains("x.com/")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        // 1. Parse tweet ID
        let tweet_id =
            extract_tweet_id(url).ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;
        debug!(tweet_id, "resolved Twitter tweet ID");

        // 2. Acquire guest token
        let guest_token = acquire_guest_token(&client).await?;
        debug!("acquired Twitter guest token");

        // 3. Fetch tweet data
        let tweet = fetch_tweet(&tweet_id, &guest_token, &client).await?;

        // Check for API error responses
        if let Some(errors) = tweet.get("errors").and_then(Value::as_array) {
            if let Some(first_error) = errors.first() {
                let msg = first_error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown Twitter API error");
                return Err(ExtractorError::Network(format!("Twitter API error: {msg}")));
            }
        }

        // 4. Parse media variants
        let graph = parse_media_variants(&tweet, url)?;

        debug!(
            title = ?graph.title,
            variants = graph.variants.len(),
            "Twitter extraction complete"
        );

        Ok(graph)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_standard_tweet_url() {
        assert_eq!(
            extract_tweet_id("https://twitter.com/user/status/1234567890"),
            Some("1234567890".to_string())
        );
    }

    #[test]
    fn parses_x_com_url() {
        assert_eq!(
            extract_tweet_id("https://x.com/elonmusk/status/9876543210"),
            Some("9876543210".to_string())
        );
    }

    #[test]
    fn parses_mobile_url() {
        assert_eq!(
            extract_tweet_id("https://mobile.twitter.com/user/status/111222333"),
            Some("111222333".to_string())
        );
    }

    #[test]
    fn rejects_non_status_url() {
        assert!(extract_tweet_id("https://twitter.com/user").is_none());
    }

    #[test]
    fn rejects_non_twitter_url() {
        assert!(extract_tweet_id("https://youtube.com/watch?v=abc").is_none());
    }

    #[test]
    fn infers_resolution_from_cdn_url() {
        let url = "https://video.twimg.com/amplify_video/123/vid/1280x720/video.mp4?tag=14";
        assert_eq!(infer_resolution_from_url(url), Some((1280, 720)));
    }

    #[test]
    fn infers_resolution_1920x1080() {
        let url = "https://video.twimg.com/ext_tw_video/99/pu/vid/1920x1080/vid.mp4";
        assert_eq!(infer_resolution_from_url(url), Some((1920, 1080)));
    }

    #[test]
    fn returns_none_for_url_without_resolution() {
        let url = "https://video.twimg.com/video/video.mp4";
        assert!(infer_resolution_from_url(url).is_none());
    }

    #[test]
    fn parses_video_variants_from_tweet() {
        let tweet = json!({
            "full_text": "Check out this video!",
            "extended_entities": {
                "media": [{
                    "type": "video",
                    "media_url_https": "https://pbs.twimg.com/thumb.jpg",
                    "video_info": {
                        "variants": [
                            {
                                "content_type": "video/mp4",
                                "url": "https://video.twimg.com/vid/1280x720/video.mp4",
                                "bitrate": 2176000
                            },
                            {
                                "content_type": "video/mp4",
                                "url": "https://video.twimg.com/vid/640x360/video.mp4",
                                "bitrate": 832000
                            },
                            {
                                "content_type": "application/x-mpegURL",
                                "url": "https://video.twimg.com/master.m3u8"
                            }
                        ]
                    }
                }]
            }
        });

        let graph = parse_media_variants(&tweet, "https://twitter.com/u/status/1").unwrap();
        assert_eq!(graph.variants.len(), 3);
        assert!(graph.variants[0].is_default);
        // Highest bitrate MP4 first
        assert_eq!(graph.variants[0].bandwidth_bps, 2_176_000);
        // HLS last
        assert!(graph.variants.last().unwrap().label.contains("HLS"));
    }

    #[test]
    fn returns_no_streams_for_tweet_without_video() {
        let tweet = json!({
            "full_text": "Just text",
            "entities": { "media": [] }
        });
        assert!(matches!(
            parse_media_variants(&tweet, "https://twitter.com/u/status/1"),
            Err(ExtractorError::NoStreams)
        ));
    }

    #[test]
    fn can_handle_all_url_forms() {
        let e = TwitterExtractor::new();
        assert!(e.can_handle("https://twitter.com/user/status/123"));
        assert!(e.can_handle("https://x.com/user/status/123"));
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
    }
}
