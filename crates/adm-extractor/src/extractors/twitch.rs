//! `TwitchExtractor` — stream extraction for twitch.tv
//!
//! ## Strategy
//! Twitch requires a signed access token before the CDN will serve HLS.
//! The extraction flow mirrors what the Twitch web player does:
//!
//! ### Live channels (`https://twitch.tv/{channel}`)
//! 1. Acquire a **Gql PlaybackAccessToken** via a POST to
//!    `https://gql.twitch.tv/gql` using the public embedded-player client ID.
//! 2. Build the Usher HLS URL:
//!    `https://usher.twitchapps.com/api/channel/hls/{channel}.m3u8?sig=…&token=…`
//! 3. Return the signed M3U8 as an HLS `StreamVariant`; the `GenericHlsExtractor`
//!    expands it in a second pass.
//!
//! ### VODs (`https://twitch.tv/videos/{vod_id}`)
//! 1. Acquire a **VideoAccessToken** via Gql for the specific VOD.
//! 2. Build the Usher VOD URL:
//!    `https://usher.twitchapps.com/vod/{vod_id}.m3u8?sig=…&token=…`
//! 3. Return the signed M3U8.
//!
//! ### Clip URLs (`https://twitch.tv/{channel}/clip/{slug}`)
//! 1. Resolve clip metadata via a separate `VideoAccessToken_Clip` Gql query.
//! 2. Return available MP4 renditions.
//!
//! ## Supported URL forms
//! - `https://www.twitch.tv/{channel}`
//! - `https://twitch.tv/videos/{vod_id}`
//! - `https://twitch.tv/{channel}/clip/{slug}`
//! - `https://clips.twitch.tv/{slug}`
//!
//! ## Limitations
//! - Subscriber-only streams require a valid OAuth token (not supported here).
//! - DRM-protected streams are not supported.

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

/// Public client ID used by the Twitch embedded player.
/// This is deliberately public — Twitch uses it for unauthenticated playback.
const TWITCH_CLIENT_ID: &str = "kimne78kx3ncx6brgo4mv6wki5h1ko";

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

const GQL_URL: &str = "https://gql.twitch.tv/gql";

// ── URL kind ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum TwitchTarget {
    /// Live channel stream.
    Channel(String),
    /// Recorded VOD.
    Vod(String),
    /// Short clip.
    Clip(String),
}

fn parse_twitch_url(url: &str) -> Option<TwitchTarget> {
    let parsed = url::Url::parse(url).ok()?;
    let host = parsed.host_str()?;

    // clips.twitch.tv/{slug}
    if host == "clips.twitch.tv" {
        let slug = parsed.path_segments()?.find(|s| !s.is_empty())?.to_string();
        return Some(TwitchTarget::Clip(slug));
    }

    if !host.contains("twitch.tv") {
        return None;
    }

    let segments: Vec<&str> = parsed.path_segments()?.filter(|s| !s.is_empty()).collect();

    match segments.as_slice() {
        // /videos/{id}
        ["videos", vod_id] => Some(TwitchTarget::Vod((*vod_id).to_string())),
        // /{channel}/clip/{slug}
        [_channel, "clip", slug] => Some(TwitchTarget::Clip((*slug).to_string())),
        // /{channel}  (live stream)
        [channel] => Some(TwitchTarget::Channel((*channel).to_string())),
        _ => None,
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
    req.headers
        .push(("Client-ID".to_string(), TWITCH_CLIENT_ID.to_string()));
    req.headers
        .push(("Accept".to_string(), "application/json, */*".to_string()));
    for (k, v) in extra_headers {
        req.headers.push((k.to_string(), v.to_string()));
    }
    if let Some(b) = body {
        req.headers
            .push(("Content-Type".to_string(), "application/json".to_string()));
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

async fn gql_post(body: Vec<u8>, client: &Arc<dyn NetworkClient>) -> Result<Value, ExtractorError> {
    let bytes = fetch_bytes(GQL_URL, client, &[], Some(body)).await?;
    serde_json::from_slice(&bytes).map_err(|e| ExtractorError::Parse {
        format: "twitch",
        reason: format!("GQL JSON parse error: {e}"),
    })
}

// ── Access token helpers ───────────────────────────────────────────────────────

/// Fetch a signed playback access token for a **live channel**.
///
/// Returns `(signature, token_value)`.
async fn fetch_channel_token(
    channel: &str,
    client: &Arc<dyn NetworkClient>,
) -> Result<(String, String), ExtractorError> {
    let gql_body = serde_json::json!([{
        "operationName": "PlaybackAccessToken",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "0828119ded1c13477966434e15800ff57ddacf13ba1911c129dc2200705b0712"
            }
        },
        "variables": {
            "isLive": true,
            "login": channel,
            "isVod": false,
            "vodID": "",
            "playerType": "embed"
        }
    }]);

    let body =
        serde_json::to_vec(&gql_body).map_err(|e| ExtractorError::Internal(e.to_string()))?;
    let resp = gql_post(body, client).await?;

    // Response is an array; take first element.
    let elem = resp
        .get(0)
        .or_else(|| resp.as_array().and_then(|a| a.first()))
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitch",
            reason: "empty GQL response array".to_string(),
        })?;

    extract_token_pair(elem, "streamPlaybackAccessToken")
}

/// Fetch a signed playback access token for a **VOD**.
async fn fetch_vod_token(
    vod_id: &str,
    client: &Arc<dyn NetworkClient>,
) -> Result<(String, String), ExtractorError> {
    let gql_body = serde_json::json!([{
        "operationName": "PlaybackAccessToken",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "0828119ded1c13477966434e15800ff57ddacf13ba1911c129dc2200705b0712"
            }
        },
        "variables": {
            "isLive": false,
            "login": "",
            "isVod": true,
            "vodID": vod_id,
            "playerType": "embed"
        }
    }]);

    let body =
        serde_json::to_vec(&gql_body).map_err(|e| ExtractorError::Internal(e.to_string()))?;
    let resp = gql_post(body, client).await?;
    let elem = resp
        .get(0)
        .or_else(|| resp.as_array().and_then(|a| a.first()))
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitch",
            reason: "empty GQL response array for VOD token".to_string(),
        })?;

    extract_token_pair(elem, "videoPlaybackAccessToken")
}

/// Extract `(signature, value)` from a Gql `PlaybackAccessToken` response element.
fn extract_token_pair(elem: &Value, field_name: &str) -> Result<(String, String), ExtractorError> {
    let token_obj = elem
        .pointer(&format!("/data/{field_name}"))
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitch",
            reason: format!("data.{field_name} missing in GQL response"),
        })?;

    let sig = token_obj
        .get("signature")
        .and_then(Value::as_str)
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitch",
            reason: "token.signature missing".to_string(),
        })?
        .to_string();

    let val = token_obj
        .get("value")
        .and_then(Value::as_str)
        .ok_or_else(|| ExtractorError::Parse {
            format: "twitch",
            reason: "token.value missing".to_string(),
        })?
        .to_string();

    Ok((sig, val))
}

// ── Usher URL builders ────────────────────────────────────────────────────────

/// Build the signed Usher HLS URL for a live channel.
fn channel_hls_url(channel: &str, sig: &str, token: &str) -> String {
    let encoded_token = url::form_urlencoded::byte_serialize(token.as_bytes()).collect::<String>();
    format!(
        "https://usher.twitchapps.com/api/channel/hls/{channel}.m3u8\
         ?sig={sig}\
         &token={encoded_token}\
         &allow_source=true\
         &allow_spectre=true\
         &allow_audio_only=true\
         &fast_bread=true\
         &p={p}",
        p = pseudo_random_p()
    )
}

/// Build the signed Usher HLS URL for a VOD.
fn vod_hls_url(vod_id: &str, sig: &str, token: &str) -> String {
    let encoded_token = url::form_urlencoded::byte_serialize(token.as_bytes()).collect::<String>();
    format!(
        "https://usher.twitchapps.com/vod/{vod_id}.m3u8\
         ?sig={sig}\
         &token={encoded_token}\
         &allow_source=true\
         &allow_spectre=true\
         &allow_audio_only=true\
         &p={p}",
        p = pseudo_random_p()
    )
}

/// Generate a pseudo-random `p` parameter (Twitch uses this for CDN load-balancing).
/// We use a hash of the current thread ID since `rand` is not a dependency.
fn pseudo_random_p() -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    (h.finish() & 0xFF_FFFF) as u32
}

// ── Clip extraction ───────────────────────────────────────────────────────────

/// Fetch clip MP4 sources via the `VideoAccessToken_Clip` GQL query.
async fn extract_clip(
    slug: &str,
    source_url: &str,
    client: &Arc<dyn NetworkClient>,
) -> Result<StreamGraph, ExtractorError> {
    let gql_body = serde_json::json!([{
        "operationName": "VideoAccessToken_Clip",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "36b89d2507fce29e5ca551df756d27c1cfe079e2609642b4390aa4c35796eb11"
            }
        },
        "variables": { "slug": slug }
    }]);

    let body =
        serde_json::to_vec(&gql_body).map_err(|e| ExtractorError::Internal(e.to_string()))?;
    let resp = gql_post(body, client).await?;
    let elem = resp
        .get(0)
        .or_else(|| resp.as_array().and_then(|a| a.first()))
        .ok_or(ExtractorError::NoStreams)?;

    let clip = elem
        .pointer("/data/clip")
        .ok_or(ExtractorError::NoStreams)?;

    let title = clip
        .get("title")
        .and_then(Value::as_str)
        .map(str::to_string);

    let video_qualities = clip
        .get("videoQualities")
        .and_then(Value::as_array)
        .ok_or(ExtractorError::NoStreams)?;

    if video_qualities.is_empty() {
        return Err(ExtractorError::NoStreams);
    }

    let mut variants: Vec<StreamVariant> = video_qualities
        .iter()
        .filter_map(|q| {
            let quality = q.get("quality").and_then(Value::as_str)?;
            let url = q.get("sourceURL").and_then(Value::as_str)?;
            if url.is_empty() {
                return None;
            }
            let frame_rate = q
                .get("frameRate")
                .and_then(Value::as_f64)
                .unwrap_or(30.0)
                .round() as u32;

            let (bps, resolution) = quality_to_metadata(quality);

            Some(StreamVariant {
                kind: StreamKind::Video,
                label: format!("{quality}p{frame_rate} [mp4]"),
                bandwidth_bps: bps,
                resolution,
                codecs: vec!["avc1".to_string()],
                playlist_url: url.to_string(),
                segments: vec![],
                associated_tracks: vec![],
                is_default: false,
            })
        })
        .collect();

    if variants.is_empty() {
        return Err(ExtractorError::NoStreams);
    }

    variants.sort_unstable_by(|a, b| b.bandwidth_bps.cmp(&a.bandwidth_bps));
    if let Some(first) = variants.first_mut() {
        first.is_default = true;
    }

    Ok(StreamGraph {
        is_live: false,
        target_duration_secs: None,
        source_url: source_url.to_string(),
        title,
        variants,
        standalone_tracks: vec![],
        format: "twitch-clip".to_string(),
    })
}

/// Convert a Twitch quality label string to approximate bandwidth + resolution.
fn quality_to_metadata(quality: &str) -> (u64, Option<(u32, u32)>) {
    match quality {
        "1080" | "1080p60" => (6_000_000, Some((1920, 1080))),
        "720" | "720p60" => (3_000_000, Some((1280, 720))),
        "480" => (1_500_000, Some((854, 480))),
        "360" => (800_000, Some((640, 360))),
        "160" => (300_000, Some((284, 160))),
        _ => (1_000_000, None),
    }
}

// ── TwitchExtractor ───────────────────────────────────────────────────────────

pub struct TwitchExtractor;

impl Default for TwitchExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl TwitchExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MediaExtractor for TwitchExtractor {
    fn name(&self) -> &'static str {
        "Twitch"
    }

    fn can_handle(&self, url: &str) -> bool {
        url.contains("twitch.tv/") || url.contains("clips.twitch.tv/")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        let target =
            parse_twitch_url(url).ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;

        match target {
            // ── Live channel ──────────────────────────────────────────────────
            TwitchTarget::Channel(ref channel) => {
                debug!(channel, "extracting Twitch live stream");
                let (sig, token) = fetch_channel_token(channel, &client).await?;
                let hls_url = channel_hls_url(channel, &sig, &token);
                debug!(hls_url, "built Twitch channel HLS URL");

                Ok(StreamGraph {
                    is_live: false,
                    target_duration_secs: None,
                    source_url: url.to_string(),
                    title: Some(format!("{channel} (live)")),
                    variants: vec![StreamVariant {
                        kind: StreamKind::Video,
                        label: "HLS (adaptive, live)".to_string(),
                        bandwidth_bps: 0,
                        resolution: None,
                        codecs: vec![],
                        playlist_url: hls_url,
                        segments: vec![],
                        associated_tracks: vec![],
                        is_default: true,
                    }],
                    standalone_tracks: vec![],
                    format: "twitch-live".to_string(),
                })
            }

            // ── VOD ───────────────────────────────────────────────────────────
            TwitchTarget::Vod(ref vod_id) => {
                debug!(vod_id, "extracting Twitch VOD");
                let (sig, token) = fetch_vod_token(vod_id, &client).await?;
                let hls_url = vod_hls_url(vod_id, &sig, &token);
                debug!(hls_url, "built Twitch VOD HLS URL");

                // Fetch VOD metadata for title
                let title = fetch_vod_title(vod_id, &client).await;

                Ok(StreamGraph {
                    is_live: false,
                    target_duration_secs: None,
                    source_url: url.to_string(),
                    title,
                    variants: vec![StreamVariant {
                        kind: StreamKind::Video,
                        label: "HLS (adaptive, VOD)".to_string(),
                        bandwidth_bps: 0,
                        resolution: None,
                        codecs: vec![],
                        playlist_url: hls_url,
                        segments: vec![],
                        associated_tracks: vec![],
                        is_default: true,
                    }],
                    standalone_tracks: vec![],
                    format: "twitch-vod".to_string(),
                })
            }

            // ── Clip ──────────────────────────────────────────────────────────
            TwitchTarget::Clip(ref slug) => {
                debug!(slug, "extracting Twitch clip");
                extract_clip(slug, url, &client).await
            }
        }
    }
}

/// Attempt to fetch the VOD title via GQL `VideoMetadata` query.
/// Returns `None` on any failure (title is optional).
async fn fetch_vod_title(vod_id: &str, client: &Arc<dyn NetworkClient>) -> Option<String> {
    let gql_body = serde_json::json!([{
        "operationName": "VideoMetadata",
        "extensions": {
            "persistedQuery": {
                "version": 1,
                "sha256Hash": "49b5b8f268cdeb259d75b58dcb0c1a748e3b575003448a2333dc5cdafd49adad"
            }
        },
        "variables": {
            "channelLogin": "",
            "videoID": vod_id
        }
    }]);

    let body = serde_json::to_vec(&gql_body).ok()?;
    let resp = gql_post(body, client).await.ok()?;
    resp.get(0)
        .or_else(|| resp.as_array().and_then(|a| a.first()))
        .and_then(|e| e.pointer("/data/video/title"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_channel_url() {
        match parse_twitch_url("https://www.twitch.tv/ninja") {
            Some(TwitchTarget::Channel(c)) => assert_eq!(c, "ninja"),
            other => panic!("expected Channel, got {other:?}"),
        }
    }

    #[test]
    fn parses_vod_url() {
        match parse_twitch_url("https://www.twitch.tv/videos/123456789") {
            Some(TwitchTarget::Vod(id)) => assert_eq!(id, "123456789"),
            other => panic!("expected Vod, got {other:?}"),
        }
    }

    #[test]
    fn parses_clip_via_channel_path() {
        match parse_twitch_url("https://www.twitch.tv/ninja/clip/SomeClugSlug") {
            Some(TwitchTarget::Clip(slug)) => assert_eq!(slug, "SomeClugSlug"),
            other => panic!("expected Clip, got {other:?}"),
        }
    }

    #[test]
    fn parses_clip_via_clips_subdomain() {
        match parse_twitch_url("https://clips.twitch.tv/AmazingSlipperyCheddar") {
            Some(TwitchTarget::Clip(slug)) => assert_eq!(slug, "AmazingSlipperyCheddar"),
            other => panic!("expected Clip, got {other:?}"),
        }
    }

    #[test]
    fn rejects_non_twitch_url() {
        assert!(parse_twitch_url("https://youtube.com/watch?v=abc").is_none());
    }

    #[test]
    fn quality_metadata_1080() {
        let (bps, res) = quality_to_metadata("1080");
        assert_eq!(bps, 6_000_000);
        assert_eq!(res, Some((1920, 1080)));
    }

    #[test]
    fn quality_metadata_unknown_returns_default() {
        let (bps, res) = quality_to_metadata("chunked");
        assert!(bps > 0);
        assert!(res.is_none());
    }

    #[test]
    fn can_handle_all_twitch_forms() {
        let e = TwitchExtractor::new();
        assert!(e.can_handle("https://www.twitch.tv/ninja"));
        assert!(e.can_handle("https://twitch.tv/videos/123"));
        assert!(e.can_handle("https://clips.twitch.tv/SomeSlug"));
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
    }
}
