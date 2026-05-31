use crate::{
    stream_graph::{StreamGraph, StreamKind, StreamVariant},
    ExtractorError, MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tracing::debug;

pub struct YouTubeExtractor;

impl Default for YouTubeExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl YouTubeExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    fn extract_video_id(url: &str) -> Option<String> {
        let parsed = url::Url::parse(url).ok()?;
        if parsed.host_str()?.contains("youtube.com") {
            parsed
                .query_pairs()
                .find(|(k, _)| k == "v")
                .map(|(_, v)| v.into_owned())
        } else if parsed.host_str()?.contains("youtu.be") {
            parsed.path_segments()?.next().map(String::from)
        } else {
            None
        }
    }

    async fn call_innertube_player(
        &self,
        video_id: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<Value, ExtractorError> {
        let url = "https://www.youtube.com/youtubei/v1/player";
        let body = serde_json::json!({
            "videoId": video_id,
            "context": {
                "client": {
                    "clientName": "ANDROID",
                    "clientVersion": "17.31.35",
                    "hl": "en",
                    "gl": "US",
                    "androidSdkVersion": 30
                }
            }
        });

        let mut req = NetworkRequest::new(url, None);
        req.headers
            .push(("Content-Type".to_string(), "application/json".to_string()));
        req.body = Some(serde_json::to_vec(&body).unwrap());

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

        serde_json::from_slice(&buf).map_err(|e| ExtractorError::Parse {
            format: "youtube",
            reason: format!("innertube json error: {e}"),
        })
    }
}

fn parse_mime_codecs(mime: &str) -> Vec<String> {
    mime.split(';')
        .find_map(|part| {
            let part = part.trim();
            part.strip_prefix("codecs=").map(|codecs| {
                codecs
                    .trim_matches('"')
                    .split(',')
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
        })
        .unwrap_or_default()
}

fn parse_kind(mime: &str) -> StreamKind {
    let lower = mime.to_ascii_lowercase();
    if lower.contains("video/") {
        StreamKind::Video
    } else if lower.contains("audio/") {
        StreamKind::Audio
    } else {
        StreamKind::Unknown
    }
}

fn format_variant(fmt: &Value) -> StreamVariant {
    let itag = fmt.get("itag").and_then(Value::as_i64).unwrap_or(0);
    let mime = fmt.get("mimeType").and_then(Value::as_str).unwrap_or("");
    let bitrate = fmt.get("bitrate").and_then(Value::as_u64).unwrap_or(0);
    let label = fmt
        .get("qualityLabel")
        .and_then(Value::as_str)
        .map_or_else(|| format!("itag {itag}"), str::to_string);

    // In real YouTube extraction, we'd need to handle 'cipher' or 'signatureCipher'
    // but InnerTube ANDROID client often returns direct URLs.
    let playlist_url = fmt
        .get("url")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    StreamVariant {
        kind: parse_kind(mime),
        label,
        bandwidth_bps: bitrate,
        resolution: None, // Could parse from width/height
        codecs: parse_mime_codecs(mime),
        playlist_url,
        segments: vec![],
        associated_tracks: vec![],
        is_default: false,
    }
}

#[async_trait]
impl MediaExtractor for YouTubeExtractor {
    fn name(&self) -> &'static str {
        "YouTube"
    }
    fn can_handle(&self, url: &str) -> bool {
        url.contains("youtube.com/watch")
            || url.contains("youtu.be/")
            || url.contains("youtube.com/shorts/")
    }
    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        let video_id = Self::extract_video_id(url)
            .ok_or_else(|| ExtractorError::InvalidUrl(url.to_string()))?;
        debug!(video_id, "extracting youtube via innertube");

        let player_response = self.call_innertube_player(&video_id, &client).await?;

        let streaming_data =
            player_response
                .get("streamingData")
                .ok_or_else(|| ExtractorError::Parse {
                    format: "youtube",
                    reason: "streamingData missing".to_string(),
                })?;

        let mut variants = Vec::new();
        if let Some(formats) = streaming_data.get("formats").and_then(Value::as_array) {
            for fmt in formats {
                variants.push(format_variant(fmt));
            }
        }
        if let Some(adaptive_formats) = streaming_data
            .get("adaptiveFormats")
            .and_then(Value::as_array)
        {
            for fmt in adaptive_formats {
                variants.push(format_variant(fmt));
            }
        }

        let title = player_response
            .get("videoDetails")
            .and_then(|v| v.get("title"))
            .and_then(Value::as_str)
            .map(String::from);

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: url.to_string(),
            title,
            variants,
            standalone_tracks: vec![],
            format: "youtube".to_string(),
        })
    }
}
