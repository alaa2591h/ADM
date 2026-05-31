//! `GenericHlsExtractor` — production-grade HLS / M3U8 manifest parser.
//!
//! ## Features implemented (P7)
//!
//! - Master playlists (`#EXT-X-STREAM-INF`) — multi-variant quality selection
//! - Media playlists (`#EXTINF`) — VOD *and* Live
//! - `#EXT-X-MEDIA` — alternate audio / subtitle tracks
//! - `#EXT-X-BYTERANGE` — byte-range segments inside a single container file
//! - `#EXT-X-KEY` — AES-128-CBC decryption:
//!   - key URL resolved and fetched **once per unique key URI** (cache avoids
//!     re-fetching the same key for every segment in a block)
//!   - IV from `#EXT-X-KEY IV=0x…` attribute, or derived from segment sequence
//!     number when the IV attribute is absent (per RFC 8216 §5.2)
//!   - resulting 16-byte key stored in `SegmentInfo::encryption_key_hex` so the
//!     worker skips the runtime key-fetch entirely
//! - `#EXT-X-DISCONTINUITY` — sets `SegmentInfo::discontinuity = true` on the
//!   segment immediately following the tag; the muxer/post-processor uses this
//!   to insert a presentation-time boundary reset
//! - `#EXT-X-TARGETDURATION` — propagated to `StreamGraph::target_duration_secs`
//! - `#EXT-X-ENDLIST` detection — sets `StreamGraph::is_live = false`; absence
//!   of the tag leaves `is_live = true` (live / event stream)
//! - Live playlist refresh loop:
//!   - polls the playlist URL every `target_duration_secs / 2` seconds (floor: 1 s)
//!   - detects new segments by sequence number
//!   - terminates when `#EXT-X-ENDLIST` appears or the caller cancels
//!   - all newly discovered segments are appended to the variant in order

use crate::{
    error::ExtractorError,
    stream_graph::{MediaTrack, SegmentInfo, StreamGraph, StreamKind, StreamVariant},
    MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tracing::{debug, warn};

// ── URL helpers ───────────────────────────────────────────────────────────────

/// Resolve `raw` against `base`.  If `raw` is already absolute, return it
/// unchanged.  Protocol-relative URLs (`//…`) inherit the base scheme.
fn resolve_url(base: &str, raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return raw.to_string();
    }
    if raw.starts_with("//") {
        let scheme = base.split(':').next().unwrap_or("https");
        return format!("{scheme}:{raw}");
    }
    // Relative URL: strip last path component from base to get the directory.
    let base_dir = base.rfind('/').map_or(base, |i| &base[..=i]);
    format!("{base_dir}{raw}")
}

// ── Network helpers ───────────────────────────────────────────────────────────

async fn fetch_text(url: &str, client: &Arc<dyn NetworkClient>) -> Result<String, ExtractorError> {
    let req = NetworkRequest::new(url, None);
    let mut stream = client
        .execute(req)
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?;

    let mut buf = Vec::new();
    loop {
        match stream.next_chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(e) => return Err(ExtractorError::Network(e.to_string())),
        }
    }

    String::from_utf8(buf).map_err(|e| ExtractorError::Parse {
        format: "hls",
        reason: format!("manifest is not valid UTF-8: {e}"),
    })
}

/// Fetch raw bytes (used for AES key retrieval).
async fn fetch_bytes(
    url: &str,
    client: &Arc<dyn NetworkClient>,
) -> Result<Vec<u8>, ExtractorError> {
    let req = NetworkRequest::new(url, None);
    let mut stream = client
        .execute(req)
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?;

    let mut buf = Vec::new();
    loop {
        match stream.next_chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(e) => return Err(ExtractorError::Network(e.to_string())),
        }
    }
    Ok(buf)
}

// ── Attribute-string tokeniser ────────────────────────────────────────────────

/// Split an HLS attribute string by commas, respecting double-quoted values
/// that may themselves contain commas (e.g. `CODECS="avc1.64001f,mp4a.40.2"`).
fn split_attributes(s: &str) -> Vec<&str> {
    let mut result = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                result.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

// ── Master-playlist helpers ───────────────────────────────────────────────────

fn parse_stream_inf_attributes(attrs: &str, url: &str) -> StreamVariant {
    let mut bandwidth: u64 = 0;
    let mut resolution: Option<(u32, u32)> = None;
    let mut codecs: Vec<String> = Vec::new();

    for pair in split_attributes(attrs) {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        match k.trim() {
            "BANDWIDTH" => bandwidth = v.trim().parse().unwrap_or(0),
            "RESOLUTION" => {
                if let Some((w, h)) = v.trim().split_once('x') {
                    if let (Ok(w), Ok(h)) = (w.parse::<u32>(), h.parse::<u32>()) {
                        resolution = Some((w, h));
                    }
                }
            }
            "CODECS" => {
                let inner = v.trim().trim_matches('"');
                codecs = inner.split(',').map(|s| s.trim().to_string()).collect();
            }
            _ => {}
        }
    }

    let label = resolution.map_or_else(
        || format!("{} kbps", bandwidth / 1000),
        |(_, h)| format!("{h}p"),
    );

    StreamVariant {
        kind: StreamKind::Video,
        label,
        bandwidth_bps: bandwidth,
        resolution,
        codecs,
        playlist_url: url.to_string(),
        segments: Vec::new(),
        associated_tracks: Vec::new(),
        is_default: false,
    }
}

fn parse_media_attributes(attrs: &str, base_url: &str) -> Option<MediaTrack> {
    let mut kind = StreamKind::Unknown;
    let mut language: Option<String> = None;
    let mut label: Option<String> = None;
    let mut uri: Option<String> = None;
    let mut default_track = false;

    for pair in split_attributes(attrs) {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        match k.trim() {
            "TYPE" => {
                kind = match v.trim() {
                    "AUDIO" => StreamKind::Audio,
                    "SUBTITLES" | "CLOSED-CAPTIONS" => StreamKind::Subtitle,
                    _ => StreamKind::Unknown,
                };
            }
            "LANGUAGE" => language = Some(v.trim().trim_matches('"').to_string()),
            "NAME" => label = Some(v.trim().trim_matches('"').to_string()),
            "URI" => uri = Some(v.trim().trim_matches('"').to_string()),
            "DEFAULT" => default_track = v.trim().eq_ignore_ascii_case("YES"),
            _ => {}
        }
    }

    let url = match uri {
        Some(ref raw) => resolve_url(base_url, raw),
        None => return None,
    };

    Some(MediaTrack {
        kind,
        language,
        label,
        url,
        default_track,
    })
}

// ── AES-128 key cache ─────────────────────────────────────────────────────────

/// Per-extraction key cache: maps key URL → 32-character lowercase hex string.
///
/// The HLS spec allows hundreds of segments to share one key URI; without
/// caching every segment would trigger a separate key-server round-trip.
type KeyCache = HashMap<String, String>;

/// Fetch an AES-128 key and return it as a lowercase hex string.
/// Returns the cached value if the URL has already been fetched.
async fn fetch_aes_key(
    key_url: &str,
    client: &Arc<dyn NetworkClient>,
    cache: &mut KeyCache,
) -> Option<String> {
    if let Some(cached) = cache.get(key_url) {
        return Some(cached.clone());
    }

    match fetch_bytes(key_url, client).await {
        Ok(bytes) if bytes.len() >= 16 => {
            let hex = hex_encode(&bytes[..16]);
            cache.insert(key_url.to_string(), hex.clone());
            debug!(key_url, "HLS: fetched and cached AES-128 key");
            Some(hex)
        }
        Ok(bytes) => {
            warn!(
                key_url,
                len = bytes.len(),
                "HLS: AES key response too short, expected 16 bytes"
            );
            None
        }
        Err(e) => {
            warn!(key_url, error = %e, "HLS: failed to fetch AES-128 key");
            None
        }
    }
}

/// Encode a byte slice as a lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ── Media-playlist state machine ──────────────────────────────────────────────

/// Parsed output of a single media-playlist pass.
#[derive(Debug)]
struct PlaylistPass {
    segments: Vec<SegmentInfo>,
    has_end_list: bool,
    target_duration: Option<u32>,
    /// Largest sequence number seen in this pass (used to detect new segments
    /// in subsequent live-refresh passes).
    max_sequence: u64,
}

/// Parse a media playlist body into segments.
///
/// `key_cache` is shared across calls in live mode so keys are not re-fetched
/// across refresh cycles.
async fn parse_media_playlist(
    body: &str,
    base_url: &str,
    client: &Arc<dyn NetworkClient>,
    key_cache: &mut KeyCache,
) -> PlaylistPass {
    let mut segments: Vec<SegmentInfo> = Vec::new();
    let mut current_sequence: u64 = 0;
    let mut pending_duration: Option<f64> = None;
    let mut pending_byte_range: Option<(u64, u64)> = None;
    let mut prev_byte_range_end: u64 = 0;
    let mut has_end_list = false;
    let mut target_duration: Option<u32> = None;
    let mut pending_discontinuity = false;

    // Current encryption context — updated by every #EXT-X-KEY tag
    let mut enc_key_url: Option<String> = None;
    let mut enc_key_hex: Option<String> = None;
    let mut enc_iv: Option<String> = None; // explicit IV from IV= attribute

    for raw_line in body.lines() {
        let line = raw_line.trim();

        // ── Sequence number ───────────────────────────────────────────────
        if let Some(stripped) = line.strip_prefix("#EXT-X-MEDIA-SEQUENCE:") {
            if let Ok(n) = stripped.parse::<u64>() {
                current_sequence = n;
            }
            continue;
        }

        // ── Target duration ───────────────────────────────────────────────
        if let Some(stripped) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
            target_duration = stripped.trim().parse().ok();
            continue;
        }

        // ── End-of-playlist ───────────────────────────────────────────────
        if line == "#EXT-X-ENDLIST" {
            has_end_list = true;
            continue;
        }

        // ── Discontinuity ─────────────────────────────────────────────────
        if line == "#EXT-X-DISCONTINUITY" {
            pending_discontinuity = true;
            continue;
        }

        // ── Segment duration ──────────────────────────────────────────────
        if let Some(after) = line.strip_prefix("#EXTINF:") {
            let dur_str = after.split(',').next().unwrap_or("0");
            pending_duration = dur_str.parse::<f64>().ok();
            continue;
        }

        // ── Byte range ────────────────────────────────────────────────────
        if let Some(spec) = line.strip_prefix("#EXT-X-BYTERANGE:") {
            if let Some((len_s, off_s)) = spec.split_once('@') {
                if let (Ok(len), Ok(off)) = (len_s.parse::<u64>(), off_s.parse::<u64>()) {
                    pending_byte_range = Some((off, off + len - 1));
                    prev_byte_range_end = off + len;
                }
            } else if let Ok(len) = spec.parse::<u64>() {
                pending_byte_range = Some((prev_byte_range_end, prev_byte_range_end + len - 1));
                prev_byte_range_end += len;
            }
            continue;
        }

        // ── Encryption key ────────────────────────────────────────────────
        if let Some(attrs) = line.strip_prefix("#EXT-X-KEY:") {
            let mut method = String::new();
            let mut uri: Option<String> = None;
            let mut iv: Option<String> = None;

            for pair in split_attributes(attrs) {
                let Some((k, v)) = pair.split_once('=') else {
                    continue;
                };
                match k.trim() {
                    "METHOD" => method = v.trim().to_string(),
                    "URI" => uri = Some(v.trim().trim_matches('"').to_string()),
                    "IV" => iv = Some(v.trim().to_string()),
                    _ => {}
                }
            }

            if method == "NONE" {
                // Clear any active encryption context
                enc_key_url = None;
                enc_key_hex = None;
                enc_iv = None;
            } else if method == "AES-128" {
                enc_iv = iv; // may be None — fallback to sequence-number IV
                if let Some(raw_uri) = uri {
                    let resolved = resolve_url(base_url, &raw_uri);
                    // Only fetch key if the URL changed from the previous one
                    if enc_key_url.as_deref() != Some(&resolved) {
                        enc_key_hex = fetch_aes_key(&resolved, client, key_cache).await;
                        enc_key_url = Some(resolved);
                    }
                    // else: same URL → keep cached enc_key_hex
                } else {
                    warn!("HLS: AES-128 key tag has no URI — ignoring");
                }
            } else {
                // SAMPLE-AES and other methods: pass key URL to worker for
                // platform-specific handling; do not attempt to pre-fetch.
                enc_iv = iv;
                if let Some(raw_uri) = uri {
                    enc_key_url = Some(resolve_url(base_url, &raw_uri));
                    enc_key_hex = None;
                }
            }
            continue;
        }

        // ── Skip other tags and blank lines ───────────────────────────────
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // ── Segment URL (non-tag, non-empty line) ─────────────────────────
        if let Some(duration) = pending_duration.take() {
            let url = resolve_url(base_url, line);

            // Derive IV: explicit attribute takes precedence; fall back to the
            // zero-padded big-endian sequence number (RFC 8216 §5.2).
            let effective_iv = enc_iv.clone().or_else(|| {
                if enc_key_hex.is_some() || enc_key_url.is_some() {
                    // Build 16-byte IV from sequence number: 0x000…0{seq}
                    let mut arr = [0u8; 16];
                    let seq_bytes = current_sequence.to_be_bytes();
                    arr[8..16].copy_from_slice(&seq_bytes);
                    Some(format!("0x{}", hex_encode(&arr)))
                } else {
                    None
                }
            });

            segments.push(SegmentInfo {
                url,
                sequence: current_sequence,
                duration_secs: duration,
                byte_range: pending_byte_range.take(),
                encryption_key_url: enc_key_url.clone(),
                encryption_iv: effective_iv,
                encryption_key_hex: enc_key_hex.clone(),
                discontinuity: std::mem::take(&mut pending_discontinuity),
            });

            current_sequence += 1;
        } else {
            warn!(
                "HLS: segment URL without preceding #EXTINF, skipping: {}",
                line
            );
        }
    }

    let max_sequence = segments
        .iter()
        .map(|s| s.sequence)
        .max()
        .unwrap_or(current_sequence.saturating_sub(1));

    PlaylistPass {
        segments,
        has_end_list,
        target_duration,
        max_sequence,
    }
}

// ── Live HLS polling ──────────────────────────────────────────────────────────

/// Refresh a live playlist until `#EXT-X-ENDLIST` appears, appending newly
/// discovered segments to `existing`.
///
/// Polls every `floor(target_duration / 2)` seconds (minimum 1 s, per RFC 8216
/// §6.3.4 recommendation).  Segments with a sequence number ≤ `last_seq` are
/// skipped to avoid duplicates.
async fn poll_live_playlist(
    playlist_url: &str,
    client: &Arc<dyn NetworkClient>,
    mut last_seq: u64,
    target_secs: u32,
    key_cache: &mut KeyCache,
    all_segments: &mut Vec<SegmentInfo>,
) {
    // Poll interval = max(1, floor(target_duration / 2)) seconds
    let poll_secs = std::cmp::max(1, target_secs / 2);
    let poll_interval = Duration::from_secs(u64::from(poll_secs));

    debug!(
        playlist_url,
        poll_interval_secs = poll_secs,
        "HLS live: starting polling loop"
    );

    loop {
        tokio::time::sleep(poll_interval).await;

        let body = match fetch_text(playlist_url, client).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    playlist_url,
                    error = %e,
                    "HLS live: refresh fetch failed, retrying"
                );
                continue;
            }
        };

        let pass = parse_media_playlist(&body, playlist_url, client, key_cache).await;

        // Append only segments we have not seen yet
        let new_segs: Vec<SegmentInfo> = pass
            .segments
            .into_iter()
            .filter(|s| s.sequence > last_seq)
            .collect();

        if !new_segs.is_empty() {
            debug!(
                count = new_segs.len(),
                max_seq = pass.max_sequence,
                "HLS live: appended new segments"
            );
            last_seq = pass.max_sequence;
            all_segments.extend(new_segs);
        }

        if pass.has_end_list {
            debug!(playlist_url, "HLS live: #EXT-X-ENDLIST detected — done");
            break;
        }
    }
}

// ── GenericHlsExtractor ───────────────────────────────────────────────────────

pub struct GenericHlsExtractor;

impl GenericHlsExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    fn looks_like_hls(url: &str) -> bool {
        let lower = url.to_ascii_lowercase();
        let path = lower.split('?').next().unwrap_or(&lower);
        let path_ref = std::path::Path::new(path);
        path_ref
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("m3u8") || ext.eq_ignore_ascii_case("m3u"))
    }

    async fn parse_master(
        &self,
        body: &str,
        base_url: &str,
        client: &Arc<dyn NetworkClient>,
        key_cache: &mut KeyCache,
    ) -> Result<(Vec<StreamVariant>, Vec<MediaTrack>), ExtractorError> {
        let mut variants: Vec<StreamVariant> = Vec::new();
        let mut tracks: Vec<MediaTrack> = Vec::new();
        let mut pending_stream_inf: Option<String> = None;

        for raw_line in body.lines() {
            let line = raw_line.trim();

            if let Some(stripped) = line.strip_prefix("#EXT-X-STREAM-INF:") {
                pending_stream_inf = Some(stripped.to_string());
                continue;
            }

            if let Some(stripped) = line.strip_prefix("#EXT-X-MEDIA:") {
                if let Some(track) = parse_media_attributes(stripped, base_url) {
                    tracks.push(track);
                }
                continue;
            }

            if line.starts_with('#') || line.is_empty() {
                if !line.starts_with("#EXT-X-STREAM-INF:") {
                    pending_stream_inf = None;
                }
                continue;
            }

            if let Some(attrs) = pending_stream_inf.take() {
                let playlist_url = resolve_url(base_url, line);
                let mut variant = parse_stream_inf_attributes(&attrs, &playlist_url);

                debug!(url = %playlist_url, "HLS: fetching variant playlist");
                match fetch_text(&playlist_url, client).await {
                    Ok(variant_body) => {
                        let pass =
                            parse_media_playlist(&variant_body, &playlist_url, client, key_cache)
                                .await;

                        // Live variant: poll until EXT-X-ENDLIST
                        let mut all_segs = pass.segments;
                        if !pass.has_end_list {
                            let target = pass.target_duration.unwrap_or(6);
                            poll_live_playlist(
                                &playlist_url,
                                client,
                                pass.max_sequence,
                                target,
                                key_cache,
                                &mut all_segs,
                            )
                            .await;
                        }

                        debug!(
                            segments = all_segs.len(),
                            url = %playlist_url,
                            "HLS: parsed variant"
                        );
                        variant.segments = all_segs;
                    }
                    Err(e) => {
                        warn!(url = %playlist_url, error = %e, "HLS: failed to fetch variant playlist");
                    }
                }

                variants.push(variant);
            }
        }

        Ok((variants, tracks))
    }
}

impl Default for GenericHlsExtractor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MediaExtractor for GenericHlsExtractor {
    fn name(&self) -> &'static str {
        "GenericHLS"
    }

    fn can_handle(&self, url: &str) -> bool {
        Self::looks_like_hls(url)
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        debug!(url, "HLS: fetching manifest");
        let body = fetch_text(url, &client).await?;

        let is_master = body.contains("#EXT-X-STREAM-INF:");

        let mut graph = StreamGraph::new(url, "hls");
        // Shared key cache for the entire extraction (master + all variants)
        let mut key_cache: KeyCache = HashMap::new();

        if is_master {
            debug!(url, "HLS: detected master playlist");
            let (mut variants, tracks) = self
                .parse_master(&body, url, &client, &mut key_cache)
                .await?;

            if variants.is_empty() {
                return Err(ExtractorError::NoStreams);
            }

            variants.sort_unstable_by_key(|b| std::cmp::Reverse(b.bandwidth_bps));
            if let Some(first) = variants.first_mut() {
                first.is_default = true;
            }

            graph.variants = variants;
            graph.standalone_tracks = tracks;
        } else {
            debug!(url, "HLS: detected media playlist (single variant)");
            let pass = parse_media_playlist(&body, url, &client, &mut key_cache).await;

            graph.target_duration_secs = pass.target_duration;
            graph.is_live = !pass.has_end_list;

            let mut all_segs = pass.segments;

            if graph.is_live {
                let target = pass.target_duration.unwrap_or(6);
                poll_live_playlist(
                    url,
                    &client,
                    pass.max_sequence,
                    target,
                    &mut key_cache,
                    &mut all_segs,
                )
                .await;
                graph.is_live = false; // settled after poll loop finishes
            }

            if all_segs.is_empty() {
                return Err(ExtractorError::NoStreams);
            }

            let duration: f64 = all_segs.iter().map(|s| s.duration_secs).sum();
            let has_encrypted = all_segs.iter().any(|s| s.encryption_key_hex.is_some());
            let has_discontinuity = all_segs.iter().any(|s| s.discontinuity);

            debug!(
                segments = all_segs.len(),
                duration,
                encrypted = has_encrypted,
                has_discontinuity,
                url,
                "HLS: single variant ready"
            );

            let variant = StreamVariant {
                kind: StreamKind::Video,
                label: "default".to_string(),
                bandwidth_bps: 0,
                resolution: None,
                codecs: Vec::new(),
                playlist_url: url.to_string(),
                segments: all_segs,
                associated_tracks: Vec::new(),
                is_default: true,
            };
            graph.variants.push(variant);
        }

        Ok(graph)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use adm_network::MockNetworkClient;
    use std::sync::Arc;

    fn mock_client(data: &[u8]) -> Arc<dyn NetworkClient> {
        Arc::new(MockNetworkClient {
            data: data.to_vec(),
            chunk_size: 4096,
            fail_at: None,
        })
    }

    // ── URL helpers ───────────────────────────────────────────────────────

    #[test]
    fn can_handle_m3u8_url() {
        let ext = GenericHlsExtractor::new();
        assert!(ext.can_handle("https://cdn.example.com/stream/index.m3u8"));
        assert!(ext.can_handle("https://cdn.example.com/live.m3u8?token=abc"));
        assert!(ext.can_handle("https://cdn.example.com/playlist.m3u"));
        assert!(!ext.can_handle("https://cdn.example.com/video.mp4"));
        assert!(!ext.can_handle("https://cdn.example.com/manifest.mpd"));
    }

    #[test]
    fn resolve_url_absolute_passthrough() {
        let r = resolve_url(
            "https://cdn.example.com/a/b.m3u8",
            "https://other.com/seg.ts",
        );
        assert_eq!(r, "https://other.com/seg.ts");
    }

    #[test]
    fn resolve_url_relative() {
        let r = resolve_url("https://cdn.example.com/a/b.m3u8", "seg001.ts");
        assert_eq!(r, "https://cdn.example.com/a/seg001.ts");
    }

    #[test]
    fn resolve_url_protocol_relative() {
        let r = resolve_url("https://cdn.example.com/a.m3u8", "//other.com/seg.ts");
        assert_eq!(r, "https://other.com/seg.ts");
    }

    #[test]
    fn split_attributes_handles_quoted_commas() {
        let parts = split_attributes(
            r#"BANDWIDTH=1280000,CODECS="avc1.64001f,mp4a.40.2",RESOLUTION=1280x720"#,
        );
        assert_eq!(parts.len(), 3);
        assert!(parts[1].contains("avc1.64001f,mp4a.40.2"));
    }

    // ── Basic media-playlist parsing ──────────────────────────────────────

    #[tokio::test]
    async fn parse_simple_vod_playlist() {
        let body = "\
#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:10\n#EXT-X-MEDIA-SEQUENCE:0\n\
#EXTINF:9.009,\nseg000.ts\n#EXTINF:9.009,\nseg001.ts\n#EXTINF:3.003,\nseg002.ts\n\
#EXT-X-ENDLIST\n";
        let client = mock_client(&[]);
        let mut kc = KeyCache::new();
        let pass = parse_media_playlist(
            body,
            "https://cdn.example.com/playlist.m3u8",
            &client,
            &mut kc,
        )
        .await;

        assert!(pass.has_end_list);
        assert_eq!(pass.segments.len(), 3);
        assert_eq!(pass.segments[0].url, "https://cdn.example.com/seg000.ts");
        assert_eq!(pass.segments[1].sequence, 1);
        assert!((pass.segments[2].duration_secs - 3.003).abs() < 1e-6);
        assert_eq!(pass.target_duration, Some(10));
    }

    #[tokio::test]
    async fn parse_playlist_without_endlist_is_live() {
        let body = "\
#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:100\n\
#EXTINF:6.0,\nlive0.ts\n#EXTINF:6.0,\nlive1.ts\n";
        let client = mock_client(&[]);
        let mut kc = KeyCache::new();
        let pass =
            parse_media_playlist(body, "https://cdn.example.com/live.m3u8", &client, &mut kc).await;

        assert!(!pass.has_end_list);
        assert_eq!(pass.segments.len(), 2);
        assert_eq!(pass.segments[0].sequence, 100);
        assert_eq!(pass.target_duration, Some(6));
    }

    // ── AES-128 key pre-fetch ─────────────────────────────────────────────

    #[tokio::test]
    async fn aes_key_prefetched_into_encryption_key_hex() {
        // MockNetworkClient returns the same bytes for every request, so our
        // 16-byte AES key = [0x00, 0x01, …, 0x0F]
        let key_bytes: Vec<u8> = (0x00u8..0x10).collect();
        let client = mock_client(&key_bytes);
        let mut kc = KeyCache::new();

        let body = format!(
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/key1\"\n\
#EXTINF:6.0,\nenc0.ts\n#EXTINF:6.0,\nenc1.ts\n#EXT-X-ENDLIST\n"
        );

        let pass =
            parse_media_playlist(&body, "https://cdn.example.com/enc.m3u8", &client, &mut kc).await;

        assert_eq!(pass.segments.len(), 2);

        // Key should be pre-fetched
        let hex = pass.segments[0].encryption_key_hex.as_deref().unwrap();
        assert_eq!(hex, "000102030405060708090a0b0c0d0e0f");

        // Both segments share the same key hex
        assert_eq!(
            pass.segments[0].encryption_key_hex,
            pass.segments[1].encryption_key_hex
        );

        // Key should have been cached — only one entry in the cache
        assert_eq!(kc.len(), 1);
    }

    #[tokio::test]
    async fn explicit_iv_attribute_is_preserved() {
        let key_bytes: Vec<u8> = (0x00u8..0x10).collect();
        let client = mock_client(&key_bytes);
        let mut kc = KeyCache::new();

        let body =
            "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\",IV=0x1234567890abcdef1234567890abcdef\n\
#EXTINF:6.0,\nenc0.ts\n#EXT-X-ENDLIST\n";

        let pass =
            parse_media_playlist(body, "https://cdn.example.com/enc.m3u8", &client, &mut kc).await;

        assert_eq!(
            pass.segments[0].encryption_iv.as_deref(),
            Some("0x1234567890abcdef1234567890abcdef")
        );
    }

    #[tokio::test]
    async fn sequence_derived_iv_when_no_iv_attribute() {
        let key_bytes: Vec<u8> = (0x00u8..0x10).collect();
        let client = mock_client(&key_bytes);
        let mut kc = KeyCache::new();

        let body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:7\n\
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"\n\
#EXTINF:6.0,\nenc0.ts\n#EXT-X-ENDLIST\n";

        let pass =
            parse_media_playlist(body, "https://cdn.example.com/enc.m3u8", &client, &mut kc).await;

        // Sequence = 7 → IV = 0x00000000000000000000000000000007
        let iv = pass.segments[0].encryption_iv.as_deref().unwrap();
        assert!(iv.ends_with("07"), "IV should end in 07, got: {iv}");
    }

    #[tokio::test]
    async fn key_none_clears_encryption_context() {
        let key_bytes: Vec<u8> = (0x00u8..0x10).collect();
        let client = mock_client(&key_bytes);
        let mut kc = KeyCache::new();

        let body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/k\"\n\
#EXTINF:6.0,\nenc0.ts\n\
#EXT-X-KEY:METHOD=NONE\n\
#EXTINF:6.0,\nclear1.ts\n#EXT-X-ENDLIST\n";

        let pass =
            parse_media_playlist(body, "https://cdn.example.com/mixed.m3u8", &client, &mut kc)
                .await;

        assert!(pass.segments[0].encryption_key_hex.is_some());
        assert!(pass.segments[1].encryption_key_hex.is_none());
        assert!(pass.segments[1].encryption_key_url.is_none());
        assert!(pass.segments[1].encryption_iv.is_none());
    }

    // ── Discontinuity ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn discontinuity_tag_sets_flag_on_next_segment() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXTINF:6.0,\nseg0.ts\n\
#EXT-X-DISCONTINUITY\n\
#EXTINF:6.0,\nseg1.ts\n\
#EXTINF:6.0,\nseg2.ts\n\
#EXT-X-ENDLIST\n";

        let client = mock_client(&[]);
        let mut kc = KeyCache::new();
        let pass =
            parse_media_playlist(body, "https://cdn.example.com/disc.m3u8", &client, &mut kc).await;

        assert!(
            !pass.segments[0].discontinuity,
            "seg0 should not be a boundary"
        );
        assert!(
            pass.segments[1].discontinuity,
            "seg1 should be marked as discontinuity boundary"
        );
        assert!(
            !pass.segments[2].discontinuity,
            "seg2 should not be a boundary"
        );
    }

    #[tokio::test]
    async fn multiple_discontinuities_are_each_set_once() {
        let body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXTINF:6.0,\na.ts\n\
#EXT-X-DISCONTINUITY\n\
#EXTINF:6.0,\nb.ts\n\
#EXTINF:6.0,\nc.ts\n\
#EXT-X-DISCONTINUITY\n\
#EXTINF:6.0,\nd.ts\n\
#EXT-X-ENDLIST\n";

        let client = mock_client(&[]);
        let mut kc = KeyCache::new();
        let pass =
            parse_media_playlist(body, "https://cdn.example.com/multi.m3u8", &client, &mut kc)
                .await;

        let disc_indices: Vec<usize> = pass
            .segments
            .iter()
            .enumerate()
            .filter(|(_, s)| s.discontinuity)
            .map(|(i, _)| i)
            .collect();

        assert_eq!(disc_indices, vec![1, 3]);
    }

    // ── Byte-range ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn byterange_explicit_offsets() {
        let body = "#EXTM3U\n#EXT-X-VERSION:4\n#EXT-X-TARGETDURATION:10\n\
#EXTINF:10.0,\n#EXT-X-BYTERANGE:100000@0\nfile.mp4\n\
#EXTINF:10.0,\n#EXT-X-BYTERANGE:100000@100000\nfile.mp4\n#EXT-X-ENDLIST\n";

        let client = mock_client(&[]);
        let mut kc = KeyCache::new();
        let pass =
            parse_media_playlist(body, "https://cdn.example.com/byte.m3u8", &client, &mut kc).await;

        assert_eq!(pass.segments.len(), 2);
        assert_eq!(pass.segments[0].byte_range, Some((0, 99_999)));
        assert_eq!(pass.segments[1].byte_range, Some((100_000, 199_999)));
    }

    // ── Full extraction pipeline ──────────────────────────────────────────

    #[tokio::test]
    async fn extract_single_variant_vod() {
        let playlist = b"#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:0\n\
#EXTINF:6.0,\nseg000.ts\n#EXTINF:6.0,\nseg001.ts\n#EXT-X-ENDLIST\n";
        let client = mock_client(playlist);
        let ext = GenericHlsExtractor::new();

        let graph = ext
            .extract("https://cdn.example.com/stream.m3u8", client)
            .await
            .unwrap();

        assert_eq!(graph.format, "hls");
        assert_eq!(graph.variants.len(), 1);
        assert!(graph.variants[0].is_default);
        assert_eq!(graph.variants[0].segments.len(), 2);
        assert!((graph.variants[0].total_duration_secs() - 12.0).abs() < 1e-6);
        assert!(!graph.is_live);
    }

    #[tokio::test]
    async fn extract_returns_no_streams_for_empty_playlist() {
        let playlist = b"#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-ENDLIST\n";
        let client = mock_client(playlist);
        let ext = GenericHlsExtractor::new();

        let result = ext
            .extract("https://cdn.example.com/empty.m3u8", client)
            .await;
        assert!(matches!(result, Err(ExtractorError::NoStreams)));
    }

    #[tokio::test]
    async fn unsupported_url_in_registry() {
        use crate::ExtractorRegistry;
        let reg = ExtractorRegistry::with_defaults();
        let client: Arc<dyn NetworkClient> = Arc::new(MockNetworkClient {
            data: vec![],
            chunk_size: 0,
            fail_at: None,
        });
        let result = reg.extract("https://example.com/video.mp4", client).await;
        assert!(matches!(result, Err(ExtractorError::Unsupported(_))));
    }

    // ── Key cache deduplication ───────────────────────────────────────────

    #[tokio::test]
    async fn same_key_url_fetched_only_once() {
        let key_bytes: Vec<u8> = (0x00u8..0x10).collect();
        let client = mock_client(&key_bytes);
        let mut kc = KeyCache::new();

        let body = "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:6\n\
#EXT-X-KEY:METHOD=AES-128,URI=\"https://keys.example.com/shared_key\"\n\
#EXTINF:6.0,\nenc0.ts\n\
#EXTINF:6.0,\nenc1.ts\n\
#EXTINF:6.0,\nenc2.ts\n\
#EXT-X-ENDLIST\n";

        let pass =
            parse_media_playlist(body, "https://cdn.example.com/enc.m3u8", &client, &mut kc).await;

        // All 3 segments should share the same pre-fetched key
        assert_eq!(pass.segments.len(), 3);
        assert!(pass.segments.iter().all(|s| s.encryption_key_hex.is_some()));
        // Only one cache entry
        assert_eq!(kc.len(), 1);
        // All have same hex value
        let expected_hex = pass.segments[0].encryption_key_hex.as_deref().unwrap();
        assert!(pass
            .segments
            .iter()
            .all(|s| s.encryption_key_hex.as_deref() == Some(expected_hex)));
    }

    // ── hex_encode helper ────────────────────────────────────────────────

    #[test]
    fn hex_encode_known_values() {
        assert_eq!(hex_encode(&[0x00, 0x0f, 0xff]), "000fff");
        assert_eq!(hex_encode(&[]), "");
        let seq7: [u8; 16] = {
            let mut a = [0u8; 16];
            a[15] = 7;
            a
        };
        assert!(hex_encode(&seq7).ends_with("07"));
    }
}
