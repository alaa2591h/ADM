//! `SoundCloudExtractor` — stream extraction for soundcloud.com
//!
//! ## Strategy
//!
//! SoundCloud does **not** expose a public documented API for audio streams.
//! Instead, the web player loads stream URLs via an internal API that requires
//! a rotating `client_id` embedded in one of the JavaScript bundles loaded
//! alongside each page.  This extractor replicates the exact sequence the
//! official web player uses:
//!
//! ### Step 1 — Page hydration
//! Fetch the track page HTML and extract the `window.__sc_hydration` JSON array.
//! This array contains a `"sound"` entry with the full track object (id, title,
//! duration, artwork, waveform URL, and sometimes a `media.transcodings` list).
//!
//! ### Step 2 — Client ID resolution
//! If the hydration data does not include stream transcodings (private tracks,
//! geo-restricted content, etc.), we need a valid `client_id` to call the API.
//! The ID is extracted from the versioned JS bundle referenced in the page HTML:
//! ```
//! <script crossorigin src="https://…/app-{version}-0.js"></script>
//! ```
//! We fetch this bundle and search for the pattern `client_id:"<32-char-hex>"`.
//!
//! ### Step 3 — API resolve (optional)
//! When the hydration JSON lacks a `media.transcodings` list, we call:
//! ```
//! GET https://api-v2.soundcloud.com/resolve?url={track_url}&client_id={id}
//! ```
//! This returns the complete track object including all available transcodings.
//!
//! ### Step 4 — Stream URL resolution
//! Each transcoding has a `url` pointing to:
//! ```
//! GET https://api-v2.soundcloud.com/media/soundcloud:tracks:{id}/{token}/stream/…
//!         ?client_id={id}
//! ```
//! This endpoint returns a one-time-use redirect URL (JSON: `{ "url": "…" }`).
//! We resolve it and store the final CDN URL as the `playlist_url`.
//!
//! ### Supported URL forms
//! - `https://soundcloud.com/{user}/{track-slug}`
//! - `https://soundcloud.com/{user}/{track-slug}/s-{token}` (private share)
//! - `https://on.soundcloud.com/{short-code}` (mobile short-link)
//! - `https://m.soundcloud.com/{user}/{track-slug}` (mobile)
//!
//! ### Not supported (yet)
//! - Playlists / sets (require `resolve` + pagination)
//! - Likes / reposts feeds (require OAuth)
//! - Go+ subscriber-only content behind paywall
//!
//! ## References
//! - <https://developers.soundcloud.com/docs/api/guide>  (public, limited)
//! - Reverse-engineered from SoundCloud web player source (2024-2025)

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

/// Browser-like User-Agent string.  SoundCloud rejects headless/bot UA strings.
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

/// SoundCloud API v2 base URL.
const SC_API_V2: &str = "https://api-v2.soundcloud.com";

/// Pattern prefix used when scanning the JS bundle for the embedded client ID.
/// The format in the bundle is: `client_id:"<HEX32>"`
const CLIENT_ID_PATTERN: &str = "client_id:\"";

/// Minimum length of a plausible SoundCloud `client_id` (hex string).
const CLIENT_ID_MIN_LEN: usize = 20;
/// Maximum length — real IDs are 32 chars, allow some slack.
const CLIENT_ID_MAX_LEN: usize = 64;

/// Maximum bytes to read from the JS bundle when scanning for client_id.
/// The bundle is several MB; the ID appears near the top — we stop early.
const JS_BUNDLE_MAX_BYTES: usize = 3 * 1024 * 1024; // 3 MiB

/// Preferred transcoding format order.  We rank these and pick the best set
/// to expose as `StreamVariant`s.
static FORMAT_PRIORITY: &[&str] = &[
    "hls/mp3",         // HLS MP3 — widest compatibility
    "hls/opus",        // HLS Opus — better quality at low bitrate
    "progressive/mp3", // Direct MP3 download — simplest path
];

// ── HTTP helper ───────────────────────────────────────────────────────────────

/// Fetch up to `max_bytes` from `url` and return as raw bytes.
async fn fetch_bytes(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    referer: Option<&str>,
    max_bytes: Option<usize>,
) -> Result<Vec<u8>, ExtractorError> {
    let mut req = NetworkRequest::new(url, None);
    req.headers
        .push(("User-Agent".to_string(), USER_AGENT.to_string()));
    req.headers.push(("Accept".to_string(), "*/*".to_string()));
    req.headers
        .push(("Accept-Language".to_string(), "en-US,en;q=0.9".to_string()));
    if let Some(ref_url) = referer {
        req.headers
            .push(("Referer".to_string(), ref_url.to_string()));
    }

    let mut stream = client
        .execute(req)
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?;

    let cap = max_bytes.unwrap_or(512 * 1024);
    let mut buf = Vec::with_capacity(cap.min(1024 * 1024));

    while let Some(chunk) = stream
        .next_chunk()
        .await
        .map_err(|e| ExtractorError::Network(e.to_string()))?
    {
        buf.extend_from_slice(&chunk);
        if let Some(limit) = max_bytes {
            if buf.len() >= limit {
                buf.truncate(limit);
                break;
            }
        }
    }

    Ok(buf)
}

/// Fetch `url` and return the body as UTF-8 text.
async fn fetch_text(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    referer: Option<&str>,
) -> Result<String, ExtractorError> {
    let bytes = fetch_bytes(url, client, referer, None).await?;
    String::from_utf8(bytes).map_err(|e| ExtractorError::Parse {
        format: "soundcloud",
        reason: format!("UTF-8 decode error: {e}"),
    })
}

/// Fetch `url` and parse response body as JSON.
async fn fetch_json(
    url: &str,
    client: &Arc<dyn NetworkClient>,
    referer: Option<&str>,
) -> Result<Value, ExtractorError> {
    let text = fetch_text(url, client, referer).await?;
    serde_json::from_str(&text).map_err(|e| ExtractorError::Parse {
        format: "soundcloud",
        reason: format!("JSON parse error at {url}: {e}"),
    })
}

// ── URL normalisation ─────────────────────────────────────────────────────────

/// Normalise a SoundCloud URL to a canonical `https://soundcloud.com/…` form.
///
/// Handles mobile (`m.soundcloud.com`) and short (`on.soundcloud.com`) links.
/// Returns the unchanged URL for everything else.
fn normalise_url(url: &str) -> String {
    // Strip trailing slashes and query params we don't need
    let url = url.trim_end_matches('/');

    if url.contains("m.soundcloud.com") {
        return url.replacen("m.soundcloud.com", "soundcloud.com", 1);
    }

    // on.soundcloud.com short links cannot be resolved without a network round-trip.
    // We return them as-is and let the resolve endpoint handle the redirect.
    url.to_string()
}

/// Quick heuristic: is `url` a single track page (not a set/playlist/user)?
fn looks_like_track_url(url: &str) -> bool {
    // Track URLs have exactly two path segments: /user/slug
    // Sets have /user/sets/slug, likes have /user/likes, etc.
    if let Ok(parsed) = url::Url::parse(url) {
        let segs: Vec<&str> = parsed
            .path_segments()
            .map(|s| s.filter(|p| !p.is_empty()).collect())
            .unwrap_or_default();

        // /user/track-slug  → 2 segments, not "sets" or "likes" or "reposts"
        if segs.len() == 2 {
            let second = segs[1];
            return second != "sets"
                && second != "likes"
                && second != "reposts"
                && second != "albums"
                && second != "tracks";
        }
        // Private share: /user/slug/s-TOKEN → 3 segments where [2] starts with "s-"
        if segs.len() == 3 && segs[2].starts_with("s-") {
            return true;
        }
    }
    false
}

// ── Page HTML parsing ─────────────────────────────────────────────────────────

/// Extract the `window.__sc_hydration` JSON array from the page HTML.
///
/// SoundCloud embeds structured data for the current page in a `<script>` tag:
/// ```html
/// <script>window.__sc_hydration = [...];</script>
/// ```
/// The array contains objects like `{"hydratable":"sound","data":{…}}`.
fn extract_hydration(html: &str) -> Option<Vec<Value>> {
    const MARKER: &str = "window.__sc_hydration = ";
    let start = html.find(MARKER)? + MARKER.len();
    // The array ends just before the `;` that terminates the statement.
    let rest = &html[start..];
    let end = rest.find(";</script>")?;
    let json_src = &rest[..end];
    serde_json::from_str(json_src).ok()
}

/// Find the first `"sound"` entry in the hydration array and return its data.
fn find_track_in_hydration(hydration: &[Value]) -> Option<&Value> {
    hydration.iter().find_map(|entry| {
        if entry.get("hydratable").and_then(Value::as_str) == Some("sound") {
            entry.get("data")
        } else {
            None
        }
    })
}

/// Extract all `<script src="…">` URLs from page HTML that look like the main
/// app bundle (matches `app-XXXXX-0.js` pattern used by SoundCloud).
fn extract_app_bundle_urls(html: &str) -> Vec<String> {
    let mut urls = Vec::new();
    let mut search = html;
    while let Some(pos) = search.find("<script crossorigin src=\"") {
        let rest = &search[pos + "<script crossorigin src=\"".len()..];
        if let Some(end) = rest.find('"') {
            let src = &rest[..end];
            // The main app bundle looks like: https://…/app-a1b2c3-0.js
            if src.contains("/app-") && src.ends_with(".js") {
                urls.push(src.to_string());
            }
        }
        search = &search[pos + 1..];
    }
    urls
}

// ── JS bundle client_id extraction ───────────────────────────────────────────

/// Scan `js_src` (partial JS bundle text) for the embedded `client_id` value.
///
/// The bundle contains lines similar to:
/// ```js
/// r.client_id="a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
/// ```
/// or (in minified form):
/// ```js
/// client_id:"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"
/// ```
fn extract_client_id_from_js(js_src: &str) -> Option<String> {
    // Try both colon and equals forms
    for pattern in &[CLIENT_ID_PATTERN, "client_id=\""] {
        let mut search = js_src;
        while let Some(pos) = search.find(pattern) {
            let rest = &search[pos + pattern.len()..];
            // Read until closing quote
            if let Some(end) = rest.find('"') {
                let candidate = &rest[..end];
                if is_valid_client_id(candidate) {
                    debug!(client_id = candidate, "extracted SoundCloud client_id");
                    return Some(candidate.to_string());
                }
            }
            search = &search[pos + 1..];
        }
    }
    None
}

/// Validate a candidate client_id: must be alphanumeric and correct length.
fn is_valid_client_id(s: &str) -> bool {
    s.len() >= CLIENT_ID_MIN_LEN
        && s.len() <= CLIENT_ID_MAX_LEN
        && s.chars().all(|c| c.is_ascii_alphanumeric())
}

// ── Track metadata parsing ────────────────────────────────────────────────────

/// Parsed transcoding from `media.transcodings[]`.
#[derive(Debug, Clone)]
struct Transcoding {
    /// Stream resolution endpoint URL (not the CDN URL yet).
    stream_url: String,
    /// MIME type string, e.g. `"audio/mpeg"` or `"audio/ogg; codecs=\"opus\""`.
    mime_type: String,
    /// Protocol string, e.g. `"hls"` or `"progressive"`.
    protocol: String,
    /// Snip flag — if `true` this is a 30-second preview only.
    snip: bool,
    /// Reported quality: `"sq"` (standard) or `"hq"` (high, Go+).
    quality: String,
}

/// Parse the `media.transcodings` array from a track object.
fn parse_transcodings(track: &Value) -> Vec<Transcoding> {
    let arr = match track
        .get("media")
        .and_then(|m| m.get("transcodings"))
        .and_then(Value::as_array)
    {
        Some(a) => a,
        None => return Vec::new(),
    };

    arr.iter()
        .filter_map(|t| {
            let stream_url = t.get("url").and_then(Value::as_str)?.to_string();
            if stream_url.is_empty() {
                return None;
            }

            let format = t.get("format")?;
            let mime_type = format
                .get("mime_type")
                .and_then(Value::as_str)
                .unwrap_or("audio/mpeg")
                .to_string();
            let protocol = format
                .get("protocol")
                .and_then(Value::as_str)
                .unwrap_or("progressive")
                .to_string();
            let snip = t.get("snipped").and_then(Value::as_bool).unwrap_or(false);
            let quality = t
                .get("quality")
                .and_then(Value::as_str)
                .unwrap_or("sq")
                .to_string();

            Some(Transcoding {
                stream_url,
                mime_type,
                protocol,
                snip,
                quality,
            })
        })
        .collect()
}

/// Resolve a transcoding's `stream_url` to the final CDN redirect URL.
///
/// SoundCloud's stream endpoint returns:
/// ```json
/// { "url": "https://cf-media.sndcdn.com/…" }
/// ```
async fn resolve_stream_url(
    stream_url: &str,
    client_id: &str,
    client: &Arc<dyn NetworkClient>,
    source_url: &str,
) -> Result<String, ExtractorError> {
    let resolve_url = format!("{}?client_id={}", stream_url, client_id);
    debug!(resolve_url = %resolve_url, "resolving SoundCloud stream URL");

    let json = fetch_json(&resolve_url, client, Some(source_url)).await?;
    let cdn_url = json
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| ExtractorError::Parse {
            format: "soundcloud",
            reason: format!("stream resolution JSON missing 'url' field at {stream_url}"),
        })?;

    Ok(cdn_url.to_string())
}

/// Assign a format priority score: lower = preferred.
fn format_priority(protocol: &str, mime_type: &str) -> usize {
    let key = format!(
        "{}/{}",
        protocol,
        if mime_type.contains("opus") {
            "opus"
        } else {
            "mp3"
        }
    );
    FORMAT_PRIORITY
        .iter()
        .position(|&f| f == key.as_str())
        .unwrap_or(FORMAT_PRIORITY.len())
}

/// Build a human-readable label for a transcoding.
fn transcoding_label(t: &Transcoding) -> String {
    let proto = if t.protocol == "hls" {
        "HLS"
    } else {
        "Progressive"
    };
    let codec = if t.mime_type.contains("opus") {
        "Opus"
    } else {
        "MP3"
    };
    let quality = if t.quality == "hq" { " [HQ]" } else { "" };
    let snip = if t.snip { " (preview)" } else { "" };
    format!("{proto} {codec}{quality}{snip}")
}

/// Determine `StreamKind` from protocol and mime type.
fn stream_kind(protocol: &str) -> StreamKind {
    if protocol == "hls" {
        StreamKind::Audio // HLS audio-only playlist
    } else {
        StreamKind::Audio // progressive MP3 direct download
    }
}

// ── Artwork helper ────────────────────────────────────────────────────────────

/// Extract the best available artwork URL from a track object.
///
/// SoundCloud thumbnail URLs follow the pattern:
/// `https://i1.sndcdn.com/artworks-{id}-{size}.jpg`
/// where `size` is one of: `t500x500`, `crop`, `t300x300`, `large`, `small`.
fn best_artwork_url(track: &Value) -> Option<String> {
    let raw = track
        .get("artwork_url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())?;

    // Upgrade to 500×500 regardless of what the API returns
    let upgraded = raw.replace("-large.", "-t500x500.");
    Some(upgraded)
}

// ── SoundCloudExtractor ───────────────────────────────────────────────────────

pub struct SoundCloudExtractor;

impl Default for SoundCloudExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl SoundCloudExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }

    // ── Step 1: fetch page HTML + hydration ──────────────────────────────────

    async fn fetch_page_and_hydration(
        &self,
        url: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<(String, Vec<Value>), ExtractorError> {
        debug!(url, "fetching SoundCloud page HTML");
        let html = fetch_text(url, client, None).await?;

        let hydration = extract_hydration(&html).ok_or_else(|| ExtractorError::Parse {
            format: "soundcloud",
            reason: "window.__sc_hydration not found in page HTML".to_string(),
        })?;

        debug!(entries = hydration.len(), "parsed SC hydration array");
        Ok((html, hydration))
    }

    // ── Step 2: extract client_id from JS bundle ─────────────────────────────

    async fn fetch_client_id(
        &self,
        html: &str,
        source_url: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<String, ExtractorError> {
        let bundle_urls = extract_app_bundle_urls(html);
        if bundle_urls.is_empty() {
            return Err(ExtractorError::Parse {
                format: "soundcloud",
                reason: "no app JS bundle URLs found in page HTML".to_string(),
            });
        }

        debug!(count = bundle_urls.len(), "found app bundle URLs");

        for bundle_url in &bundle_urls {
            debug!(bundle_url, "scanning JS bundle for client_id");
            match fetch_bytes(
                bundle_url,
                client,
                Some(source_url),
                Some(JS_BUNDLE_MAX_BYTES),
            )
            .await
            {
                Ok(bytes) => {
                    // The JS is ASCII/UTF-8 safe for our scanning purposes
                    let text = String::from_utf8_lossy(&bytes);
                    if let Some(id) = extract_client_id_from_js(&text) {
                        return Ok(id);
                    }
                    debug!(bundle_url, "client_id not found in this bundle");
                }
                Err(e) => {
                    warn!(bundle_url, error = %e, "failed to fetch JS bundle");
                }
            }
        }

        Err(ExtractorError::Parse {
            format: "soundcloud",
            reason: format!(
                "client_id not found in any of {} app bundle(s)",
                bundle_urls.len()
            ),
        })
    }

    // ── Step 3: resolve track via API v2 ─────────────────────────────────────

    async fn api_resolve_track(
        &self,
        track_url: &str,
        client_id: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<Value, ExtractorError> {
        let resolve_url = format!(
            "{}/resolve?url={}&client_id={}",
            SC_API_V2,
            url::form_urlencoded::byte_serialize(track_url.as_bytes()).collect::<String>(),
            client_id
        );
        debug!(resolve_url = %resolve_url, "calling SC API resolve endpoint");

        let json = fetch_json(&resolve_url, client, Some(track_url)).await?;

        // The resolve endpoint may redirect to a collection (playlist). Validate.
        let kind = json
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        if kind != "track" {
            return Err(ExtractorError::Parse {
                format: "soundcloud",
                reason: format!("resolve returned kind='{kind}'; only single tracks are supported"),
            });
        }

        Ok(json)
    }

    // ── Step 4: build StreamGraph from track object ───────────────────────────

    async fn build_stream_graph(
        &self,
        source_url: &str,
        track: &Value,
        client_id: &str,
        client: &Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        // ── Metadata ─────────────────────────────────────────────────────────
        let title = track
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_string);

        let artist = track
            .get("user")
            .and_then(|u| u.get("username"))
            .and_then(Value::as_str)
            .map(str::to_string);

        let duration_ms = track.get("duration").and_then(Value::as_u64).unwrap_or(0);

        let genre = track
            .get("genre")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string);

        let artwork_url = best_artwork_url(track);

        debug!(
            title = ?title,
            artist = ?artist,
            duration_ms,
            "parsed SoundCloud track metadata"
        );

        // ── Transcodings ──────────────────────────────────────────────────────
        let mut transcodings = parse_transcodings(track);
        if transcodings.is_empty() {
            return Err(ExtractorError::NoStreams);
        }

        // Filter out snipped previews when full streams are available
        let has_full = transcodings.iter().any(|t| !t.snip);
        if has_full {
            transcodings.retain(|t| !t.snip);
        }

        // Sort by format priority (best formats first)
        transcodings.sort_unstable_by_key(|t| format_priority(&t.protocol, &t.mime_type));

        // ── Resolve stream URLs & build variants ──────────────────────────────
        let mut variants: Vec<StreamVariant> = Vec::new();

        for (idx, t) in transcodings.iter().enumerate() {
            let cdn_url =
                match resolve_stream_url(&t.stream_url, client_id, client, source_url).await {
                    Ok(u) => u,
                    Err(e) => {
                        warn!(
                            stream_url = %t.stream_url,
                            error = %e,
                            "failed to resolve stream URL — skipping transcoding"
                        );
                        continue;
                    }
                };

            let label = transcoding_label(t);
            let is_default = idx == 0 && !has_full.then_some(true).map(|_| t.snip).unwrap_or(false);

            variants.push(StreamVariant {
                kind: stream_kind(&t.protocol),
                label,
                bandwidth_bps: 0, // SoundCloud does not advertise bitrate
                resolution: None,
                codecs: vec![t.mime_type.clone()],
                playlist_url: cdn_url,
                segments: vec![],
                associated_tracks: vec![],
                is_default: is_default && variants.is_empty(),
            });
        }

        if variants.is_empty() {
            return Err(ExtractorError::NoStreams);
        }

        // Ensure exactly one default
        if !variants.iter().any(|v| v.is_default) {
            if let Some(first) = variants.first_mut() {
                first.is_default = true;
            }
        }

        // ── Standalone tracks ─────────────────────────────────────────────────
        let mut standalone_tracks: Vec<MediaTrack> = Vec::new();

        // Artwork
        if let Some(art_url) = artwork_url {
            standalone_tracks.push(MediaTrack {
                kind: StreamKind::Unknown,
                language: None,
                label: Some("artwork".to_string()),
                url: art_url,
                default_track: false,
            });
        }

        // ── Enrich title with artist ──────────────────────────────────────────
        let display_title = match (title, artist) {
            (Some(t), Some(a)) => Some(format!("{a} — {t}")),
            (Some(t), None) => Some(t),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        };

        // ── Duration in standalone meta track ────────────────────────────────
        if duration_ms > 0 {
            standalone_tracks.push(MediaTrack {
                kind: StreamKind::Unknown,
                language: None,
                label: Some(format!("duration_ms:{duration_ms}")),
                url: String::new(),
                default_track: false,
            });
        }

        if let Some(g) = genre {
            standalone_tracks.push(MediaTrack {
                kind: StreamKind::Unknown,
                language: None,
                label: Some(format!("genre:{g}")),
                url: String::new(),
                default_track: false,
            });
        }

        debug!(
            title = ?display_title,
            variants = variants.len(),
            "SoundCloud extraction complete"
        );

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: source_url.to_string(),
            title: display_title,
            variants,
            standalone_tracks,
            format: "soundcloud".to_string(),
        })
    }
}

// ── MediaExtractor impl ───────────────────────────────────────────────────────

#[async_trait]
impl MediaExtractor for SoundCloudExtractor {
    fn name(&self) -> &'static str {
        "SoundCloud"
    }

    fn can_handle(&self, url: &str) -> bool {
        let lower = url.to_ascii_lowercase();
        (lower.contains("soundcloud.com/") || lower.contains("on.soundcloud.com/"))
            && !lower.contains("/sets/") // playlists not yet supported
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        let url = &normalise_url(url);

        // ── Guard: single-track URL only ──────────────────────────────────────
        if !looks_like_track_url(url) {
            return Err(ExtractorError::Unsupported(format!(
                "SoundCloud extractor only handles single tracks (got: {url})"
            )));
        }

        // ── Step 1: Fetch page + hydration JSON ───────────────────────────────
        let (html, hydration) = self.fetch_page_and_hydration(url, &client).await?;

        // ── Step 2: Acquire client_id from JS bundle ──────────────────────────
        let client_id = self.fetch_client_id(&html, url, &client).await?;
        debug!(client_id = %client_id, "acquired SoundCloud client_id");

        // ── Step 3: Get full track object ─────────────────────────────────────
        // Try hydration first — if it has transcodings we can skip the API call.
        let track_from_hydration = find_track_in_hydration(&hydration);

        let track_has_streams = track_from_hydration
            .and_then(|t| t.get("media"))
            .and_then(|m| m.get("transcodings"))
            .and_then(Value::as_array)
            .map(|a| !a.is_empty())
            .unwrap_or(false);

        let track: Value = if track_has_streams {
            debug!("using track data from page hydration JSON (no API call needed)");
            track_from_hydration.unwrap().clone()
        } else {
            debug!("hydration lacks transcodings — calling API v2 resolve");
            self.api_resolve_track(url, &client_id, &client).await?
        };

        // ── Step 4: Build StreamGraph ─────────────────────────────────────────
        self.build_stream_graph(url, &track, &client_id, &client)
            .await
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── URL normalisation ─────────────────────────────────────────────────────

    #[test]
    fn normalises_mobile_url() {
        let normalised = normalise_url("https://m.soundcloud.com/artist/track");
        assert_eq!(normalised, "https://soundcloud.com/artist/track");
    }

    #[test]
    fn normalises_trailing_slash() {
        let normalised = normalise_url("https://soundcloud.com/artist/track/");
        assert_eq!(normalised, "https://soundcloud.com/artist/track");
    }

    #[test]
    fn leaves_canonical_url_unchanged() {
        let url = "https://soundcloud.com/artist/track";
        assert_eq!(normalise_url(url), url);
    }

    // ── can_handle ────────────────────────────────────────────────────────────

    #[test]
    fn handles_standard_track_url() {
        let e = SoundCloudExtractor::new();
        assert!(e.can_handle("https://soundcloud.com/artist/track-name"));
    }

    #[test]
    fn handles_private_track_url() {
        let e = SoundCloudExtractor::new();
        assert!(e.can_handle("https://soundcloud.com/artist/track/s-AbCdE"));
    }

    #[test]
    fn handles_short_url() {
        let e = SoundCloudExtractor::new();
        assert!(e.can_handle("https://on.soundcloud.com/AbCd1"));
    }

    #[test]
    fn does_not_handle_playlist_url() {
        let e = SoundCloudExtractor::new();
        assert!(!e.can_handle("https://soundcloud.com/artist/sets/my-playlist"));
    }

    #[test]
    fn does_not_handle_youtube_url() {
        let e = SoundCloudExtractor::new();
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
    }

    // ── looks_like_track_url ──────────────────────────────────────────────────

    #[test]
    fn identifies_track_url() {
        assert!(looks_like_track_url(
            "https://soundcloud.com/artist/my-cool-track"
        ));
    }

    #[test]
    fn identifies_private_share_url() {
        assert!(looks_like_track_url(
            "https://soundcloud.com/artist/my-track/s-ABCDE"
        ));
    }

    #[test]
    fn rejects_sets_url() {
        assert!(!looks_like_track_url(
            "https://soundcloud.com/artist/sets/playlist"
        ));
    }

    #[test]
    fn rejects_likes_url() {
        assert!(!looks_like_track_url("https://soundcloud.com/artist/likes"));
    }

    // ── extract_hydration ─────────────────────────────────────────────────────

    #[test]
    fn extracts_hydration_array() {
        let html = r#"<script>window.__sc_hydration = [{"hydratable":"user","data":{"id":1}},{"hydratable":"sound","data":{"id":42,"title":"Test Track"}}];</script>"#;
        let hydration = extract_hydration(html).unwrap();
        assert_eq!(hydration.len(), 2);
    }

    #[test]
    fn finds_sound_entry_in_hydration() {
        let hydration = vec![
            json!({"hydratable": "user", "data": {"id": 1}}),
            json!({"hydratable": "sound", "data": {"id": 42, "title": "Test"}}),
        ];
        let track = find_track_in_hydration(&hydration).unwrap();
        assert_eq!(track.get("id").and_then(Value::as_u64), Some(42));
        assert_eq!(track.get("title").and_then(Value::as_str), Some("Test"));
    }

    #[test]
    fn returns_none_if_no_sound_in_hydration() {
        let hydration = vec![json!({"hydratable": "user", "data": {"id": 1}})];
        assert!(find_track_in_hydration(&hydration).is_none());
    }

    #[test]
    fn returns_none_if_hydration_missing_from_html() {
        let html = "<html><body>No hydration here</body></html>";
        assert!(extract_hydration(html).is_none());
    }

    // ── extract_app_bundle_urls ───────────────────────────────────────────────

    #[test]
    fn finds_app_bundle_script_tags() {
        let html = r#"
            <script crossorigin src="https://a.sndcdn.com/assets/48-abc.js"></script>
            <script crossorigin src="https://a.sndcdn.com/assets/app-a1b2c3-0.js"></script>
            <script crossorigin src="https://a.sndcdn.com/assets/vendor-xyz-0.js"></script>
        "#;
        let urls = extract_app_bundle_urls(html);
        // Only the app-* bundle should match
        assert_eq!(urls.len(), 1);
        assert!(urls[0].contains("/app-"));
    }

    // ── extract_client_id_from_js ─────────────────────────────────────────────

    #[test]
    fn extracts_client_id_colon_form() {
        let js = r#"var t={client_id:"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4",other:"val"}"#;
        let id = extract_client_id_from_js(js).unwrap();
        assert_eq!(id, "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4");
    }

    #[test]
    fn extracts_client_id_equals_form() {
        let js = r#"r.client_id="zz9911aabb22cc33dd44ee55ff66aa77""#;
        let id = extract_client_id_from_js(js).unwrap();
        assert_eq!(id, "zz9911aabb22cc33dd44ee55ff66aa77");
    }

    #[test]
    fn returns_none_if_client_id_too_short() {
        let js = r#"client_id:"tooshort""#;
        assert!(extract_client_id_from_js(js).is_none());
    }

    #[test]
    fn returns_none_if_client_id_absent() {
        let js = r#"var x = {foo: "bar"};"#;
        assert!(extract_client_id_from_js(js).is_none());
    }

    // ── is_valid_client_id ────────────────────────────────────────────────────

    #[test]
    fn valid_client_id() {
        assert!(is_valid_client_id("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"));
    }

    #[test]
    fn rejects_too_short_id() {
        assert!(!is_valid_client_id("short"));
    }

    #[test]
    fn rejects_non_alphanumeric_id() {
        assert!(!is_valid_client_id("a1b2c3d4e5f6a1b2-invalid!chars"));
    }

    // ── parse_transcodings ────────────────────────────────────────────────────

    fn make_track_with_transcodings() -> Value {
        json!({
            "id": 123456789,
            "kind": "track",
            "title": "Test Track",
            "duration": 234000,
            "genre": "Electronic",
            "user": {"username": "TestArtist"},
            "artwork_url": "https://i1.sndcdn.com/artworks-abc-large.jpg",
            "media": {
                "transcodings": [
                    {
                        "url": "https://api-v2.soundcloud.com/media/soundcloud:tracks:123/stream/hls",
                        "preset": "mp3_0_0",
                        "duration": 234000,
                        "snipped": false,
                        "format": {
                            "protocol": "hls",
                            "mime_type": "audio/mpeg"
                        },
                        "quality": "sq"
                    },
                    {
                        "url": "https://api-v2.soundcloud.com/media/soundcloud:tracks:123/stream/hls_opus",
                        "preset": "opus_0_0",
                        "duration": 234000,
                        "snipped": false,
                        "format": {
                            "protocol": "hls",
                            "mime_type": "audio/ogg; codecs=\"opus\""
                        },
                        "quality": "sq"
                    },
                    {
                        "url": "https://api-v2.soundcloud.com/media/soundcloud:tracks:123/stream/progressive",
                        "preset": "mp3_0_0",
                        "duration": 234000,
                        "snipped": false,
                        "format": {
                            "protocol": "progressive",
                            "mime_type": "audio/mpeg"
                        },
                        "quality": "sq"
                    }
                ]
            }
        })
    }

    #[test]
    fn parses_all_transcodings() {
        let track = make_track_with_transcodings();
        let tc = parse_transcodings(&track);
        assert_eq!(tc.len(), 3);
    }

    #[test]
    fn transcoding_protocols_parsed_correctly() {
        let track = make_track_with_transcodings();
        let tc = parse_transcodings(&track);
        assert!(tc.iter().any(|t| t.protocol == "hls"));
        assert!(tc.iter().any(|t| t.protocol == "progressive"));
    }

    #[test]
    fn snipped_transcoding_parsed() {
        let track = json!({
            "media": {
                "transcodings": [{
                    "url": "https://api-v2.soundcloud.com/media/123/stream/hls",
                    "snipped": true,
                    "format": {"protocol": "hls", "mime_type": "audio/mpeg"},
                    "quality": "sq"
                }]
            }
        });
        let tc = parse_transcodings(&track);
        assert_eq!(tc.len(), 1);
        assert!(tc[0].snip);
    }

    #[test]
    fn returns_empty_when_no_media_node() {
        let track = json!({"id": 1, "title": "No media"});
        assert!(parse_transcodings(&track).is_empty());
    }

    // ── format_priority ───────────────────────────────────────────────────────

    #[test]
    fn hls_mp3_beats_progressive() {
        assert!(
            format_priority("hls", "audio/mpeg") < format_priority("progressive", "audio/mpeg")
        );
    }

    #[test]
    fn hls_mp3_beats_hls_opus() {
        // HLS MP3 has broader compatibility
        assert!(
            format_priority("hls", "audio/mpeg")
                <= format_priority("hls", "audio/ogg; codecs=\"opus\"")
        );
    }

    // ── best_artwork_url ──────────────────────────────────────────────────────

    #[test]
    fn upgrades_artwork_to_500px() {
        let track = json!({
            "artwork_url": "https://i1.sndcdn.com/artworks-abc123-large.jpg"
        });
        let url = best_artwork_url(&track).unwrap();
        assert!(url.contains("t500x500"));
        assert!(!url.contains("-large."));
    }

    #[test]
    fn returns_none_when_no_artwork() {
        let track = json!({"id": 1});
        assert!(best_artwork_url(&track).is_none());
    }

    #[test]
    fn returns_none_when_artwork_url_empty() {
        let track = json!({"artwork_url": ""});
        assert!(best_artwork_url(&track).is_none());
    }

    // ── transcoding_label ─────────────────────────────────────────────────────

    #[test]
    fn label_for_hls_mp3() {
        let t = Transcoding {
            stream_url: "https://example.com".to_string(),
            mime_type: "audio/mpeg".to_string(),
            protocol: "hls".to_string(),
            snip: false,
            quality: "sq".to_string(),
        };
        assert!(transcoding_label(&t).contains("HLS"));
        assert!(transcoding_label(&t).contains("MP3"));
        assert!(!transcoding_label(&t).contains("preview"));
    }

    #[test]
    fn label_for_snipped_track() {
        let t = Transcoding {
            stream_url: "https://example.com".to_string(),
            mime_type: "audio/mpeg".to_string(),
            protocol: "progressive".to_string(),
            snip: true,
            quality: "sq".to_string(),
        };
        assert!(transcoding_label(&t).contains("preview"));
    }

    #[test]
    fn label_for_hq_track() {
        let t = Transcoding {
            stream_url: "https://example.com".to_string(),
            mime_type: "audio/mpeg".to_string(),
            protocol: "hls".to_string(),
            snip: false,
            quality: "hq".to_string(),
        };
        assert!(transcoding_label(&t).contains("[HQ]"));
    }
}
