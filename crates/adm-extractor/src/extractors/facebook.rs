//! `FacebookExtractor` — stream extraction for facebook.com
//!
//! ## Strategy
//! Facebook embeds video metadata directly in the HTML of video pages as
//! JSON-LD blobs and `__NEXT_DATA__` / `require()` data structures.
//! The extractor uses a multi-pass HTML scraping approach:
//!
//! 1. **Fetch the page HTML** with a desktop User-Agent and `Accept-Language: en`.
//! 2. **Pass 1 — `playable_url` / `playable_url_quality_hd`** — the most
//!    reliable source; found in Relay store JSON embedded as:
//!    `"playable_url":"https://…"` or `"playable_url_quality_hd":"https://…"`.
//! 3. **Pass 2 — `sd_src` / `hd_src`** — older Facebook video pages embed
//!    direct MP4 links as `"sd_src":"…"` and `"hd_src":"…"`.
//! 4. **Pass 3 — DASH manifest** — newer pages embed a DASH MPD URL in
//!    `"dash_manifest"` or `"dash_prefetch_src"`.
//! 5. **Title extraction** — from `<title>` tag or OG title meta tag.
//!
//! ## Supported URL forms
//! - `https://www.facebook.com/video.php?v={id}`
//! - `https://www.facebook.com/{user}/videos/{id}`
//! - `https://www.facebook.com/watch?v={id}`
//! - `https://www.facebook.com/reel/{id}`
//! - `https://fb.watch/{token}` (short link — followed via redirect)
//!
//! ## Limitations
//! - Private videos (login-required) will return a login page with no streams.
//! - DRM-protected content (Widevine) is not decryptable.
//! - The HTML scraping approach may break if Facebook changes its page structure.
//!   Consider this extractor best-effort.

use crate::{
    stream_graph::{StreamGraph, StreamKind, StreamVariant},
    ExtractorError, MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{debug, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Desktop Chrome UA — Facebook serves a reduced JS payload to mobile agents.
const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
     (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";

// ── HTTP helper ───────────────────────────────────────────────────────────────

async fn fetch_html(url: &str, client: &Arc<dyn NetworkClient>) -> Result<String, ExtractorError> {
    let mut req = NetworkRequest::new(url, None);
    req.headers
        .push(("User-Agent".to_string(), USER_AGENT.to_string()));
    req.headers
        .push(("Accept-Language".to_string(), "en-US,en;q=0.9".to_string()));
    req.headers.push((
        "Accept".to_string(),
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8".to_string(),
    ));
    // Facebook requires a locale cookie to avoid redirect loops
    req.headers
        .push(("Cookie".to_string(), "locale=en_US; datr=0".to_string()));

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

    String::from_utf8_lossy(&buf).into_owned().pipe(Ok)
}

trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
impl<T> Pipe for T {}

// ── URL helpers ───────────────────────────────────────────────────────────────

/// Normalise the Facebook URL to a canonical HTTPS desktop page URL.
fn normalise_url(url: &str) -> String {
    // Replace mobile subdomain with www
    url.replace("m.facebook.com", "www.facebook.com")
        .replace("mobile.facebook.com", "www.facebook.com")
}

fn is_facebook_url(url: &str) -> bool {
    url.contains("facebook.com/") || url.contains("fb.watch/")
}

// ── HTML scraping helpers ─────────────────────────────────────────────────────

/// Un-escape a JSON-embedded URL.
///
/// Facebook escapes forward slashes as `\/` and uses `\u0025` for `%`.
fn unescape_fb_url(raw: &str) -> String {
    raw.replace("\\/", "/")
        .replace("\\u0025", "%")
        .replace("\\u0026", "&")
        .replace("\\u003C", "<")
        .replace("\\u003E", ">")
}

/// Extract the value of a JSON string field from an HTML blob.
///
/// Searches for `"{field}":"{value}"` patterns, handles escaped characters.
fn extract_json_string_field<'a>(html: &'a str, field: &str) -> Option<&'a str> {
    let needle = format!("\"{field}\":\"");
    let start = html.find(&needle)? + needle.len();
    // Find the closing quote, skipping `\"` escape sequences
    let slice = &html[start..];
    let mut end = 0;
    let chars: Vec<char> = slice.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '\\' {
            i += 2; // skip escaped char
            continue;
        }
        if chars[i] == '"' {
            end = slice
                .char_indices()
                .nth(i)
                .map(|(b, _)| b)
                .unwrap_or(slice.len());
            break;
        }
        i += 1;
    }
    if end == 0 {
        return None;
    }
    Some(&slice[..end])
}

/// Extract the page `<title>` content.
fn extract_title(html: &str) -> Option<String> {
    // Try OG title first
    let og_marker = "property=\"og:title\" content=\"";
    if let Some(start) = html.find(og_marker) {
        let slice = &html[start + og_marker.len()..];
        if let Some(end) = slice.find('"') {
            let title = &slice[..end];
            if !title.is_empty() {
                return Some(html_decode(title));
            }
        }
    }

    // Fall back to <title> tag
    let title_start = html.find("<title>")?;
    let slice = &html[title_start + 7..];
    let title_end = slice.find("</title>")?;
    let raw = &slice[..title_end];
    if raw.trim().is_empty() {
        return None;
    }
    // Strip " | Facebook" suffix Facebook appends
    let clean = raw
        .trim()
        .trim_end_matches(" | Facebook")
        .trim_end_matches(" - Facebook");
    Some(html_decode(clean))
}

/// Minimal HTML entity decode for common entities in titles.
fn html_decode(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
}

// ── Stream extraction passes ──────────────────────────────────────────────────

/// Pass 1: Look for `playable_url_quality_hd` and `playable_url` fields.
///
/// These appear in Relay store JSON embedded in `<script>` tags.
fn pass_playable_url(html: &str) -> Vec<StreamVariant> {
    let mut variants = Vec::new();

    // HD variant
    if let Some(raw) = extract_json_string_field(html, "playable_url_quality_hd") {
        let url = unescape_fb_url(raw);
        if !url.is_empty() && url.starts_with("http") {
            debug!("found Facebook HD playable URL");
            variants.push(StreamVariant {
                kind: StreamKind::Video,
                label: "HD [mp4]".to_string(),
                bandwidth_bps: 2_500_000,
                resolution: None,
                codecs: vec!["avc1".to_string()],
                playlist_url: url,
                segments: vec![],
                associated_tracks: vec![],
                is_default: false,
            });
        }
    }

    // SD / base variant
    if let Some(raw) = extract_json_string_field(html, "playable_url") {
        let url = unescape_fb_url(raw);
        if !url.is_empty() && url.starts_with("http") {
            // Skip if this is a duplicate of the HD URL
            if !variants
                .iter()
                .any(|v: &StreamVariant| v.playlist_url == url)
            {
                debug!("found Facebook SD playable URL");
                variants.push(StreamVariant {
                    kind: StreamKind::Video,
                    label: "SD [mp4]".to_string(),
                    bandwidth_bps: 800_000,
                    resolution: None,
                    codecs: vec!["avc1".to_string()],
                    playlist_url: url,
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: false,
                });
            }
        }
    }

    variants
}

/// Pass 2: Legacy `hd_src` / `sd_src` fields (older Facebook video pages).
fn pass_legacy_src(html: &str) -> Vec<StreamVariant> {
    let mut variants = Vec::new();

    for (field, label, bps) in &[
        ("hd_src", "HD [mp4]", 2_500_000u64),
        ("sd_src", "SD [mp4]", 800_000u64),
    ] {
        if let Some(raw) = extract_json_string_field(html, field) {
            let url = unescape_fb_url(raw);
            if !url.is_empty() && url.starts_with("http") {
                debug!(field, "found Facebook legacy src URL");
                variants.push(StreamVariant {
                    kind: StreamKind::Video,
                    label: label.to_string(),
                    bandwidth_bps: *bps,
                    resolution: None,
                    codecs: vec!["avc1".to_string()],
                    playlist_url: url,
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: false,
                });
            }
        }
    }

    variants
}

/// Pass 3: DASH manifest URL.
///
/// Facebook increasingly serves DASH for longer videos.  We extract the MPD
/// URL so the `GenericDashExtractor` can expand it in a second pass.
fn pass_dash_manifest(html: &str) -> Option<StreamVariant> {
    for field in &["dash_prefetch_src", "dash_manifest_url"] {
        if let Some(raw) = extract_json_string_field(html, field) {
            let url = unescape_fb_url(raw);
            if !url.is_empty() && url.starts_with("http") {
                debug!(field, "found Facebook DASH manifest URL");
                return Some(StreamVariant {
                    kind: StreamKind::Video,
                    label: "DASH (adaptive)".to_string(),
                    bandwidth_bps: 0,
                    resolution: None,
                    codecs: vec![],
                    playlist_url: url,
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: false,
                });
            }
        }
    }
    None
}

/// Pass 4: m3u8/HLS URL (some Facebook Live / Watch Party streams).
fn pass_hls_url(html: &str) -> Option<StreamVariant> {
    for field in &["hls_url", "stream_url"] {
        if let Some(raw) = extract_json_string_field(html, field) {
            let url = unescape_fb_url(raw);
            if !url.is_empty()
                && url.starts_with("http")
                && (url.contains(".m3u8") || url.contains("m3u8"))
            {
                debug!(field, "found Facebook HLS stream URL");
                return Some(StreamVariant {
                    kind: StreamKind::Video,
                    label: "HLS (live/adaptive)".to_string(),
                    bandwidth_bps: 0,
                    resolution: None,
                    codecs: vec![],
                    playlist_url: url,
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: false,
                });
            }
        }
    }
    None
}

// ── Facebook login-wall detection ─────────────────────────────────────────────

/// Return `true` if the HTML looks like a login redirect page.
fn is_login_wall(html: &str) -> bool {
    html.contains("login_form") || html.contains("loginform") || html.contains("log into facebook")
}

// ── FacebookExtractor ─────────────────────────────────────────────────────────

pub struct FacebookExtractor;

impl Default for FacebookExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl FacebookExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MediaExtractor for FacebookExtractor {
    fn name(&self) -> &'static str {
        "Facebook"
    }

    fn can_handle(&self, url: &str) -> bool {
        is_facebook_url(url)
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        let canonical_url = normalise_url(url);
        debug!(url = %canonical_url, "fetching Facebook video page");

        // Fetch page HTML
        let html = fetch_html(&canonical_url, &client).await?;

        // Detect login wall before wasting time parsing
        if is_login_wall(&html) {
            return Err(ExtractorError::Network(
                "Facebook returned a login page — video may be private or geo-restricted"
                    .to_string(),
            ));
        }

        let title = extract_title(&html);

        // Run extraction passes in priority order
        let mut variants: Vec<StreamVariant> = Vec::new();

        // Pass 1 — playable_url (Relay store, most reliable)
        let p1 = pass_playable_url(&html);
        if !p1.is_empty() {
            debug!(count = p1.len(), "Pass 1 (playable_url) found variants");
            variants.extend(p1);
        }

        // Pass 2 — legacy sd_src/hd_src (older pages)
        if variants.is_empty() {
            let p2 = pass_legacy_src(&html);
            if !p2.is_empty() {
                debug!(count = p2.len(), "Pass 2 (legacy src) found variants");
                variants.extend(p2);
            }
        }

        // Pass 3 — DASH manifest (newer pages, adaptive)
        if let Some(dash_v) = pass_dash_manifest(&html) {
            debug!("Pass 3 (DASH) found adaptive stream");
            variants.push(dash_v);
        }

        // Pass 4 — HLS stream (live content)
        if variants.is_empty() {
            if let Some(hls_v) = pass_hls_url(&html) {
                debug!("Pass 4 (HLS) found stream");
                variants.push(hls_v);
            }
        }

        if variants.is_empty() {
            warn!("No streams found in Facebook page HTML");
            return Err(ExtractorError::NoStreams);
        }

        // Sort: MP4 HD first, MP4 SD next, adaptive (DASH/HLS) last
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

        debug!(
            title = ?title,
            variants = variants.len(),
            "Facebook extraction complete"
        );

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: url.to_string(),
            title,
            variants,
            standalone_tracks: vec![],
            format: "facebook".to_string(),
        })
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unescape_handles_forward_slashes() {
        let raw = "https:\\/\\/video.xx.fbcdn.net\\/v\\/video.mp4";
        let result = unescape_fb_url(raw);
        assert_eq!(result, "https://video.xx.fbcdn.net/v/video.mp4");
    }

    #[test]
    fn extract_json_string_field_basic() {
        let html = r#"{"playable_url":"https:\/\/cdn.fb.com\/v.mp4","other":"ignored"}"#;
        let result = extract_json_string_field(html, "playable_url").unwrap();
        assert_eq!(result, r"https:\/\/cdn.fb.com\/v.mp4");
    }

    #[test]
    fn extract_title_from_og_meta() {
        let html = r#"<meta property="og:title" content="My Video Title" /><title>Other</title>"#;
        assert_eq!(extract_title(html).as_deref(), Some("My Video Title"));
    }

    #[test]
    fn extract_title_from_title_tag() {
        let html = "<title>My Video | Facebook</title>";
        assert_eq!(extract_title(html).as_deref(), Some("My Video"));
    }

    #[test]
    fn html_decode_entities() {
        assert_eq!(html_decode("Hello &amp; World"), "Hello & World");
        assert_eq!(html_decode("It&#39;s great"), "It's great");
    }

    #[test]
    fn pass_playable_url_extracts_hd_and_sd() {
        let html = r#"
            "playable_url_quality_hd":"https:\/\/cdn.fb.com\/hd.mp4"
            "playable_url":"https:\/\/cdn.fb.com\/sd.mp4"
        "#;
        let variants = pass_playable_url(html);
        assert_eq!(variants.len(), 2);
        assert!(variants[0].label.contains("HD"));
        assert!(variants[1].label.contains("SD"));
        assert!(variants[0].playlist_url.contains("hd.mp4"));
    }

    #[test]
    fn pass_legacy_src_extracts_both() {
        let html = r#"
            "hd_src":"https:\/\/cdn.fb.com\/hd.mp4"
            "sd_src":"https:\/\/cdn.fb.com\/sd.mp4"
        "#;
        let variants = pass_legacy_src(html);
        assert_eq!(variants.len(), 2);
    }

    #[test]
    fn pass_dash_manifest_extracts_mpd() {
        let html = r#""dash_prefetch_src":"https:\/\/cdn.fb.com\/manifest.mpd""#;
        let variant = pass_dash_manifest(html);
        assert!(variant.is_some());
        assert_eq!(variant.unwrap().bandwidth_bps, 0);
    }

    #[test]
    fn login_wall_detection() {
        let html_login = "<div id='login_form'>Please log in</div>";
        let html_normal = "<div id='video_player'>Video here</div>";
        assert!(is_login_wall(html_login));
        assert!(!is_login_wall(html_normal));
    }

    #[test]
    fn normalise_url_replaces_mobile_domain() {
        let url = "https://m.facebook.com/video.php?v=123";
        assert_eq!(
            normalise_url(url),
            "https://www.facebook.com/video.php?v=123"
        );
    }

    #[test]
    fn can_handle_facebook_url_forms() {
        let e = FacebookExtractor::new();
        assert!(e.can_handle("https://www.facebook.com/video.php?v=123"));
        assert!(e.can_handle("https://www.facebook.com/user/videos/123"));
        assert!(e.can_handle("https://www.facebook.com/watch?v=123"));
        assert!(e.can_handle("https://fb.watch/sometoken"));
        assert!(!e.can_handle("https://youtube.com/watch?v=abc"));
    }
}
