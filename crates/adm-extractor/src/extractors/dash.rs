//! `GenericDashExtractor` — MPEG-DASH MPD parser.
//!
//! Handles basic MPD manifests by parsing Representation elements from
//! `AdaptationSets`, producing a `StreamGraph` with one variant per Representation.

use crate::{
    error::ExtractorError,
    stream_graph::{StreamGraph, StreamKind, StreamVariant},
    MediaExtractor,
};
use adm_network::{NetworkClient, NetworkRequest};
use async_trait::async_trait;
use std::cmp::Reverse;
use std::sync::Arc;
use tracing::{debug, warn};

pub struct GenericDashExtractor;

impl GenericDashExtractor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for GenericDashExtractor {
    fn default() -> Self {
        Self::new()
    }
}

// ── Fetch helper ──────────────────────────────────────────────────────────────

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
        format: "dash",
        reason: format!("MPD is not valid UTF-8: {e}"),
    })
}

// ── Minimal attribute extractor ───────────────────────────────────────────────

fn xml_attr<'a>(tag: &'a str, name: &str) -> Option<&'a str> {
    let needle = format!("{name}=\"");
    let start = tag.find(needle.as_str())? + needle.len();
    let end = tag[start..].find('"')? + start;
    Some(&tag[start..end])
}

fn xml_attr_owned(tag: &str, name: &str) -> Option<String> {
    xml_attr(tag, name).map(str::to_string)
}

// ── URL resolution ────────────────────────────────────────────────────────────

fn resolve_url(base: &str, raw: &str) -> String {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return raw.to_string();
    }
    if raw.starts_with("//") {
        let scheme = base.split(':').next().unwrap_or("https");
        return format!("{scheme}:{raw}");
    }
    let base_dir = base.rfind('/').map_or(base, |i| &base[..=i]);
    format!("{base_dir}{raw}")
}

// ── BaseURL extraction ────────────────────────────────────────────────────────

fn extract_base_url(block: &str) -> Option<String> {
    let start = block.find("<BaseURL>")?;
    let after = &block[start + 9..];
    let end = after.find("</BaseURL>")?;
    let url = after[..end].trim().to_string();
    if url.is_empty() {
        None
    } else {
        Some(url)
    }
}

// ── Kind inference ────────────────────────────────────────────────────────────

fn infer_kind(mime: Option<&str>, codecs: Option<&str>) -> StreamKind {
    let m = mime.unwrap_or("").to_ascii_lowercase();
    let c = codecs.unwrap_or("").to_ascii_lowercase();
    if m.starts_with("video/")
        || c.contains("avc")
        || c.contains("hevc")
        || c.contains("vp9")
        || c.contains("av1")
    {
        return StreamKind::Video;
    }
    if m.starts_with("audio/")
        || c.contains("mp4a")
        || c.contains("opus")
        || c.contains("vorbis")
        || c.contains("ac-3")
    {
        return StreamKind::Audio;
    }
    if m.contains("text/") || c.contains("wvtt") || c.contains("ttml") {
        return StreamKind::Subtitle;
    }
    StreamKind::Unknown
}

// ── MPD parsing ───────────────────────────────────────────────────────────────

fn parse_iso8601_duration(s: &str) -> Option<f64> {
    // Very small ISO8601 PT parser supporting H, M, S (integers or floats)
    if !s.starts_with('P') {
        return None;
    }
    let mut rest = &s[1..];
    if rest.starts_with('T') {
        rest = &rest[1..];
    }
    let mut secs = 0f64;
    while !rest.is_empty() {
        if let Some(i) = rest.find(['H', 'M', 'S']) {
            let (num, after) = rest.split_at(i);
            let unit = after.chars().next().unwrap();
            if let Ok(v) = num.parse::<f64>() {
                match unit {
                    'H' => secs += v * 3600.0,
                    'M' => secs += v * 60.0,
                    'S' => secs += v,
                    _ => {}
                }
            }
            rest = &after[1..];
        } else {
            break;
        }
    }
    Some(secs)
}

fn parse_segment_timeline(block: &str) -> Vec<(u32, u64, Option<u64>)> {
    // returns Vec of (duration_ticks, repeat_count+1, start_time)
    let mut out = Vec::new();
    if let Some(start) = block.find("<SegmentTimeline") {
        if let Some(end) = block[start..].find("</SegmentTimeline>") {
            let inner = &block[start..start + end];
            for s_tag in inner.split("<S").skip(1) {
                if let Some(endattr) = s_tag.find('>') {
                    let tag = &s_tag[..endattr];
                    if let Some(d_str) = xml_attr(tag, "d") {
                        if let Ok(d) = d_str.trim().parse::<u32>() {
                            let r = xml_attr(tag, "r")
                                .and_then(|r| r.trim().parse::<i64>().ok())
                                .unwrap_or(0);
                            let t = xml_attr(tag, "t").and_then(|t| t.trim().parse::<u64>().ok());
                            let repeats = u64::try_from(r.saturating_add(1)).unwrap_or(0);
                            out.push((d, repeats, t));
                        }
                    }
                }
            }
        }
    }
    out
}

fn ticks_to_seconds(ticks: u32, timescale: u32) -> f64 {
    f64::from(ticks) / f64::from(timescale.max(1))
}

fn segment_count(total_secs: f64, duration_ticks: u32, timescale: u32) -> u64 {
    let total = std::time::Duration::from_secs_f64(total_secs.max(0.0));
    let total_ticks = total
        .as_nanos()
        .saturating_mul(u128::from(timescale.max(1)))
        / 1_000_000_000;
    let count = total_ticks.div_ceil(u128::from(duration_ticks.max(1)));
    u64::try_from(count.max(1)).unwrap_or(u64::MAX)
}

fn find_template_tag<'a>(blocks: &[&'a str]) -> Option<&'a str> {
    for block in blocks {
        if let Some(pos) = block.find("<SegmentTemplate") {
            let tail = &block[pos..];
            let end = tail.find('>').unwrap_or(tail.len() - 1);
            return Some(&tail[..=end]);
        }
    }
    None
}

fn find_segment_base_tag<'a>(blocks: &[&'a str]) -> Option<&'a str> {
    for block in blocks {
        if let Some(pos) = block.find("<SegmentBase") {
            let tail = &block[pos..];
            let end = tail.find('>').unwrap_or(tail.len() - 1);
            return Some(&tail[..=end]);
        }
    }
    None
}

struct RepresentationScope<'a> {
    rep_block: &'a str,
    adaptation_block: &'a str,
    period_block: &'a str,
    mpd_text: &'a str,
    mpd_url: &'a str,
    period_base: &'a str,
    set_base_url: Option<&'a str>,
    set_mime: Option<&'a str>,
    mpd_duration: Option<f64>,
}

struct RepresentationMetadata {
    id: String,
    bandwidth: u64,
    width: Option<u32>,
    height: Option<u32>,
    codecs: Option<String>,
    mime_type: Option<String>,
    base_url: String,
}

fn parse_representation_metadata(scope: &RepresentationScope<'_>) -> RepresentationMetadata {
    let rep_tag_end = scope.rep_block.find('>').unwrap_or(scope.rep_block.len());
    let rep_tag = &scope.rep_block[..rep_tag_end];
    let codecs = xml_attr_owned(rep_tag, "codecs");
    let mime_type =
        xml_attr_owned(rep_tag, "mimeType").or_else(|| scope.set_mime.map(str::to_string));
    let base_url = extract_base_url(scope.rep_block)
        .or_else(|| scope.set_base_url.map(str::to_string))
        .or_else(|| extract_base_url(scope.period_block))
        .map_or_else(
            || scope.period_base.to_string(),
            |u| resolve_url(scope.mpd_url, &u),
        );

    RepresentationMetadata {
        id: xml_attr_owned(rep_tag, "id").unwrap_or_else(|| "0".to_string()),
        bandwidth: xml_attr(rep_tag, "bandwidth")
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0),
        width: xml_attr(rep_tag, "width").and_then(|s| s.trim().parse().ok()),
        height: xml_attr(rep_tag, "height").and_then(|s| s.trim().parse().ok()),
        codecs,
        mime_type,
        base_url,
    }
}

fn push_init_segment(
    segments: &mut Vec<crate::stream_graph::SegmentInfo>,
    init_template: Option<&String>,
    meta: &RepresentationMetadata,
    start_number: u64,
) {
    if let Some(init_tpl) = init_template {
        let init_url = resolve_url(
            &meta.base_url,
            &init_tpl.replace("$RepresentationID$", &meta.id),
        );
        segments.push(crate::stream_graph::SegmentInfo {
            url: init_url,
            sequence: start_number.saturating_sub(1),
            duration_secs: 0.0,
            byte_range: None,
            encryption_key_url: None,
            encryption_iv: None,
            encryption_key_hex: None,
            discontinuity: false,
        });
    }
}

fn push_timeline_segments(
    segments: &mut Vec<crate::stream_graph::SegmentInfo>,
    timeline: Vec<(u32, u64, Option<u64>)>,
    media_template: &str,
    meta: &RepresentationMetadata,
    start_number: u64,
    timescale: u32,
) {
    let mut seq = start_number;
    let mut current_time = timeline.iter().find_map(|(_, _, t)| *t).unwrap_or(0);
    for (duration_ticks, count, _) in timeline {
        for _ in 0..count {
            let url = media_template
                .replace("$RepresentationID$", &meta.id)
                .replace("$Number$", &seq.to_string())
                .replace("$Time$", &current_time.to_string());
            segments.push(crate::stream_graph::SegmentInfo {
                url: resolve_url(&meta.base_url, &url),
                sequence: seq,
                duration_secs: ticks_to_seconds(duration_ticks, timescale),
                byte_range: None,
                encryption_key_url: None,
                encryption_iv: None,
                encryption_key_hex: None,
                discontinuity: false,
            });
            seq = seq.saturating_add(1);
            current_time = current_time.saturating_add(u64::from(duration_ticks));
        }
    }
}

fn push_duration_segments(
    segments: &mut Vec<crate::stream_graph::SegmentInfo>,
    template_tag: &str,
    media_template: &str,
    meta: &RepresentationMetadata,
    start_number: u64,
    scope: &RepresentationScope<'_>,
) {
    let Some(total_secs) = scope.mpd_duration else {
        return;
    };
    let Some(duration_ticks) =
        xml_attr(template_tag, "duration").and_then(|s| s.trim().parse::<u32>().ok())
    else {
        return;
    };
    let timescale = xml_attr(template_tag, "timescale")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1);
    let segment_secs = ticks_to_seconds(duration_ticks, timescale);

    for i in 0..segment_count(total_secs, duration_ticks, timescale) {
        let seq = start_number.saturating_add(i);
        let url = media_template
            .replace("$RepresentationID$", &meta.id)
            .replace("$Number$", &seq.to_string());
        segments.push(crate::stream_graph::SegmentInfo {
            url: resolve_url(&meta.base_url, &url),
            sequence: seq,
            duration_secs: segment_secs,
            byte_range: None,
            encryption_key_url: None,
            encryption_iv: None,
            encryption_key_hex: None,
            discontinuity: false,
        });
    }
}

fn parse_template_segments(
    template_tag: &str,
    scope: &RepresentationScope<'_>,
    meta: &RepresentationMetadata,
) -> Vec<crate::stream_graph::SegmentInfo> {
    let mut segments = Vec::new();
    let media_template = xml_attr_owned(template_tag, "media");
    let init_template = xml_attr_owned(template_tag, "initialization");
    let timescale = xml_attr(template_tag, "timescale")
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(1);
    let start_number = xml_attr(template_tag, "startNumber")
        .and_then(|s| s.parse::<i64>().ok())
        .and_then(|n| u64::try_from(n).ok())
        .unwrap_or(1);

    push_init_segment(&mut segments, init_template.as_ref(), meta, start_number);

    if let Some(media) = media_template.as_deref() {
        let timeline = parse_segment_timeline(scope.rep_block);
        if timeline.is_empty() {
            push_duration_segments(
                &mut segments,
                template_tag,
                media,
                meta,
                start_number,
                scope,
            );
        } else {
            push_timeline_segments(
                &mut segments,
                timeline,
                media,
                meta,
                start_number,
                timescale,
            );
        }
    }

    segments
}

fn parse_segment_base_segments(
    segment_base_tag: &str,
    scope: &RepresentationScope<'_>,
    meta: &RepresentationMetadata,
) -> Vec<crate::stream_graph::SegmentInfo> {
    let Some((start, end)) =
        xml_attr(segment_base_tag, "indexRange").and_then(|idx| idx.split_once('-'))
    else {
        return Vec::new();
    };
    let (Ok(range_start), Ok(range_end)) = (start.trim().parse::<u64>(), end.trim().parse::<u64>())
    else {
        return Vec::new();
    };
    let url = extract_base_url(scope.rep_block)
        .or_else(|| extract_base_url(scope.adaptation_block))
        .or_else(|| extract_base_url(scope.period_block))
        .unwrap_or_else(|| meta.base_url.clone());
    let mut segments = vec![crate::stream_graph::SegmentInfo {
        url,
        sequence: 0,
        duration_secs: 0.0,
        byte_range: Some((range_start, range_end)),
        encryption_key_url: None,
        encryption_iv: None,
        encryption_key_hex: None,
        discontinuity: false,
    }];

    if let Some(init_attr) = xml_attr(segment_base_tag, "initialization") {
        segments.insert(
            0,
            crate::stream_graph::SegmentInfo {
                url: resolve_url(&meta.base_url, init_attr),
                sequence: 0,
                duration_secs: 0.0,
                byte_range: None,
                encryption_key_url: None,
                encryption_iv: None,
                encryption_key_hex: None,
                discontinuity: false,
            },
        );
    }
    segments
}

fn parse_progressive_segments(
    scope: &RepresentationScope<'_>,
) -> Vec<crate::stream_graph::SegmentInfo> {
    let url = extract_base_url(scope.rep_block)
        .or_else(|| scope.set_base_url.map(str::to_string))
        .or_else(|| extract_base_url(scope.period_block))
        .map_or_else(
            || scope.mpd_url.to_string(),
            |u| resolve_url(scope.mpd_url, &u),
        );
    vec![crate::stream_graph::SegmentInfo {
        url,
        sequence: 0,
        duration_secs: 0.0,
        byte_range: None,
        encryption_key_url: None,
        encryption_iv: None,
        encryption_key_hex: None,
        discontinuity: false,
    }]
}

fn parse_segments(
    scope: &RepresentationScope<'_>,
    meta: &RepresentationMetadata,
) -> Vec<crate::stream_graph::SegmentInfo> {
    if let Some(template_tag) = find_template_tag(&[
        scope.rep_block,
        scope.adaptation_block,
        scope.period_block,
        scope.mpd_text,
    ]) {
        return parse_template_segments(template_tag, scope, meta);
    }
    if let Some(segment_base_tag) = find_segment_base_tag(&[
        scope.rep_block,
        scope.adaptation_block,
        scope.period_block,
        scope.mpd_text,
    ]) {
        return parse_segment_base_segments(segment_base_tag, scope, meta);
    }
    parse_progressive_segments(scope)
}

fn build_variant(scope: &RepresentationScope<'_>) -> StreamVariant {
    let meta = parse_representation_metadata(scope);
    let label = match (meta.width, meta.height) {
        (_, Some(h)) => format!("{h}p ({}kbps)", meta.bandwidth.max(1) / 1000),
        _ if meta.bandwidth > 0 => format!("{}kbps", meta.bandwidth / 1000),
        _ => "DASH stream".to_string(),
    };
    let resolution = match (meta.width, meta.height) {
        (Some(w), Some(h)) => Some((w, h)),
        _ => None,
    };
    let kind = infer_kind(meta.mime_type.as_deref(), meta.codecs.as_deref());
    let segments = parse_segments(scope, &meta);

    StreamVariant {
        playlist_url: meta.base_url,
        bandwidth_bps: meta.bandwidth,
        resolution,
        codecs: meta.codecs.map(|c| vec![c]).unwrap_or_default(),
        kind,
        label,
        segments,
        associated_tracks: vec![],
        is_default: false,
    }
}

fn collect_variants(
    mpd_text: &str,
    mpd_url: &str,
    mpd_duration: Option<f64>,
) -> Vec<StreamVariant> {
    let periods: Vec<&str> = if mpd_text.contains("<Period") {
        mpd_text.split("<Period").skip(1).collect()
    } else {
        vec![mpd_text]
    };
    let mut variants = Vec::new();

    for period_block in periods {
        let period_base = extract_base_url(period_block)
            .map_or_else(|| mpd_url.to_string(), |u| resolve_url(mpd_url, &u));
        for adaptation_block in period_block.split("<AdaptationSet").skip(1) {
            let set_tag_end = adaptation_block.find('>').unwrap_or(0);
            let set_tag = &adaptation_block[..set_tag_end];
            let set_mime = xml_attr_owned(set_tag, "mimeType");
            let set_base_url = extract_base_url(adaptation_block);

            variants.extend(
                adaptation_block
                    .split("<Representation")
                    .skip(1)
                    .map(|rep_block| {
                        build_variant(&RepresentationScope {
                            rep_block,
                            adaptation_block,
                            period_block,
                            mpd_text,
                            mpd_url,
                            period_base: &period_base,
                            set_base_url: set_base_url.as_deref(),
                            set_mime: set_mime.as_deref(),
                            mpd_duration,
                        })
                    }),
            );
        }
    }
    variants
}

fn parse_mpd(mpd_text: &str, mpd_url: &str) -> Vec<StreamVariant> {
    let mpd_tag_end = mpd_text.find('>').unwrap_or(0);
    let mpd_tag = &mpd_text[..mpd_tag_end];
    let mpd_duration =
        xml_attr(mpd_tag, "mediaPresentationDuration").and_then(parse_iso8601_duration);
    let mut variants = collect_variants(mpd_text, mpd_url, mpd_duration);

    if let Some(best) = variants.iter_mut().max_by_key(|v| v.bandwidth_bps) {
        best.is_default = true;
    }
    variants.sort_by_key(|v| Reverse(v.bandwidth_bps));
    variants
}
#[async_trait]
impl MediaExtractor for GenericDashExtractor {
    fn name(&self) -> &'static str {
        "GenericDASH"
    }

    fn can_handle(&self, url: &str) -> bool {
        let lower = url.to_ascii_lowercase();
        let path = lower.split('?').next().unwrap_or(&lower);
        let extension_matches = std::path::Path::new(path)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("mpd"));
        extension_matches || lower.contains("format=mpd") || lower.contains("manifest.mpd")
    }

    async fn extract(
        &self,
        url: &str,
        client: Arc<dyn NetworkClient>,
    ) -> Result<StreamGraph, ExtractorError> {
        debug!(url, "DASH extractor: fetching MPD");
        let mpd_text = fetch_text(url, &client).await?;

        if !mpd_text.contains("<MPD") {
            return Err(ExtractorError::Parse {
                format: "dash",
                reason: "Response does not look like an MPD document".into(),
            });
        }

        let variants = parse_mpd(&mpd_text, url);

        if variants.is_empty() {
            warn!(
                url,
                "DASH extractor: no Representations found, using passthrough"
            );
            return Ok(StreamGraph {
                is_live: false,
                target_duration_secs: None,
                source_url: url.to_string(),
                title: None,
                variants: vec![StreamVariant {
                    playlist_url: url.to_string(),
                    bandwidth_bps: 0,
                    resolution: None,
                    codecs: vec![],
                    kind: StreamKind::Video,
                    label: "DASH stream".to_string(),
                    segments: vec![],
                    associated_tracks: vec![],
                    is_default: true,
                }],
                standalone_tracks: vec![],
                format: "dash".to_string(),
            });
        }

        let count = variants.len();
        debug!(url, count, "DASH extractor: extracted variants");

        Ok(StreamGraph {
            is_live: false,
            target_duration_secs: None,
            source_url: url.to_string(),
            title: None,
            variants,
            standalone_tracks: vec![],
            format: "dash".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adm_network::{HeadInfo, NetworkClient, NetworkError, NetworkRequest, ResponseStream};
    use async_trait::async_trait;
    use std::sync::Arc;

    struct MockResponseStream {
        bytes: Vec<u8>,
        returned: bool,
    }

    #[async_trait]
    impl ResponseStream for MockResponseStream {
        async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
            if self.returned {
                Ok(None)
            } else {
                self.returned = true;
                Ok(Some(self.bytes.clone()))
            }
        }

        fn total_bytes(&self) -> Option<u64> {
            Some(self.bytes.len() as u64)
        }

        async fn cancel(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }
    }

    struct MockClient {
        response_body: Vec<u8>,
    }

    #[async_trait]
    impl NetworkClient for MockClient {
        async fn execute(
            &self,
            _request: NetworkRequest,
        ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
            Ok(Box::new(MockResponseStream {
                bytes: self.response_body.clone(),
                returned: false,
            }))
        }

        async fn head(&self, _url: &str) -> Result<HeadInfo, NetworkError> {
            Ok(HeadInfo {
                content_length: Some(self.response_body.len() as u64),
                accept_ranges: true,
                final_url: String::new(),
            })
        }
    }

    #[tokio::test]
    async fn parse_dash_segment_template_timeline() {
        let mpd = r#"
            <MPD mediaPresentationDuration="PT10S" type="static">
              <Period>
                <AdaptationSet mimeType="video/mp4">
                  <Representation id="1" bandwidth="500000" codecs="avc1.4d401f" width="640" height="360">
                    <SegmentTemplate timescale="1000" startNumber="1" media="seg-$Number$.m4s" initialization="init-$RepresentationID$.mp4">
                      <SegmentTimeline>
                        <S t="0" d="2000" r="1"/>
                      </SegmentTimeline>
                    </SegmentTemplate>
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>
        "#;

        let variants = parse_mpd(mpd, "https://example.com/manifest.mpd");
        assert_eq!(variants.len(), 1);
        let segs = &variants[0].segments;
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0].url, "https://example.com/init-1.mp4");
        assert_eq!(segs[1].url, "https://example.com/seg-1.m4s");
        assert!((segs[1].duration_secs - 2.0).abs() < f64::EPSILON);
        assert_eq!(segs[2].url, "https://example.com/seg-2.m4s");
        assert!((segs[2].duration_secs - 2.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn generic_dash_extracts_segment_base() {
        let mpd = r#"
            <MPD>
              <Period>
                <AdaptationSet>
                  <Representation id="audio" bandwidth="128000">
                    <BaseURL>media/</BaseURL>
                    <SegmentBase indexRange="0-999" initialization="init.mp4" />
                  </Representation>
                </AdaptationSet>
              </Period>
            </MPD>
        "#;

        let extractor = GenericDashExtractor::new();
        let client = Arc::new(MockClient {
            response_body: mpd.as_bytes().to_vec(),
        });
        let graph = extractor
            .extract("https://example.com/manifest.mpd", client)
            .await
            .unwrap();

        assert_eq!(graph.variants.len(), 1);
        let segs = &graph.variants[0].segments;
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].url, "https://example.com/media/init.mp4");
        assert!(segs[0].duration_secs.abs() < f64::EPSILON);
        assert_eq!(segs[1].byte_range, Some((0, 999)));
    }
}

// ── Extractor impl ────────────────────────────────────────────────────────────
