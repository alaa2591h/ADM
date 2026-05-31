//! `FFmpeg` integration for media muxing and transcoding.
//!
//! Provides optional FFmpeg-based AV muxing as an alternative to native concatenation.
//! Enables advanced operations like codec conversion, bitrate adjustment, and format changes.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct FfmpegMuxInput {
    pub video_path: Option<PathBuf>,
    pub audio_path: Option<PathBuf>,
    pub output_path: PathBuf,
    pub codec: FfmpegCodec,
    pub bitrate_video: Option<String>, // e.g., "2M", "5000k"
    pub bitrate_audio: Option<String>, // e.g., "128k", "192k"
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FfmpegCodec {
    H264,
    H265,
    VP9,
    Copy, // pass-through
}

impl FfmpegCodec {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::H264 => "libx264",
            Self::H265 => "libx265",
            Self::VP9 => "libvpx-vp9",
            Self::Copy => "copy",
        }
    }

    #[must_use]
    pub const fn audio_codec(&self) -> &'static str {
        "aac"
    }
}

#[derive(Debug, Clone)]
pub struct FfmpegMuxOutput {
    pub output_path: PathBuf,
    pub duration_secs: f64,
    pub size_bytes: u64,
    pub video_codec: String,
    pub audio_codec: String,
    pub bitrate_kbps: u64,
}

/// Check if `FFmpeg` is available on the system.
pub async fn check_ffmpeg_available() -> bool {
    Command::new("ffmpeg")
        .arg("-version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
}

/// Mux video and audio using `FFmpeg`.
///
/// # Errors
///
/// Returns an error if `FFmpeg` is not available, process spawning fails,
/// `FFmpeg` exits unsuccessfully, or output metadata cannot be read.
pub async fn mux_with_ffmpeg(input: FfmpegMuxInput) -> Result<FfmpegMuxOutput> {
    if !check_ffmpeg_available().await {
        return Err(anyhow::anyhow!("FFmpeg not found in PATH"));
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-y"); // overwrite output

    // Input files
    if let Some(video) = input.video_path.as_ref() {
        cmd.arg("-i").arg(video.to_string_lossy().as_ref());
    }

    if let Some(audio) = input.audio_path.as_ref() {
        cmd.arg("-i").arg(audio.to_string_lossy().as_ref());
    }

    // Video codec and bitrate
    if input.codec == FfmpegCodec::Copy {
        cmd.arg("-c:v").arg("copy");
    } else {
        cmd.arg("-c:v").arg(input.codec.as_str());
    }

    if let Some(vb) = input.bitrate_video.as_ref() {
        cmd.arg("-b:v").arg(vb);
    }

    // Audio codec and bitrate
    cmd.arg("-c:a").arg(input.codec.audio_codec());
    if let Some(ab) = input.bitrate_audio.as_ref() {
        cmd.arg("-b:a").arg(ab);
    }

    // Output format and path
    cmd.arg("-f").arg("mp4");
    cmd.arg(input.output_path.to_string_lossy().as_ref());

    // Suppress banner and enable stats for progress tracking
    cmd.arg("-loglevel").arg("info");
    cmd.arg("-stats_period").arg("1"); // report every 1 second

    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("Failed to spawn ffmpeg")?;

    let stderr = child.stderr.take().context("failed to open stderr")?;
    let reader = BufReader::new(stderr);
    let mut lines = reader.lines();

    let mut duration = 0.0f64;
    let progress = 0.0f64;

    // Log FFmpeg output for debugging
    tokio::spawn(async move {
        while let Ok(Some(line)) = lines.next_line().await {
            if line.contains("Duration:") {
                // Extract duration: "Duration: HH:MM:SS.ms"
                if let Some(start) = line.find("Duration: ") {
                    let rest = &line[start + 10..];
                    if let Some(end) = rest.find(',') {
                        let dur_str = &rest[..end];
                        if let Some(secs) = parse_ffmpeg_duration(dur_str) {
                            duration = secs;
                            tracing::debug!(duration_secs = duration, "FFmpeg duration parsed");
                        }
                    }
                }
            } else if line.contains("time=") {
                tracing::debug!(ffmpeg_log = %line, "FFmpeg progress");
            }
        }
    });

    let status = child.wait().await.context("FFmpeg process error")?;

    if !status.success() {
        return Err(anyhow::anyhow!("FFmpeg exited with status: {status}"));
    }

    let output_metadata = tokio::fs::metadata(&input.output_path)
        .await
        .context("Failed to stat output file")?;
    let size_bytes = output_metadata.len();

    // Probe output for codec info
    let (video_codec, audio_codec, bitrate_kbps) = probe_ffmpeg_output(&input.output_path)
        .await
        .unwrap_or_else(|_| ("h264".to_string(), "aac".to_string(), 0));

    Ok(FfmpegMuxOutput {
        output_path: input.output_path,
        duration_secs: duration,
        size_bytes,
        video_codec,
        audio_codec,
        bitrate_kbps,
    })
}

/// Parse `FFmpeg` duration string "HH:MM:SS.ms" to seconds.
fn parse_ffmpeg_duration(dur: &str) -> Option<f64> {
    let parts: Vec<&str> = dur.split(':').collect();
    if parts.len() != 3 {
        return None;
    }

    let hours = parts[0].parse::<f64>().ok()?;
    let minutes = parts[1].parse::<f64>().ok()?;
    let seconds = parts[2].parse::<f64>().ok()?;

    Some(hours.mul_add(3600.0, minutes * 60.0) + seconds)
}

/// Probe output file for codec and bitrate info.
async fn probe_ffmpeg_output(path: &Path) -> Result<(String, String, u64)> {
    let output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("v:0")
        .arg("-show_entries")
        .arg("stream=codec_name,bit_rate")
        .arg("-of")
        .arg("csv=p=0")
        .arg(path.to_string_lossy().as_ref())
        .output()
        .await
        .context("ffprobe failed")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();

    let video_codec = lines
        .first()
        .map_or_else(|| "h264".to_string(), std::string::ToString::to_string);
    let bitrate = lines
        .get(1)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // Try audio codec
    let audio_output = Command::new("ffprobe")
        .arg("-v")
        .arg("error")
        .arg("-select_streams")
        .arg("a:0")
        .arg("-show_entries")
        .arg("stream=codec_name")
        .arg("-of")
        .arg("csv=p=0")
        .arg(path.to_string_lossy().as_ref())
        .output()
        .await;

    let audio_codec = audio_output
        .ok()
        .and_then(|out| {
            let s = String::from_utf8_lossy(&out.stdout);
            s.trim()
                .lines()
                .next()
                .map(std::string::ToString::to_string)
        })
        .unwrap_or_else(|| "aac".to_string());

    Ok((video_codec, audio_codec, bitrate / 1000))
}

/// Transcode media file with `FFmpeg`.
///
/// # Errors
///
/// Returns an error if muxing/transcoding with `FFmpeg` fails.
pub async fn transcode_with_ffmpeg(
    input_path: &Path,
    output_path: &Path,
    codec: FfmpegCodec,
    bitrate: Option<&str>,
) -> Result<FfmpegMuxOutput> {
    let input = FfmpegMuxInput {
        video_path: Some(input_path.to_path_buf()),
        audio_path: None,
        output_path: output_path.to_path_buf(),
        codec,
        bitrate_video: bitrate.map(std::string::ToString::to_string),
        bitrate_audio: None,
    };

    mux_with_ffmpeg(input).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ffmpeg_duration() {
        assert_eq!(parse_ffmpeg_duration("00:01:30.50"), Some(90.5));
        assert_eq!(parse_ffmpeg_duration("01:00:00.00"), Some(3600.0));
        assert_eq!(parse_ffmpeg_duration("00:00:05.25"), Some(5.25));
    }

    #[tokio::test]
    async fn test_ffmpeg_availability() {
        // This test may fail if ffmpeg is not installed, which is expected
        let available = check_ffmpeg_available().await;
        tracing::info!(ffmpeg_available = available);
    }
}
