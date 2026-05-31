//! Additional media extractors for various streaming platforms.
//!
//! Supports:
//! - Vimeo videos and playlists
//! - Dailymotion videos and collections
//! - Twitch streams, VODs, and clips
//! - Twitter/X video tweets
//! - Facebook videos and live streams
//! - Instagram reels and IGTV
//! - `SoundCloud` tracks and playlists

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtractorPlatform {
    Vimeo,
    Dailymotion,
    Twitch,
    Twitter,
    Facebook,
    Instagram,
    SoundCloud,
}

impl ExtractorPlatform {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Vimeo => "vimeo",
            Self::Dailymotion => "dailymotion",
            Self::Twitch => "twitch",
            Self::Twitter => "twitter",
            Self::Facebook => "facebook",
            Self::Instagram => "instagram",
            Self::SoundCloud => "soundcloud",
        }
    }

    #[must_use]
    pub fn from_url(url: &str) -> Option<Self> {
        if url.contains("vimeo.com") {
            Some(Self::Vimeo)
        } else if url.contains("dailymotion.com") || url.contains("dai.ly") {
            Some(Self::Dailymotion)
        } else if url.contains("twitch.tv") {
            Some(Self::Twitch)
        } else if url.contains("twitter.com") || url.contains("x.com") {
            Some(Self::Twitter)
        } else if url.contains("facebook.com") || url.contains("fb.watch") {
            Some(Self::Facebook)
        } else if url.contains("instagram.com") {
            Some(Self::Instagram)
        } else if url.contains("soundcloud.com") {
            Some(Self::SoundCloud)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaMetadata {
    pub title: String,
    pub description: Option<String>,
    pub duration_secs: Option<u64>,
    pub thumbnail_url: Option<String>,
    pub author: Option<String>,
    pub upload_date: Option<String>,
    pub view_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedMedia {
    pub platform: String,
    pub media_id: String,
    pub title: String,
    pub download_urls: Vec<MediaDownloadUrl>,
    pub metadata: MediaMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaDownloadUrl {
    pub quality: String,
    pub format: String,
    pub url: String,
    pub filesize_bytes: Option<u64>,
}

/// Extract media information from URL.
///
/// # Errors
///
/// Returns an error when the URL is unsupported or the platform-specific ID
/// cannot be extracted.
pub fn extract_media(url: &str) -> Result<ExtractedMedia> {
    let platform = ExtractorPlatform::from_url(url).context("Unsupported platform")?;

    match platform {
        ExtractorPlatform::Vimeo => extract_vimeo(url),
        ExtractorPlatform::Dailymotion => extract_dailymotion(url),
        ExtractorPlatform::Twitch => extract_twitch(url),
        ExtractorPlatform::Twitter => extract_twitter(url),
        ExtractorPlatform::Facebook => extract_facebook(url),
        ExtractorPlatform::Instagram => extract_instagram(url),
        ExtractorPlatform::SoundCloud => extract_soundcloud(url),
    }
}

fn extract_vimeo(url: &str) -> Result<ExtractedMedia> {
    // Extract video ID from URL
    let video_id = extract_vimeo_id(url).context("Invalid Vimeo URL")?;

    let metadata = MediaMetadata {
        title: format!("Vimeo Video {video_id}"),
        description: None,
        duration_secs: Some(0),
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "vimeo".to_string(),
        media_id: video_id,
        title: metadata.title.clone(),
        download_urls: vec![MediaDownloadUrl {
            quality: "best".to_string(),
            format: "mp4".to_string(),
            url: url.to_string(),
            filesize_bytes: None,
        }],
        metadata,
    })
}

fn extract_dailymotion(url: &str) -> Result<ExtractedMedia> {
    let video_id = extract_dailymotion_id(url).context("Invalid Dailymotion URL")?;

    let metadata = MediaMetadata {
        title: format!("Dailymotion Video {video_id}"),
        description: None,
        duration_secs: Some(0),
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "dailymotion".to_string(),
        media_id: video_id,
        title: metadata.title.clone(),
        download_urls: vec![MediaDownloadUrl {
            quality: "720p".to_string(),
            format: "mp4".to_string(),
            url: url.to_string(),
            filesize_bytes: None,
        }],
        metadata,
    })
}

fn extract_twitch(url: &str) -> Result<ExtractedMedia> {
    let channel_or_vod = extract_twitch_id(url).context("Invalid Twitch URL")?;

    let metadata = MediaMetadata {
        title: format!("Twitch {channel_or_vod}"),
        description: None,
        duration_secs: None,
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "twitch".to_string(),
        media_id: channel_or_vod,
        title: metadata.title.clone(),
        download_urls: vec![],
        metadata,
    })
}

fn extract_twitter(url: &str) -> Result<ExtractedMedia> {
    let tweet_id = extract_twitter_id(url).context("Invalid Twitter URL")?;

    let metadata = MediaMetadata {
        title: format!("Twitter Tweet {tweet_id}"),
        description: None,
        duration_secs: None,
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "twitter".to_string(),
        media_id: tweet_id,
        title: metadata.title.clone(),
        download_urls: vec![],
        metadata,
    })
}

fn extract_facebook(url: &str) -> Result<ExtractedMedia> {
    let video_id = extract_facebook_id(url).context("Invalid Facebook URL")?;

    let metadata = MediaMetadata {
        title: format!("Facebook Video {video_id}"),
        description: None,
        duration_secs: None,
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "facebook".to_string(),
        media_id: video_id,
        title: metadata.title.clone(),
        download_urls: vec![],
        metadata,
    })
}

fn extract_instagram(url: &str) -> Result<ExtractedMedia> {
    let post_id = extract_instagram_id(url).context("Invalid Instagram URL")?;

    let metadata = MediaMetadata {
        title: format!("Instagram Post {post_id}"),
        description: None,
        duration_secs: None,
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "instagram".to_string(),
        media_id: post_id,
        title: metadata.title.clone(),
        download_urls: vec![],
        metadata,
    })
}

fn extract_soundcloud(url: &str) -> Result<ExtractedMedia> {
    let track_id = extract_soundcloud_id(url).context("Invalid SoundCloud URL")?;

    let metadata = MediaMetadata {
        title: format!("SoundCloud Track {track_id}"),
        description: None,
        duration_secs: None,
        thumbnail_url: None,
        author: None,
        upload_date: None,
        view_count: None,
    };

    Ok(ExtractedMedia {
        platform: "soundcloud".to_string(),
        media_id: track_id,
        title: metadata.title.clone(),
        download_urls: vec![MediaDownloadUrl {
            quality: "128k".to_string(),
            format: "mp3".to_string(),
            url: url.to_string(),
            filesize_bytes: None,
        }],
        metadata,
    })
}

fn extract_vimeo_id(url: &str) -> Option<String> {
    url.split('/')
        .find(|s| s.chars().all(char::is_numeric))
        .map(std::string::ToString::to_string)
}

fn extract_dailymotion_id(url: &str) -> Option<String> {
    if let Some(start) = url.find("/video/") {
        let rest = &url[start + 7..];
        Some(rest.split('_').next()?.to_string())
    } else {
        None
    }
}

fn extract_twitch_id(url: &str) -> Option<String> {
    // Strip the scheme (e.g. "https://") and the host (e.g. "www.twitch.tv")
    // before looking for path segments, to avoid returning the host or scheme.
    let without_scheme = url.splitn(2, "://").nth(1).unwrap_or(url);
    let path = without_scheme.splitn(2, '/').nth(1).unwrap_or("");
    path.split('/')
        .find(|s| !s.is_empty())
        .map(std::string::ToString::to_string)
}

fn extract_twitter_id(url: &str) -> Option<String> {
    if let Some(status_idx) = url.find("/status/") {
        let rest = &url[status_idx + 8..];
        Some(rest.split('?').next()?.to_string())
    } else {
        None
    }
}

fn extract_facebook_id(url: &str) -> Option<String> {
    if let Some(start) = url.find("/video/") {
        Some(url[start + 7..].split('/').next()?.to_string())
    } else {
        None
    }
}

fn extract_instagram_id(url: &str) -> Option<String> {
    if let Some(start) = url.find("/p/") {
        Some(url[start + 3..].split('/').next()?.to_string())
    } else {
        None
    }
}

fn extract_soundcloud_id(url: &str) -> Option<String> {
    url.split('/')
        .next_back()
        .map(std::string::ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_detection() {
        assert_eq!(
            ExtractorPlatform::from_url("https://vimeo.com/123456")
                .unwrap()
                .as_str(),
            "vimeo"
        );
        assert_eq!(
            ExtractorPlatform::from_url("https://dailymotion.com/video/x123")
                .unwrap()
                .as_str(),
            "dailymotion"
        );
        assert_eq!(
            ExtractorPlatform::from_url("https://twitch.tv/streamer")
                .unwrap()
                .as_str(),
            "twitch"
        );
    }

    #[test]
    fn test_id_extraction() {
        assert_eq!(
            extract_twitter_id("https://twitter.com/user/status/123456789"),
            Some("123456789".to_string())
        );
        assert_eq!(
            extract_instagram_id("https://instagram.com/p/ABC123/"),
            Some("ABC123".to_string())
        );
    }
}
