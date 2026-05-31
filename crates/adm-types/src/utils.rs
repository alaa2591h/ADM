use std::time::{SystemTime, UNIX_EPOCH};

#[must_use]
pub fn unix_millis() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_millis();

    u64::try_from(millis).unwrap_or(u64::MAX)
}

#[must_use]
pub fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::ZERO)
        .as_secs()
}

/// Derive a safe filename from a URL.
#[must_use]
pub fn derive_filename_from_url(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| {
            u.path_segments()
                .and_then(|mut segments| segments.next_back())
                .map(std::string::ToString::to_string)
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "download.bin".to_string())
}
