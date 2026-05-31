//! Clipboard monitoring for automatic URL detection and download.
//!
//! Continuously monitors system clipboard for URLs and can:
//! - Auto-detect pasted download links
//! - Notify user of detected URLs
//! - Optionally auto-queue downloads
//! - Filter by domain whitelist/blacklist

use anyhow::Result;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardMonitorConfig {
    pub enabled: bool,
    pub poll_interval_ms: u64,
    pub auto_download: bool,
    pub url_pattern: String, // regex pattern to match URLs
    pub domain_whitelist: Vec<String>,
    pub domain_blacklist: Vec<String>,
}

impl Default for ClipboardMonitorConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_ms: 1000,
            auto_download: false,
            url_pattern: r#"https?://[^\s]+"#.to_string(),
            domain_whitelist: vec![],
            domain_blacklist: ["localhost", "127.0.0.1"].map(str::to_string).to_vec(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClipboardEvent {
    pub id: String,
    pub url: String,
    pub timestamp: i64,
    pub source: String,
    pub is_valid: bool,
}

pub struct ClipboardMonitor {
    config: ClipboardMonitorConfig,
    tx: mpsc::Sender<ClipboardEvent>,
    compiled_pattern: Option<Regex>,
}

impl ClipboardMonitor {
    #[must_use]
    pub fn new(config: ClipboardMonitorConfig) -> (Self, mpsc::Receiver<ClipboardEvent>) {
        let (tx, rx) = mpsc::channel(100);
        let compiled_pattern = Regex::new(&config.url_pattern).ok();
        (Self { config, tx, compiled_pattern }, rx)
    }

    /// Start monitoring clipboard in background task.
    pub fn start(self) {
        if !self.config.enabled {
            tracing::info!("Clipboard monitoring disabled");
            return;
        }

        tokio::spawn(async move {
            self.monitor_loop().await;
        });
    }

    async fn monitor_loop(self) {
        let mut last_clipboard = String::new();
        let poll_duration = Duration::from_millis(self.config.poll_interval_ms);

        loop {
            tokio::time::sleep(poll_duration).await;

            match get_clipboard_text() {
                Ok(current) => {
                    if current != last_clipboard && !current.is_empty() {
                        last_clipboard.clone_from(&current);

                        // Check if clipboard contains URLs
                        let urls = if let Some(ref re) = self.compiled_pattern {
                            let found: Vec<String> = re.find_iter(&current)
                                .map(|m| m.as_str().to_string())
                                .collect();
                            if found.is_empty() { None } else { Some(found) }
                        } else {
                            None
                        };
                        if let Some(urls) = urls {
                            for url in urls {
                                if self.is_url_valid(&url) {
                                    let event = ClipboardEvent {
                                        id: Uuid::new_v4().to_string(),
                                        url: url.clone(),
                                        timestamp: chrono::Utc::now().timestamp(),
                                        source: "clipboard".to_string(),
                                        is_valid: true,
                                    };

                                    if let Err(e) = self.tx.send(event.clone()).await {
                                        tracing::warn!("Failed to send clipboard event: {}", e);
                                    }

                                    tracing::info!(url = %url, "Clipboard URL detected");
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to read clipboard: {}", e);
                }
            }
        }
    }

    fn is_url_valid(&self, url: &str) -> bool {
        // Parse URL and check domain filters
        if let Ok(parsed) = url::Url::parse(url) {
            let domain = parsed.host_str().unwrap_or("");

            // Check blacklist
            if self
                .config
                .domain_blacklist
                .iter()
                .any(|d| domain.contains(d))
            {
                return false;
            }

            // Check whitelist if not empty
            if !self.config.domain_whitelist.is_empty() {
                return self
                    .config
                    .domain_whitelist
                    .iter()
                    .any(|d| domain.contains(d));
            }

            true
        } else {
            false
        }
    }
}

/// Get current clipboard text (platform-specific).
#[cfg(target_os = "windows")]
fn get_clipboard_text() -> Result<String> {
    // Windows implementation using clipboard crate
    use std::process::Command;

    let output = Command::new("powershell")
        .arg("-Command")
        .arg("Get-Clipboard")
        .output()?;

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(target_os = "macos")]
fn get_clipboard_text() -> Result<String> {
    use std::process::Command;

    let output = Command::new("pbpaste").output()?;
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(target_os = "linux")]
fn get_clipboard_text() -> Result<String> {
    use std::process::Command;

    // Try xclip first, then xsel, then fallback
    let output = Command::new("xclip")
        .arg("-selection")
        .arg("clipboard")
        .arg("-o")
        .output();

    if let Ok(out) = output {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        let output = Command::new("xsel")
            .arg("--clipboard")
            .arg("--output")
            .output()?;
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
fn get_clipboard_text() -> Result<String> {
    Err(anyhow::anyhow!("Clipboard not supported on this platform"))
}

/// Extract URLs from text using regex pattern.
fn extract_urls(text: &str, pattern: &str) -> Option<Vec<String>> {
    let Ok(re) = Regex::new(pattern) else {
        return None;
    };

    let urls: Vec<String> = re.find_iter(text).map(|m| m.as_str().to_string()).collect();

    if urls.is_empty() {
        None
    } else {
        Some(urls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_extraction() {
        let text =
            "Check this out: https://example.com/file.zip and https://another.com/data.tar.gz";
        let urls = extract_urls(text, r#"https?://[^\s]+"#);

        assert!(urls.is_some());
        let urls = urls.unwrap();
        assert_eq!(urls.len(), 2);
        assert!(urls[0].contains("example.com"));
    }

    #[test]
    fn test_clipboard_event_creation() {
        let event = ClipboardEvent {
            id: Uuid::new_v4().to_string(),
            url: "https://example.com/file.zip".to_string(),
            timestamp: 0,
            source: "clipboard".to_string(),
            is_valid: true,
        };

        assert_eq!(event.source, "clipboard");
        assert!(event.is_valid);
    }

    #[tokio::test]
    async fn test_clipboard_config() {
        let config = ClipboardMonitorConfig::default();
        assert!(config.enabled);
        assert!(!config.domain_blacklist.is_empty());
    }
}
