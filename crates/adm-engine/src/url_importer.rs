//! URL list import from various formats.
//!
//! Supports importing URLs from:
//! - Plain text files (one URL per line)
//! - M3U/M3U8 playlists
//! - PLS playlists
//! - HTML links (basic parsing)
//! - CSV/TSV formats
//! - JSON arrays

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedUrl {
    pub url: String,
    pub title: Option<String>,
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub enum ImportFormat {
    PlainText,
    M3u,
    M3u8,
    Pls,
    Html,
    Csv,
    Tsv,
    Json,
}

impl ImportFormat {
    #[must_use]
    pub fn detect_from_filename(filename: &str) -> Self {
        let lower = filename.to_lowercase();
        if lower.ends_with(".m3u8") {
            Self::M3u8
        } else if lower.ends_with(".m3u") {
            Self::M3u
        } else if lower.ends_with(".pls") {
            Self::Pls
        } else if lower.ends_with(".html") || lower.ends_with(".htm") {
            Self::Html
        } else if lower.ends_with(".csv") {
            Self::Csv
        } else if lower.ends_with(".tsv") {
            Self::Tsv
        } else if lower.ends_with(".json") {
            Self::Json
        } else {
            Self::PlainText
        }
    }
}

/// Import URLs from file.
pub async fn import_urls(file_path: &Path) -> Result<Vec<ImportedUrl>> {
    let content = tokio::fs::read_to_string(file_path)
        .await
        .context("Failed to read import file")?;

    let format = ImportFormat::detect_from_filename(
        file_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .as_ref(),
    );

    match format {
        ImportFormat::PlainText => parse_plain_text(&content),
        ImportFormat::M3u | ImportFormat::M3u8 => parse_m3u(&content),
        ImportFormat::Pls => parse_pls(&content),
        ImportFormat::Html => parse_html(&content),
        ImportFormat::Csv => parse_csv(&content),
        ImportFormat::Tsv => parse_tsv(&content),
        ImportFormat::Json => parse_json(&content),
    }
}

/// Parse plain text format (one URL per line).
fn parse_plain_text(content: &str) -> Result<Vec<ImportedUrl>> {
    let urls = content
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim().starts_with('#'))
        .map(|line| ImportedUrl {
            url: line.trim().to_string(),
            title: None,
            metadata: None,
        })
        .collect();

    Ok(urls)
}

/// Parse M3U/M3U8 playlist format.
fn parse_m3u(content: &str) -> Result<Vec<ImportedUrl>> {
    let mut urls = Vec::new();
    let mut current_title = None;

    for line in content.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("#EXTINF:") {
            // Extract title from EXTINF line
            if let Some(comma_idx) = trimmed.find(',') {
                current_title = Some(trimmed[comma_idx + 1..].to_string());
            }
        } else if !trimmed.is_empty() && !trimmed.starts_with('#') && trimmed.starts_with("http") {
            urls.push(ImportedUrl {
                url: trimmed.to_string(),
                title: current_title.take(),
                metadata: None,
            });
        }
    }

    Ok(urls)
}

/// Parse PLS playlist format.
fn parse_pls(content: &str) -> Result<Vec<ImportedUrl>> {
    let mut urls = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("File") && trimmed.contains('=') {
            if let Some(url) = trimmed.split('=').nth(1) {
                urls.push(ImportedUrl {
                    url: url.trim().to_string(),
                    title: None,
                    metadata: None,
                });
            }
        }
    }

    Ok(urls)
}

/// Parse HTML for links.
fn parse_html(content: &str) -> Result<Vec<ImportedUrl>> {
    let mut urls = Vec::new();

    // Simple regex-based HTML link extraction
    let re = regex::Regex::new(r#"href=["']([^"']+)["']"#)?;

    for cap in re.captures_iter(content) {
        if let Some(url_match) = cap.get(1) {
            let url = url_match.as_str();
            if url.starts_with("http://") || url.starts_with("https://") {
                urls.push(ImportedUrl {
                    url: url.to_string(),
                    title: None,
                    metadata: None,
                });
            }
        }
    }

    Ok(urls)
}

/// Parse CSV format.
fn parse_csv(content: &str) -> Result<Vec<ImportedUrl>> {
    let mut urls = Vec::new();
    let mut reader = csv::Reader::from_reader(content.as_bytes());

    for result in reader.records() {
        let record = result?;
        if let Some(url) = record.get(0) {
            if url.starts_with("http://") || url.starts_with("https://") {
                let title = record.get(1).map(std::string::ToString::to_string);
                urls.push(ImportedUrl {
                    url: url.to_string(),
                    title,
                    metadata: None,
                });
            }
        }
    }

    Ok(urls)
}

/// Parse TSV format.
fn parse_tsv(content: &str) -> Result<Vec<ImportedUrl>> {
    let mut urls = Vec::new();
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(content.as_bytes());

    for result in reader.records() {
        let record = result?;
        if let Some(url) = record.get(0) {
            if url.starts_with("http://") || url.starts_with("https://") {
                let title = record.get(1).map(std::string::ToString::to_string);
                urls.push(ImportedUrl {
                    url: url.to_string(),
                    title,
                    metadata: None,
                });
            }
        }
    }

    Ok(urls)
}

/// Parse JSON array format.
fn parse_json(content: &str) -> Result<Vec<ImportedUrl>> {
    let json: serde_json::Value = serde_json::from_str(content)?;

    let mut urls = Vec::new();

    if let Some(arr) = json.as_array() {
        for item in arr {
            if let Some(url) = item.as_str() {
                urls.push(ImportedUrl {
                    url: url.to_string(),
                    title: None,
                    metadata: None,
                });
            } else if let Some(obj) = item.as_object() {
                if let Some(serde_json::Value::String(url)) = obj.get("url") {
                    let title = obj
                        .get("title")
                        .and_then(|v| v.as_str())
                        .map(std::string::ToString::to_string);
                    urls.push(ImportedUrl {
                        url: url.clone(),
                        title,
                        metadata: None,
                    });
                }
            }
        }
    }

    Ok(urls)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_text_parsing() {
        let content = r#"
https://example.com/file1.zip
https://example.com/file2.tar.gz
# This is a comment
https://example.com/file3.mp4
"#;
        let urls = parse_plain_text(content).unwrap();
        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0].url, "https://example.com/file1.zip");
    }

    #[test]
    fn test_m3u_parsing() {
        let content = r#"#EXTM3U
#EXTINF:-1, Stream 1
https://stream1.example.com/live
#EXTINF:-1, Stream 2
https://stream2.example.com/live
"#;
        let urls = parse_m3u(content).unwrap();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].title, Some("Stream 1".to_string()));
    }

    #[test]
    fn test_format_detection() {
        assert_eq!(
            std::mem::discriminant(&ImportFormat::detect_from_filename("file.m3u8")),
            std::mem::discriminant(&ImportFormat::M3u8)
        );
        assert_eq!(
            std::mem::discriminant(&ImportFormat::detect_from_filename("list.txt")),
            std::mem::discriminant(&ImportFormat::PlainText)
        );
    }

    #[test]
    fn test_json_parsing() {
        let content = r#"[
            "https://example.com/1.zip",
            {"url": "https://example.com/2.zip", "title": "File 2"}
        ]"#;
        let urls = parse_json(content).unwrap();
        assert_eq!(urls.len(), 2);
        assert_eq!(urls[1].title, Some("File 2".to_string()));
    }
}
