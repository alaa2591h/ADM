use anyhow::{anyhow, Result};
use std::path::PathBuf;
use url::Url;

pub struct UrlValidator;
impl UrlValidator {
    /// Validates a URL string.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or the scheme is unsupported.
    pub fn validate(url_str: &str) -> Result<Url> {
        let url = Url::parse(url_str).map_err(|e| anyhow!("Invalid URL: {e}"))?;
        if !matches!(url.scheme(), "http" | "https" | "ftp" | "sftp") {
            return Err(anyhow!("Unsupported scheme: {}", url.scheme()));
        }
        Ok(url)
    }
}

pub struct FilePathValidator;
impl FilePathValidator {
    /// Validates an absolute file path.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is not absolute.
    pub fn validate(path: &str) -> Result<PathBuf> {
        let p = PathBuf::from(path);
        if p.is_relative() {
            return Err(anyhow!("Path must be absolute"));
        }
        Ok(p)
    }
}
