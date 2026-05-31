//! `WebDavClient` — specialized client for WebDAV storage.
//!
//! While WebDAV is built on HTTP, some servers restrict `HEAD` requests or
//! require `PROPFIND` to discover file metadata (size, last modified).
//! This client implements the `PROPFIND` depth-0 fallthrough for reliable
//! metadata discovery.

use crate::{HttpNetworkClient, NetworkClient, NetworkError, NetworkRequest, ResponseStream};
use async_trait::async_trait;
use reqwest::Method;

pub struct WebDavClient {
    http: HttpNetworkClient,
}

impl WebDavClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            http: HttpNetworkClient::new(),
        }
    }

    pub fn from_config(cfg: &crate::http::ClientConfig) -> Result<Self, NetworkError> {
        Ok(Self {
            http: HttpNetworkClient::from_config(cfg)?,
        })
    }

    /// Attempt to get file size via `PROPFIND` Depth: 0.
    /// Returns `(content_length, accept_ranges)` if successful.
    async fn propfind_metadata(&self, url: &str) -> Result<(Option<u64>, bool), NetworkError> {
        let client = &self.http.h1h2_client;

        // PROPFIND Depth 0 gets properties for the resource itself, not its children.
        let resp = client
            .request(Method::from_bytes(b"PROPFIND").unwrap(), url)
            .header("Depth", "0")
            .header("Content-Type", "text/xml; charset=utf-8")
            .body(
                r#"<?xml version="1.0" encoding="utf-8" ?>
<D:propfind xmlns:D="DAV:">
  <D:prop>
    <D:getcontentlength/>
    <D:supportedlock/>
  </D:prop>
</D:propfind>"#,
            )
            .send()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;

        if !resp.status().is_success() {
            return Err(NetworkError::Other(format!(
                "PROPFIND failed: {}",
                resp.status()
            )));
        }

        let body = resp
            .text()
            .await
            .map_err(|e| NetworkError::Other(e.to_string()))?;

        // Simple XML parsing (WebDAV responses are typically small for Depth 0)
        let content_length = if let Some(pos) = body.find("<d:getcontentlength>") {
            let start = pos + "<d:getcontentlength>".len();
            if let Some(end) = body[start..].find("</d:getcontentlength>") {
                body[start..start + end].parse::<u64>().ok()
            } else {
                None
            }
        } else if let Some(pos) = body.find("<D:getcontentlength>") {
            let start = pos + "<D:getcontentlength>".len();
            if let Some(end) = body[start..].find("</D:getcontentlength>") {
                body[start..start + end].parse::<u64>().ok()
            } else {
                None
            }
        } else {
            None
        };

        // Most WebDAV servers support Range if they support GET
        Ok((content_length, true))
    }
}

impl Default for WebDavClient {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NetworkClient for WebDavClient {
    async fn execute(
        &self,
        request: NetworkRequest,
    ) -> Result<Box<dyn ResponseStream + Send + Sync>, NetworkError> {
        // WebDAV Range GET is identical to standard HTTP Range GET.
        self.http.execute(request).await
    }

    async fn head(&self, url: &str) -> Result<crate::HeadInfo, NetworkError> {
        // Try standard HEAD first for performance.
        match self.http.head(url).await {
            Ok(info) if info.content_length.is_some() => Ok(info),
            _ => {
                // Fallback to PROPFIND if HEAD failed to return size.
                tracing::debug!(url = %url, "WebDAV HEAD failed or returned no size — trying PROPFIND");
                let (size, ranges) = self.propfind_metadata(url).await?;
                Ok(crate::HeadInfo {
                    content_length: size,
                    accept_ranges: ranges,
                    final_url: url.to_owned(),
                })
            }
        }
    }
}
