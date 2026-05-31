#[cfg(test)]
mod runtime_tests {
    use super::*;
    use crate::{NetworkClient, NetworkRequest};
    use std::sync::Arc;
    use tokio::sync::Mutex as AsyncMutex;

    struct CaptureClient {
        last: Arc<AsyncMutex<Option<NetworkRequest>>>,
    }

    #[async_trait::async_trait]
    impl crate::NetworkClient for CaptureClient {
        async fn execute(
            &self,
            request: NetworkRequest,
        ) -> Result<Box<dyn crate::ResponseStream + Send + Sync>, crate::NetworkError> {
            let mut guard = self.last.lock().await;
            *guard = Some(request.clone());
            // Use the existing MockNetworkClient to return a trivial stream
            let mock = crate::network::MockNetworkClient {
                data: b"ok".to_vec(),
                chunk_size: 4,
                fail_at: None,
            };
            let s = mock
                .execute(request)
                .await
                .map_err(|e| crate::NetworkError::Other(e.to_string()))?;
            Ok(s)
        }

        async fn head(&self, url: &str) -> Result<crate::HeadInfo, crate::NetworkError> {
            Ok(crate::HeadInfo {
                content_length: None,
                accept_ranges: false,
                final_url: url.to_string(),
            })
        }
    }

    #[tokio::test]
    async fn auth_headers_propagation_and_cookie_concat() -> anyhow::Result<()> {
        let captured = Arc::new(AsyncMutex::new(None));
        let client = Arc::new(CaptureClient {
            last: captured.clone(),
        });

        let mut request = NetworkRequest::new("https://example.com/file", None);
        request
            .headers
            .push(("Cookie".to_string(), "a=1".to_string()));
        request
            .headers
            .push(("User-Agent".to_string(), "DefaultUA/1.0".to_string()));

        let mut task = crate::DownloadTask::new("https://example.com/file");
        task.headers
            .push(("Authorization".to_string(), "Bearer XYZ".to_string()));
        task.headers.push(("Cookie".to_string(), "b=2".to_string()));

        crate::headers::merge_into_existing(&mut request.headers, &task.headers)?;

        let _ = client.execute(request.clone()).await?;

        let guard = captured.lock().await;
        let seen = guard.as_ref().expect("request should be captured");
        assert!(seen
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("authorization") && v == "Bearer XYZ"));
        assert!(seen
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("cookie") && v == "a=1; b=2"));
        Ok(())
    }

    #[test]
    fn priority_and_speed_limit_roundtrip() -> anyhow::Result<()> {
        let mut task = crate::DownloadTask::new("https://example.com/file");
        task.priority = 42;
        task.speed_limit_kbps = Some(128);

        let persisted = task.to_persisted();
        let restored = crate::DownloadTask::from_persisted(persisted)?;

        assert_eq!(restored.priority, 42);
        assert_eq!(restored.speed_limit_kbps, Some(128));
        Ok(())
    }

    #[test]
    fn batch_isolation_duplicate_overwrite_behavior() -> anyhow::Result<()> {
        let mut req1 = NetworkRequest::new("https://a/1", None);
        let mut req2 = NetworkRequest::new("https://a/2", None);

        let t1 = vec![("Authorization".to_string(), "TokenA".to_string())];
        let t2 = vec![("Authorization".to_string(), "TokenB".to_string())];

        crate::headers::merge_into_existing(&mut req1.headers, &t1)?;
        crate::headers::merge_into_existing(&mut req2.headers, &t2)?;

        assert!(req1
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("authorization") && v == "TokenA"));
        assert!(req2
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("authorization") && v == "TokenB"));
        Ok(())
    }
}
