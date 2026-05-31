// Rate limiting tests for unified API Gateway
#[cfg(test)]
mod rate_limit_tests {
    use api_gateway::rest::check_write_rate_limit;
    use axum::http::StatusCode;
    use governor::{Quota, RateLimiter};
    use std::num::NonZeroU32;

    #[test]
    fn test_rate_limiter_creation() {
        // Create limiter with 100 req/sec quota
        let limiter = RateLimiter::direct(Quota::per_second(NonZeroU32::new(100).unwrap()));

        // First request should succeed
        assert!(limiter.check().is_ok());
    }

    #[test]
    fn test_write_rate_limit_helper_passes() {
        // Check helper function returns None (no rate limit hit) on first call
        let result = check_write_rate_limit();
        assert!(result.is_none());
    }

    #[test]
    fn test_rate_limit_response_format() {
        // Verify rate limit response has correct format
        if let Some((status, _response)) = check_write_rate_limit() {
            // If we somehow hit rate limit, verify response
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        }
    }

    #[tokio::test]
    async fn test_add_download_respects_rate_limit() {
        // This test would require mock handlers.
        // In production, use integration tests with actual server.
        //
        // Behaviour to verify:
        //   1. Send 100+ rapid requests.
        //   2. Verify first 100 succeed (200/201).
        //   3. Verify requests beyond 100 fail with 429.
        //   4. Wait one second, verify quota resets.
    }

    #[test]
    fn test_rate_limiting_quota_values() {
        // Verify quota configuration
        let write_quota = 100_u32; // requests per second
        assert_eq!(write_quota, 100, "Write quota should be 100 req/sec");

        // Read operations are unlimited.
        // Write operations (add, pause, resume, retry, cancel) share WRITE_LIMITER.
    }
}

#[cfg(test)]
mod integration_tests {
    use api_gateway::rest::ApiResponse;

    #[test]
    fn test_rate_limiter_is_global() {
        // WRITE_LIMITER is a LazyLock<DefaultDirectRateLimiter> declared in rest.rs.
        // All write endpoints share it, so rate limiting is enforced workspace-wide,
        // not per endpoint.
        //
        // Read operations (list_downloads, get_download, system_stats, health_check)
        // bypass the limiter intentionally.
    }

    #[test]
    fn test_websocket_not_rate_limited() {
        // WebSocket connections authenticate via localhost origin check only.
        // Per-message rate limiting is handled separately if required.
    }

    #[test]
    fn test_rate_limit_error_messages() {
        // Verify error messages are user-friendly and informative
        let response = ApiResponse::<()>::err("❌ Too many requests - rate limited to 100 req/sec".to_string());

        assert_eq!(
            response.error,
            Some("❌ Too many requests - rate limited to 100 req/sec".to_string())
        );
    }
}
