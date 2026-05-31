//! TLS/SSL and Certificate Management Tests (Days 11-12)
//!
//! Comprehensive test suite for:
//! - TLS configuration validation
//! - Certificate lifecycle management
//! - Renewal tracking
//! - File operations
//! - HTTPS server initialization

#[cfg(test)]
mod tls_integration_tests {
    use adm_domain::certificate_lifecycle::{
        CertificateGenerationParams, CertificateRenewalStatus, RenewalStatus,
    };
    use adm_domain::tls_config::{CipherSuite, TlsConfig, TlsVersion};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_tls_version_enum() {
        assert_eq!(TlsVersion::TLS12.as_str(), "1.2");
        assert_eq!(TlsVersion::TLS13.as_str(), "1.3");
    }

    #[test]
    fn test_cipher_suite_variants() {
        let _modern = CipherSuite::Modern;
        let _intermediate = CipherSuite::Intermediate;
        let _legacy = CipherSuite::Legacy;
    }

    #[test]
    fn test_localhost_dev_tls_config() {
        let config = TlsConfig::localhost_dev("/tmp/cert.pem", "/tmp/key.pem");

        assert!(config.enabled);
        assert_eq!(config.min_version, TlsVersion::TLS12);
        assert_eq!(config.cipher_suite, CipherSuite::Intermediate);
        assert!(!config.enable_hsts);
        assert!(!config.require_sni);
    }

    #[test]
    fn test_production_tls_config() {
        let config = TlsConfig::production("/etc/certs/cert.pem", "/etc/certs/key.pem");

        assert!(config.enabled);
        assert_eq!(config.min_version, TlsVersion::TLS13);
        assert_eq!(config.cipher_suite, CipherSuite::Modern);
        assert!(config.enable_hsts);
        assert!(config.require_sni);
        assert_eq!(config.hsts_max_age, 31_536_000);
    }

    #[test]
    fn test_cert_pinning_configuration() {
        let mut config = TlsConfig::production("/cert.pem", "/key.pem");
        config.enable_cert_pinning = true;

        // Validation should fail without pins
        assert!(config.validate().is_err());

        // Add pins and validate
        config.pinned_cert_hashes = vec!["abc123def456".to_string()];
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_certificate_generation_params_localhost() {
        let params = CertificateGenerationParams::localhost_self_signed();

        assert_eq!(params.common_name, "localhost");
        assert!(params.san.contains(&"127.0.0.1".to_string()));
        assert!(params.san.contains(&"::1".to_string()));
        assert_eq!(params.key_size, 2048);
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_certificate_generation_params_production() {
        let params = CertificateGenerationParams::production(
            "example.com",
            vec!["example.com".to_string(), "www.example.com".to_string()],
        );

        assert_eq!(params.common_name, "example.com");
        assert_eq!(params.key_size, 4096);
        assert!(params.organization.is_some());
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_params_validation_key_size() {
        let mut params = CertificateGenerationParams::localhost_self_signed();

        // Invalid key size
        params.key_size = 1024;
        assert!(params.validate().is_err());

        params.key_size = 2048;
        assert!(params.validate().is_ok());

        params.key_size = 4096;
        assert!(params.validate().is_ok());
    }

    #[test]
    fn test_params_validation_empty_cn() {
        let mut params = CertificateGenerationParams::localhost_self_signed();
        params.common_name = String::new();

        assert!(params.validate().is_err());
    }

    #[test]
    fn test_params_validation_no_san() {
        let mut params = CertificateGenerationParams::localhost_self_signed();
        params.san.clear();

        assert!(params.validate().is_err());
    }

    #[test]
    fn test_certificate_renewal_status_creation() {
        let status = CertificateRenewalStatus::new();

        assert_eq!(status.status, RenewalStatus::Fresh);
        assert_eq!(status.renewal_attempts, 0);
        assert!(status.last_error.is_none());
        assert!(status.next_renewal > status.last_renewed);
    }

    #[test]
    fn test_certificate_renewal_success() {
        let mut status = CertificateRenewalStatus::new();
        let original_attempts = status.renewal_attempts;

        status.record_attempt(true, None);

        assert_eq!(status.renewal_attempts, original_attempts + 1);
        assert_eq!(status.status, RenewalStatus::Fresh);
        assert!(status.last_error.is_none());
    }

    #[test]
    fn test_certificate_renewal_failure() {
        let mut status = CertificateRenewalStatus::new();

        status.record_attempt(false, Some("Network timeout".to_string()));

        assert_eq!(status.renewal_attempts, 1);
        assert_eq!(status.status, RenewalStatus::Failed);
        assert!(status.last_error.is_some());
        assert!(status.last_error.as_ref().unwrap().contains("Network"));
    }

    #[test]
    fn test_certificate_renewal_status_update_fresh() {
        let mut status = CertificateRenewalStatus::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set next renewal far in future
        status.next_renewal = now + (60 * 24 * 60 * 60); // 60 days
        status.update_status();

        assert_eq!(status.status, RenewalStatus::Fresh);
    }

    #[test]
    fn test_certificate_renewal_status_update_due_soon() {
        let mut status = CertificateRenewalStatus::new();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Set next renewal in 5 days
        status.next_renewal = now + (5 * 24 * 60 * 60);
        status.update_status();

        assert_eq!(status.status, RenewalStatus::DueSoon);
    }

    #[test]
    fn test_certificate_renewal_status_update_overdue() {
        let mut status = CertificateRenewalStatus::new();

        // Set next renewal to past
        status.next_renewal = 0;
        status.update_status();

        assert_eq!(status.status, RenewalStatus::Overdue);
    }

    #[test]
    fn test_certificate_renewal_multiple_attempts() {
        let mut status = CertificateRenewalStatus::new();

        // First failure
        status.record_attempt(false, Some("Error 1".to_string()));
        assert_eq!(status.renewal_attempts, 1);
        assert_eq!(status.status, RenewalStatus::Failed);

        // Second failure
        status.record_attempt(false, Some("Error 2".to_string()));
        assert_eq!(status.renewal_attempts, 2);
        assert_eq!(status.status, RenewalStatus::Failed);

        // Success on third attempt
        status.record_attempt(true, None);
        assert_eq!(status.renewal_attempts, 3);
        assert_eq!(status.status, RenewalStatus::Fresh);
    }

    #[test]
    fn test_tls_configuration_disabled() {
        let mut config = TlsConfig::localhost_dev("/cert.pem", "/key.pem");
        config.enabled = false;

        // Validation should pass even with missing files
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_hsts_configuration() {
        let config = TlsConfig::production("/cert.pem", "/key.pem");

        assert!(config.enable_hsts);
        assert_eq!(config.hsts_max_age, 31_536_000); // 1 year
    }

    #[test]
    fn test_ocsp_stapling_configuration() {
        let config = TlsConfig::localhost_dev("/cert.pem", "/key.pem");
        assert!(!config.enable_ocsp_stapling);

        let config = TlsConfig::production("/cert.pem", "/key.pem");
        assert!(config.enable_ocsp_stapling);
    }

    #[test]
    fn test_sni_requirement_configuration() {
        let config = TlsConfig::localhost_dev("/cert.pem", "/key.pem");
        assert!(!config.require_sni);

        let config = TlsConfig::production("/cert.pem", "/key.pem");
        assert!(config.require_sni);
    }
}

#[cfg(test)]
mod certificate_file_manager_tests {
    use adm_domain::certificate_lifecycle::CertificateFileManager;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn test_initialize_cert_dir() {
        let temp = TempDir::new().unwrap();
        let result = CertificateFileManager::initialize_cert_dir(temp.path());

        assert!(result.is_ok());
        assert!(temp.path().join("active").exists());
        assert!(temp.path().join("archived").exists());
        assert!(temp.path().join("backups").exists());
    }

    #[test]
    fn test_backup_missing_certificate() {
        let result =
            CertificateFileManager::backup_certificate(&PathBuf::from("/nonexistent/cert.pem"));

        assert!(result.is_err());
    }

    #[test]
    fn test_list_empty_certificates() {
        let temp = TempDir::new().unwrap();
        let certs = CertificateFileManager::list_certificates(temp.path());

        assert!(certs.is_ok());
        assert_eq!(certs.unwrap().len(), 0);
    }
}

#[cfg(test)]
mod https_server_tests {
    use crate::https_server::HttpsServerBuilder;
    use adm_domain::tls_config::TlsConfig;
    use std::net::SocketAddr;
    use std::str::FromStr;
    fn test_https_server_builder_complete() {
        let addr = SocketAddr::from_str("127.0.0.1:8443").unwrap();
        let config = TlsConfig::localhost_dev("/tmp/cert.pem", "/tmp/key.pem");

        let result = HttpsServerBuilder::new()
            .addr(addr)
            .tls_config(config)
            .build();

        assert!(result.is_ok());
        let server = result.unwrap();
        assert_eq!(server.addr(), addr);
    }

    #[test]
    fn test_https_server_builder_missing_config() {
        let addr = SocketAddr::from_str("127.0.0.1:8443").unwrap();

        let result = HttpsServerBuilder::new().addr(addr).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_https_server_builder_default() {
        let builder = HttpsServerBuilder::default();
        let result = builder.build();

        assert!(result.is_err());
    }
}

#[cfg(test)]
mod security_configuration_tests {
    use adm_domain::security_config::SecurityConfig;

    #[test]
    fn test_security_config_development() {
        let config = SecurityConfig::development();

        assert_eq!(config.rate_limiting.global_rps, 1000);
        assert_eq!(config.api.max_request_size, 104_857_600);
    }

    #[test]
    fn test_security_config_production() {
        let config = SecurityConfig::production();

        assert_eq!(config.rate_limiting.global_rps, 100);
        assert_eq!(config.api.max_request_size, 5_242_880);
        assert!(config.api.enforce_https);
    }

    #[test]
    fn test_security_config_validate() {
        let config = SecurityConfig::production();
        assert!(config.validate().is_ok());
    }
}
