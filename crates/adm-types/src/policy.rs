use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpsMode {
    HttpsOnly,
    AllowHttp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CertificatePinning {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelfSignedPolicy {
    Reject,
    Allow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HostnameVerification {
    Verify,
    Skip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpsPolicy {
    /// Require HTTPS for all connections
    pub mode: HttpsMode,
    /// Minimum TLS version (e.g., "1.2" or "1.3")
    pub min_tls_version: String,
    /// Enable certificate pinning
    pub certificate_pinning: CertificatePinning,
    /// Pinned certificate hashes (SHA256)
    pub pinned_certs: Vec<String>,
    /// Reject self-signed certificates
    pub self_signed: SelfSignedPolicy,
    /// Verify hostname matches certificate
    pub hostname_verification: HostnameVerification,
}

impl Default for HttpsPolicy {
    fn default() -> Self {
        Self {
            mode: HttpsMode::HttpsOnly,
            min_tls_version: "1.2".to_string(),
            certificate_pinning: CertificatePinning::Disabled,
            pinned_certs: vec![],
            self_signed: SelfSignedPolicy::Reject,
            hostname_verification: HostnameVerification::Verify,
        }
    }
}

impl HttpsPolicy {
    #[must_use]
    pub const fn https_only(&self) -> bool {
        matches!(self.mode, HttpsMode::HttpsOnly)
    }

    #[must_use]
    pub const fn pinning_enabled(&self) -> bool {
        matches!(self.certificate_pinning, CertificatePinning::Enabled)
    }

    #[must_use]
    pub const fn reject_self_signed(&self) -> bool {
        matches!(self.self_signed, SelfSignedPolicy::Reject)
    }

    #[must_use]
    pub const fn verify_hostname(&self) -> bool {
        matches!(self.hostname_verification, HostnameVerification::Verify)
    }
}
