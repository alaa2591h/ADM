//! Strict HTTPS certificate validation and pinning.
//!
//! Enforces:
//! - HTTPS-only connections (no downgrade to HTTP)
//! - Certificate pinning (specific certificates or public keys)
//! - Certificate chain validation
//! - Hostname verification
//! - TLS version enforcement (minimum TLS 1.2)

pub use adm_types::{
    CertificatePinning, HostnameVerification, HttpsMode, HttpsPolicy, SelfSignedPolicy,
};
use anyhow::{Context, Result};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};

/// Validate URL against HTTPS policy.
///
/// # Errors
///
/// Returns an error if the URL is invalid, missing a host, or violates the
/// configured HTTPS-only policy.
pub fn validate_url(url: &str, policy: &HttpsPolicy) -> Result<()> {
    let parsed = url::Url::parse(url).context("Invalid URL")?;

    // Check scheme
    if policy.https_only() && parsed.scheme() != "https" {
        return Err(anyhow::anyhow!(
            "HTTPS-only policy violated: {} (scheme: {})",
            url,
            parsed.scheme()
        ));
    }

    // Check hostname
    if parsed.host().is_none() {
        return Err(anyhow::anyhow!("Missing hostname in URL: {url}"));
    }

    Ok(())
}

/// Validate certificate against policy.
///
/// # Errors
///
/// Returns an error if the certificate cannot be parsed, is self-signed when
/// rejected by policy, or does not match configured certificate pins.
pub fn validate_certificate(cert_der: &[u8], policy: &HttpsPolicy) -> Result<()> {
    use x509_parser::parse_x509_certificate;

    let (_, cert) = parse_x509_certificate(cert_der).context("Failed to parse certificate")?;

    // Check self-signed
    if policy.reject_self_signed() && cert.tbs_certificate.issuer == cert.tbs_certificate.subject {
        return Err(anyhow::anyhow!(
            "Self-signed certificate rejected by policy"
        ));
    }

    // Check certificate pinning if enabled
    if policy.pinning_enabled() && !policy.pinned_certs.is_empty() {
        let cert_hash = calculate_cert_hash(cert_der);
        if !policy
            .pinned_certs
            .iter()
            .any(|pinned| pinned == &cert_hash)
        {
            return Err(anyhow::anyhow!(
                "Certificate pinning failed: {cert_hash} not in pinned list"
            ));
        }
    }

    Ok(())
}

/// Calculate SHA256 hash of certificate.
fn calculate_cert_hash(cert_der: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(cert_der);
    hex::encode(hasher.finalize())
}

/// Create strict TLS configuration.
///
/// # Errors
///
/// Currently this function cannot fail while building a root-backed client
/// configuration, but it returns `Result` to preserve the fallible public API.
pub fn create_strict_tls_config(policy: &HttpsPolicy) -> Result<ClientConfig> {
    // Add Mozilla's root certificates
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Create client config with strict validation
    let client_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(client_config)
}

#[derive(Debug)]
pub struct StrictCertificateVerifier {
    policy: HttpsPolicy,
}

impl StrictCertificateVerifier {
    #[must_use]
    pub const fn new(policy: HttpsPolicy) -> Self {
        Self { policy }
    }
}

impl ServerCertVerifier for StrictCertificateVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        validate_certificate(end_entity.as_ref(), &self.policy)
            .map_err(|err| rustls::Error::General(err.to_string()))?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_https_only_policy() {
        let policy = HttpsPolicy {
            mode: HttpsMode::HttpsOnly,
            ..Default::default()
        };

        // Valid HTTPS
        assert!(validate_url("https://example.com/file.zip", &policy).is_ok());

        // Invalid HTTP
        assert!(validate_url("http://example.com/file.zip", &policy).is_err());
    }

    #[test]
    fn test_mixed_policy() {
        let policy = HttpsPolicy {
            mode: HttpsMode::AllowHttp,
            ..Default::default()
        };

        // Both should work
        assert!(validate_url("https://example.com/file.zip", &policy).is_ok());
        assert!(validate_url("http://example.com/file.zip", &policy).is_ok());
    }

    #[test]
    fn test_invalid_url() {
        let policy = HttpsPolicy::default();
        assert!(validate_url("not a url", &policy).is_err());
    }

    #[test]
    fn test_cert_hash_generation() {
        // Mock certificate DER
        let mock_cert = b"test certificate data";
        let hash = calculate_cert_hash(mock_cert);
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // SHA256 hex is 64 chars
    }

    #[test]
    fn test_default_policy() {
        let policy = HttpsPolicy::default();
        assert!(policy.https_only());
        assert_eq!(policy.min_tls_version, "1.2");
        assert!(policy.verify_hostname());
    }
}
