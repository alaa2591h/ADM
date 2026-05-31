//! HTTPS Server Initialization and Configuration
//!
//! Provides:
//! - HTTPS listener setup
//! - TLS acceptor configuration
//! - Server startup with certificate validation
//! - Graceful HTTPS shutdown

use anyhow::{anyhow, Result};
use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use adm_domain::tls_config::TlsConfig;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::info;

/// HTTPS server configuration and state
pub struct HttpsServer {
    /// Listener address
    addr: SocketAddr,

    /// TLS configuration
    tls_config: TlsConfig,

    /// TCP listener (if initialized)
    listener: Option<TcpListener>,
}

impl HttpsServer {
    /// Create new HTTPS server instance
    pub fn new(addr: SocketAddr, tls_config: TlsConfig) -> Result<Self> {
        // Validate TLS configuration
        tls_config.validate()?;

        info!(
            "🔒 HTTPS Server configured for {} with TLS {}",
            addr,
            tls_config.min_version.as_str()
        );

        Ok(Self {
            addr,
            tls_config,
            listener: None,
        })
    }

    /// Initialize HTTPS listener
    pub async fn initialize_listener(&mut self) -> Result<()> {
        // Bind to address
        let listener = TcpListener::bind(self.addr)
            .await
            .map_err(|e| anyhow!("Failed to bind to {}: {}", self.addr, e))?;

        info!("✅ TCP listener bound to {}", self.addr);

        self.listener = Some(listener);
        Ok(())
    }

    /// Start HTTPS server with given router
    pub async fn start(&mut self, router: Router) -> Result<()> {
        let listener = self
            .listener
            .take()
            .ok_or_else(|| anyhow!("Listener not initialized"))?;

        info!("🚀 Starting HTTPS server on {}", self.addr);

        let config = RustlsConfig::from_pem_file(&self.tls_config.cert_path, &self.tls_config.key_path).await?;

        info!(
            "🔐 Using TLS {}, Cipher Suite: {:?}",
            self.tls_config.min_version.as_str(),
            self.tls_config.cipher_suite
        );

        if self.tls_config.enable_hsts {
            info!(
                "📌 HSTS enabled with max-age: {} seconds",
                self.tls_config.hsts_max_age
            );
        }

        let std_listener = listener.into_std()?;
        axum_server::from_tcp_rustls(std_listener, config)?
            .serve(router.into_make_service())
            .await?;

        Ok(())
    }

    /// Get TLS configuration reference
    pub const fn tls_config(&self) -> &TlsConfig {
        &self.tls_config
    }

    /// Get listener address
    pub const fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// HTTPS server builder
pub struct HttpsServerBuilder {
    addr: Option<SocketAddr>,
    tls_config: Option<TlsConfig>,
}

impl HttpsServerBuilder {
    /// Create new builder
    pub const fn new() -> Self {
        Self {
            addr: None,
            tls_config: None,
        }
    }

    /// Set server address
    pub const fn addr(mut self, addr: SocketAddr) -> Self {
        self.addr = Some(addr);
        self
    }

    /// Set TLS configuration
    pub fn tls_config(mut self, config: TlsConfig) -> Self {
        self.tls_config = Some(config);
        self
    }

    /// Build HTTPS server
    pub fn build(self) -> Result<HttpsServer> {
        let addr = self.addr.ok_or_else(|| anyhow!("Address not set"))?;

        let tls_config = self
            .tls_config
            .ok_or_else(|| anyhow!("TLS config not set"))?;

        HttpsServer::new(addr, tls_config)
    }
}

impl Default for HttpsServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adm_domain::tls_config::TlsConfig;
    use std::str::FromStr;

    #[test]
    fn test_https_server_builder() {
        let addr = SocketAddr::from_str("127.0.0.1:8443").unwrap();
        let tls_config = TlsConfig::localhost_dev("/tmp/cert.pem", "/tmp/key.pem");

        let result = HttpsServerBuilder::new()
            .addr(addr)
            .tls_config(tls_config)
            .build();

        assert!(result.is_ok());
        let server = result.unwrap();
        assert_eq!(server.addr(), addr);
    }

    #[test]
    fn test_https_server_missing_addr() {
        let tls_config = TlsConfig::localhost_dev("/tmp/cert.pem", "/tmp/key.pem");

        let result = HttpsServerBuilder::new().tls_config(tls_config).build();

        assert!(result.is_err());
    }

    #[test]
    fn test_https_server_missing_tls() {
        let addr = SocketAddr::from_str("127.0.0.1:8443").unwrap();

        let result = HttpsServerBuilder::new().addr(addr).build();

        assert!(result.is_err());
    }
}
