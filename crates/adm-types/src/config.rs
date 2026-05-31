use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub download_dir: PathBuf,
    pub max_parallel_downloads: usize,
    pub max_chunks_per_download: usize,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            download_dir: PathBuf::from("downloads"),
            max_parallel_downloads: 3,
            max_chunks_per_download: 8,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkSettings {
    pub http_proxy: String,
    pub socks5_proxy: String,
    pub request_timeout_secs: u64,
    pub connection_timeout_secs: u64,
    pub max_connections_per_host: usize,
    pub user_agent: String,
    pub follow_redirects: bool,
    pub max_redirects: usize,
    pub tcp_keepalive_secs: Option<u64>,
    pub tcp_nodelay: bool,
    pub pool_idle_timeout_secs: Option<u64>,
    pub accept_encoding: bool,
    pub enable_http3: bool,
}

impl Default for NetworkSettings {
    fn default() -> Self {
        Self {
            http_proxy: String::new(),
            socks5_proxy: String::new(),
            request_timeout_secs: 60,
            connection_timeout_secs: 10,
            max_connections_per_host: 16,
            user_agent: "APEX/1.0".to_owned(),
            follow_redirects: true,
            max_redirects: 10,
            tcp_keepalive_secs: Some(30),
            tcp_nodelay: true,
            pool_idle_timeout_secs: Some(90),
            accept_encoding: true,
            enable_http3: true,
        }
    }
}
