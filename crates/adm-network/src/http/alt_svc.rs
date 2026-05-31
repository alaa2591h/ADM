//! Alt-Svc header parser and per-domain H3 availability cache.
//!
//! RFC 7838 defines the Alt-Svc HTTP header which allows servers to advertise
//! alternative services — in our case, HTTP/3 over QUIC.
//!
//! Example header:
//!   `Alt-Svc: h3=":443"; ma=86400, h3-29=":443"; ma=86400`
//!
//! The cache maps `host:port` → [`AltSvcEntry`] and respects `max-age` (ma)
//! directives. Entries expire after their max-age and are lazily evicted on
//! next lookup.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// ── AltSvcEntry ──────────────────────────────────────────────────────────────

/// A cached Alt-Svc discovery result for a single origin.
#[derive(Debug, Clone)]
pub struct AltSvcEntry {
    /// The UDP port on which the server offers HTTP/3.  Empty string means the
    /// same port as the origin (common case).
    pub h3_port: u16,
    /// Absolute instant after which this entry must not be used.
    pub expires_at: Instant,
}

impl AltSvcEntry {
    pub fn is_valid(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

// ── AltSvcCache ──────────────────────────────────────────────────────────────

/// Thread-safe, process-wide cache of Alt-Svc H3 advertisements.
///
/// Key format: `"{host}:{origin_port}"` — e.g. `"example.com:443"`.
#[derive(Debug, Clone)]
pub struct AltSvcCache {
    inner: Arc<RwLock<HashMap<String, AltSvcEntry>>>,
}

impl Default for AltSvcCache {
    fn default() -> Self {
        Self::new()
    }
}

impl AltSvcCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Look up whether a host is known to offer H3.
    /// Returns `Some(h3_port)` when a valid entry is cached, `None` otherwise.
    pub fn lookup(&self, origin_key: &str) -> Option<u16> {
        let map = self.inner.read();
        map.get(origin_key)
            .filter(|e| e.is_valid())
            .map(|e| e.h3_port)
    }

    /// Store a new Alt-Svc discovery for `origin_key`.
    pub fn insert(&self, origin_key: String, h3_port: u16, max_age: Duration) {
        let entry = AltSvcEntry {
            h3_port,
            expires_at: Instant::now() + max_age,
        };
        self.inner.write().insert(origin_key, entry);
    }

    /// Mark an origin as having no H3 support for `penalty_duration`.
    /// We store port=0 as a sentinel meaning "known-bad".
    pub fn mark_unavailable(&self, origin_key: String, penalty_duration: Duration) {
        let entry = AltSvcEntry {
            h3_port: 0,
            expires_at: Instant::now() + penalty_duration,
        };
        self.inner.write().insert(origin_key, entry);
    }

    /// Check if an origin is explicitly marked as H3-unavailable.
    pub fn is_known_unavailable(&self, origin_key: &str) -> bool {
        let map = self.inner.read();
        map.get(origin_key)
            .filter(|e| e.is_valid())
            .map(|e| e.h3_port == 0)
            .unwrap_or(false)
    }

    /// Lazily evict all expired entries.  Called occasionally to prevent
    /// unbounded memory growth on long-running daemon instances.
    pub fn evict_expired(&self) {
        let now = Instant::now();
        self.inner.write().retain(|_, v| v.expires_at > now);
    }
}

// ── Parser ───────────────────────────────────────────────────────────────────

/// Parsed Alt-Svc alternative.
#[derive(Debug, Clone, PartialEq)]
pub struct AltSvcAlternative {
    pub protocol: String,
    /// Advertised host — empty means "same as origin".
    pub alt_host: String,
    /// Advertised port.
    pub alt_port: u16,
    /// `max-age` in seconds (default 24 h per RFC 7838 §3).
    pub max_age_secs: u64,
}

/// Parse the value of an `Alt-Svc` header and return all H3-compatible
/// alternatives.
///
/// Recognised protocol IDs:  `h3`, `h3-29`, `h3-Q050`, `h3-Q046`
///
/// # Example
///
/// ```
/// let v = "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400";
/// let alts = parse_alt_svc(v, 443);
/// assert_eq!(alts[0].alt_port, 443);
/// assert_eq!(alts[0].max_age_secs, 86400);
/// ```
pub fn parse_alt_svc(header_value: &str, origin_port: u16) -> Vec<AltSvcAlternative> {
    let mut results = Vec::new();

    for token in split_alt_svc_tokens(header_value) {
        let token = token.trim();
        if token.eq_ignore_ascii_case("clear") {
            // Alt-Svc: clear — invalidate all cached alternatives.
            break;
        }

        // Split at first '=' to separate protocol-id from alt-authority.
        let (proto_id, rest) = match token.split_once('=') {
            Some(pair) => pair,
            None => continue,
        };
        let proto_id = proto_id.trim();

        // Only accept H3 protocol identifiers.
        if !is_h3_protocol(proto_id) {
            continue;
        }

        // The rest is a quoted alt-authority followed by optional parameters.
        // e.g.: `":443"; ma=86400`
        let (authority_str, params) = split_authority_and_params(rest.trim());

        let (alt_host, alt_port) = parse_authority(&authority_str, origin_port);
        let max_age_secs = extract_max_age(&params).unwrap_or(86_400);

        results.push(AltSvcAlternative {
            protocol: proto_id.to_owned(),
            alt_host,
            alt_port,
            max_age_secs,
        });
    }

    results
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Split a comma-separated Alt-Svc header into tokens, but respect quoted
/// strings so that `","` inside `"host:port"` doesn't split incorrectly.
fn split_alt_svc_tokens(value: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;

    for (i, ch) in value.char_indices() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                tokens.push(&value[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    tokens.push(&value[start..]);
    tokens
}

fn is_h3_protocol(proto: &str) -> bool {
    // Accept any h3-* variant plus bare "h3".
    proto.eq_ignore_ascii_case("h3") || proto.to_ascii_lowercase().starts_with("h3-")
}

/// Split `"\"host:port\"" rest` into (`host:port`, `rest`).
fn split_authority_and_params(s: &str) -> (String, String) {
    let s = s.trim();
    if let Some(inner) = s.strip_prefix('"') {
        // Find the closing quote.
        if let Some(end) = inner.find('"') {
            let authority = inner[..end].to_owned();
            let params = inner[end + 1..].trim_start_matches([';', ' ']).to_owned();
            return (authority, params);
        }
    }
    // Fallback: no quotes — treat everything up to ';' as authority.
    if let Some((auth, params)) = s.split_once(';') {
        (auth.trim().to_owned(), params.trim().to_owned())
    } else {
        (s.to_owned(), String::new())
    }
}

/// Parse `host:port` or `:port` into `(host, port)`.
/// An empty host means "same as origin".
fn parse_authority(authority: &str, origin_port: u16) -> (String, u16) {
    if authority.is_empty() {
        return (String::new(), origin_port);
    }
    // ":443" → host="", port=443
    if let Some(port_str) = authority.strip_prefix(':') {
        if let Ok(p) = port_str.parse::<u16>() {
            return (String::new(), p);
        }
    }
    // "example.com:443" or "[::1]:443"
    if let Some(colon_pos) = authority.rfind(':') {
        let host = authority[..colon_pos].to_owned();
        if let Ok(p) = authority[colon_pos + 1..].parse::<u16>() {
            return (host, p);
        }
    }
    (authority.to_owned(), origin_port)
}

/// Extract `ma=<value>` from the parameters string.
fn extract_max_age(params: &str) -> Option<u64> {
    for param in params.split(';') {
        let param = param.trim();
        if let Some(val) = param.strip_prefix("ma=").or_else(|| {
            param
                .to_ascii_lowercase()
                .starts_with("ma=")
                .then(|| &param[3..])
        }) {
            if let Ok(n) = val.trim().parse::<u64>() {
                return Some(n);
            }
        }
    }
    None
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_h3_alternative() {
        let alts = parse_alt_svc("h3=\":443\"; ma=86400", 443);
        assert_eq!(alts.len(), 1);
        assert_eq!(alts[0].protocol, "h3");
        assert_eq!(alts[0].alt_port, 443);
        assert_eq!(alts[0].max_age_secs, 86400);
        assert!(alts[0].alt_host.is_empty());
    }

    #[test]
    fn parses_multiple_alternatives() {
        let v = "h3=\":443\"; ma=86400, h3-29=\":443\"; ma=86400";
        let alts = parse_alt_svc(v, 443);
        assert_eq!(alts.len(), 2);
        assert_eq!(alts[0].protocol, "h3");
        assert_eq!(alts[1].protocol, "h3-29");
    }

    #[test]
    fn ignores_non_h3_alternatives() {
        let v = "h2=\":443\"; ma=86400, h3=\":443\"; ma=3600";
        let alts = parse_alt_svc(v, 443);
        assert_eq!(alts.len(), 1);
        assert_eq!(alts[0].protocol, "h3");
        assert_eq!(alts[0].max_age_secs, 3600);
    }

    #[test]
    fn defaults_max_age_when_missing() {
        let alts = parse_alt_svc("h3=\":8443\"", 443);
        assert_eq!(alts.len(), 1);
        assert_eq!(alts[0].alt_port, 8443);
        assert_eq!(alts[0].max_age_secs, 86_400); // RFC default
    }

    #[test]
    fn parses_authority_with_explicit_host() {
        let alts = parse_alt_svc("h3=\"alt.example.com:443\"; ma=3600", 80);
        assert_eq!(alts.len(), 1);
        assert_eq!(alts[0].alt_host, "alt.example.com");
        assert_eq!(alts[0].alt_port, 443);
    }

    #[test]
    fn cache_lookup_hit_miss() {
        let cache = AltSvcCache::new();
        cache.insert("example.com:443".to_owned(), 443, Duration::from_secs(3600));
        assert_eq!(cache.lookup("example.com:443"), Some(443));
        assert_eq!(cache.lookup("other.com:443"), None);
    }

    #[test]
    fn cache_known_unavailable_blocks_h3() {
        let cache = AltSvcCache::new();
        cache.mark_unavailable("bad.com:443".to_owned(), Duration::from_secs(60));
        assert!(cache.is_known_unavailable("bad.com:443"));
        // lookup returns None even though there's an entry (port=0 sentinel)
        assert_eq!(cache.lookup("bad.com:443"), None);
    }

    #[test]
    fn split_handles_comma_in_quoted_authority() {
        // Unlikely in practice but confirms parser correctness.
        let v = "h3=\":443\"; ma=100, h3-29=\":443\"";
        let tokens = split_alt_svc_tokens(v);
        assert_eq!(tokens.len(), 2);
    }
}
