use anyhow::{anyhow, Result};

/// Normalize a header name for comparison: ASCII-lowercase.
fn normalize_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// Basic validation for header name. Reject control chars, colon, and empty names.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("empty header name"));
    }
    if name
        .chars()
        .any(|c| c == ':' || c == '\r' || c == '\n' || c == '\0')
    {
        return Err(anyhow!("invalid header name"));
    }
    Ok(())
}

/// Basic validation for header value. Reject CR/LF and NUL to avoid header injection.
fn validate_value(val: &str) -> Result<()> {
    if val.chars().any(|c| c == '\r' || c == '\n' || c == '\0') {
        return Err(anyhow!("invalid header value"));
    }
    Ok(())
}

/// Merge `task_headers` into the provided `existing` header vector.
/// - Comparison is case-insensitive.
/// - For the `cookie` header we concatenate values with `; ` to preserve both.
/// - For other headers, task headers overwrite existing ones.
pub fn merge_into_existing(
    existing: &mut Vec<(String, String)>,
    task_headers: &[(String, String)],
) -> Result<()> {
    // Build a map of normalized name -> index in existing
    use std::collections::HashMap;
    let mut idx_map: HashMap<String, usize> = HashMap::new();
    for (i, (k, _)) in existing.iter().enumerate() {
        idx_map.insert(normalize_name(k), i);
    }

    for (k, v) in task_headers {
        validate_name(k)?;
        validate_value(v)?;
        let nk = normalize_name(k);
        if nk == "cookie" {
            if let Some(&pos) = idx_map.get(&nk) {
                let existing_val = &existing[pos].1;
                // Combine cookies preserving both sides; task headers appended
                let combined = if existing_val.is_empty() {
                    v.clone()
                } else if v.is_empty() {
                    existing_val.clone()
                } else {
                    format!("{existing_val}; {v}")
                };
                existing[pos].1 = combined;
            } else {
                existing.push((k.clone(), v.clone()));
                idx_map.insert(nk, existing.len() - 1);
            }
        } else {
            if let Some(&pos) = idx_map.get(&nk) {
                existing[pos].0.clone_from(k);
                existing[pos].1.clone_from(v);
            } else {
                existing.push((k.clone(), v.clone()));
                idx_map.insert(nk, existing.len() - 1);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_overwrite_and_cookie_concat() {
        let mut existing = vec![
            ("User-Agent".to_string(), "DefaultUA/1.0".to_string()),
            ("Cookie".to_string(), "a=1".to_string()),
            ("X-Trace".to_string(), "trace1".to_string()),
        ];

        let task = vec![
            ("user-agent".to_string(), "TaskUA/2.0".to_string()),
            ("Cookie".to_string(), "b=2".to_string()),
            ("X-New".to_string(), "val".to_string()),
        ];

        merge_into_existing(&mut existing, &task).expect("merge should succeed");

        // User-Agent should be replaced by task value (case-insensitive)
        assert!(existing
            .iter()
            .any(|(k, v)| k == "user-agent" || k == "User-Agent" && v == "TaskUA/2.0"));
        // Cookie should be combined
        assert!(existing
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("cookie") && v == "a=1; b=2"));
        // X-New should be present
        assert!(existing.iter().any(|(k, v)| k == "X-New" && v == "val"));
    }

    #[test]
    fn test_invalid_header_name() {
        let mut existing: Vec<(String, String)> = Vec::new();
        let task = vec![(":bad".to_string(), "v".to_string())];
        assert!(merge_into_existing(&mut existing, &task).is_err());
    }

    #[test]
    fn test_invalid_header_value() {
        let mut existing: Vec<(String, String)> = Vec::new();
        let task = vec![("X-Ok".to_string(), "bad\nval".to_string())];
        assert!(merge_into_existing(&mut existing, &task).is_err());
    }
}

#[cfg(test)]
#[path = "headers_runtime_tests.rs"]
mod headers_runtime_tests;
