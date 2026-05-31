//! Extended checksum and integrity verification for downloads.
//!
//! Supports multiple hash algorithms (MD5, SHA1, SHA256, SHA512, CRC32)
//! with automatic detection and verification.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha512,
    Crc32,
}

impl ChecksumAlgorithm {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Md5 => "md5",
            Self::Sha1 => "sha1",
            Self::Sha256 => "sha256",
            Self::Sha512 => "sha512",
            Self::Crc32 => "crc32",
        }
    }

    #[must_use]
    pub const fn expected_length(&self) -> usize {
        match self {
            Self::Md5 => 32,
            Self::Sha1 => 40,
            Self::Sha256 => 64,
            Self::Sha512 => 128,
            Self::Crc32 => 8,
        }
    }

    /// Auto-detect algorithm from hash string length.
    #[must_use]
    pub const fn from_hash_length(length: usize) -> Self {
        match length {
            32 => Self::Md5,
            40 => Self::Sha1,
            128 => Self::Sha512,
            8 => Self::Crc32,
            _ => Self::Sha256, // Default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumResult {
    pub algorithm: ChecksumAlgorithm,
    pub hash: String,
    pub file_path: String,
    pub file_size: u64,
    pub duration_secs: f64,
}

#[derive(Debug, Clone)]
pub struct ChecksumVerification {
    pub expected_algorithm: ChecksumAlgorithm,
    pub expected_hash: String,
    pub actual_hash: String,
    pub is_valid: bool,
}

/// Calculate file checksum with specified algorithm.
///
/// # Errors
///
/// Returns an error if the file cannot be opened, metadata cannot be read, or
/// reading from the file fails while hashing.
pub async fn calculate_checksum(
    file_path: &Path,
    algorithm: ChecksumAlgorithm,
) -> Result<ChecksumResult> {
    let file = File::open(file_path)
        .await
        .context("Failed to open file for checksum")?;
    let metadata = file
        .metadata()
        .await
        .context("Failed to get file metadata")?;
    let file_size = metadata.len();

    let reader = BufReader::new(file);
    let start = std::time::Instant::now();

    let hash = match algorithm {
        ChecksumAlgorithm::Md5 => Box::pin(calculate_md5(reader)).await?,
        ChecksumAlgorithm::Sha1 => Box::pin(calculate_sha1(reader)).await?,
        ChecksumAlgorithm::Sha256 => Box::pin(calculate_sha256(reader)).await?,
        ChecksumAlgorithm::Sha512 => Box::pin(calculate_sha512(reader)).await?,
        ChecksumAlgorithm::Crc32 => Box::pin(calculate_crc32(reader)).await?,
    };

    let duration = start.elapsed().as_secs_f64();

    Ok(ChecksumResult {
        algorithm,
        hash,
        file_path: file_path.to_string_lossy().to_string(),
        file_size,
        duration_secs: duration,
    })
}

/// Verify file checksum against expected hash.
///
/// # Errors
///
/// Returns an error if checksum calculation fails.
pub async fn verify_checksum(
    file_path: &Path,
    expected_hash: &str,
    algorithm: Option<ChecksumAlgorithm>,
) -> Result<ChecksumVerification> {
    let algo =
        algorithm.unwrap_or_else(|| ChecksumAlgorithm::from_hash_length(expected_hash.len()));

    let result = Box::pin(calculate_checksum(file_path, algo.clone())).await?;
    let actual_hash = result.hash.to_lowercase();
    let expected_hash_lower = expected_hash.to_lowercase();
    let is_valid = actual_hash == expected_hash_lower;

    Ok(ChecksumVerification {
        expected_algorithm: algo,
        expected_hash: expected_hash.to_string(),
        actual_hash,
        is_valid,
    })
}

/// Calculate MD5 hash of file.
async fn calculate_md5<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<String> {
    use md5::{Digest, Md5};

    let mut hasher = Md5::new();
    let mut buf = vec![0_u8; 65_536];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Calculate SHA1 hash of file.
async fn calculate_sha1<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<String> {
    use sha1::{Digest, Sha1};

    let mut hasher = Sha1::new();
    let mut buf = vec![0_u8; 65_536];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Calculate SHA256 hash of file.
async fn calculate_sha256<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<String> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; 65_536];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Calculate SHA512 hash of file.
async fn calculate_sha512<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<String> {
    use sha2::{Digest, Sha512};

    let mut hasher = Sha512::new();
    let mut buf = vec![0_u8; 65_536];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Calculate CRC32 checksum of file.
async fn calculate_crc32<R: AsyncReadExt + Unpin>(mut reader: R) -> Result<String> {
    use crc32fast::Hasher;

    let mut hasher = Hasher::new();
    let mut buf = vec![0_u8; 65_536];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(format!("{:08x}", hasher.finalize()))
}

// ── Post-download hook ──────────────────────────────────────────────────────

/// Outcome of a post-download file integrity check.
///
/// Returned by [`post_download_verify`] so callers can distinguish a hard
/// checksum mismatch (retriable) from an I/O or hash-computation error
/// (non-fatal) without unwrapping a nested `Result`.
#[derive(Debug, Clone)]
pub enum VerifyOutcome {
    /// Computed hash matches the expected hash — file is intact.
    Valid,
    /// Computed hash does NOT match the expected hash — file is corrupted.
    Mismatch {
        /// The expected hash provided by the caller (lower-cased).
        expected: String,
        /// The hash actually computed from the file (lower-cased).
        actual: String,
    },
    /// File I/O or hash-computation error.
    ///
    /// Treated as **non-fatal**: the scheduler logs the error and continues
    /// as though the checksum passed rather than failing the download for an
    /// infrastructure reason (e.g. disk read error mid-hash).
    Error(String),
}

/// Post-download hook: verifies the assembled file against the expected hash.
///
/// Call this once all chunks have been written and flushed to disk.
/// The function is intentionally infallible: all outcomes (match, mismatch,
/// I/O error) are encoded in the returned [`VerifyOutcome`] so the scheduler
/// can handle each case without matching nested `Result` types.
///
/// # Algorithm selection
///
/// If `algorithm` is `None` the algorithm is auto-detected from the hash
/// string length via [`ChecksumAlgorithm::from_hash_length`].  Pass an
/// explicit value when the hash length is ambiguous (e.g. a 64-char CRC32
/// collision).
///
/// # Parameters
///
/// * `file_path`   — path of the fully-assembled download file.
/// * `expected_hash` — hex-encoded expected hash (case-insensitive).
/// * `algorithm`   — optional explicit algorithm override.
pub async fn post_download_verify(
    file_path: &Path,
    expected_hash: &str,
    algorithm: Option<ChecksumAlgorithm>,
) -> VerifyOutcome {
    match Box::pin(verify_checksum(file_path, expected_hash, algorithm)).await {
        Ok(v) if v.is_valid => VerifyOutcome::Valid,
        Ok(v) => VerifyOutcome::Mismatch {
            expected: v.expected_hash,
            actual: v.actual_hash,
        },
        Err(e) => VerifyOutcome::Error(e.to_string()),
    }
}

/// Calculate multiple checksums for a file (for comprehensive verification).
///
/// # Errors
///
/// Returns an error if any individual checksum calculation fails.
pub async fn calculate_multi_checksum(file_path: &Path) -> Result<Vec<ChecksumResult>> {
    let algorithms = vec![
        ChecksumAlgorithm::Md5,
        ChecksumAlgorithm::Sha1,
        ChecksumAlgorithm::Sha256,
    ];

    let mut results = Vec::new();
    for algo in algorithms {
        let result = Box::pin(calculate_checksum(file_path, algo)).await?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_algorithm_detection() {
        // MD5: 32 chars
        let algo = ChecksumAlgorithm::from_hash_length(32);
        assert_eq!(algo, ChecksumAlgorithm::Md5);

        // SHA256: 64 chars
        let algo = ChecksumAlgorithm::from_hash_length(64);
        assert_eq!(algo, ChecksumAlgorithm::Sha256);
    }

    #[tokio::test]
    async fn test_checksum_calculation() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();

        let result = calculate_checksum(file.path(), ChecksumAlgorithm::Sha256)
            .await
            .unwrap();
        assert_eq!(result.algorithm, ChecksumAlgorithm::Sha256);
        assert!(!result.hash.is_empty());
        assert!(result.duration_secs >= 0.0);
    }

    #[test]
    fn test_algorithm_lengths() {
        assert_eq!(ChecksumAlgorithm::Md5.expected_length(), 32);
        assert_eq!(ChecksumAlgorithm::Sha1.expected_length(), 40);
        assert_eq!(ChecksumAlgorithm::Sha256.expected_length(), 64);
        assert_eq!(ChecksumAlgorithm::Sha512.expected_length(), 128);
    }
}
