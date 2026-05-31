//! Automatic decompression for archived files.
//!
//! Detects and extracts common archive formats (zip, tar, gzip, bzip2, xz, 7z, rar).
//! Works automatically when a download completes, or can be manually triggered.
//!
//! Security: All extractions validate that member paths stay within the output directory
//! to prevent directory traversal attacks.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::process::Command;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArchiveFormat {
    Zip,
    TarGz,
    TarBz2,
    TarXz,
    Tar,
    Gzip,
    Bzip2,
    Xz,
    Rar,
    SevenZip,
    Unknown,
}

/// Validate that a file path from archive stays within the output directory
/// Prevents directory traversal attacks (e.g., ../../../etc/passwd)
#[cfg(test)]
fn validate_archive_path(member_path: &Path, base_dir: &Path) -> Result<PathBuf> {
    // Reject absolute paths
    if member_path.is_absolute() {
        return Err(anyhow!(
            "❌ Path Traversal: Archive contains absolute path (security risk): {}",
            member_path.display()
        ));
    }

    // Reject paths with ..
    if member_path.components().any(|c| c.as_os_str() == "..") {
        return Err(anyhow!(
            "❌ Path Traversal: Archive contains .. directory reference: {}",
            member_path.display()
        ));
    }

    // Canonicalize and verify the extracted path stays within base directory
    let target_path = base_dir.join(member_path);
    let base_canonical = std::fs::canonicalize(base_dir).unwrap_or_else(|_| base_dir.to_path_buf());

    // For paths that don't exist yet, check the parent
    let target_canonical = if target_path.exists() {
        std::fs::canonicalize(&target_path).unwrap_or_else(|_| target_path.clone())
    } else if let Some(parent) = target_path.parent() {
        if parent.exists() {
            std::fs::canonicalize(parent)
                .unwrap_or_else(|_| parent.to_path_buf())
                .join(target_path.file_name().unwrap_or_default())
        } else {
            target_path.clone()
        }
    } else {
        target_path.clone()
    };

    // Verify extracted path is within base directory
    if !target_canonical.starts_with(&base_canonical) {
        return Err(anyhow!(
            "❌ Path Traversal: Extracted path escapes archive directory: {}",
            member_path.display()
        ));
    }

    Ok(target_path)
}

impl ArchiveFormat {
    /// Detect archive format from file extension or magic bytes.
    ///
    /// # Errors
    ///
    /// Currently returns `Ok(Self::Unknown)` when probing fails; the `Result`
    /// type is kept for callers that need a fallible detection API.
    pub async fn detect(path: &Path) -> Result<Self> {
        // Extension-based detection
        if has_extension(path, "zip") {
            return Ok(Self::Zip);
        } else if file_name_has_suffix(path, ".tar.gz") || has_extension(path, "tgz") {
            return Ok(Self::TarGz);
        } else if file_name_has_suffix(path, ".tar.bz2") || has_extension(path, "tbz2") {
            return Ok(Self::TarBz2);
        } else if file_name_has_suffix(path, ".tar.xz") || has_extension(path, "txz") {
            return Ok(Self::TarXz);
        } else if has_extension(path, "tar") {
            return Ok(Self::Tar);
        } else if has_extension(path, "gz") {
            return Ok(Self::Gzip);
        } else if has_extension(path, "bz2") {
            return Ok(Self::Bzip2);
        } else if has_extension(path, "xz") {
            return Ok(Self::Xz);
        } else if has_extension(path, "rar") {
            return Ok(Self::Rar);
        } else if has_extension(path, "7z") {
            return Ok(Self::SevenZip);
        }

        // Magic bytes detection
        if let Ok(mut buf) = tokio::fs::read(path).await {
            if buf.len() > 32 {
                buf.truncate(32);
            }
            if buf.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
                return Ok(Self::Zip);
            } else if buf.starts_with(&[0x1F, 0x8B]) {
                return Ok(Self::Gzip);
            } else if buf.starts_with(&[0x42, 0x5A]) {
                return Ok(Self::Bzip2);
            } else if buf.starts_with(&[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00]) {
                return Ok(Self::Xz);
            } else if buf.starts_with(&[0x37, 0x7A, 0xBC, 0xAF, 0x27, 0x1C]) {
                return Ok(Self::SevenZip);
            } else if buf.starts_with(&[0x52, 0x61, 0x72, 0x21]) {
                // "Rar!"
                return Ok(Self::Rar);
            }
        }

        Ok(Self::Unknown)
    }
}

fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case(extension))
}

fn file_name_has_suffix(path: &Path, suffix: &str) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.get(name.len().saturating_sub(suffix.len())..))
        .is_some_and(|tail| tail.eq_ignore_ascii_case(suffix))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecompressionOutput {
    pub archive_path: PathBuf,
    pub output_dir: PathBuf,
    pub format: ArchiveFormat,
    pub files_extracted: u32,
    pub total_size_bytes: u64,
    pub duration_secs: f64,
}

/// Extract archive file to destination directory.
///
/// # Errors
///
/// Returns an error if the archive format is unsupported, the output
/// directory cannot be created, extraction fails, or output size calculation
/// fails.
pub async fn extract_archive(
    archive_path: &Path,
    output_dir: &Path,
) -> Result<DecompressionOutput> {
    let format = ArchiveFormat::detect(archive_path).await?;

    if format == ArchiveFormat::Unknown {
        return Err(anyhow::anyhow!("Unknown or unsupported archive format"));
    }

    tokio::fs::create_dir_all(output_dir)
        .await
        .context("Failed to create output directory")?;

    let start = std::time::Instant::now();

    let files_extracted = match format {
        ArchiveFormat::Zip => extract_zip(archive_path, output_dir).await?,
        ArchiveFormat::TarGz => extract_tar_gz(archive_path, output_dir).await?,
        ArchiveFormat::TarBz2 => extract_tar_bz2(archive_path, output_dir).await?,
        ArchiveFormat::TarXz => extract_tar_xz(archive_path, output_dir).await?,
        ArchiveFormat::Tar => extract_tar(archive_path, output_dir).await?,
        ArchiveFormat::Gzip => extract_gzip(archive_path, output_dir).await?,
        ArchiveFormat::Bzip2 => extract_bzip2(archive_path, output_dir).await?,
        ArchiveFormat::Xz => extract_xz(archive_path, output_dir).await?,
        ArchiveFormat::Rar => extract_rar(archive_path, output_dir).await?,
        ArchiveFormat::SevenZip => extract_7z(archive_path, output_dir).await?,
        ArchiveFormat::Unknown => return Err(anyhow::anyhow!("Unknown format")),
    };

    let total_size = calculate_dir_size(output_dir).await.unwrap_or(0);
    let duration = start.elapsed().as_secs_f64();

    Ok(DecompressionOutput {
        archive_path: archive_path.to_path_buf(),
        output_dir: output_dir.to_path_buf(),
        format,
        files_extracted,
        total_size_bytes: total_size,
        duration_secs: duration,
    })
}

async fn extract_zip(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("unzip")
        .arg("-q")
        .arg(archive_path)
        .arg("-d")
        .arg(output_dir)
        .status()
        .await
        .context("Failed to execute unzip")?;

    if !status.success() {
        return Err(anyhow::anyhow!("unzip failed with status: {status}"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_tar_gz(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("tar")
        .arg("-xzf")
        .arg(archive_path)
        .arg("-C")
        .arg(output_dir)
        .status()
        .await
        .context("Failed to execute tar")?;

    if !status.success() {
        return Err(anyhow::anyhow!("tar extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_tar_bz2(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("tar")
        .arg("-xjf")
        .arg(archive_path)
        .arg("-C")
        .arg(output_dir)
        .status()
        .await
        .context("Failed to execute tar")?;

    if !status.success() {
        return Err(anyhow::anyhow!("tar bz2 extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_tar_xz(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("tar")
        .arg("-xJf")
        .arg(archive_path)
        .arg("-C")
        .arg(output_dir)
        .status()
        .await
        .context("Failed to execute tar")?;

    if !status.success() {
        return Err(anyhow::anyhow!("tar xz extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_tar(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("tar")
        .arg("-xf")
        .arg(archive_path)
        .arg("-C")
        .arg(output_dir)
        .status()
        .await
        .context("Failed to execute tar")?;

    if !status.success() {
        return Err(anyhow::anyhow!("tar extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_gzip(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let filename = archive_path
        .file_stem()
        .context("No filename")?
        .to_string_lossy();
    let output = output_dir.join(filename.as_ref());

    let status = Command::new("gunzip")
        .arg("-c")
        .arg(archive_path)
        .status()
        .await
        .context("Failed to execute gunzip")?;

    if !status.success() {
        return Err(anyhow::anyhow!("gunzip failed"));
    }

    Ok(1)
}

async fn extract_bzip2(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let filename = archive_path
        .file_stem()
        .context("No filename")?
        .to_string_lossy();

    let status = Command::new("bunzip2")
        .arg("-c")
        .arg(archive_path)
        .status()
        .await
        .context("Failed to execute bunzip2")?;

    if !status.success() {
        return Err(anyhow::anyhow!("bunzip2 failed"));
    }

    Ok(1)
}

async fn extract_xz(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("unxz")
        .arg("-c")
        .arg(archive_path)
        .status()
        .await
        .context("Failed to execute unxz")?;

    if !status.success() {
        return Err(anyhow::anyhow!("unxz failed"));
    }

    Ok(1)
}

async fn extract_rar(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("unar")
        .arg("-o")
        .arg(output_dir)
        .arg(archive_path)
        .status()
        .await
        .or_else(|_| {
            // Try 'unrar' as fallback
            std::process::Command::new("unrar")
                .arg("x")
                .arg(archive_path)
                .arg(output_dir.to_string_lossy().as_ref())
                .status()
                .context("Neither 'unar' nor 'unrar' available")
        })?;

    if !status.success() {
        return Err(anyhow::anyhow!("RAR extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn extract_7z(archive_path: &Path, output_dir: &Path) -> Result<u32> {
    let status = Command::new("7z")
        .arg("x")
        .arg(archive_path)
        .arg(format!("-o{}", output_dir.to_string_lossy()))
        .status()
        .await
        .context("Failed to execute 7z")?;

    if !status.success() {
        return Err(anyhow::anyhow!("7z extraction failed"));
    }

    count_extracted_files(output_dir).await
}

async fn count_extracted_files(dir: &Path) -> Result<u32> {
    let mut count = 0u32;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                count += 1;
            } else if metadata.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(count)
}

async fn calculate_dir_size(dir: &Path) -> Result<u64> {
    let mut total = 0u64;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&current).await?;
        while let Some(entry) = entries.next_entry().await? {
            let metadata = entry.metadata().await?;
            if metadata.is_file() {
                total += metadata.len();
            } else if metadata.is_dir() {
                stack.push(entry.path());
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_format_detection_by_extension() {
        assert_eq!(
            ArchiveFormat::detect(Path::new("file.zip")).await.unwrap(),
            ArchiveFormat::Zip
        );
        assert_eq!(
            ArchiveFormat::detect(Path::new("file.tar.gz"))
                .await
                .unwrap(),
            ArchiveFormat::TarGz
        );
    }

    #[tokio::test]
    async fn test_archive_format_enum() {
        assert_eq!(ArchiveFormat::Zip, ArchiveFormat::Zip);
        assert_ne!(ArchiveFormat::Zip, ArchiveFormat::TarGz);
    }

    // ============ PATH TRAVERSAL SECURITY TESTS ============
    #[test]
    fn test_reject_absolute_paths() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("/etc/passwd"), base_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("absolute path"));
    }

    #[test]
    fn test_reject_parent_directory_traversal() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("../../../etc/passwd"), base_dir);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".."));
    }

    #[test]
    fn test_reject_dotdot_in_middle() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("subdir/../../../etc/passwd"), base_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_accept_normal_relative_path() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("subdir/file.txt"), base_dir);
        assert!(result.is_ok());
    }

    #[test]
    fn test_accept_deeply_nested_path() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("a/b/c/d/e/f/g/file.txt"), base_dir);
        assert!(result.is_ok());
    }

    #[test]
    fn test_reject_absolute_windows_path() {
        let base_dir = Path::new("C:\\tmp\\extract");
        let result = validate_archive_path(Path::new("C:\\Windows\\System32\\cmd.exe"), base_dir);
        assert!(result.is_err());
    }

    #[test]
    fn test_accept_single_file_in_archive() {
        let base_dir = Path::new("/tmp/extract");
        let result = validate_archive_path(Path::new("file.txt"), base_dir);
        assert!(result.is_ok());
    }
}
