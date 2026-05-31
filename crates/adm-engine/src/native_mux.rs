use crate::mp4_mux;
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct AvMuxInput {
    pub group_id: String,
    pub video_segments: Vec<PathBuf>,
    pub audio_segments: Vec<PathBuf>,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone)]
pub struct AvMuxOutput {
    pub group_dir: PathBuf,
    pub video_joined: Option<PathBuf>,
    pub audio_joined: Option<PathBuf>,
    pub final_bundle: PathBuf,
    pub final_mp4: Option<PathBuf>,
    pub final_format: String,
    pub final_size_bytes: u64,
    pub final_sha256: String,
    pub video_probe: Option<String>,
    pub audio_probe: Option<String>,
}

pub async fn mux_group_native(input: AvMuxInput) -> Result<AvMuxOutput> {
    let group_dir = input.output_dir.join(format!("av_{}", input.group_id));
    tokio::fs::create_dir_all(&group_dir).await?;

    let video_joined = if input.video_segments.is_empty() {
        None
    } else {
        let out = group_dir.join("video.joined.bin");
        concat_files(&input.video_segments, &out).await?;
        Some(out)
    };

    let audio_joined = if input.audio_segments.is_empty() {
        None
    } else {
        let out = group_dir.join("audio.joined.bin");
        concat_files(&input.audio_segments, &out).await?;
        Some(out)
    };

    let video_probe = if let Some(v) = video_joined.as_ref() {
        Some(probe_media_kind(v).await?)
    } else {
        None
    };
    let audio_probe = if let Some(a) = audio_joined.as_ref() {
        Some(probe_media_kind(a).await?)
    } else {
        None
    };

    let mp4_allowed = !matches!(video_probe.as_deref(), Some("ts"))
        && !matches!(audio_probe.as_deref(), Some("ts"));

    let final_mp4 = if mp4_allowed {
        if let (Some(v), Some(a)) = (video_joined.as_ref(), audio_joined.as_ref()) {
            let out = group_dir.join(format!("{}.final.mp4", input.group_id));
            if mp4_mux::write_mp4_foundation(v, a, &out).await.is_ok()
                && mp4_mux::validate_mp4_structure(&out).await.is_ok()
            {
                tokio::fs::metadata(&out).await.ok().map(|_| out)
            } else {
                let _ = tokio::fs::remove_file(&out).await;
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let final_bundle = group_dir.join(format!("{}.final.adm_media", input.group_id));
    build_final_bundle(video_joined.as_ref(), audio_joined.as_ref(), &final_bundle).await?;
    let final_format = if final_mp4.is_some() {
        "mp4".to_string()
    } else {
        "adm_media".to_string()
    };

    let selected_final = final_mp4.clone().unwrap_or_else(|| final_bundle.clone());
    if final_mp4.is_none() {
        validate_adm_bundle(&selected_final).await?;
    }
    let final_size_bytes = tokio::fs::metadata(&selected_final).await?.len();
    let final_sha256 = sha256_file(&selected_final).await?;

    Ok(AvMuxOutput {
        group_dir,
        video_joined,
        audio_joined,
        final_bundle,
        final_mp4,
        final_format,
        final_size_bytes,
        final_sha256,
        video_probe,
        audio_probe,
    })
}

async fn concat_files(inputs: &[PathBuf], output: &Path) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut out = tokio::fs::File::create(output).await?;
    for path in inputs {
        if tokio::fs::metadata(path).await.is_err() {
            return Err(anyhow::anyhow!("missing mux input: {}", path.display()));
        }
        let mut f = tokio::fs::File::open(path)
            .await
            .with_context(|| format!("open input {}", path.display()))?;
        let mut buf = vec![0_u8; 1024 * 1024];
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n]).await?;
        }
    }
    out.flush().await?;
    Ok(())
}

async fn build_final_bundle(
    video_joined: Option<&PathBuf>,
    audio_joined: Option<&PathBuf>,
    out: &Path,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut f = tokio::fs::File::create(out).await?;
    f.write_all(b"ADM_MEDIA_BUNDLE_V1\n").await?;
    if let Some(v) = video_joined {
        let size = tokio::fs::metadata(v).await?.len();
        f.write_all(format!("VIDEO {size}\n").as_bytes()).await?;
        append_file(&mut f, v).await?;
        f.write_all(b"\n").await?;
    }
    if let Some(a) = audio_joined {
        let size = tokio::fs::metadata(a).await?.len();
        f.write_all(format!("AUDIO {size}\n").as_bytes()).await?;
        append_file(&mut f, a).await?;
        f.write_all(b"\n").await?;
    }
    f.flush().await?;
    Ok(())
}

async fn append_file(out: &mut tokio::fs::File, input: &Path) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut src = tokio::fs::File::open(input).await?;
    let mut buf = vec![0_u8; 1024 * 1024];
    loop {
        let n = src.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        out.write_all(&buf[..n]).await?;
    }
    Ok(())
}

async fn sha256_file(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use tokio::io::AsyncReadExt;

    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0_u8; 1024 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

async fn validate_adm_bundle(path: &Path) -> Result<()> {
    let bytes = tokio::fs::read(path).await?;
    if bytes.len() < 18 {
        return Err(anyhow::anyhow!("adm bundle too small"));
    }
    if !bytes.starts_with(b"ADM_MEDIA_BUNDLE_V1\n") {
        return Err(anyhow::anyhow!("adm bundle missing magic header"));
    }
    Ok(())
}

async fn probe_media_kind(path: &Path) -> Result<String> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await?;
    let mut head = [0u8; 16];
    let n = f.read(&mut head).await?;
    if n >= 8 {
        // ISO-BMFF files typically contain box size + "ftyp" at offset 4.
        if &head[4..8] == b"ftyp" || &head[4..8] == b"moof" {
            return Ok("fmp4".to_string());
        }
    }
    if n >= 1 && head[0] == 0x47 {
        return Ok("ts".to_string());
    }
    Ok("unknown".to_string())
}
