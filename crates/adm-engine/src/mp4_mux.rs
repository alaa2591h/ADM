use anyhow::{anyhow, Result};
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub async fn write_mp4_foundation(video: &Path, audio: &Path, out: &Path) -> Result<()> {
    let mut f = tokio::fs::File::create(out).await?;
    let video_bytes = tokio::fs::read(video).await?;
    let audio_bytes = tokio::fs::read(audio).await?;

    let mut mdat_payload = Vec::with_capacity(video_bytes.len() + audio_bytes.len());
    mdat_payload.extend_from_slice(&video_bytes);
    mdat_payload.extend_from_slice(&audio_bytes);

    let ftyp = ftyp_box();
    let mdat = make_box(*b"mdat", &mdat_payload);
    let video_size = u32::try_from(video_bytes.len())
        .map_err(|_| anyhow!("video too large for current mp4 writer"))?;
    let audio_size = u32::try_from(audio_bytes.len())
        .map_err(|_| anyhow!("audio too large for current mp4 writer"))?;
    let ftyp_size = u32::try_from(ftyp.len()).map_err(|_| anyhow!("ftyp size overflow"))?;
    let moov = moov_box(video_size, audio_size, ftyp_size + 8);

    f.write_all(&ftyp).await?;
    f.write_all(&mdat).await?;
    f.write_all(&moov).await?;
    f.flush().await?;
    Ok(())
}

pub async fn validate_mp4_structure(path: &Path) -> Result<()> {
    let bytes = tokio::fs::read(path).await?;
    if bytes.len() < 24 {
        return Err(anyhow!("mp4 too small"));
    }

    let mut cursor = 0usize;
    let mut seen_ftyp = false;
    let mut seen_mdat = false;
    let mut seen_moov = false;
    let mut moov_slice: Option<&[u8]> = None;

    while cursor + 8 <= bytes.len() {
        let size = u32::from_be_bytes([
            bytes[cursor],
            bytes[cursor + 1],
            bytes[cursor + 2],
            bytes[cursor + 3],
        ]) as usize;
        if size < 8 || cursor + size > bytes.len() {
            return Err(anyhow!("invalid box size at {cursor}"));
        }
        let kind = &bytes[cursor + 4..cursor + 8];
        match kind {
            b"ftyp" => seen_ftyp = true,
            b"mdat" => seen_mdat = true,
            b"moov" => {
                seen_moov = true;
                moov_slice = Some(&bytes[cursor + 8..cursor + size]);
            }
            _ => {}
        }
        cursor += size;
    }

    if !(seen_ftyp && seen_mdat && seen_moov) {
        return Err(anyhow!("missing required top-level boxes"));
    }

    let moov = moov_slice.ok_or_else(|| anyhow!("missing moov payload"))?;
    if !contains_box(moov, b"mvhd") {
        return Err(anyhow!("moov missing mvhd"));
    }
    if count_box(moov, b"trak") < 2 {
        return Err(anyhow!("moov has fewer than 2 trak boxes"));
    }

    Ok(())
}

fn ftyp_box() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(b"isom");
    p.extend_from_slice(&0x200u32.to_be_bytes());
    p.extend_from_slice(b"isom");
    p.extend_from_slice(b"iso2");
    p.extend_from_slice(b"mp41");
    make_box(*b"ftyp", &p)
}

fn moov_box(video_size: u32, audio_size: u32, mdat_payload_offset: u32) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&mvhd_box());
    p.extend_from_slice(&trak_box(1, b"vide", video_size, mdat_payload_offset));
    p.extend_from_slice(&trak_box(
        2,
        b"soun",
        audio_size,
        mdat_payload_offset + video_size,
    ));
    make_box(*b"moov", &p)
}

fn mvhd_box() -> Vec<u8> {
    let mut p = vec![0, 0, 0, 0];
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&1000u32.to_be_bytes());
    p.extend_from_slice(&1000u32.to_be_bytes());
    p.extend_from_slice(&0x0001_0000u32.to_be_bytes());
    p.extend_from_slice(&0x0100u16.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&[0u8; 10]);
    p.extend_from_slice(&[
        0x00, 0x01, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x00, 0x01, 0x00, 0x00, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0x40, 0, 0, 0,
    ]);
    p.extend_from_slice(&[0u8; 24]);
    p.extend_from_slice(&3u32.to_be_bytes());
    make_box(*b"mvhd", &p)
}

fn trak_box(track_id: u32, handler: &[u8; 4], sample_size: u32, chunk_offset: u32) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&tkhd_box(track_id));
    p.extend_from_slice(&mdia_box(handler, sample_size, chunk_offset));
    make_box(*b"trak", &p)
}

fn tkhd_box(track_id: u32) -> Vec<u8> {
    let mut p = vec![0, 0, 0, 7];
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&track_id.to_be_bytes());
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&1000u32.to_be_bytes());
    p.extend_from_slice(&[0u8; 8]);
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&0x0100u16.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&[
        0x00, 0x01, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x00, 0x01, 0x00, 0x00, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0x40, 0, 0, 0,
    ]);
    p.extend_from_slice(&(1920u32 << 16).to_be_bytes());
    p.extend_from_slice(&(1080u32 << 16).to_be_bytes());
    make_box(*b"tkhd", &p)
}

fn mdia_box(handler: &[u8; 4], sample_size: u32, chunk_offset: u32) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&mdhd_box());
    p.extend_from_slice(&hdlr_box(handler));
    p.extend_from_slice(&minf_box(handler, sample_size, chunk_offset));
    make_box(*b"mdia", &p)
}

fn mdhd_box() -> Vec<u8> {
    let mut p = vec![0, 0, 0, 0];
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&1000u32.to_be_bytes());
    p.extend_from_slice(&1000u32.to_be_bytes());
    p.extend_from_slice(&0x55c4u16.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes());
    make_box(*b"mdhd", &p)
}

fn hdlr_box(handler: &[u8; 4]) -> Vec<u8> {
    let mut p = vec![0, 0, 0, 0];
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(handler);
    p.extend_from_slice(&[0u8; 12]);
    p.extend_from_slice(if handler == b"vide" {
        b"VideoHandler\0"
    } else {
        b"SoundHandler\0"
    });
    make_box(*b"hdlr", &p)
}

fn minf_box(handler: &[u8; 4], sample_size: u32, chunk_offset: u32) -> Vec<u8> {
    let mut p = Vec::new();
    if handler == b"vide" {
        p.extend_from_slice(&make_box(*b"vmhd", &[0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
    } else {
        p.extend_from_slice(&make_box(*b"smhd", &[0, 0, 0, 0, 0, 0, 0, 0]));
    }
    p.extend_from_slice(&dinf_box());
    p.extend_from_slice(&stbl_box(handler, sample_size, chunk_offset));
    make_box(*b"minf", &p)
}

fn dinf_box() -> Vec<u8> {
    let url = make_box(*b"url ", &[0, 0, 0, 1]);
    let mut dref_p = vec![0, 0, 0, 0];
    dref_p.extend_from_slice(&1u32.to_be_bytes());
    dref_p.extend_from_slice(&url);
    let dref = make_box(*b"dref", &dref_p);
    make_box(*b"dinf", &dref)
}

fn stbl_box(handler: &[u8; 4], sample_size: u32, chunk_offset: u32) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&stsd_box(handler));
    p.extend_from_slice(&make_box(
        *b"stts",
        &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 3, 0],
    ));
    p.extend_from_slice(&make_box(
        *b"stsc",
        &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1],
    ));
    p.extend_from_slice(&make_box(
        *b"stsz",
        &[
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            ((sample_size >> 24) & 0xff) as u8,
            ((sample_size >> 16) & 0xff) as u8,
            ((sample_size >> 8) & 0xff) as u8,
            (sample_size & 0xff) as u8,
        ],
    ));
    p.extend_from_slice(&make_box(
        *b"stco",
        &[
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            1,
            ((chunk_offset >> 24) & 0xff) as u8,
            ((chunk_offset >> 16) & 0xff) as u8,
            ((chunk_offset >> 8) & 0xff) as u8,
            (chunk_offset & 0xff) as u8,
        ],
    ));
    make_box(*b"stbl", &p)
}

fn stsd_box(handler: &[u8; 4]) -> Vec<u8> {
    let entry = if handler == b"vide" {
        avc1_entry()
    } else {
        mp4a_entry()
    };
    let mut p = vec![0, 0, 0, 0];
    p.extend_from_slice(&1u32.to_be_bytes());
    p.extend_from_slice(&entry);
    make_box(*b"stsd", &p)
}

fn avc1_entry() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 6]);
    p.extend_from_slice(&1u16.to_be_bytes());
    p.extend_from_slice(&[0u8; 16]);
    p.extend_from_slice(&1920u16.to_be_bytes());
    p.extend_from_slice(&1080u16.to_be_bytes());
    p.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    p.extend_from_slice(&0x0048_0000u32.to_be_bytes());
    p.extend_from_slice(&0u32.to_be_bytes());
    p.extend_from_slice(&1u16.to_be_bytes());
    p.extend_from_slice(&[0u8; 32]);
    p.extend_from_slice(&0x0018u16.to_be_bytes());
    p.extend_from_slice(&0xFFFFu16.to_be_bytes());
    let avcc = avcc_box();
    p.extend_from_slice(&avcc);
    make_box(*b"avc1", &p)
}

fn avcc_box() -> Vec<u8> {
    // Static AVCDecoderConfigurationRecord scaffold (baseline-ish placeholder).
    let sps: [u8; 9] = [0x67, 0x42, 0x80, 0x1e, 0xd9, 0x01, 0x40, 0x7b, 0x20];
    let pps: [u8; 4] = [0x68, 0xce, 0x38, 0x80];
    let mut p = Vec::new();
    p.push(1);
    p.push(0x42);
    p.push(0x80);
    p.push(0x1e);
    p.push(0xFF);
    p.push(0xE1);
    p.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    p.extend_from_slice(&sps);
    p.push(1);
    p.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    p.extend_from_slice(&pps);
    make_box(*b"avcC", &p)
}

fn mp4a_entry() -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 6]);
    p.extend_from_slice(&1u16.to_be_bytes());
    p.extend_from_slice(&[0u8; 8]);
    p.extend_from_slice(&2u16.to_be_bytes()); // channels
    p.extend_from_slice(&16u16.to_be_bytes()); // sample size
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&0u16.to_be_bytes());
    p.extend_from_slice(&(48_000u32 << 16).to_be_bytes());
    p.extend_from_slice(&esds_box());
    make_box(*b"mp4a", &p)
}

fn esds_box() -> Vec<u8> {
    // Minimal ES_Descriptor structure for AAC-LC profile indication.
    let mut p = vec![0, 0, 0, 0];
    p.extend_from_slice(&[
        0x03, 0x19, // ES_DescrTag + length
        0x00, 0x02, // ES_ID
        0x00, // flags
        0x04, 0x11, // DecoderConfigDescrTag + length
        0x40, // objectTypeIndication (MPEG-4 AAC)
        0x15, // streamType/audio + upStream + reserved
        0x00, 0x00, 0x00, // bufferSizeDB
        0x00, 0x01, 0x77, 0x00, // maxBitrate
        0x00, 0x01, 0x77, 0x00, // avgBitrate
        0x05, 0x02, 0x11, 0x90, // DecoderSpecificInfo (AAC-LC 48k/2ch)
        0x06, 0x01, 0x02, // SLConfigDescriptor
    ]);
    make_box(*b"esds", &p)
}

fn make_box(kind: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let size = (8 + payload.len()) as u32;
    let mut out = Vec::with_capacity(size as usize);
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(&kind);
    out.extend_from_slice(payload);
    out
}

fn contains_box(payload: &[u8], needle: &[u8; 4]) -> bool {
    count_box(payload, needle) > 0
}

fn count_box(payload: &[u8], needle: &[u8; 4]) -> usize {
    let mut cursor = 0usize;
    let mut count = 0usize;
    while cursor + 8 <= payload.len() {
        let size = u32::from_be_bytes([
            payload[cursor],
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
        ]) as usize;
        if size < 8 || cursor + size > payload.len() {
            break;
        }
        if &payload[cursor + 4..cursor + 8] == needle {
            count += 1;
        }
        cursor += size;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn writes_and_validates_mp4_structure() {
        let dir = tempdir().unwrap();
        let video = dir.path().join("v.bin");
        let audio = dir.path().join("a.bin");
        let out = dir.path().join("o.mp4");
        tokio::fs::write(&video, vec![0x11u8; 1024]).await.unwrap();
        tokio::fs::write(&audio, vec![0x22u8; 512]).await.unwrap();

        write_mp4_foundation(&video, &audio, &out).await.unwrap();
        validate_mp4_structure(&out).await.unwrap();
    }
}
