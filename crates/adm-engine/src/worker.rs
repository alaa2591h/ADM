use crate::runtime::ThroughputSampler;
use crate::EventBus;
use crate::{
    ChunkState, ChunkUpdate, DownloadChunk, DownloadTask, FileWriteCoordinator, ShutdownToken,
    WorkerHandle,
};
use adm_network::{
    BandwidthLimiter, CancellationToken, NetworkClient, NetworkError, NetworkRequest,
    ResponseStream,
};
use aes::Aes128;
use anyhow::Result;
use async_trait::async_trait;
use cipher::{BlockCipherDecrypt, KeyInit};
use aes::cipher::generic_array::GenericArray;
use parking_lot::Mutex;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

struct ThrottledResponseStream {
    inner: Box<dyn ResponseStream + Send + Sync>,
    limiter: BandwidthLimiter,
}

impl ThrottledResponseStream {
    fn new(inner: Box<dyn ResponseStream + Send + Sync>, limiter: BandwidthLimiter) -> Self {
        Self { inner, limiter }
    }
}

#[async_trait]
impl ResponseStream for ThrottledResponseStream {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let chunk = self.inner.next_chunk().await?;
        if let Some(bytes) = chunk {
            while !self.limiter.allow_bytes(bytes.len() as u64).await {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(Some(bytes))
        } else {
            Ok(None)
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        self.inner.total_bytes()
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.inner.cancel().await
    }
}

pub struct DownloadJob {
    pub task: DownloadTask,
    pub chunk: DownloadChunk,
    pub request: NetworkRequest,
    pub client: Arc<dyn NetworkClient>,
    pub update_sender: mpsc::UnboundedSender<ChunkUpdate>,
    pub event_bus: EventBus,
    pub worker: WorkerHandle,
    pub write_coordinator: Arc<FileWriteCoordinator>,
    pub cancel_token: CancellationToken,
    pub task_cancel_token: Option<CancellationToken>,
    pub generation: u64,
    pub shutdown: ShutdownToken,
    pub worker_reservation: Option<WorkerReservation>,
    pub bandwidth_limiter: Option<BandwidthLimiter>,
}

impl DownloadJob {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task: DownloadTask,
        chunk: DownloadChunk,
        request: NetworkRequest,
        client: Arc<dyn NetworkClient>,
        update_sender: mpsc::UnboundedSender<ChunkUpdate>,
        event_bus: EventBus,
        worker: WorkerHandle,
        write_coordinator: Arc<FileWriteCoordinator>,
        cancel_token: CancellationToken,
        shutdown: ShutdownToken,
        worker_reservation: WorkerReservation,
        bandwidth_limiter: Option<BandwidthLimiter>,
        task_cancel_token: Option<CancellationToken>,
        generation: u64,
    ) -> Self {
        Self {
            task,
            chunk,
            request,
            client,
            update_sender,
            event_bus,
            worker,
            write_coordinator,
            cancel_token,
            task_cancel_token,
            generation,
            shutdown,
            worker_reservation: Some(worker_reservation),
            bandwidth_limiter,
        }
    }

    fn send_update(&self, event: &str, chunk: &DownloadChunk) {
        if event == "chunk.failed" {
            tracing::warn!(
                task_id = %chunk.task_id,
                chunk_id = %chunk.id,
                error = ?chunk.last_error,
                state = %chunk.state.as_str(),
                downloaded_bytes = chunk.downloaded_bytes,
                "worker.send_update chunk failed",
            );
        }

        if event == "chunk.cancelled" {
            let last_progress_ms = chunk
                .last_progress_instant
                .map(|i| std::time::Instant::now().duration_since(i).as_millis());
            tracing::info!(
                task_id = %chunk.task_id,
                chunk_id = %chunk.id,
                last_progress_ms = ?last_progress_ms,
                "worker.send_update chunk cancelled",
            );
        }

        match self.update_sender.send(ChunkUpdate {
            chunk: chunk.clone(),
            event: event.to_string(),
            worker: self.worker.clone(),
            generation: self.generation,
            discovered_total_bytes: None,
        }) {
            Ok(()) => {
                tracing::debug!(task_id = %chunk.task_id, chunk_id = %chunk.id, event = event, "sent update to scheduler");
            }
            Err(err) => {
                tracing::warn!(task_id = %chunk.task_id, chunk_id = %chunk.id, error = ?err, "failed to send update to scheduler");
            }
        }

        let payload = json!({
            "task_id": self.task.id.to_string(),
            "chunk_id": chunk.id.to_string(),
            "worker_id": self.worker.id.to_string(),
            "state": chunk.state.as_str(),
            "generation": self.generation,
            "downloaded_bytes": chunk.downloaded_bytes,
            "total_bytes": self.task.total_bytes,
        });
        self.event_bus.publish(event, payload);
    }

    fn touch(&mut self) {
        self.chunk.touch();
    }

    pub async fn run(mut self) -> Result<DownloadChunk> {
        let mut chunk = self.chunk.clone();
        chunk.set_state(ChunkState::Connecting);
        self.send_update("chunk.connecting", &chunk);

        let request = self.request.clone();
        if self.shutdown.is_cancelled()
            || self
                .task_cancel_token
                .as_ref()
                .map_or(false, |t| t.is_cancelled())
        {
            chunk.last_error = Some("context shut down or task cancelled".to_string());
            chunk.set_state(ChunkState::Cancelled);
            self.worker_reservation.take();
            self.send_update("chunk.cancelled", &chunk);
            return Err(anyhow::anyhow!("chunk aborted by shutdown or cancellation"));
        }

        let mut response: Box<dyn ResponseStream + Send + Sync> = match self
            .client
            .execute(request)
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                let err_str = err.to_string();
                tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run execute failed");
                chunk.last_error = Some(err_str);
                chunk.set_state(ChunkState::Failed);
                self.worker_reservation.take();
                self.send_update("chunk.failed", &chunk);
                return Err(err.into());
            }
        };

        if let Some(limiter) = self.bandwidth_limiter.clone() {
            response = Box::new(ThrottledResponseStream::new(response, limiter));
        }

        // If the HEAD probe was skipped or failed, the server may still return
        // Content-Length on the GET response. Capture it here and forward it to
        // the scheduler so task.total_bytes can be set before the download loop
        // begins, enabling progress reporting and smart chunk splitting.
        let discovered_total_bytes = if self.task.total_bytes.is_none() {
            response.total_bytes()
        } else {
            None
        };
        if let Some(len) = discovered_total_bytes {
            tracing::debug!(
                task_id = %self.task.id,
                chunk_id = %chunk.id,
                content_length = len,
                "worker discovered total_bytes from GET Content-Length",
            );
            // Send the discovery immediately so the scheduler can update
            // task.total_bytes before we start streaming bytes.
            match self.update_sender.send(ChunkUpdate {
                chunk: chunk.clone(),
                event: "chunk.size_discovered".to_string(),
                worker: self.worker.clone(),
                generation: self.generation,
                discovered_total_bytes: Some(len),
            }) {
                Ok(()) => {}
                Err(err) => tracing::warn!(error = ?err, "failed to send size_discovered update"),
            }
        }

        let lease = match self
            .write_coordinator
            .reserve_write(chunk.id, chunk.offset, chunk.length)
            .await
        {
            Ok(lease) => lease,
            Err(err) => {
                let err_str = err.to_string();
                tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run reserve_write failed");
                chunk.last_error = Some(err_str);
                chunk.set_state(ChunkState::Failed);
                self.worker_reservation.take();
                self.send_update("chunk.failed", &chunk);
                return Err(anyhow::anyhow!(err.to_string()));
            }
        };

        chunk.set_state(ChunkState::Downloading);
        self.send_update("chunk.downloading", &chunk);

        // ── Worker heartbeat ─────────────────────────────────────────────────
        // Spawn a lightweight background task that pings EventBus every 500 ms
        // while this chunk is actively downloading.  The StallDetectorSubsystem
        // consumes `worker.heartbeat` events to distinguish a slow-but-alive
        // worker from a silently-hung worker (HeartbeatLost stall reason).
        // The task self-terminates when cancel_token or shutdown fires.
        let _heartbeat_handle = {
            let hb_cancel = self.cancel_token.clone();
            let hb_shutdown = self.shutdown.clone();
            let hb_bus = self.event_bus.clone();
            let hb_worker = self.worker.id;
            let hb_task = self.task.id;
            let hb_chunk = chunk.id;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(500));
                ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticker.tick().await;
                    if hb_cancel.is_cancelled() || hb_shutdown.is_cancelled() {
                        break;
                    }
                    hb_bus.publish(
                        "worker.heartbeat",
                        serde_json::json!({
                            "worker_id": hb_worker.to_string(),
                            "task_id":   hb_task.to_string(),
                            "chunk_id":  hb_chunk.to_string(),
                            "timestamp_ms": std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as i64,
                        }),
                    );
                }
            })
        };
        // _heartbeat_handle is dropped (and the task aborted) when this
        // DownloadJob::run() frame exits, regardless of success or failure.

        let mut sampler = ThroughputSampler::new(Duration::from_secs(5));
        let mut current_offset = chunk.offset + chunk.downloaded_bytes;

        // Write buffer: accumulate small wire frames before calling queue_write.
        // Flush when the buffer reaches 512 KiB or the stream ends.
        const WRITE_BUFFER_FLUSH_THRESHOLD: u64 = 512 * 1024;
        let mut write_buf: Vec<u8> = Vec::with_capacity(WRITE_BUFFER_FLUSH_THRESHOLD as usize);
        let mut write_buf_start_offset = current_offset;

        // AES-128-CBC decryption state (optional)
        let mut aes_key: Option<Vec<u8>> = None;
        let mut iv_bytes: Option<Vec<u8>> = None;
        let mut decrypt_buf: Vec<u8> = Vec::new();
        let mut prev_cipher_block: Option<[u8; 16]> = None;

        // Inspect task headers for HLS AES metadata injected by the extractor.
        //
        // Header priority for key material:
        //   1. `X-ADM-Encryption-Key-Hex`  — pre-fetched by extractor (fast path, no RTT)
        //   2. `X-ADM-Encryption-Key-URL`  — runtime fetch (fallback / SAMPLE-AES)
        for (hk, hv) in &self.task.headers {
            match hk.as_str() {
                "X-ADM-Encryption-Key-Hex" => {
                    // Extractor pre-fetched the AES-128 key and stored it as a
                    // lowercase hex string (32 chars = 16 bytes).  Use it directly,
                    // skipping the key-server round-trip entirely.
                    let hex = hv.trim();
                    match hex::decode(hex) {
                        Ok(key_bytes) if key_bytes.len() == 16 => {
                            tracing::debug!(
                                task = %self.task.id,
                                "worker: using pre-fetched AES-128 key from extractor"
                            );
                            aes_key = Some(key_bytes);
                        }
                        Ok(key_bytes) => {
                            tracing::warn!(
                                task = %self.task.id,
                                len = key_bytes.len(),
                                "worker: X-ADM-Encryption-Key-Hex decoded to unexpected length"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                task = %self.task.id,
                                error = %e,
                                "worker: failed to hex-decode X-ADM-Encryption-Key-Hex"
                            );
                        }
                    }
                }
                "X-ADM-Encryption-Key-URL" if aes_key.is_none() => {
                    // Runtime key fetch — only runs when the extractor did NOT
                    // pre-fetch the key (i.e. X-ADM-Encryption-Key-Hex is absent).
                    let key_url = hv.clone();
                    let key_req = NetworkRequest::new(key_url, None);
                    match self.client.execute(key_req).await {
                        Ok(mut key_stream) => {
                            let mut key_bytes = Vec::new();
                            loop {
                                match key_stream.next_chunk().await {
                                    Ok(Some(chunk)) => key_bytes.extend_from_slice(&chunk),
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!(task = %self.task.id, "failed to fetch AES key: {}", e.to_string());
                                        break;
                                    }
                                }
                            }
                            if key_bytes.len() >= 16 {
                                aes_key = Some(key_bytes[..16].to_vec());
                            } else {
                                tracing::warn!(task = %self.task.id, "AES key length unexpected: {}", key_bytes.len());
                            }
                        }
                        Err(e) => {
                            tracing::warn!(task = %self.task.id, "failed to fetch AES key: {}", e.to_string());
                        }
                    }
                }
                "X-ADM-Encryption-IV" => {
                    // Hex-encoded IV
                    let hex = hv.trim().trim_start_matches("0x").trim_start_matches("0X");
                    if let Ok(vb) = hex::decode(hex) {
                        if vb.len() == 16 {
                            let mut arr = [0u8; 16];
                            arr.copy_from_slice(&vb);
                            iv_bytes = Some(arr.to_vec());
                            prev_cipher_block = Some(arr);
                        } else {
                            tracing::warn!(task = %self.task.id, "IV length is not 16 bytes: {}", vb.len());
                        }
                    }
                }
                "X-ADM-Encryption-SEQ" if iv_bytes.is_none() => {
                    if let Ok(seq_n) = hv.parse::<u64>() {
                        let mut arr = [0u8; 16];
                        arr[8..16].copy_from_slice(&seq_n.to_be_bytes());
                        iv_bytes = Some(arr.to_vec());
                        prev_cipher_block = Some(arr);
                    }
                }
                _ => {}
            }
        }

        // Initialize cipher if key is present
        let mut cipher_opt: Option<Aes128> = None;
        if let Some(ref key) = aes_key {
            if key.len() >= 16 {
                let key_arr = GenericArray::from_slice(&key[..16]);
                cipher_opt = Some(Aes128::new(key_arr));
            }
        }

        // Progress throttle: only send progress updates every 256 KiB received
        // or every 500 ms, whichever comes first, to avoid saturating the
        // scheduler's update channel with tiny increments.
        const PROGRESS_BYTE_INTERVAL: u64 = 256 * 1024;
        let mut bytes_since_last_progress: u64 = 0;
        let mut last_progress_at = std::time::Instant::now();
        const PROGRESS_TIME_INTERVAL: Duration = Duration::from_millis(500);

        loop {
            // Check cancellation before each read.
            if self.cancel_token.is_cancelled()
                || self.shutdown.is_cancelled()
                || self
                    .task_cancel_token
                    .as_ref()
                    .map_or(false, |t| t.is_cancelled())
            {
                chunk.last_error = Some("cancelled by token".to_string());
                chunk.set_state(ChunkState::Cancelled);
                let _ = response.cancel().await;
                self.worker_reservation.take();
                self.send_update("chunk.cancelled", &chunk);
                return Err(anyhow::anyhow!("chunk cancelled"));
            }

            // Directly await the next chunk — no polling sleep required.
            let bytes = match response.next_chunk().await {
                Ok(Some(data)) => data,
                Ok(None) => {
                    // Stream finished; flush the write buffer.
                    // If AES decryption active, decrypt any remaining buffered ciphertext.
                    if cipher_opt.is_some() && !decrypt_buf.is_empty() {
                        // decrypt all remaining full blocks
                        while decrypt_buf.len() >= 16 {
                            let block = decrypt_buf.drain(0..16).collect::<Vec<u8>>();
                            let mut ga = GenericArray::clone_from_slice(&block);
                            if let Some(ref mut cipher) = cipher_opt {
                                cipher.decrypt_block(&mut ga);
                            }
                            let prev = prev_cipher_block.unwrap_or([0u8; 16]);
                            let mut plain = [0u8; 16];
                            for j in 0..16 {
                                plain[j] = ga[j] ^ prev[j];
                            }
                            prev_cipher_block = Some({
                                let mut a = [0u8; 16];
                                a.copy_from_slice(&block);
                                a
                            });
                            // If this is the last block, remove PKCS7 padding
                            if decrypt_buf.is_empty() {
                                let pad = plain[15] as usize;
                                let valid_pad = pad > 0
                                    && pad <= 16
                                    && plain[16 - pad..].iter().all(|&b| b as usize == pad);
                                if valid_pad {
                                    write_buf.extend_from_slice(&plain[..16 - pad]);
                                } else {
                                    write_buf.extend_from_slice(&plain);
                                }
                            } else {
                                write_buf.extend_from_slice(&plain);
                            }
                        }
                    }

                    if !write_buf.is_empty() {
                        let buf_offset = write_buf_start_offset;
                        let buf_data = std::mem::take(&mut write_buf);
                        if self.shutdown.is_cancelled() {
                            chunk.last_error = Some("context shut down".to_string());
                            chunk.set_state(ChunkState::Cancelled);
                            self.worker_reservation.take();
                            self.send_update("chunk.cancelled", &chunk);
                            return Err(anyhow::anyhow!("chunk aborted by shutdown"));
                        }
                        if let Err(err) = self
                            .write_coordinator
                            .queue_write(lease.id, buf_offset, buf_data)
                            .await
                        {
                            let err_str = err.to_string();
                            tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run queue_write (final) failed");
                            let _ = self.write_coordinator.release_reservation(lease.id).await;
                            chunk.last_error = Some(err_str.clone());
                            chunk.set_state(ChunkState::Failed);
                            self.worker_reservation.take();
                            self.send_update("chunk.failed", &chunk);
                            return Err(anyhow::anyhow!(err_str));
                        }
                    }
                    break;
                }
                Err(err) => {
                    let err_str = err.to_string();
                    tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run response.next_chunk failed");
                    let _ = self.write_coordinator.release_reservation(lease.id).await;
                    chunk.last_error = Some(err_str.clone());
                    chunk.set_state(ChunkState::Failed);
                    self.worker_reservation.take();
                    self.send_update("chunk.failed", &chunk);
                    return Err(anyhow::anyhow!(err_str));
                }
            };

            let len = bytes.len() as u64;
            // If AES decryption is active, feed into decrypt_buf and produce plaintext blocks
            if cipher_opt.is_some() {
                decrypt_buf.extend_from_slice(&bytes);
                // Determine how many full blocks to decrypt now.
                // Keep one trailing block buffered until EOF to handle padding safely.
                let mut decryptable_blocks = decrypt_buf.len() / 16;
                // Leave one block pending so padding can be stripped correctly at EOF.
                if decryptable_blocks > 0 {
                    decryptable_blocks -= 1;
                }
                for i in 0..decryptable_blocks {
                    let off = i * 16;
                    let block = &decrypt_buf[off..off + 16];
                    let mut ga = GenericArray::clone_from_slice(block);
                    // decrypt block in-place
                    if let Some(ref mut cipher) = cipher_opt {
                        cipher.decrypt_block(&mut ga);
                    }
                    // XOR with previous ciphertext (or IV)
                    let prev = prev_cipher_block.unwrap_or([0u8; 16]);
                    let mut plain = [0u8; 16];
                    for j in 0..16 {
                        plain[j] = ga[j] ^ prev[j];
                    }
                    // update prev_cipher_block to current ciphertext block
                    let mut new_prev = [0u8; 16];
                    new_prev.copy_from_slice(block);
                    prev_cipher_block = Some(new_prev);
                    write_buf.extend_from_slice(&plain);
                }
                // remove processed bytes from decrypt_buf
                let rem = decryptable_blocks * 16;
                if rem > 0 {
                    decrypt_buf.drain(0..rem);
                }
            } else {
                write_buf.extend_from_slice(&bytes);
            }
            current_offset += len;
            chunk.downloaded_bytes += len;
            bytes_since_last_progress += len;
            sampler.record(len);
            chunk.speed_bytes_per_sec = sampler.current_rate_bps();
            chunk.touch();

            // Flush write buffer when it reaches the threshold.
            if write_buf.len() as u64 >= WRITE_BUFFER_FLUSH_THRESHOLD {
                let buf_offset = write_buf_start_offset;
                let buf_data = std::mem::take(&mut write_buf);
                write_buf_start_offset = current_offset;
                write_buf = Vec::with_capacity(WRITE_BUFFER_FLUSH_THRESHOLD as usize);

                if self.shutdown.is_cancelled()
                    || self
                        .task_cancel_token
                        .as_ref()
                        .map_or(false, |t| t.is_cancelled())
                {
                    chunk.last_error = Some("context shut down or task cancelled".to_string());
                    chunk.set_state(ChunkState::Cancelled);
                    let _ = response.cancel().await;
                    self.worker_reservation.take();
                    self.send_update("chunk.cancelled", &chunk);
                    return Err(anyhow::anyhow!("chunk aborted by shutdown or cancellation"));
                }
                if let Err(err) = self
                    .write_coordinator
                    .queue_write(lease.id, buf_offset, buf_data)
                    .await
                {
                    let err_str = err.to_string();
                    tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run queue_write failed");
                    let _ = self.write_coordinator.release_reservation(lease.id).await;
                    chunk.last_error = Some(err_str.clone());
                    chunk.set_state(ChunkState::Failed);
                    self.worker_reservation.take();
                    self.send_update("chunk.failed", &chunk);
                    return Err(anyhow::anyhow!(err_str));
                }
            }

            // Throttled progress updates.
            let now = std::time::Instant::now();
            if bytes_since_last_progress >= PROGRESS_BYTE_INTERVAL
                || now.duration_since(last_progress_at) >= PROGRESS_TIME_INTERVAL
            {
                self.send_update("chunk.progress", &chunk);
                bytes_since_last_progress = 0;
                last_progress_at = now;
            }
        }

        chunk.set_state(ChunkState::Flushing);
        self.send_update("chunk.flushing", &chunk);

        if self.shutdown.is_cancelled()
            || self
                .task_cancel_token
                .as_ref()
                .map_or(false, |t| t.is_cancelled())
        {
            chunk.last_error = Some("context shut down or task cancelled".to_string());
            chunk.set_state(ChunkState::Cancelled);
            self.worker_reservation.take();
            self.send_update("chunk.cancelled", &chunk);
            return Err(anyhow::anyhow!("chunk aborted by shutdown or cancellation"));
        }
        if let Err(err) = self.write_coordinator.commit_reservation(lease.id).await {
            let err_str = err.to_string();
            tracing::error!(task = %self.task.id, chunk = %chunk.id, error = %err_str, "worker.run commit_reservation failed");
            let _ = self.write_coordinator.release_reservation(lease.id).await;
            chunk.last_error = Some(err_str.clone());
            chunk.set_state(ChunkState::Failed);
            self.worker_reservation.take();
            self.send_update("chunk.failed", &chunk);
            return Err(anyhow::anyhow!(err_str));
        }

        let checksum_ok = self
            .write_coordinator
            .validate_checksum(lease.id)
            .await
            .map_err(|err| anyhow::anyhow!(err.to_string()))?;
        if !checksum_ok {
            let err_str = "checksum mismatch".to_string();
            tracing::error!(task = %self.task.id, chunk = %chunk.id, "worker.run checksum failed");
            let _ = self.write_coordinator.release_reservation(lease.id).await;
            chunk.last_error = Some(err_str.clone());
            chunk.set_state(ChunkState::Failed);
            self.worker_reservation.take();

            // Publish a typed checksum.chunk_failed event so the scheduler and
            // any UI subscriber can react with a targeted retry or alert.
            use ipc::contracts::{now_ms, ChecksumChunkFailedEvent};
            let cs_event = ChecksumChunkFailedEvent {
                task_id: self.task.id,
                chunk_id: chunk.id,
                error: err_str.clone(),
            };
            self.event_bus.publish(
                "checksum.chunk_failed",
                serde_json::to_value(cs_event).unwrap_or_else(|_| serde_json::json!({})),
            );

            self.send_update("chunk.failed", &chunk);
            return Err(anyhow::anyhow!(err_str));
        }

        chunk.set_state(ChunkState::Completed);
        self.touch();
        self.worker_reservation.take();
        self.send_update("chunk.completed", &chunk);
        Ok(chunk)
    }
}

#[derive(Debug)]
pub struct WorkerReservation {
    id: Uuid,
    #[allow(dead_code)]
    permit: OwnedSemaphorePermit,
    pool: Arc<Mutex<Vec<Uuid>>>,
}

impl WorkerReservation {
    pub const fn id(&self) -> Uuid {
        self.id
    }

    pub const fn handle(&self) -> WorkerHandle {
        WorkerHandle { id: self.id }
    }
}

impl Drop for WorkerReservation {
    fn drop(&mut self) {
        self.pool.lock().push(self.id);
    }
}

#[derive(Debug, Clone)]
pub struct WorkerState {
    pub id: Uuid,
    pub active_task_id: Option<Uuid>,
    pub active_chunk_id: Option<Uuid>,
    pub generation: u64,
    pub started_at: Instant,
    pub throughput_bps: f64,
}

#[derive(Debug)]
pub struct WorkerPool {
    semaphore: Arc<Semaphore>,
    idle_workers: Arc<Mutex<Vec<Uuid>>>,
    worker_states: Arc<Mutex<HashMap<Uuid, WorkerState>>>,
}

impl WorkerPool {
    #[must_use]
    pub fn new(max_workers: usize) -> Arc<Self> {
        let ids: Vec<Uuid> = (0..max_workers).map(|_| Uuid::new_v4()).collect();
        let mut states = HashMap::new();
        for &id in &ids {
            states.insert(
                id,
                WorkerState {
                    id,
                    active_task_id: None,
                    active_chunk_id: None,
                    generation: 0,
                    started_at: Instant::now(),
                    throughput_bps: 0.0,
                },
            );
        }

        Arc::new(Self {
            semaphore: Arc::new(Semaphore::new(max_workers)),
            idle_workers: Arc::new(Mutex::new(ids)),
            worker_states: Arc::new(Mutex::new(states)),
        })
    }

    #[must_use]
    pub fn available_worker_ids(&self) -> Vec<Uuid> {
        self.idle_workers.lock().clone()
    }

    pub fn snapshot(&self) -> adm_observability::WorkerPoolSnapshot {
        let states = self.worker_states.lock();
        let active_workers = states
            .values()
            .filter(|s| s.active_task_id.is_some())
            .count();
        let max_workers = states.len();

        let workers = states
            .values()
            .map(|s| adm_observability::WorkerStateSnapshot {
                id: s.id.to_string(),
                active_task_id: s.active_task_id.map(|id| id.to_string()),
                active_chunk_id: s.active_chunk_id.map(|id| id.to_string()),
                uptime_secs: s.started_at.elapsed().as_secs(),
            })
            .collect();

        adm_observability::WorkerPoolSnapshot {
            name: "engine-pool".to_string(),
            queue_depth: 0,
            active_workers,
            max_workers,
            tasks_completed: 0,
            tasks_failed: 0,
            workers,
        }
    }
}

impl adm_observability::SnapshotProvider for WorkerPool {
    fn get_snapshot(&self) -> adm_observability::WorkerPoolSnapshot {
        self.snapshot()
    }
}

impl WorkerPool {
    pub fn snapshots(&self) -> Vec<crate::WorkerSnapshot> {
        let states = self.worker_states.lock();
        states
            .values()
            .map(|s| crate::WorkerSnapshot {
                id: s.id,
                state: if s.active_task_id.is_some() { "active" } else { "idle" }.to_string(),
                active_task_id: s.active_task_id,
                active_chunk_id: s.active_chunk_id,
                generation: s.generation,
                uptime_secs: s.started_at.elapsed().as_secs(),
                throughput_bps: s.throughput_bps,
            })
            .collect()
    }

    pub async fn reserve_worker(
        &self,
        preferred: Option<Uuid>,
    ) -> Result<WorkerReservation, crate::EngineError> {
        tracing::debug!(preferred = ?preferred, "attempting to reserve worker");
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| crate::EngineError::Internal("worker semaphore closed".into()))?;
        let mut workers = self.idle_workers.lock();
        let id = if let Some(worker_id) = preferred {
            if let Some(pos) = workers.iter().position(|w| *w == worker_id) {
                workers.remove(pos)
            } else {
                workers.pop().unwrap_or_else(Uuid::new_v4)
            }
        } else {
            workers.pop().unwrap_or_else(Uuid::new_v4)
        };

        Ok(WorkerReservation {
            id,
            permit,
            pool: self.idle_workers.clone(),
        })
    }

    pub fn spawn_job(
        self: &Arc<Self>,
        mut job: DownloadJob,
    ) -> tokio::task::JoinHandle<Result<DownloadChunk>> {
        let worker_id = job.worker.id;
        let task_id = job.task.id;
        let chunk_id = job.chunk.id;
        let generation = job.generation;
        let pool = self.clone();

        tracing::debug!(worker_id = %worker_id, task_id = %task_id, chunk_id = %chunk_id, "spawning download job");

        // Update state to active
        {
            let mut states = pool.worker_states.lock();
            if let Some(state) = states.get_mut(&worker_id) {
                state.active_task_id = Some(task_id);
                state.active_chunk_id = Some(chunk_id);
                state.generation = generation;
            }
        }

        tokio::spawn(async move {
            let result = job.run().await;

            // Reset state to idle
            {
                let mut states = pool.worker_states.lock();
                if let Some(state) = states.get_mut(&worker_id) {
                    state.active_task_id = None;
                    state.active_chunk_id = None;
                }
            }

            result
        })
    }
}
