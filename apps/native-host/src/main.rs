//! ADM Native Messaging Host
//!
//! ## Responsibilities
//! - Implement the Chrome / Firefox Native Messaging wire protocol
//!   (4-byte little-endian length prefix on both stdin and stdout).
//! - Proxy every incoming JSON-RPC request to the daemon's WebSocket gateway.
//! - Proxy every JSON-RPC response / event frame back to the extension.
//! - Reconnect to the daemon automatically (exponential back-off, jitter).
//! - Emit structured JSON-RPC error objects so the extension always receives
//!   a well-formed response, even when the daemon is unreachable.
//!
//! ## Architecture invariants
//! - This binary contains **zero business logic**.
//! - It never touches the download engine, storage, or scheduler directly.
//! - All communication with the engine flows through the daemon's WS gateway.
//!
//! ## Protocol framing (Chrome Native Messaging)
//! ```text
//!   ┌──────────────────────────────────┐
//!   │ 4 bytes: message length (LE u32) │ ← header
//!   ├──────────────────────────────────┤
//!   │ N bytes: UTF-8 JSON payload      │ ← body
//!   └──────────────────────────────────┘
//! ```
//! Identical framing on both stdin (extension → host) and
//! stdout (host → extension).

use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Address of the daemon's WebSocket gateway.
const DAEMON_WS_URL: &str = "ws://127.0.0.1:9001";

/// Initial reconnect delay.
const BACKOFF_BASE_MS: u64 = 250;

/// Maximum reconnect delay (caps exponential growth).
const BACKOFF_MAX_MS: u64 = 30_000;

/// Maximum number of reconnect attempts before giving up and exiting.
/// The extension will restart the host on the next user action.
const MAX_RECONNECT_ATTEMPTS: u32 = 10;

/// Capacity of the channel buffering outbound frames while reconnecting.
const OUTBOUND_BUFFER: usize = 256;

// ── Native Messaging framing ──────────────────────────────────────────────────

/// Read one native-messaging frame from stdin (blocking).
/// Returns `None` on clean EOF (extension closed the port).
fn nm_read_frame() -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match io::stdin().read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    io::stdin().read_exact(&mut body)?;
    Ok(Some(body))
}

/// Write one native-messaging frame to stdout (blocking).
fn nm_write_frame(body: &[u8]) -> io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "native messaging frame exceeds u32 length limit",
        )
    })?;
    io::stdout().write_all(&len.to_le_bytes())?;
    io::stdout().write_all(body)?;
    io::stdout().flush()
}

// ── Structured error helpers ──────────────────────────────────────────────────

/// Build a JSON-RPC 2.0–compatible error response.
///
/// The extension always receives a well-formed JSON object so it can
/// distinguish protocol errors from application errors in a type-safe way.
fn make_error_response(id: Option<&Value>, code: i64, message: &str) -> Value {
    serde_json::json!({
        "id":    id.cloned().unwrap_or(Value::Null),
        "type":  "response",
        "result": null,
        "error": {
            "code":    code,
            "message": message
        }
    })
}

// ── Reconnecting WebSocket client ─────────────────────────────────────────────

/// A single established connection to the daemon WS gateway.
/// Wraps the sink half so multiple async writers can share it.
type WsSink = Arc<
    Mutex<
        futures_util::stream::SplitSink<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
            Message,
        >,
    >,
>;

struct Connection {
    sink: WsSink,
}

/// Attempt to connect to the daemon, retrying with exponential back-off.
///
/// Returns a `(Connection, SplitStream)` pair on success.
/// Each failed attempt is logged; after `MAX_RECONNECT_ATTEMPTS` the
/// function returns an error.
async fn connect_with_backoff() -> Result<(
    Connection,
    futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
)> {
    let mut delay_ms = BACKOFF_BASE_MS;
    let mut attempt = 0u32;

    loop {
        attempt += 1;
        match connect_async(DAEMON_WS_URL).await {
            Ok((ws_stream, _response)) => {
                info!(attempt, "connected to daemon at {}", DAEMON_WS_URL);
                let (sink, stream) = ws_stream.split();
                let conn = Connection {
                    sink: Arc::new(Mutex::new(sink)),
                };
                return Ok((conn, stream));
            }
            Err(e) => {
                if attempt >= MAX_RECONNECT_ATTEMPTS {
                    bail!("failed to connect to daemon after {attempt} attempts: {e}");
                }
                // Add ±10 % jitter to avoid thundering-herd on restart.
                let jitter = (delay_ms / 10).max(1);
                let sleep_ms = delay_ms.saturating_add(rand_jitter(jitter));
                warn!(
                    attempt,
                    delay_ms = sleep_ms,
                    "daemon unreachable ({}); retrying",
                    e
                );
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                delay_ms = (delay_ms * 2).min(BACKOFF_MAX_MS);
            }
        }
    }
}

/// Cheap deterministic jitter: use the lower bits of the current time.
fn rand_jitter(max: u64) -> u64 {
    let nanos = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos()),
    );
    nanos % max.max(1)
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    // Initialise tracing. Native hosts write diagnostics to stderr so they
    // don't corrupt the native-messaging stdout stream.
    tracing_subscriber::fmt()
        .with_writer(io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("APEX Download Manager (ADM) native messaging host starting");

    // One Tokio runtime handles the async WebSocket side.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .context("failed to build Tokio runtime")?;

    // Channel: async receiver → stdout thread.
    let (tx_to_stdout, mut rx_from_ws) = mpsc::channel::<Vec<u8>>(OUTBOUND_BUFFER);

    // ── Stdout writer thread ──────────────────────────────────────────────────
    // A dedicated blocking thread owns stdout. This keeps the async runtime
    // free of blocking I/O.
    let stdout_handle = std::thread::spawn(move || {
        while let Some(frame) = rx_from_ws.blocking_recv() {
            if nm_write_frame(&frame).is_err() {
                break; // extension closed port
            }
        }
        debug!("stdout writer thread exiting");
    });

    // ── Async WebSocket bridge ────────────────────────────────────────────────
    rt.block_on(async move {
        run_bridge(tx_to_stdout).await;
    });

    // Wait for stdout writer to drain.
    let _ = stdout_handle.join();

    info!("APEX Download Manager (ADM) native messaging host exiting");
    Ok(())
}

// ── Bridge main loop ──────────────────────────────────────────────────────────

/// Maintains a WebSocket connection to the daemon, forwarding frames in
/// both directions. Reconnects automatically on disconnection.
async fn run_bridge(tx_to_stdout: mpsc::Sender<Vec<u8>>) {
    // The stdin reader runs as a separate blocking task so it doesn't
    // block the async executor.
    let (tx_stdin_raw, mut rx_stdin_raw) = mpsc::channel::<Vec<u8>>(OUTBOUND_BUFFER);
    let stdin_task = tokio::task::spawn_blocking(move || {
        loop {
            match nm_read_frame() {
                Ok(Some(frame)) => {
                    if tx_stdin_raw.blocking_send(frame).is_err() {
                        break;
                    }
                }
                Ok(None) => {
                    debug!("stdin EOF — extension closed port");
                    break;
                }
                Err(e) => {
                    error!("stdin read error: {}", e);
                    break;
                }
            }
        }
        debug!("stdin reader thread exiting");
    });

    'reconnect: loop {
        // Attempt to establish a connection (with back-off).
        let (conn, mut ws_stream) = match connect_with_backoff().await {
            Ok(pair) => pair,
            Err(e) => {
                error!("could not reach daemon: {}; exiting bridge", e);
                break 'reconnect;
            }
        };

        let sink = conn.sink.clone();

        // Per-connection event loop.
        loop {
            tokio::select! {
                // ── Inbound from extension (stdin → WS) ──────────────────────
                maybe_frame = rx_stdin_raw.recv() => {
                    let Some(frame) = maybe_frame else {
                        info!("stdin closed; shutting down bridge");
                        break 'reconnect;
                    };

                    // Parse just enough to extract the request id for error
                    // responses — we don't need to validate the full schema.
                    let id = extract_id(&frame);

                    let Ok(text) = String::from_utf8(frame) else {
                        send_structured_error(
                            &tx_to_stdout,
                            id.as_ref(),
                            -32700,
                            "message is not valid UTF-8",
                        ).await;
                        continue;
                    };

                    debug!(len = text.len(), "extension → daemon");

                    if let Err(e) = sink.lock().await.send(Message::Text(text.into())).await {
                        warn!("WS send failed: {}; reconnecting", e);
                        // Notify the extension that this request failed.
                        send_structured_error(
                            &tx_to_stdout,
                            id.as_ref(),
                            -32000,
                            &format!("daemon connection lost: {e}"),
                        ).await;
                        break; // drop to reconnect outer loop
                    }
                }

                // ── Inbound from daemon (WS → stdout) ────────────────────────
                maybe_msg = ws_stream.next() => {
                    match maybe_msg {
                        Some(Ok(Message::Text(txt))) => {
                            debug!(len = txt.len(), "daemon → extension");
                            let bytes = txt.to_string().into_bytes();
                            if tx_to_stdout.send(bytes).await.is_err() {
                                info!("stdout channel closed; exiting");
                                break 'reconnect;
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            // The daemon only sends text frames, but handle
                            // binary gracefully.
                            if tx_to_stdout.send(bin.to_vec()).await.is_err() {
                                break 'reconnect;
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            // Respond to keep-alive pings.
                            let _ = sink.lock().await.send(Message::Pong(data)).await;
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            warn!("daemon closed WS connection; reconnecting");
                            break; // reconnect
                        }
                        Some(Err(e)) => {
                            warn!("WS error: {}; reconnecting", e);
                            break; // reconnect
                        }
                        Some(Ok(_)) => {} // Pong, etc.
                    }
                }
            }
        }

        // Pause briefly before reconnecting so we don't spin-loop on a
        // daemon that immediately closes connections.
        tokio::time::sleep(Duration::from_millis(BACKOFF_BASE_MS)).await;
    }

    // Signal the stdin reader to stop (it will exit naturally on next read).
    stdin_task.abort();
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Extract the `"id"` field from a raw JSON frame, if present.
/// Returns `None` for notifications / malformed frames.
fn extract_id(frame: &[u8]) -> Option<Value> {
    serde_json::from_slice::<Value>(frame)
        .ok()
        .and_then(|mut v| v.get_mut("id").map(Value::take))
}

/// Send a structured JSON-RPC error to the extension via the stdout channel.
async fn send_structured_error(
    tx: &mpsc::Sender<Vec<u8>>,
    id: Option<&Value>,
    code: i64,
    message: &str,
) {
    let payload = make_error_response(id, code, message);
    if let Ok(bytes) = serde_json::to_vec(&payload) {
        let _ = tx.send(bytes).await;
    }
}
