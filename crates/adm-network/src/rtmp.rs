//! Minimal RTMP client: connects to RTMP/RTMPS servers and reads stream bytes
//! This is a lightweight implementation suitable for recording live streams
//! chunk-by-chunk. It does not implement the full RTMP message set; instead
//! it performs a simple handshake and then relays the incoming bytes to a
//! provided sink callback. Extend as needed for chunking and file rotation.

use anyhow::Result;
use std::net::ToSocketAddrs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_native_tls::native_tls::TlsConnector;
use tokio_native_tls::TlsStream;

pub enum RtmpStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

pub struct RtmpClient {
    url: String,
}

impl RtmpClient {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }

    /// Connect to the RTMP server. Supports `rtmp://` and `rtmps://`.
    pub async fn connect(&self) -> Result<RtmpStream> {
        let parsed = url::Url::parse(&self.url)?;
        let host = parsed
            .host_str()
            .ok_or_else(|| anyhow::anyhow!("no host in url"))?;
        let port = parsed.port().unwrap_or_else(|| match parsed.scheme() {
            "rtmp" => 1935,
            "rtmps" => 443,
            _ => 1935,
        });
        let addr = format!("{}:{}", host, port);

        let addrs = addr.to_socket_addrs()?;
        let stream = TcpStream::connect(addrs.as_slice()).await?;

        match parsed.scheme() {
            "rtmp" => Ok(RtmpStream::Plain(stream)),
            "rtmps" => {
                let cx = TlsConnector::builder().build()?;
                let cx = tokio_native_tls::TlsConnector::from(cx);
                let tls = cx.connect(host, stream).await?;
                Ok(RtmpStream::Tls(Box::new(tls)))
            }
            _ => Ok(RtmpStream::Plain(stream)),
        }
    }

    /// Perform the basic RTMP C0C1/C2 handshake. Returns after handshake completes.
    async fn do_handshake_stream<S: AsyncReadExt + AsyncWriteExt + Unpin + Send>(
        &self,
        stream: &mut S,
    ) -> Result<()> {
        // C0: version (0x03)
        stream.write_u8(0x03).await?;
        // C1: 1536 bytes. We'll send zeros except for time and zero.
        let c1 = vec![0u8; 1536];
        stream.write_all(&c1).await?;
        stream.flush().await?;

        // Read S0S1S2 (server version + 1536 + 1536)
        let _s0 = stream.read_u8().await?;
        let mut s1 = vec![0u8; 1536];
        stream.read_exact(&mut s1).await?;
        let mut s2 = vec![0u8; 1536];
        stream.read_exact(&mut s2).await?;

        // Send C2 (echo s1)
        stream.write_all(&s1).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Start reading the stream and forward raw bytes to the provided sender.
    /// The sender receives `Vec<u8>` chunks; rotation and file handling is the caller's responsibility.
    pub async fn read_loop(&self, sink: mpsc::UnboundedSender<Vec<u8>>) -> Result<()> {
        match self.connect().await? {
            RtmpStream::Plain(mut s) => {
                self.do_handshake_stream(&mut s).await?;
                let mut buf = [0u8; 64 * 1024];
                loop {
                    let n = s.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    let chunk = buf[..n].to_vec();
                    let _ = sink.send(chunk);
                }
            }
            RtmpStream::Tls(mut s) => {
                self.do_handshake_stream(&mut s).await?;
                let mut buf = [0u8; 64 * 1024];
                loop {
                    let n = s.read(&mut buf).await?;
                    if n == 0 {
                        break;
                    }
                    let chunk = buf[..n].to_vec();
                    let _ = sink.send(chunk);
                }
            }
        }
        Ok(())
    }
}
