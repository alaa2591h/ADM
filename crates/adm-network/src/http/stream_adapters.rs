use crate::{NetworkError, ResponseStream};
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncReadExt};

pub struct AsyncReadResponseStream<R> {
    reader: Option<R>,
    buf: Vec<u8>,
}

impl<R: AsyncRead> AsyncReadResponseStream<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            buf: vec![0u8; 256 * 1024], // 256 KiB buffer
        }
    }
}

#[async_trait]
impl<R: AsyncRead + Send + Sync + Unpin + 'static> ResponseStream for AsyncReadResponseStream<R> {
    async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, NetworkError> {
        let reader = match self.reader.as_mut() {
            Some(r) => r,
            None => return Ok(None),
        };

        match reader.read(&mut self.buf).await {
            Ok(0) => {
                self.reader.take();
                Ok(None)
            }
            Ok(n) => Ok(Some(self.buf[..n].to_vec())),
            Err(e) => Err(NetworkError::Io(e.to_string())),
        }
    }

    fn total_bytes(&self) -> Option<u64> {
        None
    }

    async fn cancel(&mut self) -> Result<(), NetworkError> {
        self.reader.take();
        Ok(())
    }
}
