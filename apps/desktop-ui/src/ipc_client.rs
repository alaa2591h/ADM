use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream, MaybeTlsStream};

pub struct IpcClient {
    // A channel sender can be stored here to send messages to the websocket write half
    tx: tokio::sync::mpsc::Sender<String>,
}

impl IpcClient {
    pub async fn connect(url: &str, mut on_message: impl FnMut(String) + Send + 'static) -> Result<Self, Box<dyn std::error::Error>> {
        let (ws_stream, _) = connect_async(url).await?;
        let (mut write, mut read) = ws_stream.split();
        
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
        
        // Write task
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if write.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        });
        
        // Read task
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                if let Ok(Message::Text(text)) = msg {
                    on_message(text.to_string());
                }
            }
        });
        
        Ok(Self { tx })
    }
    
    pub async fn send_request(&self, method: &str, params: Value) -> Result<(), Box<dyn std::error::Error>> {
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": uuid::Uuid::new_v4().to_string()
        });
        self.tx.send(req.to_string()).await?;
        Ok(())
    }
}
