use crate::ipc_client::IpcClient;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DaemonBridge {
    client: Arc<Mutex<Option<IpcClient>>>,
}

impl DaemonBridge {
    pub fn new() -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
        }
    }
    
    pub async fn connect(&self, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        let client = IpcClient::connect(url, |msg| {
            // Handle incoming messages and update UI
            println!("Received from daemon: {}", msg);
        }).await?;
        
        *self.client.lock().await = Some(client);
        Ok(())
    }
    
    pub async fn add_download(&self, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(client) = self.client.lock().await.as_ref() {
            client.send_request("downloads.add", serde_json::json!({ "url": url })).await?;
        }
        Ok(())
    }
}
