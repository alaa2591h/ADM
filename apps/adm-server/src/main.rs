use adm_engine::{Engine, EventBus};
use adm_gateway::{create_router, ApiState};
use adm_network::{Downloader, HttpNetworkClient};
use adm_storage::Storage;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    tracing::info!("Starting ADM Server");

    let storage = Arc::new(Storage::open("adm.db")?);
    let network_client = Arc::new(HttpNetworkClient::new());
    let downloader = Arc::new(Downloader::new(storage.clone(), network_client));
    let engine = Arc::new(Engine::new(storage.clone(), downloader));
    let event_bus = EventBus::new(256);

    let state = ApiState {
        engine,
        event_bus,
        enable_sse: true,
        auth_token: None,
    };
    let router = create_router(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:57423").await?;
    tracing::info!("Listening on {}", listener.local_addr()?);
    axum::serve(listener, router.into_make_service_with_connect_info::<std::net::SocketAddr>()).await?;

    Ok(())
}
