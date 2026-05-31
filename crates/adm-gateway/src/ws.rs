//! WebSocket upgrade — streams domain events to connected clients.

use axum::{
  extract::{
    ws::{Message, WebSocket},
    ConnectInfo, Query, State, WebSocketUpgrade,
  },
  http::StatusCode,
  response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::net::SocketAddr;

use crate::rest::ApiState;

#[derive(Deserialize)]
pub struct WsParams {
  pub token: Option<String>,
}

pub async fn websocket_handler(
  ConnectInfo(addr): ConnectInfo<SocketAddr>,
  Query(params): Query<WsParams>,
  ws: WebSocketUpgrade,
  State(state): State<ApiState>,
) -> impl IntoResponse {
  // 🔐 WebSocket Auth 1: Only allow localhost connections by default
  // This matches the daemon's legacy security policy.
  if !addr.ip().is_loopback() {
    tracing::warn!(
      "🚫 WebSocket connection denied from non-localhost IP: {}",
      addr.ip()
    );
    return (
      StatusCode::FORBIDDEN,
      "❌ WebSocket connections only allowed from localhost",
    )
      .into_response();
  }

  // 🔐 WebSocket Auth 2: Token-based authentication (if configured)
  if let Some(ref required_token) = state.auth_token {
    match params.token {
      Some(token) if token == *required_token => {
        tracing::debug!("✅ WebSocket connection authenticated with token from {}", addr);
      }
      _ => {
        tracing::warn!("🚫 WebSocket connection denied from {}: missing or invalid token", addr);
        return (
          StatusCode::UNAUTHORIZED,
          "❌ WebSocket connection requires a valid ?token=...",
        )
          .into_response();
      }
    }
  }

  ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(socket: WebSocket, state: ApiState) {
  let (mut sender, mut receiver) = socket.split();
  let mut events = state.event_bus.subscribe();

  loop {
    tokio::select! {
      maybe_client = receiver.next() => {
        match maybe_client {
          Some(Ok(Message::Close(_))) | None => break,
          Some(Ok(Message::Ping(payload))) => {
            if sender.send(Message::Pong(payload)).await.is_err() {
              break;
            }
          }
          _ => {}
        }
      }
      maybe_evt = events.recv() => {
        match maybe_evt {
          Ok(evt) => {
            let text = match serde_json::to_string(&evt) {
              Ok(t) => t,
              Err(_) => continue,
            };
            if sender.send(Message::Text(text.into())).await.is_err() {
              break;
            }
          }
          Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
            while events.try_recv().is_ok() {}
          }
          Err(_) => break,
        }
      }
    }
  }
}
