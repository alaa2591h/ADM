//! Server-Sent Events (optional) — long-lived HTTP stream of domain events.

use axum::{
  extract::State,
  response::sse::{Event, KeepAlive, Sse},
  response::IntoResponse,
};
use futures_util::stream;
use std::convert::Infallible;
use std::time::Duration;

use crate::rest::ApiState;

pub async fn events_stream(State(state): State<ApiState>) -> impl IntoResponse {
  let mut rx = state.event_bus.subscribe();
  let stream = stream::unfold(rx, |mut rx| async move {
    loop {
      match rx.recv().await {
        Ok(evt) => {
          let data = serde_json::to_string(&evt).unwrap_or_default();
          return Some((Ok::<Event, Infallible>(Event::default().data(data)), rx));
        }
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
          // Skip lagged messages and continue receiving
          continue;
        }
        Err(_) => return None,
      }
    }
  });

  Sse::new(stream).keep_alive(
    KeepAlive::new()
      .interval(Duration::from_secs(15))
      .text("ping"),
  )
}
