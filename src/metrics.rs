use axum::{Router, response::Json, routing::get};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;

#[derive(Debug, Default, Serialize, Clone)]
pub struct Metrics {
    pub events_flushed: usize,
    pub guardian_flushed: usize,
    pub last_flush_ts: i64,
    pub errors: usize,
    pub queue_events: usize,
    pub queue_guardian: usize,
}

pub async fn run_metrics_server(metrics: Arc<Mutex<Metrics>>, port: u16) {
    let app = Router::new().route(
        "/",
        get({
            let metrics = metrics.clone();
            async move || {
                let m = metrics.lock().unwrap();
                Json((*m).clone())
            }
        }),
    );

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
