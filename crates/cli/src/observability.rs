//! Observability Surface (ADR-0008 / issue #27 / #174): Prometheus scrape adapter.
//!
//! Failure / lag / disk-warn facts come from the Deployment runtime Observability
//! assembly — CLI only serves HTTP and formats Prometheus text from that surface.

use std::net::SocketAddr;

use migraloop_runtime::{
    assemble_observability_surface, render_prometheus_metrics, status_inventory_from_url,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::CliError;

// Re-export runtime Observability helpers so CLI adapters share one event shape.
pub use migraloop_runtime::{emit_event, EventValue};

/// Serve Prometheus text exposition at `GET /metrics` until the process ends.
pub async fn serve_prometheus_metrics(
    metrics_addr: SocketAddr,
    platform_store_url: String,
) -> Result<(), CliError> {
    let listener = TcpListener::bind(metrics_addr).await.map_err(|err| {
        CliError::Failed(format!(
            "failed to bind Observability metrics listen address {metrics_addr}: {err}"
        ))
    })?;
    let bound = listener.local_addr().map_err(|err| {
        CliError::Failed(format!("failed to read metrics listen address: {err}"))
    })?;
    println!("Observability: metrics http://{bound}/metrics");
    emit_event(
        "metrics_listen",
        &[
            ("addr", EventValue::from(bound.to_string())),
            ("path", EventValue::from("/metrics")),
        ],
    );

    loop {
        let (mut socket, _) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                eprintln!("Observability metrics accept error: {err}");
                continue;
            }
        };
        let url = platform_store_url.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 4096];
            let n = match socket.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let req = String::from_utf8_lossy(&buf[..n]);
            let path = req.lines().next().unwrap_or("");
            let is_metrics = path.contains(" /metrics ") || path.starts_with("GET /metrics");
            let (status, content_type, body) = if is_metrics {
                match scrape_prometheus_metrics(&url).await {
                    Ok(body) => (
                        "200 OK",
                        "text/plain; version=0.0.4; charset=utf-8",
                        body,
                    ),
                    Err(err) => (
                        "503 Service Unavailable",
                        "text/plain; charset=utf-8",
                        format!("metrics unavailable: {err}\n"),
                    ),
                }
            } else {
                (
                    "404 Not Found",
                    "text/plain; charset=utf-8",
                    "not found\n".to_string(),
                )
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        });
    }
}

async fn scrape_prometheus_metrics(platform_store_url: &str) -> Result<String, String> {
    let inventory = status_inventory_from_url(platform_store_url)
        .await
        .map_err(|err| err.to_string())?;
    // Mirror prior metrics path: scrape durable lists when the store is reachable.
    // Unreachable open failures yield empty lists via status_inventory_from_url.
    if matches!(
        inventory.health,
        migraloop_platform_store::PlatformStoreHealth::Unreachable { .. }
    ) {
        return Err("Platform Store is unreachable".to_string());
    }
    let surface = assemble_observability_surface(&inventory);
    Ok(render_prometheus_metrics(&surface))
}
