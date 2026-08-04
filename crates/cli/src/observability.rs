//! Observability Surface (ADR-0008 / issue #27): structured operator logs and
//! Prometheus metrics scraped from durable Platform Store health state.

use std::collections::BTreeMap;
use std::net::SocketAddr;

use migraloop_platform_store::{probe_store_resources, PlatformStore};
use migraloop_runtime::status_inventory;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::CliError;

/// Emit one structured JSON operator event line (stdout).
///
/// Human-readable companion lines stay elsewhere; this is the machine-parseable
/// Observability Surface contract for key operator events.
pub fn emit_event(event: &str, fields: &[(&str, EventValue)]) {
    let mut map = BTreeMap::new();
    map.insert("event".to_string(), EventValue::Str(event.to_string()));
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    match serde_json::to_string(&map) {
        Ok(json) => println!("{json}"),
        Err(err) => eprintln!("structured log encode failed for event={event}: {err}"),
    }
}

#[derive(Clone, Debug)]
pub enum EventValue {
    Str(String),
    Int(i64),
    Bool(bool),
}

impl serde::Serialize for EventValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EventValue::Str(s) => serializer.serialize_str(s),
            EventValue::Int(n) => serializer.serialize_i64(*n),
            EventValue::Bool(b) => serializer.serialize_bool(*b),
        }
    }
}

impl From<&str> for EventValue {
    fn from(value: &str) -> Self {
        EventValue::Str(value.to_string())
    }
}

impl From<String> for EventValue {
    fn from(value: String) -> Self {
        EventValue::Str(value)
    }
}

impl From<i64> for EventValue {
    fn from(value: i64) -> Self {
        EventValue::Int(value)
    }
}

impl From<i32> for EventValue {
    fn from(value: i32) -> Self {
        EventValue::Int(i64::from(value))
    }
}

impl From<usize> for EventValue {
    fn from(value: usize) -> Self {
        EventValue::Int(value as i64)
    }
}

impl From<bool> for EventValue {
    fn from(value: bool) -> Self {
        EventValue::Bool(value)
    }
}

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
                match render_prometheus_metrics(&url).await {
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

async fn render_prometheus_metrics(platform_store_url: &str) -> Result<String, String> {
    let store = PlatformStore::open(platform_store_url)
        .await
        .map_err(|err| err.to_string())?;
    let inventory = status_inventory(&store)
        .await
        .map_err(|err| err.to_string())?;
    let bases = inventory.bases;
    let pipelines = inventory.pipelines;
    let quarantines = inventory.quarantines;
    let schema_impacts = inventory.schema_impacts;

    let mut out = String::new();
    out.push_str("# HELP migraloop_sync_lag Sync Health lag (pending Source changes not yet applied to Base).\n");
    out.push_str("# TYPE migraloop_sync_lag gauge\n");
    for base in &bases {
        out.push_str(&format!(
            "migraloop_sync_lag{{deployment=\"{}\",table=\"{}\"}} {}\n",
            prom_label(&base.deployment_name),
            prom_label(&base.source_table),
            base.sync_lag
        ));
    }

    out.push_str(
        "# HELP migraloop_sync_applied_changes Sync Health applied change count for a Base Dataset.\n",
    );
    out.push_str("# TYPE migraloop_sync_applied_changes gauge\n");
    for base in &bases {
        out.push_str(&format!(
            "migraloop_sync_applied_changes{{deployment=\"{}\",table=\"{}\"}} {}\n",
            prom_label(&base.deployment_name),
            prom_label(&base.source_table),
            base.sync_applied_changes
        ));
    }

    out.push_str("# HELP migraloop_delivery_lag Delivery Health lag (pending Delivery work).\n");
    out.push_str("# TYPE migraloop_delivery_lag gauge\n");
    for pipeline in &pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "migraloop_delivery_lag{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.name),
            pipeline.delivery_lag
        ));
    }

    out.push_str(
        "# HELP migraloop_delivery_applied_changes Delivery Health applied change count for a Pipeline.\n",
    );
    out.push_str("# TYPE migraloop_delivery_applied_changes gauge\n");
    for pipeline in &pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "migraloop_delivery_applied_changes{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.name),
            pipeline.delivery_applied_changes
        ));
    }

    out.push_str("# HELP migraloop_pipeline_paused Whether a Pipeline is paused (1) or not (0).\n");
    out.push_str("# TYPE migraloop_pipeline_paused gauge\n");
    for pipeline in &pipelines {
        out.push_str(&format!(
            "migraloop_pipeline_paused{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.name),
            if pipeline.paused { 1 } else { 0 }
        ));
    }

    out.push_str(
        "# HELP migraloop_quarantined_changes Alertable count of active Poison Change quarantines per Pipeline.\n",
    );
    out.push_str("# TYPE migraloop_quarantined_changes gauge\n");
    for pipeline in &pipelines {
        let count = quarantines
            .iter()
            .filter(|q| {
                q.deployment_name == pipeline.deployment_name && q.pipeline_name == pipeline.name
            })
            .count();
        out.push_str(&format!(
            "migraloop_quarantined_changes{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.name),
            count
        ));
    }

    out.push_str(
        "# HELP migraloop_failures Alertable failure gauge (active quarantines + blocking Schema Change impacts).\n",
    );
    out.push_str("# TYPE migraloop_failures gauge\n");
    let blocking = schema_impacts
        .iter()
        .filter(|s| s.impact == "blocking")
        .count();
    let failures = quarantines.len() + blocking;
    out.push_str(&format!("migraloop_failures {failures}\n"));

    // Platform Store resource signals (ADR-0010): warn-only disk threshold.
    let resources = probe_store_resources(platform_store_url)
        .await
        .map_err(|err| err.to_string())?;
    out.push_str(
        "# HELP migraloop_platform_store_disk_free_bytes Free bytes on the Platform Store data volume when known (-1 if unknown).\n",
    );
    out.push_str("# TYPE migraloop_platform_store_disk_free_bytes gauge\n");
    let free_metric = resources
        .free_disk_bytes
        .map(|b| b as i64)
        .unwrap_or(-1);
    out.push_str(&format!(
        "migraloop_platform_store_disk_free_bytes {free_metric}\n"
    ));
    out.push_str(
        "# HELP migraloop_platform_store_disk_warn Whether Platform Store free disk is below the warn threshold (1) or not (0). Warn-only — never auto-pauses Pipelines.\n",
    );
    out.push_str("# TYPE migraloop_platform_store_disk_warn gauge\n");
    out.push_str(&format!(
        "migraloop_platform_store_disk_warn {}\n",
        if resources.disk_warn { 1 } else { 0 }
    ));

    Ok(out)
}

fn prom_label(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '\\' => "\\\\".to_string(),
            '"' => "\\\"".to_string(),
            '\n' => "\\n".to_string(),
            other => other.to_string(),
        })
        .collect()
}
