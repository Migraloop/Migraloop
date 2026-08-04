//! Poison Change Handling seam inside Incremental Sync (ADR-0015 / issue #176).
//!
//! Distinct from Schema Change pause (ADR-0009) and Backpressure (ADR-0020):
//! a single repeatedly failing Output Identity is quarantined after bounded
//! retries; the Pipeline keeps running. Never a whole-Pipeline pause for poison.

use std::collections::BTreeSet;

use migraloop_capture::ChangeEvent;
use migraloop_delivery::{DeliveryDocument, TargetEngine};
use migraloop_platform_store::{Pipeline, PlatformStore, QuarantinedChange};

use crate::backpressure::apply_delivery_delay;
use crate::observability::{emit_event, EventValue};
use crate::RuntimeError;

/// Internal label for an Output Identity (runtime quarantine / alert lines).
///
/// Matching for poison injection, Drift reconcile, and Delivery delete/upsert uses
/// [`migraloop_types::output_identity_key`] — not this formatter. Operator `status`
/// narrative formatting lives in the CLI adapter.
pub(crate) fn format_output_identity(identity: &serde_json::Value) -> String {
    match identity {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Whether an Output Identity matches a poison-injection key set.
///
/// Encoding matches Drift reconcile and Delivery identity keys
/// ([`migraloop_types::output_identity_key`]).
pub(crate) fn output_identity_matches_poison_keys(
    identity: &serde_json::Value,
    poison_keys: &BTreeSet<String>,
) -> bool {
    if poison_keys.is_empty() {
        return false;
    }
    poison_keys.contains(&migraloop_types::output_identity_key(identity))
}

/// Run a Delivery attempt with bounded retries and optional Downstream delay.
///
/// Shared by Transform maintain (and available to Direct paths) so Poison Change
/// Handling owns the retry+quarantine-bound policy (ADR-0015). Backpressure delay
/// is applied between attempts via the Backpressure seam helper.
pub(crate) async fn with_bounded_delivery_retries<T, F, Fut>(
    max_attempts: u32,
    delivery_delay_ms: Option<u64>,
    mut attempt_once: F,
) -> Result<T, (u32, String)>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay(delivery_delay_ms).await;
        match attempt_once().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_error = err;
                if attempt < max_attempts {
                    eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
                }
            }
        }
    }
    Err((max_attempts, last_error))
}

pub(crate) async fn upsert_with_bounded_retries<T: TargetEngine>(
    target: &T,
    collection: &str,
    document: &DeliveryDocument,
    max_attempts: u32,
    poison_keys: &BTreeSet<String>,
    delivery_delay_ms: Option<u64>,
) -> Result<usize, (u32, String)> {
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay(delivery_delay_ms).await;
        if output_identity_matches_poison_keys(&document.identity, poison_keys) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(&document.identity)
            );
        } else {
            match target
                .upsert_managed(collection, std::slice::from_ref(document))
                .await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

pub(crate) async fn delete_with_bounded_retries<T: TargetEngine>(
    target: &T,
    collection: &str,
    identity: &serde_json::Value,
    max_attempts: u32,
    poison_keys: &BTreeSet<String>,
    delivery_delay_ms: Option<u64>,
) -> Result<usize, (u32, String)> {
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay(delivery_delay_ms).await;
        if output_identity_matches_poison_keys(identity, poison_keys) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(identity)
            );
        } else {
            match target
                .delete_by_identity(collection, std::slice::from_ref(identity))
                .await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

pub(crate) async fn quarantine_poison_change(
    store: &PlatformStore,
    pipeline: &Pipeline,
    schema: &str,
    table: &str,
    change: &ChangeEvent,
    output_identity: serde_json::Value,
    stage: &str,
    attempts: u32,
    last_error: &str,
) -> Result<(), RuntimeError> {
    let record = QuarantinedChange {
        deployment_name: pipeline.deployment_name.clone(),
        pipeline_name: pipeline.name.clone(),
        source_schema: schema.to_string(),
        source_table: table.to_string(),
        change_id: change.change_id.clone(),
        capture_position: change.position.as_i64(),
        output_identity,
        stage: stage.to_string(),
        attempts: attempts as i32,
        last_error: last_error.to_string(),
        status: "quarantined".to_string(),
    };
    let identity_label = format_output_identity(&record.output_identity);
    store
        .quarantine_change(&record)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    eprintln!(
        "ALERT: Poison Change quarantined Pipeline={} identity={} change_id={} \
         stage={stage} attempts={attempts}: {last_error}",
        pipeline.name, identity_label, change.change_id
    );
    println!(
        "Quarantine: Pipeline={} identity={} change_id={} stage={stage} \
         attempts={attempts} unhealthy / not aligned",
        pipeline.name, identity_label, change.change_id
    );
    emit_event(
        "poison_quarantine",
        &[
            ("level", EventValue::from("alert")),
            ("pipeline", EventValue::from(pipeline.name.as_str())),
            ("identity", EventValue::from(identity_label.as_str())),
            ("change_id", EventValue::from(change.change_id.as_str())),
            ("stage", EventValue::from(stage)),
            ("attempts", EventValue::from(attempts as i64)),
        ],
    );
    Ok(())
}

#[cfg(test)]
mod output_identity_key_tests {
    use super::{format_output_identity, output_identity_matches_poison_keys};
    use migraloop_types::output_identity_key;
    use serde_json::json;
    use std::collections::BTreeSet;

    #[test]
    fn poison_matching_uses_shared_output_identity_key() {
        // Discriminator: string identities encode with JSON quotes. The former
        // poison formatter compared the bare string and would disagree with
        // Drift/Delivery keys for the same value.
        let poison = BTreeSet::from([r#""CUST-1""#.to_string()]);
        assert!(output_identity_matches_poison_keys(&json!("CUST-1"), &poison));
        assert!(!output_identity_matches_poison_keys(
            &json!("CUST-1"),
            &BTreeSet::from(["CUST-1".to_string()])
        ));
        assert_eq!(output_identity_key(&json!("CUST-1")), r#""CUST-1""#);
    }

    #[test]
    fn operator_display_label_stays_distinct_from_match_key_for_strings() {
        let identity = json!("CUST-1");
        assert_eq!(format_output_identity(&identity), "CUST-1");
        assert_eq!(output_identity_key(&identity), r#""CUST-1""#);
    }
}
