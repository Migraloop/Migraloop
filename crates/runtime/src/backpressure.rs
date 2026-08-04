//! Bounded Backpressure seam inside Incremental Sync (ADR-0020 / issue #176).
//!
//! Distinct from Poison quarantine (ADR-0015) and Schema Change pause (ADR-0009):
//! Downstream slowness uses bounded queues and slowed capture/apply. Lag stays
//! visible; auto-pausing a Pipeline solely because the target is slow is not
//! the v1 default.

use crate::observability::{emit_event, EventValue};

/// Apply artificial Downstream Delivery delay when configured (fault injection / Lab).
pub(crate) async fn apply_delivery_delay(delay_ms: Option<u64>) {
    if let Some(ms) = delay_ms {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

/// Whether this bounded window should emit an Operator-visible backpressure signal.
///
/// True when the Incremental window is full (capture cannot pull more until the
/// window drains). Downstream delay alone does not pause Pipelines.
pub(crate) fn window_under_backpressure(queue_depth: usize, queue_capacity: usize) -> bool {
    queue_depth >= queue_capacity && queue_capacity > 0
}

/// Emit Operator-visible Backpressure signal for a full bounded window.
pub(crate) fn emit_backpressure(
    table: &str,
    deployment: &str,
    queue_depth: usize,
    queue_capacity: usize,
    lag: i32,
) {
    println!(
        "Backpressure: queue_depth={queue_depth} capacity={queue_capacity} lag={lag}"
    );
    emit_event(
        "backpressure",
        &[
            ("table", EventValue::from(table)),
            ("queue_depth", EventValue::from(queue_depth)),
            ("capacity", EventValue::from(queue_capacity)),
            ("lag", EventValue::from(lag)),
            ("deployment", EventValue::from(deployment)),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::window_under_backpressure;

    #[test]
    fn full_window_is_backpressure_not_pipeline_pause() {
        assert!(window_under_backpressure(4, 4));
        assert!(window_under_backpressure(5, 4));
        assert!(!window_under_backpressure(3, 4));
        assert!(!window_under_backpressure(0, 256));
    }
}
