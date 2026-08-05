//! Bounded Backpressure seam inside Incremental Sync (ADR-0020 / issue #203).
//!
//! Owns bounded-window queue policy: capacity, fetch sizing (including
//! already-applied skip allowance), window capping, full-window Operator-visible
//! signals, and Downstream delay injection. Distinct from Poison quarantine
//! (ADR-0015) and Schema Change pause (ADR-0009): Downstream slowness uses
//! bounded queues and slowed capture/apply. Lag stays visible; auto-pausing a
//! Pipeline solely because the target is slow is not the v1 default.

use crate::observability::{emit_event, EventValue};
use crate::sync_options::BackpressureOptions;

/// Bounded Incremental Capture window policy (ADR-0020).
///
/// Incremental Sync builds Source/schema candidates; this type decides how large
/// each window may be, how much to fetch given already-applied skips, when the
/// window is full (under backpressure), and Downstream delay injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundedWindow {
    capacity: usize,
    delivery_delay_ms: Option<u64>,
}

impl BoundedWindow {
    /// Build policy from typed [`BackpressureOptions`] (`SyncOptions.backpressure`).
    pub(crate) fn from_options(options: &BackpressureOptions) -> Self {
        let capacity = if options.queue_capacity == 0 {
            256
        } else {
            options.queue_capacity
        };
        Self {
            capacity,
            delivery_delay_ms: options.delivery_delay_ms,
        }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn delivery_delay_ms(&self) -> Option<u64> {
        self.delivery_delay_ms
    }

    /// Source fetch size so that, after skipping already-applied ids at/after the
    /// inclusive resume SCN, the window can still fill to capacity (issue #143).
    pub(crate) fn fetch_limit(&self, applied_skip_count: usize) -> Option<usize> {
        Some(self.capacity.saturating_add(applied_skip_count))
    }

    /// Cap skip-filtered candidates to this window's capacity.
    pub(crate) fn take_up_to_capacity<T, I>(&self, iter: I) -> Vec<T>
    where
        I: IntoIterator<Item = T>,
    {
        iter.into_iter().take(self.capacity).collect()
    }

    /// Truncate a built window (e.g. after merging Schema Change items) to capacity.
    pub(crate) fn truncate_to_capacity<T>(&self, items: &mut Vec<T>) {
        if items.len() > self.capacity {
            items.truncate(self.capacity);
        }
    }

    /// Whether a filled window of `queue_depth` is under backpressure (full).
    ///
    /// Full window slows further capture; Downstream delay alone does not pause
    /// Pipelines.
    pub(crate) fn is_full(&self, queue_depth: usize) -> bool {
        queue_depth >= self.capacity && self.capacity > 0
    }

    /// Observe a filled window: emit Operator-visible Backpressure when full.
    ///
    /// Returns whether the window is under backpressure. Does not pause Pipelines.
    pub(crate) fn observe_filled_window(
        &self,
        table: &str,
        deployment: &str,
        queue_depth: usize,
        lag: i32,
    ) -> bool {
        let under = self.is_full(queue_depth);
        if under {
            println!(
                "Backpressure: queue_depth={queue_depth} capacity={} lag={lag}",
                self.capacity
            );
            emit_event(
                "backpressure",
                &[
                    ("table", EventValue::from(table)),
                    ("queue_depth", EventValue::from(queue_depth)),
                    ("capacity", EventValue::from(self.capacity)),
                    ("lag", EventValue::from(lag)),
                    ("deployment", EventValue::from(deployment)),
                ],
            );
        }
        under
    }
}

/// Apply artificial Downstream Delivery delay when configured (fault injection / Lab).
///
/// Owned by the Backpressure seam; Poison retry loops apply it between Delivery
/// attempts. Delay alone does not pause Pipelines (ADR-0020).
pub(crate) async fn apply_delivery_delay(delay_ms: Option<u64>) {
    if let Some(ms) = delay_ms {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::BoundedWindow;
    use crate::sync_options::BackpressureOptions;

    #[test]
    fn from_options_uses_sync_options_queue_capacity_and_delay() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 4,
            delivery_delay_ms: Some(80),
        });
        assert_eq!(window.capacity(), 4);
        assert_eq!(window.delivery_delay_ms(), Some(80));
    }

    #[test]
    fn zero_capacity_normalizes_like_sync_options_default() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 0,
            delivery_delay_ms: None,
        });
        assert_eq!(window.capacity(), 256);
    }

    #[test]
    fn fetch_limit_allows_applied_skips_before_window_fill() {
        // Spec / issue #143: already-applied ids at the inclusive resume SCN must
        // be fetched and filtered *before* the bounded window limit, so same-SCN
        // siblings are not starved. Capacity 4 with 3 skips → fetch 7.
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 4,
            delivery_delay_ms: None,
        });
        assert_eq!(window.fetch_limit(3), Some(7));
        assert_eq!(window.fetch_limit(0), Some(4));
    }

    #[test]
    fn take_up_to_capacity_caps_candidates_to_window() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 2,
            delivery_delay_ms: None,
        });
        let capped = window.take_up_to_capacity([10, 20, 30, 40]);
        assert_eq!(capped, vec![10, 20]);
    }

    #[test]
    fn truncate_to_capacity_caps_merged_window_including_schema_items() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 3,
            delivery_delay_ms: None,
        });
        let mut items = vec!["a", "b", "c", "d", "e"];
        window.truncate_to_capacity(&mut items);
        assert_eq!(items, vec!["a", "b", "c"]);
    }

    #[test]
    fn full_window_is_backpressure_not_pipeline_pause() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 4,
            delivery_delay_ms: None,
        });
        assert!(window.is_full(4));
        assert!(window.is_full(5));
        assert!(!window.is_full(3));
        assert!(!window.is_full(0));

        let default_window = BoundedWindow::from_options(&BackpressureOptions::default());
        assert!(!default_window.is_full(0));
        assert!(default_window.is_full(256));
    }

    #[test]
    fn observe_filled_window_signals_only_when_full() {
        let window = BoundedWindow::from_options(&BackpressureOptions {
            queue_capacity: 2,
            delivery_delay_ms: None,
        });
        assert!(window.observe_filled_window("CUSTOMERS", "dep", 2, 10));
        assert!(!window.observe_filled_window("CUSTOMERS", "dep", 1, 10));
    }
}
