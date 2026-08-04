//! Typed Incremental Sync invocation options (issue #176).
//!
//! Poison Change Handling (ADR-0015), Schema Change pause (ADR-0009), and
//! Backpressure (ADR-0020) stay distinct policies. Fault-injection knobs and
//! queue bounds ride on [`SyncOptions`] so in-process runtime tests do not
//! depend on process env vars. Env vars remain a thin temporary compat shim
//! ([`SyncOptions::from_env_compat`]) for existing RQG / Lab twins.

use std::collections::BTreeSet;

/// Typed knobs for one Incremental Sync invocation.
///
/// Production CLI paths use [`SyncOptions::from_env_compat`]. In-process seam
/// tests construct explicit values (no env) so fault paths are injectable
/// through the Deployment runtime interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncOptions {
    /// Poison Change Handling (ADR-0015): quarantine + continue.
    pub poison: PoisonOptions,
    /// Bounded Backpressure (ADR-0020): queue capacity + optional delivery delay.
    pub backpressure: BackpressureOptions,
    /// Test fault: exit after N durable checkpoints (restart-resume coverage).
    pub fail_after_changes: Option<u32>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            poison: PoisonOptions::default(),
            backpressure: BackpressureOptions::default(),
            fail_after_changes: None,
        }
    }
}

/// Poison Change Handling knobs (ADR-0015).
///
/// Policy: after bounded Delivery retries, quarantine the Output Identity,
/// alert, and keep the Pipeline running — never pause the whole Pipeline for
/// a single poison identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoisonOptions {
    /// Bounded Delivery retries before quarantine. Must be > 0; default 3.
    pub max_attempts: u32,
    /// Output Identity keys ([`migraloop_types::output_identity_key`] encoding)
    /// that always fail Delivery (fault injection).
    pub poison_identity_keys: BTreeSet<String>,
}

impl Default for PoisonOptions {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            poison_identity_keys: BTreeSet::new(),
        }
    }
}

/// Bounded Backpressure knobs (ADR-0020).
///
/// Policy: stages use bounded queues and slow capture/apply; lag stays visible.
/// Auto-pausing a Pipeline solely because Downstream is slow is not the default
/// (pause remains for true blockers such as unblockable Schema Change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackpressureOptions {
    /// Max pending changes materialized per Incremental window. Must be > 0; default 256.
    pub queue_capacity: usize,
    /// Artificial Downstream Delivery slowness (milliseconds) for fault injection / Lab.
    pub delivery_delay_ms: Option<u64>,
}

impl Default for BackpressureOptions {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            delivery_delay_ms: None,
        }
    }
}

impl SyncOptions {
    /// Production defaults with no fault injection.
    pub fn production() -> Self {
        Self::default()
    }

    /// Thin temporary compat shim: read legacy env knobs so existing RQG / Lab
    /// twins keep passing while typed options become the primary test adapter.
    pub fn from_env_compat() -> Self {
        Self {
            poison: PoisonOptions {
                max_attempts: env_u32_gt0("MIGRALOOP_POISON_MAX_ATTEMPTS").unwrap_or(3),
                poison_identity_keys: env_csv_set("MIGRALOOP_DELIVERY_POISON_IDENTITIES"),
            },
            backpressure: BackpressureOptions {
                queue_capacity: env_usize_gt0("MIGRALOOP_SYNC_QUEUE_CAPACITY").unwrap_or(256),
                delivery_delay_ms: env_u64_gt0("MIGRALOOP_DELIVERY_DELAY_MS"),
            },
            fail_after_changes: env_u32_gt0("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES"),
        }
        .normalized()
    }

    /// Clamp knobs that must be > 0 (same contract as the legacy env filters).
    pub(crate) fn normalized(mut self) -> Self {
        if self.poison.max_attempts == 0 {
            self.poison.max_attempts = 3;
        }
        if self.backpressure.queue_capacity == 0 {
            self.backpressure.queue_capacity = 256;
        }
        self
    }
}

fn env_u32_gt0(name: &str) -> Option<u32> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
}

fn env_u64_gt0(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
}

fn env_usize_gt0(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
}

fn env_csv_set(name: &str) -> BTreeSet<String> {
    std::env::var(name)
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Process-global env knobs need exclusive access across parallel unit tests.
    static ENV_COMPAT_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn production_defaults_match_documented_knobs() {
        let opts = SyncOptions::production();
        assert_eq!(opts.poison.max_attempts, 3);
        assert!(opts.poison.poison_identity_keys.is_empty());
        assert_eq!(opts.backpressure.queue_capacity, 256);
        assert_eq!(opts.backpressure.delivery_delay_ms, None);
        assert_eq!(opts.fail_after_changes, None);
    }

    #[test]
    fn normalized_clamps_zero_capacity_and_attempts() {
        let opts = SyncOptions {
            poison: PoisonOptions {
                max_attempts: 0,
                poison_identity_keys: BTreeSet::new(),
            },
            backpressure: BackpressureOptions {
                queue_capacity: 0,
                delivery_delay_ms: None,
            },
            fail_after_changes: None,
        }
        .normalized();
        assert_eq!(opts.poison.max_attempts, 3);
        assert_eq!(opts.backpressure.queue_capacity, 256);
    }

    #[test]
    fn from_env_compat_reads_legacy_knobs() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_POISON_MAX_ATTEMPTS", "2");
        std::env::set_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES", "1, 42");
        std::env::set_var("MIGRALOOP_SYNC_QUEUE_CAPACITY", "4");
        std::env::set_var("MIGRALOOP_DELIVERY_DELAY_MS", "80");
        std::env::set_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "1");

        let opts = SyncOptions::from_env_compat();
        assert_eq!(opts.poison.max_attempts, 2);
        assert_eq!(
            opts.poison.poison_identity_keys,
            BTreeSet::from(["1".into(), "42".into()])
        );
        assert_eq!(opts.backpressure.queue_capacity, 4);
        assert_eq!(opts.backpressure.delivery_delay_ms, Some(80));
        assert_eq!(opts.fail_after_changes, Some(1));

        std::env::remove_var("MIGRALOOP_POISON_MAX_ATTEMPTS");
        std::env::remove_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
        std::env::remove_var("MIGRALOOP_SYNC_QUEUE_CAPACITY");
        std::env::remove_var("MIGRALOOP_DELIVERY_DELAY_MS");
        std::env::remove_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
    }
}
