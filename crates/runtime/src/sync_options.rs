//! Typed Incremental Sync invocation options (issues #176 / #180).
//!
//! Poison Change Handling (ADR-0015), Schema Change pause (ADR-0009), and
//! Backpressure (ADR-0020) stay distinct policies. Fault-injection knobs and
//! queue bounds ride on [`SyncOptions`] so RQG / Lab / in-process tests prefer
//! typed options (CLI flags or explicit structs) over process env vars.
//!
//! Legacy fault-injection env vars remain a thin temporary compat shim inside
//! [`SyncOptions::for_cli`] / [`SyncOptions::from_env_compat`] when typed
//! overrides are unset — not the primary test adapter (#180).

use std::collections::BTreeSet;

/// Typed knobs for one Incremental Sync invocation.
///
/// Production CLI paths build options via [`SyncOptions::for_cli`] (Operator env
/// knobs + optional typed overrides). In-process seam tests construct explicit
/// values (no env) so fault paths are injectable through the Deployment runtime
/// interface.
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

/// Typed overrides for [`SyncOptions::for_cli`] (CLI flags / Lab knobs).
///
/// When a field is `None` / empty, [`SyncOptions::for_cli`] may fall back to the
/// thin temporary env shim for that field. Prefer setting these explicitly for
/// new fault cases (#180).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncOptionsOverrides {
    /// When `Some` (including empty), replaces poison identity keys (no env fallback).
    pub poison_identity_keys: Option<BTreeSet<String>>,
    pub poison_max_attempts: Option<u32>,
    pub queue_capacity: Option<usize>,
    /// When `Some`, sets delivery delay (including clearing via callers that pass None? —
    /// use `Some(ms)` to set; leave `None` for env shim / production default).
    pub delivery_delay_ms: Option<u64>,
    pub fail_after_changes: Option<u32>,
    /// When true, `poison_identity_keys: None` does not read legacy env (typed path only).
    pub omit_env_fault_shim: bool,
}

impl SyncOptions {
    /// Production defaults with no fault injection.
    pub fn production() -> Self {
        Self::default()
    }

    /// Operator-facing knobs from env (queue capacity, poison max attempts).
    ///
    /// Does **not** read Test/Lab fault-injection env vars — those belong on
    /// typed overrides / CLI flags (#180).
    pub fn from_operator_env() -> Self {
        Self {
            poison: PoisonOptions {
                max_attempts: env_u32_gt0("MIGRALOOP_POISON_MAX_ATTEMPTS").unwrap_or(3),
                poison_identity_keys: BTreeSet::new(),
            },
            backpressure: BackpressureOptions {
                queue_capacity: env_usize_gt0("MIGRALOOP_SYNC_QUEUE_CAPACITY").unwrap_or(256),
                delivery_delay_ms: None,
            },
            fail_after_changes: None,
        }
        .normalized()
    }

    /// Build options for the Operator CLI / Lab product path.
    ///
    /// Typed `overrides` are the primary fault-injection adapter. Legacy
    /// fault-injection env vars remain a thin temporary shim when the matching
    /// override is unset and [`SyncOptionsOverrides::omit_env_fault_shim`] is
    /// false (#180).
    pub fn for_cli(overrides: SyncOptionsOverrides) -> Self {
        let mut opts = Self::from_operator_env();

        if let Some(n) = overrides.poison_max_attempts {
            opts.poison.max_attempts = n;
        }
        if let Some(n) = overrides.queue_capacity {
            opts.backpressure.queue_capacity = n;
        }

        if let Some(keys) = overrides.poison_identity_keys {
            opts.poison.poison_identity_keys = keys;
        } else if !overrides.omit_env_fault_shim {
            opts.poison.poison_identity_keys = env_csv_set("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
        }

        if let Some(ms) = overrides.delivery_delay_ms {
            opts.backpressure.delivery_delay_ms = Some(ms);
        } else if !overrides.omit_env_fault_shim {
            opts.backpressure.delivery_delay_ms = env_u64_gt0("MIGRALOOP_DELIVERY_DELAY_MS");
        }

        if let Some(n) = overrides.fail_after_changes {
            opts.fail_after_changes = Some(n);
        } else if !overrides.omit_env_fault_shim {
            opts.fail_after_changes = env_u32_gt0("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
        }

        opts.normalized()
    }

    /// Thin temporary compat shim: Operator env knobs + legacy fault-injection env.
    ///
    /// Prefer [`SyncOptions::for_cli`] with typed overrides, or explicit
    /// [`SyncOptions`] in-process. Kept so any leftover env-only callers keep
    /// working while twins / Lab migrate (#180).
    pub fn from_env_compat() -> Self {
        Self::for_cli(SyncOptionsOverrides::default())
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

    #[test]
    fn for_cli_typed_overrides_win_over_legacy_env_shim() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES", "env-only");
        std::env::set_var("MIGRALOOP_DELIVERY_DELAY_MS", "999");
        std::env::set_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "9");
        std::env::set_var("MIGRALOOP_SYNC_QUEUE_CAPACITY", "8");

        let opts = SyncOptions::for_cli(SyncOptionsOverrides {
            poison_identity_keys: Some(BTreeSet::from(["typed".into()])),
            poison_max_attempts: Some(2),
            queue_capacity: Some(4),
            delivery_delay_ms: Some(80),
            fail_after_changes: Some(1),
            omit_env_fault_shim: false,
        });

        assert_eq!(
            opts.poison.poison_identity_keys,
            BTreeSet::from(["typed".into()])
        );
        assert_eq!(opts.poison.max_attempts, 2);
        assert_eq!(opts.backpressure.queue_capacity, 4);
        assert_eq!(opts.backpressure.delivery_delay_ms, Some(80));
        assert_eq!(opts.fail_after_changes, Some(1));

        std::env::remove_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
        std::env::remove_var("MIGRALOOP_DELIVERY_DELAY_MS");
        std::env::remove_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
        std::env::remove_var("MIGRALOOP_SYNC_QUEUE_CAPACITY");
    }

    #[test]
    fn for_cli_omit_env_fault_shim_ignores_legacy_fault_env() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES", "env-only");
        std::env::set_var("MIGRALOOP_DELIVERY_DELAY_MS", "999");
        std::env::set_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "9");

        let opts = SyncOptions::for_cli(SyncOptionsOverrides {
            omit_env_fault_shim: true,
            ..SyncOptionsOverrides::default()
        });
        assert!(opts.poison.poison_identity_keys.is_empty());
        assert_eq!(opts.backpressure.delivery_delay_ms, None);
        assert_eq!(opts.fail_after_changes, None);

        std::env::remove_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
        std::env::remove_var("MIGRALOOP_DELIVERY_DELAY_MS");
        std::env::remove_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
    }

    #[test]
    fn from_operator_env_skips_fault_injection_env() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_POISON_MAX_ATTEMPTS", "2");
        std::env::set_var("MIGRALOOP_SYNC_QUEUE_CAPACITY", "4");
        std::env::set_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES", "1");
        std::env::set_var("MIGRALOOP_DELIVERY_DELAY_MS", "80");
        std::env::set_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "1");

        let opts = SyncOptions::from_operator_env();
        assert_eq!(opts.poison.max_attempts, 2);
        assert_eq!(opts.backpressure.queue_capacity, 4);
        assert!(opts.poison.poison_identity_keys.is_empty());
        assert_eq!(opts.backpressure.delivery_delay_ms, None);
        assert_eq!(opts.fail_after_changes, None);

        std::env::remove_var("MIGRALOOP_POISON_MAX_ATTEMPTS");
        std::env::remove_var("MIGRALOOP_SYNC_QUEUE_CAPACITY");
        std::env::remove_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
        std::env::remove_var("MIGRALOOP_DELIVERY_DELAY_MS");
        std::env::remove_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
    }
}
