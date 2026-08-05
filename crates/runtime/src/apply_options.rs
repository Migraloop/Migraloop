//! Typed Apply / Initial Load invocation options (issue #200).
//!
//! Chunk size, rows-per-sec throttle, pause-after-chunks, and store-delay ride on
//! [`ApplyOptions`] so Lab / RQG / in-process tests prefer typed options (CLI flags
//! or explicit structs) over process env vars — the same pattern as [`crate::SyncOptions`].
//!
//! Legacy Initial Load env vars remain a thin temporary compat shim inside
//! [`ApplyOptions::for_cli`] / [`ApplyOptions::from_env_compat`] when typed
//! overrides are unset — not the primary test adapter (#200).

/// Typed knobs for one Deployment `apply` / Initial Load invocation.
///
/// Production CLI paths build options via [`ApplyOptions::for_cli`] (Operator env
/// knobs + optional typed overrides). In-process seam tests construct explicit
/// values (no env) so throttle / pause / pressure paths are injectable through the
/// Deployment runtime interface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOptions {
    /// Initial Load Source-pressure and Test/Lab inject knobs.
    pub initial_load: InitialLoadOptions,
}

impl Default for ApplyOptions {
    fn default() -> Self {
        Self {
            initial_load: InitialLoadOptions::default(),
        }
    }
}

/// Initial Load knobs (bounded chunks, throttle, pause/resume inject, store pressure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialLoadOptions {
    /// Bounded Source read window. Must be > 0; default 1000.
    pub chunk_size: usize,
    /// Optional Operator throttle (rows/sec). `None` = no artificial cap beyond chunking.
    pub rows_per_sec: Option<u64>,
    /// Test/Lab inject: pause Initial Load after N successful chunks.
    pub pause_after_chunks: Option<u64>,
    /// Test/Lab inject: artificial Platform Store / Downstream delay (ms).
    pub store_delay_ms: Option<u64>,
}

impl Default for InitialLoadOptions {
    fn default() -> Self {
        Self {
            chunk_size: 1000,
            rows_per_sec: None,
            pause_after_chunks: None,
            store_delay_ms: None,
        }
    }
}

/// Typed overrides for [`ApplyOptions::for_cli`] (CLI flags / Lab knobs).
///
/// When a field is `None`, [`ApplyOptions::for_cli`] may fall back to the thin
/// temporary env shim for that field. Prefer setting these explicitly for new
/// Lab / RQG cases (#200).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ApplyOptionsOverrides {
    pub chunk_size: Option<usize>,
    pub rows_per_sec: Option<u64>,
    pub pause_after_chunks: Option<u64>,
    pub store_delay_ms: Option<u64>,
    /// When true, unset Lab-inject overrides do not read legacy env (typed path only).
    pub omit_env_shim: bool,
}

impl ApplyOptions {
    /// Production defaults with no Lab inject and no artificial throttle.
    pub fn production() -> Self {
        Self::default()
    }

    /// Operator-facing knobs from env (chunk size, rows-per-sec).
    ///
    /// Does **not** read Test/Lab inject env vars — those belong on typed
    /// overrides / CLI flags (#200).
    pub fn from_operator_env() -> Self {
        Self {
            initial_load: InitialLoadOptions {
                chunk_size: env_usize_gt0("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE").unwrap_or(1000),
                rows_per_sec: env_u64_gt0("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC"),
                pause_after_chunks: None,
                store_delay_ms: None,
            },
        }
        .normalized()
    }

    /// Build options for the Operator CLI / Lab product path.
    ///
    /// Typed `overrides` are the primary Lab / RQG adapter. Legacy Initial Load
    /// env vars remain a thin temporary shim when the matching override is unset
    /// and [`ApplyOptionsOverrides::omit_env_shim`] is false (#200).
    pub fn for_cli(overrides: ApplyOptionsOverrides) -> Self {
        let mut opts = Self::from_operator_env();

        if let Some(n) = overrides.chunk_size {
            opts.initial_load.chunk_size = n;
        }

        if let Some(n) = overrides.rows_per_sec {
            // `0` clears the throttle (same contract as legacy env filter).
            opts.initial_load.rows_per_sec = if n > 0 { Some(n) } else { None };
        }

        if let Some(n) = overrides.pause_after_chunks {
            opts.initial_load.pause_after_chunks = if n > 0 { Some(n) } else { None };
        } else if !overrides.omit_env_shim {
            opts.initial_load.pause_after_chunks =
                env_u64_gt0("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
        }

        if let Some(n) = overrides.store_delay_ms {
            opts.initial_load.store_delay_ms = if n > 0 { Some(n) } else { None };
        } else if !overrides.omit_env_shim {
            opts.initial_load.store_delay_ms = env_u64_gt0("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
        }

        opts.normalized()
    }

    /// Thin temporary compat shim: Operator env knobs + legacy Lab-inject env.
    ///
    /// Prefer [`ApplyOptions::for_cli`] with typed overrides, or explicit
    /// [`ApplyOptions`] in-process. Kept so any leftover env-only callers keep
    /// working while twins / Lab migrate (#200).
    pub fn from_env_compat() -> Self {
        Self::for_cli(ApplyOptionsOverrides::default())
    }

    /// Clamp knobs that must be > 0 (same contract as the legacy env filters).
    pub(crate) fn normalized(mut self) -> Self {
        if self.initial_load.chunk_size == 0 {
            self.initial_load.chunk_size = 1000;
        }
        self
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Process-global env knobs need exclusive access across parallel unit tests.
    static ENV_COMPAT_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn production_defaults_match_documented_knobs() {
        let opts = ApplyOptions::production();
        assert_eq!(opts.initial_load.chunk_size, 1000);
        assert_eq!(opts.initial_load.rows_per_sec, None);
        assert_eq!(opts.initial_load.pause_after_chunks, None);
        assert_eq!(opts.initial_load.store_delay_ms, None);
    }

    #[test]
    fn normalized_clamps_zero_chunk_size() {
        let opts = ApplyOptions {
            initial_load: InitialLoadOptions {
                chunk_size: 0,
                ..InitialLoadOptions::default()
            },
        }
        .normalized();
        assert_eq!(opts.initial_load.chunk_size, 1000);
    }

    #[test]
    fn from_env_compat_reads_legacy_knobs() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "50");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC", "200");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "2");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS", "20");

        let opts = ApplyOptions::from_env_compat();
        assert_eq!(opts.initial_load.chunk_size, 50);
        assert_eq!(opts.initial_load.rows_per_sec, Some(200));
        assert_eq!(opts.initial_load.pause_after_chunks, Some(2));
        assert_eq!(opts.initial_load.store_delay_ms, Some(20));

        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
    }

    #[test]
    fn for_cli_typed_overrides_win_over_legacy_env_shim() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "999");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC", "999");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "9");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS", "999");

        let opts = ApplyOptions::for_cli(ApplyOptionsOverrides {
            chunk_size: Some(50),
            rows_per_sec: Some(200),
            pause_after_chunks: Some(2),
            store_delay_ms: Some(20),
            omit_env_shim: false,
        });

        assert_eq!(opts.initial_load.chunk_size, 50);
        assert_eq!(opts.initial_load.rows_per_sec, Some(200));
        assert_eq!(opts.initial_load.pause_after_chunks, Some(2));
        assert_eq!(opts.initial_load.store_delay_ms, Some(20));

        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
    }

    #[test]
    fn for_cli_omit_env_shim_ignores_legacy_inject_env() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "9");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS", "999");

        let opts = ApplyOptions::for_cli(ApplyOptionsOverrides {
            omit_env_shim: true,
            ..ApplyOptionsOverrides::default()
        });
        assert_eq!(opts.initial_load.pause_after_chunks, None);
        assert_eq!(opts.initial_load.store_delay_ms, None);

        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
    }

    #[test]
    fn from_operator_env_skips_lab_inject_env() {
        let _guard = ENV_COMPAT_LOCK.lock().expect("env compat lock");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "50");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC", "200");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "2");
        std::env::set_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS", "20");

        let opts = ApplyOptions::from_operator_env();
        assert_eq!(opts.initial_load.chunk_size, 50);
        assert_eq!(opts.initial_load.rows_per_sec, Some(200));
        assert_eq!(opts.initial_load.pause_after_chunks, None);
        assert_eq!(opts.initial_load.store_delay_ms, None);

        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
        std::env::remove_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
    }
}
