//! Deployment runtime public surface seam (issue #172 / #208).
//!
//! Agreed seams (#168 / #199 Testing Decisions / #172 / #208 AC):
//! 1. Operator CLI / RQG contract-path twins remain the Release Quality Gate.
//! 2. Deployment runtime public interface — Operator Deployment verbs plus
//!    necessary session / factory entry points only (no parallel orchestration
//!    seam after apply / Incremental locality split).
//!
//! This file proves continuous Sync prefers an open Platform Store session,
//! one-shot Sync uses `run_incremental_sync` + `SyncInvocation::OneShot` (collapsed
//! Sync aliases), and the narrowed verb surface without reaching demoted helpers.

mod common;

use std::time::Duration;

use migraloop_platform_store::PlatformStore;
use migraloop_runtime::{
    run_continuous_incremental_sync, status_inventory, SyncCycleOutcome, SyncInvocation,
    SyncOptions,
};

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_surface_{suffix}");
    let admin = admin_url();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin)
        .await
        .expect("connect to admin database for test setup");

    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&pool)
        .await
        .expect("create ephemeral Platform Store database");

    let base = admin
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .expect("admin url must include a database path");
    format!("{base}/{db_name}")
}

/// Continuous Incremental Sync prefers an open Platform Store session (#172);
/// one-shot Sync uses the same verb + [`SyncInvocation::OneShot`] (#208).
#[tokio::test]
async fn incremental_sync_entries_use_open_session_and_oneshot_invocation() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    // OneShot on empty store errors (no Deployments) — proves collapsed alias entry.
    let one_shot = migraloop_runtime::run_incremental_sync(
        &store,
        SyncInvocation::OneShot,
        SyncOptions::default(),
    )
    .await;
    assert!(
        one_shot.is_err(),
        "OneShot Incremental Sync should fail when no Deployments are applied"
    );

    // Empty store: ContinuousCycle idles — prove the session-shaped entry runs.
    let outcome = migraloop_runtime::run_incremental_sync(
        &store,
        SyncInvocation::ContinuousCycle,
        SyncOptions::default(),
    )
    .await
    .expect("one ContinuousCycle on open session");
    assert_eq!(outcome, SyncCycleOutcome::Idle);

    let inventory = status_inventory(&store)
        .await
        .expect("status inventory on same session");
    assert!(inventory.deployments.is_empty());

    // Continuous loop uses the same open session (no URL reopen required).
    let ran = tokio::time::timeout(
        Duration::from_millis(250),
        run_continuous_incremental_sync(&store, SyncOptions::default()),
    )
    .await;
    assert!(
        ran.is_err(),
        "continuous Sync should keep running on the open session until cancelled"
    );
}
