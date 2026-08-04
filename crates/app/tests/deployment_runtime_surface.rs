//! Deployment runtime public surface seam (issue #172).
//!
//! Agreed seams (#168 Testing Decisions / #172 AC):
//! 1. Operator CLI / RQG contract-path twins remain the Release Quality Gate.
//! 2. Deployment runtime public interface — Operator Deployment verbs plus
//!    necessary session / factory entry points only.
//!
//! This file proves continuous Sync prefers an open Platform Store session,
//! and exercises the narrowed verb surface without reaching demoted helpers.

mod common;

use std::time::Duration;

use migraloop_platform_store::PlatformStore;
use migraloop_runtime::{
    run_continuous_incremental_sync, status_inventory, SyncCycleOutcome, SyncInvocation,
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

/// Continuous Incremental Sync entry takes an open Platform Store session (#172).
#[tokio::test]
async fn continuous_incremental_sync_prefers_open_store_session() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    // Empty store: ContinuousCycle idles — prove the session-shaped entry runs.
    let outcome = migraloop_runtime::run_incremental_sync(&store, SyncInvocation::ContinuousCycle)
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
        run_continuous_incremental_sync(&store),
    )
    .await;
    assert!(
        ran.is_err(),
        "continuous Sync should keep running on the open session until cancelled"
    );
}
