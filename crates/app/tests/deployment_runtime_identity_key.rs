//! Behaviour seam: one Output Identity key across poison, Drift, and Delivery (#170).
//!
//! Agreed seams (#168 Testing Decisions / #170 AC):
//! - Shared types helper is the source of truth for poison matching, Drift
//!   reconcile, and Delivery delete/upsert identity handling.
//! - Same logical identity value produces one key across those paths.
//! - Operator-visible Output Identity semantics stay unchanged (status labels
//!   remain the existing display formatter; numeric poison env twins stay valid).

use std::collections::BTreeSet;

use migraloop_delivery::{test_delivery_document, FakeTarget, TargetEngine};
use migraloop_runtime::{
    format_output_identity, identity_key, output_identity_matches_poison_keys,
};
use migraloop_types::output_identity_key;
use serde_json::json;

#[tokio::test]
async fn same_output_identity_produces_one_key_across_poison_drift_and_delivery() {
    let cases = [
        (json!(1), "1"),
        (json!("CUST-1"), "\"CUST-1\""),
        (
            json!({"ID": 1, "REGION": "APAC"}),
            "{\"ID\":1,\"REGION\":\"APAC\"}",
        ),
    ];

    for (identity, expected_key) in cases {
        // Shared helper — source of truth (independent expected literals).
        assert_eq!(output_identity_key(&identity), expected_key);

        // Drift Check / Delivery reconcile path.
        assert_eq!(identity_key(&identity), expected_key);

        // Poison injection matching path (same encoding as Drift/Delivery).
        let poison_keys: BTreeSet<String> = [expected_key.to_string()].into();
        assert!(
            output_identity_matches_poison_keys(&identity, &poison_keys),
            "poison injection matching must use the shared Output Identity key for {identity}"
        );
        // Discriminator vs Operator display label for string identities.
        if identity.as_str().is_some() {
            assert_ne!(
                format_output_identity(&identity),
                expected_key,
                "Operator display label must not be used as the match key"
            );
        }

        // Delivery delete/upsert identity handling (FakeTarget map keys).
        let target = FakeTarget::new();
        let mut managed = serde_json::Map::new();
        managed.insert("NAME".into(), json!("Ada"));
        let doc = test_delivery_document(identity.clone(), managed);
        target
            .upsert_managed("customers", std::slice::from_ref(&doc))
            .await
            .expect("upsert by Output Identity");
        let deleted = target
            .delete_by_identity("customers", std::slice::from_ref(&identity))
            .await
            .expect("delete by Output Identity");
        assert_eq!(
            deleted, 1,
            "Delivery must address the same Output Identity key for {identity}"
        );
        assert!(target
            .list_documents("customers")
            .await
            .expect("list")
            .is_empty());
    }
}
