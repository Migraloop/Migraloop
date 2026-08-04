//! Target System engine interface (issue #156 / ADR-0002).
//!
//! Deployment runtime Delivery depends on [`TargetEngine`], not Mongo concrete
//! types. v1 ships [`MongoTargetConnection`] as the production adapter and
//! [`FakeTarget`] for in-process seam tests — no second production Target engine.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::Value;

use crate::{
    delete_documents_by_identity, list_target_documents, upsert_managed_documents,
    DeliveryDocument, DeliveryError, MongoTargetConnection,
};

/// Target System Delivery surface used by Sync / Delivery orchestration.
///
/// Document vs relational Managed-field ownership is an adapter concern
/// (ADR-0002): document adapters `$set` only Managed keys; relational adapters
/// (future) would also maintain Managed schema.
pub trait TargetEngine: Send + Sync {
    /// Upsert Managed fields for each Output Identity.
    fn upsert_managed(
        &self,
        collection: &str,
        documents: &[DeliveryDocument],
    ) -> impl std::future::Future<Output = Result<usize, DeliveryError>> + Send;

    /// Delete entire Target documents/rows for the given Output Identities.
    fn delete_by_identity(
        &self,
        collection: &str,
        identities: &[Value],
    ) -> impl std::future::Future<Output = Result<usize, DeliveryError>> + Send;

    /// List/read helper for Drift Check / status / inspect.
    fn list_documents(
        &self,
        collection: &str,
    ) -> impl std::future::Future<Output = Result<Vec<Value>, DeliveryError>> + Send;

    /// Operator-visible Target engine kind label.
    fn kind_label(&self) -> &'static str;
}

impl TargetEngine for MongoTargetConnection {
    async fn upsert_managed(
        &self,
        collection: &str,
        documents: &[DeliveryDocument],
    ) -> Result<usize, DeliveryError> {
        upsert_managed_documents(self, collection, documents).await
    }

    async fn delete_by_identity(
        &self,
        collection: &str,
        identities: &[Value],
    ) -> Result<usize, DeliveryError> {
        delete_documents_by_identity(self, collection, identities).await
    }

    async fn list_documents(&self, collection: &str) -> Result<Vec<Value>, DeliveryError> {
        list_target_documents(self, collection).await
    }

    fn kind_label(&self) -> &'static str {
        "mongodb"
    }
}

/// In-memory Target adapter for engine-seam tests (not a production engine).
///
/// Honors document ownership: upsert merges Managed fields only and leaves any
/// other keys already present on the document untouched (ADR-0002).
#[derive(Debug, Default)]
pub struct FakeTarget {
    /// collection → (identity JSON → document JSON object)
    inner: Mutex<BTreeMap<String, BTreeMap<String, serde_json::Map<String, Value>>>>,
}

impl FakeTarget {
    pub fn new() -> Self {
        Self::default()
    }

    fn identity_key(identity: &Value) -> String {
        // Shared Output Identity key (issue #170) — same encoding as poison / Drift.
        migraloop_types::output_identity_key(identity)
    }
}

impl TargetEngine for FakeTarget {
    async fn upsert_managed(
        &self,
        collection: &str,
        documents: &[DeliveryDocument],
    ) -> Result<usize, DeliveryError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| DeliveryError::Apply("FakeTarget lock poisoned".to_string()))?;
        let coll = guard.entry(collection.to_string()).or_default();
        let mut delivered = 0usize;
        for doc_row in documents {
            let key = Self::identity_key(&doc_row.identity);
            let entry = coll.entry(key).or_insert_with(serde_json::Map::new);
            // Document semantics: never clear non-Managed keys; only write Managed.
            for (field, value) in &doc_row.managed_fields {
                if matches!(
                    doc_row.field_as.get(field).copied(),
                    Some(crate::ManagedFieldAs::Omit)
                ) {
                    continue;
                }
                entry.insert(field.clone(), value.clone());
            }
            entry.insert("_id".to_string(), doc_row.identity.clone());
            delivered += 1;
        }
        Ok(delivered)
    }

    async fn delete_by_identity(
        &self,
        collection: &str,
        identities: &[Value],
    ) -> Result<usize, DeliveryError> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| DeliveryError::Apply("FakeTarget lock poisoned".to_string()))?;
        let Some(coll) = guard.get_mut(collection) else {
            return Ok(0);
        };
        let mut deleted = 0usize;
        for identity in identities {
            let key = Self::identity_key(identity);
            if coll.remove(&key).is_some() {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    async fn list_documents(&self, collection: &str) -> Result<Vec<Value>, DeliveryError> {
        let guard = self
            .inner
            .lock()
            .map_err(|_| DeliveryError::Apply("FakeTarget lock poisoned".to_string()))?;
        let Some(coll) = guard.get(collection) else {
            return Ok(Vec::new());
        };
        Ok(coll.values().map(|m| Value::Object(m.clone())).collect())
    }

    fn kind_label(&self) -> &'static str {
        "fake"
    }
}

/// Shared Delivery round-trip used by engine-seam tests (issue #156).
///
/// Upserts Managed documents, lists them back, optionally deletes by identity.
/// Orchestration is written only against [`TargetEngine`] so Mongo and Fake swap
/// without rewriting this path.
pub async fn target_engine_delivery_roundtrip<T: TargetEngine>(
    target: &T,
    collection: &str,
    documents: &[DeliveryDocument],
    delete_identities: &[Value],
) -> Result<Vec<Value>, DeliveryError> {
    target.upsert_managed(collection, documents).await?;
    let listed = target.list_documents(collection).await?;
    if !delete_identities.is_empty() {
        target
            .delete_by_identity(collection, delete_identities)
            .await?;
    }
    Ok(listed)
}

/// Build a minimal Delivery document for Fake/Mongo seam tests.
pub fn test_delivery_document(identity: Value, managed: serde_json::Map<String, Value>) -> DeliveryDocument {
    DeliveryDocument {
        identity,
        managed_fields: managed,
        columns: Vec::new(),
        field_as: BTreeMap::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn fake_target_upserts_managed_leaves_other_fields() {
        let target = FakeTarget::new();
        let id = json!(1);
        // Seed a non-Managed field by writing then upserting Managed-only.
        {
            let mut guard = target.inner.lock().unwrap();
            let coll = guard.entry("customers".into()).or_default();
            let mut doc = serde_json::Map::new();
            doc.insert("_id".into(), id.clone());
            doc.insert("EXTRA".into(), json!("keep-me"));
            doc.insert("NAME".into(), json!("old"));
            coll.insert(serde_json::to_string(&id).unwrap(), doc);
        }

        let mut managed = serde_json::Map::new();
        managed.insert("NAME".into(), json!("Ada"));
        let doc = test_delivery_document(id.clone(), managed);
        target
            .upsert_managed("customers", std::slice::from_ref(&doc))
            .await
            .unwrap();

        let listed = target.list_documents("customers").await.unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["NAME"], json!("Ada"));
        assert_eq!(listed[0]["EXTRA"], json!("keep-me"));
        assert_eq!(target.kind_label(), "fake");
    }

    #[tokio::test]
    async fn fake_target_deletes_by_output_identity() {
        let target = FakeTarget::new();
        let mut managed = serde_json::Map::new();
        managed.insert("NAME".into(), json!("Ada"));
        let doc = test_delivery_document(json!(7), managed);
        target
            .upsert_managed("customers", std::slice::from_ref(&doc))
            .await
            .unwrap();
        let deleted = target
            .delete_by_identity("customers", &[json!(7)])
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(target.list_documents("customers").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn target_engine_roundtrip_is_adapter_agnostic() {
        let target = FakeTarget::new();
        let mut managed = serde_json::Map::new();
        managed.insert("NAME".into(), json!("Bob"));
        let doc = test_delivery_document(json!(2), managed);
        let listed = target_engine_delivery_roundtrip(
            &target,
            "customers",
            std::slice::from_ref(&doc),
            &[],
        )
        .await
        .unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["NAME"], json!("Bob"));
    }
}
