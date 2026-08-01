//! Delivery of Managed fields to a Target System by Output Identity.
//!
//! v1: MongoDB document Delivery. Updates `$set` only Managed fields so
//! non-Managed keys are left alone (ADR-0002). Inserts are identity + Managed.

use std::time::Duration;

use bson::{doc, Bson, Document};
use mongodb::options::{ClientOptions, UpdateOptions};
use mongodb::{Client, Collection};
use serde_json::Value;
use thiserror::Error;

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "delivery";

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error("failed to connect to Target System: {0}")]
    Connect(String),
    #[error("failed to deliver to Target System: {0}")]
    Apply(String),
    #[error("invalid Output Identity: {0}")]
    Identity(String),
}

/// Non-secret Target System connection used for Mongo Delivery.
#[derive(Debug, Clone)]
pub struct MongoTargetConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

/// One Output Identity plus the Managed fields Delivery will write.
#[derive(Debug, Clone)]
pub struct DeliveryDocument {
    /// Value stored as Mongo `_id` (Output Identity).
    pub identity: Value,
    /// Managed fields only (may include identity source columns).
    pub managed_fields: serde_json::Map<String, Value>,
}

fn json_to_bson(value: &Value) -> Result<Bson, DeliveryError> {
    bson::to_bson(value).map_err(|err| DeliveryError::Identity(err.to_string()))
}

fn build_uri(target: &MongoTargetConnection) -> String {
    format!(
        "mongodb://{}:{}@{}:{}/{}?authSource=admin",
        urlencoding::encode(&target.username),
        urlencoding::encode(&target.password),
        target.host,
        target.port,
        urlencoding::encode(&target.database),
    )
}

async fn collection(
    target: &MongoTargetConnection,
    collection_name: &str,
) -> Result<Collection<Document>, DeliveryError> {
    let uri = build_uri(target);
    let mut options = ClientOptions::parse(&uri)
        .await
        .map_err(|err| DeliveryError::Connect(err.to_string()))?;
    options.server_selection_timeout = Some(Duration::from_secs(5));
    options.connect_timeout = Some(Duration::from_secs(5));
    let client =
        Client::with_options(options).map_err(|err| DeliveryError::Connect(err.to_string()))?;
    Ok(client
        .database(&target.database)
        .collection::<Document>(collection_name))
}

/// Upsert Managed fields for each Output Identity.
///
/// Uses `$set` of Managed keys only so non-Managed Target fields are not cleared.
/// On insert, the document is identity (`_id`) + Managed fields only.
pub async fn upsert_managed_documents(
    target: &MongoTargetConnection,
    collection_name: &str,
    documents: &[DeliveryDocument],
) -> Result<usize, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut delivered = 0usize;

    for doc_row in documents {
        let identity = json_to_bson(&doc_row.identity)?;
        let mut set_doc = Document::new();
        for (key, value) in &doc_row.managed_fields {
            set_doc.insert(key, json_to_bson(value)?);
        }

        let filter = doc! { "_id": identity };
        let update = doc! { "$set": set_doc };
        let options = UpdateOptions::builder().upsert(true).build();

        coll.update_one(filter, update)
            .with_options(options)
            .await
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        delivered += 1;
    }

    Ok(delivered)
}

/// Delete entire Target documents for the given Output Identities.
///
/// When an Output Identity disappears from the platform dataset, Delivery removes
/// the whole Mongo document (ADR-0002). Missing identities are a no-op.
pub async fn delete_documents_by_identity(
    target: &MongoTargetConnection,
    collection_name: &str,
    identities: &[Value],
) -> Result<usize, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut deleted = 0usize;

    for identity_value in identities {
        let identity = json_to_bson(identity_value)?;
        let filter = doc! { "_id": identity };
        let result = coll
            .delete_one(filter)
            .await
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        deleted += result.deleted_count as usize;
    }

    Ok(deleted)
}

/// Read Target documents for operator-facing inspection (CLI seam).
pub async fn list_target_documents(
    target: &MongoTargetConnection,
    collection_name: &str,
) -> Result<Vec<Value>, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut cursor = coll
        .find(Document::new())
        .await
        .map_err(|err| DeliveryError::Apply(err.to_string()))?;

    let mut out = Vec::new();
    while cursor
        .advance()
        .await
        .map_err(|err| DeliveryError::Apply(err.to_string()))?
    {
        let mongo_doc = cursor
            .deserialize_current()
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        let value = bson::from_bson::<Value>(Bson::Document(mongo_doc))
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        out.push(value);
    }
    Ok(out)
}
