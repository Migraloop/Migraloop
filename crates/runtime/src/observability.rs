//! Structured operator events for Deployment runtime (ADR-0008).
//!
//! Human-readable companion lines stay next to the verb; this is the
//! machine-parseable Observability Surface for key apply / Initial Load /
//! Delivery events.

use std::collections::BTreeMap;

/// Emit one structured JSON operator event line (stdout).
pub fn emit_event(event: &str, fields: &[(&str, EventValue)]) {
    let mut map = BTreeMap::new();
    map.insert("event".to_string(), EventValue::Str(event.to_string()));
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    match serde_json::to_string(&map) {
        Ok(json) => println!("{json}"),
        Err(err) => eprintln!("structured log encode failed for event={event}: {err}"),
    }
}

#[derive(Clone, Debug)]
pub enum EventValue {
    Str(String),
    Int(i64),
    Bool(bool),
}

impl serde::Serialize for EventValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EventValue::Str(s) => serializer.serialize_str(s),
            EventValue::Int(n) => serializer.serialize_i64(*n),
            EventValue::Bool(b) => serializer.serialize_bool(*b),
        }
    }
}

impl From<&str> for EventValue {
    fn from(value: &str) -> Self {
        EventValue::Str(value.to_string())
    }
}

impl From<String> for EventValue {
    fn from(value: String) -> Self {
        EventValue::Str(value)
    }
}

impl From<i64> for EventValue {
    fn from(value: i64) -> Self {
        EventValue::Int(value)
    }
}

impl From<i32> for EventValue {
    fn from(value: i32) -> Self {
        EventValue::Int(i64::from(value))
    }
}

impl From<usize> for EventValue {
    fn from(value: usize) -> Self {
        EventValue::Int(value as i64)
    }
}

impl From<u64> for EventValue {
    fn from(value: u64) -> Self {
        EventValue::Int(value as i64)
    }
}

impl From<bool> for EventValue {
    fn from(value: bool) -> Self {
        EventValue::Bool(value)
    }
}
