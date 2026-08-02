//! Schema-driven Oracle type allow-list, NUMBER precision, and temporal rules.
//!
//! ADR-0018 / ADR-0022 / ADR-0023.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use thiserror::Error;

/// Maximum RAW byte length accepted in v1 (cap from ADR-0018 intent).
pub const RAW_SIZE_CAP_BYTES: i32 = 2000;

/// Decimal128 significant digits (IEEE 754 decimal128).
pub const DECIMAL128_MAX_PRECISION: i32 = 34;

/// Signed Int64 / NumberLong digit budget that always fits.
pub const INT64_SAFE_PRECISION: i32 = 18;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("unsupported Oracle type {oracle_type} cannot be used as a Managed/transform input")]
    UnsupportedAsManaged { oracle_type: String },
    #[error(
        "NUMBER column {column} has unsafe declared precision/scale \
         (precision={precision:?}, scale={scale:?}); resolve at Pipeline apply time \
         with fields.{column}.as: string or fields.{column}.as: omit"
    )]
    UnsafeNumber {
        column: String,
        precision: Option<i32>,
        scale: Option<i32>,
    },
    #[error(
        "naive DATE/TIMESTAMP requires Source DB timezone or source.timezone; \
         neither is available"
    )]
    MissingTimezone,
    #[error("invalid timezone {0:?}")]
    InvalidTimezone(String),
    #[error("invalid temporal value {0:?}: {1}")]
    InvalidTemporal(String, String),
}

/// How a declared Oracle NUMBER maps into Mongo numeric types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberMongoMapping {
    /// scale 0 (or absent scale treated as integer) and precision ≤ 18 → NumberLong
    Long,
    /// precision ≤ 34 and fits Decimal128 → Decimal128 (never IEEE double)
    Decimal128,
    /// declared precision/scale cannot fit safe Mongo numeric types
    Unsafe,
}

/// Normalize an Oracle type name for allow-list checks (strip length/precision args).
pub fn normalize_oracle_type(oracle_type: &str) -> String {
    let trimmed = oracle_type.trim();
    let base = trimmed
        .split_once('(')
        .map(|(head, _)| head)
        .unwrap_or(trimmed)
        .trim();
    base.to_ascii_uppercase()
}

/// v1 allow-list (ADR-0018). RAW is allow-listed only when size ≤ cap.
pub fn is_allow_listed_oracle_type(oracle_type: &str, size: Option<i32>) -> bool {
    match normalize_oracle_type(oracle_type).as_str() {
        "NUMBER" | "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" | "CHAR" | "NCHAR" | "VARCHAR2"
        | "NVARCHAR2" | "DATE" | "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE"
        | "TIMESTAMP WITH LOCAL TIME ZONE" => true,
        "RAW" => match size {
            Some(n) if n >= 0 && n <= RAW_SIZE_CAP_BYTES => true,
            _ => false,
        },
        _ => false,
    }
}

/// Classify NUMBER(p,s) for precision-preserving Mongo mapping (ADR-0023).
///
/// Unconstrained / missing precision is unsafe — never default to IEEE double.
pub fn classify_number(precision: Option<i32>, scale: Option<i32>) -> NumberMongoMapping {
    let Some(precision) = precision else {
        return NumberMongoMapping::Unsafe;
    };
    if precision <= 0 || precision > 38 {
        return NumberMongoMapping::Unsafe;
    }
    let scale = scale.unwrap_or(0);
    if scale < 0 || scale > precision {
        return NumberMongoMapping::Unsafe;
    }
    if scale == 0 && precision <= INT64_SAFE_PRECISION {
        return NumberMongoMapping::Long;
    }
    if precision <= DECIMAL128_MAX_PRECISION {
        return NumberMongoMapping::Decimal128;
    }
    NumberMongoMapping::Unsafe
}

/// Resolve the timezone used to interpret naive DATE/TIMESTAMP (ADR-0022).
/// Prefers readable Source DB timezone; else user-configured Source/Deployment zone.
pub fn resolve_temporal_timezone(
    db_timezone: Option<&str>,
    configured_timezone: Option<&str>,
) -> Result<Tz, TypeError> {
    if let Some(db) = db_timezone.map(str::trim).filter(|s| !s.is_empty()) {
        return db
            .parse::<Tz>()
            .map_err(|_| TypeError::InvalidTimezone(db.to_string()));
    }
    if let Some(cfg) = configured_timezone.map(str::trim).filter(|s| !s.is_empty()) {
        return cfg
            .parse::<Tz>()
            .map_err(|_| TypeError::InvalidTimezone(cfg.to_string()));
    }
    Err(TypeError::MissingTimezone)
}

/// Interpret a naive Oracle DATE/TIMESTAMP wall-clock value in `tz`, return UTC.
///
/// Accepts `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`, or `YYYY-MM-DD HH:MM:SS`.
pub fn naive_temporal_to_utc(value: &str, tz: Tz) -> Result<DateTime<Utc>, TypeError> {
    let trimmed = value.trim();
    let naive = if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S") {
        dt
    } else if let Ok(dt) = NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S") {
        dt
    } else if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        date.and_hms_opt(0, 0, 0)
            .ok_or_else(|| TypeError::InvalidTemporal(trimmed.to_string(), "bad midnight".into()))?
    } else {
        return Err(TypeError::InvalidTemporal(
            trimmed.to_string(),
            "expected YYYY-MM-DD[THH:MM:SS]".into(),
        ));
    };

    tz.from_local_datetime(&naive)
        .single()
        .map(|dt| dt.with_timezone(&Utc))
        .ok_or_else(|| {
            TypeError::InvalidTemporal(
                trimmed.to_string(),
                format!("ambiguous or invalid in timezone {tz}"),
            )
        })
}

/// Parse timezone-aware Oracle TIMESTAMP WITH TIME ZONE into UTC.
/// Accepts RFC3339 / ISO-8601 with offset (e.g. `2024-01-15T10:30:00+09:00`).
pub fn aware_temporal_to_utc(value: &str) -> Result<DateTime<Utc>, TypeError> {
    DateTime::parse_from_rfc3339(value.trim())
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| TypeError::InvalidTemporal(value.to_string(), err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allow_list_accepts_scalars_rejects_blob() {
        assert!(is_allow_listed_oracle_type("NUMBER(10,0)", None));
        assert!(is_allow_listed_oracle_type("VARCHAR2(100)", None));
        assert!(is_allow_listed_oracle_type("DATE", None));
        assert!(is_allow_listed_oracle_type("RAW(16)", Some(16)));
        assert!(!is_allow_listed_oracle_type("BLOB", None));
        assert!(!is_allow_listed_oracle_type("CLOB", None));
        assert!(!is_allow_listed_oracle_type("RAW(4000)", Some(4000)));
    }

    #[test]
    fn number_mapping_never_defaults_to_double() {
        assert_eq!(
            classify_number(Some(10), Some(0)),
            NumberMongoMapping::Long
        );
        assert_eq!(
            classify_number(Some(12), Some(2)),
            NumberMongoMapping::Decimal128
        );
        assert_eq!(classify_number(None, None), NumberMongoMapping::Unsafe);
        assert_eq!(
            classify_number(Some(38), Some(10)),
            NumberMongoMapping::Unsafe
        );
    }

    #[test]
    fn naive_date_uses_configured_zone_then_utc() {
        let tz = resolve_temporal_timezone(None, Some("America/New_York")).unwrap();
        let utc = naive_temporal_to_utc("2024-01-15T10:30:00", tz).unwrap();
        assert_eq!(utc.to_rfc3339(), "2024-01-15T15:30:00+00:00");
    }

    #[test]
    fn db_timezone_preferred_over_configured() {
        let tz = resolve_temporal_timezone(Some("Asia/Tokyo"), Some("America/New_York")).unwrap();
        let utc = naive_temporal_to_utc("2024-01-15T10:30:00", tz).unwrap();
        assert_eq!(utc.to_rfc3339(), "2024-01-15T01:30:00+00:00");
    }
}
