//! Cutover hand-off for Initial Load ↔ Incremental Capture (ADR-0004 / issue #175).
//!
//! Single owner for low-watermark establish fields, overlap-window checkpoint
//! (wm−1), readiness for Incremental Capture, and inclusive resume. Initial Load
//! and Incremental Sync call this module; Operator cutover status formats from
//! [`CutoverFacts`] so CLI narrative cannot invent a second cutover model.
//!
//! Policy is unchanged: prefer duplicate applies over gaps.

use migraloop_capture::CapturePosition;
use migraloop_platform_store::BaseDataset;

use crate::RuntimeError;

/// Durable cutover facts for one Base Dataset.
///
/// Operator `status` / `base` cutover lines format from these fields — not from
/// ad-hoc `match` on store columns elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CutoverFacts {
    pub low_watermark: Option<i64>,
    pub checkpoint: Option<i64>,
    /// True when Incremental Capture may start (low-watermark present).
    pub ready_for_incremental: bool,
}

/// Read cutover facts from a Base Dataset.
pub fn cutover_facts_from_base(base: &BaseDataset) -> CutoverFacts {
    CutoverFacts {
        low_watermark: base.capture_low_watermark,
        checkpoint: base.capture_checkpoint,
        ready_for_incremental: base.capture_low_watermark.is_some(),
    }
}

/// Durable hand-off fields after Source establishes a low-watermark.
///
/// Checkpoint is `low_watermark − 1` so inclusive Incremental resume still
/// covers the ADR-0004 overlap window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CutoverHandoff {
    pub low_watermark: i64,
    pub checkpoint: i64,
}

/// Establish cutover hand-off from a Source-established low-watermark.
pub fn handoff_from_low_watermark(low_watermark: CapturePosition) -> CutoverHandoff {
    let wm = low_watermark.as_i64();
    CutoverHandoff {
        low_watermark: wm,
        checkpoint: wm.saturating_sub(1),
    }
}

/// Optional hand-off when Initial Load may pause before any watermark exists.
pub fn handoff_from_optional_low_watermark(
    low_watermark: Option<CapturePosition>,
) -> Option<CutoverHandoff> {
    low_watermark.map(handoff_from_low_watermark)
}

/// Inclusive resume cursor for Incremental Capture after cutover.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncrementalResume {
    pub low_watermark: CapturePosition,
    pub resume_from: CapturePosition,
    /// Checkpoint shown before this Incremental window advances.
    pub checkpoint_before: i64,
}

/// Resolve Inclusive resume for Incremental Capture, or fail if the cutover
/// low-watermark is missing (gap-tolerant hand-off is rejected).
pub fn resume_for_incremental(
    table: &str,
    low_watermark: Option<i64>,
    checkpoint: Option<i64>,
) -> Result<IncrementalResume, RuntimeError> {
    let Some(low_watermark_i64) = low_watermark else {
        return Err(RuntimeError::Failed(format!(
            "cannot start Incremental Capture for {table} without low-watermark overlap \
             (cutover watermark missing; re-run Initial Load via `migraloop apply`)"
        )));
    };
    let low_watermark = CapturePosition::from_i64(low_watermark_i64).ok_or_else(|| {
        RuntimeError::Failed(format!(
            "invalid low-watermark for Base Dataset {table}: {low_watermark_i64}"
        ))
    })?;

    let resume_from = match checkpoint {
        Some(cp) => CapturePosition::from_i64(cp).ok_or_else(|| {
            RuntimeError::Failed(format!(
                "invalid capture checkpoint for Base Dataset {table}: {cp}"
            ))
        })?,
        None => low_watermark,
    };

    let checkpoint_before = checkpoint.unwrap_or(low_watermark_i64.saturating_sub(1));

    Ok(IncrementalResume {
        low_watermark,
        resume_from,
        checkpoint_before,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use migraloop_platform_store::{BaseColumn, BaseDataset};

    fn base_with(wm: Option<i64>, cp: Option<i64>) -> BaseDataset {
        BaseDataset {
            deployment_name: "d".into(),
            source_table: "CUSTOMERS".into(),
            source_schema: "APP".into(),
            status: "initial_load_complete".into(),
            primary_key: vec!["ID".into()],
            columns: vec![BaseColumn {
                name: "ID".into(),
                oracle_type: "NUMBER".into(),
                precision: Some(10),
                scale: Some(0),
            }],
            omitted_columns: vec![],
            row_count: 1,
            sync_applied_changes: 0,
            sync_health: "unknown".into(),
            capture_low_watermark: wm,
            capture_checkpoint: cp,
            sync_lag: 0,
            source_alignment: "unknown".into(),
            source_alignment_checked_rows: 0,
            source_alignment_mismatched_rows: 0,
            initial_load_cursor: None,
        }
    }

    #[test]
    fn handoff_sets_checkpoint_to_watermark_minus_one() {
        // Known fixture: CUSTOMERS_LOW_WATERMARK = 1000 → checkpoint 999.
        let handoff = handoff_from_low_watermark(CapturePosition(1000));
        assert_eq!(handoff.low_watermark, 1000);
        assert_eq!(handoff.checkpoint, 999);
    }

    #[test]
    fn facts_ready_only_when_low_watermark_present() {
        let ready = cutover_facts_from_base(&base_with(Some(1000), Some(999)));
        assert_eq!(
            ready,
            CutoverFacts {
                low_watermark: Some(1000),
                checkpoint: Some(999),
                ready_for_incremental: true,
            }
        );

        let missing = cutover_facts_from_base(&base_with(None, None));
        assert!(!missing.ready_for_incremental);
        assert_eq!(missing.low_watermark, None);
    }

    #[test]
    fn resume_rejects_missing_low_watermark() {
        let err = resume_for_incremental("CUSTOMERS", None, None).unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("watermark") || msg.contains("overlap") || msg.contains("cutover"),
            "rejection must mention watermark/overlap/cutover, got: {msg}"
        );
    }

    #[test]
    fn resume_from_cutover_checkpoint_covers_overlap() {
        let resume = resume_for_incremental("CUSTOMERS", Some(1000), Some(999))
            .expect("cutover hand-off must be ready");
        assert_eq!(resume.low_watermark, CapturePosition(1000));
        assert_eq!(resume.resume_from, CapturePosition(999));
        assert_eq!(resume.checkpoint_before, 999);
    }

    #[test]
    fn resume_without_checkpoint_falls_back_to_low_watermark() {
        let resume = resume_for_incremental("CUSTOMERS", Some(1000), None)
            .expect("watermark alone is enough to start Incremental");
        assert_eq!(resume.resume_from, CapturePosition(1000));
        assert_eq!(resume.checkpoint_before, 999);
    }
}
