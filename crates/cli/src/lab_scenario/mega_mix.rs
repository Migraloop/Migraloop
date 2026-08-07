//! Mega-mix Lab Scenario harness (issue #251 / ADR-0031).
//!
//! Deployment-realistic mix covering every shipped path family, with solo
//! baselines, mix Incremental e2e QPS, Direct/Transform path aggregates, and
//! the 0.7 / 0.95 multi-Pipeline gates. Absolute 100k/50k floors are reported
//! honestly but are not Scenario accept thresholds on this ticket (later
//! throughput children raise product performance).

use std::fmt::Write as _;
use std::sync::Mutex;

/// Pending evidence from the mega-mix adapter, attached to the Scenario report
/// after component-pressure enrichment (so `gate_0_95` can become `n/a` when
/// infra-saturated).
static PENDING_EVIDENCE: Mutex<Option<MegaMixEvidence>> = Mutex::new(None);

pub(crate) fn store_pending_evidence(evidence: MegaMixEvidence) {
    *PENDING_EVIDENCE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(evidence);
}

pub(crate) fn take_pending_evidence() -> Option<MegaMixEvidence> {
    PENDING_EVIDENCE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take()
}

/// Selectable Scenario id (`lab scenario list` / `run`).
pub(crate) const MEGA_MIX_ID: &str = "mega-mix";
pub(crate) const MEGA_MIX_DEPLOYMENT: &str = "lab-mega-mix";

/// Aggregate scaling gate: `QPS_mix ≥ 0.7 × Σ QPS_solo_i` (ADR-0031).
pub(crate) const GATE_AGGREGATE_RATIO: f64 = 0.7;
/// Per-Pipeline degradation gate when components are not saturated.
pub(crate) const GATE_PER_PIPELINE_RATIO: f64 = 0.95;
/// Path-aggregate Incremental e2e QPS floors (hard floors; Lab-manual evidence).
pub(crate) const DIRECT_FLOOR_QPS: f64 = 100_000.0;
pub(crate) const TRANSFORM_FLOOR_QPS: f64 = 50_000.0;

/// Source rows injected per Pipeline during each solo / mix Incremental window.
/// Modest on purpose: this ticket ships the harness + honest gate reporting;
/// absolute floors may still fail until later throughput tickets.
pub(crate) const INCREMENTAL_BATCH_ROWS: u64 = 100;

/// ID base for solo Incremental inserts (avoids colliding with seed / correctness mutate).
pub(crate) const SOLO_ID_BASE: i64 = 10_000;
/// ID base for mix Incremental inserts.
pub(crate) const MIX_ID_BASE: i64 = 20_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathKind {
    Direct,
    Transform,
}

impl PathKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Transform => "transform",
        }
    }
}

/// One Pipeline in the mega-mix Deployment (Namespace identities + path family).
#[derive(Debug, Clone, Copy)]
pub(crate) struct MegaMixPipeline {
    pub name: &'static str,
    pub path: PathKind,
    /// Path-family label for catalog / COVERAGE honesty.
    pub family: &'static str,
    /// Primary Source table(s) driven during Incremental QPS windows.
    pub workload_tables: &'static [&'static str],
}

/// Shipped path-family representatives under one Scenario Namespace (`LAB_MM_*`).
pub(crate) fn mega_mix_pipelines() -> &'static [MegaMixPipeline] {
    &[
        MegaMixPipeline {
            name: "lab-mm-direct-a",
            path: PathKind::Direct,
            family: "direct",
            workload_tables: &["LAB_MM_DIRECT_A"],
        },
        MegaMixPipeline {
            name: "lab-mm-direct-b",
            path: PathKind::Direct,
            family: "direct",
            workload_tables: &["LAB_MM_DIRECT_B"],
        },
        MegaMixPipeline {
            name: "lab-mm-project",
            path: PathKind::Transform,
            family: "$project",
            workload_tables: &["LAB_MM_PROJECT"],
        },
        MegaMixPipeline {
            name: "lab-mm-field-ops",
            path: PathKind::Transform,
            family: "field-ops",
            workload_tables: &["LAB_MM_FIELD_OPS"],
        },
        MegaMixPipeline {
            name: "lab-mm-match",
            path: PathKind::Transform,
            family: "$match",
            workload_tables: &["LAB_MM_MATCH"],
        },
        MegaMixPipeline {
            name: "lab-mm-lookup",
            path: PathKind::Transform,
            family: "$lookup",
            workload_tables: &["LAB_MM_LOOKUP_CUSTOMERS"],
        },
        MegaMixPipeline {
            name: "lab-mm-union",
            path: PathKind::Transform,
            family: "$unionWith",
            workload_tables: &["LAB_MM_EAST"],
        },
        MegaMixPipeline {
            name: "lab-mm-unwind",
            path: PathKind::Transform,
            family: "$unwind",
            workload_tables: &["LAB_MM_UNWIND_CUSTOMERS"],
        },
        MegaMixPipeline {
            name: "lab-mm-group",
            path: PathKind::Transform,
            family: "$group",
            workload_tables: &["LAB_MM_GROUP_ORDERS"],
        },
        MegaMixPipeline {
            name: "lab-mm-distinct",
            path: PathKind::Transform,
            family: "distinct",
            workload_tables: &["LAB_MM_DIST_ORDERS"],
        },
        MegaMixPipeline {
            name: "lab-mm-addtoset",
            path: PathKind::Transform,
            family: "$addToSet",
            workload_tables: &["LAB_MM_DIST_ORDERS"],
        },
    ]
}

/// Required path-family labels the mix must cover (issue #251 acceptance).
pub(crate) fn required_path_families() -> &'static [&'static str] {
    &[
        "direct",
        "$project",
        "field-ops",
        "$match",
        "$lookup",
        "$unionWith",
        "$unwind",
        "$group",
        "distinct",
        "$addToSet",
    ]
}

#[derive(Debug, Clone)]
pub(crate) struct PipelineQpsSample {
    pub name: String,
    pub path: PathKind,
    pub qps_solo: f64,
    pub qps_mix: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct MegaMixEvidence {
    pub pipelines: Vec<PipelineQpsSample>,
    pub direct_aggregate_qps: f64,
    pub transform_aggregate_qps: f64,
    pub sum_solo_qps: f64,
    pub sum_mix_qps: f64,
    /// `QPS_mix ≥ 0.7 × Σ QPS_solo_i`
    pub gate_0_7_pass: bool,
    /// `each QPS_mix_i ≥ 0.95 × QPS_solo_i` — `None` when infra-saturated (gate N/A).
    pub gate_0_95_pass: Option<bool>,
    /// Reported honestly; not a Scenario accept threshold on #251.
    pub floor_direct_pass: bool,
    pub floor_transform_pass: bool,
}

/// Evaluate solo/mix samples into path aggregates + ADR-0031 gates.
pub(crate) fn evaluate_mega_mix_gates(
    samples: &[PipelineQpsSample],
    infra_saturated: bool,
) -> MegaMixEvidence {
    let sum_solo_qps: f64 = samples.iter().map(|p| p.qps_solo).sum();
    let sum_mix_qps: f64 = samples.iter().map(|p| p.qps_mix).sum();
    let direct_aggregate_qps: f64 = samples
        .iter()
        .filter(|p| p.path == PathKind::Direct)
        .map(|p| p.qps_mix)
        .sum();
    let transform_aggregate_qps: f64 = samples
        .iter()
        .filter(|p| p.path == PathKind::Transform)
        .map(|p| p.qps_mix)
        .sum();

    let gate_0_7_pass = sum_mix_qps >= GATE_AGGREGATE_RATIO * sum_solo_qps;
    let gate_0_95_pass = if infra_saturated {
        None
    } else {
        Some(
            samples
                .iter()
                .all(|p| p.qps_mix >= GATE_PER_PIPELINE_RATIO * p.qps_solo),
        )
    };

    MegaMixEvidence {
        pipelines: samples.to_vec(),
        direct_aggregate_qps,
        transform_aggregate_qps,
        sum_solo_qps,
        sum_mix_qps,
        gate_0_7_pass,
        gate_0_95_pass,
        floor_direct_pass: direct_aggregate_qps >= DIRECT_FLOOR_QPS,
        floor_transform_pass: transform_aggregate_qps >= TRANSFORM_FLOOR_QPS,
    }
}

/// End-to-end Managed Delivery QPS from Source rows and wall time.
pub(crate) fn e2e_qps(source_rows: u64, elapsed_secs: f64) -> f64 {
    if source_rows == 0 {
        return 0.0;
    }
    if elapsed_secs <= 0.0 {
        return source_rows as f64;
    }
    source_rows as f64 / elapsed_secs
}

/// Operator-visible mega-mix section for `lab scenario run` reports.
pub(crate) fn format_mega_mix_report_section(evidence: &MegaMixEvidence) -> String {
    let mut out = String::new();
    out.push_str("  mega_mix:\n");
    out.push_str("    protocol=solo_baseline_then_mix\n");
    out.push_str("    pipelines:\n");
    for p in &evidence.pipelines {
        let _ = writeln!(
            out,
            "      - name={} path={} qps_solo={:.2} qps_mix={:.2}",
            p.name,
            p.path.as_str(),
            p.qps_solo,
            p.qps_mix
        );
    }
    let _ = writeln!(
        out,
        "    path_aggregate_direct_qps={:.2}",
        evidence.direct_aggregate_qps
    );
    let _ = writeln!(
        out,
        "    path_aggregate_transform_qps={:.2}",
        evidence.transform_aggregate_qps
    );
    let _ = writeln!(out, "    sum_solo_qps={:.2}", evidence.sum_solo_qps);
    let _ = writeln!(out, "    sum_mix_qps={:.2}", evidence.sum_mix_qps);
    let _ = writeln!(
        out,
        "    gate_0_7={} (QPS_mix >= {:.2} * sum_solo)",
        if evidence.gate_0_7_pass {
            "pass"
        } else {
            "fail"
        },
        GATE_AGGREGATE_RATIO
    );
    match evidence.gate_0_95_pass {
        Some(true) => out.push_str("    gate_0_95=pass (each qps_mix >= 0.95 * qps_solo)\n"),
        Some(false) => out.push_str("    gate_0_95=fail (each qps_mix >= 0.95 * qps_solo)\n"),
        None => out.push_str(
            "    gate_0_95=n/a (infra-saturated — resize Fixture and re-run; not a product fail)\n",
        ),
    }
    let _ = writeln!(
        out,
        "    floor_direct_100k={} (evidence; not Scenario accept on #251)",
        if evidence.floor_direct_pass {
            "pass"
        } else {
            "fail"
        }
    );
    let _ = writeln!(
        out,
        "    floor_transform_50k={} (evidence; not Scenario accept on #251)",
        if evidence.floor_transform_pass {
            "pass"
        } else {
            "fail"
        }
    );
    out
}

/// True when the catalog covers every required path family (at least one Pipeline each).
pub(crate) fn covers_required_path_families(pipelines: &[MegaMixPipeline]) -> bool {
    let direct_count = pipelines
        .iter()
        .filter(|p| p.path == PathKind::Direct)
        .count();
    if direct_count < 2 {
        return false;
    }
    required_path_families()
        .iter()
        .all(|family| pipelines.iter().any(|p| p.family == *family))
}

/// SQL body (no sqlplus preamble) inserting `n` Incremental rows starting at `id_base`.
/// Returns `(sql, source_rows_counted_for_qps)`.
pub(crate) fn incremental_batch_sql(table: &str, id_base: i64, n: u64) -> (String, u64) {
    let mut sql = String::new();
    match table {
        "LAB_MM_DIRECT_A" | "LAB_MM_DIRECT_B" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO {table} (ID, NAME) VALUES ({id}, 'mm-{id}');"
                );
            }
            (sql, n)
        }
        "LAB_MM_PROJECT" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO {table} (ID, NAME, EMAIL) VALUES ({id}, 'mm-{id}', 'mm-{id}@example.com');"
                );
            }
            (sql, n)
        }
        "LAB_MM_FIELD_OPS" | "LAB_MM_MATCH" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES ({id}, 'mm-{id}', 'mm-{id}@example.com', 1);"
                );
            }
            (sql, n)
        }
        "LAB_MM_LOOKUP_CUSTOMERS" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_LOOKUP_CUSTOMERS (ID, NAME) VALUES ({id}, 'mm-{id}');"
                );
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_LOOKUP_ORDERS (ORDER_ID, CUSTOMER_ID, AMOUNT) VALUES ({id}, {id}, 1.00);"
                );
            }
            (sql, n)
        }
        "LAB_MM_EAST" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_EAST (ID, NAME) VALUES ({id}, 'east-mm-{id}');"
                );
            }
            (sql, n)
        }
        "LAB_MM_UNWIND_CUSTOMERS" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let order_id = id_base + 100_000 + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_UNWIND_CUSTOMERS (ID, NAME) VALUES ({id}, 'mm-{id}');"
                );
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_UNWIND_ORDERS (ORDER_ID, CUSTOMER_ID, AMOUNT) VALUES ({order_id}, {id}, 1.00);"
                );
            }
            (sql, n)
        }
        "LAB_MM_GROUP_ORDERS" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let customer = id_base + 1_000 + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_GROUP_ORDERS (ID, CUSTOMER_ID, AMOUNT, NOTE) VALUES ({id}, {customer}, 3, 'mm');"
                );
            }
            (sql, n)
        }
        "LAB_MM_DIST_ORDERS" => {
            for i in 0..n {
                let id = id_base + i as i64;
                let customer = id_base + 2_000 + i as i64;
                let _ = writeln!(
                    sql,
                    "INSERT INTO LAB_MM_DIST_ORDERS (ORDER_ID, CUSTOMER_ID, AMOUNT, ADDRESS) VALUES ({id}, {customer}, 3.00, 'mm');"
                );
            }
            (sql, n)
        }
        other => {
            let _ = writeln!(sql, "-- unsupported mega-mix workload table: {other}");
            (sql, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, path: PathKind, solo: f64, mix: f64) -> PipelineQpsSample {
        PipelineQpsSample {
            name: name.to_string(),
            path,
            qps_solo: solo,
            qps_mix: mix,
        }
    }

    #[test]
    fn catalog_covers_every_shipped_path_family_with_multiple_direct() {
        let pipes = mega_mix_pipelines();
        assert!(
            covers_required_path_families(pipes),
            "mega-mix catalog missing a required path family"
        );
        let direct = pipes.iter().filter(|p| p.path == PathKind::Direct).count();
        assert!(direct >= 2, "need multiple Direct Pipelines, got {direct}");
        for family in required_path_families() {
            if *family == "direct" {
                continue;
            }
            assert!(
                pipes.iter().any(|p| p.family == *family),
                "missing family {family}"
            );
        }
    }

    #[test]
    fn e2e_qps_from_rows_and_wall_time() {
        assert!((e2e_qps(100, 2.0) - 50.0).abs() < f64::EPSILON);
        assert!((e2e_qps(100, 0.0) - 100.0).abs() < f64::EPSILON);
        assert!((e2e_qps(0, 1.0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn gate_0_7_passes_when_mix_keeps_aggregate_scaling() {
        let samples = vec![
            sample("a", PathKind::Direct, 100.0, 80.0),
            sample("b", PathKind::Transform, 100.0, 70.0),
        ];
        // sum_mix=150, 0.7*sum_solo=140 → pass
        let ev = evaluate_mega_mix_gates(&samples, false);
        assert!(ev.gate_0_7_pass);
        assert_eq!(ev.gate_0_95_pass, Some(false)); // 70 < 0.95*100
        assert!((ev.direct_aggregate_qps - 80.0).abs() < f64::EPSILON);
        assert!((ev.transform_aggregate_qps - 70.0).abs() < f64::EPSILON);
    }

    #[test]
    fn gate_0_7_fails_when_mix_collapses() {
        let samples = vec![
            sample("a", PathKind::Direct, 100.0, 10.0),
            sample("b", PathKind::Direct, 100.0, 10.0),
        ];
        let ev = evaluate_mega_mix_gates(&samples, false);
        assert!(!ev.gate_0_7_pass);
    }

    #[test]
    fn gate_0_95_passes_when_each_pipeline_holds() {
        let samples = vec![
            sample("a", PathKind::Direct, 100.0, 96.0),
            sample("b", PathKind::Transform, 50.0, 48.0),
        ];
        let ev = evaluate_mega_mix_gates(&samples, false);
        assert_eq!(ev.gate_0_95_pass, Some(true));
        assert!(ev.gate_0_7_pass);
    }

    #[test]
    fn gate_0_95_is_na_when_infra_saturated() {
        let samples = vec![sample("a", PathKind::Direct, 100.0, 10.0)];
        let ev = evaluate_mega_mix_gates(&samples, true);
        assert_eq!(ev.gate_0_95_pass, None);
    }

    #[test]
    fn floors_reported_honestly_against_path_aggregates() {
        let samples = vec![
            sample("d1", PathKind::Direct, 1.0, 60_000.0),
            sample("d2", PathKind::Direct, 1.0, 50_000.0),
            sample("t1", PathKind::Transform, 1.0, 40_000.0),
        ];
        let ev = evaluate_mega_mix_gates(&samples, false);
        assert!(ev.floor_direct_pass); // 110k
        assert!(!ev.floor_transform_pass); // 40k
    }

    #[test]
    fn report_section_includes_per_pipeline_aggregates_and_gates() {
        let samples = vec![
            sample("lab-mm-direct-a", PathKind::Direct, 100.0, 96.0),
            sample("lab-mm-project", PathKind::Transform, 80.0, 76.0),
        ];
        let ev = evaluate_mega_mix_gates(&samples, false);
        let rendered = format_mega_mix_report_section(&ev);
        assert!(rendered.contains("mega_mix:"), "{rendered}");
        assert!(rendered.contains("protocol=solo_baseline_then_mix"), "{rendered}");
        assert!(rendered.contains("name=lab-mm-direct-a"), "{rendered}");
        assert!(rendered.contains("qps_solo=100.00"), "{rendered}");
        assert!(rendered.contains("qps_mix=96.00"), "{rendered}");
        assert!(rendered.contains("path_aggregate_direct_qps="), "{rendered}");
        assert!(rendered.contains("path_aggregate_transform_qps="), "{rendered}");
        assert!(rendered.contains("gate_0_7=pass"), "{rendered}");
        assert!(rendered.contains("gate_0_95=pass"), "{rendered}");
        assert!(rendered.contains("floor_direct_100k=fail"), "{rendered}");
        assert!(rendered.contains("floor_transform_50k=fail"), "{rendered}");
        assert!(
            rendered.contains("not Scenario accept on #251"),
            "{rendered}"
        );
    }

    #[test]
    fn report_marks_0_95_na_under_infra_saturated() {
        let samples = vec![sample("a", PathKind::Direct, 100.0, 96.0)];
        let ev = evaluate_mega_mix_gates(&samples, true);
        let rendered = format_mega_mix_report_section(&ev);
        assert!(rendered.contains("gate_0_95=n/a"), "{rendered}");
        assert!(rendered.contains("infra-saturated"), "{rendered}");
    }

    #[test]
    fn incremental_batch_sql_counts_source_rows() {
        let (sql, n) = incremental_batch_sql("LAB_MM_DIRECT_A", 10_000, 3);
        assert_eq!(n, 3);
        assert!(sql.contains("INSERT INTO LAB_MM_DIRECT_A"));
        assert!(sql.contains("10000"));
        assert!(sql.contains("10002"));
        let (sql, n) = incremental_batch_sql("LAB_MM_LOOKUP_CUSTOMERS", 10_000, 2);
        assert_eq!(n, 2);
        assert!(sql.contains("LAB_MM_LOOKUP_ORDERS"));
    }
}
