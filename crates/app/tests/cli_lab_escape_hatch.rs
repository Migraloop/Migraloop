//! Operator-visible seam: Lab DB-level restore/load escape hatch (issue #87 / PRD #55 US24).
//!
//! Agreed seam: documented operator commands against the disposable Lab Fixture
//! (compose-exec sqlplus/mongosh + ordinary product `apply` / `status` / inspect),
//! not Lab Scenario recipes and not the Release Quality Gate.
//!
//! Always-on tests cover the escape-hatch package shape and Lab-only Deployment
//! bindings. Full Fixture load + product-path continue is ignored by default
//! (Docker Compose + Instant Client).

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

fn lab_dir() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest).join("../../lab")
}

fn escape_hatch_dir() -> PathBuf {
    lab_dir().join("escape-hatch")
}

/// Documented compose-exec with stdin payload (sqlplus / mongosh escape-hatch load).
fn compose_exec_stdin(lab: &PathBuf, service_args: &[&str], stdin: &[u8]) -> (bool, String) {
    let mut cmd = Command::new("docker");
    cmd.args([
        "compose",
        "-f",
        lab.join("compose.yaml").to_str().unwrap(),
        "-p",
        "migraloop-lab",
        "exec",
        "-T",
    ]);
    for arg in service_args {
        cmd.arg(arg);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn compose exec");
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(stdin)
            .expect("write stdin");
    }
    let out = child.wait_with_output().expect("compose exec wait");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn escape_hatch_package_is_not_a_scenario_recipe() {
    let dir = escape_hatch_dir();
    assert!(
        dir.is_dir(),
        "expected lab/escape-hatch/ directory for DB-level load samples"
    );
    for name in [
        "oracle-load.sql",
        "mongo-load.js",
        "deployment.yaml",
        "README.md",
    ] {
        let path = dir.join(name);
        assert!(
            path.is_file(),
            "escape-hatch package missing {}: {}",
            name,
            path.display()
        );
    }
    // Must not live under scenarios/ — this is not a second Scenario authoring model.
    assert!(
        !dir.join("recipe.yaml").is_file(),
        "escape-hatch must not ship recipe.yaml (not a Lab Scenario)"
    );
    let scenarios_escape = lab_dir().join("scenarios/escape-hatch");
    assert!(
        !scenarios_escape.exists(),
        "escape-hatch must not be registered under lab/scenarios/"
    );
}

#[test]
fn escape_hatch_oracle_sql_loads_namespaced_table_with_supplemental_logging() {
    let sql = fs::read_to_string(escape_hatch_dir().join("oracle-load.sql"))
        .expect("read oracle-load.sql");
    assert!(
        sql.contains("LAB_ESCAPE_CUSTOMERS"),
        "oracle-load.sql must create/load LAB_ESCAPE_CUSTOMERS"
    );
    assert!(
        sql.to_ascii_uppercase()
            .contains("SUPPLEMENTAL LOG DATA (ALL) COLUMNS"),
        "oracle-load.sql must enable table-level supplemental logging for LogMiner sync"
    );
    assert!(
        sql.to_ascii_uppercase().contains("INSERT"),
        "oracle-load.sql must insert sample rows"
    );
}

#[test]
fn escape_hatch_mongo_js_loads_into_lab_database() {
    let js = fs::read_to_string(escape_hatch_dir().join("mongo-load.js"))
        .expect("read mongo-load.js");
    assert!(
        js.contains("lab_escape_manual"),
        "mongo-load.js must target lab_escape_manual collection"
    );
    assert!(
        js.contains("insertMany") || js.contains("insertOne"),
        "mongo-load.js must insert sample documents"
    );
}

#[test]
fn escape_hatch_deployment_binds_lab_fixture_engines_only() {
    let yaml = fs::read_to_string(escape_hatch_dir().join("deployment.yaml"))
        .expect("read deployment.yaml");
    assert!(
        yaml.contains("host: 127.0.0.1"),
        "escape-hatch Deployment must bind Lab loopback hosts"
    );
    assert_eq!(
        yaml.matches("host: 127.0.0.1").count(),
        2,
        "both Source and Target hosts must be Lab loopback 127.0.0.1"
    );
    assert!(
        !yaml.contains("db.example.com") && !yaml.contains("localhost.internal"),
        "escape-hatch Deployment must not point at non-Lab example hosts"
    );
    assert!(
        yaml.contains("table: LAB_ESCAPE_CUSTOMERS"),
        "Deployment Source table must match oracle-load.sql"
    );
    assert!(
        yaml.contains("collection: lab_escape_customers"),
        "Deployment Target collection must be declared for Delivery/inspect"
    );
    assert!(
        yaml.contains("fromEnv: ORACLE_PASSWORD") && yaml.contains("fromEnv: MONGO_PASSWORD"),
        "Deployment must use Lab secret env references"
    );
}

#[test]
fn escape_hatch_readme_distinguishes_scenarios_and_release_gate() {
    let readme =
        fs::read_to_string(escape_hatch_dir().join("README.md")).expect("read README.md");
    let lower = readme.to_ascii_lowercase();
    assert!(
        lower.contains("escape") || lower.contains("db-level") || lower.contains("restore"),
        "README should name the escape-hatch / restore purpose"
    );
    assert!(
        lower.contains("scenario"),
        "README must distinguish this flow from Lab Scenarios"
    );
    assert!(
        lower.contains("release quality gate") || lower.contains("ci"),
        "README must distinguish this flow from Release Quality Gate / CI"
    );
}

/// Full documented escape-hatch flow on the disposable Lab Fixture.
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_escape_hatch -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Lab Fixture + Instant Client; not part of Release Quality Gate"]
async fn lab_db_level_load_then_product_status_inspect_path() {
    let lab = lab_dir();
    let lab_str = lab.to_string_lossy();
    let escape = escape_hatch_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    // Bring Fixture up (idempotent enough for a dedicated ignored seam).
    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab_str])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");
    assert!(
        up_out.contains("Oracle Source:") && up_out.contains("MongoDB Target:"),
        "lab up must print Lab connection details used by the escape hatch, got:\n{up_out}"
    );
    assert!(
        up_out.contains("escape-hatch") || up_out.contains("DB-level"),
        "lab up Next tips should mention the DB-level escape hatch, got:\n{up_out}"
    );

    // Documented Oracle load: sqlplus inside Lab Oracle (no BYO production engine).
    let oracle_sql = fs::read(escape.join("oracle-load.sql")).expect("oracle-load.sql bytes");
    let (oracle_ok, oracle_text) = compose_exec_stdin(
        &lab,
        &["oracle", "sqlplus", "-s", "SYNC_USER/lab_oracle@FREEPDB1"],
        &oracle_sql,
    );
    assert!(oracle_ok, "documented Oracle load failed:\n{oracle_text}");

    // Documented Mongo load: mongosh inside Lab Mongo (stdin script).
    let mongo_js = fs::read(escape.join("mongo-load.js")).expect("mongo-load.js bytes");
    let (mongo_ok, mongo_text) = compose_exec_stdin(
        &lab,
        &[
            "mongo",
            "mongosh",
            "--quiet",
            "--host",
            "127.0.0.1",
            "-u",
            "migraloop",
            "-p",
            "lab_mongo",
            "--authenticationDatabase",
            "admin",
            "lab",
        ],
        &mongo_js,
    );
    assert!(mongo_ok, "documented Mongo load failed:\n{mongo_text}");
    assert!(
        mongo_text.contains("lab_escape_manual") || mongo_text.contains("count="),
        "Mongo load should report seeded collection, got:\n{mongo_text}"
    );

    // Continue on the real product path (not a Lab Scenario).
    let apply = Command::new(bin())
        .env("MIGRALOOP_PLATFORM_STORE_URL", store_url)
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "apply",
            "-f",
            escape.join("deployment.yaml").to_str().unwrap(),
            "--platform-store-url",
            store_url,
        ])
        .output()
        .expect("migraloop apply");
    let apply_text = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        apply.status.success(),
        "product apply after DB-level Oracle load failed:\n{apply_text}"
    );
    assert!(
        apply_text.contains("Initial Load") || apply_text.contains("applied") || apply_text.contains("Pipeline"),
        "apply should exercise product path after escape-hatch load, got:\n{apply_text}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("migraloop status");
    let status_text = format!(
        "{}{}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        status.status.success(),
        "product status after escape-hatch load failed:\n{status_text}"
    );
    assert!(
        status_text.contains("lab-escape-hatch")
            || status_text.contains("LAB_ESCAPE_CUSTOMERS")
            || status_text.contains("lab-escape"),
        "status should reflect escape-hatch Deployment/Pipeline/Base, got:\n{status_text}"
    );

    let base = Command::new(bin())
        .args([
            "base",
            "--table",
            "LAB_ESCAPE_CUSTOMERS",
            "--platform-store-url",
            store_url,
        ])
        .output()
        .expect("migraloop base");
    let base_text = format!(
        "{}{}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    assert!(
        base.status.success(),
        "product base inspect after escape-hatch load failed:\n{base_text}"
    );
    assert!(
        base_text.contains("Alice") || base_text.contains("alice") || base_text.contains("1"),
        "base inspect should show loaded Source rows, got:\n{base_text}"
    );

    let target = Command::new(bin())
        .args([
            "target",
            "--collection",
            "lab_escape_customers",
            "--platform-store-url",
            store_url,
        ])
        .output()
        .expect("migraloop target");
    let target_text = format!(
        "{}{}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    assert!(
        target.status.success(),
        "product target inspect after escape-hatch Delivery failed:\n{target_text}"
    );
    assert!(
        target_text.contains("Alice") || target_text.contains("alice") || target_text.contains("1"),
        "target inspect should show Delivered rows from Oracle load, got:\n{target_text}"
    );

    let sync = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["sync", "--platform-store-url", store_url])
        .output()
        .expect("migraloop sync");
    let sync_text = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    assert!(
        sync.status.success(),
        "product sync after escape-hatch load failed:\n{sync_text}"
    );

    // Keep Fixture up for follow-on manual work; do not lab down here.
}
