//! Shared Scenario Namespace lifecycle for the Lab recipe runner (#201 / ADR-0025).
//!
//! Owns isomorphic wipe / prepare (CREATE + supplemental logging + seed) / mutate
//! SQL so catalog Scenarios do not copy prepare/mutate/remove triples. Rare
//! escapes (parallel sessions, CLI verbs, generated backlog, DDL bridges) stay
//! in thin adapter hooks.

use std::path::Path;

use crate::lab::{
    mongosh_in_mongo, sqlplus_in_oracle, LAB_ORACLE_PASSWORD_DEFAULT, LAB_ORACLE_USER,
};
use crate::CliError;

use super::lab_delete_deployment;
use super::recipe::{
    ScenarioRecipe, ScenarioRecipeNamespace, ScenarioRecipeNamespaceLifecycle,
    ScenarioRecipeNamespaceTable,
};

const SQL_PREAMBLE: &str = "SET HEADING OFF FEEDBACK OFF PAGES 0\n\
WHENEVER SQLERROR EXIT SQL.SQLCODE\n";

/// Build idempotent Oracle DROP TABLE blocks (SQLCODE -942 = missing table).
pub(crate) fn wipe_oracle_sql(tables: &[String]) -> String {
    let mut body = String::from(SQL_PREAMBLE);
    // Drop later-listed tables first (child tables commonly follow parents).
    for table in tables.iter().rev() {
        body.push_str(&format!(
            "BEGIN\n\
  EXECUTE IMMEDIATE 'DROP TABLE {table} PURGE';\n\
EXCEPTION\n\
  WHEN OTHERS THEN\n\
    IF SQLCODE != -942 THEN RAISE; END IF;\n\
END;\n\
/\n"
        ));
    }
    body.push_str("EXIT;\n");
    body
}

/// Build Mongo `drop()` statements for Scenario Namespace collections.
pub(crate) fn wipe_mongo_js(collections: &[String]) -> String {
    collections
        .iter()
        .map(|c| format!("db.getCollection('{c}').drop();"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Build CREATE TABLE + optional supplemental logging + seed SQL for prepare.
pub(crate) fn prepare_oracle_sql(lifecycle: &ScenarioRecipeNamespaceLifecycle) -> String {
    let mut body = String::from(SQL_PREAMBLE);
    for table in &lifecycle.tables {
        let columns = table.columns.trim().trim_end_matches(',');
        body.push_str(&format!("CREATE TABLE {} (\n{columns}\n);\n", table.name));
        if table.supplemental_logging {
            body.push_str(&format!(
                "ALTER TABLE {} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;\n",
                table.name
            ));
        }
    }
    let seed = lifecycle.seed_sql.trim();
    if !seed.is_empty() {
        body.push_str(seed);
        if !seed.ends_with('\n') {
            body.push('\n');
        }
    }
    body.push_str("COMMIT;\nEXIT;\n");
    body
}

/// Wrap recipe mutate SQL body with sqlplus preamble / commit / exit.
pub(crate) fn mutate_oracle_sql(mutate_sql: &str) -> String {
    let mut body = String::from(SQL_PREAMBLE);
    let mutate = mutate_sql.trim();
    body.push_str(mutate);
    if !mutate.ends_with('\n') {
        body.push('\n');
    }
    body.push_str("COMMIT;\nEXIT;\n");
    body
}

fn oracle_connect() -> String {
    format!("{LAB_ORACLE_USER}/{LAB_ORACLE_PASSWORD_DEFAULT}@FREEPDB1")
}

/// Fully remove Scenario Namespace (Source tables, Target collections, Deployment).
/// Idempotent. Driven from recipe `namespace` identity fields.
pub(crate) async fn wipe_namespace(
    lab_dir: &Path,
    namespace: &ScenarioRecipeNamespace,
) -> Result<(), CliError> {
    println!(
        "Lab Scenario: removing Namespace \
         (tables={}, collections={}, deployment={})",
        namespace.source_tables.join(","),
        namespace.target_collections.join(","),
        namespace.deployment
    );

    if !namespace.source_tables.is_empty() {
        let sql = wipe_oracle_sql(&namespace.source_tables);
        sqlplus_in_oracle(lab_dir, &oracle_connect(), &sql)
            .await
            .map_err(|err| {
                CliError::Failed(format!(
                    "Failed to drop Oracle Scenario Namespace tables {}:\n{err}",
                    namespace.source_tables.join("/")
                ))
            })?;
    }

    if !namespace.target_collections.is_empty() {
        let js = wipe_mongo_js(&namespace.target_collections);
        mongosh_in_mongo(lab_dir, &js).await.map_err(|err| {
            CliError::Failed(format!(
                "Failed to drop Mongo Scenario Namespace collections {}:\n{err}",
                namespace.target_collections.join("/")
            ))
        })?;
    }

    lab_delete_deployment(&namespace.deployment)
        .await
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to delete Platform Store Deployment `{}` \
                 for Scenario Namespace cleanup:\n{err}",
                namespace.deployment
            ))
        })?;

    Ok(())
}

/// Wipe then recreate Namespace tables + supplemental logging + seed from recipe lifecycle.
pub(crate) async fn prepare_namespace(
    lab_dir: &Path,
    recipe: &ScenarioRecipe,
) -> Result<(), CliError> {
    let lifecycle = recipe.namespace.lifecycle.as_ref().ok_or_else(|| {
        CliError::Failed(format!(
            "Lab Scenario `{}` product_path prepare_namespace requires \
             namespace.lifecycle (shared Namespace lifecycle; issue #201)",
            recipe.id
        ))
    })?;
    wipe_namespace(lab_dir, &recipe.namespace).await?;

    let sql = prepare_oracle_sql(lifecycle);
    sqlplus_in_oracle(lab_dir, &oracle_connect(), &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to prepare Scenario Namespace for `{}`:\n{err}",
                recipe.id
            ))
        })
}

/// Drive Source mutate SQL from recipe lifecycle when `mutate_sql` is set.
/// Returns `true` when shared mutate ran; `false` when the Scenario uses a thin escape only.
pub(crate) async fn mutate_namespace_from_recipe(
    lab_dir: &Path,
    recipe: &ScenarioRecipe,
) -> Result<bool, CliError> {
    let Some(lifecycle) = recipe.namespace.lifecycle.as_ref() else {
        return Ok(false);
    };
    let Some(mutate_sql) = lifecycle.mutate_sql.as_ref() else {
        return Ok(false);
    };
    if mutate_sql.trim().is_empty() {
        return Ok(false);
    }
    let sql = mutate_oracle_sql(mutate_sql);
    sqlplus_in_oracle(lab_dir, &oracle_connect(), &sql)
        .await
        .map(|_| ())
        .map_err(|err| {
            CliError::Failed(format!(
                "Failed to drive Source mutations for Lab Scenario `{}`:\n{err}",
                recipe.id
            ))
        })?;
    Ok(true)
}

/// Validate `namespace.lifecycle` against namespace identity + product_path needs (#201).
pub(crate) fn validate_namespace_lifecycle(
    path_display: &str,
    namespace: &ScenarioRecipeNamespace,
    has_prepare_step: bool,
    has_mutate_step: bool,
) -> Result<(), CliError> {
    let Some(lifecycle) = namespace.lifecycle.as_ref() else {
        if has_prepare_step {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {path_display} declares product_path `prepare_namespace` \
                 but namespace.lifecycle is missing (shared Namespace lifecycle required; issue #201)"
            )));
        }
        return Ok(());
    };
    if lifecycle.tables.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} namespace.lifecycle.tables must be non-empty"
        )));
    }
    for table in &lifecycle.tables {
        validate_lifecycle_table(path_display, namespace, table)?;
    }
    // Every declared source table must have a lifecycle table shape.
    for source in &namespace.source_tables {
        if !lifecycle.tables.iter().any(|t| t.name == *source) {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {path_display} namespace.source_tables entry `{source}` \
                 has no namespace.lifecycle.tables[].name match"
            )));
        }
    }
    if lifecycle.seed_sql.trim().is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} namespace.lifecycle.seed_sql must be non-empty"
        )));
    }
    // Mutate SQL is optional: rare escapes (CLI verbs, parallel sessions, backlog
    // generators) omit it and keep a thin adapter. When present it must be non-empty.
    if let Some(mutate_sql) = &lifecycle.mutate_sql {
        if mutate_sql.trim().is_empty() {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {path_display} namespace.lifecycle.mutate_sql must be \
                 non-empty when set (omit the field for thin mutate escapes)"
            )));
        }
    } else if has_mutate_step {
        // Allowed: thin adapter owns mutate. No extra validation here.
        let _ = has_mutate_step;
    }
    Ok(())
}

fn validate_lifecycle_table(
    path_display: &str,
    namespace: &ScenarioRecipeNamespace,
    table: &ScenarioRecipeNamespaceTable,
) -> Result<(), CliError> {
    if table.name.trim().is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} namespace.lifecycle.tables[].name must be non-empty"
        )));
    }
    if !namespace.source_tables.iter().any(|t| t == &table.name) {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} namespace.lifecycle table `{}` must also appear \
             in namespace.source_tables",
            table.name
        )));
    }
    if table.columns.trim().is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} namespace.lifecycle table `{}` columns must be \
             non-empty",
            table.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lab_scenario::recipe::ScenarioRecipeNamespaceTable;

    fn customers_lifecycle(table: &str, with_active: bool) -> ScenarioRecipeNamespaceLifecycle {
        let columns = if with_active {
            "ID NUMBER(10) PRIMARY KEY,\n  NAME VARCHAR2(100) NOT NULL,\n  EMAIL VARCHAR2(200),\n  ACTIVE NUMBER(1)".to_string()
        } else {
            "ID NUMBER(10) PRIMARY KEY,\n  NAME VARCHAR2(100) NOT NULL,\n  EMAIL VARCHAR2(200)".to_string()
        };
        let seed_sql = if with_active {
            format!(
                "INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1);\n\
INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0);"
            )
        } else {
            format!(
                "INSERT INTO {table} (ID, NAME, EMAIL) VALUES (1, 'Alice', 'alice@example.com');\n\
INSERT INTO {table} (ID, NAME, EMAIL) VALUES (2, 'Bob', 'bob@example.com');"
            )
        };
        let mutate_sql = if with_active {
            Some(format!(
                "UPDATE {table} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1);\n\
DELETE FROM {table} WHERE ID = 2;"
            ))
        } else {
            Some(format!(
                "UPDATE {table} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1;\n\
INSERT INTO {table} (ID, NAME, EMAIL) VALUES (3, 'Carol', 'carol@example.com');\n\
DELETE FROM {table} WHERE ID = 2;"
            ))
        };
        ScenarioRecipeNamespaceLifecycle {
            tables: vec![ScenarioRecipeNamespaceTable {
                name: table.to_string(),
                columns,
                supplemental_logging: true,
            }],
            seed_sql,
            mutate_sql,
        }
    }

    #[test]
    fn wipe_oracle_sql_drops_tables_idempotently_in_reverse_order() {
        let sql = wipe_oracle_sql(&["LAB_A".into(), "LAB_B".into()]);
        assert!(sql.contains("DROP TABLE LAB_B PURGE"), "sql={sql}");
        assert!(sql.contains("DROP TABLE LAB_A PURGE"), "sql={sql}");
        assert!(
            sql.find("LAB_B").unwrap() < sql.find("LAB_A").unwrap(),
            "child/later tables drop first; sql={sql}"
        );
        assert!(sql.contains("SQLCODE != -942"), "sql={sql}");
    }

    #[test]
    fn wipe_mongo_js_drops_each_collection() {
        let js = wipe_mongo_js(&["lab_a".into(), "lab_b".into()]);
        assert_eq!(
            js,
            "db.getCollection('lab_a').drop();\ndb.getCollection('lab_b').drop();"
        );
    }

    #[test]
    fn prepare_oracle_sql_creates_tables_supplemental_and_seed() {
        let lifecycle = customers_lifecycle("LAB_DP_CUSTOMERS", true);
        let sql = prepare_oracle_sql(&lifecycle);
        assert!(
            sql.contains("CREATE TABLE LAB_DP_CUSTOMERS (\nID NUMBER(10) PRIMARY KEY,"),
            "sql={sql}"
        );
        assert!(
            sql.contains("ALTER TABLE LAB_DP_CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;"),
            "sql={sql}"
        );
        assert!(sql.contains("INSERT INTO LAB_DP_CUSTOMERS"), "sql={sql}");
        assert!(sql.contains("COMMIT;"), "sql={sql}");
        assert!(sql.contains("EXIT;"), "sql={sql}");
    }

    #[test]
    fn mutate_oracle_sql_wraps_body() {
        let sql = mutate_oracle_sql("UPDATE T SET NAME = 'Alicia' WHERE ID = 1;");
        assert!(sql.starts_with("SET HEADING OFF"), "sql={sql}");
        assert!(sql.contains("UPDATE T SET NAME = 'Alicia' WHERE ID = 1;"), "sql={sql}");
        assert!(sql.contains("COMMIT;\nEXIT;\n"), "sql={sql}");
    }

    #[test]
    fn validate_namespace_lifecycle_requires_lifecycle_for_prepare() {
        let namespace = ScenarioRecipeNamespace {
            source_tables: vec!["LAB_X".into()],
            target_collections: vec!["lab_x".into()],
            deployment: "lab-x".into(),
            pipelines: vec![],
            lifecycle: None,
        };
        let err = validate_namespace_lifecycle("demo.yaml", &namespace, true, true)
            .expect_err("lifecycle required");
        assert!(
            err.to_string().contains("namespace.lifecycle"),
            "err={err}"
        );
    }

    #[test]
    fn validate_namespace_lifecycle_rejects_table_not_in_source_tables() {
        let mut lifecycle = customers_lifecycle("LAB_OTHER", true);
        lifecycle.tables[0].name = "LAB_OTHER".into();
        let namespace = ScenarioRecipeNamespace {
            source_tables: vec!["LAB_X".into()],
            target_collections: vec!["lab_x".into()],
            deployment: "lab-x".into(),
            pipelines: vec![],
            lifecycle: Some(lifecycle),
        };
        let err = validate_namespace_lifecycle("demo.yaml", &namespace, true, false)
            .expect_err("name mismatch");
        assert!(
            err.to_string().contains("source_tables"),
            "err={err}"
        );
    }

    #[test]
    fn validate_namespace_lifecycle_accepts_escape_without_mutate_sql() {
        let mut lifecycle = customers_lifecycle("LAB_X", true);
        lifecycle.mutate_sql = None;
        let namespace = ScenarioRecipeNamespace {
            source_tables: vec!["LAB_X".into()],
            target_collections: vec!["lab_x".into()],
            deployment: "lab-x".into(),
            pipelines: vec![],
            lifecycle: Some(lifecycle),
        };
        validate_namespace_lifecycle("demo.yaml", &namespace, true, true)
            .expect("thin mutate escape ok");
    }
}
