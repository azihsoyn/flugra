use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::BTreeMap;

/// Dump the current database schema (tables, columns, types, indexes, constraints).
/// Returns a normalized, deterministic string representation for comparison.
pub async fn dump_schema(pool: &PgPool) -> Result<SchemaSnapshot> {
    let tables = dump_tables(pool).await?;
    let types = dump_custom_types(pool).await?;
    let functions = dump_functions(pool).await?;
    let views = dump_views(pool).await?;

    Ok(SchemaSnapshot {
        tables,
        types,
        functions,
        views,
    })
}

#[derive(Debug, Clone)]
pub struct SchemaSnapshot {
    pub tables: BTreeMap<String, TableInfo>,
    pub types: BTreeMap<String, TypeInfo>,
    pub functions: BTreeMap<String, FunctionInfo>,
    pub views: BTreeMap<String, ViewInfo>,
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub indexes: Vec<IndexInfo>,
    pub policies: Vec<PolicyInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ConstraintInfo {
    pub name: String,
    pub constraint_type: String,
    pub definition: String,
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub definition: String,
}

#[derive(Debug, Clone)]
pub struct PolicyInfo {
    pub name: String,
    pub command: String,
    pub permissive: String,
    pub roles: String,
    pub qual: Option<String>,
    pub with_check: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TypeInfo {
    pub name: String,
    pub kind: String,
    pub labels: Vec<String>, // for enums
}

#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub result_type: String,
    pub argument_types: String,
}

#[derive(Debug, Clone)]
pub struct ViewInfo {
    pub name: String,
    pub definition: String,
    pub is_materialized: bool,
}

async fn dump_tables(pool: &PgPool) -> Result<BTreeMap<String, TableInfo>> {
    let table_rows: Vec<(String,)> = sqlx::query_as(
        "SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
         ORDER BY table_name"
    )
    .fetch_all(pool)
    .await
    .context("Failed to query tables")?;

    let mut tables = BTreeMap::new();

    for (table_name,) in table_rows {
        let columns = dump_columns(pool, &table_name).await?;
        let constraints = dump_constraints(pool, &table_name).await?;
        let indexes = dump_indexes(pool, &table_name).await?;
        let policies = dump_policies(pool, &table_name).await?;

        tables.insert(
            table_name.clone(),
            TableInfo {
                name: table_name,
                columns,
                constraints,
                indexes,
                policies,
            },
        );
    }

    Ok(tables)
}

async fn dump_columns(pool: &PgPool, table: &str) -> Result<Vec<ColumnInfo>> {
    let rows: Vec<(String, String, String, Option<String>)> = sqlx::query_as(
        "SELECT column_name, data_type || COALESCE('(' || character_maximum_length || ')', ''),
                is_nullable, column_default
         FROM information_schema.columns
         WHERE table_schema = 'public' AND table_name = $1
         ORDER BY ordinal_position"
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .context("Failed to query columns")?;

    Ok(rows
        .into_iter()
        .map(|(name, data_type, nullable, default)| ColumnInfo {
            name,
            data_type,
            is_nullable: nullable == "YES",
            column_default: default,
        })
        .collect())
}

async fn dump_constraints(pool: &PgPool, table: &str) -> Result<Vec<ConstraintInfo>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT c.conname, c.contype::text,
                pg_get_constraintdef(c.oid)
         FROM pg_constraint c
         JOIN pg_class t ON c.conrelid = t.oid
         JOIN pg_namespace n ON t.relnamespace = n.oid
         WHERE n.nspname = 'public' AND t.relname = $1
         ORDER BY c.conname"
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .context("Failed to query constraints")?;

    Ok(rows
        .into_iter()
        .map(|(name, ctype, def)| ConstraintInfo {
            name,
            constraint_type: ctype,
            definition: def,
        })
        .collect())
}

async fn dump_indexes(pool: &PgPool, table: &str) -> Result<Vec<IndexInfo>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT i.relname, pg_get_indexdef(i.oid)
         FROM pg_index x
         JOIN pg_class i ON i.oid = x.indexrelid
         JOIN pg_class t ON t.oid = x.indrelid
         JOIN pg_namespace n ON t.relnamespace = n.oid
         WHERE n.nspname = 'public' AND t.relname = $1
           AND NOT x.indisprimary
         ORDER BY i.relname"
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .context("Failed to query indexes")?;

    Ok(rows
        .into_iter()
        .map(|(name, def)| IndexInfo {
            name,
            definition: def,
        })
        .collect())
}

async fn dump_policies(pool: &PgPool, table: &str) -> Result<Vec<PolicyInfo>> {
    let rows: Vec<(String, String, String, String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT pol.polname,
                CASE pol.polcmd
                    WHEN 'r' THEN 'SELECT'
                    WHEN 'a' THEN 'INSERT'
                    WHEN 'w' THEN 'UPDATE'
                    WHEN 'd' THEN 'DELETE'
                    WHEN '*' THEN 'ALL'
                END,
                CASE pol.polpermissive WHEN true THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
                pg_get_userbyid(unnest(pol.polroles)),
                pg_get_expr(pol.polqual, pol.polrelid),
                pg_get_expr(pol.polwithcheck, pol.polrelid)
         FROM pg_policy pol
         JOIN pg_class t ON pol.polrelid = t.oid
         JOIN pg_namespace n ON t.relnamespace = n.oid
         WHERE n.nspname = 'public' AND t.relname = $1
         ORDER BY pol.polname"
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    Ok(rows
        .into_iter()
        .map(|(name, cmd, perm, roles, qual, with_check)| PolicyInfo {
            name,
            command: cmd,
            permissive: perm,
            roles,
            qual,
            with_check,
        })
        .collect())
}

async fn dump_custom_types(pool: &PgPool) -> Result<BTreeMap<String, TypeInfo>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT t.typname, t.typtype::text
         FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
         WHERE n.nspname = 'public'
           AND t.typtype IN ('e', 'c', 'd')
         ORDER BY t.typname"
    )
    .fetch_all(pool)
    .await
    .context("Failed to query types")?;

    let mut types = BTreeMap::new();

    for (name, kind) in rows {
        let labels = if kind == "e" {
            let label_rows: Vec<(String,)> = sqlx::query_as(
                "SELECT e.enumlabel
                 FROM pg_enum e
                 JOIN pg_type t ON e.enumtypid = t.oid
                 JOIN pg_namespace n ON t.typnamespace = n.oid
                 WHERE n.nspname = 'public' AND t.typname = $1
                 ORDER BY e.enumsortorder"
            )
            .bind(&name)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            label_rows.into_iter().map(|(l,)| l).collect()
        } else {
            vec![]
        };

        types.insert(
            name.clone(),
            TypeInfo {
                name,
                kind: match kind.as_str() {
                    "e" => "enum".to_string(),
                    "c" => "composite".to_string(),
                    "d" => "domain".to_string(),
                    _ => kind,
                },
                labels,
            },
        );
    }

    Ok(types)
}

async fn dump_functions(pool: &PgPool) -> Result<BTreeMap<String, FunctionInfo>> {
    let rows: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT p.proname,
                pg_get_function_result(p.oid),
                pg_get_function_identity_arguments(p.oid)
         FROM pg_proc p
         JOIN pg_namespace n ON p.pronamespace = n.oid
         WHERE n.nspname = 'public'
         ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)"
    )
    .fetch_all(pool)
    .await
    .context("Failed to query functions")?;

    let mut funcs = BTreeMap::new();
    for (name, result_type, arg_types) in rows {
        let key = format!("{}({})", name, arg_types);
        funcs.insert(
            key,
            FunctionInfo {
                name,
                result_type,
                argument_types: arg_types,
            },
        );
    }

    Ok(funcs)
}

async fn dump_views(pool: &PgPool) -> Result<BTreeMap<String, ViewInfo>> {
    let mut views = BTreeMap::new();

    // Regular views
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, view_definition
         FROM information_schema.views
         WHERE table_schema = 'public'
         ORDER BY table_name"
    )
    .fetch_all(pool)
    .await
    .context("Failed to query views")?;

    for (name, def) in rows {
        views.insert(
            name.clone(),
            ViewInfo {
                name,
                definition: def,
                is_materialized: false,
            },
        );
    }

    // Materialized views
    let mat_rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT c.relname, pg_get_viewdef(c.oid)
         FROM pg_class c
         JOIN pg_namespace n ON c.relnamespace = n.oid
         WHERE n.nspname = 'public' AND c.relkind = 'm'
         ORDER BY c.relname"
    )
    .fetch_all(pool)
    .await
    .unwrap_or_default();

    for (name, def) in mat_rows {
        views.insert(
            name.clone(),
            ViewInfo {
                name,
                definition: def,
                is_materialized: true,
            },
        );
    }

    Ok(views)
}

impl SchemaSnapshot {
    /// Compare two snapshots and return differences.
    pub fn diff(&self, other: &SchemaSnapshot) -> SchemaDiff {
        let mut diffs = Vec::new();

        // Compare tables
        for (name, table) in &self.tables {
            if let Some(other_table) = other.tables.get(name) {
                diff_table(name, table, other_table, &mut diffs);
            } else {
                diffs.push(format!("Table '{}': exists in source but not in target", name));
            }
        }
        for name in other.tables.keys() {
            if !self.tables.contains_key(name) {
                diffs.push(format!("Table '{}': exists in target but not in source", name));
            }
        }

        // Compare types
        for (name, t) in &self.types {
            if let Some(other_t) = other.types.get(name) {
                if t.kind != other_t.kind {
                    diffs.push(format!("Type '{}': kind differs ({} vs {})", name, t.kind, other_t.kind));
                }
                if t.labels != other_t.labels {
                    diffs.push(format!("Type '{}': labels differ", name));
                    diffs.push(format!("  source: {:?}", t.labels));
                    diffs.push(format!("  target: {:?}", other_t.labels));
                }
            } else {
                diffs.push(format!("Type '{}': exists in source but not in target", name));
            }
        }
        for name in other.types.keys() {
            if !self.types.contains_key(name) {
                diffs.push(format!("Type '{}': exists in target but not in source", name));
            }
        }

        // Compare functions
        for name in self.functions.keys() {
            if !other.functions.contains_key(name) {
                diffs.push(format!("Function '{}': exists in source but not in target", name));
            }
        }
        for name in other.functions.keys() {
            if !self.functions.contains_key(name) {
                diffs.push(format!("Function '{}': exists in target but not in source", name));
            }
        }

        // Compare views
        for (name, v) in &self.views {
            if let Some(other_v) = other.views.get(name) {
                if v.definition != other_v.definition {
                    diffs.push(format!("View '{}': definition differs", name));
                }
                if v.is_materialized != other_v.is_materialized {
                    diffs.push(format!("View '{}': materialized flag differs", name));
                }
            } else {
                diffs.push(format!("View '{}': exists in source but not in target", name));
            }
        }
        for name in other.views.keys() {
            if !self.views.contains_key(name) {
                diffs.push(format!("View '{}': exists in target but not in source", name));
            }
        }

        SchemaDiff {
            differences: diffs,
        }
    }

    /// Format schema as a human-readable summary.
    pub fn summary(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("Tables: {}\n", self.tables.len()));
        out.push_str(&format!("Types: {}\n", self.types.len()));
        out.push_str(&format!("Functions: {}\n", self.functions.len()));
        out.push_str(&format!("Views: {}\n", self.views.len()));
        out
    }
}

fn diff_table(name: &str, a: &TableInfo, b: &TableInfo, diffs: &mut Vec<String>) {
    // Compare columns
    let a_cols: BTreeMap<_, _> = a.columns.iter().map(|c| (c.name.clone(), c)).collect();
    let b_cols: BTreeMap<_, _> = b.columns.iter().map(|c| (c.name.clone(), c)).collect();

    for (col_name, col) in &a_cols {
        if let Some(other_col) = b_cols.get(col_name) {
            if col.data_type != other_col.data_type {
                diffs.push(format!(
                    "Table '{}' column '{}': type differs ('{}' vs '{}')",
                    name, col_name, col.data_type, other_col.data_type
                ));
            }
            if col.is_nullable != other_col.is_nullable {
                diffs.push(format!(
                    "Table '{}' column '{}': nullable differs ({} vs {})",
                    name, col_name, col.is_nullable, other_col.is_nullable
                ));
            }
            if col.column_default != other_col.column_default {
                diffs.push(format!(
                    "Table '{}' column '{}': default differs ({:?} vs {:?})",
                    name, col_name, col.column_default, other_col.column_default
                ));
            }
        } else {
            diffs.push(format!(
                "Table '{}' column '{}': exists in source but not in target",
                name, col_name
            ));
        }
    }
    for col_name in b_cols.keys() {
        if !a_cols.contains_key(col_name) {
            diffs.push(format!(
                "Table '{}' column '{}': exists in target but not in source",
                name, col_name
            ));
        }
    }

    // Compare constraints
    let a_cons: BTreeMap<_, _> = a.constraints.iter().map(|c| (c.name.clone(), c)).collect();
    let b_cons: BTreeMap<_, _> = b.constraints.iter().map(|c| (c.name.clone(), c)).collect();

    for (con_name, con) in &a_cons {
        if let Some(other_con) = b_cons.get(con_name) {
            if con.definition != other_con.definition {
                diffs.push(format!(
                    "Table '{}' constraint '{}': definition differs",
                    name, con_name
                ));
            }
        } else {
            diffs.push(format!(
                "Table '{}' constraint '{}': exists in source but not in target",
                name, con_name
            ));
        }
    }
    for con_name in b_cons.keys() {
        if !a_cons.contains_key(con_name) {
            diffs.push(format!(
                "Table '{}' constraint '{}': exists in target but not in source",
                name, con_name
            ));
        }
    }

    // Compare indexes
    let a_idx: BTreeMap<_, _> = a.indexes.iter().map(|i| (i.name.clone(), i)).collect();
    let b_idx: BTreeMap<_, _> = b.indexes.iter().map(|i| (i.name.clone(), i)).collect();

    for (idx_name, idx) in &a_idx {
        if let Some(other_idx) = b_idx.get(idx_name) {
            if idx.definition != other_idx.definition {
                diffs.push(format!(
                    "Table '{}' index '{}': definition differs",
                    name, idx_name
                ));
            }
        } else {
            diffs.push(format!(
                "Table '{}' index '{}': exists in source but not in target",
                name, idx_name
            ));
        }
    }
    for idx_name in b_idx.keys() {
        if !a_idx.contains_key(idx_name) {
            diffs.push(format!(
                "Table '{}' index '{}': exists in target but not in source",
                name, idx_name
            ));
        }
    }
}

#[derive(Debug)]
pub struct SchemaDiff {
    pub differences: Vec<String>,
}

impl SchemaDiff {
    pub fn is_empty(&self) -> bool {
        self.differences.is_empty()
    }

    pub fn display(&self) {
        if self.is_empty() {
            println!("Schemas are identical.");
        } else {
            println!("Found {} difference(s):\n", self.differences.len());
            for diff in &self.differences {
                println!("  {}", diff);
            }
        }
    }
}
