use anyhow::{Context, Result};
use sqlx::PgPool;

use crate::discovery::Unit;
use crate::ledger;

/// Check if SQL contains ALTER TYPE ... ADD VALUE pattern.
pub fn needs_statement_mode(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    upper.contains("ADD VALUE") && upper.contains("ALTER TYPE")
}

/// Execute a single unit within a transaction.
/// If the SQL contains ALTER TYPE ADD VALUE, falls back to statement-by-statement
/// execution (no wrapping transaction) since PostgreSQL doesn't allow using
/// newly added enum values in the same transaction.
pub async fn execute_unit(pool: &PgPool, unit: &Unit, checksum: &str) -> Result<()> {
    let sql = unit.read_sql()?;

    if needs_statement_mode(&sql) {
        for stmt in crate::cli::split_sql_statements(&sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() || trimmed == ";" {
                continue;
            }
            sqlx::raw_sql(trimmed)
                .execute(pool)
                .await
                .with_context(|| format!("Failed to execute unit '{}' at: {}...", unit.id, &trimmed[..trimmed.len().min(80)]))?;
        }

        sqlx::raw_sql(
            &format!(
                "INSERT INTO schema_migrations (unit_id, checksum) VALUES ('{}', '{}')
                 ON CONFLICT (unit_id) DO UPDATE SET checksum = '{}', applied_at = now()",
                unit.id.replace('\'', "''"),
                checksum.replace('\'', "''"),
                checksum.replace('\'', "''"),
            )
        )
        .execute(pool)
        .await
        .with_context(|| format!("Failed to record unit '{}' in ledger", unit.id))?;
    } else {
        let mut tx = pool
            .begin()
            .await
            .context("Failed to begin transaction")?;

        sqlx::raw_sql(&sql)
            .execute(&mut *tx)
            .await
            .with_context(|| format!("Failed to execute unit '{}'", unit.id))?;

        sqlx::raw_sql(
            &format!(
                "INSERT INTO schema_migrations (unit_id, checksum) VALUES ('{}', '{}')
                 ON CONFLICT (unit_id) DO UPDATE SET checksum = '{}', applied_at = now()",
                unit.id.replace('\'', "''"),
                checksum.replace('\'', "''"),
                checksum.replace('\'', "''"),
            )
        )
        .execute(&mut *tx)
        .await
        .with_context(|| format!("Failed to record unit '{}' in ledger", unit.id))?;

        tx.commit()
            .await
            .with_context(|| format!("Failed to commit unit '{}'", unit.id))?;
    }

    Ok(())
}

/// Apply all pending units in order.
pub async fn apply_all(
    pool: &PgPool,
    units: &std::collections::BTreeMap<String, Unit>,
    order: &[String],
    checksums: &std::collections::BTreeMap<String, String>,
) -> Result<ApplyResult> {
    ledger::ensure_table(pool).await?;
    let applied = ledger::applied_units(pool).await?;

    let mut applied_count = 0;
    let mut skipped_count = 0;

    for unit_id in order {
        if applied.contains(unit_id) {
            skipped_count += 1;
            continue;
        }

        let unit = &units[unit_id];
        let checksum = &checksums[unit_id];

        println!("  Applying: {}", unit_id);
        execute_unit(pool, unit, checksum).await?;
        applied_count += 1;
    }

    Ok(ApplyResult {
        applied: applied_count,
        skipped: skipped_count,
    })
}

#[derive(Debug)]
pub struct ApplyResult {
    pub applied: usize,
    pub skipped: usize,
}
