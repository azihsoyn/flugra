use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::HashSet;

const LEDGER_TABLE: &str = "schema_migrations";

/// Ensure the ledger table exists.
pub async fn ensure_table(pool: &PgPool) -> Result<()> {
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            unit_id TEXT PRIMARY KEY,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
        LEDGER_TABLE
    ))
    .execute(pool)
    .await
    .context("Failed to create schema_migrations table")?;
    Ok(())
}

/// Record a unit as applied.
#[allow(dead_code)]
pub async fn record(pool: &PgPool, unit_id: &str, checksum: &str) -> Result<()> {
    sqlx::query(&format!(
        "INSERT INTO {} (unit_id, checksum) VALUES ($1, $2)
         ON CONFLICT (unit_id) DO UPDATE SET checksum = $2, applied_at = now()",
        LEDGER_TABLE
    ))
    .bind(unit_id)
    .bind(checksum)
    .execute(pool)
    .await
    .with_context(|| format!("Failed to record unit '{}'", unit_id))?;
    Ok(())
}

/// A record of an applied unit.
#[derive(Debug)]
pub struct AppliedUnit {
    pub unit_id: String,
    pub checksum: String,
    pub applied_at: chrono::DateTime<chrono::Utc>,
}

/// Get all applied unit IDs.
pub async fn applied_units(pool: &PgPool) -> Result<HashSet<String>> {
    let rows: Vec<(String,)> = sqlx::query_as(&format!("SELECT unit_id FROM {}", LEDGER_TABLE))
        .fetch_all(pool)
        .await
        .context("Failed to query applied units")?;
    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Get detailed info about all applied units.
pub async fn applied_units_detail(pool: &PgPool) -> Result<Vec<AppliedUnit>> {
    let rows: Vec<(String, String, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(&format!(
        "SELECT unit_id, checksum, applied_at FROM {} ORDER BY applied_at",
        LEDGER_TABLE
    ))
    .fetch_all(pool)
    .await
    .context("Failed to query applied units")?;

    Ok(rows
        .into_iter()
        .map(|(unit_id, checksum, applied_at)| AppliedUnit {
            unit_id,
            checksum,
            applied_at,
        })
        .collect())
}
