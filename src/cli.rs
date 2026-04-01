use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::{discovery, executor, hooks, ledger, planner, schema};

#[derive(Parser)]
#[command(name = "flugra", about = "fluent migration — dependency-aware execution manager for native SQL units")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Show pending units and execution plan
    Plan {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,

        /// Database connection URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },

    /// Apply pending units to the database
    Apply {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,

        /// Database connection URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },

    /// Import existing migration state into the ledger
    ///
    /// Determines which units have already been applied by comparing schemas:
    /// applies all migrations to a temporary database and compares the result
    /// with the reference database to find the boundary between applied and pending.
    Import {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,

        /// Database connection URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Show what would be imported without actually writing
        #[arg(long)]
        dry_run: bool,

        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },

    /// Verify migrations by comparing schemas
    ///
    /// Applies all migrations to a temporary database and compares
    /// the resulting schema against a reference database.
    Diff {
        /// Reference database URL to compare against
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Root directory containing migration files
        #[arg(default_value = ".")]
        root: PathBuf,

        /// Copy functions from reference DB before applying migrations
        /// (for projects with externally managed functions)
        #[arg(long)]
        copy_schema_objects: bool,
    },
}

pub async fn plan(root: &PathBuf, database_url: &str) -> Result<()> {
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    let deps = planner::resolve_dependencies(&units)?;
    planner::validate_no_cycles(&deps)?;
    let order = planner::execution_order(&deps)?;

    // Compute checksums
    let mut checksums: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (id, unit) in &units {
        checksums.insert(id.clone(), unit.checksum()?);
    }

    // Connect to database
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to database")?;

    ledger::ensure_table(&pool).await?;
    let applied = ledger::applied_units(&pool).await?;

    let pending: Vec<&String> = order.iter().filter(|id| !applied.contains(*id)).collect();

    if pending.is_empty() {
        println!("All {} unit(s) are already applied.", order.len());
        return Ok(());
    }

    println!("Pending {} unit(s) (of {} total):\n", pending.len(), order.len());
    for (i, unit_id) in pending.iter().enumerate() {
        let dep = &deps[*unit_id];
        let checksum = &checksums[*unit_id];
        print!("  {}. {} ({}...)", i + 1, unit_id, &checksum[..8.min(checksum.len())]);
        if !dep.depends_on_units.is_empty() {
            print!(" depends on: {}", dep.depends_on_units.join(", "));
        }
        println!();
    }

    println!("\nAlready applied: {} unit(s)", applied.len());

    Ok(())
}

pub async fn apply(root: &PathBuf, database_url: &str) -> Result<()> {
    let hooks_config = hooks::HooksConfig::load(root)?;
    if hooks_config.has_hooks() {
        println!("Loaded hooks from flugra.hooks.yaml");
    }

    let units = discovery::discover(root)?;
    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    let deps = planner::resolve_dependencies(&units)?;
    planner::validate_no_cycles(&deps)?;
    let order = planner::execution_order(&deps)?;

    let mut checksums: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (id, unit) in &units {
        checksums.insert(id.clone(), unit.checksum()?);
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to database")?;

    // Run pre_apply hooks
    let root_abs = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    hooks::run_hooks(&hooks_config.pre_apply, "pre_apply", database_url, &root_abs)?;

    println!("Applying migrations...\n");
    let result = executor::apply_all(&pool, &units, &order, &checksums).await?;

    // Run post_apply hooks
    hooks::run_hooks(&hooks_config.post_apply, "post_apply", database_url, &root_abs)?;

    println!(
        "\nDone. Applied: {}, Skipped (already applied): {}",
        result.applied, result.skipped
    );

    Ok(())
}

pub async fn import(root: &PathBuf, database_url: &str, dry_run: bool, yes: bool) -> Result<()> {
    let hooks_config = hooks::HooksConfig::load(root)?;
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    let ordered_ids: Vec<String> = units.keys().cloned().collect();
    println!("Discovered {} unit(s)", ordered_ids.len());

    let ref_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to database")?;

    println!("Snapshotting reference database schema...");
    let ref_schema = schema::dump_schema(&ref_pool).await?;

    let temp_db_name = format!("flugra_import_{}", std::process::id());
    println!("Creating temporary database '{}'...", temp_db_name);

    sqlx::query(&format!("CREATE DATABASE \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await
        .with_context(|| format!("Failed to create temporary database '{}'", temp_db_name))?;

    let temp_url = replace_db_in_url(database_url, &temp_db_name)?;

    let result = import_detect_applied(
        &ref_pool, &ref_schema, &temp_url,
        &units, &ordered_ids, dry_run, yes, &hooks_config, root,
    ).await;

    println!("Dropping temporary database '{}'...", temp_db_name);
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await;

    result
}

async fn import_detect_applied(
    ref_pool: &sqlx::PgPool,
    ref_schema: &schema::SchemaSnapshot,
    temp_url: &str,
    units: &std::collections::BTreeMap<String, discovery::Unit>,
    ordered_ids: &[String],
    dry_run: bool,
    yes: bool,
    hooks_config: &hooks::HooksConfig,
    root: &PathBuf,
) -> Result<()> {
    let temp_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(temp_url)
        .await
        .context("Failed to connect to temporary database")?;

    // Install extensions from reference DB
    let extensions: Vec<(String,)> = sqlx::query_as(
        "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname"
    ).fetch_all(ref_pool).await.unwrap_or_default();

    for (ext,) in &extensions {
        let sql = format!("CREATE EXTENSION IF NOT EXISTS \"{}\" CASCADE", ext);
        let _ = sqlx::raw_sql(&sql).execute(&temp_pool).await;
    }

    // Run pre_apply hooks
    let root_abs = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    hooks::run_hooks(&hooks_config.pre_apply, "pre_apply", temp_url, &root_abs)?;

    println!("\nApplying all migrations to temporary database...");

    let mut apply_results: Vec<(String, bool)> = Vec::new();
    for unit_id in ordered_ids {
        let unit = &units[unit_id];
        let sql = unit.read_sql()?;
        if sql.trim().is_empty() {
            apply_results.push((unit_id.clone(), true));
            continue;
        }
        let ok = execute_migration_sql(&temp_pool, &sql).await.is_ok();
        apply_results.push((unit_id.clone(), ok));
    }
    let ok_count = apply_results.iter().filter(|(_, ok)| *ok).count();
    let fail_count = apply_results.iter().filter(|(_, ok)| !*ok).count();
    println!("  Applied: {}, Failed: {}", ok_count, fail_count);

    // Compare final temp schema with reference
    let temp_schema = schema::dump_schema(&temp_pool).await?;
    let diff = temp_schema.diff(ref_schema);

    let applied_ids: Vec<String>;
    let pending_ids: Vec<String>;

    if diff.source_only.is_empty() && diff.modified.is_empty() {
        println!("  All migration objects exist in reference DB.");
        applied_ids = ordered_ids.to_vec();
        pending_ids = Vec::new();
    } else {
        println!("  Found {} object(s) not in reference DB. Detecting boundary...", diff.source_only.len() + diff.modified.len());

        let extra_names: std::collections::HashSet<String> = diff.source_only.iter()
            .filter_map(|s| s.split('\'').nth(1).map(|n| n.to_string()))
            .collect();

        let mut boundary = ordered_ids.len();
        for (i, unit_id) in ordered_ids.iter().enumerate() {
            let unit = &units[unit_id];
            let sql = unit.read_sql()?;
            let analysis = crate::parser::analyze(&sql);

            for table in &analysis.creates {
                if extra_names.contains(table) {
                    boundary = i;
                    break;
                }
            }
            if boundary < ordered_ids.len() {
                break;
            }
        }

        applied_ids = ordered_ids[..boundary].to_vec();
        pending_ids = ordered_ids[boundary..].to_vec();
    }

    println!("  Result: {} applied, {} pending", applied_ids.len(), pending_ids.len());

    // Build import list with checksums
    let mut to_import: Vec<(String, String)> = Vec::new();
    for id in &applied_ids {
        let unit = &units[id];
        let checksum = unit.checksum()?;
        to_import.push((id.clone(), checksum));
    }

    // Check schema_migrations table state
    let table_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'schema_migrations')"
    ).fetch_one(ref_pool).await.unwrap_or((false,));

    let existing_ids: std::collections::HashSet<String> = if table_exists.0 {
        ledger::applied_units(ref_pool).await.unwrap_or_default()
    } else {
        std::collections::HashSet::new()
    };
    let new_records: Vec<&(String, String)> = to_import.iter()
        .filter(|(id, _)| !existing_ids.contains(id))
        .collect();

    // Show schema_migrations table state
    println!();
    if !table_exists.0 {
        println!("Table 'schema_migrations' does not exist and will be created.");
    } else if !existing_ids.is_empty() {
        println!("Table 'schema_migrations' already exists with {} record(s).", existing_ids.len());
    }
    println!("{} record(s) will be inserted into schema_migrations.", new_records.len());
    if !new_records.is_empty() && !existing_ids.is_empty() {
        let overlap = to_import.len() - new_records.len();
        if overlap > 0 {
            println!("{} record(s) already exist and will be skipped.", overlap);
        }
    }

    // Show unit list
    let label = if dry_run { "[DRY RUN] Would import" } else { "Will import" };
    println!("\n{} {} unit(s) as applied:\n", label, to_import.len());
    for (id, checksum) in &to_import {
        let marker = if existing_ids.contains(id) { " (already in ledger)" } else { "" };
        println!("  {} (checksum: {}...){}", id, &checksum[..8.min(checksum.len())], marker);
    }

    if !pending_ids.is_empty() {
        println!("\nPending (not yet applied to reference DB): {} unit(s)\n", pending_ids.len());
        for id in &pending_ids {
            println!("  {}", id);
        }
    }

    println!("\nSummary:");
    println!("  Applied:    {}", applied_ids.len());
    println!("  To insert:  {}", new_records.len());
    println!("  Pending:    {}", pending_ids.len());
    println!("  Total:      {}", ordered_ids.len());

    if dry_run {
        temp_pool.close().await;
        return Ok(());
    }

    // Confirm before writing
    if !yes {
        use std::io::{self, Write};
        print!("\nProceed? [y/N] ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            temp_pool.close().await;
            return Ok(());
        }
    }

    println!("\nImporting {} unit(s) into schema_migrations...\n", to_import.len());

    ledger::ensure_table(ref_pool).await?;
    let already = ledger::applied_units(ref_pool).await?;
    let mut imported = 0;
    let mut skipped = 0;

    for (id, checksum) in &to_import {
        if already.contains(id) {
            skipped += 1;
            continue;
        }
        ledger::record(ref_pool, id, checksum).await?;
        imported += 1;
    }

    println!(
        "Done. Imported: {}, Already in ledger: {}, Pending: {}",
        imported, skipped, pending_ids.len()
    );

    temp_pool.close().await;
    Ok(())
}

pub async fn diff(database_url: &str, root: &PathBuf, copy_schema_objects: bool) -> Result<()> {
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    let hooks_config = hooks::HooksConfig::load(root)?;
    if hooks_config.has_hooks() {
        println!("Loaded hooks from flugra.hooks.yaml");
    }

    println!("Discovered {} unit(s) in {}", units.len(), root.display());

    let mut ordered_ids: Vec<String> = units.keys().cloned().collect();
    ordered_ids.sort();

    let ref_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to reference database")?;

    let temp_db_name = format!("flugra_diff_{}", std::process::id());
    println!("Creating temporary database '{}'...", temp_db_name);

    sqlx::query(&format!("CREATE DATABASE \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await
        .with_context(|| format!("Failed to create temporary database '{}'", temp_db_name))?;

    let temp_url = replace_db_in_url(database_url, &temp_db_name)?;

    let result = apply_and_compare(&ref_pool, &temp_url, database_url, &units, &ordered_ids, copy_schema_objects, &hooks_config, root).await;

    println!("Dropping temporary database '{}'...", temp_db_name);
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await;

    result
}

async fn apply_and_compare(
    ref_pool: &sqlx::PgPool,
    temp_url: &str,
    ref_url: &str,
    units: &std::collections::BTreeMap<String, discovery::Unit>,
    ordered_ids: &[String],
    copy_schema_objects: bool,
    hooks_config: &hooks::HooksConfig,
    root: &PathBuf,
) -> Result<()> {
    let temp_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(temp_url)
        .await
        .context("Failed to connect to temporary database")?;

    // Copy extensions from reference database
    let extensions: Vec<(String,)> = sqlx::query_as(
        "SELECT extname FROM pg_extension WHERE extname != 'plpgsql' ORDER BY extname"
    )
    .fetch_all(ref_pool)
    .await
    .unwrap_or_default();

    if !extensions.is_empty() {
        println!("Installing extensions from reference database...");
        for (ext,) in &extensions {
            let sql = format!("CREATE EXTENSION IF NOT EXISTS \"{}\" CASCADE", ext);
            match sqlx::raw_sql(&sql).execute(&temp_pool).await {
                Ok(_) => println!("  Extension '{}' ... OK", ext),
                Err(e) => println!("  Extension '{}' ... FAILED ({})", ext, e),
            }
        }
    }

    if copy_schema_objects {
        println!("\nPre-copying functions from reference database...");
        copy_functions_from_ref(ref_url, temp_url).await?;
    }

    // Run pre_apply hooks
    let root_abs = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    hooks::run_hooks(&hooks_config.pre_apply, "pre_apply", temp_url, &root_abs)?;

    // Apply all migrations
    println!("\nApplying {} migration(s) to temporary database...", ordered_ids.len());

    let mut ok_count = 0usize;
    let mut fail_count = 0usize;
    let mut skip_count = 0usize;
    let mut failed_units: Vec<(String, String)> = Vec::new();

    for unit_id in ordered_ids {
        let unit = &units[unit_id];
        let sql = unit.read_sql()?;

        if sql.trim().is_empty() {
            skip_count += 1;
            continue;
        }

        match execute_migration_sql(&temp_pool, &sql).await {
            Ok(_) => ok_count += 1,
            Err(e) => {
                fail_count += 1;
                failed_units.push((unit_id.clone(), format!("{}", e)));
            }
        }
    }

    print!("  Progress: {}/{}", ordered_ids.len(), ordered_ids.len());
    if fail_count > 0 {
        print!(" ({} failed)", fail_count);
    }
    println!();

    if !failed_units.is_empty() {
        println!("\nFailed migrations:");
        for (unit_id, err) in &failed_units {
            println!("  {} -- {}", unit_id, err);
        }
    }

    println!(
        "\nMigration summary: {} OK, {} failed, {} skipped",
        ok_count, fail_count, skip_count
    );

    // Run post_apply hooks
    hooks::run_hooks(&hooks_config.post_apply, "post_apply", temp_url, &root_abs)?;

    // Copy functions after migrations if requested
    if copy_schema_objects {
        println!("\nCopying functions from reference database (post-migration)...");
        temp_pool.close().await;
        copy_functions_from_ref(ref_url, temp_url).await?;
        let temp_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(temp_url)
            .await?;

        let ref_schema = schema::dump_schema(ref_pool).await?;
        let temp_schema = schema::dump_schema(&temp_pool).await?;
        print_schema_comparison(&ref_schema, &temp_schema);
        temp_pool.close().await;
        return Ok(());
    }

    // Compare schemas
    println!("\nComparing schemas...");
    let ref_schema = schema::dump_schema(ref_pool).await?;
    let temp_schema = schema::dump_schema(&temp_pool).await?;
    print_schema_comparison(&ref_schema, &temp_schema);

    temp_pool.close().await;
    Ok(())
}

async fn copy_functions_from_ref(ref_url: &str, temp_url: &str) -> Result<()> {
    let ref_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(ref_url)
        .await?;
    let temp_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(temp_url)
        .await?;

    let functions: Vec<(String,)> = sqlx::query_as(
        "SELECT pg_get_functiondef(p.oid)
         FROM pg_proc p
         JOIN pg_namespace n ON p.pronamespace = n.oid
         WHERE n.nspname = 'public'
         ORDER BY p.proname"
    ).fetch_all(&ref_pool).await.unwrap_or_default();

    let mut func_count = 0;
    let mut func_errors = 0;
    for (def,) in &functions {
        let replace_def = def.replacen("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1);
        match sqlx::raw_sql(&replace_def).execute(&temp_pool).await {
            Ok(_) => func_count += 1,
            Err(_) => func_errors += 1,
        }
    }
    println!("  Functions: {} copied, {} failed", func_count, func_errors);

    ref_pool.close().await;
    temp_pool.close().await;
    Ok(())
}

/// Execute migration SQL, handling ALTER TYPE ADD VALUE specially.
async fn execute_migration_sql(pool: &sqlx::PgPool, sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();
    if upper.contains("ADD VALUE") && upper.contains("ALTER TYPE") {
        for stmt in split_sql_statements(sql) {
            let trimmed = stmt.trim();
            if trimmed.is_empty() || trimmed == ";" {
                continue;
            }
            sqlx::raw_sql(trimmed)
                .execute(pool)
                .await
                .with_context(|| {
                    let preview: String = trimmed.chars().take(80).collect();
                    format!("Failed at: {}...", preview)
                })?;
        }
        Ok(())
    } else {
        sqlx::raw_sql(sql)
            .execute(pool)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

/// Split SQL text into individual statements.
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut paren_depth = 0i32;
    let mut in_dollar_quote = false;
    let mut dollar_tag = String::new();

    while let Some(c) = chars.next() {
        current.push(c);

        if c == '$' && paren_depth == 0 {
            if in_dollar_quote {
                if current.ends_with(&dollar_tag) {
                    in_dollar_quote = false;
                    dollar_tag.clear();
                }
            } else {
                let before = &current[..current.len() - 1];
                if let Some(tag_start) = before.rfind('$') {
                    let tag = &before[tag_start..];
                    let inner = &tag[1..];
                    if inner.is_empty() || inner.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        in_dollar_quote = true;
                        dollar_tag = format!("{}$", tag);
                    }
                }
            }
        }

        if in_dollar_quote {
            continue;
        }

        if c == '-' && chars.peek() == Some(&'-') {
            current.push(chars.next().unwrap());
            while let Some(&nc) = chars.peek() {
                if nc == '\n' {
                    break;
                }
                current.push(chars.next().unwrap());
            }
            continue;
        }

        if c == '(' {
            paren_depth += 1;
        } else if c == ')' {
            paren_depth -= 1;
        } else if c == ';' && paren_depth <= 0 {
            statements.push(current.clone());
            current.clear();
        }
    }

    if !current.trim().is_empty() {
        statements.push(current);
    }

    statements
}

fn print_schema_comparison(ref_schema: &schema::SchemaSnapshot, temp_schema: &schema::SchemaSnapshot) {
    println!("\n  Schema Comparison:");
    println!("  {:<24} {:>8}  {:>8}", "", "Reference", "Migration");
    println!("  {:<24} {:>8}  {:>8}", "Tables", ref_schema.tables.len(), temp_schema.tables.len());
    println!("  {:<24} {:>8}  {:>8}", "Types", ref_schema.types.len(), temp_schema.types.len());
    println!("  {:<24} {:>8}  {:>8}", "Functions", ref_schema.functions.len(), temp_schema.functions.len());
    println!("  {:<24} {:>8}  {:>8}", "Views", ref_schema.views.len(), temp_schema.views.len());

    println!();
    let diff = temp_schema.diff(ref_schema);
    diff.display();
}

fn replace_db_in_url(url: &str, new_db: &str) -> Result<String> {
    if let Some(pos) = url.rfind('/') {
        let base = &url[..pos];
        let after_slash = &url[pos + 1..];
        if let Some(q_pos) = after_slash.find('?') {
            let params = &after_slash[q_pos..];
            Ok(format!("{}/{}{}", base, new_db, params))
        } else {
            Ok(format!("{}/{}", base, new_db))
        }
    } else {
        anyhow::bail!("Invalid database URL: {}", url);
    }
}
