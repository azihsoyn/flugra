use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::{discovery, executor, hooks, ledger, lock::LockFile, planner, schema};

#[derive(Parser)]
#[command(name = "flugra", about = "Dependency-aware execution manager for native SQL units")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Extract only "Up" section from migration files
    /// (for migration files with Up/Down sections)
    #[arg(long, global = true, default_value = "false")]
    pub extract_up: bool,
}

#[derive(Subcommand)]
pub enum Command {
    /// Discover units, build dependency graph, and show execution plan
    Plan {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,
    },

    /// Generate or update the lock file
    Lock {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,
    },

    /// Apply pending units to the database
    Apply {
        /// Root directory containing SQL units
        #[arg(default_value = ".")]
        root: PathBuf,

        /// Database connection URL (e.g., postgres://user:pass@localhost/db)
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },

    /// Show applied and pending units
    Status {
        /// Database connection URL
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },

    /// Compare database schema with migration result
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

        /// Copy functions, domains, and custom types from reference DB
        /// before applying migrations (for projects with externally managed functions)
        #[arg(long)]
        copy_schema_objects: bool,
    },

    /// Convert existing flat migration files to flugra native format
    ///
    /// Reads migration files with Up/Down sections, extracts Up sections,
    /// and creates directory-per-unit structure.
    Convert {
        /// Source directory containing flat migration files
        source: PathBuf,

        /// Output directory for flugra native format
        output: PathBuf,
    },
}

pub async fn plan(root: &PathBuf, extract_up: bool) -> Result<()> {
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    println!("Discovered {} unit(s):\n", units.len());

    let deps = planner::resolve_dependencies_with_options(&units, extract_up)?;
    planner::validate_no_cycles(&deps)?;
    let order = planner::execution_order(&deps)?;

    for (i, unit_id) in order.iter().enumerate() {
        let dep = &deps[unit_id];
        let unit = &units[unit_id];

        println!("  {}. {}", i + 1, unit_id);

        // Show SQL files
        for f in &unit.sql_files {
            println!("     - {}", f.file_name().unwrap_or_default().to_string_lossy());
        }

        // Show creates
        if !dep.creates.is_empty() {
            let tables: Vec<_> = dep.creates.iter().collect();
            println!("     creates: {}", tables.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
        }

        // Show dependencies
        if !dep.depends_on_units.is_empty() {
            println!("     depends on: {}", dep.depends_on_units.join(", "));
        }

        println!();
    }

    Ok(())
}

pub async fn lock(root: &PathBuf, extract_up: bool) -> Result<()> {
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    let deps = planner::resolve_dependencies_with_options(&units, extract_up)?;
    planner::validate_no_cycles(&deps)?;

    let lock = LockFile::from_units_with_options(&units, &deps, extract_up)?;
    lock.write(root)?;

    println!("Lock file written with {} unit(s)", lock.units.len());
    Ok(())
}

pub async fn apply(root: &PathBuf, database_url: &str, extract_up: bool) -> Result<()> {
    // Load hooks
    let hooks_config = hooks::HooksConfig::load(root)?;
    if hooks_config.has_hooks() {
        println!("Loaded hooks from flugra.hooks.yaml");
    }

    // Load and validate lock file
    let lock = LockFile::read(root)?;
    let units = discovery::discover(root)?;
    lock.validate_with_options(&units, extract_up)?;

    // Build dependency graph from lock file for ordering
    let deps = planner::resolve_dependencies_with_options(&units, extract_up)?;
    let order = planner::execution_order(&deps)?;

    // Collect checksums from lock
    let checksums: std::collections::BTreeMap<String, String> = lock
        .units
        .iter()
        .map(|(id, u)| (id.clone(), u.checksum.clone()))
        .collect();

    // Connect to database
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to database")?;

    // Run pre_apply hooks
    let root_abs = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    hooks::run_hooks(&hooks_config.pre_apply, "pre_apply", database_url, &root_abs)?;

    println!("\nApplying migrations...\n");
    let result = executor::apply_all(&pool, &units, &order, &checksums, extract_up).await?;

    // Run post_apply hooks
    hooks::run_hooks(&hooks_config.post_apply, "post_apply", database_url, &root_abs)?;

    println!(
        "\nDone. Applied: {}, Skipped (already applied): {}",
        result.applied, result.skipped
    );

    Ok(())
}

pub async fn status(database_url: &str) -> Result<()> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to database")?;

    ledger::ensure_table(&pool).await?;
    let applied = ledger::applied_units_detail(&pool).await?;

    if applied.is_empty() {
        println!("No units have been applied yet.");
    } else {
        println!("Applied units:\n");
        for unit in &applied {
            println!(
                "  {} (checksum: {}..., applied: {})",
                unit.unit_id,
                &unit.checksum[..8.min(unit.checksum.len())],
                unit.applied_at.format("%Y-%m-%d %H:%M:%S UTC")
            );
        }
    }

    Ok(())
}

pub async fn diff(database_url: &str, root: &PathBuf, extract_up: bool, copy_schema_objects: bool) -> Result<()> {
    let units = discovery::discover(root)?;

    if units.is_empty() {
        println!("No SQL units found in {}", root.display());
        return Ok(());
    }

    // Load hooks from migration root
    let hooks_config = hooks::HooksConfig::load(root)?;
    if hooks_config.has_hooks() {
        println!("Loaded hooks from flugra.hooks.yaml");
    }

    println!("Discovered {} unit(s) in {}", units.len(), root.display());

    // Sort units by ID (filename order = execution order for flat migrations)
    let mut ordered_ids: Vec<String> = units.keys().cloned().collect();
    ordered_ids.sort();

    // Connect to the reference database
    let ref_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .context("Failed to connect to reference database")?;

    // Create a temporary database for applying migrations
    let temp_db_name = format!("flugra_diff_{}", std::process::id());
    println!("Creating temporary database '{}'...", temp_db_name);

    sqlx::query(&format!("CREATE DATABASE \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await
        .with_context(|| format!("Failed to create temporary database '{}'", temp_db_name))?;

    // Build connection URL for temp database
    let temp_url = replace_db_in_url(database_url, &temp_db_name)?;

    let result = apply_and_compare(&ref_pool, &temp_url, database_url, &temp_db_name, &units, &ordered_ids, extract_up, copy_schema_objects, &hooks_config, root).await;

    // Clean up: drop temporary database
    println!("Dropping temporary database '{}'...", temp_db_name);
    // Close ref_pool connection to temp DB won't block drop
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", temp_db_name))
        .execute(&ref_pool)
        .await;

    result
}

async fn apply_and_compare(
    ref_pool: &sqlx::PgPool,
    temp_url: &str,
    ref_url: &str,
    _temp_db_name: &str,
    units: &std::collections::BTreeMap<String, discovery::Unit>,
    ordered_ids: &[String],
    extract_up: bool,
    copy_schema_objects: bool,
    hooks_config: &hooks::HooksConfig,
    root: &PathBuf,
) -> Result<()> {
    let temp_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(temp_url)
        .await
        .context("Failed to connect to temporary database")?;

    // Copy extensions from reference database to temp database
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

    // Copy functions from reference DB if requested (for externally managed functions)
    if copy_schema_objects {
        println!("\nPre-copying functions from reference database...");
        copy_functions_from_ref(ref_url, temp_url).await?;
    }

    // Run pre_apply hooks against the temporary database
    let root_abs = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    hooks::run_hooks(&hooks_config.pre_apply, "pre_apply", temp_url, &root_abs)?;

    // Apply all migrations to temp database
    println!("\nApplying {} migration(s) to temporary database...", ordered_ids.len());

    let mut ok_count = 0usize;
    let mut fail_count = 0usize;
    let mut skip_count = 0usize;
    let mut failed_units: Vec<(String, String)> = Vec::new();

    for (i, unit_id) in ordered_ids.iter().enumerate() {
        let unit = &units[unit_id];
        let sql = unit.read_sql_with_options(extract_up)?;

        if sql.trim().is_empty() {
            skip_count += 1;
            continue;
        }

        match execute_migration_sql(&temp_pool, &sql).await {
            Ok(_) => {
                ok_count += 1;
            }
            Err(e) => {
                fail_count += 1;
                let err_msg = format!("{}", e);
                failed_units.push((unit_id.clone(), err_msg));
            }
        }

    }

    // Print progress as a single line
    print!("  Progress: {}/{}", ordered_ids.len(), ordered_ids.len());
    if fail_count > 0 {
        print!(" ({} failed)", fail_count);
    }
    println!();

    // Show failed migrations
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

    // Run post_apply hooks against the temporary database
    hooks::run_hooks(&hooks_config.post_apply, "post_apply", temp_url, &root_abs)?;

    // Copy functions after migrations (they depend on types created by migrations)
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

    // Dump and compare schemas
    println!("\nComparing schemas...");
    let ref_schema = schema::dump_schema(ref_pool).await?;
    let temp_schema = schema::dump_schema(&temp_pool).await?;
    print_schema_comparison(&ref_schema, &temp_schema);

    // Disconnect from temp DB before dropping
    temp_pool.close().await;

    Ok(())
}

/// Copy types, domains, casts from reference DB (pre-migration).
/// Functions are copied separately after migrations to avoid dependency conflicts.
async fn copy_types_from_ref(ref_url: &str, temp_url: &str) -> Result<()> {
    let ref_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(ref_url)
        .await?;
    let temp_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(temp_url)
        .await?;

    // 1. Copy enum types
    let enums: Vec<(String,)> = sqlx::query_as(
        "SELECT t.typname FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
         WHERE n.nspname = 'public' AND t.typtype = 'e'
         ORDER BY t.typname"
    ).fetch_all(&ref_pool).await.unwrap_or_default();

    let mut enum_count = 0;
    for (name,) in &enums {
        let labels: Vec<(String,)> = sqlx::query_as(
            "SELECT e.enumlabel FROM pg_enum e
             JOIN pg_type t ON e.enumtypid = t.oid
             JOIN pg_namespace n ON t.typnamespace = n.oid
             WHERE n.nspname = 'public' AND t.typname = $1
             ORDER BY e.enumsortorder"
        ).bind(name).fetch_all(&ref_pool).await.unwrap_or_default();

        let label_list: Vec<String> = labels.iter().map(|(l,)| format!("'{}'", l.replace('\'', "''"))).collect();
        let sql = format!("CREATE TYPE \"{}\" AS ENUM ({})", name, label_list.join(", "));
        if sqlx::raw_sql(&sql).execute(&temp_pool).await.is_ok() {
            enum_count += 1;
        }
    }
    println!("  Enums: {} copied", enum_count);

    // 2. Copy domain types
    let domain_defs: Vec<(String,)> = sqlx::query_as(
        "SELECT format('CREATE DOMAIN %I AS %s %s',
                t.typname,
                format_type(t.typbasetype, t.typtypmod),
                COALESCE((SELECT string_agg(pg_get_constraintdef(c.oid), ' ') FROM pg_constraint c WHERE c.contypid = t.oid), ''))
         FROM pg_type t
         JOIN pg_namespace n ON t.typnamespace = n.oid
         WHERE n.nspname = 'public' AND t.typtype = 'd'
         ORDER BY t.typname"
    ).fetch_all(&ref_pool).await.unwrap_or_default();

    let mut domain_count = 0;
    for (def,) in &domain_defs {
        if sqlx::raw_sql(def).execute(&temp_pool).await.is_ok() {
            domain_count += 1;
        }
    }
    println!("  Domains: {} copied", domain_count);

    // 3. Copy casts
    let casts: Vec<(String,)> = sqlx::query_as(
        "SELECT format('CREATE CAST (%s AS %s) WITH INOUT AS IMPLICIT',
                format_type(c.castsource, NULL),
                format_type(c.casttarget, NULL))
         FROM pg_cast c
         JOIN pg_type st ON c.castsource = st.oid
         JOIN pg_type tt ON c.casttarget = tt.oid
         WHERE (EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname = 'public' AND (st.typnamespace = n.oid OR tt.typnamespace = n.oid)))
           AND c.castmethod = 'i'
         ORDER BY 1"
    ).fetch_all(&ref_pool).await.unwrap_or_default();

    let mut cast_count = 0;
    for (def,) in &casts {
        if sqlx::raw_sql(def).execute(&temp_pool).await.is_ok() {
            cast_count += 1;
        }
    }
    println!("  Casts: {} copied", cast_count);

    ref_pool.close().await;
    temp_pool.close().await;

    Ok(())
}

/// Copy functions from reference DB (post-migration).
/// Run after migrations so that types/domains exist for function signatures.
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
        // Use CREATE OR REPLACE to handle functions already created by migrations
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

pub async fn convert(source: &PathBuf, output: &PathBuf) -> Result<()> {
    use crate::migration_parser;

    // Collect source SQL files
    let mut sql_files: Vec<std::path::PathBuf> = Vec::new();
    for entry in std::fs::read_dir(source)
        .with_context(|| format!("Cannot read source directory: {}", source.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().map_or(false, |ext| ext == "sql") {
            sql_files.push(path);
        }
    }
    sql_files.sort();

    if sql_files.is_empty() {
        println!("No SQL files found in {}", source.display());
        return Ok(());
    }

    println!("Found {} migration file(s)", sql_files.len());

    // Create output directory
    std::fs::create_dir_all(output)
        .with_context(|| format!("Cannot create output directory: {}", output.display()))?;

    let mut converted = 0;
    let mut skipped = 0;

    for file in &sql_files {
        let filename = file.file_name().unwrap_or_default().to_string_lossy();
        let raw = std::fs::read_to_string(file)
            .with_context(|| format!("Failed to read {}", file.display()))?;

        let up_sql = migration_parser::extract_up_section(&raw);

        if up_sql.trim().is_empty() {
            println!("  Skipping (empty Up section): {}", filename);
            skipped += 1;
            continue;
        }

        // Derive unit name from filename
        // e.g., "20201101000001-initialize.sql" → "20201101000001-initialize"
        let unit_name = filename
            .strip_suffix(".sql")
            .unwrap_or(&filename)
            .to_string();

        // Create unit directory
        let unit_dir = output.join(&unit_name);
        std::fs::create_dir_all(&unit_dir)
            .with_context(|| format!("Cannot create unit directory: {}", unit_dir.display()))?;

        // Write the Up SQL as 001.sql
        let target_file = unit_dir.join("001.sql");
        std::fs::write(&target_file, &up_sql)
            .with_context(|| format!("Failed to write {}", target_file.display()))?;

        converted += 1;
    }

    println!(
        "\nConverted {} unit(s), skipped {} (output: {})",
        converted,
        skipped,
        output.display()
    );

    // Generate lock file
    println!("\nGenerating lock file...");
    let units = discovery::discover(output)?;
    let deps = planner::resolve_dependencies_with_options(&units, false)?;
    planner::validate_no_cycles(&deps)?;
    let order = planner::execution_order(&deps)?;

    let lock = LockFile::from_units_with_options(&units, &deps, false)?;
    lock.write(output)?;

    println!("Lock file written with {} unit(s)", lock.units.len());

    // Show execution plan summary
    println!("\nExecution order ({} units):", order.len());
    for (i, unit_id) in order.iter().enumerate() {
        let dep = &deps[unit_id];
        if dep.depends_on_units.is_empty() {
            println!("  {}. {}", i + 1, unit_id);
        } else {
            println!(
                "  {}. {} (depends on: {})",
                i + 1,
                unit_id,
                dep.depends_on_units.join(", ")
            );
        }
    }

    Ok(())
}

/// Execute migration SQL, handling ALTER TYPE ADD VALUE specially.
///
/// PostgreSQL's ALTER TYPE ADD VALUE cannot be used in the same transaction
/// as statements that reference the new value. When this pattern is detected,
/// we split the SQL into individual statements and execute each one separately
/// (autocommit mode).
async fn execute_migration_sql(pool: &sqlx::PgPool, sql: &str) -> Result<()> {
    let upper = sql.to_uppercase();
    if upper.contains("ADD VALUE") && upper.contains("ALTER TYPE") {
        // Split into statements and execute individually
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

/// Split SQL text into individual statements, respecting dollar-quoted strings
/// and parenthesized expressions.
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut paren_depth = 0i32;
    let mut in_dollar_quote = false;
    let mut dollar_tag = String::new();

    while let Some(c) = chars.next() {
        current.push(c);

        // Handle dollar-quoted strings: $$ ... $$ or $tag$ ... $tag$
        if c == '$' && paren_depth == 0 {
            if in_dollar_quote {
                // Check if this closes the dollar quote
                if current.ends_with(&dollar_tag) {
                    in_dollar_quote = false;
                    dollar_tag.clear();
                }
            } else {
                // Try to find opening dollar quote tag
                let before = &current[..current.len() - 1];
                if let Some(tag_start) = before.rfind('$') {
                    let tag = &before[tag_start..];
                    // Validate tag (alphanumeric + underscore only between $...$)
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

        // Handle single-line comments
        if c == '-' && chars.peek() == Some(&'-') {
            // Consume until end of line
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

/// Replace the database name in a PostgreSQL connection URL.
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
