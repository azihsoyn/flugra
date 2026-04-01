# flugra

**flu**ent mi**gra**tion — dependency-aware execution manager for native SQL units.

## Design Principles

- Native SQL only (no DSL)
- Transaction unit as the primary abstraction
- Execution order derived from dependency graph (not global versioning)
- Conflict-free design (no global sequence numbers)
- Git is NOT the source of truth (filesystem is)
- Execution must be deterministic
- Humans must be able to review execution plans

## Architecture

### Execution Unit
- A unit is a directory = transaction boundary
- `.sql` files within a unit are executed in filename order
- Leaf directories (no child directories containing `.sql` files) become units
- Flat directories (SQL files only, no subdirectories) are auto-detected; each file becomes its own unit

### Dependency Resolution
- Table-level dependencies inferred via lightweight SQL heuristics
- `CREATE TABLE` → creates; `ALTER/INSERT/UPDATE/DELETE/JOIN/REFERENCES` → references
- Duplicate CREATE of the same table: last writer wins (supports DROP → CREATE pattern)
- Execution order: topological sort + lexical fallback

### Lock File (`flugra.lock`)
- YAML format, map structure (not list) → merge-friendly
- Stores checksum + depends_on
- Final execution order is NOT stored (derived dynamically)

### Ledger Table (`schema_migrations`)
- Tracks applied units in PostgreSQL
- unit_id, checksum, applied_at

## Modules

| Module | Role |
|---|---|
| `discovery` | Directory scanning, leaf unit detection, checksum computation, flat directory auto-detection |
| `parser` | SQL analysis (table creates/references extraction) |
| `planner` | Dependency graph construction, topological sort, cycle detection |
| `lock` | `flugra.lock` generation, reading, validation |
| `executor` | Unit execution (single transaction, raw_sql) |
| `ledger` | `schema_migrations` table management |
| `migration_parser` | Up/Down section extraction from existing migration files |
| `hooks` | Lifecycle hooks (pre_apply, post_apply) |
| `schema` | DB schema dump & comparison (tables, columns, types, constraints, indexes, views, functions) |
| `cli` | CLI command definitions (plan, lock, apply, status, diff, convert) |

## CLI Commands

- `flugra plan <root>` — discover units, dependency graph, execution order
- `flugra lock <root>` — generate/update lock file
- `flugra apply <root> --database-url <url>` — validate lock, execute unapplied units (hooks supported)
- `flugra status --database-url <url>` — show applied/pending units
- `flugra diff <root> --database-url <url>` — apply migrations to temp DB and compare schema with reference DB (hooks supported)
- `flugra convert <source> <output>` — convert flat migration files to flugra native directory-per-unit format
- `--extract-up` (global) — extract only Up section from migration files with Up/Down format

## Hooks (`flugra.hooks.yaml`)

Placed in the migration root. Runs shell commands at `pre_apply` / `post_apply`.
`DATABASE_URL` environment variable is set to the target DB (temp DB when using `diff`).

```yaml
pre_apply:
  - command: "psql \"$DATABASE_URL\" -f ./plv8-functions.sql -q"
    description: "Deploy plv8 functions"
post_apply:
  - command: "./scripts/post-migrate.sh"
```

## Build & Test

```sh
cargo build
cargo test
```

## Tech Stack

- Rust (edition 2021)
- clap 4 (CLI, env feature enabled)
- sqlx 0.8 (PostgreSQL, chrono feature enabled, raw_sql)
- serde + serde_yaml (lock file)
- sha2 + hex (checksum)
- chrono (timestamps)
- BTreeMap used throughout (deterministic ordering)

## Technical Notes

- SQL containing ALTER TYPE ADD VALUE is automatically split into per-statement execution (PostgreSQL transaction constraint)
- Executor: normally runs raw_sql within a transaction; falls back to autocommit mode when ALTER TYPE ADD VALUE is detected
