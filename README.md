# flugra

**flu**ent mi**gra**tion — A dependency-aware execution manager for native SQL units.

## What is flugra?

flugra replaces traditional migration systems with:

- **Native SQL only** — no DSL, no code generation
- **Dependency-based execution** — order derived from SQL analysis, not global sequence numbers
- **Transaction units** — each unit (directory) is a transaction boundary
- **Conflict-free workflow** — no global version numbers that cause merge conflicts
- **Human-reviewable plans** — inspect execution order before applying

## Quick Start

```sh
# Build
cargo build --release

# Show execution plan
flugra plan ./migrations

# Generate lock file
flugra lock ./migrations

# Apply to database
flugra apply ./migrations --database-url postgres://user:pass@localhost/mydb

# Check status
flugra status --database-url postgres://user:pass@localhost/mydb
```

## Concepts

### Execution Unit

A **unit** is a directory containing `.sql` files. It represents a transaction boundary — all SQL files in the directory are executed in filename order within a single transaction.

```
migrations/
  users/create/
    001_create_table.sql
    002_add_index.sql
  orders/create/
    001_create_table.sql
  orders/add-user-ref/
    001_add_foreign_key.sql
```

### Discovery Rules

- Recursively scan a root directory
- **Leaf directories** (containing `.sql` files with no child directories that also have `.sql` files) become units
- **Flat directories** (a single directory with multiple `.sql` files) are auto-detected — each file becomes its own unit

### Dependency Resolution

Dependencies are inferred by analyzing SQL:

| SQL Pattern | Effect |
|---|---|
| `CREATE TABLE users` | Unit **produces** `users` |
| `INSERT INTO users` | Unit **depends on** `users` |
| `ALTER TABLE users` | Unit **depends on** `users` |
| `REFERENCES users` | Unit **depends on** `users` |
| `FROM users` / `JOIN users` | Unit **depends on** `users` |

### Execution Order

1. Build dependency graph (topological sort)
2. Break ties with **lexical order** (deterministic)
3. Validate: no circular dependencies

### Lock File (`flugra.lock`)

```yaml
version: 1
units:
  users/create:
    checksum: abc123...
    depends_on: []
  orders/add-user-ref:
    checksum: def456...
    depends_on:
      - users/create
      - orders/create
```

- Stores dependency graph as a **map** (not ordered list) — merge-friendly
- Execution order is derived dynamically, not stored
- Checksums ensure filesystem matches lock file

### Ledger Table

Applied units are tracked in PostgreSQL:

```sql
CREATE TABLE schema_migrations (
  unit_id TEXT PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## CLI Commands

### `flugra plan [root]`

Discover units, build dependency graph, and display execution order.

```sh
flugra plan ./migrations
```

### `flugra lock [root]`

Generate or update `flugra.lock` with checksums and dependencies.

```sh
flugra lock ./migrations
```

### `flugra apply [root] --database-url <url>`

Validate lock file, then execute unapplied units in dependency order.

```sh
flugra apply ./migrations --database-url postgres://localhost/mydb
```

### `flugra status --database-url <url>`

Show applied and pending units.

```sh
flugra status --database-url postgres://localhost/mydb
```

### `flugra diff [root] --database-url <url>`

Compare a reference database schema against the result of applying migrations to a fresh temporary database.

```sh
flugra diff ./migrations --database-url postgres://localhost/mydb
```

Options:
- `--copy-schema-objects` — Copy functions/types from reference DB (for projects with externally managed functions)

### `flugra convert <source> <output>`

Convert existing flat migration files to flugra native directory-per-unit format.

```sh
flugra convert ./old-migrations ./flugra-migrations
```

This extracts "Up" sections, creates one directory per migration, and generates a lock file with inferred dependencies.

## Hooks

flugra supports lifecycle hooks via `flugra.hooks.yaml` placed in the migration root directory.

```yaml
pre_apply:
  - command: "psql \"$DATABASE_URL\" -f ./seed-functions.sql -q"
    description: "Deploy plv8 functions"
  - command: "./scripts/setup.sh"
    workdir: "/path/to/project"

post_apply:
  - command: "./scripts/post-migrate.sh"
    description: "Run post-migration tasks"
```

Hooks receive `DATABASE_URL` as an environment variable pointing to the target database. This works with both `apply` and `diff` commands — when using `diff`, hooks run against the temporary database.

### Use Case: Externally Managed Functions

For projects where plv8 or other functions are deployed outside of SQL migrations (e.g., transpiled from TypeScript):

```yaml
pre_apply:
  - command: "psql \"$DATABASE_URL\" -f ./plv8-functions.sql -q"
    description: "Deploy plv8 functions before migrations"
```

## Global Options

### `--extract-up`

Extract only the "Up" section from migration files. Supports:
- `-- +migrate Up` / `-- +migrate Down`
- `-- Up Migration` / `-- Down Migration`

```sh
flugra --extract-up plan ./migrations
flugra --extract-up diff --database-url postgres://localhost/mydb ./migrations
```

## Building

```sh
cargo build
cargo test
```

## Architecture

| Module | Role |
|---|---|
| `discovery` | Directory scanning, leaf unit detection, checksum computation |
| `parser` | Lightweight SQL analysis (table creates/references) |
| `planner` | Dependency graph, topological sort, cycle detection |
| `lock` | `flugra.lock` generation, reading, validation |
| `executor` | Unit execution within single transactions |
| `ledger` | `schema_migrations` table management |
| `hooks` | Lifecycle hooks (`pre_apply`, `post_apply`) |
| `migration_parser` | Up/Down section extraction from existing migration files |
| `schema` | Database schema dumping and comparison |
| `cli` | CLI command definitions and handlers |
