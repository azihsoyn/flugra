# flugra

```
         +----------------------------+
        /- - - - - - - - - - - - - - - \
       /                                \
      |                                  |
      |    ___ _                         |
      |   |  _| |_  _  __ _ _ __ __ _    |
      |   | |_| | || |/ _` | '__/ _` |   |
      |   |  _| | || | (_| | | | (_| |   |
      |   |_| |_|\_,_|\__, |_|  \__,_|   |
      |                |___/             |
      |       ~ fluent migration ~       |
      |                                  |
      |  +----------------------------+  |
      |  | CREATE TABLE ~~~~~~~~~~~~~ |  |
      |  | ALTER TABLE  ~~~~~~~~~~~~~ |  |
      |  | INSERT INTO  ~~~~~~~~~~~~~ |  |
      |  | CREATE INDEX ~~~~~~~~~~~~~ |  |
      |  +----------------------------+  |
      |      ~ native SQL units ~        |
      +==================================+
```

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

# Show pending units and execution plan
flugra plan --database-url postgres://user:pass@localhost/mydb ./migrations

# Apply pending units
flugra apply --database-url postgres://user:pass@localhost/mydb ./migrations
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

### Up/Down Section Handling

Migration files with `-- +migrate Up` / `-- +migrate Down` or `-- Up Migration` / `-- Down Migration` markers are automatically handled — only the "Up" section is used. No flags needed.

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

### `flugra plan [root] --database-url <url>`

Show pending units and execution plan. Connects to the database to check which units have already been applied.

```sh
flugra plan --database-url postgres://localhost/mydb ./migrations
```

### `flugra apply [root] --database-url <url>`

Apply pending units in dependency order.

```sh
flugra apply --database-url postgres://localhost/mydb ./migrations
```

### `flugra import [root] --database-url <url>`

Import existing migration state into flugra's ledger. Determines which units have already been applied by schema comparison — no dependency on the previous migration tool's tracking table.

How it works:
1. Snapshots the reference database schema
2. Applies all migrations to a temporary database
3. Compares the result with the reference schema
4. Objects only in the migration result (not in reference DB) indicate pending migrations
5. All units before the first pending unit are marked as applied

```sh
# Preview what would be imported
flugra import --dry-run --database-url postgres://localhost/mydb ./migrations

# Import (with confirmation prompt)
flugra import --database-url postgres://localhost/mydb ./migrations

# Import without confirmation
flugra import -y --database-url postgres://localhost/mydb ./migrations
```

### `flugra diff [root] --database-url <url>`

Verify migrations by applying them to a temporary database and comparing the resulting schema against the reference database.

```sh
flugra diff --database-url postgres://localhost/mydb ./migrations
```

Options:
- `--copy-schema-objects` — Copy functions from reference DB before applying (for projects with externally managed functions)

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

Hooks receive `DATABASE_URL` as an environment variable pointing to the target database. This works with `apply`, `import`, and `diff` — when using `diff` or `import`, hooks run against the temporary database.

### Use Case: Externally Managed Functions

For projects where plv8 or other functions are deployed outside of SQL migrations (e.g., transpiled from TypeScript):

```yaml
pre_apply:
  - command: "psql \"$DATABASE_URL\" -f ./plv8-functions.sql -q"
    description: "Deploy plv8 functions before migrations"
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
| `executor` | Unit execution within single transactions |
| `ledger` | `schema_migrations` table management |
| `hooks` | Lifecycle hooks (`pre_apply`, `post_apply`) |
| `migration_parser` | Up/Down section extraction from migration files |
| `schema` | Database schema dumping and comparison |
| `cli` | CLI command definitions and handlers |
