# Dendrite

Selective Postgres branching tool. Copy schema and filtered data from any source Postgres database into any target — a local dev instance, a staging server, a CI ephemeral database, or an isolated sandbox.

## How it works

Dendrite pipes `pg_dump --schema-only` into `pg_restore` for structure, then streams filtered rows via `COPY TO` / `COPY FROM` between any two Postgres databases. No intermediate files, no full dumps — just the tables and rows you specify.

```
┌─────────────────────┐                          ┌─────────────────────┐
│   Source Database    │                          │   Target Database   │
│   (any Postgres)     │                          │   (any Postgres)    │
│                     │    pg_dump ──► pg_restore │                     │
│  production         │──────────────────────────►│  local dev          │
│  staging            │                          │  staging             │
│  data warehouse     │   COPY TO ──► COPY FROM  │  CI ephemeral       │
│  ...                │──────────────────────────►│  sandbox            │
│                     │   (filtered rows only)    │  ...                │
└─────────────────────┘                          └─────────────────────┘
```

Source and target are configured independently — any Postgres you can connect to works for either side.

## Sandboxes

Sandboxes create fully isolated databases (own `DATABASE` + `ROLE`) inside a shared Postgres instance. Useful when you want multiple independent environments on the same server without managing separate Postgres clusters.

```
  dendrite sandbox create --program-id 4
           │
           ▼
  ┌────────────────────────────────────────┐
  │  1. CREATE ROLE "sandbox_<id>"         │
  │  2. CREATE DATABASE "sandbox_<id>"     │
  │  3. REVOKE CONNECT FROM PUBLIC         │
  │  4. pg_dump schema ──► pg_restore      │
  │  5. COPY filtered data per spec        │
  │  6. Install change-tracking triggers   │
  │  7. Write branch metadata              │
  └────────────────────────────────────────┘
           │
           ▼
  Sandbox ready ── use the printed DSN
           │
  dendrite sandbox destroy <id>
           │
           ▼
  ┌────────────────────────────────────────┐
  │  1. pg_terminate_backend (connections) │
  │  2. DROP DATABASE IF EXISTS            │
  │  3. DROP ROLE IF EXISTS                │
  └────────────────────────────────────────┘
```

The target Postgres for sandboxes is configured via `DST_*` env vars (or `--dst` flag) — it doesn't have to be the included Compose instance.

### Diff & merge

Sandboxes automatically track all changes (inserts, updates, deletes) via Postgres triggers installed at creation time. You can diff those changes against the source and merge them back:

```
  dendrite sandbox diff <id>
           │
           ▼
  ┌────────────────────────────────────────┐
  │  1. Read collapsed changeset from      │
  │     sandbox (_dendrite_changes)        │
  │  2. Compare each changed row against   │
  │     current source state               │
  │  3. Report changes + conflicts         │
  └────────────────────────────────────────┘

  dendrite sandbox merge <id>
           │
           ▼
  ┌────────────────────────────────────────┐
  │  1. Compute diff (same as above)       │
  │  2. Abort if conflicts (unless --force)│
  │  3. Apply in REPEATABLE READ tx:       │
  │     - DELETEs (reverse FK order)       │
  │     - INSERTs (FK order)               │
  │     - UPDATEs (FK order)               │
  │  4. Retry on serialization failure     │
  └────────────────────────────────────────┘
```

**Conflict detection:** If the source row changed since branching, the merge aborts by default. Use `--force` to skip conflicted rows, or `--dry-run` to preview the SQL without executing.

## Project structure

```
dendrite/
├── cmd/dendrite/          CLI (Cobra commands)
│   ├── main.go            root command, DSN helpers
│   ├── branch.go          branch — raw QUERY:TABLE branching
│   ├── webom.go           webom — branch WEBOM data by program ID
│   ├── sandbox.go         sandbox create/list/destroy/diff/merge
│   ├── merge.go           sandbox diff + merge subcommands
│   ├── schema.go          copy-schema — schema-only copy
│   └── temp.go            temp — branch with per-step timing
├── internal/
│   ├── postgres/
│   │   ├── conn.go        source DB connection (from .env)
│   │   └── copy.go        CopySchema, CopyData, Branch, TableNames
│   ├── contrib/
│   │   └── webom.go       WEBOMSpecs — FK-ordered copy specs
│   ├── sandbox/
│   │   ├── manager.go     Manager — sandbox lifecycle + ConnectSandbox
│   │   └── manager_test.go
│   ├── tracker/
│   │   ├── tracker.go     Install triggers, ReadChanges (with collapsing)
│   │   ├── pk.go          DiscoverPKs — PK column discovery
│   │   └── meta.go        WriteMeta/ReadMeta — branch metadata
│   └── merge/
│       └── merge.go       Diff, Merge, GenerateSQL
├── compose.yaml           optional local Postgres
├── justfile               task runner
├── Dockerfile             multi-stage build
└── .env                   source DB credentials (not committed)
```

## Prerequisites

- Go 1.25+
- `pg_dump` / `pg_restore` / `psql` (Postgres client tools)
- [just](https://github.com/casey/just) (task runner, optional)
- [golangci-lint](https://golangci-lint.run/) (for linting, optional)
- Docker & Docker Compose (only if using the included local Postgres)

## Configuration

### Source database

Set via `.env` file (or environment variables):

```
DB_USER=...
DB_PASSWORD=...
DB_HOST=...
DB_PORT=...
DB_NAME=...
```

### Target database

Set via `--dst` flag or `DST_*` environment variables:

```
DST_USER=...
DST_PASSWORD=...
DST_HOST=...
DST_PORT=...
DST_NAME=...
```

If unset, defaults to `devuser:devpass@localhost:15432/dendrite_dev` (the included Compose Postgres).

A `compose.yaml` is included for convenience, but Dendrite works against any Postgres you point it at.

## Usage

### Branch (raw)

Copy schema + filtered data using `QUERY:TABLE` specs:

```sh
# Any source → any target
dendrite branch --dst "postgres://user:pass@target:5432/mydb" \
  "SELECT * FROM users WHERE org_id = 42:users" \
  "SELECT * FROM orders WHERE org_id = 42:orders"
```

### Schema only

```sh
dendrite copy-schema --dst "postgres://user:pass@target:5432/mydb"
```

### Sandboxes

```sh
# Create an isolated sandbox (branched from source)
dendrite sandbox create --program-id 4

# List active sandboxes
dendrite sandbox list

# Connect to a sandbox and make changes
psql "<DSN from create output>"

# See what changed (summary or SQL)
dendrite sandbox diff <id>
dendrite sandbox diff <id> --format sql

# Preview merge without applying
dendrite sandbox merge <id> --dry-run

# Merge changes back into source
dendrite sandbox merge <id>

# Merge, skipping conflicted rows
dendrite sandbox merge <id> --force

# Destroy when done
dendrite sandbox destroy <id>
```

### Local dev (with Compose)

```sh
just up                              # start local Postgres
just run webom 4                     # branch WEBOM data into it
just run sandbox create --program-id 4  # or create a sandbox
just down                            # stop (keeps data)
just nuke                            # stop + delete all data
```

## Just recipes

| Recipe     | Description                            |
|------------|----------------------------------------|
| `just up`  | Start Compose Postgres                 |
| `just down`| Stop containers (keep data volume)     |
| `just nuke`| Stop containers and delete all data    |
| `just run` | Run any dendrite subcommand            |
| `just lint`| Run golangci-lint                      |

## Testing

```sh
go test ./...
```

Integration tests auto-skip when Postgres is not reachable.
