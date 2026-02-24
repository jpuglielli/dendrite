# Dendrite

Selective Postgres branching tool. Copy schema and filtered data from a production (or staging) database into lightweight local databases for development and testing.

## How it works

Dendrite connects to a **source** Postgres database (via `.env`), spins up a **destination** Postgres via Docker Compose, and selectively copies schema + data using `pg_dump`/`pg_restore` and `COPY` pipelines.

```
┌──────────────────────┐         pg_dump / COPY TO
│   Source Database     │────────────────────────────────┐
│   (production/stage)  │                                │
└──────────────────────┘                                 │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  pg_restore / COPY  │
                                              │      FROM STDIN     │
                                              └────────┬────────────┘
                                                       │
                           ┌───────────────────────────┬┴──────────────────────────┐
                           ▼                           ▼                           ▼
                 ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
                 │  dendrite_dev    │       │  sandbox_a1b2... │       │  sandbox_c3d4... │
                 │  (default dst)   │       │  (isolated)      │       │  (isolated)      │
                 └──────────────────┘       └──────────────────┘       └──────────────────┘
                           └───────────────────────────┬───────────────────────────┘
                                                       │
                                              ┌────────┴────────┐
                                              │  Compose Postgres│
                                              │  localhost:15432 │
                                              │  (pgdata volume) │
                                              └─────────────────┘
```

## Sandboxes

Sandboxes are fully isolated databases (own `DATABASE` + `ROLE`) inside the shared Compose Postgres instance. Create, use, and destroy them without affecting anything else.

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

## Project structure

```
dendrite/
├── cmd/dendrite/          CLI (Cobra commands)
│   ├── main.go            root command, DSN helpers
│   ├── branch.go          branch — raw QUERY:TABLE branching
│   ├── webom.go           webom — branch WEBOM data by program ID
│   ├── sandbox.go         sandbox create/list/destroy
│   ├── schema.go          copy-schema — schema-only copy
│   └── temp.go            temp — branch with per-step timing
├── internal/
│   ├── postgres/
│   │   ├── conn.go        source DB connection (from .env)
│   │   └── copy.go        CopySchema, CopyData, Branch
│   ├── contrib/
│   │   └── webom.go       WEBOMSpecs — FK-ordered copy specs
│   └── sandbox/
│       ├── manager.go     Manager — sandbox lifecycle
│       └── manager_test.go
├── compose.yaml           Postgres 17 (pgdata volume)
├── justfile               task runner
├── Dockerfile             multi-stage build
└── .env                   source DB credentials (not committed)
```

## Prerequisites

- Go 1.25+
- Docker & Docker Compose
- `pg_dump` / `pg_restore` / `psql` (Postgres client tools)
- [just](https://github.com/casey/just) (task runner)
- [golangci-lint](https://golangci-lint.run/) (for linting)

## Setup

1. Copy `.env.example` to `.env` and fill in your source database credentials:
   ```
   DB_USER=...
   DB_PASSWORD=...
   DB_HOST=...
   DB_PORT=...
   DB_NAME=...
   ```

2. Start the local Postgres:
   ```
   just up
   ```

## Usage

### Branch (raw)

Copy schema + filtered data using `QUERY:TABLE` specs:

```sh
just run branch "SELECT * FROM users WHERE id < 100:users"
```

### WEBOM

Branch schema + WEBOM data for a program:

```sh
just run webom 4
```

### Sandboxes

```sh
# Create an isolated sandbox
just run sandbox create --program-id 4

# List active sandboxes
just run sandbox list

# Connect to a sandbox
psql "<DSN from create output>"

# Destroy when done
just run sandbox destroy <id>
```

## Just recipes

| Recipe   | Description                              |
|----------|------------------------------------------|
| `just up`   | Start Compose Postgres                |
| `just down` | Stop containers (keep data volume)    |
| `just nuke` | Stop containers and delete all data   |
| `just run`  | Run any dendrite subcommand           |
| `just lint` | Run golangci-lint                     |

## Testing

```sh
# Unit + integration tests (integration tests require `just up`)
go test ./...
```

Integration tests auto-skip when the Compose Postgres is not reachable.
