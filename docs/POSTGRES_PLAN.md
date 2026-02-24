# Postgres upgrade plan (approved)

## Current status (this repo)
The MVP uses **SQLite (WAL)** today. This is what is tested and supported in code.

## Why Postgres
If you run many workers in parallel (especially on a busy VPS), SQLite can become a bottleneck due to write locks.

## Recommended migration (production)
1) Add Postgres (docker-compose or system package).
2) Replace the SQLite DB layer with a Postgres-backed DB layer (schema + queries). This requires:
   - schema migration (types/serial/constraints)
   - switching parameter style (SQLite `?` vs Postgres `%s`)
   - re-validating all worker cycles under concurrency
3) Cutover:
   - stop workers
   - migrate data (or start with empty DB)
   - start API + workers

## What is included here
- `deploy/docker-compose.postgres.yml` (Postgres container)
- This doc (steps + constraints)

## What is NOT included here
- A Postgres DB implementation in code.
- Automatic data migration from existing SQLite.
