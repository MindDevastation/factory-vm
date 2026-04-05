# Epic-5 schema provisioning contract (repo-grounded)

## Provisioning model
Project uses **inline migration via `dbm.migrate(conn)`** (no separate migration framework/scripts).

- DB connect opens SQLite and configures pragmas, but does not apply schema by itself.
- Schema is materialized by explicit `dbm.migrate(conn)` calls.

## Runtime/production lineage
Worker entrypoints call `dbm.migrate(conn)` at cycle start before processing jobs:
- orchestrator
- importer
- uploader

This is the runtime guarantee that Epic-5 schema (including visual tables) is present before job processing.

## Test/bootstrap lineage
Test bootstrap (`seed_minimal_db`) also calls `dbm.migrate(conn)` before seeding data.

## Epic-5 schema guarantee
Epic-5 tables are created inside `dbm.migrate(conn)` using additive/idempotent SQL (`CREATE TABLE IF NOT EXISTS` + migration helpers).

Therefore, Epic-5 schema provenance in this project is:
1) connect DB
2) call inline migrate
3) proceed with runtime/API/test flow
