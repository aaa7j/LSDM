Overview

This folder contains SQL DDL and views to run the end‑to‑end demo:

- Schemas: staging, std, gav, wh
- Staging tables for the selected CSV/JSONL sources under `data/`
- Standardization views (std.*)
- GAV integration views (gav.*) with provenance columns
- Optional materialized views (wh.*) for Spark

Run Order

1) 01_schemas.sql
2) 02_staging_tables.sql
3) Load data into staging via `scripts/run_demo.py` (COPY for CSV, JSON parse for JSONL)
4) 03_std_views.sql
5) 04_gav_views.sql
6) 05_wh_matviews.sql (optional)

Connection

- Configure Postgres in `db/config.json` or via env vars: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

