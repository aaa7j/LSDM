Trino setup (templates)

This folder provides ready-to-edit templates to run Trino locally and expose a virtual GAV schema over Postgres (CSV) and Mongo (JSONL).

Steps

1) Install Java 17+ and download Trino server zip. Unzip to e.g. C:\trino
2) Copy files from trino/etc/* into C:\trino\etc (create folders if missing)
   - Edit catalog credentials in etc/catalog/postgresql.properties and mongodb.properties
3) Start Trino: C:\trino\bin\launcher.bat run (UI: http://localhost:8080/ui)
4) Load Postgres staging and Mongo JSONL as per repo instructions (scripts/run_demo.py for CSV; mongoimport for JSONL)
5) From a Trino client (CLI or DBeaver), run views/gav.sql to create the GAV views (in memory.gav)
6) Run queries/before_after.sql to showcase “raw vs GAV” queries

Notes

- The memory connector stores views in-memory; re-run views/gav.sql after restart
- Catalog names in queries assume:
  - postgresql catalog points to DB lsdm (schema: staging)
  - mongodb catalog has DB lsdm (collections: player_bio, team_details)

