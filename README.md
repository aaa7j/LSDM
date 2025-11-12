# Basketball Analytics – GAV Integration & PySpark Analytics

This project unifies heterogeneous basketball datasets using a Global-As-View (GAV) schema and builds PySpark analytics for player performance and game outcome modelling. It ships with utilities to load Kaggle CSVs, register temp views, run quality checks, train models, and launch a lightweight dashboard.

## Repository Structure

- `src/`
  - `etl/`
    - `read_sources.py` – Spark session + CSV registration helper
    - `gav.py` – runs the GAV SQL views and saves outputs
    - `quality.py` – row-count and primary-key assertions
    - `transform_gav.sql` – SQL definitions for all `GLOBAL_*` views
  - `analytics/`
    - `player_metrics.py` – scoring aggregates from play-by-play
    - `game_prediction.py` – logistic regression for home-win prediction
- `scripts/`
  - `run_gav.py` – end-to-end ETL (register, transform, QA, persist)
  - `run_analytics.py` – player metrics + outcome model CLI
- `dashboard/app.py` – Streamlit interface on top of the GAV views
- `USAGE.md` – quick command reference
- `csv/` – expected location for Kaggle CSV files (not committed)

## Data Sources

Download the Kaggle dataset: https://www.kaggle.com/datasets/wyattowalsh/basketball/data and place the CSV files under a base directory (default `csv/`). Key files used:

- `player.csv`, `common_player_info.csv`
- `team.csv`, `team_info_common.csv`
- `game.csv`, `game_info.csv`, `game_summary.csv`
- `play_by_play.csv`
- `line_score.csv`
- `other_stats.csv`
- `officials.csv`
- `draft_combine_stats.csv`
- `draft_history.csv`

## Environment Setup

```bash
python -m venv .venv
.venv\Scripts\activate  # or source .venv/bin/activate
pip install pyspark streamlit
```

## Run the GAV ETL

Register CSVs, create the `GLOBAL_*` views, run QA, and optionally persist Parquet snapshots:

```bash
python scripts/run_gav.py --base csv --validate --preview
python scripts/run_gav.py --base csv --save warehouse
```

- `--validate` prints row counts and enforces primary-key uniqueness.
- `--preview` shows a sample of `GLOBAL_PLAYER`, `GLOBAL_TEAM`, `GLOBAL_GAME`.
- `--save` writes each global view to `warehouse/<view_name>/` as Parquet.

## Analytics CLI

Compute player scoring metrics and train a logistic regression for home-win prediction:

```bash
python scripts/run_analytics.py --base csv --metrics-out analytics/player-metrics --model-dir models/home-win
```

Flags:

- `--top-n` – number of top scorers displayed in the console (default 10)
- `--metrics-out` – directory to persist player metrics (`--metrics-format` controls Parquet/CSV)
- `--model-dir` – path to save the PySpark PipelineModel
- `--predictions-out` – optional Parquet dump of test-set predictions
- `--reg-param`, `--elastic-net` — logistic regression hyperparameters

Alternatively, if you already saved the GLOBAL_* views to Parquet (`warehouse/`), you can run the analytics directly from the warehouse without CSVs:

```bash
python scripts/run_models.py --warehouse warehouse \
  --metrics-out analytics/player-metrics \
  --model-dir models/home-win \
  --predictions-out analytics/predictions
```

## Dashboard

Launch the Streamlit app for interactive exploration:

```bash
streamlit run dashboard/app.py
```

Sidebar controls allow you to select the CSV base path. The dashboard renders:

- Top N scorers derived from the play-by-play feed
- On-demand training of the outcome model with metrics and prediction preview

## Data Quality

Use `scripts/run_gav.py --validate` to enforce primary-key uniqueness for every global view. Additional checks can be scripted via `src/etl/quality.py`.

## ER Diagram

Refer to `ER.mmd` (Mermaid source) and the generated `Global-Schema.png` for the conceptual model covering players, teams, games, play-by-play, officials, line scores, and draft data.

## Next Ideas

- Enrich player analytics with advanced metrics (usage rate, true shooting percentage)
- Add team trends dashboards (rolling offensive/defensive ratings)
- Integrate predictive pipelines with MLflow for experiment tracking
- Deploy the dashboard with scheduled refreshes against cloud storage

## Search Service

Run a lightweight FastAPI service that keeps a single long‑lived PySpark session for fast queries (players/teams/games combined):

1) Install requirements:

```
pip install -r requirements.txt
```

2) Start the service (Windows‑friendly runner):

```
python scripts/run_search_service.py --warehouse warehouse --host 127.0.0.1 --port 8000
```

3) Endpoints:
- Health: `http://127.0.0.1:8000/health`
- Search: `http://127.0.0.1:8000/search?q=michael%20jordan&limit=25`

4) Minimal Web UI:
- Open `http://127.0.0.1:8000/` in your browser for a basic search page.

Notes:
- The service reads Parquet snapshots from the `warehouse/` folder (create them with `scripts/run_gav.py --save warehouse`).
- On Windows ensure a JDK is installed; you can pass `--java-home "C:\\Path\\To\\JDK"` to the runner if needed.

## Hadoop vs PySpark demo (UI)

- Jobs
  - Hadoop Streaming: `powershell -File scripts/run_hadoop_streaming.ps1 -Only all`
  - PySpark cluster:  `powershell -File scripts/run_spark_cluster.ps1 -Only all`
  - Tip: re-run with `-Reuse` for fast clicks in the UI
- Generate UI JSON: `powershell -File scripts/generate_web_results.ps1`
- Serve UI: `powershell -File scripts/serve_ui.ps1 -Port 8080`
- Open: `http://localhost:8080/hadoop_pyspark_results.html`

Notes
- Spark exports (Parquet) and Hadoop outputs (TSV) are converted to JSON under `web/data/`.
- If using the Streamlit dashboard (`app.py`), ensure modules under `src/` are reachable or document them in `requirements.txt`.

## Comparative analysis (Streamlit)

Launch side-by-side analysis, speedups and resource profile:

```
streamlit run app_pyspark_vs_hadoop.py
```

- Reads `results/pyspark_vs_hadoop.jsonl` (if present)
- Computes average `wall_ms` per tool/query and `speedup_vs_hadoop`
- Plots bars via Altair; optional Spark resource profile in expander

## Exam Playbook (3 commands)

Prereqs
- Postgres running with the integrated datasets loaded (teams, games). Set env vars for JDBC.
- Java (JDK) available for Spark; Python venv activated.

1) Prepare Spark inputs via JDBC (Postgres)

PowerShell (Windows):

```
$env:PG_HOST = "localhost"
$env:PG_DB = "lsdm"
$env:PG_USER = "postgres"
$env:PG_PASSWORD = "postgres"
python bigdata/spark/prepare_team_points.py --source postgres --warehouse warehouse
```

This reads from Postgres using Spark JDBC and writes Parquet/TSV under `warehouse/bigdata/`.
If you don’t have the Postgres JDBC driver on the classpath, add it via packages:

```
$env:PYSPARK_SUBMIT_ARGS = "--packages org.postgresql:postgresql:42.7.3 pyspark-shell"
```

2) Run the benchmark and append a run to results

```
python scripts/run_bigdata_compare.py --runs 1 --topn 3 --append-results
```

This executes Hadoop Streaming and PySpark for q1/q2/q3 and appends a JSONL line per tool/query.

3) Open the Streamlit dashboard

```
streamlit run ui_gav.py
```

The “Performance” page reads `results/pyspark_vs_hadoop.jsonl` and renders:
- Tempi medi (1 decimale, barre affiancate per tool)
- Speedup medio/mediano (3 decimali)
- Distribuzioni con boxplot + punti grezzi
- Trend per run (assi integri, griglia leggera)
- Throughput con 3 grafici (uno per query)

