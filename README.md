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
- `--reg-param`, `--elastic-net` – logistic regression hyperparameters

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
