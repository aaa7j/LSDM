Run Instructions
----------------

- Prerequisites:
  - Python 3.9+
  - Install PySpark: `pip install pyspark`
  - Place Kaggle CSVs under a base folder (default: `csv/` in repo root). Example files:
    - `csv/player.csv`, `csv/common_player_info.csv`
    - `csv/team.csv`, `csv/team_info_common.csv`
    - `csv/game.csv`, `csv/game_info.csv`, `csv/game_summary.csv`
    - `csv/play_by_play.csv`, `csv/line_score.csv`, `csv/other_stats.csv`
    - `csv/officials.csv`, `csv/draft_combine_stats.csv`, `csv/draft_history.csv`

- Execute GAV pipeline
  - Preview only:
    - `python scripts/run_gav.py --base csv --preview`
  - Validate + preview:
    - `python scripts/run_gav.py --base csv --validate --preview`
  - Save integrated views to Parquet:
    - `python scripts/run_gav.py --base csv --save warehouse`

- Analytics CLI
  - Show top scorers + train outcome model:
    - `python scripts/run_analytics.py --base csv`
  - Persist outputs:
    - `python scripts/run_analytics.py --base csv --metrics-out analytics/player-metrics --model-dir models/home-win --predictions-out analytics/predictions`

- Dashboard
  - Launch Streamlit:
    - `streamlit run dashboard/app.py`
  - Use the sidebar to set the CSV base directory before loading data.

- Outputs
  - When `--save` is used, each `GLOBAL_*` view is written to a subfolder under the chosen output directory as Parquet (e.g., `warehouse/global_player`).
  - Player metrics / predictions are also written as Parquet (or CSV) when configured.
  - Use `spark.read.parquet('warehouse/global_player')` to load results in PySpark.
