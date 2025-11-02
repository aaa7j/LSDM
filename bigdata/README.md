Big Data comparison: PySpark vs Hadoop Streaming

Overview
- This module provides a reproducible comparison between PySpark (DataFrame/Spark SQL) and Hadoop-style MapReduce (via Python Streaming with a local runner) on NBA games data.
- Workloads:
  - Q1 Aggregation: total/avg points per team and season.
  - Q2 Join: join aggregated points with a team dimension.
  - Q3 Top-N: top N highest-scoring games per team.

Inputs
- JSON Lines: `data/json/games.jsonl` (required)
- Optional: other datasets are not required for these workloads.

Outputs
- Parquet (Spark): `warehouse/bigdata/...`
- TSV (Streaming): `warehouse/bigdata/team_game_points_tsv` and `warehouse/bigdata/teams_dim_tsv`
- Results: `results/pyspark_vs_hadoop.jsonl` summary timings
- Query outputs: under `outputs/spark/` and `outputs/hadoop/`

How to run (quick start)
1) Prepare data (Spark flattens JSON to per-team-per-game rows):
   - `python bigdata/spark/prepare_team_points.py --games data/json/games.jsonl --warehouse warehouse`

2) Run Spark workloads:
   - Q1: `python bigdata/spark/q1_agg_points.py --warehouse warehouse --out outputs/spark/q1`
   - Q2: `python bigdata/spark/q2_join_teamname.py --warehouse warehouse --out outputs/spark/q2`
   - Q3: `python bigdata/spark/q3_topn_games.py --warehouse warehouse --out outputs/spark/q3 --topn 3`

3) Prepare TSV for streaming and run Hadoop-style jobs locally:
   - TSV prep: `python bigdata/hadoop/prep/flatten_games_to_tsv.py --games data/json/games.jsonl --out warehouse/bigdata/team_game_points_tsv`
   - Teams dim TSV: `python bigdata/hadoop/prep/build_teams_dim.py --inp warehouse/bigdata/team_game_points_tsv --out warehouse/bigdata/teams_dim_tsv`
   - Q1: `python bigdata/streaming/local_runner.py --mapper bigdata/hadoop/streaming/mapper_q1_agg.py --reducer bigdata/hadoop/streaming/reducer_q1_agg.py --inp warehouse/bigdata/team_game_points_tsv --out outputs/hadoop/q1.tsv`
   - Q2: `python bigdata/streaming/local_runner.py --combine-q2 --agg outputs/hadoop/q1.tsv --dim warehouse/bigdata/teams_dim_tsv --out outputs/hadoop/q2.tsv`
   - Q3: `python bigdata/streaming/local_runner.py --mapper bigdata/hadoop/streaming/mapper_q3_topn.py --reducer bigdata/hadoop/streaming/reducer_q3_topn.py --inp warehouse/bigdata/team_game_points_tsv --out outputs/hadoop/q3.tsv --topn 3`

4) Optional: End-to-end orchestrator and Streamlit app
   - Orchestrator: `python scripts/run_bigdata_compare.py --runs 1 --topn 3`
   - App: `streamlit run app_pyspark_vs_hadoop.py`

Trino (opzionale)
- Requisiti: Trino server attivo su `localhost:8080` con i cataloghi configurati (almeno `mongodb` verso il DB con la collection `lsdm.games`). Vedi `trino/README.md` e usa `scripts/load_mongo.py` per caricare `data/json/*.jsonl` in MongoDB.
- Esegui i workload Trino e registra i tempi insieme agli altri:
  - `python scripts/run_trino_bigdata.py --host localhost --port 8080 --runs 1 --topn 3`
- I risultati verranno aggiunti a `results/pyspark_vs_hadoop.jsonl` con `tool = "trino"` e gli output CSV in `outputs/trino/q{1,2,3}.csv`.

Notes
- The streaming jobs use a portable local runner that emulates Hadoop Streaming (map → sort/shuffle → reduce) without requiring Hadoop installation.
- For fair Spark runs, ensure Java and PySpark are installed (see repository `requirements.txt`).

Real Hadoop + Spark (Docker)
- Start single-node Hadoop (HDFS+YARN) and Spark cluster via Docker:
  - `powershell ./scripts/hadoop_up.ps1`
  - UIs: HDFS NameNode http://localhost:9870, Spark Master http://localhost:8090
- (First time) Install Python in Hadoop containers for Streaming mappers:
  - `powershell ./scripts/hadoop_prepare_python.ps1`
- Run Hadoop Streaming (real Hadoop):
  - `powershell ./scripts/run_hadoop_streaming.ps1 -TopN 3`
  - Outputs in `outputs/hadoop/q{1,2,3}.tsv`
- Run Spark on cluster via spark-submit:
  - `powershell ./scripts/run_spark_cluster.ps1 -TopN 3`
  - Outputs in `outputs/spark/q{1,2,3}` (Parquet)
- Stop cluster:
  - `powershell ./scripts/hadoop_down.ps1`

Note: Docker is recommended on Windows. If you prefer WSL/VM install, ask and we’ll provide system-level steps for Hadoop/Spark installation.
