import argparse
import csv
import os
import sys
import time
from typing import List, Tuple

try:
    from trino.dbapi import connect
except Exception as e:
    connect = None  # type: ignore


def exec_sql(cur, sql: str):
    cur.execute(sql)
    # Fetch is not always required; here we ignore results for DDL


def fetch_to_csv(cur, sql: str, out_path: str) -> int:
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow(list(r))
    return len(rows)


def run(host: str, port: int, user: str, runs: int, topn: int, outdir: str, results_file: str,
        mongo_catalog: str = "mongodb", mongo_schema: str = "lsdm"):
    if connect is None:
        raise SystemExit("Python trino client not installed. pip install trino")

    conn = connect(host=host, port=port, user=user, catalog="memory", schema="gav")
    cur = conn.cursor()

    # Prepare schema and views that reference MongoDB games
    exec_sql(cur, "CREATE SCHEMA IF NOT EXISTS memory.gav")

    tgp_view = f"""
    CREATE OR REPLACE VIEW memory.gav.team_game_points_trino AS
    SELECT season,
           CAST(home.team_id AS VARCHAR) AS team_id,
           TRY_CAST(home.score AS INTEGER) AS points,
           game_id
    FROM {mongo_catalog}.{mongo_schema}.games
    WHERE home.team_id IS NOT NULL AND home.score IS NOT NULL
    UNION ALL
    SELECT season,
           CAST(away.team_id AS VARCHAR) AS team_id,
           TRY_CAST(away.score AS INTEGER) AS points,
           game_id
    FROM {mongo_catalog}.{mongo_schema}.games
    WHERE away.team_id IS NOT NULL AND away.score IS NOT NULL
    """
    exec_sql(cur, tgp_view)

    teams_view = f"""
    CREATE OR REPLACE VIEW memory.gav.teams_dim_trino AS
    SELECT team_id, MAX(team_name) AS team_name FROM (
      SELECT CAST(home.team_id AS VARCHAR) AS team_id, CAST(home.team_name AS VARCHAR) AS team_name
      FROM {mongo_catalog}.{mongo_schema}.games WHERE home.team_id IS NOT NULL
      UNION ALL
      SELECT CAST(away.team_id AS VARCHAR) AS team_id, CAST(away.team_name AS VARCHAR) AS team_name
      FROM {mongo_catalog}.{mongo_schema}.games WHERE away.team_id IS NOT NULL
    ) t GROUP BY team_id
    """
    exec_sql(cur, teams_view)

    os.makedirs(outdir, exist_ok=True)
    os.makedirs(os.path.dirname(results_file), exist_ok=True)

    for i in range(runs):
        # Q1
        q1_sql = (
            "SELECT season, team_id, SUM(points) AS total_points, AVG(points) AS avg_points, COUNT(*) AS games "
            "FROM memory.gav.team_game_points_trino GROUP BY season, team_id ORDER BY season, team_id"
        )
        t0 = time.perf_counter()
        n1 = fetch_to_csv(cur, q1_sql, os.path.join(outdir, "q1.csv"))
        ms = (time.perf_counter() - t0) * 1000.0
        with open(results_file, "a", encoding="utf-8") as f:
            f.write(f"{{\"tool\": \"trino\", \"query\": \"q1_agg_points\", \"wall_ms\": {ms:.3f}, \"rows\": {n1}}}\n")

        # Q2 (join)
        q2_sql = (
            "SELECT q1.season, q1.team_id, t.team_name, q1.total_points, q1.avg_points, q1.games "
            "FROM (SELECT season, team_id, SUM(points) AS total_points, AVG(points) AS avg_points, COUNT(*) AS games "
            "      FROM memory.gav.team_game_points_trino GROUP BY season, team_id) q1 "
            "LEFT JOIN memory.gav.teams_dim_trino t ON q1.team_id = t.team_id "
            "ORDER BY q1.season, q1.team_id"
        )
        t0 = time.perf_counter()
        n2 = fetch_to_csv(cur, q2_sql, os.path.join(outdir, "q2.csv"))
        ms = (time.perf_counter() - t0) * 1000.0
        with open(results_file, "a", encoding="utf-8") as f:
            f.write(f"{{\"tool\": \"trino\", \"query\": \"q2_join_teamname\", \"wall_ms\": {ms:.3f}, \"rows\": {n2}}}\n")

        # Q3 (TopN)
        q3_sql = (
            "WITH ranked AS (SELECT team_id, season, game_id, points, "
            " row_number() OVER (PARTITION BY team_id ORDER BY points DESC, game_id) AS rn "
            " FROM memory.gav.team_game_points_trino) "
            f"SELECT team_id, season, game_id, points FROM ranked WHERE rn <= {int(topn)} ORDER BY team_id, points DESC"
        )
        t0 = time.perf_counter()
        n3 = fetch_to_csv(cur, q3_sql, os.path.join(outdir, "q3.csv"))
        ms = (time.perf_counter() - t0) * 1000.0
        with open(results_file, "a", encoding="utf-8") as f:
            f.write(f"{{\"tool\": \"trino\", \"query\": \"q3_topn_games\", \"wall_ms\": {ms:.3f}, \"rows\": {n3}, \"topn\": {int(topn)} }}\n")

    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="Run Trino bigdata queries and record timings")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--user", default="lsdm")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--topn", type=int, default=3)
    ap.add_argument("--outdir", default="outputs/trino")
    ap.add_argument("--results", default="results/pyspark_vs_hadoop.jsonl")
    ap.add_argument("--mongo-catalog", default="mongodb")
    ap.add_argument("--mongo-schema", default="lsdm")
    args = ap.parse_args()

    try:
        run(
            host=args.host,
            port=args.port,
            user=args.user,
            runs=args.runs,
            topn=args.topn,
            outdir=args.outdir,
            results_file=args.results,
            mongo_catalog=args.mongo_catalog,
            mongo_schema=args.mongo_schema,
        )
        print("[OK] Trino runs completed.")
    except Exception as e:
        print("[ERROR] Trino run failed:", e, file=sys.stderr)
        raise


if __name__ == "__main__":
    main()

