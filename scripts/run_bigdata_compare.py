import argparse
import json
import os
import subprocess
import sys
import time


def run(cmd, cwd=None):
    t0 = time.perf_counter()
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - t0) * 1000.0
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise SystemExit(f"Command failed: {' '.join(cmd)}")
    return elapsed


def main():
    ap = argparse.ArgumentParser(description="Run PySpark vs Hadoop Streaming comparison")
    ap.add_argument("--games", default="data/json/games.jsonl")
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--topn", type=int, default=3)
    args = ap.parse_args()

    os.makedirs("outputs/spark/q1", exist_ok=True)
    os.makedirs("outputs/spark/q2", exist_ok=True)
    os.makedirs("outputs/spark/q3", exist_ok=True)
    os.makedirs("outputs/hadoop", exist_ok=True)
    os.makedirs("results", exist_ok=True)

    run([sys.executable, "bigdata/spark/prepare_team_points.py", "--games", args.games, "--warehouse", args.warehouse])

    for i in range(args.runs):
        run([sys.executable, "bigdata/spark/q1_agg_points.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q1"])
        run([sys.executable, "bigdata/spark/q2_join_teamname.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q2"])
        run([sys.executable, "bigdata/spark/q3_topn_games.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q3", "--topn", str(args.topn)])

    run([sys.executable, "bigdata/hadoop/prep/flatten_games_to_tsv.py", "--games", args.games, "--out", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv")])
    run([sys.executable, "bigdata/hadoop/prep/build_teams_dim.py", "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"), "--out", os.path.join(args.warehouse, "bigdata", "teams_dim_tsv")])

    results_path = os.path.join("results", "pyspark_vs_hadoop.jsonl")
    for i in range(args.runs):
        # Q1 Hadoop
        t = run([
            sys.executable,
            "bigdata/streaming/local_runner.py",
            "--mapper", "bigdata/hadoop/streaming/mapper_q1_agg.py",
            "--reducer", "bigdata/hadoop/streaming/reducer_q1_agg.py",
            "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"),
            "--out", "outputs/hadoop/q1.tsv",
            "--label", "q1_agg_points",
        ])
        try:
            rows = sum(1 for _ in open("outputs/hadoop/q1.tsv", "r", encoding="utf-8", errors="ignore"))
        except Exception:
            rows = None
        with open(results_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"tool":"hadoop_streaming","query":"q1_agg_points","wall_ms":round(t,3),"rows":rows})+"\n")

        # Q2 Hadoop (high-scoring share)
        t = run([
            sys.executable,
            "bigdata/streaming/local_runner.py",
            "--mapper", "bigdata/hadoop/streaming/mapper_q2_join.py",
            "--reducer", "bigdata/hadoop/streaming/reducer_q2_join.py",
            "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"),
            "--out", "outputs/hadoop/q2.tsv",
            "--label", "q2_high_scoring_share",
        ])
        try:
            rows = sum(1 for _ in open("outputs/hadoop/q2.tsv", "r", encoding="utf-8", errors="ignore"))
        except Exception:
            rows = None
        with open(results_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"tool":"hadoop_streaming","query":"q2_high_scoring_share","wall_ms":round(t,3),"rows":rows})+"\n")

        # Q3 Hadoop
        t = run([
            sys.executable,
            "bigdata/streaming/local_runner.py",
            "--mapper", "bigdata/hadoop/streaming/mapper_q3_topn.py",
            "--reducer", "bigdata/hadoop/streaming/reducer_q3_topn.py",
            "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"),
            "--out", "outputs/hadoop/q3.tsv",
            "--topn", str(args.topn),
            "--label", "q3_topn_games",
        ])
        try:
            rows = sum(1 for _ in open("outputs/hadoop/q3.tsv", "r", encoding="utf-8", errors="ignore"))
        except Exception:
            rows = None
        with open(results_path, "a", encoding="utf-8") as f:
            f.write(json.dumps({"tool":"hadoop_streaming","query":"q3_topn_games","wall_ms":round(t,3),"rows":rows,"topn":args.topn})+"\n")

    print("Done. See results/pyspark_vs_hadoop.jsonl and outputs/.")


if __name__ == "__main__":
    main()
