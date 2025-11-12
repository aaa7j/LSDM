import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def run(cmd, cwd=None):
    t0 = time.perf_counter()
    res = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - t0) * 1000.0
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise SystemExit(f"Command failed: {' '.join(cmd)}")
    return elapsed


def _exists_nonempty(path: str | Path) -> bool:
    p = Path(path)
    if not p.exists():
        return False
    if p.is_file():
        try:
            return p.stat().st_size > 0
        except Exception:
            return True
    # directory: consider non-empty if it contains at least one file
    for _ in p.rglob("*"):
        return True
    return False


def main():
    ap = argparse.ArgumentParser(description="Run PySpark vs Hadoop Streaming comparison")
    ap.add_argument("--games", default="data/json/games.jsonl")
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--topn", type=int, default=3)
    ap.add_argument("--append-results", action="store_true", help="Append to results file instead of overwriting")
    ap.add_argument("--reuse", action="store_true", default=True, help="Reuse prepared inputs (Spark parquet/TSV) when present")
    args = ap.parse_args()

    os.makedirs("outputs/spark/q1", exist_ok=True)
    os.makedirs("outputs/spark/q2", exist_ok=True)
    os.makedirs("outputs/spark/q3", exist_ok=True)
    os.makedirs("outputs/hadoop", exist_ok=True)
    os.makedirs("results", exist_ok=True)

    # Prepare results file: overwrite unless --append-results is set
    results_path = os.path.join("results", "pyspark_vs_hadoop.jsonl")
    if not args.append_results and os.path.exists(results_path):
        os.remove(results_path)

    # Prepare Spark inputs (parquet) only when needed or when reuse is disabled
    parquet_dir = Path(args.warehouse) / "bigdata" / "team_game_points"
    if not args.reuse or not _exists_nonempty(parquet_dir):
        print("[prep] Building Spark inputs (team_game_points)...")
        cmd = [sys.executable, "bigdata/spark/prepare_team_points.py", "--warehouse", args.warehouse]
        if os.environ.get("PG_HOST") or os.environ.get("PG_URL") or os.environ.get("PG_DATABASE"):
            cmd += ["--source", "auto"]
        else:
            cmd += ["--source", "file", "--games", args.games]
        run(cmd)
    else:
        print(f"[prep] Reusing Spark inputs at {parquet_dir}")

    for i in range(args.runs):
        print(f"[spark] Run {i+1}/{args.runs}: q1_agg_points ...")
        t = run([sys.executable, "bigdata/spark/q1_agg_points.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q1"])
        print(f"[spark] q1_agg_points done in {t:.0f} ms")
        print(f"[spark] Run {i+1}/{args.runs}: q2_join_teamname ...")
        t = run([sys.executable, "bigdata/spark/q2_join_teamname.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q2"])
        print(f"[spark] q2_join_teamname done in {t:.0f} ms")
        print(f"[spark] Run {i+1}/{args.runs}: q3_topn_games (topn={args.topn}) ...")
        t = run([sys.executable, "bigdata/spark/q3_topn_games.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q3", "--topn", str(args.topn)])
        print(f"[spark] q3_topn_games done in {t:.0f} ms")

    # Build TSV inputs for streaming (reuse if present)
    tsv_points = Path(args.warehouse) / "bigdata" / "team_game_points_tsv" / "part-00000.tsv"
    tsv_dimdir = Path(args.warehouse) / "bigdata" / "teams_dim_tsv"
    if not args.reuse or not _exists_nonempty(tsv_points.parent):
        print("[prep] Writing TSV for Hadoop (flatten games -> team points)...")
        run([sys.executable, "bigdata/hadoop/prep/flatten_games_to_tsv.py", "--games", args.games, "--out", str(tsv_points.parent)])
    else:
        print(f"[prep] Reusing TSV inputs at {tsv_points.parent}")
    if not args.reuse or not _exists_nonempty(tsv_dimdir):
        print("[prep] Building teams dim TSV (unique team_id -> names)...")
        run([sys.executable, "bigdata/hadoop/prep/build_teams_dim.py", "--inp", str(tsv_points.parent), "--out", str(tsv_dimdir)])
    else:
        print(f"[prep] Reusing teams dim TSV at {tsv_dimdir}")

    for i in range(args.runs):
        # Q1 Hadoop
        print(f"[hadoop] Run {i+1}/{args.runs}: q1_agg_points (streaming) ...")
        t = run([
            sys.executable,
            "bigdata/streaming/local_runner.py",
            "--mapper", "bigdata/hadoop/streaming/mapper_q1_agg.py",
            "--reducer", "bigdata/hadoop/streaming/reducer_q1_agg.py",
            "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"),
            "--out", "outputs/hadoop/q1.tsv",
            "--label", "q1_agg_points",
        ])
        print(f"[hadoop] q1_agg_points done in {t:.0f} ms")

        # Q2 Hadoop (high-scoring share)
        print(f"[hadoop] Run {i+1}/{args.runs}: q2_join_teamname (streaming) ...")
        t = run([
            sys.executable,
            "bigdata/streaming/local_runner.py",
            "--mapper", "bigdata/hadoop/streaming/mapper_q2_join.py",
            "--reducer", "bigdata/hadoop/streaming/reducer_q2_join.py",
            "--inp", os.path.join(args.warehouse, "bigdata", "team_game_points_tsv"),
            "--out", "outputs/hadoop/q2.tsv",
            "--label", "q2_join_teamname",
        ])
        print(f"[hadoop] q2_join_teamname done in {t:.0f} ms")

        # Q3 Hadoop
        print(f"[hadoop] Run {i+1}/{args.runs}: q3_topn_games (streaming, topn={args.topn}) ...")
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
        print(f"[hadoop] q3_topn_games done in {t:.0f} ms")

    print("Done. See results/pyspark_vs_hadoop.jsonl and outputs/.")


if __name__ == "__main__":
    main()
