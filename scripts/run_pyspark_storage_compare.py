import argparse
import os
import sys
import time
from pathlib import Path


def run(cmd):
    t0 = time.perf_counter()
    from subprocess import run as srun

    res = srun(cmd, capture_output=True, text=True)
    elapsed = (time.perf_counter() - t0) * 1000.0
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr, file=sys.stderr)
        raise SystemExit(f"Command failed: {' '.join(cmd)}")
    return elapsed


def main():
    ap = argparse.ArgumentParser(description="Compare PySpark CSV/TSV vs Parquet inputs")
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--topn", type=int, default=3)
    ap.add_argument("--results", default="results/pyspark_storage.jsonl")
    args = ap.parse_args()

    Path("outputs/spark/q1").mkdir(parents=True, exist_ok=True)
    Path("outputs/spark/q2").mkdir(parents=True, exist_ok=True)
    Path("outputs/spark/q3").mkdir(parents=True, exist_ok=True)
    Path("results").mkdir(parents=True, exist_ok=True)

    for i in range(args.runs):
        for fmt in ("parquet", "tsv"):
            print(f"[pyspark-{fmt}] Run {i+1}/{args.runs}: q1_agg_points ...")
            run([sys.executable, "bigdata/spark/q1_agg_points.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q1", "--fmt", fmt, "--results", args.results])
            print(f"[pyspark-{fmt}] Run {i+1}/{args.runs}: q2_join_teamname ...")
            run([sys.executable, "bigdata/spark/q2_join_teamname.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q2", "--fmt", fmt, "--results", args.results])
            print(f"[pyspark-{fmt}] Run {i+1}/{args.runs}: q3_topn_games (topn={args.topn}) ...")
            run([sys.executable, "bigdata/spark/q3_topn_games.py", "--warehouse", args.warehouse, "--out", "outputs/spark/q3", "--topn", str(args.topn), "--fmt", fmt, "--results", args.results])

    print(f"Done. See {args.results}")


if __name__ == "__main__":
    main()

