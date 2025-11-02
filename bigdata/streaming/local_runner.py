import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def cat_inputs(inp_dir: str):
    for root, _, files in os.walk(inp_dir):
        for name in files:
            if name.endswith(".tsv") or name.endswith(".jsonl") or name.endswith(".json") or name.startswith("part-"):
                with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                    for line in f:
                        yield line


def run_one_phase(mapper: str, reducer: str, inp: str, out: str, env_extra=None):
    os.makedirs(os.path.dirname(out), exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="mr_local_")
    map_out = os.path.join(tmp, "map_out.tsv")
    red_out = out
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    try:
        with subprocess.Popen([sys.executable, mapper], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, text=True) as pmap:
            for line in cat_inputs(inp):
                pmap.stdin.write(line)
            pmap.stdin.close()
            mapped = pmap.stdout.read()
        with open(map_out, "w", encoding="utf-8") as f:
            f.write(mapped)
        with open(map_out, "r", encoding="utf-8") as f:
            sorted_lines = sorted([ln for ln in f if ln.strip()])
        with subprocess.Popen([sys.executable, reducer], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, text=True) as pred:
            pred.stdin.write("".join(sorted_lines))
            pred.stdin.close()
            reduced = pred.stdout.read()
        with open(red_out, "w", encoding="utf-8") as f:
            f.write(reduced)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def combine_q2(agg_path: str, dim_path: str):
    tmp = tempfile.mkdtemp(prefix="mr_join_")
    combined = os.path.join(tmp, "combined.tsv")
    with open(combined, "w", encoding="utf-8") as out:
        # A \t season \t team_id \t total_points \t avg_points \t games
        for root, _, files in os.walk(agg_path if os.path.isdir(agg_path) else os.path.dirname(agg_path)):
            for name in files:
                if name.endswith(".tsv") or name.startswith("part-"):
                    with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            parts = line.split("\t")
                            if len(parts) >= 5:
                                out.write("\t".join(["A"] + parts) + "\n")
        # B \t team_id \t team_city \t team_name
        for root, _, files in os.walk(dim_path):
            for name in files:
                if name.endswith(".tsv") or name.startswith("part-"):
                    with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            out.write("B\t" + line + "\n")
    return combined


def main():
    ap = argparse.ArgumentParser(description="Local Hadoop Streaming runner")
    ap.add_argument("--mapper")
    ap.add_argument("--reducer")
    ap.add_argument("--inp", help="Input directory with TSV/JSONL files")
    ap.add_argument("--out", help="Output file path")
    ap.add_argument("--topn", type=int, default=3)
    ap.add_argument("--label", default="auto")
    ap.add_argument("--combine-q2", action="store_true")
    ap.add_argument("--agg", help="Q1 aggregated TSV path (for join)")
    ap.add_argument("--dim", help="Teams dim TSV dir (for join)")
    args = ap.parse_args()

    t0 = time.perf_counter()
    if args.combine_q2:
        combined = combine_q2(args.agg, args.dim)
        run_one_phase(
            mapper=os.path.join("bigdata", "hadoop", "streaming", "mapper_q2_join.py"),
            reducer=os.path.join("bigdata", "hadoop", "streaming", "reducer_q2_join.py"),
            inp=str(Path(combined).parent),
            out=args.out,
            env_extra=None,
        )
    else:
        env = None
        # pass TOPN only for Q3 (mapper filename contains 'q3_topn')
        try:
            mapper_name = os.path.basename(args.mapper)
            if "q3_topn" in mapper_name:
                env = {"TOPN": str(args.topn)}
        except Exception:
            pass
        run_one_phase(args.mapper, args.reducer, args.inp, args.out, env_extra=env)
    elapsed = (time.perf_counter() - t0) * 1000.0
    os.makedirs("results", exist_ok=True)
    with open("results/pyspark_vs_hadoop.jsonl", "a", encoding="utf-8") as f:
        f.write(
            "{" + f"\"tool\": \"hadoop_streaming\", \"query\": \"{args.label}\", \"wall_ms\": {elapsed:.3f}" + "}\n"
        )


if __name__ == "__main__":
    main()
