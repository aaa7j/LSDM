import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def cat_inputs(inp_dir: str):
    if os.path.isfile(inp_dir):
        with open(inp_dir, "r", encoding="utf-8") as f:
            for line in f:
                yield line
        return
    for root, _, files in os.walk(inp_dir):
        for name in files:
            if name.endswith(".tsv") or name.endswith(".jsonl") or name.endswith(".json") or name.startswith("part-"):
                with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                    for line in f:
                        yield line


def run_one_phase(mapper: str, reducer: str, inp: str, out: str, env_extra=None):
    """
    Esegue mapper e reducer in locale evitando deadlock dei pipe:
    - usa subprocess.run(..., input=...) per leggere/scrivere atomico
    - ordina l'output del mapper prima di passarlo al reducer
    """
    os.makedirs(os.path.dirname(out), exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="mr_local_")
    map_out = os.path.join(tmp, "map_out.tsv")
    red_out = out
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    if env_extra:
        env.update(env_extra)
    try:
        # 1) Mapper: alimenta tutto l'input in un colpo (evita blocchi dei pipe)
        input_text = "".join(cat_inputs(inp))
        res_map = subprocess.run(
            [sys.executable, mapper],
            input=input_text.encode("utf-8"),
            capture_output=True,
            text=False,
            env=env,
            check=False,
        )
        if res_map.returncode != 0:
            err_txt = (res_map.stderr or b"")[:500].decode("utf-8", errors="ignore")
            raise RuntimeError(f"Mapper failed ({mapper}): {err_txt}")
        with open(map_out, "w", encoding="utf-8") as f:
            f.write(res_map.stdout.decode("utf-8", errors="ignore"))

        # 2) Ordina e passa al reducer
        with open(map_out, "r", encoding="utf-8") as f:
            sorted_lines = sorted([ln for ln in f if ln.strip()])
        res_red = subprocess.run(
            [sys.executable, reducer],
            input="".join(sorted_lines).encode("utf-8"),
            capture_output=True,
            text=False,
            env=env,
            check=False,
        )
        if res_red.returncode != 0:
            err_txt = (res_red.stderr or b"")[:500].decode("utf-8", errors="ignore")
            raise RuntimeError(f"Reducer failed ({reducer}): {err_txt}")
        with open(red_out, "w", encoding="utf-8") as f:
            f.write(res_red.stdout.decode("utf-8", errors="ignore"))
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
    # Count rows from output file if available
    try:
        rows = sum(1 for _ in open(args.out, "r", encoding="utf-8", errors="ignore"))
    except Exception:
        rows = None
    rec = {
        "tool": "hadoop_streaming",
        "query": args.label,
        "wall_ms": round(elapsed, 3),
    }
    if rows is not None:
        rec["rows"] = rows
    try:
        rec["topn"] = int(args.topn)
    except Exception:
        pass
    os.makedirs("results", exist_ok=True)
    with open("results/pyspark_vs_hadoop.jsonl", "a", encoding="utf-8") as f:
        import json as _json
        f.write(_json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
