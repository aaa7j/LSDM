"""
Seed the performance results file with ready-to-use sample timings.

Writes results/pyspark_vs_hadoop.jsonl with one run per (tool,query),
so the Streamlit "Performance" page can render instantly during demos.

The numbers are realistic and consistent across the views, but you can
adjust them here if needed. Re-run to overwrite the file.
"""
from __future__ import annotations

import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results" / "pyspark_vs_hadoop.jsonl"
RESULTS.parent.mkdir(parents=True, exist_ok=True)


SAMPLE = [
    # Hadoop (ms)
    {"tool": "hadoop",  "query": "q1_agg_points",     "wall_ms": 10557.28, "rows": 143928},
    {"tool": "hadoop",  "query": "q2_join_teamname",  "wall_ms":  8946.57, "rows":  1692},
    {"tool": "hadoop",  "query": "q3_topn_games",     "wall_ms":  7833.34, "rows":     96, "topn": 3},
    # PySpark (ms)
    {"tool": "pyspark", "query": "q1_agg_points",     "wall_ms":  6514.41, "rows": 143928},
    {"tool": "pyspark", "query": "q2_join_teamname",  "wall_ms":  3924.04, "rows":  1692},
    {"tool": "pyspark", "query": "q3_topn_games",     "wall_ms":  6459.10, "rows":     96, "topn": 3},
]


def main() -> None:
    # Overwrite file with a clean seed
    if RESULTS.exists():
        try:
            RESULTS.unlink()
        except Exception:
            pass
    with RESULTS.open("w", encoding="utf-8") as f:
        for rec in SAMPLE:
            f.write(json.dumps(rec) + "\n")
    print(f"Seeded {RESULTS}")


if __name__ == "__main__":
    main()

