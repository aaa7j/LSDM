import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "Q1AggPoints"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def run_q1(warehouse_dir: str, out_dir: str):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = spark.read.parquet(os.path.join(base, "team_game_points"))

    t0 = time.perf_counter()
    agg = (
        points.groupBy("season", "team_id")
        .agg(
            F.sum("points").alias("total_points"),
            F.avg("points").alias("avg_points"),
            F.count("points").alias("games"),
        )
        .select("season", "team_id", "total_points", "avg_points", "games")
        .orderBy("season", "team_id")
    )
    agg.write.mode("overwrite").parquet(out_dir)
    elapsed = (time.perf_counter() - t0) * 1000.0
    rows_out = agg.count()

    os.makedirs("results", exist_ok=True)
    with open("results/pyspark_vs_hadoop.jsonl", "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q1_agg_points",
                    "wall_ms": round(elapsed, 3),
                    "rows": rows_out,
                }
            )
            + "\n"
        )

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--out", default="outputs/spark/q1")
    args = ap.parse_args()
    run_q1(args.warehouse, args.out)


if __name__ == "__main__":
    main()
