import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "Q2HighScoringShare"):
    parts = max(2, (os.cpu_count() or 4) * 2)
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(parts))
        .getOrCreate()
    )


def run_q2(warehouse_dir: str, out_dir: str, threshold: int = 120):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = spark.read.parquet(os.path.join(base, "team_game_points"))
    teams = spark.read.parquet(os.path.join(base, "teams_dim"))

    t0 = time.perf_counter()
    hs = points.select(
        "season",
        "team_id",
        (F.col("points") >= F.lit(threshold)).cast("int").alias("is_high"),
    )
    agg = (
        hs.groupBy("season", "team_id")
        .agg(F.sum("is_high").alias("high_games"), F.count(F.lit(1)).alias("total_games"))
        .withColumn("pct_high", (F.col("high_games") / F.col("total_games")).cast("double"))
        .withColumn("pct_high", F.round(F.col("pct_high"), 3))
    )
    joined = (
        agg.join(teams, on="team_id", how="left")
        .select("season", "team_id", "team_name", "high_games", "total_games", "pct_high")
        .orderBy("season", "team_id")
    )
    joined.write.mode("overwrite").partitionBy("season").parquet(out_dir)
    elapsed = (time.perf_counter() - t0) * 1000.0
    rows_out = joined.count()

    os.makedirs("results", exist_ok=True)
    with open("results/pyspark_vs_hadoop.jsonl", "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q2_join_teamname",
                    "wall_ms": round(elapsed, 3),
                    "rows": int(rows_out),
                }
            )
            + "\n"
        )

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--out", default="outputs/spark/q2")
    ap.add_argument("--threshold", type=int, default=120)
    args = ap.parse_args()
    run_q2(args.warehouse, args.out, args.threshold)


if __name__ == "__main__":
    main()

