import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F, Window


def build_spark(app_name: str = "Q3TopNGames"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def run_q3(warehouse_dir: str, out_dir: str, topn: int):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = spark.read.parquet(os.path.join(base, "team_game_points"))

    t0 = time.perf_counter()
    w = Window.partitionBy("team_id").orderBy(F.col("points").desc(), F.col("game_id"))
    top = (
        points.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") <= F.lit(topn))
        .select("team_id", "season", "game_id", "points")
        .orderBy("team_id", F.col("points").desc())
    )
    top.write.mode("overwrite").parquet(out_dir)
    elapsed = (time.perf_counter() - t0) * 1000.0
    rows_out = top.count()

    os.makedirs("results", exist_ok=True)
    with open("results/pyspark_vs_hadoop.jsonl", "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q3_topn_games",
                    "wall_ms": round(elapsed, 3),
                    "rows": int(rows_out),
                    "topn": topn,
                }
            )
            + "\n"
        )

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--out", default="outputs/spark/q3")
    ap.add_argument("--topn", type=int, default=3)
    args = ap.parse_args()
    run_q3(args.warehouse, args.out, args.topn)


if __name__ == "__main__":
    main()


