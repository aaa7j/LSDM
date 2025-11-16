import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql import types as T


def build_spark(app_name: str = "Q3TopNGames"):
    parts = max(2, (os.cpu_count() or 4) * 2)
    mem_driver = os.environ.get("SPARK_DRIVER_MEMORY", "4g")
    mem_exec = os.environ.get("SPARK_EXECUTOR_MEMORY", mem_driver)
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(parts))
        .config("spark.driver.memory", mem_driver)
        .config("spark.executor.memory", mem_exec)
    )
    return builder.getOrCreate()


def _read_points(spark: SparkSession, base: str, fmt: str):
    if fmt.lower() in ("parquet", "pq"):
        return spark.read.parquet(os.path.join(base, "team_game_points"))
    schema = T.StructType([
        T.StructField("season", T.StringType(), True),
        T.StructField("team_id", T.StringType(), True),
        T.StructField("points", T.IntegerType(), True),
        T.StructField("game_id", T.StringType(), True),
    ])
    return (
        spark.read.option("sep", "\t").option("header", "false").schema(schema)
        .csv(os.path.join(base, "team_game_points_tsv"))
    )


def run_q3(warehouse_dir: str, out_dir: str, topn: int, fmt: str = "parquet", results_path: str | None = None):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = _read_points(spark, base, fmt)

    t0 = time.perf_counter()
    w = Window.partitionBy("team_id").orderBy(F.col("points").desc(), F.col("game_id"))
    top = (
        points.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") <= F.lit(topn))
        .select("team_id", "season", "game_id", "points")
        .orderBy("team_id", F.col("points").desc())
    )
    top.write.mode("overwrite").partitionBy("season").parquet(out_dir)
    elapsed = (time.perf_counter() - t0) * 1000.0
    rows_out = top.count()

    if results_path is None:
        results_path = "results/pyspark_vs_hadoop.jsonl"
    os.makedirs(os.path.dirname(results_path) or "results", exist_ok=True)
    with open(results_path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q3_topn_games",
                    "wall_ms": round(elapsed, 3),
                    "rows": int(rows_out),
                    "fmt": fmt.lower(),
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
    ap.add_argument("--fmt", choices=["parquet", "tsv", "csv"], default="parquet")
    ap.add_argument("--results", default=None)
    args = ap.parse_args()
    run_q3(args.warehouse, args.out, args.topn, fmt=args.fmt, results_path=args.results)


if __name__ == "__main__":
    main()


