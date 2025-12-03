import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "Q1AggPoints"):
    master = os.environ.get("SPARK_MASTER_URI", "local[2]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "2g"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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


def run_q1(warehouse_dir: str, out_dir: str, fmt: str = "parquet", results_path: str | None = None):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = _read_points(spark, base, fmt)

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
    agg.write.mode("overwrite").partitionBy("season").parquet(out_dir)
    elapsed = (time.perf_counter() - t0) * 1000.0
    rows_out = agg.count()

    if results_path is None:
        results_path = "results/pyspark_vs_hadoop.jsonl"
    os.makedirs(os.path.dirname(results_path) or "results", exist_ok=True)
    with open(results_path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q1_agg_points",
                    "wall_ms": round(elapsed, 3),
                    "rows": rows_out,
                    "fmt": fmt.lower(),
                }
            )
            + "\n"
        )

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--out", default="outputs/spark/q1")
    ap.add_argument("--fmt", choices=["parquet", "tsv", "csv"], default="parquet")
    ap.add_argument("--results", default=None)
    args = ap.parse_args()
    run_q1(args.warehouse, args.out, fmt=args.fmt, results_path=args.results)


if __name__ == "__main__":
    main()
