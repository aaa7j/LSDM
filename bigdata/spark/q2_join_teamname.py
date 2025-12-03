import argparse
import json
import os
import time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T


def build_spark(app_name: str = "Q2HighScoringShare"):
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


def _read_teams(spark: SparkSession, base: str, fmt: str):
    if fmt.lower() in ("parquet", "pq"):
        return spark.read.parquet(os.path.join(base, "teams_dim"))
    schema = T.StructType([
        T.StructField("team_id", T.StringType(), True),
        T.StructField("team_city", T.StringType(), True),
        T.StructField("team_name", T.StringType(), True),
    ])
    return (
        spark.read.option("sep", "\t").option("header", "false").schema(schema)
        .csv(os.path.join(base, "teams_dim_tsv"))
    )


def run_q2(warehouse_dir: str, out_dir: str, threshold: int = 120, fmt: str = "parquet", results_path: str | None = None):
    spark = build_spark()
    base = os.path.join(warehouse_dir, "bigdata")
    points = _read_points(spark, base, fmt)
    teams = _read_teams(spark, base, fmt)

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

    if results_path is None:
        results_path = "results/pyspark_vs_hadoop.jsonl"
    os.makedirs(os.path.dirname(results_path) or "results", exist_ok=True)
    with open(results_path, "a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "tool": "pyspark",
                    "query": "q2_join_teamname",
                    "wall_ms": round(elapsed, 3),
                    "rows": int(rows_out),
                    "fmt": fmt.lower(),
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
    ap.add_argument("--fmt", choices=["parquet", "tsv", "csv"], default="parquet")
    ap.add_argument("--results", default=None)
    args = ap.parse_args()
    run_q2(args.warehouse, args.out, args.threshold, fmt=args.fmt, results_path=args.results)


if __name__ == "__main__":
    main()

