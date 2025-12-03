import argparse
import json
import os
import time
from typing import Optional

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


def build_spark(app_name: str = "Q4MultifactorRanking") -> SparkSession:
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


def run_usage(
    input_path: str,
    out_dir: str,
    season: Optional[int] = None,
    team: Optional[str] = None,
    min_games: int = 10,
    results_path: Optional[str] = None,
) -> None:
    spark = build_spark()
    t0 = time.perf_counter()

    schema = StructType(
        [
            StructField("season", IntegerType(), True),
            StructField("team_abbr", StringType(), True),
            StructField("player_id", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("pos", StringType(), True),
            StructField("g", IntegerType(), True),
            StructField("gs", IntegerType(), True),
            StructField("mp", DoubleType(), True),
            StructField("usage_pct", DoubleType(), True),
            StructField("ts_percent", DoubleType(), True),
            StructField("pts_per_36", DoubleType(), True),
            StructField("team_pts", DoubleType(), True),
            StructField("ws", DoubleType(), True),
            StructField("vorp", DoubleType(), True),
        ]
    )

    df = (
        spark.read.option("header", "false")
        .option("delimiter", "\t")
        .schema(schema)
        .csv(input_path)
    )

    if season is not None:
        df = df.filter(F.col("season") == int(season))
    if team:
        df = df.filter(F.col("team_abbr") == team.upper())

    df = df.filter((F.col("g").isNotNull()) & (F.col("g") >= int(min_games)))

    df = df.withColumn(
        "minutes_per_game",
        F.when(F.col("g") > 0, F.round(F.col("mp") / F.col("g"), 1)),
    )

    win_team = Window.partitionBy("season", "team_abbr")
    df = df.withColumn("team_minutes_sum", F.sum("mp").over(win_team))
    df = df.withColumn(
        "team_minutes_share",
        F.round(F.when(F.col("team_minutes_sum") > 0, 100.0 * F.col("mp") / F.col("team_minutes_sum")), 1),
    )

    df = df.withColumn(
        "est_points",
        F.when(F.col("pts_per_36").isNotNull(), F.col("pts_per_36") * F.col("mp") / F.lit(36.0)),
    )
    df = df.withColumn(
        "team_pts_share",
        F.round(F.when((F.col("team_pts") > 0) & F.col("est_points").isNotNull(), 100.0 * F.col("est_points") / F.col("team_pts")), 1),
    )

    # Multifactor score (normalized ranges) with advanced stats and penalty for low minutes and few games
    df = df.withColumn("usage_norm", F.col("usage_pct") / F.lit(35.0))
    df = df.withColumn("ts_norm", F.col("ts_percent"))
    df = df.withColumn("pts_norm", F.col("pts_per_36") / F.lit(40.0))
    df = df.withColumn("team_pts_share_norm", F.col("team_pts_share") / F.lit(100.0))
    # Rough normalization for advanced metrics
    df = df.withColumn(
        "ws_norm",
        F.when(F.col("ws").isNull(), F.lit(0.0)).otherwise(
            F.when(F.col("ws") <= F.lit(0.0), F.lit(0.0)).otherwise(
                F.when(F.col("ws") >= F.lit(10.0), F.lit(1.0)).otherwise(F.col("ws") / F.lit(10.0))
            )
        ),
    )
    df = df.withColumn(
        "vorp_norm",
        F.when(F.col("vorp").isNull(), F.lit(0.0)).otherwise(
            F.when(F.col("vorp") <= F.lit(0.0), F.lit(0.0)).otherwise(
                F.when(F.col("vorp") >= F.lit(8.0), F.lit(1.0)).otherwise(F.col("vorp") / F.lit(8.0))
            )
        ),
    )
    # minutes_weight ~0 for <=10 mpg, ~1 for >=30 mpg
    df = df.withColumn(
        "minutes_weight",
        F.when(F.col("minutes_per_game") <= F.lit(10.0), F.lit(0.0)).otherwise(
            F.when(
                F.col("minutes_per_game") >= F.lit(30.0),
                F.lit(1.0),
            ).otherwise((F.col("minutes_per_game") - F.lit(10.0)) / F.lit(20.0))
        ),
    )
    # games_weight ~0 for 10 games, ~1 for 82 games
    df = df.withColumn(
        "games_weight",
        F.when(F.col("g") <= F.lit(10), F.lit(0.0)).otherwise(
            F.when(
                F.col("g") >= F.lit(82),
                F.lit(1.0),
            ).otherwise((F.col("g") - F.lit(10.0)) / F.lit(72.0))
        ),
    )
    df = df.withColumn(
        "minutes_games_factor",
        F.lit(0.5) * F.col("minutes_weight") + F.lit(0.5) * F.col("games_weight"),
    )
    df = df.withColumn(
        "score_core",
        0.30 * (F.col("team_minutes_share") / F.lit(100.0))
        + 0.15 * F.coalesce(F.col("usage_norm"), F.lit(0.0))
        + 0.15 * F.coalesce(F.col("ts_norm"), F.lit(0.0))
        + 0.15 * F.coalesce(F.col("pts_norm"), F.lit(0.0))
        + 0.10 * F.coalesce(F.col("team_pts_share_norm"), F.lit(0.0))
        + 0.075 * F.coalesce(F.col("ws_norm"), F.lit(0.0))
        + 0.075 * F.coalesce(F.col("vorp_norm"), F.lit(0.0)),
    )
    df = df.withColumn(
        "score",
        F.col("score_core") * F.col("minutes_games_factor"),
    )

    w_rank = Window.partitionBy("season", "team_abbr").orderBy(F.col("score").desc_nulls_last(), F.col("team_minutes_share").desc_nulls_last(), F.col("mp").desc_nulls_last())
    out = (
        df.withColumn("rank", F.row_number().over(w_rank))
        .filter(F.col("rank") <= 15)
        .select(
            "season",
            "team_abbr",
            F.lower(F.col("player_name")).alias("player_name"),
            "player_id",
            "pos",
            "g",
            "gs",
            F.round("mp", 1).alias("mp"),
            "minutes_per_game",
            F.round("usage_pct", 1).alias("usage_pct"),
            F.round("ts_percent", 3).alias("ts_percent"),
            F.round("pts_per_36", 1).alias("pts_per_36"),
            "team_minutes_share",
            "team_pts_share",
            F.round("score", 3).alias("score"),
            "rank",
        )
        .orderBy("season", "team_abbr", "rank")
    )

    out.write.mode("overwrite").parquet(out_dir)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    rows = out.count()
    print(
        f"[q4_playbyplay_usage] season={season or 'ALL'} team={team or 'ALL'} "
        f"rows={rows} time_ms={elapsed_ms:.1f}"
    )

    # Optionally append timing to main performance results JSONL
    if results_path:
        try:
            os.makedirs(os.path.dirname(results_path) or "results", exist_ok=True)
            rec = {
                "tool": "pyspark",
                "query": "q4_playbyplay_usage",
                "wall_ms": round(elapsed_ms, 3),
                "rows": int(rows),
            }
            with open(results_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception:
            # Non-blocking: log to stderr but do not fail the job
            import sys as _sys
            print("[q4_playbyplay_usage] WARNING: could not write results JSON", file=_sys.stderr)
    spark.stop()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="PySpark multifactor usage ranking (minutes, usage, TS%, PTS/36)"
    )
    ap.add_argument(
        "--input",
        default=os.path.join("warehouse", "bigdata", "q4_multifactor.tsv"),
        help="Path to enriched TSV produced by build_q4_multifactor.py",
    )
    ap.add_argument(
        "--out",
        default=os.path.join("outputs", "spark", "q4_playbyplay_usage"),
        help="Output directory for Parquet results",
    )
    ap.add_argument("--season", type=int, default=None)
    ap.add_argument("--team", type=str, default=None)
    ap.add_argument(
        "--min-games",
        type=int,
        default=10,
        dest="min_games",
        help="Minimum games played to keep a player",
    )
    ap.add_argument(
        "--results",
        default=None,
        help="Optional JSONL path to append timing/results (pyspark_vs_hadoop.jsonl)",
    )
    args = ap.parse_args()
    run_usage(
        input_path=args.input,
        out_dir=args.out,
        season=args.season,
        team=args.team,
        min_games=args.min_games,
        results_path=args.results,
    )


if __name__ == "__main__":
    main()
