import argparse
import os
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "PrepareTeamPoints"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def prepare(games_path: str, warehouse_dir: str):
    spark = build_spark()
    df = spark.read.json(games_path)

    home = (
        df.select(
            F.col("game_id"),
            F.col("season"),
            F.lit("H").alias("home_away"),
            F.col("home.team_id").alias("team_id"),
            F.col("home.team_city").alias("team_city"),
            F.col("home.team_name").alias("team_name"),
            F.col("away.team_id").alias("opponent_team_id"),
            F.col("home.score").cast("int").alias("points"),
        )
    )
    away = (
        df.select(
            F.col("game_id"),
            F.col("season"),
            F.lit("A").alias("home_away"),
            F.col("away.team_id").alias("team_id"),
            F.col("away.team_city").alias("team_city"),
            F.col("away.team_name").alias("team_name"),
            F.col("home.team_id").alias("opponent_team_id"),
            F.col("away.score").cast("int").alias("points"),
        )
    )
    points = home.unionByName(away).where(F.col("team_id").isNotNull())

    base = os.path.join(warehouse_dir, "bigdata")
    parquet_dir = os.path.join(base, "team_game_points")
    points.write.mode("overwrite").parquet(parquet_dir)

    teams_dim = (
        points.groupBy("team_id")
        .agg(
            F.first("team_city", ignorenulls=True).alias("team_city"),
            F.first("team_name", ignorenulls=True).alias("team_name"),
        )
        .orderBy("team_id")
    )
    teams_parquet = os.path.join(base, "teams_dim")
    teams_dim.write.mode("overwrite").parquet(teams_parquet)

    tsv_points_dir = os.path.join(base, "team_game_points_tsv")
    (
        points.select("season", "team_id", "points", "game_id")
        .coalesce(1)
        .write.mode("overwrite").option("sep", "\t").option("header", "false").csv(tsv_points_dir)
    )

    tsv_teams_dir = os.path.join(base, "teams_dim_tsv")
    (
        teams_dim.select("team_id", "team_city", "team_name")
        .coalesce(1)
        .write.mode("overwrite").option("sep", "\t").option("header", "false").csv(tsv_teams_dir)
    )

    spark.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", required=True, help="Path to games.jsonl")
    ap.add_argument("--warehouse", default="warehouse", help="Base warehouse dir")
    args = ap.parse_args()

    prepare(args.games, args.warehouse)


if __name__ == "__main__":
    main()

