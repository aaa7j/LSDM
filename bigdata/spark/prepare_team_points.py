import argparse
import os
from typing import Optional
from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "PrepareTeamPoints"):
    parts = max(2, (os.cpu_count() or 4) * 2)
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(parts))
        .getOrCreate()
    )


def _read_points_from_postgres(spark: SparkSession, url: str, user: str, password: str,
                               table: str = "staging.team_game_points"):
    try:
        return (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as e:
        print(f"[postgres] read failed: {e}")
        return None


def _read_teams_dim_from_postgres(spark: SparkSession, url: str, user: str, password: str,
                                  table: str = "staging.teams_dim"):
    try:
        return (
            spark.read.format("jdbc")
            .option("url", url)
            .option("dbtable", table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    except Exception as e:
        print(f"[postgres] read teams_dim failed: {e}")
        return None


def prepare(games_path: Optional[str], warehouse_dir: str,
            source: str = "auto",
            pg_url: Optional[str] = None,
            pg_user: Optional[str] = None,
            pg_password: Optional[str] = None):
    spark = build_spark()

    points_df = None
    teams_dim_df = None

    # Auto-detect Postgres credentials from env if not provided
    if source in ("auto", "postgres"):
        if not pg_url:
            host = os.environ.get("PG_HOST")
            db = os.environ.get("PG_DB") or os.environ.get("PG_DATABASE")
            port = os.environ.get("PG_PORT", "5432")
            if host and db:
                pg_url = f"jdbc:postgresql://{host}:{port}/{db}"
        if not pg_user:
            pg_user = os.environ.get("PG_USER") or os.environ.get("POSTGRES_USER")
        if not pg_password:
            pg_password = os.environ.get("PG_PASSWORD") or os.environ.get("POSTGRES_PASSWORD")

        if pg_url and pg_user and pg_password:
            print(f"[source] Trying Postgres at {pg_url} ...")
            points_df = _read_points_from_postgres(spark, pg_url, pg_user, pg_password)
            teams_dim_df = _read_teams_dim_from_postgres(spark, pg_url, pg_user, pg_password)
        else:
            if source == "postgres":
                print("[source] Postgres selected but credentials missing; falling back to files")

    if points_df is None:
        # Fallback: read from games JSON (original behaviour)
        if not games_path:
            raise SystemExit("games_path required when source is not postgres")
        print(f"[source] Reading games from file: {games_path}")
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
    else:
        # Align column names to expected schema
        cols = [c.lower() for c in points_df.columns]
        for c in points_df.columns:
            points_df = points_df.withColumnRenamed(c, c.lower())
        required = {"game_id", "season", "home_away", "team_id", "team_city", "team_name", "opponent_team_id", "points"}
        missing = [c for c in required if c not in set(points_df.columns)]
        if missing:
            print(f"[postgres] WARNING: missing columns in team_game_points: {missing}")
        points = points_df

    base = os.path.join(warehouse_dir, "bigdata")
    parquet_dir = os.path.join(base, "team_game_points")
    points.write.mode("overwrite").partitionBy("season").parquet(parquet_dir)

    if teams_dim_df is not None:
        td = teams_dim_df
        for c in td.columns:
            td = td.withColumnRenamed(c, c.lower())
        if "team_id" not in td.columns:
            td = None
    else:
        td = None

    teams_dim = td or (
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
    ap.add_argument("--games", required=False, help="Path to games.jsonl (fallback when not using Postgres)")
    ap.add_argument("--warehouse", default="warehouse", help="Base warehouse dir")
    ap.add_argument("--source", choices=["auto", "postgres", "file"], default="auto", help="Input source for Spark prepare")
    ap.add_argument("--pg-url", default=None)
    ap.add_argument("--pg-user", default=None)
    ap.add_argument("--pg-password", default=None)
    args = ap.parse_args()

    prepare(args.games, args.warehouse, source=args.source, pg_url=args.pg_url, pg_user=args.pg_user, pg_password=args.pg_password)


if __name__ == "__main__":
    main()
