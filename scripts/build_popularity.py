"""
Build lightweight popularity signals to improve search/suggest ranking.

Outputs (Parquet by default) under --out (default: metadata/popularity):
 - players/  with columns: player_id (string), score (double), cnt (long)
 - teams/    with columns: team_id (string), score (double), cnt (long)

Signals used:
 - Players: recency‑weighted scoring events joined to GLOBAL_GAME.
   Prefers GLOBAL_SCORING_EVENTS (from GAV). Falls back to computing on
   GLOBAL_PLAY_BY_PLAY if scoring events parquet is not available.
 - Teams: recency‑weighted game appearances from GLOBAL_GAME.

Usage:
  python scripts/build_popularity.py --warehouse warehouse --out metadata/popularity \
      --years 5 --half-life-days 540 --driver-mem 4g
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
import math

# --- Windows-friendly env (match other scripts) ---
import os

JAVA_HOME_DEFAULT = r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
JAVA_HOME = os.environ.get("JAVA_HOME", JAVA_HOME_DEFAULT)
if JAVA_HOME and JAVA_HOME.lower().endswith("bin\\java.exe"):
    import os as _os
    JAVA_HOME = _os.path.dirname(_os.path.dirname(JAVA_HOME))
if JAVA_HOME and os.path.isdir(JAVA_HOME):
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

TMP_DIR = os.environ.get("SPARK_LOCAL_DIRS", r"C:\\spark-tmp")
os.makedirs(TMP_DIR, exist_ok=True)
os.environ.setdefault("TMP", TMP_DIR)
os.environ.setdefault("TEMP", TMP_DIR)
os.environ.setdefault("SPARK_LOCAL_DIRS", TMP_DIR)
os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")


def _get_spark(driver_mem: str = "4g", shuffle_parts: int = 8) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("LSDM-BuildPopularity")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", TMP_DIR)
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")
        .config("spark.driver.memory", driver_mem)
        .config("spark.sql.shuffle.partitions", str(shuffle_parts))
        .config("spark.sql.inMemoryColumnarStorage.compressed", "false")
        .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _register_from_warehouse(spark: SparkSession, base: str) -> None:
    base = os.fspath(base)
    # Always required
    required = {
        "GLOBAL_PLAYER": "global_player",
        "GLOBAL_GAME": "global_game",
    }
    for view, sub in required.items():
        path = os.path.join(base, sub)
        if not os.path.isdir(path):
            raise SystemExit(f"Missing Parquet path: {path}")
        spark.read.parquet(path).createOrReplaceTempView(view)

    # Prefer precomputed scoring events, else fall back to raw PBP
    se_path = os.path.join(base, "global_scoring_events")
    pbp_path = os.path.join(base, "global_play_by_play")
    if os.path.isdir(se_path):
        spark.read.parquet(se_path).createOrReplaceTempView("GLOBAL_SCORING_EVENTS")
    elif os.path.isdir(pbp_path):
        spark.read.parquet(pbp_path).createOrReplaceTempView("GLOBAL_PLAY_BY_PLAY")
    else:
        raise SystemExit(
            "Missing Parquet path: expected either 'global_scoring_events' or 'global_play_by_play'"
        )
    # No caching here to avoid OOM on large PBP; we only read the needed columns


def main() -> None:
    ap = argparse.ArgumentParser(description="Build popularity maps for players and teams")
    ap.add_argument("--warehouse", default="warehouse", help="Parquet warehouse with GLOBAL_* subfolders")
    ap.add_argument("--out", default="metadata/popularity", help="Output directory for popularity parquet")
    ap.add_argument("--driver-mem", default="4g", help="Spark driver memory (e.g., 2g, 4g)")
    ap.add_argument("--shuffle-partitions", type=int, default=8, help="spark.sql.shuffle.partitions")
    ap.add_argument("--years", type=int, default=5, help="Use last N years of data for recency weighting")
    ap.add_argument("--half-life-days", type=int, default=540, help="Half-life for recency weight in days")
    args = ap.parse_args()

    spark = _get_spark(driver_mem=args.driver_mem, shuffle_parts=args.shuffle_partitions)
    _register_from_warehouse(spark, args.warehouse)

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    # Players: recency-weighted scoring contributions keyed by player_id
    # --- recency window using GLOBAL_GAME ---
    baseg = spark.table("GLOBAL_GAME").select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("game_date").cast("date").alias("game_date"),
        F.col("home_team_id").cast("string").alias("home_team_id"),
        F.col("away_team_id").cast("string").alias("away_team_id"),
    )
    game = baseg.select("game_id", "game_date")
    maxd = game.select(F.max("game_date").alias("d")).collect()[0]["d"]
    cutoff = None
    if maxd is not None:
        cutoff = spark.sql(f"SELECT DATE_SUB(DATE('{maxd}'), {args.years*365}) AS c").collect()[0]["c"]

    if cutoff is not None:
        game = game.where(F.col("game_date") >= F.lit(cutoff))

    # recency weight with half‑life
    ln2 = math.log(2.0)
    half = max(1, int(args.half_life_days))
    days_old = F.datediff(F.lit(maxd), F.col("game_date")) if maxd is not None else F.lit(0)
    weight = F.exp(F.lit(-ln2) * (days_old / F.lit(float(half))))

    tables = {t.name.upper() for t in spark.catalog.listTables()}
    if "GLOBAL_SCORING_EVENTS" in tables:
        # Use precomputed scoring events: faster and more robust
        ev = spark.table("GLOBAL_SCORING_EVENTS").select(
            F.col("game_id").cast("string").alias("game_id"),
            F.col("player_id").cast("string").alias("p1"),
            F.col("assist_id").cast("string").alias("ast"),
            F.col("points").cast("int").alias("points"),
        )
        ev = ev.join(game, on="game_id", how="inner").withColumn("w", weight)
        p1 = ev.where(F.col("p1").isNotNull()).select(
            F.col("p1").alias("player_id"), (F.col("w") * (F.col("points") + F.lit(0.5))).alias("score")
        )
        a1 = ev.where(F.col("ast").isNotNull()).select(
            F.col("ast").alias("player_id"), (F.col("w") * F.lit(0.25)).alias("score")
        )
        ppl = p1.unionByName(a1)
        players = (
            ppl.groupBy("player_id").
            agg(F.sum("score").alias("score"), F.count(F.lit(1)).alias("cnt")).
            coalesce(1)
        )
    else:
        # Fallback to raw PBP (older path): compute per-event points then credit
        pbp = spark.table("GLOBAL_PLAY_BY_PLAY").select(
            F.col("game_id").cast("string").alias("game_id"),
            F.col("eventnum").cast("int").alias("eventnum"),
            F.col("score").alias("score"),
            F.col("player1_id").cast("string").alias("p1"),
            F.col("player2_id").cast("string").alias("p2"),
            F.col("player3_id").cast("string").alias("p3"),
        )
        pbp = pbp.join(game, on="game_id", how="inner")
        pbp = pbp.withColumn("score_clean", F.regexp_replace("score", "\\s+", ""))
        pbp = pbp.withColumn("sa", F.split("score_clean", "-"))
        pbp = pbp.withColumn("h", F.when(F.size("sa") == 2, F.element_at("sa", 1).cast("int")))
        pbp = pbp.withColumn("a", F.when(F.size("sa") == 2, F.element_at("sa", 2).cast("int")))
        w = Window.partitionBy("game_id").orderBy(F.col("eventnum").asc())
        pbp = pbp.withColumn("ph", F.coalesce(F.lag("h").over(w), F.lit(0)))
        pbp = pbp.withColumn("pa", F.coalesce(F.lag("a").over(w), F.lit(0)))
        pbp = pbp.withColumn("delta", F.greatest(F.col("h") - F.col("ph"), F.col("a") - F.col("pa")))
        pbp = pbp.withColumn("points", F.when(F.col("delta").isNotNull(), F.col("delta")).otherwise(F.lit(0)))
        pbp = pbp.withColumn("w", weight)

        p1 = pbp.where(F.col("p1").isNotNull()).select(
            F.col("p1").alias("player_id"), (F.col("w") * (F.col("points") + F.lit(0.5))).alias("score")
        )
        p2 = pbp.where(F.col("p2").isNotNull()).select(
            F.col("p2").alias("player_id"), (F.col("w") * F.lit(0.25)).alias("score")
        )
        p3 = pbp.where(F.col("p3").isNotNull()).select(
            F.col("p3").alias("player_id"), (F.col("w") * F.lit(0.25)).alias("score")
        )
        ppl = p1.unionByName(p2).unionByName(p3)
        players = (
            ppl.groupBy("player_id").
            agg(F.sum("score").alias("score"), F.count(F.lit(1)).alias("cnt")).
            coalesce(1)
        )
    players.write.mode("overwrite").parquet(str(out / "players"))

    # Teams: count of games appearances (home+away)
    # Build team-date rows from the base game table that includes team ids
    gbase = baseg
    if cutoff is not None:
        gbase = gbase.where(F.col("game_date") >= F.lit(cutoff))
    g2 = gbase.select(F.col("home_team_id").alias("team_id"), F.col("game_date")).unionByName(
         gbase.select(F.col("away_team_id").alias("team_id"), F.col("game_date"))
    )
    g2 = g2.withColumn("w", F.exp(F.lit(-ln2) * (F.datediff(F.lit(maxd), F.col("game_date")) / F.lit(float(half)))))
    teams = (g2.groupBy("team_id")
             .agg(F.sum("w").alias("score"), F.count(F.lit(1)).alias("cnt"))
             .coalesce(1))
    teams.write.mode("overwrite").parquet(str(out / "teams"))

    print(f"[OK] Wrote popularity Parquet to {out}")
    spark.stop()


if __name__ == "__main__":
    # Make project importable
    ROOT = pathlib.Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    main()
