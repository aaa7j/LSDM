"""
Train a richer single-shot outcome model using all available games.

Features (built from GLOBAL_* tables):
- Rolling team form (avg point diff last 5/10/20 games)
- Rest days per team and back-to-back flags
- Arena/home-court baseline from all-time home win rate (Laplace smoothed)
- Month and day-of-week cyclic encodings
- Expected diff feature (home_roll10 - away_roll10) for continuity

Trains a Spark ML GBTClassifier and saves a PipelineModel.

Usage:
  python scripts/train_full_outcome.py --warehouse warehouse --out models/outcome-full

Set OUTCOME_MODEL_DIR=models/outcome-full in serving to use this model.
"""
from __future__ import annotations

import argparse
import os
import pathlib

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler


def _ensure_windows_env(java_home_override: str | None = None):
    """Best-effort Windows/Spark/Java environment setup.
    Mirrors scripts/run_search_service.py so training works the same way.
    """
    import os
    JAVA_HOME = java_home_override or os.environ.get("JAVA_HOME")
    # If JAVA_HOME mistakenly points to java.exe, normalize to JDK root
    if JAVA_HOME and JAVA_HOME.lower().endswith("bin\\java.exe"):
        JAVA_HOME = os.path.dirname(os.path.dirname(JAVA_HOME))
    if JAVA_HOME and os.path.isfile(JAVA_HOME):
        parent = os.path.dirname(JAVA_HOME)
        if parent.lower().endswith("\\bin"):
            parent = os.path.dirname(parent)
        JAVA_HOME = parent
    if JAVA_HOME and os.path.isdir(JAVA_HOME):
        os.environ["JAVA_HOME"] = JAVA_HOME
        os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

    tmp_dir = os.environ.get("SPARK_LOCAL_DIRS", r"C:\\spark-tmp")
    try:
        os.makedirs(tmp_dir, exist_ok=True)
    except Exception:
        pass
    os.environ.setdefault("TMP", tmp_dir)
    os.environ.setdefault("TEMP", tmp_dir)
    os.environ.setdefault("SPARK_LOCAL_DIRS", tmp_dir)
    os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={tmp_dir} -Dfile.encoding=UTF-8")


def mk_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("LSDM-Train-Full-Outcome")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "3g"))
        .config("spark.local.dir", os.environ.get("SPARK_LOCAL_DIRS", r"C:\\spark-tmp"))
        .getOrCreate()
    )


def register_warehouse_views(spark: SparkSession, base: str):
    views = {
        "GLOBAL_PLAYER": "global_player",
        "GLOBAL_TEAM": "global_team",
        "GLOBAL_GAME": "global_game",
        "GLOBAL_LINE_SCORE": "global_line_score",
        "GLOBAL_OTHER_STATS": "global_other_stats",
    }
    for v, sub in views.items():
        path = os.path.join(base, sub)
        if os.path.isdir(path):
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(v)


def ensure_game_result(spark: SparkSession):
    tbls = {t.name.upper() for t in spark.catalog.listTables()}
    if "GAME_RESULT" in tbls:
        return
    if "GLOBAL_LINE_SCORE" in tbls:
        lp = (
            spark.table("GLOBAL_LINE_SCORE")
            .groupBy("game_id", "team_id")
            .agg(F.sum(F.col("points")).alias("pts"))
        )
        lp.createOrReplaceTempView("GAME_POINTS")
        g = spark.table("GLOBAL_GAME").select(
            F.col("game_id").cast("string").alias("game_id"),
            F.col("game_date").cast("date").alias("game_date"),
            F.col("home_team_id").cast("int").alias("home_team_id"),
            F.col("away_team_id").cast("int").alias("away_team_id"),
        )
        hp = lp.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("pts", "home_pts")
        ap = lp.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("pts", "away_pts")
        gr = (
            g.join(hp, ["game_id", "home_team_id"], "left")
             .join(ap, ["game_id", "away_team_id"], "left")
             .where(F.col("home_pts").isNotNull() & F.col("away_pts").isNotNull())
        )
        gr.createOrReplaceTempView("GAME_RESULT")
    else:
        # Assume GLOBAL_GAME has final scores
        g = spark.table("GLOBAL_GAME").select(
            F.col("game_id").cast("string").alias("game_id"),
            F.col("game_date").cast("date").alias("game_date"),
            F.col("home_team_id").cast("int").alias("home_team_id"),
            F.col("away_team_id").cast("int").alias("away_team_id"),
            F.col("final_score_home").alias("home_pts"),
            F.col("final_score_away").alias("away_pts"),
        )
        gr = g.where(F.col("home_pts").isNotNull() & F.col("away_pts").isNotNull())
        gr.createOrReplaceTempView("GAME_RESULT")


def build_training_frame(spark: SparkSession):
    gr = spark.table("GLOBAL_GAME").select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("game_date").cast("date").alias("date"),
        F.col("home_team_id").cast("int").alias("home_team_id"),
        F.col("away_team_id").cast("int").alias("away_team_id"),
        F.col("location").alias("location"),
    )
    res = spark.table("GAME_RESULT").select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("home_pts").cast("int").alias("home_pts"),
        F.col("away_pts").cast("int").alias("away_pts"),
    )
    base = (
        gr.join(res, on="game_id", how="inner")
          .withColumn("home_win", (F.col("home_pts") > F.col("away_pts")).cast("int"))
    )

    # Arena baseline (all-time home win rate, Laplace smooth)
    arena_key = F.coalesce(F.col("location"), F.col("home_team_id").cast("string"))
    arena_stats = (
        base.withColumn("arena", arena_key)
            .groupBy("arena")
            .agg(
                F.count(F.lit(1)).alias("games"),
                F.sum(F.col("home_win").cast("int")).alias("hw")
            )
            .withColumn("arena_home_edge", (F.col("hw") + F.lit(3.0)) / (F.col("games") + F.lit(6.0)))
            .select("arena", "arena_home_edge")
    )

    # Team stream for rolling stats and rest
    ts_home = base.select(
        F.col("game_id"), F.col("date"),
        F.col("home_team_id").alias("team_id"),
        F.col("away_team_id").alias("opp_id"),
        F.lit(1).alias("is_home"),
        (F.col("home_pts") - F.col("away_pts")).cast("double").alias("diff")
    )
    ts_away = base.select(
        F.col("game_id"), F.col("date"),
        F.col("away_team_id").alias("team_id"),
        F.col("home_team_id").alias("opp_id"),
        F.lit(0).alias("is_home"),
        (F.col("away_pts") - F.col("home_pts")).cast("double").alias("diff")
    )
    ts = ts_home.unionByName(ts_away)
    w_team = Window.partitionBy("team_id").orderBy(F.col("date").asc())
    ts = (
        ts.withColumn("rn", F.row_number().over(w_team))
          .withColumn("rest_days", F.datediff(F.col("date"), F.lag(F.col("date")).over(w_team)))
          .withColumn("b2b", (F.col("rest_days") <= F.lit(1)).cast("int"))
          .withColumn("roll5", F.avg("diff").over(w_team.rowsBetween(-5, -1)))
          .withColumn("roll10", F.avg("diff").over(w_team.rowsBetween(-10, -1)))
          .withColumn("roll20", F.avg("diff").over(w_team.rowsBetween(-20, -1)))
    )

    # Join back to game rows: one row per game with home/away features
    th = ts.select(
        F.col("game_id").alias("gid_h"), F.col("team_id").alias("home_id"),
        F.col("rest_days").alias("home_rest"), F.col("b2b").alias("home_b2b"),
        F.col("roll5").alias("h_roll5"), F.col("roll10").alias("h_roll10"), F.col("roll20").alias("h_roll20"),
    )
    ta = ts.select(
        F.col("game_id").alias("gid_a"), F.col("team_id").alias("away_id"),
        F.col("rest_days").alias("away_rest"), F.col("b2b").alias("away_b2b"),
        F.col("roll5").alias("a_roll5"), F.col("roll10").alias("a_roll10"), F.col("roll20").alias("a_roll20"),
    )

    feat = (
        base.alias("b")
        .join(th, (F.col("b.game_id") == F.col("gid_h")) & (F.col("b.home_team_id") == F.col("home_id")), "left")
        .join(ta, (F.col("b.game_id") == F.col("gid_a")) & (F.col("b.away_team_id") == F.col("away_id")), "left")
        .withColumn("arena", F.coalesce(F.col("b.location"), F.col("b.home_team_id").cast("string")))
        .join(arena_stats, on="arena", how="left")
        .withColumn("rest_diff", (F.col("home_rest") - F.col("away_rest")).cast("double"))
        .withColumn("b2b_any", ((F.col("home_b2b") + F.col("away_b2b")) >= 1).cast("int"))
        .withColumn("expected_diff", (F.col("h_roll10") - F.col("a_roll10")).cast("double"))
        .withColumn("month", F.month(F.col("b.date")).cast("int"))
        .withColumn("dow", F.dayofweek(F.col("b.date")).cast("int"))
        .withColumn("month_sin", F.sin(2 * 3.14159265 * F.col("month") / F.lit(12.0)))
        .withColumn("month_cos", F.cos(2 * 3.14159265 * F.col("month") / F.lit(12.0)))
        .withColumn("dow_sin", F.sin(2 * 3.14159265 * F.col("dow") / F.lit(7.0)))
        .withColumn("dow_cos", F.cos(2 * 3.14159265 * F.col("dow") / F.lit(7.0)))
        .select(
            F.col("b.game_id").alias("game_id"),
            F.col("b.home_win").alias("label"),
            F.col("arena_home_edge").cast("double").alias("arena_home_edge"),
            F.col("expected_diff").alias("expected_diff"),
            F.col("rest_diff").alias("rest_diff"),
            F.col("b2b_any").cast("double").alias("b2b_any"),
            F.col("month_sin").alias("month_sin"),
            F.col("month_cos").alias("month_cos"),
            F.col("dow_sin").alias("dow_sin"),
            F.col("dow_cos").alias("dow_cos"),
        )
        .na.fill({"arena_home_edge": 0.5, "expected_diff": 0.0, "rest_diff": 0.0, "b2b_any": 0.0})
    )
    return feat


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", default="warehouse")
    ap.add_argument("--out", default=os.path.join("models", "outcome-full"))
    ap.add_argument("--java-home", default=None, help="Override JAVA_HOME (JDK root directory)")
    ap.add_argument("--driver-mem", default="3g", help="Spark driver memory, e.g. 3g or 4096m")
    ap.add_argument("--iters", type=int, default=120, help="GBT max iterations (trees)")
    ap.add_argument("--depth", type=int, default=5, help="GBT max tree depth")
    ap.add_argument("--subsample", type=float, default=0.8, help="GBT subsampling rate [0,1]")
    args = ap.parse_args()

    # Prepare Windows env so Spark can start (JAVA_HOME, temp dirs)
    _ensure_windows_env(args.java_home)

    # Memory tuning
    os.environ["SPARK_DRIVER_MEMORY"] = args.driver_mem
    spark = mk_spark()
    try:
        register_warehouse_views(spark, args.warehouse)
        ensure_game_result(spark)

        df = build_training_frame(spark)
        # Materialize and persist to avoid recomputation during GBT
        df = df.repartition(8).cache()
        _ = df.count()
        # Balance check
        labs = df.groupBy("label").count().orderBy("label").collect()
        print("Class distribution:", [(int(r[0]), int(r[1])) for r in labs])

        features = [
            "arena_home_edge",
            "expected_diff",
            "rest_diff",
            "b2b_any",
            "month_sin", "month_cos",
            "dow_sin", "dow_cos",
        ]
        assembler = VectorAssembler(inputCols=features, outputCol="features")
        clf = GBTClassifier(
            featuresCol="features",
            labelCol="label",
            maxIter=int(args.iters),
            maxDepth=int(args.depth),
            stepSize=0.05,
            subsamplingRate=float(args.subsample),
            cacheNodeIds=True,
        )
        pipe = Pipeline(stages=[assembler, clf])

        model = pipe.fit(df)
        out_dir = args.out
        os.makedirs(out_dir, exist_ok=True)
        model.write().overwrite().save(out_dir)
        print(f"[OK] Trained outcome model saved to: {out_dir}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
