"""
FastAPI search service that keeps a single long‑lived PySpark session.

Notes
- Spark is created on FastAPI startup and stopped on shutdown.
- GLOBAL_* tables are loaded from the local Parquet warehouse and cached.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

from src.search.engine import run_query
from src.analytics.outcome import (
    FEATURE_DIFFS,
    build_pregame_features,
    build_match_features,
    load_pipeline_model,
    train_pregame_outcome_model,
)
from src.analytics.game_prediction import build_game_feature_frame, train_outcome_model as train_simple_outcome
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator


WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "warehouse")

GLOBAL_PARQUET = {
    # Views needed for search and predictions
    "GLOBAL_PLAYER": "global_player",
    "GLOBAL_TEAM": "global_team",
    "GLOBAL_GAME": "global_game",
    "GLOBAL_LINE_SCORE": "global_line_score",
    "GLOBAL_OTHER_STATS": "global_other_stats",
}


def _mk_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("LSDM-SearchService")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
    )
    # Respect SPARK_LOCAL_DIRS if provided (Windows temp location)
    local_dirs = os.environ.get("SPARK_LOCAL_DIRS")
    if local_dirs:
        builder = builder.config("spark.local.dir", local_dirs)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    spark = builder.getOrCreate()
    # Reduce noisy WARNs like FileStreamSink/metadata when probing non-existent paths
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def _register_views_from_warehouse(spark: SparkSession, base: str = WAREHOUSE_DIR):
    # Load Parquet folders and register temp views
    print(f"[INIT] Registering GLOBAL_* from: {os.path.abspath(base)}")
    for view_name, sub in GLOBAL_PARQUET.items():
        path = os.path.join(base, sub)
        if not os.path.isdir(path):
            print(f"[WARN] Missing path for {view_name}: {path}")
            continue
        print(f"[INIT] -> {view_name} from {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(view_name)
    # Cache for faster responses
    for view_name in GLOBAL_PARQUET.keys():
        if view_name in [t.name for t in spark.catalog.listTables()]:
            spark.catalog.cacheTable(view_name)
    # Build helper views to reconstruct game results from line scores if needed
    try:
        if "GLOBAL_LINE_SCORE" in [t.name for t in spark.catalog.listTables()]:
            lp = (spark.table("GLOBAL_LINE_SCORE")
                  .groupBy("game_id", "team_id")
                  .agg(F.sum(F.col("points")).alias("pts")))
            lp.createOrReplaceTempView("GAME_POINTS")
            g = spark.table("GLOBAL_GAME").select(
                F.col("game_id").cast("string").alias("game_id"),
                F.col("game_date").cast("date").alias("game_date"),
                F.col("home_team_id").cast("int").alias("home_team_id"),
                F.col("away_team_id").cast("int").alias("away_team_id"),
            )
            hp = (lp
                  .withColumnRenamed("team_id", "home_team_id")
                  .withColumnRenamed("pts", "home_pts"))
            ap = (lp
                  .withColumnRenamed("team_id", "away_team_id")
                  .withColumnRenamed("pts", "away_pts"))
            gr = (g
                  .join(hp, ["game_id", "home_team_id"], "left")
                  .join(ap, ["game_id", "away_team_id"], "left")
                  .where(F.col("home_pts").isNotNull() & F.col("away_pts").isNotNull()))
            gr.createOrReplaceTempView("GAME_RESULT")
            spark.catalog.cacheTable("GAME_POINTS")
            spark.catalog.cacheTable("GAME_RESULT")
    except Exception as e:
        print(f"[WARN] Could not prepare GAME_RESULT from GLOBAL_LINE_SCORE: {e}")
    print("[OK] GLOBAL_* views registered and cached (where available)")


def build_labels_and_train_model(spark: SparkSession) -> Optional[PipelineModel]:
    """Build labels from GLOBAL_LINE_SCORE, compute pre-game cum-avg feature, train LR.
    Returns a PipelineModel or None if dataset remains single-class.
    """
    try:
        # Ensure helper views exist (GAME_POINTS/GAME_RESULT)
        tbls = {t.name for t in spark.catalog.listTables()}
        if "GAME_RESULT" not in tbls:
            spark.sql(
                """
                CREATE OR REPLACE TEMP VIEW GAME_POINTS AS
                SELECT game_id, team_id, SUM(points) AS pts
                FROM GLOBAL_LINE_SCORE
                GROUP BY game_id, team_id
                """
            )
            spark.sql(
                """
                CREATE OR REPLACE TEMP VIEW GAME_RESULT AS
                SELECT
                    g.game_id,
                    CAST(g.game_date AS DATE) AS game_date,
                    CAST(g.home_team_id AS INT) AS home_team_id,
                    CAST(g.away_team_id AS INT) AS away_team_id,
                    hp.pts AS home_pts,
                    ap.pts AS away_pts,
                    CASE WHEN hp.pts > ap.pts THEN 1 ELSE 0 END AS home_win
                FROM GLOBAL_GAME g
                INNER JOIN GAME_POINTS hp
                    ON hp.game_id = g.game_id AND hp.team_id = g.home_team_id
                INNER JOIN GAME_POINTS ap
                    ON ap.game_id = g.game_id AND ap.team_id = g.away_team_id
                """
            )
            spark.sql("CACHE TABLE GAME_POINTS")
            spark.sql("CACHE TABLE GAME_RESULT")

        # Determine cutoff to cap training horizon (speeds up on large warehouses)
        train_years = int(os.environ.get("TRAIN_YEARS", "1"))
        maxd = spark.sql("SELECT MAX(CAST(game_date AS DATE)) AS d FROM GAME_RESULT").collect()[0]["d"]
        cutoff_expr = f"DATE_SUB(DATE('{maxd}'), {train_years*365})" if maxd else None

        # Team daily diff and cumulative averages (restricted to last N years)
        if cutoff_expr:
            spark.sql(
                f"""
                CREATE OR REPLACE TEMP VIEW TEAM_DAILY_DIFF AS
                SELECT game_date, home_team_id AS team_id, CAST(home_pts - away_pts AS DOUBLE) AS diff
                FROM GAME_RESULT WHERE game_date >= {cutoff_expr}
                UNION ALL
                SELECT game_date, away_team_id AS team_id, CAST(away_pts - home_pts AS DOUBLE) * (-1.0) AS diff
                FROM GAME_RESULT WHERE game_date >= {cutoff_expr}
                """
            )
        else:
            spark.sql(
                """
                CREATE OR REPLACE TEMP VIEW TEAM_DAILY_DIFF AS
                SELECT game_date, home_team_id AS team_id, CAST(home_pts - away_pts AS DOUBLE) AS diff FROM GAME_RESULT
                UNION ALL
                SELECT game_date, away_team_id AS team_id, CAST(away_pts - home_pts AS DOUBLE) * (-1.0) AS diff FROM GAME_RESULT
                """
            )
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW TEAM_CUMAVG AS
            SELECT
                team_id,
                game_date,
                AVG(diff) OVER (
                    PARTITION BY team_id
                    ORDER BY game_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cum_avg_diff
            FROM TEAM_DAILY_DIFF
            """
        )

        train_df = spark.sql(
            """
            SELECT r.game_id,
                   r.home_win AS label,
                   AVG(COALESCE(h.cum_avg_diff, 0.0) - COALESCE(a.cum_avg_diff, 0.0)) AS expected_diff
            FROM GAME_RESULT r
            LEFT JOIN TEAM_CUMAVG h ON h.team_id = r.home_team_id AND h.game_date < r.game_date
            LEFT JOIN TEAM_CUMAVG a ON a.team_id = r.away_team_id AND a.game_date < r.game_date
            GROUP BY r.game_id, r.home_win
            HAVING r.home_win IS NOT NULL
            """
        ).where(F.col("expected_diff").isNotNull())

        lbl = [r[0] for r in train_df.groupBy("label").count().collect()]
        if len(lbl) < 2:
            print("[WARN] After rebuild, training still single-class. Model not trained.")
            return None

        assembler = VectorAssembler(inputCols=["expected_diff"], outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
        pipe = Pipeline(stages=[assembler, lr])
        model = pipe.fit(train_df)
        print("[OK] Outcome model (pre-game cum-avg diff) trained.")
        return model
    except Exception as e:
        print(f"[WARN] build_labels_and_train_model failed: {e}")
        return None


app = FastAPI(title="LSDM Search Service", version="1.0.0")


# Global Spark handle (initialized on startup)
spark: Optional[SparkSession] = None
outcome_model: Optional[PipelineModel] = None
MODEL_DIR = os.environ.get("OUTCOME_MODEL_DIR", os.path.join("models", "home-win"))
FORM_WINDOW = int(os.environ.get("FORM_WINDOW", "10"))
# Default to 1 year as requested
FORM_YEARS = int(os.environ.get("FORM_YEARS", "1"))
ACTIVE_FORM_WINDOW = FORM_WINDOW
ACTIVE_FORM_YEARS = FORM_YEARS


@app.on_event("startup")
def _on_startup():
    global spark, outcome_model, ACTIVE_FORM_WINDOW, ACTIVE_FORM_YEARS
    spark = _mk_spark()
    _register_views_from_warehouse(spark)
    # Optional warmup to trigger JVM init and cache population
    try:
        spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 1").collect()
        spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 1").collect()
        spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 1").collect()
    except Exception as e:
        print(f"[WARN] Warmup failed: {e}")
    # Train minimal pre-game model (cum-avg diff). Keep in memory.
    global outcome_model
    try:
        outcome_model = build_labels_and_train_model(spark)
        if outcome_model is None:
            print("[WARN] Outcome model unavailable (single-class or empty TRAIN_SET)")
        else:
            print("[OK] Outcome model ready.")
        return
    except Exception as e:
        print(f"[WARN] Outcome model unavailable: {e}")
        return
    # Load or train outcome model
    try:
        # Try load existing model and verify metadata matches current config (avoid loading if folder missing)
        m = None
        if os.path.isdir(MODEL_DIR) and os.path.isdir(os.path.join(MODEL_DIR, "stages")) and os.path.isdir(os.path.join(MODEL_DIR, "metadata")):
            m = load_pipeline_model(MODEL_DIR)
        metadata_path = os.path.join(MODEL_DIR, "metadata.json")
        need_retrain = True
        if m is not None and os.path.isfile(metadata_path):
            try:
                with open(metadata_path, "r", encoding="utf-8") as fh:
                    meta = json.load(fh)
                if int(meta.get("years", 0)) == FORM_YEARS and int(meta.get("window", 0)) == FORM_WINDOW:
                    need_retrain = False
                # sync active params to metadata when loading existing model
                ACTIVE_FORM_YEARS = int(meta.get("years", FORM_YEARS))
                ACTIVE_FORM_WINDOW = int(meta.get("window", FORM_WINDOW))
            except Exception:
                need_retrain = True
        if need_retrain:
            print("[INIT] Training outcome model (pre-game features)...")
            # Try with configured window/years; if no features, relax params
            train_window, train_years = FORM_WINDOW, FORM_YEARS
            try:
                feats = build_pregame_features(spark, window_n=train_window, years=train_years)
                if feats.limit(1).count() == 0:
                    print(f"[WARN] No training features with years={train_years}, window={train_window}. Trying fallback...")
                    # First fallback: extend years
                    train_years = max(2, FORM_YEARS)
                    feats = build_pregame_features(spark, window_n=train_window, years=train_years)
                    if feats.limit(1).count() == 0:
                        # Second fallback: shrink window
                        train_window = max(3, FORM_WINDOW // 2)
                        feats = build_pregame_features(spark, window_n=train_window, years=train_years)
                        if feats.limit(1).count() == 0:
                            raise RuntimeError("No training features available even after pre-game fallbacks")
            except Exception as e:
                print(f"[WARN] Pre-game feature build failed: {e}")
                # Absolute fallback: simple per-game diffs (no rolling form)
                print("[INIT] Falling back to simple outcome model (per-game diffs)")
                # Rebuild simple features with safe casts to avoid type mismatches
                # Use GAME_RESULT when available to ensure final scores exist
                table_names = {t.name for t in spark.catalog.listTables()}
                if "GAME_RESULT" in table_names:
                    g = (spark.table("GAME_RESULT")
                            .select(
                                F.col("game_id").cast("string").alias("game_id"),
                                F.col("game_date").cast("date").alias("game_date"),
                                F.col("home_team_id").cast("int").alias("home_team_id"),
                                F.col("away_team_id").cast("int").alias("away_team_id"),
                                F.col("home_pts").cast("int").alias("final_score_home"),
                                F.col("away_pts").cast("int").alias("final_score_away"),
                            ))
                else:
                    g = (spark.table("GLOBAL_GAME")
                            .select(
                                F.col("game_id").cast("string").alias("game_id"),
                                F.col("game_date"),
                                F.col("home_team_id").cast("int").alias("home_team_id"),
                                F.col("away_team_id").cast("int").alias("away_team_id"),
                                F.col("final_score_home"),
                                F.col("final_score_away"),
                            ))
                s = (spark.table("GLOBAL_OTHER_STATS")
                     .select(
                         F.col("game_id").cast("string").alias("game_id"),
                         F.col("team_id").cast("int").alias("team_id"),
                         F.col("points"),
                         F.col("rebounds"), F.col("assists"), F.col("steals"), F.col("blocks"), F.col("turnovers"), F.col("fouls"),
                     ))
                home = s.alias("home")
                away = s.alias("away")
                # First try: inner-join both sides so both stats exist; label from final scores, else points
                simple_inner = (g.alias("g")
                          .join(home,
                                (F.col("g.game_id") == F.col("home.game_id")) & (F.col("g.home_team_id") == F.col("home.team_id")),
                                "inner")
                          .join(away,
                                (F.col("g.game_id") == F.col("away.game_id")) & (F.col("g.away_team_id") == F.col("away.team_id")),
                                "inner")
                          .select(
                              F.col("g.game_id").alias("game_id"),
                              F.col("g.game_date").alias("game_date"),
                              F.when(
                                  (F.col("g.final_score_home").isNotNull()) & (F.col("g.final_score_away").isNotNull()),
                                  (F.col("g.final_score_home") > F.col("g.final_score_away")).cast("double")
                              ).otherwise(
                                  (F.coalesce(F.col("home.points"), F.lit(0)) > F.coalesce(F.col("away.points"), F.lit(0))).cast("double")
                              ).alias("label"),
                              (F.coalesce(F.col("home.rebounds"), F.lit(0.0)) - F.coalesce(F.col("away.rebounds"), F.lit(0.0))).alias("rebounds_diff"),
                              (F.coalesce(F.col("home.assists"), F.lit(0.0)) - F.coalesce(F.col("away.assists"), F.lit(0.0))).alias("assists_diff"),
                              (F.coalesce(F.col("home.steals"), F.lit(0.0)) - F.coalesce(F.col("away.steals"), F.lit(0.0))).alias("steals_diff"),
                              (F.coalesce(F.col("home.blocks"), F.lit(0.0)) - F.coalesce(F.col("away.blocks"), F.lit(0.0))).alias("blocks_diff"),
                              (F.coalesce(F.col("home.turnovers"), F.lit(0.0)) - F.coalesce(F.col("away.turnovers"), F.lit(0.0))).alias("turnovers_diff"),
                              (F.coalesce(F.col("home.fouls"), F.lit(0.0)) - F.coalesce(F.col("away.fouls"), F.lit(0.0))).alias("fouls_diff"),
                          )
                          .where(F.col("label").isNotNull())
                          )
                ds = simple_inner
                if ds.limit(1).count() == 0:
                    # Second try: left joins (keep more rows), label strictly from final scores
                    ds = (g.alias("g")
                          .join(home,
                                (F.col("g.game_id") == F.col("home.game_id")) & (F.col("g.home_team_id") == F.col("home.team_id")),
                                "left")
                          .join(away,
                                (F.col("g.game_id") == F.col("away.game_id")) & (F.col("g.away_team_id") == F.col("away.team_id")),
                                "left")
                          .select(
                              F.col("g.game_id").alias("game_id"),
                              F.col("g.game_date").alias("game_date"),
                              (F.col("g.final_score_home") > F.col("g.final_score_away")).cast("double").alias("label"),
                              (F.coalesce(F.col("home.rebounds"), F.lit(0.0)) - F.coalesce(F.col("away.rebounds"), F.lit(0.0))).alias("rebounds_diff"),
                              (F.coalesce(F.col("home.assists"), F.lit(0.0)) - F.coalesce(F.col("away.assists"), F.lit(0.0))).alias("assists_diff"),
                              (F.coalesce(F.col("home.steals"), F.lit(0.0)) - F.coalesce(F.col("away.steals"), F.lit(0.0))).alias("steals_diff"),
                              (F.coalesce(F.col("home.blocks"), F.lit(0.0)) - F.coalesce(F.col("away.blocks"), F.lit(0.0))).alias("blocks_diff"),
                              (F.coalesce(F.col("home.turnovers"), F.lit(0.0)) - F.coalesce(F.col("away.turnovers"), F.lit(0.0))).alias("turnovers_diff"),
                              (F.coalesce(F.col("home.fouls"), F.lit(0.0)) - F.coalesce(F.col("away.fouls"), F.lit(0.0))).alias("fouls_diff"),
                          )
                          .where(F.col("label").isNotNull())
                          )
                if ds.limit(1).count() == 0:
                    raise RuntimeError("No features available for simple outcome model either")
                simple = ds
                # Ensure both classes exist in training data
                distinct_labels = simple.select("label").distinct().count()
                if distinct_labels < 2:
                    raise RuntimeError("Training set has a single class after filtering; cannot train binary classifier")
                # Train LR on full dataset (avoid random split dropping a class)
                assembler = VectorAssembler(inputCols=[
                    "rebounds_diff","assists_diff","steals_diff","blocks_diff","turnovers_diff","fouls_diff"
                ], outputCol="features_vector")
                scaler = StandardScaler(inputCol="features_vector", outputCol="features", withMean=True, withStd=True)
                lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
                pipeline = Pipeline(stages=[assembler, scaler, lr])
                m = pipeline.fit(simple)
                preds = m.transform(simple)
                # Compute simple accuracy metric (on train set)
                total = preds.count()
                correct = preds.filter(F.col("prediction") == F.col("label")).count()
                acc = float(correct)/float(total) if total else 0.0
                metrics = {"train_rows": float(total), "accuracy": float(acc)}
                # Record that this model was trained in 'simple' mode
                train_years, train_window = 0, 0
            else:
                m, metrics = train_pregame_outcome_model(spark, window_n=train_window, years=train_years)
            os.makedirs(MODEL_DIR, exist_ok=True)
            m.write().overwrite().save(MODEL_DIR)
            # Save sidecar metadata for future compatibility checks
            try:
                with open(metadata_path, "w", encoding="utf-8") as fh:
                    json.dump({"years": train_years, "window": train_window, "metrics": metrics}, fh)
            except Exception as e:
                print(f"[WARN] Could not write model metadata: {e}")
            ACTIVE_FORM_YEARS, ACTIVE_FORM_WINDOW = train_years, train_window
            print(f"[OK] Outcome model trained and saved to {MODEL_DIR}. Metrics: {metrics}  (years={train_years}, window={train_window})")
            outcome_model = m
        else:
            outcome_model = m
            print(f"[OK] Outcome model loaded from {MODEL_DIR}")
    except Exception as e:
        print(f"[WARN] Outcome model unavailable: {e}")


@app.on_event("shutdown")
def _on_shutdown():
    global spark, outcome_model
    try:
        if spark is not None:
            spark.stop()
    finally:
        spark = None
        outcome_model = None


class SearchResponse(BaseModel):
    total: int
    items: List[Dict]
    meta: Dict


@app.get("/health")
def health():
    if spark is None:
        return {"status": "starting"}
    cnt = spark.sql("SELECT COUNT(*) AS c FROM GLOBAL_PLAYER").collect()[0]["c"]
    return {"status": "ok", "players": cnt}


@app.get("/warmup")
def warmup():
    if spark is None:
        return {"status": "starting"}
    spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 1").collect()
    return {"status": "warmed"}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., description="Search text, e.g. 'michael jordan' or 'lakers 2010'"),
    limit: int = Query(200, ge=1, le=2000, description="candidates per entity"),
):
    if spark is None:
        return {"total": 0, "items": [], "meta": {"query": q, "notes": "service starting"}}
    rows, meta = run_query(spark, q.strip(), top_n=limit)
    return {"total": len(rows), "items": rows, "meta": meta}


def _resolve_team_id(token: str) -> Optional[int]:
    """Resolve a team identifier from id or name/city token."""
    if spark is None:
        return None
    t = spark.table("GLOBAL_TEAM")
    # Numeric id
    if token.isdigit():
        val = int(token)
        row = t.where(F.col("team_id") == val).select("team_id").limit(1).collect()
        return int(row[0][0]) if row else None
    # By name/city (case-insensitive contains)
    token_l = token.strip().lower()
    cand = (
        t.select("team_id", "team_name", "city")
         .where(
            (F.lower(F.col("team_name")).contains(token_l)) |
            (F.lower(F.col("city")).contains(token_l))
         )
         .orderBy(F.col("team_name").asc())
         .limit(1)
         .collect()
    )
    return int(cand[0][0]) if cand else None


@app.get("/predict")
def predict(
    home: str = Query(..., description="Home team (id or name)"),
    away: str = Query(..., description="Away team (id or name)"),
    date: Optional[str] = Query(None, description="Cutoff date YYYY-MM-DD (optional)"),
):
    if spark is None:
        return {"error": "service starting"}
    if outcome_model is None:
        return {"error": "outcome model not available"}

    home_id = _resolve_team_id(home)
    away_id = _resolve_team_id(away)
    if not home_id or not away_id:
        return {"error": "unable to resolve team ids", "home": home, "away": away}

    # Determine cutoff date
    cutoff = date
    if not cutoff:
        tbls = {t.name for t in spark.catalog.listTables()}
        base = "GAME_RESULT" if "GAME_RESULT" in tbls else "GLOBAL_GAME"
        last = spark.table(base).select(F.max(F.col("game_date").cast("date")).alias("d")).collect()[0]["d"]
        cutoff = str(last)

    # Compute single-row expected_diff using TEAM_CUMAVG views (built during training)
    try:
        q = f"""
        SELECT
          (COALESCE(h.cum, 0.0) - COALESCE(a.cum, 0.0)) AS expected_diff
        FROM
          (SELECT AVG(cum_avg_diff) AS cum FROM TEAM_CUMAVG WHERE team_id = {home_id} AND game_date < TO_DATE('{cutoff}')) h
        CROSS JOIN
          (SELECT AVG(cum_avg_diff) AS cum FROM TEAM_CUMAVG WHERE team_id = {away_id} AND game_date < TO_DATE('{cutoff}')) a
        """
        feats = spark.sql(q)
    except Exception as e:
        return {"error": f"feature build failed: {e}"}

    pred = outcome_model.transform(feats).select("probability").collect()[0]
    prob = pred["probability"]
    p_home = float(prob[1])  # class 1 == home win
    p_away = float(1.0 - p_home)
    winner = "home" if p_home >= 0.5 else "away"

    # Resolve names for convenience
    team_df = spark.table("GLOBAL_TEAM").select("team_id", "team_name", "city")
    hname = team_df.where(F.col("team_id") == home_id).select("team_name", "city").limit(1).collect()
    aname = team_df.where(F.col("team_id") == away_id).select("team_name", "city").limit(1).collect()
    home_label = (hname[0][0] or "") + (f" ({hname[0][1]})" if hname and hname[0][1] else "") if hname else str(home_id)
    away_label = (aname[0][0] or "") + (f" ({aname[0][1]})" if aname and aname[0][1] else "") if aname else str(away_id)

    return {
        "home_team_id": home_id,
        "away_team_id": away_id,
        "home": home_label,
        "away": away_label,
        "p_home_win": p_home,
        "p_away_win": p_away,
        "winner": winner,
        "feature": "expected_diff(cum-avg)",
        "cutoff": cutoff,
        "train_years": int(os.environ.get("TRAIN_YEARS", "1")),
    }


# --- CORS (useful for local dev/frontends) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"]
)


# --- Static HTML UI ---
static_dir = os.path.join(os.getcwd(), "web")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir, html=False), name="static")


@app.get("/")
def index_page():
    """Serve a minimal search UI."""
    index_path = os.path.join(static_dir, "index.html")
    return FileResponse(index_path) if os.path.isfile(index_path) else {"message": "UI not found. Create web/index.html."}


@app.get("/predict-ui")
def predict_page():
    """Serve a minimal prediction UI."""
    index_path = os.path.join(static_dir, "predict.html")
    return FileResponse(index_path) if os.path.isfile(index_path) else {"message": "UI not found. Create web/predict.html."}


@app.get("/debug/outcome-status")
def outcome_status():
    if spark is None:
        return {"status": "starting"}
    result = {}
    try:
        counts = {
            "GLOBAL_GAME": int(spark.table("GLOBAL_GAME").count()),
            "GLOBAL_OTHER_STATS": int(spark.table("GLOBAL_OTHER_STATS").count()),
        }
        if "GLOBAL_LINE_SCORE" in [t.name for t in spark.catalog.listTables()]:
            counts["GLOBAL_LINE_SCORE"] = int(spark.table("GLOBAL_LINE_SCORE").count())
        if "GAME_RESULT" in [t.name for t in spark.catalog.listTables()]:
            counts["GAME_RESULT"] = int(spark.table("GAME_RESULT").count())
        result["counts"] = counts
        # Rolling features sample size (fast estimate)
        try:
            roll = build_pregame_features(spark, window_n=ACTIVE_FORM_WINDOW, years=ACTIVE_FORM_YEARS)
            result["pregame_features"] = int(roll.limit(1000).count())  # light probe
        except Exception as e:
            result["pregame_features_error"] = str(e)
        # Simple features sample size (light)
        try:
            g = spark.table("GLOBAL_GAME").select("game_id","game_date","home_team_id","away_team_id","final_score_home","final_score_away")
            s = spark.table("GLOBAL_OTHER_STATS").select("game_id","team_id","rebounds","assists","steals","blocks","turnovers","fouls")
            home = s.alias("home"); away = s.alias("away")
            simple_probe = (g.alias("g")
                .join(home, (F.col("g.game_id") == F.col("home.game_id")) & (F.col("g.home_team_id") == F.col("home.team_id")), "left")
                .join(away, (F.col("g.game_id") == F.col("away.game_id")) & (F.col("g.away_team_id") == F.col("away.team_id")), "left")
                .select((F.col("final_score_home") > F.col("final_score_away")).cast("double").alias("label"))
                .where(F.col("label").isNotNull())
                .limit(1000))
            result["simple_features"] = int(simple_probe.count())
        except Exception as e:
            result["simple_features_error"] = str(e)
        result["model"] = {
            "loaded": outcome_model is not None,
            "model_dir": MODEL_DIR,
            "years": ACTIVE_FORM_YEARS,
            "window": ACTIVE_FORM_WINDOW,
        }
    except Exception as e:
        result["error"] = str(e)
    return result
