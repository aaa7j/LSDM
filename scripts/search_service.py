from __future__ import annotations



# --- In-memory suggest globals & helpers ---
import re
import difflib

_MEM_PLAYERS = []  # list of dicts
_MEM_TEAMS = []    # list of dicts

def _norm(s: str) -> str:
    import re as _re
    s = (s or "").lower().strip()
    s = _re.sub(r"[^a-z0-9 ]+", " ", s)
    s = _re.sub(r"\\s+", " ", s)
    return s

def _build_mem_indexes(spark):
    global _MEM_PLAYERS, _MEM_TEAMS
    try:
        pdf = (
            spark.table("GLOBAL_PLAYER")
            .select(
                F.col("player_id").cast("string").alias("id"),
                F.col("full_name").alias("title"),
                F.col("position").alias("subtitle"),
            )
        )
        _MEM_PLAYERS = [
            {
                "_entity": "player",
                "_id": r["id"],
                "_title": r["title"],
                "_subtitle": r["subtitle"],
                "_extra": None,
                "_norm": _norm(str(r["title"] or "")),
            }
            for r in pdf.collect()
            if (r["id"] is not None and r["title"])
        ]
    except Exception as e:
        print(f"[WARN] Could not build _MEM_PLAYERS: {e}")
        _MEM_PLAYERS = []
    try:
        tdf = (
            spark.table("GLOBAL_TEAM")
            .select(
                F.col("team_id").cast("string").alias("id"),
                F.col("team_name").alias("title"),
                F.col("city").alias("subtitle"),
            )
        )
        _MEM_TEAMS = [
            {
                "_entity": "team",
                "_id": r["id"],
                "_title": r["title"],
                "_subtitle": r["subtitle"],
                "_extra": None,
                "_norm": _norm(f"{str(r['subtitle'] or '')} {str(r['title'] or '')}"),
            }
            for r in tdf.collect()
            if (r["id"] is not None and r["title"])
        ]
    except Exception as e:
        print(f"[WARN] Could not build _MEM_TEAMS: {e}")
        _MEM_TEAMS = []

def _looks_game_query(q: str) -> bool:
    """Heuristics to detect a game-like query (matchup/date/id)."""
    ql = (q or "").lower().strip()
    if not ql:
        return False
    if ' vs ' in ql or ' @ ' in ql:
        return True
    import re as _re
    if _re.search(r"\b(19|20)\d{2}([\-/]\d{1,2}([\-/]\d{1,2})?)?\b", ql):
        return True
    digits = ''.join(c for c in ql if c.isdigit())
    return len(digits) >= 6

def _mem_rank(items, q, limit):
    if not items:
        return []
    qn = _norm(q)
    if not qn:
        return []
    toks = [t for t in qn.split(" ") if t]
    rev = " ".join(reversed(toks)) if toks else qn
    # Dynamic acceptance threshold to avoid irrelevant team spam
    if len(toks) >= 2:
        r_cut = 0.65
    elif len(qn) <= 2:
        r_cut = 0.75
    else:
        r_cut = 0.6
    ranked = []
    for it in items:
        name = it.get("_norm") or _norm(str(it.get("_title") or ""))
        pref = sum(1 for t in toks if name.startswith(t))
        cont = sum(1 for t in toks if t in name)
        r1 = difflib.SequenceMatcher(None, name, qn).ratio()
        r2 = difflib.SequenceMatcher(None, name, rev).ratio()
        r = max(r1, r2)
        # require meaningful match: prefix/contains or decent ratio
        if pref == 0 and cont == 0 and r < r_cut:
            continue
        score = (pref * 5.0) + (cont * 2.0) + (r * 50.0)
        ranked.append((score, it))
    if not ranked:
        return []
    ranked.sort(key=lambda x: x[0], reverse=True)
    out = []
    for sc, it in ranked[:limit]:
        o = {k: v for k, v in it.items() if k != "_norm"}
        o["_score"] = float(sc)
        out.append(o)
    return out

def _quick_suggest(q, limit, entity):
    ent = (entity or "").lower().strip()
    # Let Spark handle game-like queries or explicit game entity
    if ent == "game" or _looks_game_query(q):
        return []
    if ent == "player":
        return _mem_rank(_MEM_PLAYERS, q, limit)
    if ent == "team":
        return _mem_rank(_MEM_TEAMS, q, limit)
    tokens = re.findall(r"[A-Za-z0-9]+", q.lower())
    alpha_tokens = [t for t in tokens if not t.isdigit()]
    prefer_players = len(alpha_tokens) >= 2
    if prefer_players:
        per_p = max(5, int(round(limit * 0.7)))
        p = _mem_rank(_MEM_PLAYERS, q, per_p)
        t = _mem_rank(_MEM_TEAMS, q, max(0, limit - len(p)))
        items = p + t
    else:
        per_t = max(3, int(round(limit * 0.5)))
        t = _mem_rank(_MEM_TEAMS, q, per_t)
        p = _mem_rank(_MEM_PLAYERS, q, max(0, limit - len(t)))
        items = t + p
    seen=set(); out=[]
    for it in items:
        key=(it.get("_entity"), it.get("_id"))
        if key in seen: continue
        seen.add(key); out.append(it)
        if len(out)>=limit: break
    return out


"""
FastAPI search service that keeps a single longâ€‘lived PySpark session.

Notes
- Spark is created on FastAPI startup and stopped on shutdown.
- GLOBAL_* tables are loaded from the local Parquet warehouse and cached.
"""

import os
from typing import Dict, List, Optional
import re

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json

from src.search.engine import run_query
import src.search.engine as search_engine
import math
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
import time


WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "warehouse")
POPULARITY_DIR = os.environ.get("POPULARITY_DIR", os.path.join("metadata", "popularity"))

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


def _ensure_prediction_views(spark: SparkSession) -> None:
    """Create TEAM_DAILY_DIFF and TEAM_CUMAVG used by prediction/training.
    Uses GAME_RESULT if present for reliable scores; restricts to last TRAIN_YEARS.
    """
    tbls = {t.name.upper() for t in spark.catalog.listTables()}
    if "GAME_RESULT" not in tbls:
        # Build GAME_RESULT from GLOBAL_LINE_SCORE if missing
        if "GLOBAL_LINE_SCORE" in tbls:
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
            hp = lp.withColumnRenamed("team_id", "home_team_id").withColumnRenamed("pts", "home_pts")
            ap = lp.withColumnRenamed("team_id", "away_team_id").withColumnRenamed("pts", "away_pts")
            gr = (g
                  .join(hp, ["game_id", "home_team_id"], "left")
                  .join(ap, ["game_id", "away_team_id"], "left")
                  .where(F.col("home_pts").isNotNull() & F.col("away_pts").isNotNull()))
            gr.createOrReplaceTempView("GAME_RESULT")
            spark.catalog.cacheTable("GAME_RESULT")

    # Determine cutoff based on TRAIN_YEARS
    maxd = spark.sql("SELECT MAX(CAST(game_date AS DATE)) AS d FROM GAME_RESULT").collect()[0]["d"] if "GAME_RESULT" in {t.name.upper() for t in spark.catalog.listTables()} else None
    cutoff_expr = f"DATE_SUB(DATE('{maxd}'), {int(os.environ.get('TRAIN_YEARS','1'))*365})" if maxd else None

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


def _load_popularity_maps(spark: SparkSession) -> tuple[dict[str, float], dict[str, float]]:
    """Load popularity Parquet (players, teams) if available and return normalized maps.
    Returns (player_pop, team_pop) with scores in [0,1].
    """
    ppl: dict[str, float] = {}
    tpm: dict[str, float] = {}
    try:
        players_pq = os.path.join(POPULARITY_DIR, "players")
        teams_pq = os.path.join(POPULARITY_DIR, "teams")
        if os.path.isdir(players_pq):
            pdf = spark.read.parquet(players_pq)
            score_col = "score" if "score" in pdf.columns else "cnt"
            pdf = pdf.select(F.col("player_id").cast("string").alias("id"), F.col(score_col).cast("double").alias("score"))
            row = pdf.agg(F.max("score").alias("mx")).collect()[0]
            mx = float(row["mx"]) if row and row["mx"] is not None else 1.0
            for r in pdf.collect():
                c = float(r["score"]) if r["score"] is not None else 0.0
                ppl[str(r["id"])]= (math.log1p(c)/math.log1p(mx)) if mx>0 else 0.0
        if os.path.isdir(teams_pq):
            tdf = spark.read.parquet(teams_pq)
            score_col = "score" if "score" in tdf.columns else "cnt"
            tdf = tdf.select(F.col("team_id").cast("string").alias("id"), F.col(score_col).cast("double").alias("score"))
            row = tdf.agg(F.max("score").alias("mx")).collect()[0]
            mx = float(row["mx"]) if row and row["mx"] is not None else 1.0
            for r in tdf.collect():
                c = float(r["score"]) if r["score"] is not None else 0.0
                tpm[str(r["id"])]= (math.log1p(c)/math.log1p(mx)) if mx>0 else 0.0
    except Exception as e:
        print(f"[WARN] Popularity load failed: {e}")
    return ppl, tpm

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
    # Build views needed for prediction always
    try:
        _ensure_prediction_views(spark)
    except Exception as e:
        print(f"[WARN] Prediction views build failed: {e}")
    # Load popularity maps for better suggestion ranking
    try:
        global _POP_PLAYER, _POP_TEAM
        _POP_PLAYER, _POP_TEAM = _load_popularity_maps(spark)
        print(f"[OK] Popularity maps loaded (players={len(_POP_PLAYER)}, teams={len(_POP_TEAM)})")
    except Exception as e:
        print(f"[WARN] Popularity maps not loaded: {e}")
    # Build fast in-memory indexes for instant suggestions
    try:
        _build_mem_indexes(spark)
        print(f"[OK] In-memory suggest indexes ready (players={len(_MEM_PLAYERS)}, teams={len(_MEM_TEAMS)})")
    except Exception as e:
        print(f"[WARN] Could not build in-memory indexes: {e}")
    # Load model if present; otherwise keep unavailable (no training here)
    try:
        model_path_ok = os.path.isdir(MODEL_DIR) and os.path.isdir(os.path.join(MODEL_DIR, "stages"))
        if model_path_ok:
            outcome_model = PipelineModel.load(MODEL_DIR)
            print(f"[OK] Outcome model loaded from {MODEL_DIR}")
        else:
            print(f"[WARN] Outcome model not found at {MODEL_DIR}. Use scripts/train_outcome.py to train it.")
    except Exception as e:
        print(f"[WARN] Outcome model load/train failed: {e}")
    # (legacy training block removed: serving is load-only)


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
    # Ensure UI field compatibility: map 'title' -> '_title' when needed
    norm_items = []
    for it in rows or []:
        if isinstance(it, dict):
            if "_title" not in it and "title" in it:
                it = {**it, "_title": it.get("title")}
        norm_items.append(it)
    return {"total": len(norm_items), "items": norm_items, "meta": meta}


@app.get("/suggest")
def suggest(
    q: str = Query(..., description="Text to search for suggestions"),
    limit: int = Query(15, ge=1, le=50, description="Max suggestions"),
    entity: Optional[str] = Query(None, description="Filter by entity: player/team/game"),
):
    if spark is None:
        return {"items": []}
    q2 = (q or "").strip()
    if not q2:
        return {"items": []}

    # --- simple in-memory cache for fast repeated suggest ---
    _CACHE_TTL = 20.0
    global _SUGGEST_CACHE  # type: ignore
    try:
        _SUGGEST_CACHE
    except NameError:
        _SUGGEST_CACHE = {}
    key = (q2.lower(), (entity or '').lower(), int(limit))
    now = time.time()
    hit = _SUGGEST_CACHE.get(key)
    if hit and (now - hit[0] <= _CACHE_TTL):
        return {"items": hit[1]}

    rows: list[dict] = []
    ent = (entity or '').strip().lower()
    # quick in-memory fast path
    try:
        fast = _quick_suggest(q2, limit, entity)
        if fast:
            _SUGGEST_CACHE[key] = (now, fast)
            return {"items": fast}
    except Exception:
        pass
    try:
        if ent == 'team':
            df = search_engine.candidate_teams(spark, q2, limit=limit)  # noqa: SLF001
            df2 = (df
                   .select(
                       F.lit('team').alias('_entity'),
                       F.col('_score'),
                       F.col('team_id').cast('string').alias('_id'),
                       F.col('team_name').alias('_title'),
                       F.col('city').alias('_subtitle'),
                       F.lit(None).cast('string').alias('_extra')
                   ))
            rows = [r.asDict(recursive=True) for r in df2.collect()]
        elif ent == 'player':
            df = search_engine.candidate_players(spark, q2, limit=limit)  # noqa: SLF001
            df2 = (df
                   .select(
                       F.lit('player').alias('_entity'),
                       F.col('_score'),
                       F.col('player_id').cast('string').alias('_id'),
                       F.col('full_name').alias('_title'),
                       F.col('position').alias('_subtitle'),
                       F.lit(None).cast('string').alias('_extra')
                   ))
            rows = [r.asDict(recursive=True) for r in df2.collect()]
        elif ent == 'game':
            df = search_engine.candidate_games(spark, q2, limit=limit)  # noqa: SLF001
            df2 = (df.select(
                F.lit('game').alias('_entity'),
                F.col('_score'),
                F.col('_id'),
                F.col('title').alias('_title'),
                F.lit(None).cast('string').alias('_subtitle'),
                F.lit(None).cast('string').alias('_extra')
            ))
            rows = [r.asDict(recursive=True) for r in df2.collect()]
        else:
            # Heuristic: if query looks like a person name (>=2 alpha tokens), fetch players first
            tokens = re.findall(r"[A-Za-z0-9]+", q2.lower())
            alpha_tokens = [t for t in tokens if not t.isdigit()]
            prefer_players = len(alpha_tokens) >= 2

            per = max(3, int(math.ceil(limit / 3)))
            items: list[dict] = []

            def pull_teams() -> list[dict]:
                try:
                    tdf = search_engine.candidate_teams(spark, q2, limit=per)  # noqa: SLF001
                    tdf2 = tdf.select(
                        F.lit('team').alias('_entity'), F.col('_score'),
                        F.col('team_id').cast('string').alias('_id'),
                        F.col('team_name').alias('_title'), F.col('city').alias('_subtitle'), F.lit(None).cast('string').alias('_extra'))
                    return [r.asDict(recursive=True) for r in tdf2.collect()]
                except Exception:
                    return []

            def pull_players() -> list[dict]:
                try:
                    pdf = search_engine.candidate_players(spark, q2, limit=per)  # noqa: SLF001
                    pdf2 = pdf.select(
                        F.lit('player').alias('_entity'), F.col('_score'),
                        F.col('player_id').cast('string').alias('_id'),
                        F.col('full_name').alias('_title'), F.col('position').alias('_subtitle'), F.lit(None).cast('string').alias('_extra'))
                    return [r.asDict(recursive=True) for r in pdf2.collect()]
                except Exception:
                    return []

            def pull_games() -> list[dict]:
                if not _looks_game_query(q2):
                    return []
                try:
                    gdf = search_engine.candidate_games(spark, q2, limit=max(3, limit//3))  # noqa: SLF001
                    gdf2 = gdf.select(
                        F.lit('game').alias('_entity'),
                        F.col('_score'),
                        F.col('_id'),
                        F.col('title').alias('_title'),
                        F.lit(None).cast('string').alias('_subtitle'),
                        F.lit(None).cast('string').alias('_extra')
                    )
                    return [r.asDict(recursive=True) for r in gdf2.collect()]
                except Exception:
                    return []

            if prefer_players:
                items += pull_players() + pull_teams() + pull_games()
            else:
                items += pull_teams() + pull_players() + pull_games()

            # sort by score desc and clip; slight bias: players > teams > games when person-like query
            def rank_key(x: dict):
                typ = x.get('_entity')
                base_boost = 0.02 if typ == 'team' else (0.01 if typ == 'player' else 0.0)
                if prefer_players:
                    base_boost = 0.02 if typ == 'player' else (0.01 if typ == 'team' else 0.0)
                return (x.get('_score') is not None, float(x.get('_score') or 0.0) + base_boost)
            rows = sorted(items, key=rank_key, reverse=True)[:limit]
    except Exception:
        rows = []

    # Popularity-aware re-ranking
    # Fuzzy fallback if few/no rows (e.g., "michaek jordan")
    try:
        need_fuzzy = (not rows) or (len(rows) < max(3, limit // 3))
        if need_fuzzy:
            # Prefer players for name-like queries
            pf = _fuzzy_players_suggest(spark, q2, limit=max(limit, 10))
            tf = _fuzzy_teams_suggest(spark, q2, limit=max(2, limit // 2))
            # Merge, keep best score per id/entity
            tmp = {}
            for it in (pf + tf):
                key = (it.get('_entity'), it.get('_id'))
                if key not in tmp or float(it.get('_score') or 0) > float(tmp[key].get('_score') or 0):
                    tmp[key] = it
            rows = list(tmp.values())
    except Exception:
        pass

    if rows:
        boost_p = float(os.environ.get("SUGGEST_BOOST_PLAYER", "0.8"))
        boost_t = float(os.environ.get("SUGGEST_BOOST_TEAM", "0.6"))
        pop_player = globals().get("_POP_PLAYER", {})
        pop_team = globals().get("_POP_TEAM", {})
        ql = q2.lower()
        def rank_of(item: dict) -> float:
            base = float(item.get('_score') or 0.0)
            ent = str(item.get('_entity',''))
            title = str(item.get('_title') or '').lower()
            # small prefix/exact boost for better UX
            name_boost = 0.0
            if title:
                if title == ql:
                    name_boost = 0.6
                elif title.startswith(ql):
                    name_boost = 0.25
            if ent == 'player':
                pop = pop_player.get(str(item.get('_id')), 0.0)
                return base + boost_p * pop + name_boost
            if ent == 'team':
                pop = pop_team.get(str(item.get('_id')), 0.0)
                return base + boost_t * pop + name_boost
            return base + name_boost
        rows = sorted(rows, key=rank_of, reverse=True)

    clipped = rows[:limit]
    _SUGGEST_CACHE[key] = (now, clipped)
    return {"items": clipped}

# --- Entity detail endpoints ---
@app.get("/entity/player/{pid}")
def entity_player(pid: str):
    if spark is None:
        return {"error": "service starting"}
    try:
        df = spark.table("GLOBAL_PLAYER")
        row = (df
               .where(F.col("player_id").cast("string") == F.lit(pid))
               .select(
                   F.col("player_id").cast("string").alias("id"),
                   F.col("full_name").alias("name"),
                   F.col("first_name"), F.col("last_name"),
                   F.col("position"), F.col("height"), F.col("weight"),
                   F.col("birth_date"), F.col("nationality"), F.col("college"),
                   F.col("experience"), F.col("team_id").cast("string").alias("team_id")
               )
               .limit(1).collect())
        if not row:
            return {"error": "not found", "id": pid}
        rec = row[0].asDict(recursive=True)
        # resolve team label
        try:
            t = spark.table("GLOBAL_TEAM").where(F.col("team_id").cast("string") == F.lit(rec.get("team_id")))
            trow = t.select(F.col("team_name").alias("team_name"), F.col("city").alias("team_city")).limit(1).collect()
            if trow:
                rec["team"] = {"id": rec.get("team_id"), "name": trow[0]["team_name"], "city": trow[0]["team_city"]}
        except Exception:
            pass
        return {"_entity": "player", **rec}
    except Exception as e:
        return {"error": str(e)}

@app.get("/entity/team/{tid}")
def entity_team(tid: str):
    if spark is None:
        return {"error": "service starting"}
    try:
        df = spark.table("GLOBAL_TEAM")
        row = (df.where(F.col("team_id").cast("string") == F.lit(tid))
                 .select(F.col("team_id").cast("string").alias("id"), F.col("team_name").alias("name"), F.col("city"), F.col("conference"), F.col("division"), F.col("state"), F.col("arena"))
                 .limit(1).collect())
        if not row:
            return {"error": "not found", "id": tid}
        rec = row[0].asDict(recursive=True)
        return {"_entity": "team", **rec}
    except Exception as e:
        return {"error": str(e)}

@app.get("/entity/game/{gid}")
def entity_game(gid: str, page: int = Query(0, ge=0), size: int = Query(5, ge=1, le=50)):
    if spark is None:
        return {"error": "service starting"}
    try:
        g = spark.table("GLOBAL_GAME").where(F.col("game_id").cast("string") == F.lit(gid)).alias("g")
        t = spark.table("GLOBAL_TEAM").select(F.col("team_id").alias("tid"), F.col("team_name").alias("tname"))
        df = (g
              .join(t.alias("h"), F.col("g.home_team_id") == F.col("h.tid"), "left")
              .join(t.alias("a"), F.col("g.away_team_id") == F.col("a.tid"), "left"))
        row = (df.select(
                F.col("g.game_id").cast("string").alias("id"),
                F.col("g.game_date").cast("date").alias("date"),
                F.col("g.home_team_id").cast("string").alias("home_id"),
                F.col("g.away_team_id").cast("string").alias("away_id"),
                F.col("h.tname").alias("home"), F.col("a.tname").alias("away"),
                F.col("g.final_score_home").alias("home_pts"), F.col("g.final_score_away").alias("away_pts"),
                F.col("g.location").alias("location"), F.col("g.attendance").alias("attendance"), F.col("g.period_count").alias("period_count")
             ).limit(1).collect())
        if not row:
            return {"error": "not found", "id": gid}
        rec = row[0].asDict(recursive=True)

        # Optional per-period line score
        try:
            if "GLOBAL_LINE_SCORE" in [tt.name for tt in spark.catalog.listTables()]:
                ls = spark.table("GLOBAL_LINE_SCORE").alias("ls")
                home_id = rec.get("home_id")
                away_id = rec.get("away_id")
                per = (ls.where(F.col("game_id").cast("string") == F.lit(gid))
                        .groupBy("period")
                        .agg(
                            F.sum(F.when(F.col("team_id").cast("string") == F.lit(home_id), F.col("points")).otherwise(F.lit(0))).alias("home_pts"),
                            F.sum(F.when(F.col("team_id").cast("string") == F.lit(away_id), F.col("points")).otherwise(F.lit(0))).alias("away_pts"),
                        )
                        .orderBy(F.col("period").asc()))
                rec["line"] = [r.asDict(recursive=True) for r in per.collect()]
        except Exception:
            pass

        # Head-to-head history (previous games only), paginated
        try:
            home_id = int(rec["home_id"]) if rec.get("home_id") is not None else None
            away_id = int(rec["away_id"]) if rec.get("away_id") is not None else None
            game_date = rec.get("date")
            if home_id and away_id and game_date:
                base = spark.table("GLOBAL_GAME").select(
                    F.col("game_id").cast("string").alias("gid"),
                    F.col("game_date").cast("date").alias("date"),
                    F.col("home_team_id").cast("int").alias("home_team_id"),
                    F.col("away_team_id").cast("int").alias("away_team_id"),
                    F.col("final_score_home").alias("home_pts"),
                    F.col("final_score_away").alias("away_pts"),
                )
                filt = (
                    ((F.col("home_team_id") == F.lit(home_id)) & (F.col("away_team_id") == F.lit(away_id))) |
                    ((F.col("home_team_id") == F.lit(away_id)) & (F.col("away_team_id") == F.lit(home_id)))
                ) & (F.col("date") < F.lit(game_date))
                h2h = base.where(filt)
                tm = spark.table("GLOBAL_TEAM").select(F.col("team_id").alias("tid"), F.col("team_name").alias("tname"))
                h2h = (h2h
                       .join(tm.alias("h"), F.col("home_team_id") == F.col("h.tid"), "left")
                       .join(tm.alias("a"), F.col("away_team_id") == F.col("a.tid"), "left"))
                from pyspark.sql.window import Window
                w = Window.orderBy(F.col("date").desc())
                h2h = h2h.withColumn("rn", F.row_number().over(w))
                start = page * size + 1
                end = (page + 1) * size
                page_df = (h2h.where((F.col("rn") >= F.lit(start)) & (F.col("rn") <= F.lit(end)))
                               .select(
                                   F.col("gid").alias("id"),
                                   F.col("date").cast("string").alias("date"),
                                   F.col("h.tname").alias("home"),
                                   F.col("a.tname").alias("away"),
                                   F.col("home_pts"), F.col("away_pts")
                               ))
                total = h2h.count()
                rec["history"] = [r.asDict(recursive=True) for r in page_df.collect()]
                rec["history_total"] = int(total)
                rec["history_page"] = int(page)
                rec["history_size"] = int(size)
        except Exception:
            pass

        return {"_entity": "game", **{k: (v if k != "date" else str(v)) for k, v in rec.items()}}
    except Exception as e:
        return {"error": str(e)}

# --------------------------
# Fuzzy fallback (typo-tolerant)
# --------------------------
def _fuzzy_teams_suggest(spark: SparkSession, q: str, limit: int) -> list[dict]:
    ql = q.lower().strip()
    if not ql:
        return []
    thr = max(1, int(round(len(ql) * 0.34)))
    df = spark.table("GLOBAL_TEAM").select(
        F.col("team_id").cast("string").alias("team_id"),
        F.col("team_name"), F.col("city")
    )
    base = df.withColumn("q", F.lit(ql))
    d1 = F.levenshtein(F.lower(F.col("team_name")), F.col("q"))
    d2 = F.levenshtein(F.lower(F.col("city")), F.col("q"))
    d = F.least(d1, d2)
    cand = (base
            .withColumn("d", d)
            .where(F.col("d") <= F.lit(thr))
            .orderBy(F.col("d").asc(), F.col("team_name").asc())
            .limit(limit))
    out = (cand.select(
        F.lit('team').alias('_entity'),
        (F.lit(100.0) - F.col('d').cast('double')).alias('_score'),
        F.col('team_id').alias('_id'),
        F.col('team_name').alias('_title'),
        F.col('city').alias('_subtitle'),
        F.lit(None).cast('string').alias('_extra')
    ))
    return [r.asDict(recursive=True) for r in out.collect()]


def _fuzzy_players_suggest(spark: SparkSession, q: str, limit: int) -> list[dict]:
    ql = q.lower().strip()
    if not ql:
        return []
    thr = max(1, int(round(len(ql) * 0.34)))
    df = spark.table("GLOBAL_PLAYER").select(
        F.col("player_id").cast("string").alias("player_id"),
        F.col("full_name"), F.col("position")
    )
    base = df.withColumn("q", F.lit(ql))
    d = F.levenshtein(F.lower(F.col("full_name")), F.col("q"))
    cand = (base
            .withColumn("d", d)
            .where(F.col("d") <= F.lit(thr))
            .orderBy(F.col("d").asc(), F.col("full_name").asc())
            .limit(limit))
    out = (cand.select(
        F.lit('player').alias('_entity'),
        (F.lit(100.0) - F.col('d').cast('double')).alias('_score'),
        F.col('player_id').alias('_id'),
        F.col('full_name').alias('_title'),
        F.col('position').alias('_subtitle'),
        F.lit(None).cast('string').alias('_extra')
    ))
    return [r.asDict(recursive=True) for r in out.collect()]

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


@app.get("/player/{pid}")
def player_page(pid: str):
    path = os.path.join(static_dir, "player.html")
    return FileResponse(path) if os.path.isfile(path) else {"message": "UI not found. Create web/player.html."}


@app.get("/team/{tid}")
def team_page(tid: str):
    path = os.path.join(static_dir, "team.html")
    return FileResponse(path) if os.path.isfile(path) else {"message": "UI not found. Create web/team.html."}


@app.get("/game/{gid}")
def game_page(gid: str):
    path = os.path.join(static_dir, "game.html")
    return FileResponse(path) if os.path.isfile(path) else {"message": "UI not found. Create web/game.html."}


@app.get("/matchup/{home}/{away}")
def matchup_page(home: str, away: str):
    path = os.path.join(static_dir, "matchup.html")
    return FileResponse(path) if os.path.isfile(path) else {"message": "UI not found. Create web/matchup.html."}


@app.get("/suggest-matchup")
def suggest_matchup(q: str = Query(...), limit: int = Query(10, ge=1, le=30)):
    if spark is None:
        return {"items": []}
    text = (q or "").strip()
    if not text:
        return {"items": []}
    import re as _re
    parts = _re.split(r"\s+(?:vs\.?|@|v)\s+", text, maxsplit=1, flags=_re.IGNORECASE)
    if len(parts) != 2:
        return {"items": []}
    left, right = parts[0].strip(), parts[1].strip()
    if not left or not right:
        return {"items": []}
    a = _mem_rank(_MEM_TEAMS, left, min(5, max(3, limit//2)))
    b = _mem_rank(_MEM_TEAMS, right, min(5, max(3, limit//2)))
    # fallback via Spark if needed
    def _spark_side(txt: str):
        try:
            t = (spark.table("GLOBAL_TEAM")
                    .select(F.col("team_id").cast("string").alias("id"),
                            F.col("team_name").alias("title"),
                            F.col("city").alias("subtitle")))
            t = t.where((F.lower(F.col("team_name")).contains(txt.lower())) | (F.lower(F.col("city")).contains(txt.lower())))
            return [{"_entity":"team","_id":r["id"],"_title":r["title"],"_subtitle":r["subtitle"],"_score":1.0} for r in t.limit(5).collect()]
        except Exception:
            return []
    if not a:
        a = _spark_side(left)
    if not b:
        b = _spark_side(right)
    pairs = []
    seen = set()
    for ta in a:
        for tb in b:
            key = (ta.get("_id"), tb.get("_id"))
            if not key or key in seen or ta.get("_id") == tb.get("_id"):
                continue
            seen.add(key)
            pairs.append({
                "_entity": "matchup",
                "_id": f"{ta.get('_id')}-{tb.get('_id')}",
                "_title": f"{ta.get('_title')} vs {tb.get('_title')}",
                "_subtitle": "Head-to-head",
                "_extra": None,
                "_score": float(ta.get("_score") or 0) + float(tb.get("_score") or 0)
            })
            if len(pairs) >= limit:
                break
        if len(pairs) >= limit:
            break
    return {"items": pairs}


@app.get("/entity/head2head")
def entity_head2head(
    home: str = Query(..., description="Home team id"),
    away: str = Query(..., description="Away team id"),
    page: int = Query(0, ge=0),
    size: int = Query(5, ge=1, le=50),
):
    if spark is None:
        return {"items": []}
    try:
        h = int(home); a = int(away)
    except Exception:
        return {"error": "invalid team ids"}
    try:
        g = spark.table("GLOBAL_GAME").select(
            F.col("game_id").cast("string").alias("id"),
            F.col("game_date").cast("date").alias("date"),
            F.col("home_team_id").cast("int").alias("home_id"),
            F.col("away_team_id").cast("int").alias("away_id"),
            F.col("final_score_home").alias("home_pts"),
            F.col("final_score_away").alias("away_pts"),
        )
        filt = (((F.col("home_id") == F.lit(h)) & (F.col("away_id") == F.lit(a))) |
                ((F.col("home_id") == F.lit(a)) & (F.col("away_id") == F.lit(h))))
        base = g.where(filt)
        tm = spark.table("GLOBAL_TEAM").select(F.col("team_id").alias("tid"), F.col("team_name").alias("tname"))
        base = (base
            .join(tm.alias("th"), F.col("home_id") == F.col("th.tid"), "left")
            .join(tm.alias("ta"), F.col("away_id") == F.col("ta.tid"), "left"))
        from pyspark.sql.window import Window
        w = Window.orderBy(F.col("date").desc())
        base = base.withColumn("rn", F.row_number().over(w))
        start = page*size + 1; end = (page+1)*size
        page_df = (base.where((F.col("rn")>=F.lit(start)) & (F.col("rn")<=F.lit(end)))
                        .select(
                            F.col("id"),
                            F.col("date").cast("string").alias("date"),
                            F.col("th.tname").alias("home"),
                            F.col("ta.tname").alias("away"),
                            F.col("home_pts"), F.col("away_pts")
                        ))
        total = base.count()
        return {"items": [r.asDict(recursive=True) for r in page_df.collect()], "total": int(total), "page": int(page), "size": int(size)}
    except Exception as e:
        return {"error": str(e)}


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





