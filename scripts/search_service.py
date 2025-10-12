# scripts/search_service.py
from __future__ import annotations

# --- In-memory suggest globals & helpers ---
import re
import difflib
import os
import math
import time
from typing import Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from src.search.engine import run_query
import src.search.engine as search_engine
from src.analytics.outcome import (
    FEATURE_DIFFS,
    build_pregame_features,
    build_match_features,
    load_pipeline_model,
    train_pregame_outcome_model,
)
from src.analytics.game_prediction import (
    build_game_feature_frame,
    train_outcome_model as train_simple_outcome,
)

import json  # (se usi risposte debug)

_MEM_PLAYERS: List[dict] = []  # list of dicts
_MEM_TEAMS: List[dict] = []    # list of dicts
_STANDINGS_CACHE = {}
_STANDINGS_TTL_SECONDS = int(os.environ.get("STANDINGS_TTL_SECONDS", "600"))
STANDINGS_CACHE_DIR = os.environ.get("STANDINGS_CACHE_DIR", os.path.join("metadata", "cache", "standings"))

def _norm(s: str) -> str:
    import re as _re
    s = (s or "").lower().strip()
    s = _re.sub(r"[^a-z0-9 ]+", " ", s)
    s = _re.sub(r"\s+", " ", s)
    return s

def _build_mem_indexes(spark: SparkSession):
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

def _mem_rank(items: List[dict], q: str, limit: int) -> List[dict]:
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

def _quick_suggest(q: str, limit: int, entity: Optional[str]) -> List[dict]:
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
FastAPI search service that keeps a single long-lived PySpark session.

Notes
- Spark is created on FastAPI startup and stopped on shutdown.
- GLOBAL_* tables are loaded from the local Parquet warehouse and cached.
"""

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
    # Try to load persisted TEAM_CONFERENCE if present
    try:
        tc_path = os.path.join(base, "team_conference")
        if os.path.isdir(tc_path):
            df = spark.read.parquet(tc_path)
            df.createOrReplaceTempView("TEAM_CONFERENCE")
            spark.catalog.cacheTable("TEAM_CONFERENCE")
            print(f"[INIT] -> TEAM_CONFERENCE from {tc_path}")
    except Exception as e:
        print(f"[WARN] Could not load persisted TEAM_CONFERENCE: {e}")


def _materialize_team_conference(spark: SparkSession, *, persist: bool | None = None, path: Optional[str] = None) -> None:
    """
    Crea/aggiorna una temp view TEAM_CONFERENCE con (team_id, team_name, city, conference)
    normalizzando conference e, se mancante, deducendo da division (anche con abbreviazioni).
    """
    F_ = F
    t = (
        spark.table("GLOBAL_TEAM")
        .select(
            F_.col("team_id").cast("int").alias("team_id"),
            F_.col("team_name").alias("team_name"),
            F_.col("city").alias("city"),
            F_.col("conference").alias("conference_raw"),
            F_.col("division").alias("division_raw"),
        )
    )

    # normalizza: togli parole "conference"/"division", tiene solo lettere, lowercase
    def _norm_letters(col):
        return F_.lower(
            F_.trim(
                F_.regexp_replace(
                    F_.regexp_replace(F_.coalesce(col, F_.lit("")), r"(?i)conference", ""),
                    r"(?i)division",
                    "",
                )
            )
        )

    conf_norm = _norm_letters(F_.col("conference_raw"))
    div_norm = _norm_letters(F_.col("division_raw"))
    # primo token della division (es. "atlantic division" -> "atlantic")
    first_div = F_.regexp_extract(div_norm, r"^([a-z]+)", 1)

    # Robust mapping from normalized conference/division tokens
    east_conf = ["e", "east", "eastern", "est"]
    west_conf = ["w", "west", "western", "ovest"]

    # Fallback mapping by team_name when needed
    east_names = [
        "celtics","nets","knicks","76ers","bucks","cavaliers","bulls","pistons","pacers",
        "hawks","hornets","heat","magic","raptors","wizards"
    ]
    west_names = [
        "nuggets","grizzlies","warriors","clippers","suns","lakers","timberwolves","thunder",
        "mavericks","pelicans","kings","trail blazers","jazz","rockets","spurs"
    ]

    tc = (
        t.withColumn("_conf_norm", conf_norm)
        .withColumn("_div_norm", F_.when(first_div == "", None).otherwise(first_div))
        .withColumn("_name_norm", F_.lower(F_.trim(F_.col("team_name"))))
        .withColumn(
            "_from_conf",
            F_.when(
                (
                    F_.col("_conf_norm").startswith(F_.lit("e"))
                    | F_.col("_conf_norm").isin(*east_conf)
                ),
                F_.lit("East"),
            )
            .when(
                (
                    F_.col("_conf_norm").startswith(F_.lit("w"))
                    | F_.col("_conf_norm").isin(*west_conf)
                ),
                F_.lit("West"),
            )
            .otherwise(F_.lit(None)),
        )
        .withColumn(
            "_from_div",
            F_.when(
                (
                    F_.col("_div_norm").startswith(F_.lit("atl"))
                    | F_.col("_div_norm").startswith(F_.lit("cen"))
                    | F_.col("_div_norm").startswith(F_.lit("southe"))
                    | F_.col("_div_norm").startswith(F_.lit("se"))
                ),
                F_.lit("East"),
            )
            .when(
                (
                    F_.col("_div_norm").startswith(F_.lit("northw"))
                    | F_.col("_div_norm").startswith(F_.lit("nw"))
                    | F_.col("_div_norm").startswith(F_.lit("pac"))
                    | F_.col("_div_norm").startswith(F_.lit("southw"))
                    | F_.col("_div_norm").startswith(F_.lit("sw"))
                ),
                F_.lit("West"),
            )
            .otherwise(F_.lit(None)),
        )
        .withColumn(
            "_from_name",
            F_.when(F_.col("_name_norm").isin(*east_names), F_.lit("East"))
             .when(F_.col("_name_norm").isin(*west_names), F_.lit("West"))
             .otherwise(F_.lit(None))
        )
        .withColumn("conference", F_.coalesce(F_.col("_from_conf"), F_.col("_from_div"), F_.col("_from_name")))
        .select("team_id", "team_name", "city", "conference")
    )

    tc.createOrReplaceTempView("TEAM_CONFERENCE")
    spark.catalog.cacheTable("TEAM_CONFERENCE")

    # optional persist to warehouse for faster subsequent startups
    do_persist = persist if persist is not None else (os.environ.get("PERSIST_TEAM_CONFERENCE", "0") in ["1","true","yes"])
    if do_persist:
        out_dir = path or os.path.join(WAREHOUSE_DIR, "team_conference")
        try:
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            pass
        try:
            (
                tc.coalesce(1)
                .write.mode("overwrite")
                .parquet(out_dir)
            )
            print(f"[OK] TEAM_CONFERENCE persisted to {os.path.abspath(out_dir)}")
        except Exception as e:
            print(f"[WARN] Persist TEAM_CONFERENCE failed: {e}")
    print("[OK] TEAM_CONFERENCE view ready.")

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
            SELECT game_date, away_team_id AS team_id, CAST(away_pts - home_pts AS DOUBLE) AS diff
            FROM GAME_RESULT WHERE game_date >= {cutoff_expr}
            """
        )
    else:
        spark.sql(
            """
            CREATE OR REPLACE TEMP VIEW TEAM_DAILY_DIFF AS
            SELECT game_date, home_team_id AS team_id, CAST(home_pts - away_pts AS DOUBLE) AS diff FROM GAME_RESULT
            UNION ALL
            SELECT game_date, away_team_id AS team_id, CAST(away_pts - home_pts AS DOUBLE) AS diff FROM GAME_RESULT
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
MODEL_DIR = os.environ.get("OUTCOME_MODEL_DIR", os.path.join("models", "outcome-full"))
FORM_WINDOW = int(os.environ.get("FORM_WINDOW", "10"))
# Default to 1 year as requested
FORM_YEARS = int(os.environ.get("FORM_YEARS", "1"))
ACTIVE_FORM_WINDOW = FORM_WINDOW
ACTIVE_FORM_YEARS = FORM_YEARS

def _train_outcome_quick(spark: SparkSession) -> Optional[PipelineModel]:
    try:
        # Ensure views ready
        _ensure_prediction_views(spark)
        train_df = spark.sql(
            """
            SELECT r.game_id,
                   CAST(CASE WHEN r.home_pts > r.away_pts THEN 1 ELSE 0 END AS INT) AS label,
                   AVG(COALESCE(h.cum_avg_diff, 0.0) - COALESCE(a.cum_avg_diff, 0.0)) AS expected_diff
            FROM GAME_RESULT r
            LEFT JOIN TEAM_CUMAVG h ON h.team_id = r.home_team_id AND h.game_date < r.game_date
            LEFT JOIN TEAM_CUMAVG a ON a.team_id = r.away_team_id AND a.game_date < r.game_date
            GROUP BY r.game_id, r.home_pts, r.away_pts
            """
        ).where(F.col("expected_diff").isNotNull())

        # Check both classes present
        lbl = [r[0] for r in train_df.groupBy("label").count().collect()]
        if len(lbl) < 2:
            print("[WARN] Training aborted: single class. Keeping previous model if any.")
            return None

        assembler = VectorAssembler(inputCols=["expected_diff"], outputCol="features")
        lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
        pipe = Pipeline(stages=[assembler, lr])
        model = pipe.fit(train_df)

        # Optionally persist
        try:
            os.makedirs(MODEL_DIR, exist_ok=True)
            model.write().overwrite().save(MODEL_DIR)
            print(f"[OK] Outcome model retrained and saved to {MODEL_DIR}")
        except Exception as e:
            print(f"[WARN] Could not persist outcome model: {e}")
        return model
    except Exception as e:
        print(f"[WARN] Quick training failed: {e}")
        return None

@app.on_event("startup")
def _on_startup():
    global spark, outcome_model, ACTIVE_FORM_WINDOW, ACTIVE_FORM_YEARS
    spark = _mk_spark()
    _register_views_from_warehouse(spark)
    # Build normalized TEAM_CONFERENCE view for consistent joins
    try:
        persist = os.environ.get("PERSIST_TEAM_CONFERENCE", "0") in ["1","true","yes"]
        out_dir = os.path.join(WAREHOUSE_DIR, "team_conference")
        _materialize_team_conference(spark, persist=persist, path=out_dir)
    except Exception as e:
        print(f"[WARN] TEAM_CONFERENCE build failed: {e}")
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
        retrain = os.environ.get("RETRAIN_OUTCOME", "0") in ["1","true","yes"]
        if model_path_ok and not retrain:
            outcome_model = PipelineModel.load(MODEL_DIR)
            print(f"[OK] Outcome model loaded from {MODEL_DIR}")
        else:
            print("[INFO] Training lightweight outcome model (cum-avg diff, 1y window)...")
            m = _train_outcome_quick(spark)
            if m is not None:
                outcome_model = m
            elif model_path_ok:
                outcome_model = PipelineModel.load(MODEL_DIR)
                print(f"[OK] Outcome model loaded from {MODEL_DIR}")
            else:
                print(f"[WARN] Outcome model not available.")
    except Exception as e:
        print(f"[WARN] Outcome model load/train failed: {e}")
    # (legacy training block removed: serving is load-only)

    # Prewarm standings cache asynchronously (latest season) for faster first paint
    try:
        do_prewarm = os.environ.get("PREWARM_STANDINGS", "1") in ["1", "true", "yes"]
        if do_prewarm:
            import threading as _th

            def _prewarm_worker():
                try:
                    tbls = {t.name.upper() for t in spark.catalog.listTables()}
                    source = "GAME_RESULT" if "GAME_RESULT" in tbls else "GLOBAL_GAME"
                    row = spark.table(source).select(F.max(F.col("game_date").cast("date")).alias("d")).limit(1).collect()[0]
                    last = row["d"] if row else None
                    if not last:
                        return
                    season = int(last.year - (0 if last.month >= 7 else 1))
                    # Try load persisted JSON first
                    try:
                        path = os.path.join(STANDINGS_CACHE_DIR, f"{season}.json")
                        if os.path.isfile(path):
                            with open(path, "r", encoding="utf-8") as fh:
                                data = json.load(fh)
                            import time as _time
                            _STANDINGS_CACHE[("standings", season)] = (_time.time(), data)
                            print(f"[INIT] Standings cache loaded for season {season} from {path}")
                            return
                    except Exception:
                        pass
                    # Compute and persist
                    res = standings(season)  # type: ignore[arg-type]
                    try:
                        os.makedirs(STANDINGS_CACHE_DIR, exist_ok=True)
                        path = os.path.join(STANDINGS_CACHE_DIR, f"{season}.json")
                        with open(path, "w", encoding="utf-8") as fh:
                            json.dump(res, fh)
                        print(f"[INIT] Standings season {season} persisted to {path}")
                    except Exception as e:
                        print(f"[WARN] Persist standings cache failed: {e}")
                except Exception as e:
                    print(f"[WARN] Standings prewarm failed: {e}")

            _th.Thread(target=_prewarm_worker, daemon=True).start()
    except Exception as e:
        print(f"[WARN] Could not start standings prewarm: {e}")

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

# --- Fuzzy fallback (typo-tolerant) ---
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
def _build_outcome_features_full(spark: SparkSession, home_id: int, away_id: int, cutoff: str):
    cutoff_date = F.to_date(F.lit(cutoff))
    tdd = spark.table("TEAM_DAILY_DIFF").select("team_id", "game_date", "diff")
    def roll10(team: int) -> float:
        rows = (tdd.where((F.col("team_id") == F.lit(int(team))) & (F.col("game_date") < cutoff_date))
                  .orderBy(F.col("game_date").desc())
                  .select("diff").limit(10).collect())
        vals = [float(r[0]) for r in rows if r[0] is not None]
        return float(sum(vals) / len(vals)) if vals else 0.0
    h10 = roll10(home_id)
    a10 = roll10(away_id)
    expected_diff = float(h10 - a10)
    def rest_days(team: int) -> int:
        rows = (tdd.where((F.col("team_id") == F.lit(int(team))) & (F.col("game_date") < cutoff_date))
                  .orderBy(F.col("game_date").desc())
                  .select("game_date").limit(2).collect())
        if len(rows) < 2:
            return 3
        try:
            return int((rows[0][0] - rows[1][0]).days)
        except Exception:
            return 3
    home_rest = rest_days(home_id); away_rest = rest_days(away_id)
    rest_diff = float((home_rest or 0) - (away_rest or 0))
    b2b_any = float(1.0 if (home_rest <= 1 or away_rest <= 1) else 0.0)
    g = spark.table("GLOBAL_GAME").select(
        F.col("game_id").cast("string").alias("gid"),
        F.col("location").alias("location"),
        F.col("home_team_id").cast("int").alias("home_team_id"),
    )
    r = spark.table("GAME_RESULT").select(
        F.col("game_id").cast("string").alias("rgid"),
        (F.col("home_pts") > F.col("away_pts")).cast("int").alias("home_win"),
    )
    arena_key = str(home_id)
    base = g.join(r, g["gid"] == r["rgid"], "inner").withColumn(
        "arena", F.coalesce(F.col("location"), F.col("home_team_id").cast("string"))
    )
    stats = (base.where(F.col("arena") == F.lit(arena_key))
                  .agg(F.count(F.lit(1)).alias("games"), F.sum("home_win").alias("hw")).collect())
    if stats and stats[0]["games"] and stats[0]["games"] > 0:
        arena_home_edge = float((float(stats[0]["hw"]) + 3.0) / (float(stats[0]["games"]) + 6.0))
    else:
        arena_home_edge = 0.5
    from datetime import datetime
    import math as _m
    dt = datetime.strptime(cutoff, "%Y-%m-%d")
    month = dt.month; dow = dt.isoweekday() % 7
    month_sin = _m.sin(2*_m.pi*month/12.0); month_cos = _m.cos(2*_m.pi*month/12.0)
    dow_sin = _m.sin(2*_m.pi*dow/7.0); dow_cos = _m.cos(2*_m.pi*dow/7.0)
    rows = [{
        "arena_home_edge": arena_home_edge,
        "expected_diff": expected_diff,
        "rest_diff": rest_diff,
        "b2b_any": b2b_any,
        "month_sin": month_sin,
        "month_cos": month_cos,
        "dow_sin": dow_sin,
        "dow_cos": dow_cos,
    }]
    return spark.createDataFrame(rows)

@app.get("/predict")
def predict(
    home: str = Query(..., description="Home team (id or name)"),
    away: str = Query(..., description="Away team (id or name)"),
):
    if spark is None:
        return {"error": "service starting"}
    if outcome_model is None:
        return {"error": "outcome model not available"}

    home_id = _resolve_team_id(home)
    away_id = _resolve_team_id(away)
    if not home_id or not away_id:
        return {"error": "unable to resolve team ids", "home": home, "away": away}

    # Determine cutoff date (always latest available); training horizon fixed to last 1 year via views
    tbls = {t.name for t in spark.catalog.listTables()}
    base = "GAME_RESULT" if "GAME_RESULT" in tbls else "GLOBAL_GAME"
    last = spark.table(base).select(F.max(F.col("game_date").cast("date")).alias("d")).collect()[0]["d"]
    cutoff = str(last)

    # Compute single-row expected_diff using TEAM_CUMAVG views (built during training)
    try:
        feats = _build_outcome_features_full(spark, int(home_id), int(away_id), cutoff)








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

@app.post("/admin/retrain-outcome")
def admin_retrain_outcome():
    if spark is None:
        return {"status": "starting"}
    try:
        global outcome_model
        m = _train_outcome_quick(spark)
        if m is not None:
            outcome_model = m
            return {"status": "ok", "saved_to": MODEL_DIR}
        return {"status": "error", "error": "training returned no model"}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/admin/materialize/team-conference")
def admin_materialize_team_conference(persist: bool = True):
    if spark is None:
        return {"status": "starting"}
    try:
        out_dir = os.path.join(WAREHOUSE_DIR, "team_conference")
        _materialize_team_conference(spark, persist=persist, path=out_dir)
        return {"status": "ok", "path": out_dir}
    except Exception as e:
        return {"status": "error", "error": str(e)}

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
        g = spark.table("GLOBAL_GAME").where(F.col("game_id").cast("string") == F.lit(gid))
        g = g.alias("g")
        teams = spark.table("TEAM_CONFERENCE").alias("t")
        th = teams.alias("th")
        ta = teams.alias("ta")
        df = (
            g
            .join(th, g["home_team_id"].cast("int") == th["team_id"], "left")
            .join(ta, g["away_team_id"].cast("int") == ta["team_id"], "left")
        )
        row = (
            df.select(
                g["game_id"].cast("string").alias("id"),
                g["game_date"].cast("date").alias("date"),
                g["home_team_id"].cast("string").alias("home_id"),
                g["away_team_id"].cast("string").alias("away_id"),
                th["team_name"].alias("home"),
                ta["team_name"].alias("away"),
                g["final_score_home"].alias("home_pts"),
                g["final_score_away"].alias("away_pts"),
                g["location"].alias("location"),
                g["attendance"].alias("attendance"),
                g["period_count"].alias("period_count"),
            )
            .limit(1)
            .collect()
        )
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
                    ((F.col("home_team_id") == F.lit(home_id)) & (F.col("away_team_id") == F.lit(away_id)))
                    | ((F.col("home_team_id") == F.lit(away_id)) & (F.col("away_team_id") == F.lit(home_id)))
                ) & (F.col("date") < F.lit(game_date))
                h2h = base.where(filt)
                tc = spark.table("TEAM_CONFERENCE").alias("tc")
                th = tc.alias("h")
                ta = tc.alias("a")
                h2h = (
                    h2h.join(th, h2h["home_team_id"] == th["team_id"], "left")
                    .join(ta, h2h["away_team_id"] == ta["team_id"], "left")
                )
                from pyspark.sql.window import Window
                w = Window.orderBy(F.col("date").desc())
                h2h = h2h.withColumn("rn", F.row_number().over(w))
                start = page * size + 1
                end = (page + 1) * size
                page_df = (
                    h2h.where((F.col("rn") >= F.lit(start)) & (F.col("rn") <= F.lit(end)))
                    .select(
                        F.col("gid").alias("id"),
                        F.col("date").cast("string").alias("date"),
                        th["team_name"].alias("home"),
                        ta["team_name"].alias("away"),
                        F.col("home_pts"),
                        F.col("away_pts"),
                    )
                )
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
# Helpers: derive conference reliably from available columns
# --------------------------
def _derive_team_conference(spark: SparkSession):
    """
    Ritorna un DataFrame (team_id:int, team_name, city, conference:string)
    Conferenza risolta in quest'ordine:
      1) GLOBAL_TEAM.conference se esiste e non null
      2) mapping da divisione (Atlantic/Central/Southeast -> East; Northwest/Pacific/Southwest -> West)
      3) TEAM_DETAILS / GLOBAL_TEAM_DETAILS (se presenti)
      4) TEAM_INFO_COMMON (se presente)
    """
    gt = spark.table("GLOBAL_TEAM")
    sel = [
        F.col("team_id").cast("int").alias("team_id"),
        F.col("team_name").alias("team_name"),
        F.col("city").alias("city"),
    ]
    if "conference" in gt.columns:
        sel.append(F.col("conference"))
    else:
        sel.append(F.lit(None).cast("string").alias("conference"))
    if "division" in gt.columns:
        sel.append(F.col("division"))
    teams = gt.select(*sel)

    # fallback da division
    if "division" in teams.columns:
        east_divs = F.array(F.lit("atlantic"), F.lit("central"), F.lit("southeast"))
        west_divs = F.array(F.lit("northwest"), F.lit("pacific"), F.lit("southwest"))
        div_n = F.lower(F.trim(F.col("division")))
        conf_from_div = F.when(F.array_contains(east_divs, div_n), F.lit("East")) \
                         .when(F.array_contains(west_divs, div_n), F.lit("West")) \
                         .otherwise(F.lit(None))
        teams = teams.withColumn(
            "conference",
            F.coalesce(F.col("conference"), conf_from_div)
        )

    # Prova TEAM_DETAILS / GLOBAL_TEAM_DETAILS
    tbls = {t.name.upper() for t in spark.catalog.listTables()}
    if "TEAM_DETAILS" in tbls or "GLOBAL_TEAM_DETAILS" in tbls:
        det_name = "GLOBAL_TEAM_DETAILS" if "GLOBAL_TEAM_DETAILS" in tbls else "TEAM_DETAILS"
        det = spark.table(det_name)
        cols = {c.lower(): c for c in det.columns}
        det_sel = det.select(
            F.col(cols.get("team_id", "team_id")).cast("int").alias("team_id"),
            *([F.col(cols["conference"]).alias("det_conference")] if "conference" in cols else []),
            *([F.col(cols["division"]).alias("det_division")] if "division" in cols else [])
        )
        teams = (teams.alias("t")
                 .join(det_sel.alias("d"), F.col("t.team_id") == F.col("d.team_id"), "left")
                 .withColumn("conference",
                             F.coalesce(
                                 F.col("t.conference"),
                                 F.col("d.det_conference"),
                                 F.when(
                                     F.lower(F.col("d.det_division")).isin("atlantic","central","southeast"), F.lit("East")
                                 ).when(
                                     F.lower(F.col("d.det_division")).isin("northwest","pacific","southwest"), F.lit("West")
                                 )
                             ))
                 .select("t.team_id","t.team_name","t.city","conference"))

    # Prova TEAM_INFO_COMMON
    if "TEAM_INFO_COMMON" in tbls:
        info = spark.table("TEAM_INFO_COMMON")
        cols = {c.lower(): c for c in info.columns}
        if "teamid" in cols:
            inf = info.select(
                F.col(cols["teamid"]).cast("int").alias("team_id"),
                *([F.col(cols["conference"]).alias("inf_conference")] if "conference" in cols else [])
            )
            teams = (teams.alias("t")
                     .join(inf.alias("i"), F.col("t.team_id") == F.col("i.team_id"), "left")
                     .withColumn("conference", F.coalesce(F.col("t.conference"), F.col("i.inf_conference")))
                     .select("t.team_id","t.team_name","t.city","conference"))

    # normalizza + fallback da nome squadra
    teams = teams.withColumn("_conf", F.lower(F.trim(F.coalesce(F.col("conference"), F.lit("")))))
    teams = teams.withColumn(
        "conference",
        F.when(F.col("_conf").startswith(F.lit("e")), F.lit("East"))
         .when(F.col("_conf").startswith(F.lit("w")), F.lit("West"))
         .otherwise(F.lit(None))
    ).drop("_conf")

    east_names = [
        "celtics","nets","knicks","76ers","bucks","cavaliers","bulls","pistons","pacers",
        "hawks","hornets","heat","magic","raptors","wizards"
    ]
    west_names = [
        "nuggets","grizzlies","warriors","clippers","suns","lakers","timberwolves","thunder",
        "mavericks","pelicans","kings","trail blazers","jazz","rockets","spurs"
    ]
    teams = (teams
             .withColumn("_name_norm", F.lower(F.trim(F.col("team_name"))))
             .withColumn(
                 "conference",
                 F.coalesce(
                     F.col("conference"),
                     F.when(F.col("_name_norm").isin(*east_names), F.lit("East"))
                      .when(F.col("_name_norm").isin(*west_names), F.lit("West"))
                 )
             )
             .drop("_name_norm"))

    return teams

# --------------------------
# Standings (by latest season year)
# --------------------------


@app.get("/standings")
def standings(year: Optional[int] = Query(None, ge=1900, le=2100)):
    """
    Standings REGULAR SEASON (first 82 games), robust East/West split:
    - normalize conference aggressively (eastern, western, etc.)
    - derive from division (abbrev ATL/CEN/SE/NW/PAC/SW supported)
    - no parity fallback: if unknown -> "unknown"
    - debug meta with distinct tokens
    """
    if spark is None:
        return {"season": None, "east": [], "west": [], "unknown": [], "meta": {"note": "service starting"}}

    F_ = F

    # 1) Sorgente e anno target (calcolo veloce)
    import datetime as _dt
    tbls = {t.name.upper() for t in spark.catalog.listTables()}
    source = "GAME_RESULT" if "GAME_RESULT" in tbls else "GLOBAL_GAME"
    maxd_row = (
        spark.table(source)
        .select(F_.max(F_.col("game_date").cast("date")).alias("d"))
        .collect()
    )
    last_date = maxd_row[0]["d"] if maxd_row and maxd_row[0] and maxd_row[0]["d"] else None
    chosen_year = int(year) if year is not None else (last_date.year - (0 if last_date and last_date.month >= 7 else 1) if last_date else None)

    # Cache: ritorna risultato memorizzato se recente
    try:
        import time as _time
        key = ("standings", chosen_year)
        ent = _STANDINGS_CACHE.get(key)
        if ent and (_time.time() - ent[0] <= _STANDINGS_TTL_SECONDS):
            return ent[1]
    except Exception:
        pass

    # 2) Base risultati solo per la stagione selezionata (range date, evita colonna season_start)
    if source == "GAME_RESULT":
        g = (
            spark.table("GAME_RESULT").select(
                F_.col("game_date").cast("date").alias("date"),
                F_.col("home_team_id").cast("int").alias("home_id"),
                F_.col("away_team_id").cast("int").alias("away_id"),
                F_.col("home_pts"),
                F_.col("away_pts"),
            )
        )
    else:
        gg = spark.table("GLOBAL_GAME")
        g = (
            gg.select(
                F_.col("game_date").cast("date").alias("date"),
                F_.col("home_team_id").cast("int").alias("home_id"),
                F_.col("away_team_id").cast("int").alias("away_id"),
                F_.col("final_score_home").alias("home_pts"),
                F_.col("final_score_away").alias("away_pts"),
            )
        )
    if chosen_year is None:
        return {"season": None, "east": [], "west": [], "unknown": [], "meta": {"note": "no season found"}}
    start = F_.to_date(F_.lit(f"{chosen_year}-07-01"))
    end = F_.to_date(F_.lit(f"{chosen_year+1}-06-30"))
    base = (
        g.where((F_.col("home_pts").isNotNull()) & (F_.col("away_pts").isNotNull()))
         .where((F_.col("date") >= start) & (F_.col("date") <= end))
    )
    if base.limit(1).count() == 0:
        return {"season": None, "east": [], "west": [], "unknown": [], "meta": {"note": "no games found"}}

    if base.limit(1).count() == 0:
        return {"season": None, "east": [], "west": [], "unknown": [], "meta": {"note": "no games found"}}

    # 4) Eventi per team e prime 82 gare
    from pyspark.sql.window import Window
    ev_home = base.select(
        F_.col("date"),
        F_.col("home_id").alias("team_id"),
        F_.when(F_.col("home_pts") > F_.col("away_pts"), F_.lit(1)).otherwise(F_.lit(0)).alias("w"),
        F_.when(F_.col("home_pts") > F_.col("away_pts"), F_.lit(0)).otherwise(F_.lit(1)).alias("l"),
    )
    ev_away = base.select(
        F_.col("date"),
        F_.col("away_id").alias("team_id"),
        F_.when(F_.col("away_pts") > F_.col("home_pts"), F_.lit(1)).otherwise(F_.lit(0)).alias("w"),
        F_.when(F_.col("away_pts") > F_.col("home_pts"), F_.lit(0)).otherwise(F_.lit(1)).alias("l"),
    )
    events = ev_home.unionByName(ev_away)
    w_team_chrono = Window.partitionBy("team_id").orderBy(F_.col("date").asc())
    events = events.withColumn("rn", F_.row_number().over(w_team_chrono))
    reg = events.where(F_.col("rn") <= F_.lit(82)).cache()

    # 5) Tally W/L per team
    tally = reg.groupBy("team_id").agg(F_.sum("w").alias("w"), F_.sum("l").alias("l"))

    # 6) Teams + conference using TEAM_CONFERENCE if available; else derive inline
    teams = None
    try:
        tbls2 = {t.name.upper() for t in spark.catalog.listTables()}
        if "TEAM_CONFERENCE" not in tbls2:
            try:
                _materialize_team_conference(spark)
                tbls2 = {t.name.upper() for t in spark.catalog.listTables()}
            except Exception:
                pass
        else:
            # Rebuild if view exists but has no non-null conference
            try:
                nn = (
                    spark.table("TEAM_CONFERENCE")
                    .where(F_.col("conference").isNotNull())
                    .limit(1)
                    .count()
                )
                if int(nn) == 0:
                    _materialize_team_conference(spark)
            except Exception:
                pass
        if "TEAM_CONFERENCE" in tbls2:
            teams = (
                spark.table("TEAM_CONFERENCE")
                .select(
                    F_.col("team_id").cast("int").alias("team_id"),
                    F_.col("team_name").alias("team_name"),
                    F_.col("city").alias("city"),
                    F_.col("conference").alias("conference"),
                )
                .alias("t")
            )
        else:
            # robust fallback
            td = _derive_team_conference(spark)
            teams = (
                td.select(
                    F_.col("team_id").cast("int").alias("team_id"),
                    F_.col("team_name").alias("team_name"),
                    F_.col("city").alias("city"),
                    F_.col("conference").alias("conference"),
                )
                .alias("t")
            )
    except Exception as _:
        # last resort: minimal fields from GLOBAL_TEAM, conference unknown
        gt = spark.table("GLOBAL_TEAM")
        teams = (
            gt.select(
                F_.col("team_id").cast("int").alias("team_id"),
                F_.col("team_name").alias("team_name"),
                F_.col("city").alias("city"),
                F_.lit(None).cast("string").alias("conference"),
            )
            .alias("t")
        )

    joined = (
        tally.alias("ta")
        .join(F_.broadcast(teams), F_.col("ta.team_id") == F_.col("t.team_id"), "left")
        .select(
            F_.col("t.team_id").cast("int").alias("team_id"),
            F_.col("t.team_name").alias("name"),
            F_.col("t.city").alias("city"),
            F_.col("t.conference").alias("conference"),
            F_.col("ta.w").cast("int").alias("w"),
            F_.col("ta.l").cast("int").alias("l"),
        )
    )

    # 7) Win% e Games Behind per conference
    wconf = Window.partitionBy(F_.col("conference"))
    joined = joined.withColumn(
        "pct",
        F_.when((F_.col("w") + F_.col("l")) > 0, F_.col("w") / (F_.col("w") + F_.col("l"))).otherwise(F_.lit(0.0))
    )
    leader_w = F_.max(F_.col("w")).over(wconf)
    leader_l = F_.min(F_.col("l")).over(wconf)
    joined = joined.withColumn("gb", ((leader_w - F_.col("w")) + (F_.col("l") - leader_l)) / F_.lit(2.0))

    # 8) Streak
    r = reg.select(
        F_.col("date"),
        F_.col("team_id"),
        F_.when(F_.col("w") > F_.col("l"), F_.lit(1)).otherwise(F_.lit(-1)).alias("res"),
    )
    w_team = Window.partitionBy("team_id").orderBy(F_.col("date").asc())
    r = r.withColumn("chg", F_.when(F_.col("res") != F_.lag(F_.col("res")).over(w_team), F_.lit(1)).otherwise(F_.lit(0)))
    r = r.fillna({"chg": 0})
    r = r.withColumn(
        "grp",
        F_.sum(F_.col("chg")).over(Window.partitionBy("team_id").orderBy(F_.col("date").asc())
                                   .rowsBetween(Window.unboundedPreceding, 0))
    )
    g = (r.groupBy("team_id", "grp")
           .agg(F_.count(F_.lit(1)).alias("len"), F_.max("date").alias("last_date"), F_.max("res").alias("sign")))
    g_last = (g.join(g.groupBy("team_id").agg(F_.max("grp").alias("m")), ["team_id"])
                .where(F_.col("grp") == F_.col("m"))
                .select(F_.col("team_id"), F_.col("len").alias("streak_len"), F_.col("sign").alias("streak_sign")))
    full = (joined.join(g_last, on="team_id", how="left")
                  .withColumn(
                      "streak",
                      F_.when(F_.col("streak_len").isNull(), F_.lit(None))
                       .otherwise(
                           F_.when(F_.col("streak_sign") > 0, F_.concat(F_.lit("W"), F_.col("streak_len")))
                            .otherwise(F_.concat(F_.lit("L"), F_.col("streak_len")))
                       )
                  ))

    # 9) Split East/West/Unknown (NO fallback parità)
    j2 = full.withColumn("_conf", F_.lower(F_.trim(F_.col("conference"))))
    order_cols = [F_.col("w").desc(), F_.col("l").asc(), F_.col("name").asc()]

    east_df = (j2.where(F_.col("_conf") == F_.lit("east"))
                 .dropDuplicates(["team_id"])
                 .orderBy(*order_cols))
    west_df = (j2.where(F_.col("_conf") == F_.lit("west"))
                 .dropDuplicates(["team_id"])
                 .orderBy(*order_cols))
    unknown_df = (j2.where(F_.col("_conf").isNull() | ((F_.col("_conf") != F_.lit("east")) & (F_.col("_conf") != F_.lit("west"))))
                    .dropDuplicates(["team_id"])
                    .orderBy(*order_cols))

    def to_list(df):
        return [
            {
                "id": str(r["team_id"]) if r["team_id"] is not None else None,
                "name": r["name"],
                "city": r["city"],
                "conference": r["conference"],
                "w": int(r["w"]) if r["w"] is not None else 0,
                "l": int(r["l"]) if r["l"] is not None else 0,
                "pct": float(r["pct"]) if r["pct"] is not None else 0.0,
                "gb": float(r["gb"]) if r["gb"] is not None else 0.0,
                "streak": r["streak"],
            }
            for r in df.collect()
        ]

    east = to_list(east_df)
    west = to_list(west_df)
    unknown = to_list(unknown_df)

    meta = {
        "season": int(chosen_year) if chosen_year is not None else None,
        "counts": {
            "games": int(base.count()),
            "teams_wl": int(tally.count()),
            "east": len(east),
            "west": len(west),
            "unknown": len(unknown),
        },
    }

    result = {"season": meta["season"], "east": east, "west": west, "unknown": unknown, "meta": meta}
    try:
        import time as _time
        _STANDINGS_CACHE[("standings", int(chosen_year))] = (_time.time(), result)
    except Exception:
        pass
    return result






