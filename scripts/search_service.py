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
                F.col("team_name").alias("name"),
                F.col("city").alias("city"),
            )
        )
        _MEM_TEAMS = [
            {
                "_entity": "team",
                "_id": r["id"],
                "_title": f"{(r['city'] or '').strip()} {(r['name'] or '').strip()}".strip(),
                "_subtitle": None,
                "_extra": None,
                "_norm": _norm(f"{str(r['city'] or '')} {str(r['name'] or '')}"),
            }
            for r in tdf.collect()
            if (r["id"] is not None and r["name"])
        ]
    except Exception as e:
        print(f"[WARN] Could not build _MEM_TEAMS: {e}")
        _MEM_TEAMS = []

def _quick_player_summary(spark: SparkSession, pid: int) -> dict:
    """Compute quick scoring summary and basic bio/draft for a single player.

    Uses play-by-play deltas to infer points scored (fast enough for single id).
    """
    try:
        pid_i = int(pid)
        tables = {t.name.upper() for t in spark.catalog.listTables()}
        has_pbp = "GLOBAL_PLAY_BY_PLAY" in tables
        pbp = spark.table("GLOBAL_PLAY_BY_PLAY") if has_pbp else None
        # Filter by player for efficiency
        pbp1 = (pbp.where(F.col("player1_id").cast("int") == F.lit(pid_i))
                if pbp is not None else None)

        # Base bio record
        base_df = (spark.table("GLOBAL_PLAYER")
                   .where(F.col("player_id").cast("int") == F.lit(pid_i))
                   .select(F.col("player_id").cast("int").alias("player_id"),
                           F.col("player_id").cast("string").alias("id"),
                           F.col("full_name").alias("name"),
                           F.col("position"),
                           F.col("team_id").cast("int").alias("team_id"),
                           F.col("experience"),
                           F.col("college"),
                           F.col("nationality"))
                   .limit(1))

        # If no PBP data, return base info with draft when available
        if (pbp1 is None) or (pbp1.limit(1).count() == 0):
            row = base_df.limit(1).collect()
            out = {"_entity": "player", "id": str(pid_i)}
            if row:
                r = row[0].asDict(recursive=True)
                out.update({k: r.get(k) for k in ["name","position","team_id","experience","college","nationality"]})
            try:
                dh = (
                    spark.table("GLOBAL_DRAFT_HISTORY")
                         .where(F.col("player_id").cast("int") == F.lit(int(pid)))
                         .orderBy(F.col("draft_year").asc(), F.col("overall_pick").asc())
                         .limit(1).collect()
                )
                if dh:
                    dr = dh[0].asDict(recursive=True)
                    out.update({
                        "draft_year": dr.get("draft_year"),
                        "overall_pick": dr.get("overall_pick"),
                    })
            except Exception:
                pass
            return out

        # Robust counting using event types and text markers (no score delta needed)
        cols = [
            F.col("game_id").cast("int").alias("game_id"),
            F.col("eventmsgtype").cast("int").alias("t"),
            F.col("player1_id").cast("int").alias("p1"),
            F.col("player2_id").cast("int").alias("p2"),
            F.coalesce(F.col("homedescription"), F.col("visitordescription"), F.col("neutraldescription")).alias("desc"),
            F.col("player1_name").alias("p1n"),
            F.col("player2_name").alias("p2n"),
        ]
        ev = pbp.select(*cols)
        # Try id-based first
        ev_any = ev.where((F.col("p1") == pid_i) | (F.col("p2") == pid_i))
        if ev_any.limit(1).count() == 0:
            # Fallback by name
            try:
                nm = (spark.table("GLOBAL_PLAYER")
                          .where(F.col("player_id").cast("int") == F.lit(pid_i))
                          .select(F.col("full_name").alias("n")).limit(1).collect())
                if nm:
                    full_name = nm[0]["n"]
                    ev_any = ev.where((F.lower(F.col("p1n")) == F.lit(str(full_name).lower())) | (F.lower(F.col("p2n")) == F.lit(str(full_name).lower())))
            except Exception:
                pass
        ev_any = ev.where((F.col("p1") == pid_i) | (F.col("p2") == pid_i))
        # if id-based match is empty, try name fallback (data inconsistencies)
        if ev_any.limit(1).count() == 0:
            try:
                nm_row = base_df.limit(1).collect()
                full_name = (nm_row[0]["name"] if nm_row else None) or None
                if full_name:
                    nm = F.lit(str(full_name).lower())
                    ev_any = ev.where((F.lower(F.col("p1n")) == nm) | (F.lower(F.col("p2n")) == nm))
                    if ev_any.limit(1).count() == 0:
                        # also try last name only (some datasets shorten names)
                        ln = str(full_name).split(" ")[-1].lower()
                        ev_any = ev.where((F.lower(F.col("p1n")).endswith(F.lit(" "+ln))) | (F.lower(F.col("p2n")).endswith(F.lit(" "+ln))))
            except Exception:
                pass
        shooter = ev.where(F.col("p1") == pid_i)
        if shooter.limit(1).count() == 0:
            # name-based shooter fallback as well
            try:
                nm_row = base_df.limit(1).collect()
                full_name = (nm_row[0]["name"] if nm_row else None) or None
                if full_name:
                    shooter = ev.where(F.lower(F.col("p1n")) == F.lit(str(full_name).lower()))
            except Exception:
                pass
        is_make = (F.col("t") == 1)
        is_miss = (F.col("t") == 2)
        is_ft = (F.col("t") == 3)
        is_three = F.lower(F.col("desc")).contains(F.lit("3pt")) | F.lower(F.col("desc")).contains(F.lit("3-pt")) | F.lower(F.col("desc")).contains(F.lit("three"))
        is_miss_text = F.lower(F.col("desc")).contains(F.lit("miss"))

        # Aggregations
        if ev_any.limit(1).count() == 0:
            games_played = 0
            fgm_val = fg3m_val = ftm_val = 0
        else:
            games_played = int(ev_any.select("game_id").distinct().count())
            agg = (shooter.agg(
                F.sum(F.when(is_make, 1).otherwise(0)).alias("fgm"),
                F.sum(F.when(is_three & is_make, 1).otherwise(0)).alias("fg3m"),
                F.sum(F.when(is_ft & (~is_miss_text), 1).otherwise(0)).alias("ftm"),
            ).collect())
            if agg:
                fgm_val = int(agg[0]["fgm"] or 0)
                fg3m_val = int(agg[0]["fg3m"] or 0)
                ftm_val = int(agg[0]["ftm"] or 0)
            else:
                fgm_val = fg3m_val = ftm_val = 0

        made_threes = fg3m_val
        made_twos = max(0, fgm_val - fg3m_val)
        made_fts = ftm_val
        total_points = 3 * made_threes + 2 * made_twos + made_fts

        joined = base_df
        row = joined.limit(1).collect()
        out = {"_entity": "player", "id": str(pid_i)}
        if row:
            r = row[0].asDict(recursive=True)
            out.update({
                "name": r.get("name"),
                "position": r.get("position"),
                "team_id": r.get("team_id"),
                "experience": r.get("experience"),
                "college": r.get("college"),
                "nationality": r.get("nationality"),
                "games_played": int(games_played or 0),
                "total_points": int(total_points or 0),
                "avg_points": float(round((total_points / games_played), 2) if games_played else 0.0),
                "made_threes": int(made_threes or 0),
                "made_twos": int(made_twos or 0),
                "made_fts": int(made_fts or 0),
            })

        # Draft info (first record by year)
        try:
            dh = (spark.table("GLOBAL_DRAFT_HISTORY")
                    .where(F.col("player_id").cast("int") == F.lit(int(pid)))
                    .orderBy(F.col("draft_year").asc(), F.col("overall_pick").asc())
                    .limit(1).collect())
            if dh:
                dr = dh[0].asDict(recursive=True)
                out.update({
                    "draft_year": dr.get("draft_year"),
                    "overall_pick": dr.get("overall_pick"),
                })
        except Exception:
            pass

        return out
    except Exception as e:
        return {"error": str(e)}

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
    # Core views needed for search and predictions
    "GLOBAL_PLAYER": "global_player",
    "GLOBAL_TEAM": "global_team",
    "GLOBAL_GAME": "global_game",
    "GLOBAL_LINE_SCORE": "global_line_score",
    "GLOBAL_OTHER_STATS": "global_other_stats",
    # Add missing but available datasets used by player endpoints
    "GLOBAL_PLAY_BY_PLAY": "global_play_by_play",
    "GLOBAL_DRAFT_HISTORY": "global_draft_history",
    "GLOBAL_DRAFT_COMBINE": "global_draft_combine",
    "GLOBAL_OFFICIAL": "global_official",
    "GLOBAL_GAME_OFFICIAL": "global_game_official",
    # New: pre-aggregated per-player per-season totals from PBP
    "GLOBAL_PLAYER_SEASON_FROM_PBP": "global_player_season_from_pbp",
    # New: player stats outputs from tools/build_player_stats.py
    "GLOBAL_PLAYER_STATS": "global_player_stats",
    "GLOBAL_PLAYER_CAREER": "global_player_career",
    "GLOBAL_PLAYER_LAST_SEASON": "global_player_last_season",
    "GLOBAL_PLAYER_SEASON_SCORING": "global_player_season_scoring",
}

def _mk_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("LSDM-SearchService")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
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
        tbls_up = {t.name.upper() for t in spark.catalog.listTables()}
        if "GLOBAL_LINE_SCORE" in tbls_up:
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


# --- Build per-player per-season totals from PBP into warehouse (no UDF) ---
def _build_player_season_from_pbp(spark: SparkSession, warehouse: str) -> str:
    pbp_tbls = {t.name.upper() for t in spark.catalog.listTables()}
    if ("GLOBAL_PLAY_BY_PLAY" not in pbp_tbls) or ("GLOBAL_GAME" not in pbp_tbls):
        raise RuntimeError("GLOBAL_PLAY_BY_PLAY or GLOBAL_GAME not available")

    g = spark.table("GLOBAL_GAME").select(
        F.col("game_id").cast("int").alias("game_id"),
        F.col("game_date").cast("date").alias("game_date"),
    )
    pbp = spark.table("GLOBAL_PLAY_BY_PLAY")
    ev = pbp.select(
        F.col("game_id").cast("int").alias("game_id"),
        F.col("eventnum").cast("int").alias("eventnum"),
        F.col("eventmsgtype").cast("int").alias("t"),
        F.col("player1_id").cast("int").alias("p1"),
        F.col("player2_id").cast("int").alias("p2"),
        F.col("score").alias("score"),
        F.coalesce(F.col("homedescription"), F.col("visitordescription"), F.col("neutraldescription")).alias("desc"),
    )
    # Games per season: union(p1,p2) distinct per (game_id, player_id)
    gp_pairs = (
        ev.where(F.col("p1").isNotNull()).select(F.col("game_id"), F.col("p1").alias("player_id"))
        .unionByName(ev.where(F.col("p2").isNotNull()).select(F.col("game_id"), F.col("p2").alias("player_id")))
        .dropna(subset=["player_id"]).dropDuplicates(["game_id", "player_id"]))
    gp = (gp_pairs.join(g, "game_id", "left")
                    .withColumn("season", F.year(F.col("game_date")))
                    .groupBy("season", "player_id")
                    .agg(F.countDistinct(F.col("game_id")).alias("g")))

    # Shots/FT for shooter p1
    is_make = (F.col("t") == 1)
    is_miss = (F.col("t") == 2)
    is_ft = (F.col("t") == 3)
    is_three = (F.lower(F.col("desc")).contains(F.lit("3pt")) |
                F.lower(F.col("desc")).contains(F.lit("3-pt")) |
                F.lower(F.col("desc")).contains(F.lit("three")))
    is_miss_text = F.lower(F.col("desc")).contains(F.lit("miss"))

    shooter = ev.where(F.col("p1").isNotNull()).join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
    shots = (shooter.groupBy("season", F.col("p1").alias("player_id"))
                   .agg(F.sum(F.when(is_make, 1).otherwise(0)).alias("fgm"),
                        F.sum(F.when(is_make | is_miss, 1).otherwise(0)).alias("fga"),
                        F.sum(F.when(is_three & is_make, 1).otherwise(0)).alias("fg3m"),
                        F.sum(F.when(is_three & (is_make | is_miss), 1).otherwise(0)).alias("fg3a"),
                        F.sum(F.when(is_ft & (~is_miss_text), 1).otherwise(0)).alias("ftm"),
                        F.sum(F.when(is_ft, 1).otherwise(0)).alias("fta")))

    # Assists p2 and rebounds t==4 by p1
    ast = (ev.where(F.col("p2").isNotNull() & is_make)
              .join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
              .groupBy("season", F.col("p2").alias("player_id"))
              .agg(F.count(F.lit(1)).alias("ast")))
    reb = (ev.where((F.col("t") == 4) & F.col("p1").isNotNull())
              .join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
              .groupBy("season", F.col("p1").alias("player_id"))
              .agg(F.count(F.lit(1)).alias("reb")))

    # Points via score deltas on shooter events
    from pyspark.sql import Window as _W
    sc = shooter.where(F.col("score").isNotNull())
    sc = (sc.withColumn("score_clean", F.regexp_replace("score", "\\s+", ""))
             .withColumn("arr", F.split(F.col("score_clean"), "-"))
             .withColumn("sh", F.when(F.size(F.col("arr")) == 2, F.element_at(F.col("arr"), 1).cast("int")))
             .withColumn("sa", F.when(F.size(F.col("arr")) == 2, F.element_at(F.col("arr"), 2).cast("int"))))
    w = _W.partitionBy("game_id").orderBy(F.col("eventnum"))
    sc = (sc.withColumn("prevh", F.coalesce(F.lag(F.col("sh")).over(w), F.lit(0)))
             .withColumn("preva", F.coalesce(F.lag(F.col("sa")).over(w), F.lit(0)))
             .withColumn("delta_h", F.when(F.col("sh").isNotNull(), F.col("sh") - F.col("prevh")).otherwise(0))
             .withColumn("delta_a", F.when(F.col("sa").isNotNull(), F.col("sa") - F.col("preva")).otherwise(0))
             .withColumn("points", F.greatest(F.col("delta_h"), F.col("delta_a"))))
    pts_season = (sc.where(F.col("points") > 0)
                    .groupBy("season", F.col("p1").alias("player_id"))
                    .agg(F.sum("points").alias("pts_sc")))

    base = (gp.join(shots, ["season", "player_id"], "left")
               .join(ast, ["season", "player_id"], "left")
               .join(reb, ["season", "player_id"], "left")
               .join(pts_season, ["season", "player_id"], "left"))
    out = (base.fillna({"fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0, "ast": 0, "reb": 0, "pts_sc": 0})
               .withColumn("pts_formula", (F.col("fgm") - F.col("fg3m")) * F.lit(2) + F.col("fg3m") * F.lit(3) + F.col("ftm"))
               .withColumn("pts", F.when(F.col("pts_sc").isNotNull(), F.col("pts_sc")).otherwise(F.col("pts_formula")))
               .select("season", "player_id", F.col("g").cast("int").alias("g"),
                       F.col("pts").cast("int").alias("pts"), F.col("ast").cast("int").alias("ast"), F.col("reb").cast("int").alias("reb"),
                       F.col("fgm").cast("int").alias("fgm"), F.col("fga").cast("int").alias("fga"),
                       F.col("fg3m").cast("int").alias("fg3m"), F.col("fg3a").cast("int").alias("fg3a"),
                       F.col("ftm").cast("int").alias("ftm"), F.col("fta").cast("int").alias("fta")))

    dest = os.path.join(warehouse, "global_player_season_from_pbp")
    out.write.mode("overwrite").parquet(dest)
    # Register view for immediate use
    spark.read.parquet(dest).createOrReplaceTempView("GLOBAL_PLAYER_SEASON_FROM_PBP")
    spark.catalog.cacheTable("GLOBAL_PLAYER_SEASON_FROM_PBP")
    print(f"[OK] GLOBAL_PLAYER_SEASON_FROM_PBP written to {os.path.abspath(dest)}")
    return dest


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
                       F.col('_id'),
                       F.col('title').alias('_title'),
                       F.lit(None).cast('string').alias('_subtitle'),
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
                        F.lit('team').alias('_entity'),
                        F.col('_score'),
                        F.col('_id'),
                        F.col('title').alias('_title'),
                        F.lit(None).cast('string').alias('_subtitle'),
                        F.lit(None).cast('string').alias('_extra')
                    )
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
    """Resolve a team identifier from id or free text.

    Accepts numeric ids, or a variety of name inputs like
    "Bulls", "Chicago", or "Chicago Bulls" (case-insensitive).
    Uses fast in-memory index first, then Spark fallback.
    """
    if spark is None:
        return None
    tok = (token or "").strip()
    if not tok:
        return None

    # 1) Numeric id fast path (accept either numeric string or int-like)
    if tok.isdigit():
        try:
            val = int(tok)
        except Exception:
            val = None
        if val is not None:
            t = spark.table("GLOBAL_TEAM")
            row = (t.where(F.col("team_id").cast("int") == F.lit(val))
                     .select("team_id").limit(1).collect())
            return int(row[0][0]) if row else None

    # 2) In-memory resolution against _MEM_TEAMS (City + Name normalized)
    try:
        if _MEM_TEAMS:
            import difflib as _dl
            qn = _norm(tok)
            best_id: Optional[str] = None
            best = 0.0
            for it in _MEM_TEAMS:
                cid = str(it.get("_id") or "").strip()
                nm = it.get("_norm") or _norm(str(it.get("_title") or ""))
                if not cid or not nm:
                    continue
                if nm == qn:
                    return int(cid)
                score = 0.0
                if qn and nm.startswith(qn):
                    score = 0.98
                elif qn and qn in nm:
                    score = 0.9
                else:
                    score = _dl.SequenceMatcher(None, nm, qn).ratio()
                if score > best:
                    best = score
                    best_id = cid
            if best_id is not None and best >= 0.72:
                return int(best_id)
    except Exception:
        pass

    # 3) Spark fallback: check team_name/city and also full name (city + name)
    token_l = tok.lower()
    t = spark.table("GLOBAL_TEAM").select(
        F.col("team_id").cast("int").alias("team_id"),
        F.col("team_name").alias("team_name"),
        F.col("city").alias("city"),
        F.concat_ws(" ", F.col("city"), F.col("team_name")).alias("full")
    )

    # Exact match on full first (normalized)
    cand = (
        t.where(F.lower(F.col("full")) == F.lit(token_l))
         .select("team_id").limit(1).collect()
    )
    if cand:
        return int(cand[0][0])

    # Contains on any component
    cand = (
        t.where(
            F.lower(F.col("team_name")).contains(F.lit(token_l)) |
            F.lower(F.col("city")).contains(F.lit(token_l)) |
            F.lower(F.col("full")).contains(F.lit(token_l))
        )
        .orderBy(F.col("team_name").asc())
        .select("team_id").limit(1).collect()
    )
    if cand:
        return int(cand[0][0])

    # 4) Engine fallback (strongest): use the same candidate logic used by /suggest
    try:
        cdf = search_engine.candidate_teams(spark, tok, limit=1)  # type: ignore[arg-type]
        rows = cdf.select(F.col("_id")).limit(1).collect()
        if rows:
            return int(rows[0][0])
    except Exception:
        pass

    return None
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
    home_label = (f"{(hname[0][1] or '').strip()} {(hname[0][0] or '').strip()}".strip()) if hname else str(home_id)
    away_label = (f"{(aname[0][1] or '').strip()} {(aname[0][0] or '').strip()}".strip()) if aname else str(away_id)

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

@app.post("/admin/build-player-agg")
def admin_build_player_agg(force: bool = False):
    """Build GLOBAL_PLAYER_SEASON_FROM_PBP parquet if missing (or force)."""
    if spark is None:
        return {"error": "service starting"}
    dest = os.path.join(WAREHOUSE_DIR, "global_player_season_from_pbp")
    exists = os.path.isdir(dest)
    if exists and not force:
        try:
            spark.read.parquet(dest).createOrReplaceTempView("GLOBAL_PLAYER_SEASON_FROM_PBP")
            spark.catalog.cacheTable("GLOBAL_PLAYER_SEASON_FROM_PBP")
        except Exception:
            pass
        return {"ok": True, "path": dest, "built": False}
    try:
        os.makedirs(dest, exist_ok=True)
    except Exception:
        pass
    try:
        out = _build_player_season_from_pbp(spark, WAREHOUSE_DIR)
        return {"ok": True, "path": out, "built": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

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
                            F.col("team_name").alias("name"),
                            F.col("city").alias("city")))
            t = t.where((F.lower(F.col("team_name")).contains(txt.lower())) | (F.lower(F.col("city")).contains(txt.lower())))
            return [{"_entity":"team","_id":r["id"],"_title":f"{(r['city'] or '').strip()} {(r['name'] or '').strip()}".strip(),"_subtitle":None,"_score":1.0} for r in t.limit(5).collect()]
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

@app.get("/player/summary/{pid}")
def player_summary(pid: str):
    """Return a compact player summary for search results cards."""
    if spark is None:
        return {"error": "service starting"}
    try:
        pid_int = int(str(pid))
    except Exception:
        return {"error": "invalid player id"}
    try:
        # Prefer precomputed last season stats if available
        tables = {t.name.upper() for t in spark.catalog.listTables()}
        if "GLOBAL_PLAYER_LAST_SEASON" in tables:
            df = (spark.table("GLOBAL_PLAYER_LAST_SEASON")
                    .where(F.col("player_id").cast("int") == F.lit(pid_int))
                    .limit(1))
            row = df.collect()
            out = {"_entity": "player", "id": str(pid_int)}
            if row:
                r = row[0].asDict(recursive=True)
                gp = int(r.get("gp") or 0)
                pts_total = int(r.get("pts_total") or 0)
                tpm_total = int(r.get("tpm_total") or 0)
                fgm_total = int(r.get("fgm_total") or 0)
                ftm_total = int(r.get("ftm_total") or 0)
                out.update({
                    "games_played": gp,
                    "total_points": pts_total,
                    "avg_points": round(float(r.get("pts_pg") or 0.0), 2) if gp else 0.0,
                    "made_threes": tpm_total,
                    "made_twos": max(0, fgm_total - tpm_total),
                    "made_fts": ftm_total,
                    "team_id": (int(r.get("team_id")) if r.get("team_id") is not None else None),
                    "season": r.get("season"),
                })
                # sanity check: fall back if clearly unrealistic
                try:
                    if out.get("avg_points", 0) > 60 or out.get("total_points", 0) > 5000:
                        raise ValueError("implausible last-season stats, recompute")
                except Exception:
                    pass
            if row and out.get("avg_points", 0) <= 60 and out.get("total_points", 0) <= 5000:
                # enrich with base bio + draft if available and return
                try:
                    base = (spark.table("GLOBAL_PLAYER")
                            .where(F.col("player_id").cast("int") == F.lit(pid_int))
                            .limit(1).collect())
                    if base:
                        rb = base[0].asDict(recursive=True)
                        out.update({k: rb.get(k) for k in ["full_name","position","team_id","experience","college","nationality"]})
                        if out.get("full_name") and not out.get("name"):
                            out["name"] = out.pop("full_name")
                    try:
                        dh = (spark.table("GLOBAL_DRAFT_HISTORY")
                                  .where(F.col("player_id").cast("int") == F.lit(pid_int))
                                  .orderBy(F.col("draft_year").asc(), F.col("overall_pick").asc())
                                  .limit(1).collect())
                        if dh:
                            dr = dh[0].asDict(recursive=True)
                            out.update({"draft_year": dr.get("draft_year"), "overall_pick": dr.get("overall_pick")})
                    except Exception:
                        pass
                except Exception:
                    pass
                return out
            # fall through to resilient computation
        # Fast path: pre-aggregated table
        tbls = tables
        if "GLOBAL_PLAYER_SEASON_FROM_PBP" in tbls:
            ps = spark.table("GLOBAL_PLAYER_SEASON_FROM_PBP")
            agg = (ps.where(F.col("player_id").cast("int") == F.lit(pid_int))
                     .agg(
                        F.sum(F.col("g")).alias("games_played"),
                        F.sum(F.col("pts")).alias("total_points"),
                        F.sum(F.col("fg3m")).alias("made_threes"),
                        (F.sum(F.col("fgm")) - F.sum(F.col("fg3m"))).alias("made_twos"),
                        F.sum(F.col("ftm")).alias("made_fts"),
                     ).collect())
            out = {"_entity": "player", "id": str(pid_int)}
            if agg:
                a = agg[0].asDict(recursive=True)
                gp = int(a.get("games_played") or 0)
                tp = int(a.get("total_points") or 0)
                out.update({
                    "games_played": gp,
                    "total_points": tp,
                    "avg_points": round((tp/gp), 2) if gp else 0.0,
                    "made_threes": int(a.get("made_threes") or 0),
                    "made_twos": int(a.get("made_twos") or 0),
                    "made_fts": int(a.get("made_fts") or 0),
                })
            # enrich with base bio + draft if available
            try:
                base = spark.table("GLOBAL_PLAYER").where(F.col("player_id").cast("int") == F.lit(pid_int)).limit(1).collect()
                if base:
                    r = base[0].asDict(recursive=True)
                    out.update({k: r.get(k) for k in ["full_name","position","team_id","experience","college","nationality"]})
                    if out.get("full_name") and not out.get("name"):
                        out["name"] = out.pop("full_name")
                try:
                    dh = (spark.table("GLOBAL_DRAFT_HISTORY")
                              .where(F.col("player_id").cast("int") == F.lit(pid_int))
                              .orderBy(F.col("draft_year").asc(), F.col("overall_pick").asc())
                              .limit(1).collect())
                    if dh:
                        dr = dh[0].asDict(recursive=True)
                        out.update({"draft_year": dr.get("draft_year"), "overall_pick": dr.get("overall_pick")})
                except Exception:
                    pass
            except Exception:
                pass
            return out
        # Fallback: compute quick summary on-the-fly
        out = _quick_player_summary(spark, pid_int)
        return out
    except Exception as e:
        return {"error": str(e)}

@app.get("/player/scoring-by-season/{pid}")
def player_scoring_by_season(pid: str, type: str = "regular"):
    if spark is None:
        return {"items": []}

    # parse id
    try:
        pid_i = int(str(pid))
    except Exception:
        return {"items": [], "error": "invalid player id"}

    # carica la tabella/materializzata
    wh = os.environ.get("WAREHOUSE_DIR", "warehouse")
    path = os.path.join(wh, "global_player_season_scoring")
    try:
        df = spark.read.parquet(path)
    except Exception:
        # se esiste come vista registrata
        tables = {t.name.upper() for t in spark.catalog.listTables()}
        if "GLOBAL_PLAYER_SEASON_SCORING" in tables:
            df = spark.table("GLOBAL_PLAYER_SEASON_SCORING")
        else:
            return {"items": []}

    # filtro stagione: regular/playoffs/both
    st = (type or "regular").strip().lower()
    if st in ("playoff", "playoffs", "po"):
        season_filter = "Playoffs"
    elif st in ("all", "both"):
        season_filter = None
    else:
        season_filter = "Regular Season"

    q = (F.col("player_id").cast("int") == pid_i)
    if season_filter:
        q = q & (F.col("season_type") == F.lit(season_filter))

    rows = (
        df.where(q)
          .orderBy(F.col("season_start").asc())
          .select(
              F.col("season_start").alias("season"),
              F.col("gp").alias("games"),
              F.col("pts").alias("pts"),
              F.col("ppg").alias("ppg"),
              F.col("three_pm").alias("threes"),
              F.col("two_pm").alias("twos"),
              F.col("ftm").alias("fts"),
          )
          .collect()
    )

    return {"items": [r.asDict(recursive=True) for r in rows]}

def _player_career_totals(spark: SparkSession, pid: int) -> dict:
    tables = {t.name.upper() for t in spark.catalog.listTables()}
    # Fast path: pre-aggregated
    if "GLOBAL_PLAYER_SEASON_FROM_PBP" in tables:
        ps = spark.table("GLOBAL_PLAYER_SEASON_FROM_PBP")
        pid_i = int(pid)
        agg = (ps.where(F.col("player_id").cast("int") == F.lit(pid_i))
                 .agg(
                     F.sum("g").alias("gp"),
                     F.sum("pts").alias("pts"),
                     F.sum("ast").alias("ast"),
                     F.sum("reb").alias("reb"),
                 ).collect())
        if not agg:
            return {"gp": 0, "pts": 0, "ast": 0, "reb": 0, "ppg": 0.0, "apg": 0.0, "rpg": 0.0}
        a = agg[0].asDict(recursive=True)
        gp = int(a.get("gp") or 0); pts = int(a.get("pts") or 0); ast = int(a.get("ast") or 0); reb = int(a.get("reb") or 0)
        return {"gp": gp, "pts": pts, "ast": ast, "reb": reb, "ppg": round(pts/gp,2) if gp else 0.0, "apg": round(ast/gp,2) if gp else 0.0, "rpg": round(reb/gp,2) if gp else 0.0}
    if "GLOBAL_PLAY_BY_PLAY" not in tables:
        return {"gp": 0, "pts": 0, "ast": 0, "reb": 0, "ppg": 0.0, "apg": 0.0, "rpg": 0.0}
    pbp = spark.table("GLOBAL_PLAY_BY_PLAY")
    pid_i = int(pid)
    ev = (pbp
          .select(F.col("game_id").cast("int").alias("game_id"),
                  F.col("eventnum").cast("int").alias("eventnum"),
                  F.col("eventmsgtype").cast("int").alias("t"),
                  F.col("player1_id").cast("int").alias("p1"),
                  F.col("player2_id").cast("int").alias("p2"),
                  F.col("homedescription").alias("hd"),
                  F.col("visitordescription").alias("vd"),
                  F.col("neutraldescription").alias("nd"),
                  F.col("player1_name").alias("p1n"),
                  F.col("player2_name").alias("p2n")))
    ev_any = ev.where((F.col("p1") == pid_i) | (F.col("p2") == pid_i))
    if ev_any.limit(1).count() == 0:
        # Fallback by name
        try:
            nm = (spark.table("GLOBAL_PLAYER")
                      .where(F.col("player_id").cast("int") == F.lit(pid_i))
                      .select(F.col("full_name").alias("n")).limit(1).collect())
            if nm:
                full_name = nm[0]["n"]
                ev_any = ev.where((F.lower(F.col("p1n")) == F.lit(str(full_name).lower())) | (F.lower(F.col("p2n")) == F.lit(str(full_name).lower())))
        except Exception:
            pass
    gp = (ev_any.select("game_id").distinct().count())

    # Points via event types (no score delta requirement)
    is_make = (F.col("t") == 1)
    is_three = F.lower(F.coalesce(F.col("hd"), F.col("vd"), F.col("nd"))).contains(F.lit("3pt")) | \
               F.lower(F.coalesce(F.col("hd"), F.col("vd"), F.col("nd"))).contains(F.lit("3-pt")) | \
               F.lower(F.coalesce(F.col("hd"), F.col("vd"), F.col("nd"))).contains(F.lit("three"))
    is_ft = (F.col("t") == 3)
    is_miss_text = F.lower(F.coalesce(F.col("hd"), F.col("vd"), F.col("nd"))).contains(F.lit("miss"))
    shooter2 = ev.where(F.col("p1") == pid_i)
    if shooter2.limit(1).count() == 0:
        try:
            nm = (spark.table("GLOBAL_PLAYER")
                      .where(F.col("player_id").cast("int") == F.lit(pid_i))
                      .select(F.col("full_name").alias("n")).limit(1).collect())
            if nm:
                full_name = nm[0]["n"]
                shooter2 = ev.where(F.lower(F.col("p1n")) == F.lit(str(full_name).lower()))
        except Exception:
            pass
    agg2 = shooter2.agg(
        F.sum(F.when(is_make & (~is_three), 1).otherwise(0)).alias("twos"),
        F.sum(F.when(is_make & is_three, 1).otherwise(0)).alias("threes"),
        F.sum(F.when(is_ft & (~is_miss_text), 1).otherwise(0)).alias("fts"),
    ).collect()
    if agg2:
        twos = int(agg2[0]["twos"] or 0)
        threes = int(agg2[0]["threes"] or 0)
        fts = int(agg2[0]["fts"] or 0)
        pts = 2*twos + 3*threes + fts
    else:
        pts = 0

    # Assists approximate: player2 on made shot (t == 1)
    ast = int(ev.where((F.col("p2") == pid_i) & (F.col("t") == 1)).count())
    # Rebounds: rebound event (t == 4) by player1
    reb = int(ev.where((F.col("p1") == pid_i) & (F.col("t") == 4)).count())
    ppg = float(pts / gp) if gp else 0.0
    apg = float(ast / gp) if gp else 0.0
    rpg = float(reb / gp) if gp else 0.0
    return {"gp": int(gp), "pts": pts, "ast": ast, "reb": reb, "ppg": round(ppg, 2), "apg": round(apg, 2), "rpg": round(rpg, 2)}

@app.get("/player/career/{pid}")
def player_career(pid: str):
    if spark is None:
        return {"error": "service starting"}
    try:
        pid_i = int(str(pid))
    except Exception:
        return {"error": "invalid player id"}
    try:
        tables = {t.name.upper() for t in spark.catalog.listTables()}
        # Compute robust PBP-based first
        pbp_out = _player_career_totals(spark, pid_i)
        # If warehouse career exists and looks plausible, compare and pick sane result
        if "GLOBAL_PLAYER_CAREER" in tables:
            try:
                df = (spark.table("GLOBAL_PLAYER_CAREER")
                      .where((F.col("season_type") == F.lit("Regular Season")) & (F.col("player_id").cast("int") == F.lit(pid_i)))
                      .limit(1))
                row = df.collect()
                if row:
                    r = row[0].asDict(recursive=True)
                    gp = int(r.get("gp") or 0)
                    pts = int(r.get("pts_total") or 0)
                    # sanity check: reject implausible values
                    if gp <= 2000 and pts <= 60000:
                        out = {
                            "gp": gp,
                            "pts": pts,
                            "ast": int(r.get("ast_total") or 0),
                            "reb": int(r.get("trb_total") or 0),
                            "ppg": round(float(r.get("pts_pg") or (pts/gp if gp else 0.0)), 2) if gp else 0.0,
                            "apg": round(float(r.get("ast_pg") or 0.0), 2) if r.get("ast_pg") is not None else (round((pbp_out.get("ast",0)/gp),2) if gp else 0.0),
                            "rpg": round(float(r.get("trb_pg") or 0.0), 2) if r.get("trb_pg") is not None else (round((pbp_out.get("reb",0)/gp),2) if gp else 0.0),
                            "fg_pct": round(float(r.get("fg_pct") or 0.0) * 100.0, 1) if r.get("fg_pct") is not None else None,
                            "fg3_pct": round(float(r.get("tp_pct") or 0.0) * 100.0, 1) if r.get("tp_pct") is not None else None,
                            "ft_pct": round(float(r.get("ft_pct") or 0.0) * 100.0, 1) if r.get("ft_pct") is not None else None,
                        }
                        return out
            except Exception:
                pass
        return pbp_out
    except Exception as e:
        return {"error": str(e)}

@app.get("/player/season-summary/{pid}")
def player_season_summary(pid: str):
    """Return per-season basic box metrics and a career row similar to BR header summary.

    Metrics: G, PTS, AST, REB, FGM, FGA, FG%, 3PM, 3PA, 3P%, FTM, FTA, FT%.
    Derived from GLOBAL_PLAY_BY_PLAY and GLOBAL_GAME text fields.
    """
    if spark is None:
        return {"items": [], "career": {}}
    try:
        pid_i = int(str(pid))
    except Exception:
        return {"items": [], "career": {}, "error": "invalid player id"}
    tables = {t.name.upper() for t in spark.catalog.listTables()}
    # Optionally prefer GLOBAL_PLAYER_STATS via env toggle; default disabled to avoid inflated data
    # Revert: use parquet GLOBAL_PLAYER_STATS by default (previous behavior)
    use_gps = os.environ.get("USE_GLOBAL_PLAYER_STATS_FOR_SEASONS", "1").lower() in ["1","true","yes"]
    if use_gps and ("GLOBAL_PLAYER_STATS" in tables):
        try:
            df = (spark.table("GLOBAL_PLAYER_STATS")
                    .where((F.col("season_type") == F.lit("Regular Season")) & (F.col("player_id").cast("int") == F.lit(int(pid))))
                    .orderBy(F.col("season").asc()))
            rows = df.collect()
            items = []
            career = {"g": 0, "pts": 0, "ast": 0, "reb": 0, "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0}
            for r in rows:
                d = r.asDict(recursive=True)
                g_ = int(d.get("gp") or 0)
                pts_ = int(d.get("pts_total") or 0)
                ast_ = int(d.get("ast_total") or 0)
                reb_ = int(d.get("trb_total") or 0)
                fgm_ = int(d.get("fgm_total") or 0); fga_ = int(d.get("fga_total") or 0)
                fg3m_ = int(d.get("tpm_total") or 0); fg3a_ = int(d.get("tpa_total") or 0)
                ftm_ = int(d.get("ftm_total") or 0); fta_ = int(d.get("fta_total") or 0)
                items.append({
                    "season": d.get("season"),
                    "g": g_, "pts": pts_, "ast": ast_, "reb": reb_,
                    "fgm": fgm_, "fga": fga_, "fg_pct": (round(float(d.get("fg_pct") or 0.0) * 100.0, 1) if d.get("fg_pct") is not None else None),
                    "fg3m": fg3m_, "fg3a": fg3a_, "fg3_pct": (round(float(d.get("tp_pct") or 0.0) * 100.0, 1) if d.get("tp_pct") is not None else None),
                    "ftm": ftm_, "fta": fta_, "ft_pct": (round(float(d.get("ft_pct") or 0.0) * 100.0, 1) if d.get("ft_pct") is not None else None),
                    "ppg": (round(float(d.get("pts_pg") or (pts_/g_ if g_ else 0.0)), 2) if g_ else 0.0),
                    "apg": (round(float(d.get("ast_pg") or (ast_/g_ if g_ else 0.0)), 2) if g_ else 0.0),
                    "rpg": (round(float(d.get("trb_pg") or (reb_/g_ if g_ else 0.0)), 2) if g_ else 0.0),
                })
                career["g"] += g_; career["pts"] += pts_; career["ast"] += ast_; career["reb"] += reb_
                career["fgm"] += fgm_; career["fga"] += fga_; career["fg3m"] += fg3m_; career["fg3a"] += fg3a_; career["ftm"] += ftm_; career["fta"] += fta_
            if career["g"] > 0:
                career.update({
                    "ppg": round(career["pts"]/career["g"], 2),
                    "apg": round(career["ast"]/career["g"], 2),
                    "rpg": round(career["reb"]/career["g"], 2),
                    "fg_pct": round((career["fgm"]/career["fga"]*100) if career["fga"] else 0.0, 1),
                    "fg3_pct": round((career["fg3m"]/career["fg3a"]*100) if career["fg3a"] else 0.0, 1),
                    "ft_pct": round((career["ftm"]/career["fta"]*100) if career["fta"] else 0.0, 1),
                })
            # Sanity guard: reject implausible per-season rows (e.g., >100 GP or >50 PPG)
            bad = False
            for it in items:
                g = int(it.get("g") or 0)
                ppg = float(it.get("ppg") or 0.0)
                if g > 100 or ppg > 50.0:
                    bad = True
                    break
            if (not bad) and (career.get("pts", 0) <= 60000 and career.get("g", 0) <= 2000):
                return {"items": items, "career": career}
            # else: fall through to PBP-based path
        except Exception:
            pass
    # Fast path: use pre-aggregated per-season totals if present
    if "GLOBAL_PLAYER_SEASON_FROM_PBP" in tables:
        try:
            ps = spark.table("GLOBAL_PLAYER_SEASON_FROM_PBP")
            rows = (ps.where(F.col("player_id").cast("int") == F.lit(pid_i))
                      .orderBy(F.col("season").asc()).collect())
            items = []
            career = {"g": 0, "pts": 0, "ast": 0, "reb": 0, "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0}
            for r in rows:
                d = r.asDict(recursive=True)
                g_ = int(d.get("g") or 0)
                p_ = int(d.get("pts") or 0)
                a_ = int(d.get("ast") or 0)
                r_ = int(d.get("reb") or 0)
                fgm_ = int(d.get("fgm") or 0); fga_ = int(d.get("fga") or 0)
                fg3m_ = int(d.get("fg3m") or 0); fg3a_ = int(d.get("fg3a") or 0)
                ftm_ = int(d.get("ftm") or 0); fta_ = int(d.get("fta") or 0)
                items.append({
                    "season": int(d.get("season") or 0),
                    "g": g_, "pts": p_, "ast": a_, "reb": r_,
                    "fgm": fgm_, "fga": fga_, "fg_pct": (round(fgm_/fga_*100,1) if fga_ else 0.0),
                    "fg3m": fg3m_, "fg3a": fg3a_, "fg3_pct": (round(fg3m_/fg3a_*100,1) if fg3a_ else 0.0),
                    "ftm": ftm_, "fta": fta_, "ft_pct": (round(ftm_/fta_*100,1) if fta_ else 0.0),
                    "ppg": (round(p_/g_, 2) if g_ else 0.0), "apg": (round(a_/g_, 2) if g_ else 0.0), "rpg": (round(r_/g_, 2) if g_ else 0.0)
                })
                career["g"] += g_; career["pts"] += p_; career["ast"] += a_; career["reb"] += r_
                career["fgm"] += fgm_; career["fga"] += fga_; career["fg3m"] += fg3m_; career["fg3a"] += fg3a_; career["ftm"] += ftm_; career["fta"] += fta_
            if career["g"] > 0:
                career.update({
                    "ppg": round(career["pts"]/career["g"], 2),
                    "apg": round(career["ast"]/career["g"], 2),
                    "rpg": round(career["reb"]/career["g"], 2),
                    "fg_pct": round((career["fgm"]/career["fga"]*100) if career["fga"] else 0.0, 1),
                    "fg3_pct": round((career["fg3m"]/career["fg3a"]*100) if career["fg3a"] else 0.0, 1),
                    "ft_pct": round((career["ftm"]/career["fta"]*100) if career["fta"] else 0.0, 1),
                })
            return {"items": items, "career": career}
        except Exception:
            pass
    if ("GLOBAL_PLAY_BY_PLAY" not in tables) or ("GLOBAL_GAME" not in tables):
        return {"items": [], "career": {}}
    pbp = spark.table("GLOBAL_PLAY_BY_PLAY")
    g = spark.table("GLOBAL_GAME").select(F.col("game_id").cast("int").alias("game_id"), F.col("game_date").cast("date").alias("game_date"))

    cols = [
        F.col("game_id").cast("int").alias("game_id"),
        F.col("eventnum").cast("int").alias("eventnum"),
        F.col("eventmsgtype").cast("int").alias("t"),
        F.col("eventmsgactiontype").cast("int").alias("a"),
        F.col("player1_id").cast("int").alias("p1"),
        F.col("player2_id").cast("int").alias("p2"),
        F.col("homedescription").alias("hd"),
        F.col("visitordescription").alias("vd"),
        F.col("neutraldescription").alias("nd"),
        F.col("score")
    ]
    ev = pbp.select(*cols)
    ev = ev.where((F.col("p1") == pid_i) | (F.col("p2") == pid_i))
    if ev.limit(1).count() == 0:
        return {"items": [], "career": {"gp": 0, "pts": 0, "ast": 0, "reb": 0}}

    desc = F.coalesce(F.col("hd"), F.col("vd"), F.col("nd")).alias("desc")
    ev = ev.withColumn("desc", desc)
    # Identify tags
    is_make = (F.col("t") == 1)
    is_miss = (F.col("t") == 2)
    is_ft = (F.col("t") == 3)
    is_reb = (F.col("t") == 4)
    is_ast = (F.col("t") == 1) & (F.col("p2") == pid_i)
    # three-pointer detection via desc
    is_three = F.lower(F.col("desc")).contains(F.lit("3pt")) | F.lower(F.col("desc")).contains(F.lit("3-pt")) | F.lower(F.col("desc")).contains(F.lit("three"))
    # free-throw miss detection via desc contains 'miss'
    is_miss_text = F.lower(F.col("desc")).contains(F.lit("miss"))

    # Shooter perspective: p1
    shooter = ev.where(F.col("p1") == pid_i)
    shots = shooter.where(is_make | is_miss)
    threes = shooter.where(is_three & (is_make | is_miss))
    fgm = F.sum(F.when(is_make, 1).otherwise(0)).alias("fgm")
    fga = F.sum(F.when(is_make | is_miss, 1).otherwise(0)).alias("fga")
    fg3m = F.sum(F.when(is_three & is_make, 1).otherwise(0)).alias("fg3m")
    fg3a = F.sum(F.when(is_three & (is_make | is_miss), 1).otherwise(0)).alias("fg3a")
    # free throws: p1 on FT events; text 'miss' indicates missed
    fts = shooter.where(is_ft)
    ftm = F.sum(F.when(is_ft & (~is_miss_text), 1).otherwise(0)).alias("ftm")
    fta = F.sum(F.when(is_ft, 1).otherwise(0)).alias("fta")
    # points via score delta for shooter p1
    from pyspark.sql import Window as _W
    win = _W.partitionBy("game_id").orderBy(F.col("eventnum"))
    shooter_sc = (shooter.where(F.col("score").isNotNull())
                  .withColumn("score_clean", F.regexp_replace("score", "\\s+", ""))
                  .withColumn("arr", F.split("score_clean", "-"))
                  .withColumn("sh", F.when(F.size("arr") == 2, F.element_at("arr", 1).cast("int")))
                  .withColumn("sa", F.when(F.size("arr") == 2, F.element_at("arr", 2).cast("int")))
                  .withColumn("prevh", F.coalesce(F.lag("sh").over(win), F.lit(0)))
                  .withColumn("preva", F.coalesce(F.lag("sa").over(win), F.lit(0)))
                  .withColumn("pts", F.greatest(F.col("sh")-F.col("prevh"), F.col("sa")-F.col("preva"))))
    pts = F.sum(F.when(F.col("pts") > 0, F.col("pts")).otherwise(0)).alias("pts")
    ast = F.sum(F.when(is_ast, 1).otherwise(0)).alias("ast")
    reb = F.sum(F.when(is_reb & (F.col("p1") == pid_i), 1).otherwise(0)).alias("reb")

    # join game date for season
    # Join on the common key to avoid duplicate columns/ambiguity
    j = ev.join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
    base = (j.groupBy("season")
              .agg(F.countDistinct("game_id").alias("g")))
    shooter_by_season = shooter.join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
    shots_season = (shooter_by_season.groupBy("season").agg(fgm, fga, fg3m, fg3a, ftm, fta))
    # pts/ast/reb per season from earlier expressions need recomputation with season
    # Derive points from makes + FT (no score delta).
    # Compute inside shots_season columns so we can compose later
    shots_season = shots_season.withColumn("pts", F.col("ftm") + (F.col("fgm") - F.col("fg3m")) * F.lit(2) + F.col("fg3m") * F.lit(3))
    ast_season = (ev.where(is_ast).join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
                  .groupBy("season").agg(ast))
    reb_season = (ev.where(is_reb & (F.col("p1") == pid_i)).join(g, "game_id", "left").withColumn("season", F.year(F.col("game_date")))
                  .groupBy("season").agg(reb))

    out = (base
           .join(shots_season, "season", "left")
           .join(ast_season, "season", "left")
           .join(reb_season, "season", "left")
           .orderBy(F.col("season").asc()))

    rows = out.collect()
    items = []
    career = {"g": 0, "pts": 0, "ast": 0, "reb": 0, "fgm": 0, "fga": 0, "fg3m": 0, "fg3a": 0, "ftm": 0, "fta": 0}
    for r in rows:
        rec = {k: (int(r[k]) if r[k] is not None and k in ["season","g","fgm","fga","fg3m","fg3a","ftm","fta","pts","ast","reb"] else r[k]) for k in r.asDict().keys()}
        season = int(rec.get("season") or 0) if rec.get("season") is not None else None
        g_ = int(rec.get("g") or 0)
        p_ = int(rec.get("pts") or 0)
        a_ = int(rec.get("ast") or 0)
        r_ = int(rec.get("reb") or 0)
        fgm_ = int(rec.get("fgm") or 0); fga_ = int(rec.get("fga") or 0)
        fg3m_ = int(rec.get("fg3m") or 0); fg3a_ = int(rec.get("fg3a") or 0)
        ftm_ = int(rec.get("ftm") or 0); fta_ = int(rec.get("fta") or 0)
        items.append({
            "season": season,
            "g": g_,
            "pts": p_,
            "ast": a_,
            "reb": r_,
            "fgm": fgm_, "fga": fga_, "fg_pct": (round(fgm_/fga_*100,1) if fga_ else 0.0),
            "fg3m": fg3m_, "fg3a": fg3a_, "fg3_pct": (round(fg3m_/fg3a_*100,1) if fg3a_ else 0.0),
            "ftm": ftm_, "fta": fta_, "ft_pct": (round(ftm_/fta_*100,1) if fta_ else 0.0),
            "ppg": (round(p_/g_, 2) if g_ else 0.0), "apg": (round(a_/g_, 2) if g_ else 0.0), "rpg": (round(r_/g_, 2) if g_ else 0.0)
        })
        # career totals
        career["g"] += g_; career["pts"] += p_; career["ast"] += a_; career["reb"] += r_
        career["fgm"] += fgm_; career["fga"] += fga_; career["fg3m"] += fg3m_; career["fg3a"] += fg3a_; career["ftm"] += ftm_; career["fta"] += fta_

    if career["g"] > 0:
        career.update({
            "ppg": round(career["pts"]/career["g"], 2),
            "apg": round(career["ast"]/career["g"], 2),
            "rpg": round(career["reb"]/career["g"], 2),
            "fg_pct": round((career["fgm"]/career["fga"]*100) if career["fga"] else 0.0, 1),
            "fg3_pct": round((career["fg3m"]/career["fg3a"]*100) if career["fg3a"] else 0.0, 1),
            "ft_pct": round((career["ftm"]/career["fta"]*100) if career["fta"] else 0.0, 1),
        })
    return {"items": items, "career": career}

# ---------- Debug helpers ----------
@app.get("/debug/tables")
def debug_tables():
    try:
        names = [t.name for t in spark.catalog.listTables()] if spark is not None else []
        return {"warehouse": os.path.abspath(WAREHOUSE_DIR), "tables": names}
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/config")
def debug_config():
    try:
        return {
            "WAREHOUSE_DIR": os.path.abspath(WAREHOUSE_DIR),
            "SPARK_LOCAL_DIRS": os.environ.get("SPARK_LOCAL_DIRS"),
            "JAVA_HOME": os.environ.get("JAVA_HOME"),
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/debug/pbp-check/{pid}")
def debug_pbp_check(pid: str):
    if spark is None:
        return {"error": "service starting"}
    try:
        pid_i = int(str(pid))
    except Exception:
        return {"error": "invalid player id"}
    try:
        tables = {t.name.upper() for t in spark.catalog.listTables()}
        if "GLOBAL_PLAY_BY_PLAY" not in tables:
            return {"error": "GLOBAL_PLAY_BY_PLAY missing"}
        pbp = spark.table("GLOBAL_PLAY_BY_PLAY")
        ev = pbp.select(
            F.col("game_id").cast("int").alias("game_id"),
            F.col("player1_id").cast("int").alias("p1"),
            F.col("player2_id").cast("int").alias("p2"),
            F.col("player1_name").alias("p1n"),
            F.col("player2_name").alias("p2n"),
        )
        cnt_id = int(ev.where((F.col("p1") == pid_i) | (F.col("p2") == pid_i)).count())
        nm_row = (spark.table("GLOBAL_PLAYER")
                        .where(F.col("player_id").cast("int") == F.lit(pid_i))
                        .select(F.col("full_name").alias("n")).limit(1).collect())
        n = nm_row[0]["n"] if nm_row else None
        if n:
            cnt_name = int(ev.where((F.lower(F.col("p1n")) == F.lit(str(n).lower())) | (F.lower(F.col("p2n")) == F.lit(str(n).lower()))).count())
        else:
            cnt_name = 0
        sample = []
        if cnt_name and cnt_id == 0:
            sample = [r.asDict(recursive=True) for r in ev.where((F.lower(F.col("p1n")) == F.lit(str(n).lower())) | (F.lower(F.col("p2n")) == F.lit(str(n).lower()))).limit(5).collect()]
        return {"id": pid_i, "name": n, "pbp_by_id": cnt_id, "pbp_by_name": cnt_name, "sample": sample}
    except Exception as e:
        return {"error": str(e)}

@app.get("/entity/team/{tid}")
def entity_team(tid: str):
    import os
    from pyspark.sql import functions as F

    # utility locali
    DATA_DIR = os.getenv("DATA_DIR", "data")

    def _table_exists(name: str) -> bool:
        try:
            spark.table(name).limit(1).collect()
            return True
        except Exception:
            return False

    def _read_csv_local(name: str):
        path = os.path.join(DATA_DIR, f"{name}.csv")
        try:
            df = (spark.read.option("header", True).option("inferSchema", True).csv(path))
            # normalizza colonne
            for c in df.columns:
                df = df.withColumnRenamed(c, c.strip().lower())
            return df
        except Exception:
            return None

    def _pick_key(df):
        for k in ("team_id", "teamid", "tid"):
            if k in df.columns:
                return k
        return None

    # --- NUOVO: helper per scegliere la prima colonna disponibile e rinominarla ---
    def _first_col(df, *cands):
        cols = set(df.columns)
        for c in cands:
            if c in cols:
                from pyspark.sql import functions as F
                return F.col(c)
        return None

    def _select_team_info(df):
        """
        Restituisce una select con alias "canonici" (city/state/arena/arena_capacity/conference/division)
        pescando dai nomi effettivi del DF (es. team_city, arena_name, ecc.).
        """
        from pyspark.sql import functions as F
        cols = []
        c_city   = _first_col(df, "city", "team_city", "teamcity")
        c_state  = _first_col(df, "state", "team_state", "teamstate")
        c_arena  = _first_col(df, "arena", "arena_name", "stadium", "arena_name_text")
        c_acap   = _first_col(df, "arena_capacity", "capacity", "arena_cap", "arena_capacity_text")
        c_conf   = _first_col(df, "conference", "team_conference", "conf")
        c_div    = _first_col(df, "division", "team_division", "div")

        if c_city  is not None: cols.append(c_city.alias("city"))
        if c_state is not None: cols.append(c_state.alias("state"))
        if c_arena is not None: cols.append(c_arena.alias("arena"))
        if c_acap  is not None: cols.append(c_acap.alias("arena_capacity"))
        if c_conf  is not None: cols.append(c_conf.alias("conference"))
        if c_div   is not None: cols.append(c_div.alias("division"))

        # se nessuna delle colonne è presente, selezioniamo una sola costante per evitare select([]) che esplode
        if not cols:
            cols = [F.lit(None).alias("city")]
        return df.select(*cols)

    # --- base: GLOBAL_TEAM ---
    rec = {"_entity":"team","id": str(tid), "name": None, "city": None, "state": None,
           "conference": None, "division": None, "arena": None, "arena_capacity": None}

    try:
        g = spark.table("GLOBAL_TEAM").alias("g")
        row = (g.where(F.col("g.team_id").cast("string") == F.lit(tid))
                 .select(
                    F.col("g.team_id").cast("string").alias("id"),
                    F.col("g.team_name").alias("name"),
                    F.col("g.city").alias("city"),
                    F.col("g.state").alias("state"),
                    F.col("g.conference").alias("conference"),
                    F.col("g.division").alias("division"),
                    F.col("g.arena").alias("arena")
                 ).limit(1).collect())
        if row:
            rec.update(row[0].asDict(recursive=True))
    except Exception:
        pass

    # --- enrich da TEAM_INFO_COMMON / TEAM_DETAILS se esistono ---
    for tname in ("TEAM_INFO_COMMON", "TEAM_DETAILS", "GLOBAL_TEAM_DETAILS"):
        if not _table_exists(tname):
            continue
        tdf = spark.table(tname)
        key = _pick_key(tdf)
        if not key:
            continue
        add = (_select_team_info(
          tdf.where(F.col(key).cast("string")==F.lit(tid))
        ).limit(1).collect())
        if add:
            dd = add[0].asDict(recursive=True)
            rec["city"]            = rec["city"] or dd.get("city")
            rec["state"]           = rec["state"] or dd.get("state")
            rec["arena"]           = rec["arena"] or dd.get("arena") or dd.get("arena_name")
            rec["arena_capacity"]  = rec["arena_capacity"] or dd.get("arena_capacity")
            rec["conference"]      = rec["conference"] or dd.get("conference")
            rec["division"]        = rec["division"] or dd.get("division")

    # --- fallback CSV se ancora mancano campi ---
    if (not rec.get("arena")) or (not rec.get("state")) or (rec.get("arena_capacity") in (None, "", "null")):
        for fname in ("team_info_common", "team_details"):
            df = _read_csv_local(fname)
            if df is None:
                continue
            key = _pick_key(df)
            if not key:
                continue
            rows = (_select_team_info(
                df.where(F.col(key).cast("string")==F.lit(tid))
            ).limit(1).collect())
            if rows:
                x = rows[0].asDict(recursive=True)
                rec["city"]            = rec["city"] or x.get("city")
                rec["state"]           = rec["state"] or x.get("state")
                rec["arena"]           = rec["arena"] or x.get("arena") or x.get("arena_name")
                rec["arena_capacity"]  = rec["arena_capacity"] or x.get("arena_capacity")
                rec["conference"]      = rec["conference"] or x.get("conference")
                rec["division"]        = rec["division"] or x.get("division")

    # --- roster: GLOBAL_PLAYER → fallback CSV common_player_info ---
    roster = []
    try:
        if _table_exists("GLOBAL_PLAYER"):
            gp = spark.table("GLOBAL_PLAYER")
            rows = (gp.where(F.col("team_id").cast("string")==F.lit(tid))
                      .select(
                        F.col("player_id").cast("string").alias("id"),
                        F.col("full_name").alias("name"),
                        F.col("position"),
                        F.col("jersey_number").alias("jersey"))
                      .orderBy(F.col("name").asc())
                      .collect())
            roster = [r.asDict(recursive=True) for r in rows]
    except Exception:
        pass

    if not roster:
        cpi = _read_csv_local("common_player_info")
        if cpi is not None:
            cols = set(cpi.columns)
            pid = "player_id" if "player_id" in cols else ("person_id" if "person_id" in cols else None)
            name = "display_first_last" if "display_first_last" in cols else ("display_name" if "display_name" in cols else None)
            key = _pick_key(cpi)
            if pid and name and key:
                dfp = cpi.where(F.col(key).cast("string")==F.lit(tid))
                if "is_active" in cols:
                    dfp = dfp.where(F.col("is_active")==1)
                elif "rosterstatus" in cols:
                    dfp = dfp.where(F.lower(F.col("rosterstatus"))==F.lit("active"))
                sel = [pid, name] + (["position"] if "position" in cols else []) + (["jersey_number"] if "jersey_number" in cols else [])
                rows = dfp.select(*sel).orderBy(F.col(name).asc()).collect()
                for r in rows:
                    rr = r.asDict(recursive=True)
                    roster.append({
                        "id": str(rr.get(pid)),
                        "name": rr.get(name),
                        "position": rr.get("position") if "position" in sel else None,
                        "jersey": rr.get("jersey_number") if "jersey_number" in sel else None
                    })

    rec["roster"] = roster
    rec["roster_count"] = len(roster)

    # cleanup stringhe vuote
    def _clean(v):
        if v is None: return None
        s = str(v).strip()
        return s if s and s.lower() not in ("null","none","nan") else None

    rec["arena"] = _clean(rec.get("arena"))
    rec["state"] = _clean(rec.get("state"))
    rec["arena_capacity"] = _clean(rec.get("arena_capacity"))

    return rec



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







