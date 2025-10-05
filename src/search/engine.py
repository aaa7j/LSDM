# src/search/engine.py
from __future__ import annotations
import re
from typing import Dict, List, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, lower, trim, length, levenshtein, coalesce

from functools import reduce
from operator import and_ as op_and

# ----------------------------
# Utils
# ----------------------------
_WORD = re.compile(r"[A-Za-z0-9]+")

def _tokens(q: str) -> List[str]:
    return [t.lower() for t in _WORD.findall(q)]

def _mk_like_any_for_token(cols: List[str], token: str):
    """
    Ritorna una singola espressione booleana: OR di contains(token) su tutte le colonne.
    """
    expr = None
    for c in cols:
        cond = lower(trim(col(c))).contains(token)
        expr = cond if expr is None else (expr | cond)
    return expr

def _all_tokens_condition(cols: List[str], qtok: List[str]):
    """
    Ogni token deve essere presente in almeno una colonna (AND tra i token).
    Se qtok è vuoto -> None.
    """
    wheres = []
    for t in qtok:
        like_any = _mk_like_any_for_token(cols, t)
        if like_any is not None:
            wheres.append(like_any)
    if not wheres:
        return None
    return reduce(op_and, wheres)

def _score_contains(df: DataFrame, cols: List[str], qtok: List[str]):
    """
    Score di rilevanza:
      +3 per exact full_name match (se presente)
      +2 per startswith su qualsiasi colonna (per token)
      +1 per contains su qualsiasi colonna (per token)
      -penalità Levenshtein normalizzata sulla colonna "migliore"
    """
    from pyspark.sql.functions import when, sum as fsum

    score = lit(0)

    # exact su full_name se c'è
    if "full_name" in df.columns:
        score = score + when(lower(col("full_name")) == lit(" ".join(qtok)), lit(3)).otherwise(lit(0))

    # startswith / contains per token
    for t in qtok:
        starts_any = None
        contains_any = None
        for c in cols:
            base = lower(trim(col(c)))
            s = base.startswith(t)
            ctn = base.contains(t)
            starts_any = s if starts_any is None else (starts_any | s)
            contains_any = ctn if contains_any is None else (contains_any | ctn)
        if starts_any is not None:
            score = score + when(starts_any, lit(2)).otherwise(lit(0))
        if contains_any is not None:
            score = score + when(contains_any, lit(1)).otherwise(lit(0))

    # penalità Levenshtein
    if "full_name" in df.columns:
        lv = levenshtein(lower(col("full_name")), lit(" ".join(qtok)))
        penalty = (lv / (length(col("full_name")) + lit(1))).cast("double")
        score = score - penalty
    elif cols:
        lv = levenshtein(lower(col(cols[0])), lit(" ".join(qtok)))
        penalty = (lv / (length(col(cols[0])) + lit(1))).cast("double")
        score = score - penalty

    return score.alias("_score")


# ----------------------------
# Query builders per tabella
# ----------------------------
def _candidate_players(spark: SparkSession, q: str, limit: int) -> DataFrame:
    qtok = _tokens(q)
    df = spark.table("GLOBAL_PLAYER")

    cols = [c for c in ["full_name", "first_name", "last_name", "position", "college", "nationality"] if c in df.columns]

    cond = _all_tokens_condition(cols, qtok)
    if cond is not None:
        df = df.where(cond)

    score = _score_contains(df, cols, qtok)

    out = (df
           .select(
               col("player_id").cast("string"),
               col("full_name"),
               col("first_name"),
               col("last_name"),
               col("position"),
               col("height"),
               col("weight"),
               col("birth_date"),
               col("nationality"),
               col("college"),
               col("experience"),
               col("team_id"),
               col("jersey_number"),
               score
           )
           .withColumn("_entity", lit("player"))
           .orderBy(col("_score").desc_nulls_last(), col("full_name").asc())
           .limit(limit)
    )
    return out

def _candidate_teams(spark: SparkSession, q: str, limit: int) -> DataFrame:
    from pyspark.sql.functions import concat_ws
    qtok = _tokens(q)
    df = spark.table("GLOBAL_TEAM")

    cols = [c for c in ["team_name", "city", "conference", "division"] if c in df.columns]

    # sintetico full_name per city + name (aiuta score/exact)
    df = df.withColumn("full_name", concat_ws(" ", coalesce(col("city"), lit("")), coalesce(col("team_name"), lit(""))))

    cond = _all_tokens_condition(cols + ["full_name"], qtok)
    if cond is not None:
        df = df.where(cond)

    score = _score_contains(df, ["full_name", "team_name", "city"], qtok)

    out = (df
           .select(
               col("team_id").cast("string"),
               col("team_name"),
               col("city"),
               col("state"),
               col("arena"),
               col("conference"),
               col("division"),
               score
           )
           .withColumn("_entity", lit("team"))
           .orderBy(col("_score").desc_nulls_last(), col("team_name").asc())
           .limit(limit)
    )
    return out

def _candidate_games(spark: SparkSession, q: str, limit: int) -> DataFrame:
    """
    Ricerca partite:
      - per ID numerico lungo
      - per data (yyyy / yyyy-mm / yyyy-mm-dd)
      - per nomi/città team (join con GLOBAL_TEAM)
    """
    from pyspark.sql.functions import date_format, concat_ws, coalesce as Fcoalesce

    qtok = _tokens(q)
    g = spark.table("GLOBAL_GAME").alias("g")
    df = g

    # 1) match per ID
    numeric = "".join([t for t in qtok if t.isdigit()])
    if len(numeric) >= 6:
        df = df.where(lower(col("game_id")).contains(numeric))

    # 2) match per data
    date_txt = " ".join(qtok)
    ymd = re.findall(r"\b(20\d{2}|19\d{2})(?:[-/](\d{1,2})(?:[-/](\d{1,2}))?)?\b", date_txt)
    if ymd:
        y, m, d = ymd[0][0], (ymd[0][1] or ""), (ymd[0][2] or "")
        if d:
            df = df.where(date_format(col("game_date"), "yyyy-MM-dd") == f"{int(y):04d}-{int(m):02d}-{int(d):02d}")
        elif m:
            df = df.where(date_format(col("game_date"), "yyyy-MM") == f"{int(y):04d}-{int(m):02d}")
        else:
            df = df.where(date_format(col("game_date"), "yyyy") == f"{int(y):04d}")

    # 3) join team per cercare per nomi/città
    t = spark.table("GLOBAL_TEAM").select(
        col("team_id").alias("tid"),
        coalesce(col("team_name"), lit("")).alias("tname"),
        coalesce(col("city"), lit("")).alias("tcity")
    ).alias("t")
    u = t.alias("u")

    df = (df
          .join(t, col("g.home_team_id") == col("t.tid"), "left")
          .join(u, col("g.away_team_id") == col("u.tid"), "left")
    )

    # token testuali devono comparire in uno dei nomi/città
    text_tokens = [t for t in qtok if not t.isdigit()]
    if text_tokens:
        wheres = []
        for tk in text_tokens:
            like_any = (
                lower(col("t.tname")).contains(tk) |
                lower(col("t.tcity")).contains(tk) |
                lower(col("u.tname")).contains(tk) |
                lower(col("u.tcity")).contains(tk)
            )
            wheres.append(like_any)
        df = df.where(reduce(op_and, wheres))

    # score
    s = _score_contains(df, ["t.tname", "t.tcity", "u.tname", "u.tcity"], qtok)

    out = (df
           .select(
               col("g.game_id").cast("string").alias("game_id"),
               col("g.game_date").alias("game_date"),
               col("g.home_team_id").cast("string").alias("home_team_id"),
               col("g.away_team_id").cast("string").alias("away_team_id"),
               col("g.final_score_home").alias("final_score_home"),
               col("g.final_score_away").alias("final_score_away"),
               col("t.tname").alias("home_name"),
               col("u.tname").alias("away_name"),
               s
           )
           .withColumn("_entity", lit("game"))
           .orderBy(col("_score").desc_nulls_last(), col("game_date").desc_nulls_last())
           .limit(limit)
    )
    return out


# ----------------------------
# API principale
# ----------------------------
def run_query(spark: SparkSession, query: str, top_n: int = 10) -> Tuple[List[Dict], Dict]:
    """
    Ricerca globale stile Basketball Reference: player + team + game.
    """
    q = (query or "").strip()
    if not q:
        return [], {"query": q, "notes": "empty query"}

    p = _candidate_players(spark, q, limit=top_n)
    t = _candidate_teams(spark, q, limit=top_n)
    g = _candidate_games(spark, q, limit=top_n)

    from pyspark.sql.functions import concat_ws, coalesce as Fcoalesce

    p2 = (p
          .withColumn("_id", col("player_id"))
          .withColumn("_title", col("full_name"))
          .withColumn("_subtitle", col("position"))
          .withColumn("_extra", lit(None).cast("string"))
          .select("_entity", "_score", "_id", "_title", "_subtitle", "_extra")
    )

    t2 = (t
          .withColumn("_id", col("team_id"))
          .withColumn("_title", col("team_name"))
          .withColumn("_subtitle", col("city"))
          .withColumn("_extra", lit(None).cast("string"))
          .select("_entity", "_score", "_id", "_title", "_subtitle", "_extra")
    )

    g2 = (g
          .withColumn("_id", col("game_id"))
          .withColumn("_title", concat_ws(" vs ", Fcoalesce(col("home_name"), col("home_team_id")), Fcoalesce(col("away_name"), col("away_team_id"))))
          .withColumn("_subtitle", col("game_date").cast("string"))
          .withColumn("_extra", concat_ws(" - ", col("final_score_home").cast("string"), col("final_score_away").cast("string")))
          .select("_entity", "_score", "_id", "_title", "_subtitle", "_extra")
    )

    allc = p2.unionByName(t2).unionByName(g2).orderBy(col("_score").desc_nulls_last())

    rows = [r.asDict(recursive=True) for r in allc.collect()]
    meta = {
        "query": q,
        "returned": len(rows),
        "hints": "Risultati combinati da player/team/game, ordinati per rilevanza.",
    }
    return rows, meta
