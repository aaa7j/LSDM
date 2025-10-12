# src/search/engine.py
# -----------------------------------------------------------------------------
# Motore di ricerca con typo-tolerance e ottimizzazioni di performance.
# - Normalizzazione robusta *_n (lower/trim/alfanumerico)
# - Fuzzy "guardato": Levenshtein solo su candidati (prefix/contains || guardie)
# - Niente window per dedup: merge lato driver
# - Top-K per entitÃ  + raccolta leggera
# API compatibile con codice legacy:
#   run_query(spark, q, top_n=..., limit=..., entity=...) -> (rows, meta)
# -----------------------------------------------------------------------------

from typing import List, Optional, Dict, Any, Tuple
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, lower, trim, concat_ws, coalesce as Fcoalesce, regexp_replace,
    when, least, length, substring, expr, monotonically_increasing_id
)
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType

__all__ = [
    "tokenize",
    "looks_like_person_query",
    "candidate_players",
    "candidate_teams",
    "candidate_games",
    "fuzzy_players_suggest",
    "suggest_all",
    "run_query",
]

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------

_WORD_RE = re.compile(r"[a-z0-9]+")

def tokenize(q: str) -> List[str]:
    if not q:
        return []
    q = q.lower()
    return _WORD_RE.findall(q)

def looks_like_person_query(tokens: List[str]) -> bool:
    alpha = [t for t in tokens if not t.isdigit()]
    return len(alpha) >= 2

def _pick(df: DataFrame, *cands: str):
    for c in cands:
        if c in df.columns:
            return col(c)
    return lit(None).cast(StringType())

def _norm_col(c):
    # lower + trim + rimozione non alfanumerico (ASCII)
    return regexp_replace(lower(trim(c)), r"[^a-z0-9 ]", "")

# -----------------------------------------------------------------------------
# Normalizzazioni
# -----------------------------------------------------------------------------

def _ensure_player_name_columns(df: DataFrame) -> DataFrame:
    fn = _pick(df, "first_name", "FIRST_NAME")
    ln = _pick(df, "last_name",  "LAST_NAME")
    fl = _pick(df, "full_name",  "FULL_NAME", "display_first_last", "DISPLAY_FIRST_LAST")

    df = df.withColumn("first_name", fn) \
           .withColumn("last_name", ln) \
           .withColumn("full_name", Fcoalesce(fl, concat_ws(" ", fn, ln)))

    df = (df
          .withColumn("first_name_n", _norm_col(col("first_name")))
          .withColumn("last_name_n",  _norm_col(col("last_name")))
          .withColumn("full_name_n",  _norm_col(col("full_name")))
          )
    return df

def _ensure_team_name_columns(df: DataFrame) -> DataFrame:
    tn = _pick(df, "team_name", "TEAM_NAME")
    city = _pick(df, "team_city", "TEAM_CITY")
    # Force a consistent full name: City + Team name (avoid dataset variants like "Name (City)")
    df = (df
          .withColumn("team_name", tn)
          .withColumn("team_city", city)
          .withColumn("full_name", concat_ws(" ", city, tn))
          )

    df = (df
          .withColumn("team_name_n", _norm_col(col("team_name")))
          .withColumn("team_city_n",  _norm_col(col("team_city")))
          .withColumn("full_name_n",  _norm_col(col("full_name")))
          )
    return df

# -----------------------------------------------------------------------------
# Filtri & scoring
# -----------------------------------------------------------------------------

def _mk_like_any_for_token(cols: List[str], token: str):
    # OR di contains; le colonne devono essere *_n
    if not token:
        return None
    cond = None
    for c in cols:
        cnd = col(c).contains(lit(token))
        cond = cnd if cond is None else (cond | cnd)
    return cond

def _guarded_min_levenshtein(fuzzy_cols: List[str], token: str, max_edits: int) -> F.Column:
    """
    Calcola min(levenshtein(col, token)) ma SOLO quando:
      - stessa prima lettera (o col contiene token)  **oppure**
      - |len(col) - len(token)| <= 2
    Altrimenti usa un valore alto cosÃ¬ non passa il threshold.
    """
    guards = []
    for fc in fuzzy_cols:
        same_first = (substring(col(fc), 1, 1) == lit(token[0])) if token else lit(False)
        len_band   = (F.abs(length(col(fc)) - lit(len(token))) <= lit(2))
        contains   = col(fc).contains(lit(token))
        guard = (same_first | len_band | contains)
        # se guard true -> calcolo LV; else -> 999
        lv = F.when(guard, F.levenshtein(col(fc), lit(token))).otherwise(lit(999))
        guards.append(lv)

    # min tra le colonne
    lv_min = guards[0]
    for g in guards[1:]:
        lv_min = least(lv_min, g)
    return lv_min

def _all_tokens_condition_fuzzy(
    cols: List[str],
    tokens: List[str],
    fuzzy_cols: Optional[List[str]] = None,
    max_edits: int = 2
):
    """
    (contains su almeno una col) OR (guarded levenshtein <= max_edits) per ogni token, con AND tra i token.
    """
    if not tokens:
        return None
    fuzzy_cols = fuzzy_cols or cols
    overall = None
    for t in tokens:
        like_any = _mk_like_any_for_token(cols, t)
        lv_min   = _guarded_min_levenshtein(fuzzy_cols, t, max_edits)
        fuzzy_ok = (lv_min <= lit(max_edits))
        this_tok = like_any | fuzzy_ok if like_any is not None else fuzzy_ok
        overall  = this_tok if overall is None else (overall & this_tok)
    return overall

def _score_contains(df: DataFrame, tokens: List[str], full_col: str, last_col: str) -> DataFrame:
    if not tokens:
        return df.withColumn("_score", lit(0.0))

    fullq = " ".join(tokens)
    last_tok = tokens[-1]

    base = df
    base = base.withColumn("_s_prefix_full",
                           when(col(full_col).startswith(lit(fullq)), lit(40.0)).otherwise(lit(0.0)))
    base = base.withColumn("_s_last_prefix",
                           when(col(last_col).startswith(lit(last_tok)), lit(25.0)).otherwise(lit(0.0)))

    tok_scores = None
    for t in tokens:
        sc = when(col(full_col).contains(lit(t)), lit(10.0)).otherwise(lit(0.0))
        tok_scores = sc if tok_scores is None else (tok_scores + sc)
    base = base.withColumn("_s_tok", tok_scores if tok_scores is not None else lit(0.0))

    lv = F.levenshtein(col(full_col), lit(fullq)).cast(IntegerType())
    # bonus inverso LV, ma solo quando differenza di lunghezza non Ã¨ enorme
    inv = F.when(F.abs(length(col(full_col)) - lit(len(fullq))) <= lit(3),
                 F.greatest(lit(0.0), lit(25.0) - lv.cast(FloatType()))
                 ).otherwise(lit(0.0))
    base = base.withColumn("_s_fuzzy_inv", inv)

    base = base.withColumn("_score", col("_s_prefix_full") + col("_s_last_prefix") + col("_s_tok") + col("_s_fuzzy_inv"))
    return base

# -----------------------------------------------------------------------------
# Players
# -----------------------------------------------------------------------------

def candidate_players(spark: SparkSession, q: str, limit: int = 10) -> DataFrame:
    tokens = tokenize(q)
    df = spark.table("GLOBAL_PLAYER")
    df = _ensure_player_name_columns(df)

    # SOLO le colonne che servono fino al sort (riduce I/O)
    df = df.select(
        (col("PLAYER_ID") if "PLAYER_ID" in df.columns else col("player_id")).alias("PLAYER_ID"),
        "first_name", "last_name", "full_name",
        "first_name_n", "last_name_n", "full_name_n"
    )

    cols = ["full_name_n", "first_name_n", "last_name_n"]
    cond = _all_tokens_condition_fuzzy(cols, tokens, fuzzy_cols=cols, max_edits=2)
    if cond is not None:
        df = df.where(cond)

    # Fallback full-name aggiuntivo ma *guardato*
    if looks_like_person_query(tokens):
        fullq = " ".join(tokens)
        # usa le stesse guardie per evitare LV pieno
        same_first = (substring(col("full_name_n"), 1, 1) == lit(fullq[0])) if fullq else lit(False)
        len_band   = (F.abs(length(col("full_name_n")) - lit(len(fullq))) <= lit(2))
        cond_full  = (F.when(same_first | len_band | col("full_name_n").contains(lit(fullq)),
                             F.levenshtein(col("full_name_n"), lit(fullq))
                             ).otherwise(lit(999)) <= lit(2))
        df = df.where(cond_full | lit(True))

    df = _score_contains(df, tokens, full_col="full_name_n", last_col="last_name_n")

    # Evita sort globale pesante: riduci partizioni prima
    df = df.repartition(8)
    out = (df
           .orderBy(col("_score").desc(), col("last_name_n").asc(), col("first_name_n").asc())
           .select(
               col("PLAYER_ID").alias("_id"),
               F.lit("player").alias("_entity"),
               col("full_name").alias("title"),
               col("_score")
           )
           .limit(limit)
           )
    return out

# -----------------------------------------------------------------------------
# Teams
# -----------------------------------------------------------------------------

def candidate_teams(spark: SparkSession, q: str, limit: int = 10) -> DataFrame:
    tokens = tokenize(q)
    has_global = any(t.name.upper() == "GLOBAL_TEAM" for t in spark.catalog.listTables())
    df = spark.table("GLOBAL_TEAM") if has_global else spark.table("TEAM")
    df = _ensure_team_name_columns(df)

    df = df.select(
        (col("TEAM_ID") if "TEAM_ID" in df.columns else col("team_id")).alias("TEAM_ID"),
        "team_name", "team_city", "full_name",
        "team_name_n", "team_city_n", "full_name_n"
    )

    cols = ["full_name_n", "team_name_n", "team_city_n"]
    cond = _all_tokens_condition_fuzzy(cols, tokens, fuzzy_cols=cols, max_edits=2)
    if cond is not None:
        df = df.where(cond)

    df = _score_contains(df, tokens, full_col="full_name_n", last_col="team_name_n")

    df = df.repartition(8)
    out = (df
           .orderBy(col("_score").desc(), col("full_name_n").asc())
           .select(
               col("TEAM_ID").alias("_id"),
               F.lit("team").alias("_entity"),
               col("full_name").alias("title"),
               col("_score")
           )
           .limit(limit)
           )
    return out

# -----------------------------------------------------------------------------
# Games
# -----------------------------------------------------------------------------

def candidate_games(spark: SparkSession, q: str, limit: int = 10) -> DataFrame:
    """Return game candidates filtered by team names/cities and dates.

    Joins GLOBAL_TEAM to resolve labels; builds title "Home vs Away - yyyy-mm-dd".
    """
    from pyspark.sql.functions import date_format, coalesce as Fcoalesce

    tokens = tokenize(q)
    if not any(t.name.upper() == "GLOBAL_GAME" for t in spark.catalog.listTables()):
        return spark.createDataFrame([], schema="""_id string, _entity string, title string, _score double""")

    g = spark.table("GLOBAL_GAME").alias("g")
    t = spark.table("GLOBAL_TEAM").select(
        col("team_id").alias("tid"),
        Fcoalesce(col("team_name"), lit("")).alias("tname"),
        Fcoalesce(col("city"), lit("")).alias("tcity"),
    )
    u = t.alias("u")

    df = (g
          .join(t, col("g.home_team_id") == col("t.tid"), "left")
          .join(u, col("g.away_team_id") == col("u.tid"), "left"))

    # Date filter if present in query
    date_txt = " ".join(tokens)
    ymd = re.findall(r"\b(20\d{2}|19\d{2})(?:[-/](\d{1,2})(?:[-/](\d{1,2}))?)?\b", date_txt)
    if ymd:
        y, m, d = ymd[0][0], (ymd[0][1] or ""), (ymd[0][2] or "")
        if d:
            df = df.where(date_format(col("g.game_date"), "yyyy-MM-dd") == f"{int(y):04d}-{int(m):02d}-{int(d):02d}")
        elif m:
            df = df.where(date_format(col("g.game_date"), "yyyy-MM") == f"{int(y):04d}-{int(m):02d}")
        else:
            df = df.where(date_format(col("g.game_date"), "yyyy") == f"{int(y):04d}")

    # Text tokens against team names/cities (drop matchup stopwords)
    stop = {"vs", "v", "at"}
    text_tokens = [t for t in tokens if (not t.isdigit()) and (t not in stop)]
    if text_tokens:
        wh = None
        for tk in text_tokens:
            c = (
                lower(col("t.tname")).contains(lit(tk)) |
                lower(col("t.tcity")).contains(lit(tk)) |
                lower(col("u.tname")).contains(lit(tk)) |
                lower(col("u.tcity")).contains(lit(tk))
            )
            wh = c if wh is None else (wh | c)
        if wh is not None:
            df = df.where(wh)

    tmp = (df
           .withColumn("home_label", Fcoalesce(col("t.tname"), col("g.home_team_id")))
           .withColumn("away_label", Fcoalesce(col("u.tname"), col("g.away_team_id")))
    )
    tmp = (tmp
           .withColumn("home_label_n", _norm_col(col("home_label")))
           .withColumn("away_label_n", _norm_col(col("away_label")))
           .withColumn("full_label_n", concat_ws(" ", col("home_label_n"), col("away_label_n")))
    )
    tmp = _score_contains(tmp, tokens, full_col="full_label_n", last_col="away_label_n")

    title = concat_ws(" - ", concat_ws(" vs ", col("home_label"), col("away_label")), col("g.game_date").cast("string"))

    out = (tmp
           .orderBy(col("_score").desc(), col("g.game_date").desc())
           .select(
               col("g.game_id").cast("string").alias("_id"),
               F.lit("game").alias("_entity"),
               title.alias("title"),
               col("_score")
           )
           .limit(limit)
    )
    return out

# -----------------------------------------------------------------------------
# Fallback fuzzy aggressivo (players) â€” ancora utile ma con guardie
# -----------------------------------------------------------------------------

def fuzzy_players_suggest(spark: SparkSession, q: str, limit: int = 10) -> DataFrame:
    tokens = tokenize(q)
    if not tokens:
        return spark.createDataFrame([], schema="""_id string, _entity string, title string, _score double""")

    fullq = " ".join(tokens)
    if not fullq:
        return spark.createDataFrame([], schema="""_id string, _entity string, title string, _score double""")

    df = spark.table("GLOBAL_PLAYER")
    df = _ensure_player_name_columns(df)
    df = df.select(
        (col("PLAYER_ID") if "PLAYER_ID" in df.columns else col("player_id")).alias("PLAYER_ID"),
        "full_name", "full_name_n"
    )

    same_first = (substring(col("full_name_n"), 1, 1) == lit(fullq[0]))
    len_band   = (F.abs(length(col("full_name_n")) - lit(len(fullq))) <= lit(2))
    guard      = same_first | len_band | col("full_name_n").contains(lit(fullq))
    lv_full    = F.when(guard, F.levenshtein(col("full_name_n"), lit(fullq))).otherwise(lit(999))

    df = (df
          .withColumn("_lv_full", lv_full)
          .where(col("_lv_full") <= lit(2))
          )

    sc = (lit(100.0) - col("_lv_full").cast(FloatType()) * lit(10.0)) + \
         when(col("full_name_n").startswith(lit(fullq)), lit(20.0)).otherwise(lit(0.0))
    df = df.withColumn("_score", sc)

    out = (df
           .orderBy(col("_score").desc())
           .select(
               col("PLAYER_ID").alias("_id"),
               F.lit("player").alias("_entity"),
               col("full_name").alias("title"),
               col("_score")
           )
           .limit(limit)
           )
    return out

# -----------------------------------------------------------------------------
# Raccolta e merge lato driver (niente window/shuffle)
# -----------------------------------------------------------------------------

def _collect_df_rows(df: DataFrame) -> List[Dict[str, Any]]:
    return [
        {
            "_entity": r["_entity"],
            "_id": str(r["_id"]),
            "title": r["title"],
            "_score": float(r["_score"]) if r["_score"] is not None else 0.0,
        }
        for r in df.collect()
    ]

def suggest_all(spark: SparkSession, q: str, limit: int = 10) -> List[Dict[str, Any]]:
    tokens = tokenize(q)

    # prendi pochi risultati per entitÃ  e unisci lato driver
    out: List[Dict[str, Any]] = []
    try:
        out += _collect_df_rows(candidate_players(spark, q, limit=limit))
    except Exception:
        pass

    # per alleggerire, mettiamo limiti minori agli altri
    try:
        out += _collect_df_rows(candidate_teams(spark, q, limit=max(5, limit // 3)))
    except Exception:
        pass
    try:
        out += _collect_df_rows(candidate_games(spark, q, limit=max(5, limit // 3)))
    except Exception:
        pass

    if looks_like_person_query(tokens):
        try:
            out += _collect_df_rows(fuzzy_players_suggest(spark, q, limit=max(5, limit // 2)))
        except Exception:
            pass

    # dedup lato driver tenendo lo score migliore
    best: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for it in out:
        k = (it["_entity"], it["_id"])
        if k not in best or it["_score"] > best[k]["_score"]:
            best[k] = it

    ordered = sorted(best.values(), key=lambda x: (-(x.get("_score") or 0), x["title"]))
    return ordered[:limit]

# -----------------------------------------------------------------------------
# API backward-compatible
# -----------------------------------------------------------------------------

def run_query(
    spark: SparkSession,
    q: str,
    top_n: Optional[int] = None,  # alias legacy
    limit: Optional[int] = None,  # alias nuovo
    entity: Optional[str] = None, # "player"|"team"|"game"|None
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Ritorna (rows, meta) ed accetta sia 'top_n' che 'limit' (precedenza a top_n).
    """
    q2 = (q or "").strip()
    n = top_n if (top_n is not None) else (limit if limit is not None else 20)
    ent = (entity or "").lower().strip() if entity else None

    if not q2:
        return [], {"query": q2, "entity": ent, "limit": n, "count": 0}

    try:
        if ent == "player":
            rows = _collect_df_rows(candidate_players(spark, q2, limit=n))
        elif ent == "team":
            rows = _collect_df_rows(candidate_teams(spark, q2, limit=n))
        elif ent == "game":
            rows = _collect_df_rows(candidate_games(spark, q2, limit=n))
        else:
            rows = suggest_all(spark, q2, limit=n)
    except Exception:
        rows = []

    meta = {"query": q2, "entity": ent, "limit": n, "count": len(rows)}
    return rows, meta


