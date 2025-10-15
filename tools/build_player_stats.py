# tools/build_player_stats.py — FIX JOIN MOLTIPLICATIVI
# - game_u: una sola riga per game_id
# - base_game: una sola riga per (game_id, player_id) (team_id = first)
# - per_season: aggrega da chiave unica per evitare duplicazioni

import argparse, os, sys
from pyspark.sql import SparkSession, functions as F, Window

def die(msg, code=2):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(code)

def safe_div(numer, denom):
    numer = numer.cast("double"); denom = denom.cast("double")
    return F.when(denom.isNull() | (denom == 0), F.lit(None)).otherwise(numer / denom)

parser = argparse.ArgumentParser(description="Build aggregated player stats with safe joins")
parser.add_argument("--warehouse", required=True, help="Path to warehouse folder")
args = parser.parse_args()

WAREHOUSE = os.path.abspath(args.warehouse)
LOCAL_TMP = r"C:\spark-tmp"

print("WD:", os.getcwd())
print("Warehouse path:", args.warehouse)
print("Exists?", os.path.exists(WAREHOUSE))
os.makedirs(LOCAL_TMP, exist_ok=True)

spark = (
    SparkSession.builder.appName("BuildPlayerStats-FixedJoins")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.local.dir", "C:/spark-tmp")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# ---------- GAME → season fields (con season_type euristico) ----------
game_raw = spark.read.parquet(os.path.join(WAREHOUSE, "global_game"))
# Canonicalize game_id in the source game table to avoid duplicates caused by
# whitespace / formatting inconsistencies.
from pyspark.sql import functions as _F
if 'game_id' in game_raw.columns:
    game_raw = game_raw.withColumn('game_id', _F.trim(_F.col('game_id').cast('string')))

gdate = F.coalesce(
    F.to_timestamp(F.col("game_date")),
    F.to_timestamp(F.col("game_date").cast("string")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "yyyy-MM-dd").cast("timestamp")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "yyyyMMdd").cast("timestamp")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "MM/dd/yyyy").cast("timestamp")),
)

base_game = (
    game_raw.withColumn("gdate", gdate)
            .withColumn("year",  F.year("gdate"))
            .withColumn("month", F.month("gdate"))
            .withColumn("season_start", F.when(F.col("month") >= 8, F.col("year")).otherwise(F.col("year") - 1))
            .withColumn(
                "season",
                F.concat_ws("-", F.col("season_start").cast("string"),
                            F.format_string("%02d", (F.col("season_start")+1) % 100))
            )
            .select(
                F.trim(F.col("game_id").cast("string")).alias("game_id"),
                "gdate","season","season_start",
                F.col("home_team_id").cast("int").alias("home_team_id"),
                F.col("away_team_id").cast("int").alias("away_team_id"),
            )
            .dropDuplicates(["game_id"])
)

# 1) Se troviamo un campo nativo di tipo stagione in un parquet, usiamolo
season_type_from = None
for name in ["global_game", "global_other_stats", "global_line_score"]:
    p = os.path.join(WAREHOUSE, name)
    if os.path.isdir(p):
        df_tmp = spark.read.parquet(p)
        # prova colonne comuni
        cand = next((c for c in df_tmp.columns
                     if c.lower() in ("season_type","seasoncategory","game_type","is_playoffs")), None)
        if cand:
            season_type_from = (df_tmp
                .select(
                    F.col("game_id").cast("string").alias("game_id"),
                    F.when(F.lower(F.col(cand)).isin("playoffs","postseason","po","p","1", "true"), "Playoffs")
                     .when(F.lower(F.col(cand)).isin("regular season","regular","rs","r","0", "false"), "Regular Season")
                     .otherwise(F.lit(None)).alias("season_type"))
                .dropDuplicates(["game_id"]))
            break

if season_type_from is not None:
    game = (base_game.join(season_type_from, "game_id", "left")
                    .withColumn(
                        "season_type",
                        F.when(F.col("season_type").isNull(), F.lit("Regular Season")).otherwise(F.col("season_type"))
                    ))
else:
    # 2) Fallback euristico ROBUSTO (finestra più stretta e pausa maggiore)
    # Parametri (modificabili anche via env):
    # We change RS detection to: window from mid-September to mid-April.
    THR_FACTOR = float(os.environ.get("RS_THR_FACTOR", "0.65"))  # threshold factor for "full day"
    THR_MIN    = int(os.environ.get("RS_THR_MIN", "9"))          # minimal full-day games
    THR_MAX    = int(os.environ.get("RS_THR_MAX", "14"))         # maximal threshold
    GAP_MIN    = int(os.environ.get("RS_GAP_MIN", "2"))          # require >= 2 days pause after last regular day
    # RS start: mid-September (15 Sep)
    RS_START_MONTH = int(os.environ.get("RS_START_MONTH", "9"))
    RS_START_DAY   = int(os.environ.get("RS_START_DAY", "15"))
    # RS last candidate window: up to mid-April (15 Apr). Additionally require
    # the selected last day to be at least April 7 (to avoid very early season ends).
    RS_END_MONTH = int(os.environ.get("RS_END_MONTH", "4"))
    RS_END_DAY   = int(os.environ.get("RS_END_DAY", "15"))
    RS_LAST_MIN_MONTH = int(os.environ.get("RS_LAST_MIN_MONTH", "4"))
    RS_LAST_MIN_DAY   = int(os.environ.get("RS_LAST_MIN_DAY", "7"))

    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # giorni per stagione
    daily = (base_game
             .groupBy("season_start", F.to_date("gdate").alias("gday"))
             .agg(F.count(F.lit(1)).alias("games_in_day")))

    # per-stagione: massimo partite in un giorno (per soglia dinamica)
    per_season_max = (daily
                      .groupBy("season_start")
                      .agg(F.max("games_in_day").alias("max_gpd"),
                           F.min("gday").alias("min_gday")))

    daily2 = (daily.join(per_season_max, "season_start")
                   .withColumn("thr_raw", (F.col("max_gpd") * F.lit(THR_FACTOR)).cast("int"))
                   .withColumn("thr", F.greatest(F.lit(THR_MIN), F.least(F.lit(THR_MAX), F.col("thr_raw")))))

    # gap con il giorno successivo
    w = Window.partitionBy("season_start").orderBy(F.col("gday").asc())
    daily2 = daily2.withColumn("next_gday", F.lead("gday", 1).over(w))
    daily2 = daily2.withColumn("gap_days", F.datediff(F.col("next_gday"), F.col("gday")))

    # helper per vincoli su data (replaced by RS start/end window logic below)

    # candidati: giornata piena, tra metà settembre (start) e metà aprile (end),
    # e pausa >= GAP_MIN dopo la giornata.
    # Note: gday for the season end is in year (season_start + 1)
    # Filter candidate days to months March/April and early-April window, then
    # require gap_days >= GAP_MIN. We'll pick the max candidate per season and
    # then require it to be at least April 7 (RS_LAST_MIN_DAY).
    in_end_window = (
        (F.month(F.col("gday")).between(3, RS_END_MONTH)) &
        (
            (F.month(F.col("gday")) < RS_END_MONTH) |
            ((F.month(F.col("gday")) == RS_END_MONTH) & (F.dayofmonth(F.col("gday")) <= RS_END_DAY))
        )
    )

    candidates = (daily2
                  .where(F.col("games_in_day") >= F.col("thr"))
                  .where(in_end_window)
                  .where(F.col("gap_days") >= F.lit(GAP_MIN)))

    rs_last_candidate = (candidates
               .groupBy("season_start")
               .agg(F.max("gday").alias("rs_last_day")))

    # enforce rs_last_day >= April 7 (year = season_start + 1)
    apr7 = F.to_date(F.concat_ws("-", (F.col("season_start") + 1).cast("string"), F.lit("04"), F.lit(str(RS_LAST_MIN_DAY))))
    rs_last = rs_last_candidate.where(F.col("rs_last_day") >= apr7)

    # Fallback: if for some season no candidate met the April 7 constraint,
    # pick the maximum game day in the season (best-effort) as rs_last.
    rs_last_fallback = (daily2.groupBy("season_start").agg(F.max("gday").alias("rs_last_day_fallback")))
    rs_last = rs_last.join(rs_last_fallback, "season_start", "right")
    rs_last = rs_last.withColumn("rs_last_day", F.coalesce(F.col("rs_last_day"), F.col("rs_last_day_fallback"))).select("season_start","rs_last_day")

    # RS start: max(15 settembre, primo giorno con partite) -> gestisce lockout/ritardi
    sept15 = F.to_date(F.concat_ws("-", F.col("season_start").cast("string"), F.lit("09"), F.lit(str(RS_START_DAY))))
    rs_start = (per_season_max
                .withColumn("sept15", sept15)
                .withColumn("rs_start_day", F.greatest(F.col("sept15"), F.col("min_gday")))
                .select("season_start", F.col("rs_start_day")))

    # etichetta RS/PO
    game = (base_game
            .join(rs_last, "season_start", "left")
            .join(rs_start, "season_start", "left")
            .withColumn(
                "season_type",
                F.when(F.col("gdate") > F.col("rs_last_day").cast("timestamp"), "Playoffs")
                 .otherwise("Regular Season")
            )
            # (opzionale) tagliare fuori giochi fuori finestra RS "forte":
            # .where((F.col("gdate") >= F.col("rs_start_day").cast("timestamp")) &
            #        (F.col("gdate") <= F.col("rs_last_day").cast("timestamp")))
            .drop("rs_last_day", "rs_start_day", "max_gpd", "min_gday", "thr_raw", "thr", "next_gday", "gap_days"))

# colonne finali usate dal resto della pipeline
game = game.select("game_id","season","season_start","season_type","home_team_id","away_team_id")

game_u = game.dropDuplicates(["game_id"])  # una riga per game_id
# ===================== SOURCING: BOX SCORE O PBP =====================
def has(path): return os.path.exists(os.path.join(WAREHOUSE, path))
use_box = has("global_other_stats")

if use_box:
    box = spark.read.parquet(os.path.join(WAREHOUSE, "global_other_stats"))
    # mappatura colonne
    def pick(df, *cands):
        for c in cands:
            if c in df.columns: return c
        return None

    colmap = {
        "player_id": pick(box, "player_id","PLAYER_ID"),
        "team_id":   pick(box, "team_id","TEAM_ID"),
        "game_id":   pick(box, "game_id","GAME_ID"),
        "fgm":       pick(box, "fgm","FGM","field_goals_made"),
        "fga":       pick(box, "fga","FGA","field_goals_attempted"),
        "tpm":       pick(box, "tpm","TPM","FG3M","three_pointers_made","t3m"),
        "tpa":       pick(box, "tpa","TPA","FG3A","three_pointers_attempted","t3a"),
        "ftm":       pick(box, "ftm","FTM","free_throws_made"),
        "fta":       pick(box, "fta","FTA","free_throws_attempted"),
        "trb":       pick(box, "reb","REB","trb","TRB","rebound","tot_reb","REB_TOTAL"),
        "ast":       pick(box, "ast","AST","assists","ASSISTS"),
        "pts":       pick(box, "pts","PTS","points","POINTS"),
    }
    needed = ["player_id","team_id","game_id","pts","trb","ast","fgm","fga","tpm","tpa","ftm","fta"]
    if any(colmap[k] is None for k in needed):
        print("[INFO] global_other_stats non è box per-giocatore. Uso fallback PBP.")
        use_box = False

if use_box:
    print("[INFO] Using BOX SCORE global_other_stats")
    df = box
    # escludi eventuali "team totals" e id nulli/0
    maybe_name = "player_name" if "player_name" in df.columns else ("PLAYER_NAME" if "PLAYER_NAME" in df.columns else None)
    if maybe_name:
        df = df.filter(F.lower(F.col(maybe_name)) != F.lit("team totals"))
    df = df.filter(F.col(colmap["player_id"]).isNotNull() & (F.col(colmap["player_id"]) != 0))

    per_row = df.select(
        F.trim(F.col(colmap["game_id"]).cast("string")).alias("game_id"),
        F.col(colmap["player_id"]).cast("int").alias("player_id"),
        F.col(colmap["team_id"]).cast("int").alias("team_id"),
        F.col(colmap["fgm"]).cast("long").alias("fgm"),
        F.col(colmap["fga"]).cast("long").alias("fga"),
        F.col(colmap["tpm"]).cast("long").alias("tpm"),
        F.col(colmap["tpa"]).cast("long").alias("tpa"),
        F.col(colmap["ftm"]).cast("long").alias("ftm"),
        F.col(colmap["fta"]).cast("long").alias("fta"),
        F.col(colmap["trb"]).cast("long").alias("trb"),
        F.col(colmap["ast"]).cast("long").alias("ast"),
        F.col(colmap["pts"]).cast("long").alias("pts"),
    )

    # ✅ UNA RIGA per (game, player): se il box è per quarto/tempo, compattiamo
    base_game = (
        per_row.groupBy("game_id","player_id")
        .agg(
            F.first("team_id").alias("team_id"),  # in una partita non cambia
            F.sum("fgm").alias("fgm"), F.sum("fga").alias("fga"),
            F.sum("tpm").alias("tpm"), F.sum("tpa").alias("tpa"),
            F.sum("ftm").alias("ftm"), F.sum("fta").alias("fta"),
            F.sum("trb").alias("trb"), F.sum("ast").alias("ast"),
            F.sum("pts").alias("pts"),
        )
    )

else:
    # ===================== FALLBACK PBP =====================
    ppath = os.path.join(WAREHOUSE, "global_play_by_play")
    if not os.path.exists(ppath):
        die("Missing global_play_by_play and no box score available")

    print("[INFO] Using PLAY-BY-PLAY fallback")
    pbp = spark.read.parquet(ppath).filter(F.col("eventmsgtype").isin(1,2,3,4))  # tiri/liberi/rimbalzi
    # Normalize game_id in PBP as well
    if 'game_id' in pbp.columns:
        pbp = pbp.withColumn('game_id', F.trim(F.col('game_id').cast('string')))

    # dedup robusto: (game_id, period, eventnum) se disponibile
    dedup_keys = [c for c in ["game_id","period","eventnum"] if c in pbp.columns]
    if dedup_keys:
        pbp = pbp.dropDuplicates(dedup_keys)

    evt  = F.col("eventmsgtype")
    p1   = F.col("player1_id")
    p2   = F.col("player2_id")
    t1   = F.col("player1_team_id")
    desc = F.coalesce("homedescription","visitordescription","neutraldescription")
    dl   = F.lower(desc)

    # chiave evento per countDistinct (una per riga PBP reale)
    # Use a deterministic event key: prefer eventnum when present, otherwise
    # build a stable hash from period + event descriptions + player ids. Avoid
    # using monotonically_increasing_id() because it's non-deterministic
    # between runs/partitions and can prevent proper deduplication.
    desc_col = F.coalesce(F.col("homedescription"), F.col("visitordescription"), F.col("neutraldescription"), F.lit(""))
    evk = F.md5(F.concat_ws("|",
                           F.coalesce(F.col("period").cast("string"), F.lit("0")),
                           F.coalesce(F.col("eventnum").cast("string"), F.lit("")),
                           F.coalesce(desc_col.cast("string"), F.lit("")),
                           F.coalesce(F.col("player1_id").cast("string"), F.lit("")),
                           F.coalesce(F.col("player2_id").cast("string"), F.lit(""))
                          ))

    pbp2 = (pbp
        .select(
            F.col("game_id").cast("string").alias("game_id"),
            p1.cast("int").alias("player1_id"),
            p2.cast("int").alias("player2_id"),
            t1.cast("int").alias("team1_id"),
            evt.cast("int").alias("t"),
            evk.alias("evk"),
            dl.alias("dl"))
    )

    # Robust dedup: drop duplicates on (game_id, evk, player1_id, player2_id)
    # but always include (game_id, evk) as primary keys for deduplication.
    dedup_cols = [c for c in ["game_id","evk","player1_id","player2_id"] if c in pbp2.columns]
    if 'game_id' in pbp2.columns and 'evk' in pbp2.columns:
        base_dedup = ['game_id','evk']
        # extend with player ids when available
        if 'player1_id' in pbp2.columns and 'player2_id' in pbp2.columns:
            base_dedup += ['player1_id','player2_id']
        pbp2 = pbp2.dropDuplicates(base_dedup)

    is_make  = (F.col("t") == 1)
    is_miss  = (F.col("t") == 2)
    is_ft    = (F.col("t") == 3)
    is_three = F.col("dl").contains("3pt") | F.col("dl").contains("3-pt") | F.col("dl").contains("three")
    is_ft_made = is_ft & (~F.col("dl").contains("miss"))

    shooter = pbp2.select(
        "game_id",
        F.col("player1_id").alias("player_id"),
        F.col("team1_id").alias("team_id"),
        "t","evk","dl"
    )

    # ✅ UNA RIGA per (game, player): aggrego su countDistinct(evk) e prendo FIRST(team_id)
    agg = (
        shooter.groupBy("game_id","player_id")
        .agg(
            F.first("team_id").alias("team_id"),
            F.countDistinct(F.when(is_make, F.col("evk"))).alias("fgm"),
            F.countDistinct(F.when(is_make | is_miss, F.col("evk"))).alias("fga"),
            F.countDistinct(F.when(is_make & is_three, F.col("evk"))).alias("tpm"),
            F.countDistinct(F.when((is_make | is_miss) & is_three, F.col("evk"))).alias("tpa"),
            F.countDistinct(F.when(is_ft_made, F.col("evk"))).alias("ftm"),
            F.countDistinct(F.when(is_ft, F.col("evk"))).alias("fta"),
        )
    )

    assist = (
        pbp2.where(is_make & F.col("player2_id").isNotNull())
            .groupBy("game_id", F.col("player2_id").alias("player_id"))
            .agg(F.countDistinct("evk").alias("ast"))
    )

    rebs = (
        pbp2.where((F.col("t")==4) & F.col("player1_id").isNotNull())
            .groupBy("game_id", F.col("player1_id").alias("player_id"))
            .agg(F.countDistinct("evk").alias("trb"))
    )

    base_game = (
        agg.join(assist, ["game_id","player_id"], "left")
           .join(rebs,   ["game_id","player_id"], "left")
           .fillna({"ast":0, "trb":0})
    ).withColumn(
        "pts", F.col("tpm")*3 + (F.col("fgm")-F.col("tpm"))*2 + F.col("ftm")
    )

# ✅ “Airbag” finale: garantisci univocità per (game, player)
base_game = (
    base_game
    .groupBy("game_id","player_id")
    .agg(
        F.first("team_id").alias("team_id"),
        F.sum("fgm").alias("fgm"),
        F.sum("fga").alias("fga"),
        F.sum("tpm").alias("tpm"),
        F.sum("tpa").alias("tpa"),
        F.sum("ftm").alias("ftm"),
        F.sum("fta").alias("fta"),
        F.sum("trb").alias("trb"),
        F.sum("ast").alias("ast"),
        F.sum("pts").alias("pts"),
    )
)
# ------------ DA QUI: per-game → per-season SCORING tab ------------

# 0) UNA SOLA RIGA per (game_id, player_id)
#    Anche se base_game ha team_id/dupliche, riduciamo a per-game/per-player.
per_game = (
    base_game
    .groupBy("game_id", "player_id")
    .agg(
        F.first("team_id", ignorenulls=True).alias("team_id"),
        F.sum("fgm").alias("fgm"),
        F.sum("fga").alias("fga"),
        F.sum("tpm").alias("tpm"),
        F.sum("tpa").alias("tpa"),
        F.sum("ftm").alias("ftm"),
        F.sum("fta").alias("fta"),
        F.sum("trb").alias("trb"),
        F.sum("ast").alias("ast"),
        F.sum("pts").alias("pts"),
    )
)

# 1) JOIN con una tabella game UNICA per game_id (no moltiplicazioni)
game_u = game.dropDuplicates(["game_id"])
per_game = per_game.join(game_u, "game_id", "left")

# Final safety: ensure exactly one row per (game_id, player_id) before
# aggregating to season. This prevents any remaining duplicates from
# inflating gp after joins.
if 'game_id' in per_game.columns and 'player_id' in per_game.columns:
    per_game = per_game.dropDuplicates(['game_id','player_id'])

# Defensive filtering: alcuni snapshot possono contenere righe "team totals" o
# righe con team_id al posto di player_id (es. 1610612749). Se è disponibile
# la tabella GLOBAL_PLAYER nel warehouse, usiamola per whitelistare player_id
# validi; altrimenti applichiamo una soglia empirica per escludere id troppo
# grandi che sono quasi certamente team ids.
players_path = os.path.join(WAREHOUSE, "global_player")
if os.path.exists(players_path):
    try:
        players_df = spark.read.parquet(players_path).select(F.col("player_id").cast("int").alias("player_id")).dropDuplicates(["player_id"])
        per_game = per_game.join(players_df, on="player_id", how="inner")
        print("[INFO] Filtered per_game using GLOBAL_PLAYER whitelist")
    except Exception:
        # fallback conservative filter
        per_game = per_game.where((F.col("player_id").isNotNull()) & (F.col("player_id") < F.lit(1000000)))
        print("[WARN] Could not read GLOBAL_PLAYER; applied numeric threshold filter on player_id")
else:
    per_game = per_game.where((F.col("player_id").isNotNull()) & (F.col("player_id") < F.lit(1000000)))
    print("[WARN] GLOBAL_PLAYER parquet not found; applied numeric threshold filter on player_id")

# 2) per-season SOLO sui campi necessari allo scoring
#    - gp = countDistinct(game_id)
#    - two_pm = fgm_total - tpm_total
per_season_scoring = (
    per_game
    .groupBy("player_id", "season_type", "season_start")
    .agg(
        F.countDistinct("game_id").alias("gp"),
        F.sum("pts").alias("pts"),
        F.sum("tpm").alias("three_pm"),
        (F.sum("fgm") - F.sum("tpm")).alias("two_pm"),
        F.sum("ftm").alias("ftm"),
    )
    .withColumn("ppg", F.round(F.col("pts") / F.col("gp"), 2))
)

# 3) scrivi la tabella/materializzata usata dalla pagina "Scoring by season"
out_scoring = os.path.join(WAREHOUSE, "global_player_season_scoring")
(per_season_scoring
    .coalesce(1)
    .write.mode("overwrite")
    .partitionBy("season_type")
    .parquet(out_scoring)
)
print("[OK] wrote", out_scoring)

# Diagnostic: salva le righe anomale (gp > 82) per ispezione
anoms_out = os.path.join(WAREHOUSE, "global_player_season_scoring_anomalies")
anoms = per_season_scoring.where(F.col("gp") > 82)
if anoms.count() > 0:
    anoms.coalesce(1).write.mode("overwrite").parquet(anoms_out)
    print("[WARN] wrote anomalies to", anoms_out)
else:
    print("[INFO] no anomalies (gp>82) found")

# ------------ FINE BLOCCO SCORING ------------

# (opzionale) se vuoi tenere anche le altre tabelle che già scrivevi, lasciale sotto

spark.stop(); print("[DONE]")
