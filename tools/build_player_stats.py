# tools/build_player_stats.py — preferisce BOX SCORE; fallback al PBP se manca

import argparse, os, sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

def die(msg, code=2):
    print(f"[FATAL] {msg}", file=sys.stderr); sys.exit(code)

def safe_div(numer, denom):
    numer = numer.cast("double")
    denom = denom.cast("double")
    return F.when(denom.isNull() | (denom == 0), F.lit(None).cast("double")).otherwise(numer / denom)

parser = argparse.ArgumentParser(description="Build aggregated player stats (prefer BOX SCORE, fallback PBP)")
parser.add_argument("--warehouse", required=True, help="Path to warehouse folder")
args = parser.parse_args()

WAREHOUSE = os.path.abspath(args.warehouse)
LOCAL_TMP = r"C:\spark-tmp"

print("WD:", os.getcwd())
print("Warehouse path:", args.warehouse)
print("Exists?", os.path.exists(WAREHOUSE))
os.makedirs(LOCAL_TMP, exist_ok=True)

# Spark
spark = (
    SparkSession.builder.appName("BuildPlayerStats")
    .config("spark.driver.memory", "6g")
    .config("spark.executor.memory", "6g")
    .config("spark.local.dir", "C:/spark-tmp")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.default.parallelism", "4")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

# ---------- GAME → season fields ----------
if not os.path.exists(os.path.join(WAREHOUSE, "global_game")):
    die(f"Missing global_game in {WAREHOUSE}")

game_raw = spark.read.parquet(os.path.join(WAREHOUSE, "global_game"))

gdate = F.coalesce(
    F.to_timestamp(F.col("game_date")),
    F.to_timestamp(F.col("game_date").cast("string")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "yyyy-MM-dd").cast("timestamp")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "yyyyMMdd").cast("timestamp")),
    F.to_timestamp(F.to_date(F.col("game_date").cast("string"), "MM/dd/yyyy").cast("timestamp")),
)

game = (
    game_raw.withColumn("gdate", gdate)
    .withColumn("year", F.year("gdate"))
    .withColumn("month", F.month("gdate"))
    .withColumn("season_start", F.when(F.col("month") >= 8, F.col("year")).otherwise(F.col("year") - 1))
    .withColumn(
        "season",
        F.concat_ws("-", F.col("season_start").cast("string"),
                    F.format_string("%02d", (F.col("season_start") + 1) % 100)),
    )
    .withColumn("season_type", F.lit("Regular Season"))  # adegua se hai un flag per i Playoffs
    .select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("season"),
        F.col("season_start"),
        F.col("season_type"),
        F.col("home_team_id").cast("int").alias("home_team_id"),
        F.col("away_team_id").cast("int").alias("away_team_id"),
    )
    .dropDuplicates(["game_id"])  # garantisce 1 riga per partita
)

# ---------- Prefer BOX SCORE (global_other_stats) ----------
def has(path): return os.path.exists(os.path.join(WAREHOUSE, path))

use_box = has("global_other_stats")
source = None

if use_box:
    source = "BOX"
    box = spark.read.parquet(os.path.join(WAREHOUSE, "global_other_stats"))

    # Proviamo a mappare i nomi tipici (adatta qui se i tuoi campi sono diversi)
    # cerchiamo colonne candidate
    colmap = {}
    def pick(*cands):
        for c in cands:
            if c in box.columns: return c
        return None

    colmap["player_id"] = pick("player_id","PLAYER_ID")
    colmap["team_id"]   = pick("team_id","TEAM_ID")
    colmap["game_id"]   = pick("game_id","GAME_ID")
    colmap["fgm"]       = pick("fgm","FGM","field_goals_made")
    colmap["fga"]       = pick("fga","FGA","field_goals_attempted")
    colmap["tpm"]       = pick("tpm","TPM","FG3M","three_pointers_made","t3m")
    colmap["tpa"]       = pick("tpa","TPA","FG3A","three_pointers_attempted","t3a")
    colmap["ftm"]       = pick("ftm","FTM","free_throws_made")
    colmap["fta"]       = pick("fta","FTA","free_throws_attempted")
    colmap["trb"]       = pick("reb","REB","trb","TRB","rebound","tot_reb","REB_TOTAL")
    colmap["ast"]       = pick("ast","AST","assists","ASSISTS")
    colmap["pts"]       = pick("pts","PTS","points","POINTS")
    # verifichiamo colonne minime
    needed = ["player_id","team_id","game_id","pts","trb","ast","fgm","fga","tpm","tpa","ftm","fta"]
    missing = [k for k in needed if not colmap.get(k)]
    if missing:
        dbg = os.environ.get("DEBUG_BOX", "0").lower() in ["1","true","yes"]
        if dbg:
            print(f"[INFO] global_other_stats columns missing for per-player box mapping: {missing} -> using PBP fallback")
        else:
            print("[INFO] global_other_stats is not a per-player box score. Using PBP fallback.")
        use_box = False

if use_box:
    print("[INFO] Using BOX SCORE global_other_stats")
    base_game = (
        box.select(
            F.col(colmap["game_id"]).cast("string").alias("game_id"),
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
        .groupBy("game_id","player_id","team_id")
        .agg(
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
else:
    # ---------- Fallback al PBP filtrato ----------
    if not os.path.exists(os.path.join(WAREHOUSE, "global_play_by_play")):
        die("Missing global_play_by_play and no box score available")

    print("[INFO] Using PLAY-BY-PLAY fallback")
    pbp = spark.read.parquet(os.path.join(WAREHOUSE, "global_play_by_play"))
    # Considera solo eventi box-relevant per evitare gonfiature
    pbp = pbp.filter(F.col("eventmsgtype").isin(1,2,3,4))

    # Dedup robusto: usa (game_id, period, eventnum) quando disponibili, altrimenti fallback
    dedup_keys = [c for c in ["game_id","period","eventnum","eventmsgtype","player1_id","player2_id"] if c in pbp.columns]
    if dedup_keys:
        pbp = pbp.dropDuplicates(dedup_keys)

    # Colonne base
    evt  = F.col("eventmsgtype")
    p1   = F.col("player1_id")
    p2   = F.col("player2_id")
    t1   = F.col("player1_team_id")
    desc = F.coalesce(F.col("homedescription"), F.col("visitordescription"), F.col("neutraldescription"))
    dl   = F.lower(desc)

    # Chiave evento unica per partita (gestisce duplicati del medesimo evento)
    evk = F.concat_ws("-",
                      F.coalesce(F.col("period").cast("string"), F.lit("0")),
                      F.coalesce(F.col("eventnum").cast("string"), F.monotonically_increasing_id().cast("string")))

    # Proiezione normalizzata
    pbp2 = (pbp
            .select(F.col("game_id").cast("string").alias("game_id"),
                    p1.cast("int").alias("player1_id"),
                    p2.cast("int").alias("player2_id"),
                    t1.cast("int").alias("team1_id"),
                    evt.cast("int").alias("t"),
                    evk.alias("evk"),
                    dl.alias("dl")))

    # Predicati
    is_make  = (F.col("t") == 1)
    is_miss  = (F.col("t") == 2)
    is_ft    = (F.col("t") == 3)
    is_three = F.col("dl").contains("3pt") | F.col("dl").contains("3-pt") | F.col("dl").contains("three")
    is_ft_made = is_ft & (~F.col("dl").contains("miss"))

    # Aggregati per-gioco per-giocatore con countDistinct sull'evento
    shooter = pbp2.select("game_id",
                          F.col("player1_id").alias("player_id"),
                          F.col("team1_id").alias("team_id"),
                          "t","evk","dl")

    base_game = (shooter.groupBy("game_id","player_id","team_id")
                 .agg(
                     F.countDistinct(F.when(is_make, F.col("evk"))).alias("fgm"),
                     F.countDistinct(F.when(is_make | is_miss, F.col("evk"))).alias("fga"),
                     F.countDistinct(F.when(is_make & is_three, F.col("evk"))).alias("tpm"),
                     F.countDistinct(F.when((is_make | is_miss) & is_three, F.col("evk"))).alias("tpa"),
                     F.countDistinct(F.when(is_ft_made, F.col("evk"))).alias("ftm"),
                     F.countDistinct(F.when(is_ft, F.col("evk"))).alias("fta"),
                 ))

    assist = (pbp2.where(is_make & F.col("player2_id").isNotNull())
                   .select("game_id", F.col("player2_id").alias("player_id"), "evk")
                   .groupBy("game_id","player_id").agg(F.countDistinct("evk").alias("ast")))

    rebs = (pbp2.where((F.col("t") == 4) & F.col("player1_id").isNotNull())
                 .select("game_id", F.col("player1_id").alias("player_id"), "evk")
                 .groupBy("game_id","player_id").agg(F.countDistinct("evk").alias("trb")))

    base_game = (base_game
                 .join(assist, ["game_id","player_id"], "left")
                 .join(rebs,   ["game_id","player_id"], "left")
                 .fillna({"ast":0, "trb":0})
                 .withColumn("pts", F.col("tpm")*3 + (F.col("fgm")-F.col("tpm"))*2 + F.col("ftm")))

# Join stagione
pg = base_game.join(game, "game_id", "left").withColumn("gp", F.lit(1))

# ---------- Per-season ----------
per_season = (
    pg.groupBy("player_id","team_id","season","season_start","season_type")
      .agg(
          F.sum("gp").alias("gp"),
          F.sum("fgm").alias("fgm_total"),
          F.sum("fga").alias("fga_total"),
          F.sum("tpm").alias("tpm_total"),
          F.sum("tpa").alias("tpa_total"),
          F.sum("ftm").alias("ftm_total"),
          F.sum("fta").alias("fta_total"),
          F.sum("trb").alias("trb_total"),
          F.sum("ast").alias("ast_total"),
          F.sum("pts").alias("pts_total"),
      )
      .withColumn("pts_pg", safe_div(F.col("pts_total"), F.col("gp")))
      .withColumn("trb_pg", safe_div(F.col("trb_total"), F.col("gp")))
      .withColumn("ast_pg", safe_div(F.col("ast_total"), F.col("gp")))
      .withColumn("fg_pct",  safe_div(F.col("fgm_total"), F.col("fga_total")))
      .withColumn("tp_pct",  safe_div(F.col("tpm_total"), F.col("tpa_total")))
      .withColumn("ft_pct",  safe_div(F.col("ftm_total"), F.col("fta_total")))
)

# ---------- Career ----------
career = (
    per_season.groupBy("player_id","season_type")
      .agg(
          F.sum("gp").alias("gp"),
          F.sum("fgm_total").alias("fgm_total"),
          F.sum("fga_total").alias("fga_total"),
          F.sum("tpm_total").alias("tpm_total"),
          F.sum("tpa_total").alias("tpa_total"),
          F.sum("ftm_total").alias("ftm_total"),
          F.sum("fta_total").alias("fta_total"),
          F.sum("trb_total").alias("trb_total"),
          F.sum("ast_total").alias("ast_total"),
          F.sum("pts_total").alias("pts_total"),
      )
      .withColumn("pts_pg", safe_div(F.col("pts_total"), F.col("gp")))
      .withColumn("trb_pg", safe_div(F.col("trb_total"), F.col("gp")))
      .withColumn("ast_pg", safe_div(F.col("ast_total"), F.col("gp")))
      .withColumn("fg_pct",  safe_div(F.col("fgm_total"), F.col("fga_total")))
      .withColumn("tp_pct",  safe_div(F.col("tpm_total"), F.col("tpa_total")))
      .withColumn("ft_pct",  safe_div(F.col("ftm_total"), F.col("fta_total")))
)

# ---------- Last season (Regular) ----------
w = Window.partitionBy("player_id").orderBy(F.col("season_start").desc())
last = (
    per_season.filter(F.col("season_type")=="Regular Season")
              .withColumn("rk", F.row_number().over(w))
              .filter("rk=1").drop("rk")
)

# ---------- Write (single file) ----------
per_season = per_season.coalesce(1)
career     = career.coalesce(1)
last       = last.coalesce(1)

out_stats  = os.path.join(WAREHOUSE, "global_player_stats")
out_career = os.path.join(WAREHOUSE, "global_player_career")
out_last   = os.path.join(WAREHOUSE, "global_player_last_season")

per_season.write.mode("overwrite").parquet(out_stats)
print("[OK] wrote", out_stats)

career.write.mode("overwrite").parquet(out_career)
print("[OK] wrote", out_career)

last.write.mode("overwrite").parquet(out_last)
print("[OK] wrote", out_last)

spark.stop()
print("[DONE]")
