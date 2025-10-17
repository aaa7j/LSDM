"""
Run the FastAPI search service with a single long-lived PySpark session.

Windows-friendly launcher: configures JAVA_HOME and temp dirs, then starts
uvicorn with workers=1 so only one Spark session is created.
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys
import glob
import uvicorn

# ------------------------- Windows/Spark env helpers ------------------------- #
def _ensure_windows_env(java_home_override: str | None = None):
    JAVA_HOME = java_home_override or os.environ.get(
        "JAVA_HOME",
        r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot",
    )
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

    tmp_dir = os.environ.get("SPARK_LOCAL_DIRS", r"C:\spark-tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    os.environ.setdefault("TMP", tmp_dir)
    os.environ.setdefault("TEMP", tmp_dir)
    os.environ.setdefault("SPARK_LOCAL_DIRS", tmp_dir)
    os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={tmp_dir} -Dfile.encoding=UTF-8")

    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--conf spark.local.dir={tmp_dir} "
        f"--conf spark.sql.session.timeZone=UTC "
        "pyspark-shell"
    )

# ----------------------- IO helpers ------------------------ #
from pyspark.sql import functions as F  # noqa: F401

def _read_parquet_if_exists(spark, path: str | pathlib.Path | None):
    import traceback
    try:
        if not path:
            return None
        p = pathlib.Path(path).resolve()
        if not p.exists():
            return None
        return spark.read.parquet(p.as_uri())
    except Exception as e:
        print(f"[boot][PARQUET READ FAIL] {path} -> {e}")
        traceback.print_exc()
        return None

def _read_csv_if_exists(spark, path: str | pathlib.Path | None):
    """
    CSV reader robusto:
    - Risolve in URI file:///
    - Prova delimitatori comuni (',' ';' '\\t')
    - Normalizza i nomi colonna a lower()
    """
    import traceback
    try:
        if not path:
            return None
        p = pathlib.Path(path).resolve()
        if not p.exists():
            return None
        uri = p.as_uri()
        for delim in [",", ";", "\t"]:
            try:
                df = (spark.read.format("csv")
                      .option("header", True)
                      .option("inferSchema", True)
                      .option("multiLine", True)
                      .option("escape", '"')
                      .option("delimiter", delim)
                      .load(uri))
                if not df.columns:
                    continue
                for c in df.columns:
                    df = df.withColumnRenamed(c, c.strip().lower())
                return df
            except Exception as e2:
                print(f"[boot][CSV READ TRY delim='{delim}'] {path} -> {e2}")
                continue
        print(f"[boot][CSV READ FAIL] Nessun delimitatore ha funzionato per: {path}")
        return None
    except Exception as e:
        print(f"[boot][CSV READ FAIL] {path} -> {e}")
        traceback.print_exc()
        return None

def _normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    return df

def _candidate_dirs(base: str | pathlib.Path):
    script_dir = pathlib.Path(__file__).resolve().parent
    root = script_dir.parent
    p = pathlib.Path(base)
    return list(dict.fromkeys([
        p,
        root / p,
        pathlib.Path.cwd() / p,
        root / str(base),
        script_dir.parent / str(base),
    ]))

def _first_existing(base_dirs, patterns):
    for base in base_dirs:
        for pat in patterns:
            for p in glob.glob(str(pathlib.Path(base) / pat)):
                if pathlib.Path(p).exists():
                    return str(pathlib.Path(p).resolve())
    return None

# ----------------------- Register support temp views ------------------------ #
def register_team_support_views(spark, warehouse="warehouse", data_dir="data"):
    """Create temp views TEAM_INFO_COMMON, TEAM_DETAILS/GLOBAL_TEAM_DETAILS, COMMON_PLAYER_INFO, GLOBAL_PLAYER."""
    wdirs = _candidate_dirs(warehouse)
    ddirs = _candidate_dirs(data_dir)

    # parquet directories
    pq_team_info = _first_existing(wdirs, ["team_info_common", "team_info_common/*"])
    pq_team_det  = _first_existing(wdirs, ["team_details", "team_details/*", "global_team_details", "global_team_details/*"])
    pq_gplayer   = _first_existing(wdirs, ["global_player", "global_player/*"])

    # csv files
    csv_team_info = _first_existing(ddirs, ["team_info_common.csv", "*team*info*common*.csv"])
    csv_team_det  = _first_existing(ddirs, ["team_details.csv", "*team*details*.csv"])
    csv_cpi       = _first_existing(ddirs, ["common_player_info.csv", "*common*player*info*.csv"])

    print("[boot] PQ team_info_common      ->", pq_team_info or "NOT FOUND")
    print("[boot] PQ team_details          ->", pq_team_det  or "NOT FOUND")
    print("[boot] PQ global_player         ->", pq_gplayer   or "NOT FOUND")
    print("[boot] CSV team_info_common     ->", csv_team_info or "NOT FOUND")
    print("[boot] CSV team_details         ->", csv_team_det  or "NOT FOUND")
    print("[boot] CSV common_player_info   ->", csv_cpi       or "NOT FOUND")

    # TEAM_INFO_COMMON
    tic = _read_parquet_if_exists(spark, pq_team_info) or _read_csv_if_exists(spark, csv_team_info)
    if tic is not None:
        _normalize_columns(tic).createOrReplaceTempView("TEAM_INFO_COMMON")

    # TEAM_DETAILS (+ alias GLOBAL_TEAM_DETAILS)
    tdet = _read_parquet_if_exists(spark, pq_team_det) or _read_csv_if_exists(spark, csv_team_det)
    if tdet is not None:
        tdet = _normalize_columns(tdet)
        tdet.createOrReplaceTempView("TEAM_DETAILS")
        tdet.createOrReplaceTempView("GLOBAL_TEAM_DETAILS")

    # COMMON_PLAYER_INFO (solo CSV)
    cpi = _read_csv_if_exists(spark, csv_cpi)
    if cpi is not None:
        _normalize_columns(cpi).createOrReplaceTempView("COMMON_PLAYER_INFO")

    # GLOBAL_PLAYER (se parquet presente)
    gp = _read_parquet_if_exists(spark, pq_gplayer)
    if gp is not None:
        _normalize_columns(gp).createOrReplaceTempView("GLOBAL_PLAYER")

    print("[boot] Registered support views (if available):",
          "[TEAM_INFO_COMMON]", bool(tic is not None),
          "[TEAM_DETAILS/GLOBAL_TEAM_DETAILS]", bool(tdet is not None),
          "[COMMON_PLAYER_INFO]", bool(cpi is not None),
          "[GLOBAL_PLAYER]", bool(gp is not None))

# ----------------------- Build TEAM_ARENA_RESOLVED ------------------------ #
from pyspark.sql import Window

def build_team_arena_resolved(spark, data_dir: str = "data"):
    """
    Costruisce TEAM_ARENA_RESOLVED unendo fonti se presenti (GLOBAL_TEAM, TEAM_DETAILS,
    TEAM_INFO_COMMON, GLOBAL_GAME) e applicando COALESCE in ordine di priorità.
    Tutto è condizionale: nessun riferimento a colonne mancanti.
    """
    def _exists(name: str) -> bool:
        try:
            spark.table(name).limit(1).collect()
            return True
        except Exception:
            return False

    def _read_csv(name: str):
        p = pathlib.Path(data_dir).resolve() / f"{name}.csv"
        return _read_csv_if_exists(spark, p)

    # ----------- Caricamento sorgenti (tutte opzionali) -----------
    gt = None
    try:
        if _exists("GLOBAL_TEAM"):
            gt0 = _normalize_columns(spark.table("GLOBAL_TEAM"))
            if {"team_id", "arena"} <= set(gt0.columns):
                gt = gt0.select(
                    F.col("team_id").cast("string").alias("team_id"),
                    F.col("arena").alias("arena_gt")
                )
    except Exception:
        gt = None

    # TEAM_DETAILS (con varianti nomi colonna)
    td = None
    try:
        base = None
        if _exists("TEAM_DETAILS"):
            base = _normalize_columns(spark.table("TEAM_DETAILS"))
        elif _exists("GLOBAL_TEAM_DETAILS"):
            base = _normalize_columns(spark.table("GLOBAL_TEAM_DETAILS"))
        if base is None:
            base = _read_csv("team_details")

        if base is not None:
            cols = set(base.columns)
            tid_col = next((k for k in ("team_id", "teamid", "tid") if k in cols), None)
            a_col   = next((k for k in ("arena", "arena_name", "venue", "stadium", "arena_full_name") if k in cols), None)
            cap_col = next((k for k in ("arena_capacity", "arenacapacity", "capacity", "arena_cap") if k in cols), None)
            if tid_col and a_col:
                td = base.select(
                    F.col(tid_col).cast("string").alias("team_id"),
                    F.col(a_col).alias("arena_td"),
                    (F.col(cap_col) if cap_col else F.lit(None)).alias("arena_capacity_td")
                )
    except Exception:
        td = None

    # TEAM_INFO_COMMON
    tic = None
    try:
        base = _normalize_columns(spark.table("TEAM_INFO_COMMON")) if _exists("TEAM_INFO_COMMON") else _read_csv("team_info_common")
        if base is not None:
            cols = set(base.columns)
            tid_col = next((k for k in ("team_id", "teamid", "tid") if k in cols), None)
            a_col   = next((k for k in ("arena", "arena_name", "venue", "stadium", "home_arena", "arena_full_name") if k in cols), None)
            cap_col = next((k for k in ("arena_capacity", "arenacapacity", "capacity", "arena_cap") if k in cols), None)
            if tid_col and a_col:
                tic = base.select(
                    F.col(tid_col).cast("string").alias("team_id"),
                    F.col(a_col).alias("arena_tic"),
                    (F.col(cap_col) if cap_col else F.lit(None)).alias("arena_capacity_tic")
                )
    except Exception:
        tic = None

    # GLOBAL_GAME → arena più frequente in casa
    gg_mode = None
    try:
        if _exists("GLOBAL_GAME"):
            g0 = _normalize_columns(spark.table("GLOBAL_GAME"))
            cols = set(g0.columns)
            home_k  = next((k for k in ("home_team_id", "team_id_home", "homeid", "home_teamid", "team_home_id") if k in cols), None)
            arena_k = next((k for k in ("arena", "arena_name", "venue", "stadium", "arena_full_name") if k in cols), None)
            if home_k and arena_k:
                agg = (g0
                    .select(F.col(home_k).cast("string").alias("team_id"), F.col(arena_k).alias("arena"))
                    .groupBy("team_id", "arena").count())
                w = Window.partitionBy("team_id").orderBy(F.col("count").desc(), F.col("arena").asc())
                gg_mode = (agg
                    .withColumn("rn", F.row_number().over(w))
                    .where(F.col("rn") == 1)
                    .select(F.col("team_id"), F.col("arena").alias("arena_gg")))
    except Exception:
        gg_mode = None

    # ----------- Base team_id -----------
    bases = [df.select("team_id") for df in (gt, td, tic, gg_mode) if df is not None]
    if not bases:
        spark.createDataFrame([], "team_id string, arena string, arena_capacity string") \
             .createOrReplaceTempView("TEAM_ARENA_RESOLVED")
        print("[build] TEAM_ARENA_RESOLVED -> EMPTY (no sources)")
        return

    base = bases[0]
    for b in bases[1:]:
        base = base.unionByName(b, allowMissingColumns=True)
    base = base.dropDuplicates(["team_id"])

    # ----------- Join incrementali -----------
    res = base
    if gt is not None:
        res = res.join(gt, "team_id", "left")
    if td is not None:
        res = res.join(td, "team_id", "left")
    if tic is not None:
        res = res.join(tic, "team_id", "left")
    if gg_mode is not None:
        res = res.join(gg_mode, "team_id", "left")

    # ----------- Selezione finale (solo colonne esistenti) -----------
    res_cols = set(res.columns)

    def _coalesce_existing(candidates: list[str]):
        exprs = [F.col(c) for c in candidates if c in res_cols]
        return F.coalesce(*exprs) if exprs else F.lit(None)

    arena_expr = _coalesce_existing(["arena_gt", "arena_td", "arena_tic", "arena_gg"]).cast("string")
    cap_expr   = _coalesce_existing(["arena_capacity_td", "arena_capacity_tic"]).cast("string")

    res = res.select(
        F.col("team_id"),
        arena_expr.alias("arena"),
        cap_expr.alias("arena_capacity"),
    ).dropDuplicates(["team_id"])

    # Materializza/cacha per velocizzare query successive
    res = res.cache()
    rows = res.count()
    res.createOrReplaceTempView("TEAM_ARENA_RESOLVED")
    print(f"[build] TEAM_ARENA_RESOLVED OK. rows={rows}")

# ---------------------------------- main ----------------------------------- #
def main():
    parser = argparse.ArgumentParser(description="Start the NBA search FastAPI service (single Spark session)")
    parser.add_argument("--warehouse", default="warehouse", help="Parquet warehouse root directory")
    parser.add_argument("--data-dir", default=os.getenv("DATA_DIR", "data"), help="CSV directory for fallbacks")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host for the HTTP server")
    parser.add_argument("--port", type=int, default=8000, help="Bind port for the HTTP server")
    parser.add_argument("--reload", action="store_true", help="Enable autoreload (dev mode)")
    parser.add_argument("--java-home", default=None, help="Override JAVA_HOME (JDK root directory)")
    args = parser.parse_args()

    # Make repo importable (so `import scripts.search_service` works)
    ROOT = pathlib.Path(__file__).resolve().parents[1]
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    # Prepare Windows/Java/Spark env vars and point the service to the warehouse
    _ensure_windows_env(args.java_home)
    os.environ.setdefault("WAREHOUSE_DIR", args.warehouse)
    os.environ.setdefault("DATA_DIR", args.data_dir)
    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    except Exception:
        pass

    # Import app module and ensure shared SparkSession
    import scripts.search_service as svc
    app = svc.app

    from pyspark.sql import SparkSession
    s = getattr(svc, "spark", None) or SparkSession.getActiveSession()
    if s is None:
        s = (SparkSession.builder
                .appName("nba-search-service")
                .getOrCreate())
    svc.spark = s

    # Views di supporto + TEAM_ARENA_RESOLVED
    register_team_support_views(s, warehouse=args.warehouse, data_dir=args.data_dir)
    build_team_arena_resolved(s, data_dir=args.data_dir)

    # Startup diagnostics
    print(">>> Python:", sys.executable)
    print(">>> JAVA_HOME:", os.environ.get("JAVA_HOME"))
    try:
        import subprocess
        out = subprocess.check_output(["java", "-version"], stderr=subprocess.STDOUT).decode(errors="ignore").strip()
        print(">>> java -version:\n" + out)
    except Exception as e:
        print("[WARN] java -version failed:", e)
    print(">>> WAREHOUSE_DIR:", os.environ.get("WAREHOUSE_DIR"))
    print(">>> DATA_DIR:", os.environ.get("DATA_DIR"))

    # Use import string when --reload (fixes the warning)
    if args.reload:
        uvicorn.run("scripts.search_service:app", host=args.host, port=args.port, reload=True, workers=1)
    else:
        uvicorn.run(app, host=args.host, port=args.port, reload=False, workers=1)

if __name__ == "__main__":
    main()
