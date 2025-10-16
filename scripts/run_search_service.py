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

# ----------------------- Register support temp views ------------------------ #
from pyspark.sql import functions as F  # noqa: F401

def _read_parquet_if_exists(spark, path: str):
    try:
        return spark.read.parquet(path)
    except Exception:
        return None

def _read_csv_if_exists(spark, path: str):
    try:
        return (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )
    except Exception:
        return None

def _normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower())
    return df

def _candidate_dirs(base: str):
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

def register_team_support_views(spark, warehouse="warehouse", data_dir="data"):
    """Create temp views TEAM_INFO_COMMON, TEAM_DETAILS/GLOBAL_TEAM_DETAILS, COMMON_PLAYER_INFO, GLOBAL_PLAYER."""
    wdirs = _candidate_dirs(warehouse)
    ddirs = _candidate_dirs(data_dir)

    # parquet directories
    pq_team_info = _first_existing(wdirs, ["team_info_common", "team_info_common/*"])
    pq_team_det  = _first_existing(wdirs, ["team_details", "team_details/*", "global_team_details", "global_team_details/*"])
    pq_gplayer   = _first_existing(wdirs, ["global_player", "global_player/*"])

    # csv files (match anche case-insensitive/pattern)
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
    # Ensure PySpark uses the same interpreter as the driver/venv
    try:
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    except Exception:
        pass

    # Import after env is ready so Spark picks up config; also fetch spark instance
    from scripts.search_service import app, spark  # noqa: WPS433

    # Register supporting temp views (so /entity/team/{tid} can read arena/state/roster)
    register_team_support_views(spark, warehouse=args.warehouse, data_dir=args.data_dir)

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
