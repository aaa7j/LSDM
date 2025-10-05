"""
Run the FastAPI search service with a single long‑lived PySpark session.

Windows-friendly launcher: configures JAVA_HOME and temp dirs, then starts
uvicorn with workers=1 so only one Spark session is created.

Usage examples:

  python scripts/run_search_service.py \
      --warehouse warehouse --host 127.0.0.1 --port 8000

  # Query:
  #   http://127.0.0.1:8000/search?q=michael%20jordan&limit=25
"""
from __future__ import annotations

import argparse
import os
import pathlib
import sys

import uvicorn


def _ensure_windows_env(java_home_override: str | None = None):
    # Mirror settings from scripts/run_search.py for Windows convenience
    JAVA_HOME = java_home_override or os.environ.get(
        "JAVA_HOME",
        r"C:\\Users\\angel\\AppData\\Local\\Programs\\Eclipse Adoptium\\jdk-17.0.16.8-hotspot",
    )
    # If JAVA_HOME mistakenly points to java.exe, normalize to JDK root
    if JAVA_HOME and JAVA_HOME.lower().endswith("bin\\java.exe"):
        JAVA_HOME = os.path.dirname(os.path.dirname(JAVA_HOME))
    # If JAVA_HOME is a file, normalize up to the JDK directory
    if JAVA_HOME and os.path.isfile(JAVA_HOME):
        parent = os.path.dirname(JAVA_HOME)
        if parent.lower().endswith("\\bin"):
            parent = os.path.dirname(parent)
        JAVA_HOME = parent
    if JAVA_HOME and os.path.isdir(JAVA_HOME):
        os.environ["JAVA_HOME"] = JAVA_HOME
        os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

    tmp_dir = os.environ.get("SPARK_LOCAL_DIRS", r"C:\\spark-tmp")
    try:
        os.makedirs(tmp_dir, exist_ok=True)
    except Exception:
        pass
    os.environ.setdefault("TMP", tmp_dir)
    os.environ.setdefault("TEMP", tmp_dir)
    os.environ.setdefault("SPARK_LOCAL_DIRS", tmp_dir)
    os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={tmp_dir} -Dfile.encoding=UTF-8")

    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", (
        f"--conf spark.local.dir={tmp_dir} "
        f"--conf spark.sql.session.timeZone=UTC "
        "pyspark-shell"
    ))


def main():
    parser = argparse.ArgumentParser(description="Start the NBA search FastAPI service (single Spark session)")
    parser.add_argument("--warehouse", default="warehouse", help="Parquet warehouse root directory")
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

    # Import here so env vars are in place before FastAPI module init
    # Import the service that initializes Spark on startup
    from scripts.search_service_fast import app  # noqa: WPS433

    # One worker => one JVM/Spark session kept alive
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
    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload, workers=1)


if __name__ == "__main__":
    main()
