# --- HARDEN ENV for Spark on Windows (no move needed) ---
import os, sys, subprocess, pathlib, argparse

# 1) Java 17 path (ADATTA se diverso sul tuo pc)
JAVA_HOME = r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# 2) ASCII-only temp dirs (avoid OneDrive/Università paths)
TMP_DIR = r"C:\spark-tmp"
os.makedirs(TMP_DIR, exist_ok=True)
os.environ["TMP"] = TMP_DIR
os.environ["TEMP"] = TMP_DIR
os.environ["SPARK_LOCAL_DIRS"] = TMP_DIR
os.environ["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8"

# 3) Also pass same opts to Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f'--conf spark.local.dir={TMP_DIR} '
    f'--conf hadoop.tmp.dir={TMP_DIR} '
    f'--conf spark.driver.extraJavaOptions="-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8" '
    f'--conf spark.executor.extraJavaOptions="-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8" '
    "pyspark-shell"
)

print(">>> Python exe:", sys.executable)
print(">>> JAVA_HOME :", os.environ.get("JAVA_HOME"))
try:
    out = subprocess.check_output(["java","-version"], stderr=subprocess.STDOUT).decode(errors="ignore")
    print(">>> java -version:\n", out)
except Exception as e:
    print(">>> java -version FAILED:", e)
# ---------------------------------------------------------

# --- ensure project root is importable ---
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
print(">>> PROJECT ROOT:", ROOT)
print(">>> has src?     ", os.path.isdir(os.path.join(ROOT, "src")))
print(">>> has read_sources.py?", os.path.exists(os.path.join(ROOT, "src", "etl", "read_sources.py")))
# -----------------------------------------

from src.etl.read_sources import get_spark, register_sources  # :contentReference[oaicite:0]{index=0}
from src.etl.quality import summarize_row_counts, assert_primary_keys  # :contentReference[oaicite:1]{index=1}
from src.etl.gav import (
    execute_gav_sql,
    save_global_views,
    GLOBAL_VIEWS,
    GLOBAL_PRIMARY_KEYS,
)  # :contentReference[oaicite:2]{index=2}

def main():
    parser = argparse.ArgumentParser(description="Run GAV integration over Kaggle CSVs")
    parser.add_argument("--base", default="data", help="Base folder containing CSV files (default: data)")
    parser.add_argument("--save", default=None, help="If set, saves each GLOBAL_* view as Parquet under this directory")
    parser.add_argument("--preview", action="store_true", help="Show sample rows from key global views")
    parser.add_argument("--validate", action="store_true", help="Run data quality checks (row counts, PK uniqueness)")
    args = parser.parse_args()

    spark = get_spark()
    registered = register_sources(spark, base=args.base)
    print(f"Registered tables: {sorted(registered)}")

    execute_gav_sql(spark)  # creates GLOBAL_* views from src/etl/transform_gav.sql

    if args.validate:
        summarize_row_counts(spark, GLOBAL_VIEWS)
        pk_results = assert_primary_keys(spark, {k: list(v) for k, v in GLOBAL_PRIMARY_KEYS.items()})
        failed = [table for table, ok in pk_results.items() if not ok]
        if failed:
            raise SystemExit(f"Primary key violations detected: {failed}")

    if args.preview:
        print("Previewing GLOBAL_PLAYER / GLOBAL_TEAM / GLOBAL_GAME ...")
        spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 5").show(truncate=False)
        spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 5").show(truncate=False)
        spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 5").show(truncate=False)

    if args.save:
        save_global_views(spark, args.save)

if __name__ == "__main__":
    main()
