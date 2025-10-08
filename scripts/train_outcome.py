"""
Offline training script for the pre-game outcome model.

It reads GLOBAL_* tables from a Parquet warehouse, reconstructs GAME_RESULT
from GLOBAL_LINE_SCORE, builds simple pre-game features (cum-avg diff), and
trains a Logistic Regression model. The trained PipelineModel is saved to
--model-dir for the API to load at startup.

Usage:
  python scripts/train_outcome.py --warehouse warehouse --model-dir models/home-win --train-years 1 --overwrite
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Dict

from pyspark.sql import SparkSession

# --- Windows-friendly env (match other scripts) ---
import os
JAVA_HOME_DEFAULT = r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
JAVA_HOME = os.environ.get("JAVA_HOME", JAVA_HOME_DEFAULT)
if JAVA_HOME and JAVA_HOME.lower().endswith("bin\\java.exe"):
    JAVA_HOME = os.path.dirname(os.path.dirname(JAVA_HOME))
if JAVA_HOME and os.path.isdir(JAVA_HOME):
    os.environ["JAVA_HOME"] = JAVA_HOME
    os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
TMP_DIR = os.environ.get("SPARK_LOCAL_DIRS", r"C:\\spark-tmp")
os.makedirs(TMP_DIR, exist_ok=True)
os.environ.setdefault("TMP", TMP_DIR)
os.environ.setdefault("TEMP", TMP_DIR)
os.environ.setdefault("SPARK_LOCAL_DIRS", TMP_DIR)
os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")

# Make project importable (src/ layout)
import sys, pathlib
ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.outcome import build_game_result_views, ensure_prediction_views, train_cumavg_model


GLOBAL_PARQUET = {
    "GLOBAL_PLAYER": "global_player",
    "GLOBAL_TEAM": "global_team",
    "GLOBAL_GAME": "global_game",
    "GLOBAL_LINE_SCORE": "global_line_score",
}


def _get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("LSDM-TrainOutcome")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", TMP_DIR)
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _register_views_from_warehouse(spark: SparkSession, base: str) -> None:
    for view_name, sub in GLOBAL_PARQUET.items():
        path = os.path.join(base, sub)
        if not os.path.isdir(path):
            raise SystemExit(f"Missing Parquet path: {path}")
        spark.read.parquet(path).createOrReplaceTempView(view_name)
    for vn in GLOBAL_PARQUET.keys():
        spark.catalog.cacheTable(vn)


def main() -> None:
    ap = argparse.ArgumentParser(description="Train pre-game outcome model (cum-avg diff)")
    ap.add_argument("--warehouse", default="warehouse", help="Parquet warehouse with GLOBAL_* subfolders")
    ap.add_argument("--model-dir", default="models/home-win", help="Directory to save the trained model")
    ap.add_argument("--train-years", type=int, default=1, help="Use only the last N years for training")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing model directory")
    args = ap.parse_args()

    spark = _get_spark()
    _register_views_from_warehouse(spark, args.warehouse)

    # Build game result + prediction views
    build_game_result_views(spark)
    ensure_prediction_views(spark, train_years=args.train_years)

    model, metrics = train_cumavg_model(spark, train_years=args.train_years)
    if model is None:
        raise SystemExit("Training failed: dataset is single-class or empty. Check GAME_RESULT and training horizon.")

    if os.path.isdir(args.model_dir) and not args.overwrite:
        raise SystemExit(f"Model directory already exists: {args.model_dir}. Use --overwrite to replace it.")

    os.makedirs(args.model_dir, exist_ok=True)
    model.write().overwrite().save(args.model_dir)
    meta_path = os.path.join(args.model_dir, "metadata.json")
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump({"train_years": args.train_years, "metrics": metrics}, fh)

    print(f"[OK] Saved model to {args.model_dir}")
    print(f"[OK] Metrics: {metrics}")
    spark.stop()


if __name__ == "__main__":
    main()
