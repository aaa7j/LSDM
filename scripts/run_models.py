"""
Train simple analytics models on the Parquet warehouse:

- Player performance: aggregate scoring metrics from GLOBAL_PLAY_BY_PLAY
- Game outcome: logistic regression predicting home win from GLOBAL_OTHER_STATS

This script reads the GLOBAL_* views from a Parquet warehouse (no CSVs needed).
"""
from __future__ import annotations

import argparse
import os
from typing import Dict

from pyspark.sql import SparkSession

from src.analytics.player_metrics import (
    top_scorers,
    write_player_metrics,
)
from src.analytics.game_prediction import (
    build_game_feature_frame,
    save_model,
    train_outcome_model,
)


GLOBAL_PARQUET = {
    "GLOBAL_PLAYER": "global_player",
    "GLOBAL_TEAM": "global_team",
    "GLOBAL_GAME": "global_game",
    "GLOBAL_PLAY_BY_PLAY": "global_play_by_play",
    "GLOBAL_OTHER_STATS": "global_other_stats",
}


def _get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BasketballAnalytics-Models")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _register_views_from_warehouse(spark: SparkSession, base: str) -> None:
    print(f"[INIT] Registering GLOBAL_* from: {os.path.abspath(base)}")
    missing = []
    for view_name, sub in GLOBAL_PARQUET.items():
        path = os.path.join(base, sub)
        if not os.path.isdir(path):
            print(f"[WARN] Missing path for {view_name}: {path}")
            missing.append(view_name)
            continue
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(view_name)
        print(f"[INIT] -> {view_name}")
    if missing:
        print("[WARN] Some views are missing; related features may be unavailable:", ", ".join(missing))


def main() -> None:
    parser = argparse.ArgumentParser(description="Train analytics models from Parquet warehouse")
    parser.add_argument("--warehouse", default="warehouse", help="Parquet warehouse directory (GLOBAL_* subfolders)")
    parser.add_argument("--top-n", type=int, default=10, help="Top N scorers to show")
    parser.add_argument("--metrics-out", default=None, help="Directory to save player metrics (Parquet by default)")
    parser.add_argument("--metrics-format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--model-dir", default=None, help="Directory to save the trained outcome model")
    parser.add_argument("--predictions-out", default=None, help="Directory to save test predictions (Parquet)")
    parser.add_argument("--reg-param", type=float, default=0.0, help="Logistic regression L2 parameter")
    parser.add_argument("--elastic-net", type=float, default=0.0, help="Logistic regression elastic net parameter")
    args = parser.parse_args()

    spark = _get_spark()
    _register_views_from_warehouse(spark, args.warehouse)

    print("=== Player scoring leaders ===")
    top_df = top_scorers(spark, limit=args.top_n)
    top_df.show(truncate=False)

    if args.metrics_out:
        write_player_metrics(
            spark,
            out_dir=args.metrics_out,
            format=args.metrics_format,
        )
        print(f"Saved player metrics to {args.metrics_out} ({args.metrics_format})")

    print("=== Building features for outcome model ===")
    features = build_game_feature_frame(spark)
    if features.count() == 0:
        raise SystemExit("No game features available. Ensure GLOBAL_OTHER_STATS is present in the warehouse.")

    model, metrics, predictions = train_outcome_model(
        features,
        reg_param=args.reg_param,
        elastic_net_param=args.elastic_net,
    )

    print("=== Model evaluation ===")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")

    if args.model_dir:
        save_model(model, args.model_dir)
        print(f"Persisted model to {args.model_dir}")

    if args.predictions_out:
        predictions.select(
            "game_id",
            "home_team_id",
            "away_team_id",
            "label",
            "prediction",
            "probability",
        ).write.mode("overwrite").parquet(args.predictions_out)
        print(f"Saved predictions to {args.predictions_out}")

    spark.stop()


if __name__ == "__main__":
    main()

