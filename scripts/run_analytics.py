import argparse

from src.etl.read_sources import get_spark, register_sources
from src.etl.gav import execute_gav_sql
from src.analytics.player_metrics import (
    player_scoring_summary,
    top_scorers,
    write_player_metrics,
)
from src.analytics.game_prediction import (
    build_game_feature_frame,
    save_model,
    train_outcome_model,
)


def main():
    parser = argparse.ArgumentParser(description="Analytics workflows on the GAV schema")
    parser.add_argument("--base", default="csv", help="Base folder with Kaggle CSV files")
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top scorers to display",
    )
    parser.add_argument(
        "--metrics-out",
        default=None,
        help="If provided, save player metrics to this directory (Parquet by default)",
    )
    parser.add_argument(
        "--metrics-format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format when --metrics-out is used",
    )
    parser.add_argument(
        "--model-dir",
        default=None,
        help="Directory to persist the trained outcome model",
    )
    parser.add_argument(
        "--predictions-out",
        default=None,
        help="If provided, save test predictions to this directory (Parquet)",
    )
    parser.add_argument("--reg-param", type=float, default=0.0, help="Logistic regression L2 parameter")
    parser.add_argument(
        "--elastic-net",
        type=float,
        default=0.0,
        help="Logistic regression elastic net parameter",
    )
    args = parser.parse_args()

    spark = get_spark("BasketballAnalytics-Analytics")
    register_sources(spark, base=args.base)
    execute_gav_sql(spark)

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
        raise SystemExit("No game features available. Ensure GLOBAL_OTHER_STATS is populated.")

    model, metrics, predictions = train_outcome_model(
        features,
        reg_param=args.reg_param,
        elastic_net_param=args.elastic_net,
    )

    print("=== Model evaluation ===")
    for key, value in metrics.items():
        print(f"{key}: {value:.4f}" if isinstance(value, float) else f"{key}: {value}")

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


if __name__ == "__main__":
    main()
