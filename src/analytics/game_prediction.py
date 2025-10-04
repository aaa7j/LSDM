"""Game outcome prediction using PySpark ML."""

from typing import Dict, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession, functions as F


FEATURE_COLUMNS = [
    "rebounds_diff",
    "assists_diff",
    "steals_diff",
    "blocks_diff",
    "turnovers_diff",
    "fouls_diff",
]


def build_game_feature_frame(spark: SparkSession) -> DataFrame:
    """Return a DataFrame with engineered features for model training."""

    games = spark.table("GLOBAL_GAME").select(
        "game_id",
        "game_date",
        "home_team_id",
        "away_team_id",
        "final_score_home",
        "final_score_away",
    )

    stats = spark.table("GLOBAL_OTHER_STATS")

    home = stats.alias("home")
    away = stats.alias("away")

    joined = (
        games.alias("g")
        .join(
            home,
            (F.col("g.game_id") == F.col("home.game_id"))
            & (F.col("g.home_team_id") == F.col("home.team_id")),
            "inner",
        )
        .join(
            away,
            (F.col("g.game_id") == F.col("away.game_id"))
            & (F.col("g.away_team_id") == F.col("away.team_id")),
            "inner",
        )
    )

    engineered = joined.select(
        F.col("g.game_id"),
        F.col("g.game_date"),
        F.col("g.home_team_id"),
        F.col("g.away_team_id"),
        (F.col("g.final_score_home") > F.col("g.final_score_away")).cast("double").alias("label"),
        (F.col("home.rebounds") - F.col("away.rebounds")).alias("rebounds_diff"),
        (F.col("home.assists") - F.col("away.assists")).alias("assists_diff"),
        (F.col("home.steals") - F.col("away.steals")).alias("steals_diff"),
        (F.col("home.blocks") - F.col("away.blocks")).alias("blocks_diff"),
        (F.col("home.turnovers") - F.col("away.turnovers")).alias("turnovers_diff"),
        (F.col("home.fouls") - F.col("away.fouls")).alias("fouls_diff"),
    )

    return engineered.na.drop(subset=FEATURE_COLUMNS + ["label"])


def train_outcome_model(
    features: DataFrame,
    reg_param: float = 0.0,
    elastic_net_param: float = 0.0,
    seed: int = 13,
) -> Tuple[PipelineModel, Dict[str, float], DataFrame]:
    """Train a logistic regression model and return metrics and predictions."""

    assembler = VectorAssembler(inputCols=FEATURE_COLUMNS, outputCol="features_vector")
    scaler = StandardScaler(
        inputCol="features_vector",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=100,
        regParam=reg_param,
        elasticNetParam=elastic_net_param,
    )

    pipeline = Pipeline(stages=[assembler, scaler, lr])

    train_df, test_df = features.randomSplit([0.8, 0.2], seed=seed)

    model = pipeline.fit(train_df)
    predictions = model.transform(test_df)

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    roc_auc = evaluator.evaluate(predictions)

    correct = predictions.filter(F.col("prediction") == F.col("label")).count()
    total = predictions.count()
    train_count = train_df.count()
    accuracy = correct / total if total else 0.0

    metrics: Dict[str, float] = {
        "train_rows": float(train_count),
        "test_rows": float(total),
        "roc_auc": float(roc_auc),
        "accuracy": float(accuracy),
    }

    return model, metrics, predictions


def save_model(model: PipelineModel, path: str) -> None:
    model.write().overwrite().save(path)
