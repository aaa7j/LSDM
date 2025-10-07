from __future__ import annotations

from typing import Dict, List, Optional, Tuple

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Feature names for team form (averaged over recent games)
BASE_STATS: List[str] = [
    "rebounds",
    "assists",
    "steals",
    "blocks",
    "turnovers",
    "fouls",
]

FEATURE_DIFFS: List[str] = [
    f"{c}_diff" for c in BASE_STATS
]


def _games_last_years(spark: SparkSession, years: int = 4) -> DataFrame:
    # Prefer reconstructed GAME_RESULT if available (more reliable scores)
    table_names = {t.name for t in spark.catalog.listTables()}
    if "GAME_RESULT" in table_names:
        gsrc = (spark.table("GAME_RESULT")
                .select(
                    F.col("game_id").cast("string").alias("game_id"),
                    F.col("game_date").cast("date").alias("game_date"),
                    F.col("home_team_id").cast("int").alias("home_team_id"),
                    F.col("away_team_id").cast("int").alias("away_team_id"),
                    F.col("home_pts").cast("int").alias("final_score_home"),
                    F.col("away_pts").cast("int").alias("final_score_away"),
                ))
    else:
        gsrc = (spark.table("GLOBAL_GAME")
                .select(
                    F.col("game_id").cast("string").alias("game_id"),
                    F.col("game_date").cast("date").alias("game_date"),
                    F.col("home_team_id").cast("int").alias("home_team_id"),
                    F.col("away_team_id").cast("int").alias("away_team_id"),
                    F.col("final_score_home").cast("int").alias("final_score_home"),
                    F.col("final_score_away").cast("int").alias("final_score_away"),
                ))
    g = gsrc
    max_year = g.select(F.max(F.year("game_date")).alias("y")).collect()[0]["y"]
    cutoff = max_year - (years - 1)
    return g.where(F.year("game_date") >= cutoff)


def build_pregame_features(
    spark: SparkSession,
    window_n: int = 10,
    years: int = 4,
) -> DataFrame:
    """Build pre-game features using rolling team form with a lookback window.

    For each game, compute per-team rolling averages over the previous N games
    (excluding the current game), then take home minus away differences.
    """

    g = _games_last_years(spark, years=years)

    os = spark.table("GLOBAL_OTHER_STATS").select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("team_id").cast("int").alias("team_id"),
        *[F.col(c).cast("double").alias(c) for c in BASE_STATS if c in spark.table("GLOBAL_OTHER_STATS").columns],
    )

    # Ensure all base stats exist (fill missing with 0.0 if absent)
    for c in BASE_STATS:
        if c not in os.columns:
            os = os.withColumn(c, F.lit(0.0))

    tstats = os.join(g.select("game_id", "game_date"), on="game_id", how="inner")

    w = Window.partitionBy("team_id").orderBy(F.col("game_date").asc()).rowsBetween(-window_n, -1)

    form_cols = [F.avg(c).over(w).alias(f"{c}_avg") for c in BASE_STATS]
    form = tstats.select("team_id", "game_id", "game_date", *form_cols)

    # Join rolling form back to games for home and away
    h = (form.alias("h")
         .join(g.alias("g"), (F.col("h.game_id") == F.col("g.game_id")) & (F.col("g.home_team_id") == F.col("h.team_id")), "inner"))
    a = (form.alias("a")
         .join(g.alias("g"), (F.col("a.game_id") == F.col("g.game_id")) & (F.col("g.away_team_id") == F.col("a.team_id")), "inner"))

    joined = (h.select(
        F.col("g.game_id").alias("game_id"),
        F.col("g.game_date").alias("game_date"),
        *[F.col(f"h.{c}_avg").alias(f"h_{c}") for c in BASE_STATS],
        F.col("g.final_score_home").alias("final_score_home"),
        F.col("g.final_score_away").alias("final_score_away"),
    ).join(
        a.select(
            F.col("g.game_id").alias("game_id"),
            *[F.col(f"a.{c}_avg").alias(f"a_{c}") for c in BASE_STATS],
        ), on="game_id", how="inner"
    ))

    # Build diffs home - away and label (home win)
    diff_exprs = []
    for c in BASE_STATS:
        hc = F.coalesce(F.col(f"h_{c}"), F.lit(0.0))
        ac = F.coalesce(F.col(f"a_{c}"), F.lit(0.0))
        diff_exprs.append((hc - ac).alias(f"{c}_diff"))
    out = joined.select(
        "game_id",
        "game_date",
        ((F.col("final_score_home") > F.col("final_score_away")).cast("double")).alias("label"),
        *diff_exprs,
    )

    # Drop rows without a valid label (scores missing)
    out = out.where(F.col("label").isNotNull())

    # If some diffs are null, treat them as 0 (already coalesced above)
    return out


def train_pregame_outcome_model(
    spark: SparkSession,
    window_n: int = 10,
    years: int = 4,
    reg_param: float = 0.0,
    elastic_net: float = 0.0,
    seed: int = 13,
) -> Tuple[PipelineModel, Dict[str, float]]:
    features = build_pregame_features(spark, window_n=window_n, years=years)
    # Ensure label is valid and diffs are numeric
    features = features.where(F.col("label").isNotNull())

    assembler = VectorAssembler(inputCols=FEATURE_DIFFS, outputCol="features_vector")
    scaler = StandardScaler(inputCol="features_vector", outputCol="features", withMean=True, withStd=True)
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, regParam=reg_param, elasticNetParam=elastic_net)

    pipeline = Pipeline(stages=[assembler, scaler, lr])

    train_df, test_df = features.orderBy("game_date").randomSplit([0.8, 0.2], seed=seed)
    model = pipeline.fit(train_df)
    preds = model.transform(test_df)

    total = preds.count()
    correct = preds.filter(F.col("prediction") == F.col("label")).count()
    accuracy = float(correct) / float(total) if total else 0.0

    metrics: Dict[str, float] = {
        "train_rows": float(train_df.count()),
        "test_rows": float(total),
        "accuracy": float(accuracy),
    }
    return model, metrics


def team_form_at(
    spark: SparkSession,
    team_id: int,
    cutoff_date: str,
    window_n: int = 10,
) -> Optional[Dict[str, float]]:
    """Compute average form for a team using the last N games before cutoff_date."""
    g = spark.table("GLOBAL_GAME").select("game_id", F.col("game_date").cast("date").alias("game_date"))
    os = spark.table("GLOBAL_OTHER_STATS").select(
        F.col("game_id").cast("string").alias("game_id"),
        F.col("team_id").cast("int").alias("team_id"),
        *[F.col(c).cast("double").alias(c) for c in BASE_STATS if c in spark.table("GLOBAL_OTHER_STATS").columns],
    )
    for c in BASE_STATS:
        if c not in os.columns:
            os = os.withColumn(c, F.lit(0.0))

    df = (os.join(g, on="game_id", how="inner")
            .where((F.col("team_id") == team_id) & (F.col("game_date") < F.to_date(F.lit(cutoff_date))))
            .orderBy(F.col("game_date").desc())
            .limit(window_n))

    if df.count() == 0:
        return None

    agg = df.agg(*[F.avg(c).alias(c) for c in BASE_STATS]).collect()[0].asDict()
    return {k: float(agg.get(k) or 0.0) for k in BASE_STATS}


def build_match_features(
    spark: SparkSession,
    home_team_id: int,
    away_team_id: int,
    cutoff_date: Optional[str],
    window_n: int = 10,
) -> Optional[DataFrame]:
    """Prepare a single-row feature DataFrame for prediction (home - away)."""
    # Default cutoff: day after the latest game_date to use most recent form
    if not cutoff_date:
        last_date = spark.table("GLOBAL_GAME").select(F.max(F.col("game_date").cast("date")).alias("d")).collect()[0]["d"]
        cutoff_date = str(last_date)

    home_form = team_form_at(spark, home_team_id, cutoff_date, window_n=window_n)
    away_form = team_form_at(spark, away_team_id, cutoff_date, window_n=window_n)
    if home_form is None or away_form is None:
        return None

    diffs = {f"{k}_diff": float(home_form.get(k, 0.0) - away_form.get(k, 0.0)) for k in BASE_STATS}

    # Create a single-row DF with the expected feature columns
    row = [diffs.get(c, 0.0) for c in FEATURE_DIFFS]
    pdf = spark.createDataFrame([row], schema=FEATURE_DIFFS)
    return pdf


def load_pipeline_model(path: str) -> Optional[PipelineModel]:
    try:
        return PipelineModel.load(path)
    except Exception:
        return None
