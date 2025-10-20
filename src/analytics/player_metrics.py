"""Player performance analytics built on the global GAV views."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession, functions as F, Window


def _scoring_events(pbp: DataFrame) -> DataFrame:
    """Derive per-event scoring information from play-by-play events."""

    cleaned = pbp.filter(F.col("score").isNotNull()).filter(F.col("player1_id").isNotNull())

    # Normalise the score string and split into home / away integers.
    cleaned = cleaned.withColumn("score_clean", F.regexp_replace("score", "\\s+", ""))
    cleaned = cleaned.withColumn("score_arr", F.split("score_clean", "-"))
    cleaned = cleaned.withColumn(
        "score_home",
        F.when(F.size("score_arr") == 2, F.element_at("score_arr", 1).cast("int")),
    ).withColumn(
        "score_away",
        F.when(F.size("score_arr") == 2, F.element_at("score_arr", 2).cast("int")),
    )

    window_spec = Window.partitionBy("game_id").orderBy("eventnum")
    cleaned = cleaned.withColumn(
        "prev_home",
        F.coalesce(F.lag("score_home").over(window_spec), F.lit(0)),
    ).withColumn(
        "prev_away",
        F.coalesce(F.lag("score_away").over(window_spec), F.lit(0)),
    )

    cleaned = cleaned.withColumn(
        "delta_home",
        F.when(F.col("score_home").isNotNull(), F.col("score_home") - F.col("prev_home")).otherwise(0),
    ).withColumn(
        "delta_away",
        F.when(F.col("score_away").isNotNull(), F.col("score_away") - F.col("prev_away")).otherwise(0),
    )

    cleaned = cleaned.withColumn(
        "points",
        F.greatest(F.col("delta_home"), F.col("delta_away")),
    )

    return cleaned.filter(F.col("points") > 0)


def player_scoring_summary(spark: SparkSession) -> DataFrame:
    """Compute aggregate scoring metrics per player."""

    # Prefer precomputed scoring events if available
    tables = {t.name.upper() for t in spark.catalog.listTables()}
    if "GLOBAL_SCORING_EVENTS" in tables:
        scoring = (
            spark.table("GLOBAL_SCORING_EVENTS")
            .where(F.col("player_id").isNotNull() & (F.col("player_id").cast("int") > 0))
            .select(
                F.col("player_id").cast("int").alias("player1_id"),
                "game_id",
                "points",
            )
        )
    else:
        pbp = spark.table("GLOBAL_PLAY_BY_PLAY")
        scoring = _scoring_events(pbp).where(F.col("player1_id").cast("int") > 0)

    summary = (
        scoring.groupBy("player1_id")
        .agg(
            F.countDistinct("game_id").alias("games_played"),
            F.sum("points").alias("total_points"),
            F.sum(F.when(F.col("points") == 3, 1).otherwise(0)).alias("made_threes"),
            F.sum(F.when(F.col("points") == 2, 1).otherwise(0)).alias("made_twos"),
            F.sum(F.when(F.col("points") == 1, 1).otherwise(0)).alias("made_fts"),
        )
        .filter(F.col("player1_id").isNotNull())
    )

    players = spark.table("GLOBAL_PLAYER").select(
        F.col("player_id").cast("int").alias("player1_id"),
        "full_name",
        "team_id",
        "position",
    )

    joined = summary.join(players, on="player1_id", how="left")
    joined = joined.withColumn(
        "avg_points",
        F.round(F.col("total_points") / F.col("games_played"), 2),
    )

    return joined.orderBy(F.col("total_points").desc())


def top_scorers(spark: SparkSession, limit: int = 10) -> DataFrame:
    return player_scoring_summary(spark).limit(limit)


def write_player_metrics(
    spark: SparkSession, out_dir: str, format: str = "parquet", limit: Optional[int] = None
) -> None:
    """Persist player scoring metrics to disk."""

    df = player_scoring_summary(spark)
    if limit is not None:
        df = df.limit(limit)

    if format == "parquet":
        df.write.mode("overwrite").parquet(out_dir)
    elif format == "csv":
        df.write.mode("overwrite").option("header", True).csv(out_dir)
    else:
        raise ValueError(f"Unsupported format: {format}")
