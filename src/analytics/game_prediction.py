# src/analytics/outcome.py
# -----------------------------------------------------------------------------
# Pre-game outcome prediction (win_home) using team rolling averages.
# - Costruisce viste di risultato gara
# - Genera feature pre-match (medie ultime N partite per squadra)
# - Allena un modello Logistic Regression (Spark ML)
# - Funzioni utility: predire un match "oggi", baseline cumulativa, load/save
# -----------------------------------------------------------------------------

from __future__ import annotations
from typing import List, Tuple, Optional, Dict
import datetime

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# =========================
# 1) GAME RESULT VIEWS
# =========================

def build_game_result_views(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    Crea due viste logiche d'appoggio:
    - GAME_POINTS: home_pts/away_pts + meta partita
    - GAME_RESULT: label win_home (1 se home vince)
    Ritorna (game_points, game_result)
    """
    g = spark.table("GLOBAL_GAME").select(
        "game_id", "game_date", "home_team_id", "away_team_id",
        "final_score_home", "final_score_away"
    )

    # Se non ci sono i final score in GLOBAL_GAME, prova a ricostruirli da GLOBAL_LINE_SCORE
    if g.where(F.col("final_score_home").isNull() | F.col("final_score_away").isNull()).limit(1).count() > 0:
        ls = spark.table("GLOBAL_LINE_SCORE").groupBy("game_id").agg(
            F.sum(F.when(F.col("team_id") == F.first("team_id").over(Window.partitionBy("game_id").orderBy("team_id")),
                        F.col("points")).otherwise(F.lit(0))).alias("sum_points_any")  # dummy agg
        )
        # In pratica, se i final score sono mancanti, è meglio fidarsi di GLOBAL_GAME:
        # qui lasciamo i NULL; la label verrà esclusa in training dove mancante.
        pass

    game_points = g.selectExpr(
        "game_id",
        "game_date",
        "home_team_id",
        "away_team_id",
        "CAST(final_score_home AS INT) AS home_pts",
        "CAST(final_score_away AS INT) AS away_pts"
    )

    game_result = (game_points
                   .withColumn("label",
                               F.when(F.col("home_pts") > F.col("away_pts"), F.lit(1.0))
                                .when(F.col("home_pts") < F.col("away_pts"), F.lit(0.0))
                                .otherwise(F.lit(None)))  # pareggi improbabili nella NBA -> NULL
                   )

    game_points.createOrReplaceTempView("GAME_POINTS")
    game_result.createOrReplaceTempView("GAME_RESULT")
    return game_points, game_result


# =========================
# 2) PREGAME FEATURES
# =========================

# Colonne base disponibili in GLOBAL_OTHER_STATS per team/game
BASE_STATS: List[str] = [
    "points", "rebounds", "assists", "steals", "blocks", "turnovers", "fouls"
]

def _per_team_games(spark: SparkSession) -> DataFrame:
    """
    Costruisce la cronologia partite per squadra con data.
    Ritorna (team_id, game_id, game_date, is_home).
    """
    g = spark.table("GLOBAL_GAME").select("game_id", "game_date", "home_team_id", "away_team_id")
    home = g.select(F.col("game_id"),
                    F.col("game_date"),
                    F.col("home_team_id").alias("team_id"),
                    F.lit(1).alias("is_home"))
    away = g.select(F.col("game_id"),
                    F.col("game_date"),
                    F.col("away_team_id").alias("team_id"),
                    F.lit(0).alias("is_home"))
    return home.unionByName(away)


def _team_stat_games(spark: SparkSession) -> DataFrame:
    """
    GLOBAL_OTHER_STATS: (game_id, team_id) + stats di squadra nella singola partita.
    Joina con la data gara.
    """
    per_team = _per_team_games(spark)
    ost = spark.table("GLOBAL_OTHER_STATS").select(
        "game_id", "team_id",
        "points", "rebounds", "assists", "steals", "blocks", "turnovers", "fouls"
    )
    out = (ost.join(per_team, on=["game_id", "team_id"], how="left")
           .withColumn("game_date", F.to_date("game_date"))
           )
    return out


def build_pregame_features(
    spark: SparkSession,
    lookback: int = 10,
    min_games: int = 3,
) -> DataFrame:
    """
    Costruisce il feature frame per training pre-match:
    - per ogni gara: differenza (home_avg - away_avg) delle medie delle ultime N partite
      di ciascuna squadra (escluse le partite future/attuali).
    - label = win_home (da GAME_RESULT)

    Ritorna DataFrame con colonne:
      game_id, game_date, home_team_id, away_team_id,
      [diff_*], label
    """
    # Assicura le viste risultato
    _, game_result = build_game_result_views(spark)

    per_game = _team_stat_games(spark)
    # finestra: per squadra, ordina per data, considera le ultime N partite precedenti
    w = (Window
         .partitionBy("team_id")
         .orderBy(F.col("game_date").cast("timestamp").cast("long"))
         .rowsBetween(-lookback, -1))

    # rolling averages
    rolling = per_game.select(
        "team_id", "game_id", "game_date", "is_home",
        *[F.avg(F.col(c)).over(w).alias(f"{c}_avg") for c in BASE_STATS],
        F.count(F.lit(1)).over(w).alias("games_window")
    )

    # home/away medie prima della gara
    g = spark.table("GLOBAL_GAME").select("game_id", "game_date", "home_team_id", "away_team_id")
    r = rolling.alias("r")
    # join per home
    h = (g.join(r, (F.col("home_team_id") == F.col("r.team_id")) & (F.col("g.game_id") == F.col("r.game_id")),
                how="left")
         .select(
             F.col("g.*"),
             *[F.col(f"r.{c}_avg").alias(f"home_{c}_avg") for c in BASE_STATS],
             F.col("r.games_window").alias("home_games_window")
         )
         )
    h = h.alias("h")
    # join per away
    a = (h.join(rolling.alias("ra"),
                (F.col("h.away_team_id") == F.col("ra.team_id")) & (F.col("h.game_id") == F.col("ra.game_id")),
                how="left")
         .select(
             F.col("h.*"),
             *[F.col(f"ra.{c}_avg").alias(f"away_{c}_avg") for c in BASE_STATS],
             F.col("ra.games_window").alias("away_games_window")
         )
         )

    # differenze home - away
    for c in BASE_STATS:
        a = a.withColumn(f"diff_{c}", F.col(f"home_{c}_avg") - F.col(f"away_{c}_avg"))

    # minima copertura: richiedi almeno min_games nel rolling di entrambe
    a = (a.where((F.col("home_games_window") >= F.lit(min_games)) &
                 (F.col("away_games_window") >= F.lit(min_games))))

    # label
    fr = (a.join(game_result.select("game_id", "label"), on="game_id", how="left")
            .where(F.col("label").isNotNull()))

    # Ordina per data (utile per split temporale a valle)
    out = fr.select(
        "game_id", "game_date", "home_team_id", "away_team_id", "label",
        *[f"diff_{c}" for c in BASE_STATS]
    ).orderBy("game_date")

    out.createOrReplaceTempView("PREGAME_FEATURES")
    return out


# =========================
# 3) TRAIN / EVAL
# =========================

def _train_test_split_time(df: DataFrame, frac_train: float = 0.8) -> Tuple[DataFrame, DataFrame]:
    """
    Split temporale: usa la data per separare train/test (80/20 di default).
    Fallback a randomSplit se quantile non disponibile.
    """
    dd = df.withColumn("ts", F.col("game_date").cast("timestamp").cast("long"))
    try:
        q = dd.approxQuantile("ts", [frac_train], 0.01)[0]
        train = dd.where(F.col("ts") <= F.lit(q)).drop("ts")
        test = dd.where(F.col("ts") > F.lit(q)).drop("ts")
    except Exception:
        train, test = df.randomSplit([frac_train, 1 - frac_train], seed=42)
    return train, test


def train_pregame_outcome_model(
    spark: SparkSession,
    lookback: int = 10,
    min_games: int = 3,
    regularization: float = 0.0,
) -> Tuple[PipelineModel, Dict[str, float], DataFrame]:
    """
    Allena una Logistic Regression per predire win_home usando le differenze di rolling avg.
    Ritorna (model, metrics, predictions_on_test)
    """
    df = build_pregame_features(spark, lookback=lookback, min_games=min_games)

    feature_cols = [f"diff_{c}" for c in BASE_STATS]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=False)
    lr = LogisticRegression(featuresCol="features", labelCol="label", regParam=regularization)

    pipe = Pipeline(stages=[assembler, scaler, lr])
    train, test = _train_test_split_time(df)

    model = pipe.fit(train)
    preds = model.transform(test)

    ev = BinaryClassificationEvaluator(labelCol="label")
    metrics = {
        "roc_auc": float(ev.evaluate(preds, {ev.metricName: "areaUnderROC"})),
        "pr_auc": float(ev.evaluate(preds, {ev.metricName: "areaUnderPR"})),
        "accuracy": float(
            preds.select(F.avg((F.col("prediction") == F.col("label")).cast("double"))).first()[0]
        ),
        "n_train": train.count(),
        "n_test":  test.count(),
    }
    return model, metrics, preds


# =========================
# 4) PREDIRE UN MATCH "OGGI"
# =========================

def team_form_at(
    spark: SparkSession,
    team_id: int,
    as_of_date: Optional[datetime.date] = None,
    lookback: int = 10,
    min_games: int = 3,
) -> Optional[DataFrame]:
    """
    Calcola la forma media di una squadra fino a 'as_of_date' esclusa (ultime N gare).
    Ritorna un DF a singola riga con le medie; None se copertura insufficiente.
    """
    as_of_date = as_of_date or datetime.date.today()
    stats = _team_stat_games(spark).where((F.col("team_id") == F.lit(team_id)) &
                                          (F.col("game_date") < F.lit(as_of_date)))
    w = (Window
         .partitionBy("team_id")
         .orderBy(F.col("game_date").cast("timestamp").cast("long"))
         .rowsBetween(-lookback, -1))
    roll = stats.select(
        "team_id",
        *[F.avg(F.col(c)).over(w).alias(f"{c}_avg") for c in BASE_STATS],
        F.count(F.lit(1)).over(w).alias("games_window")
    ).orderBy(F.desc("games_window")).limit(1)

    row = roll.where(F.col("games_window") >= F.lit(min_games))
    if row.count() == 0:
        return None
    return row


def build_match_features(
    spark: SparkSession,
    home_team_id: int,
    away_team_id: int,
    as_of_date: Optional[datetime.date] = None,
    lookback: int = 10,
    min_games: int = 3,
) -> DataFrame:
    """
    Costruisce il vettore di feature per la partita ipotetica 'oggi' (o as_of_date).
    Ritorna un DF con colonne diff_* e meta (home_team_id, away_team_id).
    """
    as_of_date = as_of_date or datetime.date.today()

    fh = team_form_at(spark, home_team_id, as_of_date, lookback, min_games)
    fa = team_form_at(spark, away_team_id, as_of_date, lookback, min_games)

    if fh is None or fa is None:
        # ritorna DF vuoto con schema atteso
        schema_cols = ["home_team_id", "away_team_id"] + [f"diff_{c}" for c in BASE_STATS]
        return spark.createDataFrame([], ", ".join([f"{c} double" if c.startswith("diff_") else f"{c} int"
                                                    for c in schema_cols]))

    h = fh.selectExpr("team_id as home_team_id",
                      *[f"{c}_avg as home_{c}_avg" for c in BASE_STATS])
    a = fa.selectExpr("team_id as away_team_id",
                      *[f"{c}_avg as away_{c}_avg" for c in BASE_STATS])

    feat = (h.crossJoin(a)
              .withColumn("home_team_id", F.lit(home_team_id))
              .withColumn("away_team_id", F.lit(away_team_id)))

    for c in BASE_STATS:
        feat = feat.withColumn(f"diff_{c}", F.col(f"home_{c}_avg") - F.col(f"away_{c}_avg"))

    return feat.select("home_team_id", "away_team_id", *[f"diff_{c}" for c in BASE_STATS])


# =========================
# 5) BASELINE: CUMULATIVE AVERAGE MODEL (opzionale)
# =========================

def ensure_prediction_views(spark: SparkSession) -> None:
    """
    Crea una vista semplice GAME_RESULT (se non esiste già) per funzioni baseline.
    """
    build_game_result_views(spark)


def train_cumavg_model(spark: SparkSession) -> Tuple[PipelineModel, Dict[str, float], DataFrame]:
    """
    Baseline: usa la media cumulativa del differenziale punti come unica feature.
    """
    ensure_prediction_views(spark)
    g = spark.table("GLOBAL_GAME").select("game_id", "game_date", "home_team_id", "away_team_id")
    r = spark.table("GAME_RESULT").select("game_id", "label", "home_pts", "away_pts")

    # Differenziale per team in ogni gara (home: +diff, away: -diff)
    diff = (g.join(r, "game_id", "inner")
              .select(
                  "game_id", "game_date",
                  F.col("home_team_id").alias("team_id"),
                  (F.col("home_pts") - F.col("away_pts")).alias("pt_diff"))
              .unionByName(
                  g.join(r, "game_id", "inner")
                   .select(
                       "game_id", "game_date",
                       F.col("away_team_id").alias("team_id"),
                       (F.col("away_pts") - F.col("home_pts")).alias("pt_diff_neg"))
                   .withColumnRenamed("pt_diff_neg", "pt_diff")
              )
           )

    w = (Window.partitionBy("team_id")
               .orderBy(F.col("game_date").cast("timestamp").cast("long"))
               .rowsBetween(Window.unboundedPreceding, -1))

    cumavg = (diff
              .withColumn("cumavg_diff", F.avg("pt_diff").over(w))
              .select("game_id", "team_id", "cumavg_diff"))

    # Join su home e away e crea diff feature
    feat = (g.join(cumavg.alias("h"), (F.col("g.game_id") == F.col("h.game_id")) &
                                  (F.col("g.home_team_id") == F.col("h.team_id")), "left")
              .join(cumavg.alias("a"), (F.col("g.game_id") == F.col("a.game_id")) &
                                  (F.col("g.away_team_id") == F.col("a.team_id")), "left")
              .select(
                  F.col("g.game_id"), F.col("g.game_date"),
                  (F.col("h.cumavg_diff") - F.col("a.cumavg_diff")).alias("diff_cumavg")
              ))

    df = (feat.join(r.select("game_id", "label"), "game_id", "left")
              .where(F.col("label").isNotNull())
              .orderBy("game_date"))

    assembler = VectorAssembler(inputCols=["diff_cumavg"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    pipe = Pipeline(stages=[assembler, lr])

    train, test = _train_test_split_time(df)
    model = pipe.fit(train)
    preds = model.transform(test)

    ev = BinaryClassificationEvaluator(labelCol="label")
    metrics = {
        "roc_auc": float(ev.evaluate(preds, {ev.metricName: "areaUnderROC"})),
        "pr_auc": float(ev.evaluate(preds, {ev.metricName: "areaUnderPR"})),
        "accuracy": float(
            preds.select(F.avg((F.col("prediction") == F.col("label")).cast("double"))).first()[0]
        ),
        "n_train": train.count(),
        "n_test":  test.count(),
    }
    return model, metrics, preds


# =========================
# 6) SAVE / LOAD HELPERS
# =========================

def save_pipeline_model(model: PipelineModel, path: str) -> None:
    model.write().overwrite().save(path)

def load_pipeline_model(path: str) -> PipelineModel:
    return PipelineModel.load(path)
