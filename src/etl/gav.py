"""Utility helpers for working with the GAV transformation SQL."""

import os
from typing import Iterable, List, Optional

from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F, types as T


GLOBAL_VIEWS = [
    "GLOBAL_PLAYER",
    "GLOBAL_TEAM",
    "GLOBAL_GAME",
    "GLOBAL_PLAY_BY_PLAY",
    "GLOBAL_LINE_SCORE",
    "GLOBAL_OTHER_STATS",
    "GLOBAL_OFFICIAL",
    "GLOBAL_GAME_OFFICIAL",
    "GLOBAL_DRAFT_COMBINE",
    "GLOBAL_DRAFT_HISTORY",
]


GLOBAL_PRIMARY_KEYS = {
    "GLOBAL_PLAYER": ("player_id",),
    "GLOBAL_TEAM": ("team_id",),
    "GLOBAL_GAME": ("game_id",),
    "GLOBAL_PLAY_BY_PLAY": ("game_id", "eventnum"),
    "GLOBAL_LINE_SCORE": ("game_id", "team_id", "period"),
    "GLOBAL_OTHER_STATS": ("game_id", "team_id"),
    "GLOBAL_OFFICIAL": ("official_id",),
    "GLOBAL_GAME_OFFICIAL": ("game_id", "official_id"),
    "GLOBAL_DRAFT_COMBINE": ("player_id", "year"),
    "GLOBAL_DRAFT_HISTORY": ("player_id", "draft_year"),
}


def _default_sql_path() -> str:
    return os.path.join("src", "etl", "transform_gav.sql")


def execute_gav_sql(spark, sql_path: Optional[str] = None) -> List[str]:
    """Execute the SQL statements defining the GLOBAL_* views.

    Returns the list of executed SQL statements (trimmed), mainly for debugging.
    """

    path = sql_path or _default_sql_path()
    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read()

    executed: List[str] = []
    for statement in [s.strip() for s in raw.split(";") if s.strip()]:
        spark.sql(statement)
        executed.append(statement)

    return executed


def save_global_views(spark, out_dir: str, views: Optional[Iterable[str]] = None) -> None:
    """Save selected GLOBAL_* views as Parquet datasets under ``out_dir``."""

    target_views = list(views) if views is not None else GLOBAL_VIEWS
    os.makedirs(out_dir, exist_ok=True)

    skipped: List[str] = []
    for view in target_views:
        df = spark.sql(f"SELECT * FROM {view}")
        null_columns = [field.name for field in df.schema if field.dataType.simpleString() == "void"]
        for column in null_columns:
            df = df.withColumn(column, F.lit(None).cast("string"))
        destination = os.path.join(out_dir, view.lower())
        try:
            df.write.mode("overwrite").parquet(destination)
            print(f"Saved {view} -> {destination}")
        except Py4JJavaError as err:
            message = str(getattr(err, "java_exception", err))
            if "HADOOP_HOME" in message or "winutils" in message:
                print(f"WARNING: Skipping {view} -> {destination} (missing winutils.exe on Windows)")
                skipped.append(view)
            else:
                raise
    if skipped:
        print("WARNING: Parquet export skipped for: " + ", ".join(skipped))
        print("Install winutils.exe (set HADOOP_HOME) to enable saving on Windows.")
