"""Utility helpers for working with the GAV transformation SQL."""

import os
from typing import Iterable, List, Optional

from py4j.protocol import Py4JJavaError
from pyspark.sql import functions as F


GLOBAL_VIEWS = [
    "GLOBAL_PLAYER",
    "GLOBAL_TEAM",
    "GLOBAL_GAME",
    "GLOBAL_PLAY_BY_PLAY",
    "GLOBAL_SCORING_EVENTS",
    "GLOBAL_PLAYER_GAME_OFFENSE",
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
    "GLOBAL_SCORING_EVENTS": ("game_id", "eventnum"),
    "GLOBAL_PLAYER_GAME_OFFENSE": ("game_id", "player_id"),
    "GLOBAL_LINE_SCORE": ("game_id", "team_id", "period"),
    "GLOBAL_OTHER_STATS": ("game_id", "team_id"),
    "GLOBAL_OFFICIAL": ("official_id",),
    "GLOBAL_GAME_OFFICIAL": ("game_id", "official_id"),
    "GLOBAL_DRAFT_COMBINE": ("player_id", "year"),
    "GLOBAL_DRAFT_HISTORY": ("player_id", "draft_year"),
}


def _default_sql_path() -> str:
    return os.path.join("src", "etl", "transform_gav.sql")


def _split_sql_statements(raw: str) -> List[str]:
    """Split SQL into executable statements.

    - Strip single-line comments starting with '--' (remove the rest of the line)
    - Then split by ';'
    - Trim and drop empty fragments
    """
    cleaned_lines: List[str] = []
    for line in raw.splitlines():
        # Remove content after '--' (SQL single-line comment)
        idx = line.find("--")
        if idx != -1:
            line = line[:idx]
        cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines)

    frags = []
    for chunk in cleaned.split(";"):
        stmt = chunk.strip()
        if stmt:
            frags.append(stmt)
    return frags


def execute_gav_sql(spark, sql_path: Optional[str] = None) -> List[str]:
    """Execute the SQL statements defining the GLOBAL_* views.

    Returns the list of executed SQL statements (trimmed), mainly for debugging.
    """

    path = sql_path or _default_sql_path()
    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read()

    executed: List[str] = []
    for idx, statement in enumerate(_split_sql_statements(raw), start=1):
        try:
            spark.sql(statement)
            executed.append(statement)
        except Exception:
            # Print a short preview to aid debugging, then re-raise
            preview = statement.replace("\n", " ")
            if len(preview) > 160:
                preview = preview[:157] + "..."
            print(f"[GAV] SQL error on statement #{idx}: {preview}")
            raise

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
        if null_columns:
            print(f"[WARN] {view} has NullType columns coerced to string: {', '.join(null_columns)}")
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
