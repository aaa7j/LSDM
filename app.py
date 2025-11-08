"""Streamlit dashboard for the Basketball Analytics project."""

from __future__ import annotations

import os
import pathlib
import streamlit as st

# Optional ETL imports (fallbacks provided below if missing)
try:
    from src.etl.read_sources import get_spark as _get_spark, register_sources as _register_sources  # type: ignore
    from src.etl.gav import execute_gav_sql as _execute_gav_sql  # type: ignore
    from src.etl.warehouse import register_from_warehouse as _register_from_warehouse  # type: ignore
except Exception:
    _get_spark = None
    _register_sources = None
    _execute_gav_sql = None
    _register_from_warehouse = None

# Analytics (present in repo)
from src.analytics.player_metrics import player_scoring_summary
from src.analytics.game_prediction import build_game_feature_frame, train_outcome_model


st.set_page_config(page_title="Basketball Analytics", layout="wide")


# ---------------------------- Spark helpers ---------------------------- #
def _ensure_windows_spark_env(java_home_override: str | None = None) -> None:
    """
    Harden Windows environment for PySpark:
    - Ensure ASCII-only temp dirs
    - Optionally set JAVA_HOME (fallback to Adoptium path on this machine)
    - Provide minimal submit args
    """
    java_home = (
        java_home_override
        or os.environ.get("JAVA_HOME")
        or r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
    )
    if java_home and os.path.isdir(java_home):
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")
    tmp_dir = os.environ.get("SPARK_LOCAL_DIRS", r"C:\spark-tmp")
    try:
        os.makedirs(tmp_dir, exist_ok=True)
    except Exception:
        # Last-resort fallback if C:\ not writable
        tmp_dir = str(pathlib.Path.cwd() / "spark-tmp")
        os.makedirs(tmp_dir, exist_ok=True)
    os.environ.setdefault("TMP", tmp_dir)
    os.environ.setdefault("TEMP", tmp_dir)
    os.environ.setdefault("SPARK_LOCAL_DIRS", tmp_dir)
    os.environ.setdefault("JAVA_TOOL_OPTIONS", f"-Djava.io.tmpdir={tmp_dir} -Dfile.encoding=UTF-8")
    os.environ.setdefault(
        "PYSPARK_SUBMIT_ARGS",
        f"--conf spark.local.dir={tmp_dir} --conf spark.sql.session.timeZone=UTC pyspark-shell",
    )


def _fallback_get_spark(app_name: str):
    from pyspark.sql import SparkSession  # lazy import

    _ensure_windows_spark_env()
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _register_from_warehouse_fallback(spark, base_dir: str = "warehouse") -> list[str]:
    """
    Register each subfolder in a Parquet warehouse as a temp view:
      warehouse/global_player -> GLOBAL_PLAYER, etc.
    Returns the list of registered view names.
    """
    base = pathlib.Path(base_dir)
    if not base.exists():
        return []
    views = []
    for child in sorted(base.iterdir()):
        if child.is_dir():
            name = child.name.upper()
            try:
                df = spark.read.parquet(child.as_uri())
                df.createOrReplaceTempView(name)
                views.append(name)
            except Exception:
                # Skip non-parquet or unreadable folders
                continue
    return views


def _has_table(spark, name: str) -> bool:
    try:
        spark.table(name).limit(1).collect()
        return True
    except Exception:
        return False


# ---------------------------- App wiring ---------------------------- #
def init_session(use_warehouse: bool, base_dir: str, warehouse_dir: str):
    # Prefer project ETL if available; otherwise fallback to direct Parquet registration
    spark = (
        _get_spark("BasketballAnalytics-Dashboard")
        if _get_spark
        else _fallback_get_spark("BasketballAnalytics-Dashboard")
    )
    if use_warehouse:
        if _register_from_warehouse:
            _register_from_warehouse(spark, base_dir=warehouse_dir)
        else:
            _register_from_warehouse_fallback(spark, base_dir=warehouse_dir)
    else:
        if _register_sources and _execute_gav_sql:
            _register_sources(spark, base=base_dir)
            _execute_gav_sql(spark)
        else:
            # CSV path requires ETL modules; fall back to warehouse if present
            _register_from_warehouse_fallback(spark, base_dir=warehouse_dir)
    return spark


def main():
    st.title("Basketball Analytics Dashboard")
    st.write("Explore integrated basketball data powered by the GAV schema and PySpark analytics.")

    st.sidebar.subheader("Data source")
    use_wh = st.sidebar.checkbox("Use Parquet warehouse (recommended)", value=True)
    warehouse_dir = st.sidebar.text_input("Warehouse directory", value="warehouse")
    base_dir = st.sidebar.text_input("CSV base directory (fallback)", value="data")

    if st.sidebar.button("Load data"):
        with st.spinner("Initializing Spark session and loading sources..."):
            spark = init_session(use_wh, base_dir, warehouse_dir)
            st.session_state["spark"] = spark
            st.session_state["base_dir"] = base_dir
            st.session_state["warehouse_dir"] = warehouse_dir
        st.sidebar.success("Data loaded")

    if "spark" not in st.session_state:
        st.info("Use the sidebar to load data (warehouse recommended).")
        return

    spark = st.session_state["spark"]

    # Validate required views and guide the user
    missing = []
    if not _has_table(spark, "GLOBAL_PLAYER"):
        missing.append("GLOBAL_PLAYER")
    if not (_has_table(spark, "GLOBAL_SCORING_EVENTS") or _has_table(spark, "GLOBAL_PLAY_BY_PLAY")):
        missing.append("GLOBAL_SCORING_EVENTS or GLOBAL_PLAY_BY_PLAY")
    if not _has_table(spark, "GLOBAL_GAME"):
        missing.append("GLOBAL_GAME")

    model_missing_other = not _has_table(spark, "GLOBAL_OTHER_STATS")
    if missing:
        st.warning(
            f"Missing required views: {', '.join(missing)}. Ensure your warehouse contains these Parquet folders."
        )

    st.subheader("Top Scorers")
    top_n = st.slider("Number of players", min_value=5, max_value=50, value=10, step=5)
    with st.spinner("Computing scoring leaders..."):
        try:
            leaders = player_scoring_summary(spark).limit(top_n)
            st.dataframe(leaders.toPandas())
        except Exception as e:
            st.error(f"Failed to compute player metrics: {e}")

    st.subheader("Game Outcome Model")
    if model_missing_other:
        st.info("GLOBAL_OTHER_STATS not available — training disabled.")
    else:
        if st.button("Train / refresh model"):
            with st.spinner("Engineering features and training logistic regression..."):
                try:
                    features = build_game_feature_frame(spark)
                    if features.count() == 0:
                        st.warning("No features available. Ensure GLOBAL_OTHER_STATS is populated.")
                    else:
                        _, metrics, predictions = train_outcome_model(features)
                        st.write("Metrics", metrics)
                        preview = (
                            predictions.select(
                                "game_id",
                                "home_team_id",
                                "away_team_id",
                                "label",
                                "prediction",
                                "probability",
                            )
                            .limit(20)
                            .toPandas()
                        )
                        st.dataframe(preview)
                except Exception as e:
                    st.error(f"Model training failed: {e}")


if __name__ == "__main__":
    main()
