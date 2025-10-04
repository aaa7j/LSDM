"""Streamlit dashboard for the Basketball Analytics project."""

import streamlit as st

from src.etl.read_sources import get_spark, register_sources
from src.etl.gav import execute_gav_sql
from src.analytics.player_metrics import player_scoring_summary
from src.analytics.game_prediction import build_game_feature_frame, train_outcome_model


st.set_page_config(page_title="Basketball Analytics", layout="wide")


def init_session(base_dir: str):
    spark = get_spark("BasketballAnalytics-Dashboard")
    register_sources(spark, base=base_dir)
    execute_gav_sql(spark)
    return spark


def main():
    st.title("Basketball Analytics Dashboard")
    st.write(
        "Explore integrated basketball data powered by the GAV schema and PySpark analytics."
    )

    base_dir = st.sidebar.text_input("CSV base directory", value="csv")

    if st.sidebar.button("Load data"):
        with st.spinner("Initializing Spark session and loading sources..."):
            spark = init_session(base_dir)
            st.session_state["spark"] = spark
            st.session_state["base_dir"] = base_dir
        st.sidebar.success("Data loaded")

    if "spark" not in st.session_state:
        st.info("Use the sidebar to load data from the Kaggle CSV directory.")
        return

    spark = st.session_state["spark"]

    st.subheader("Top Scorers")
    top_n = st.slider("Number of players", min_value=5, max_value=50, value=10, step=5)
    with st.spinner("Computing scoring leaders..."):
        leaders = player_scoring_summary(spark).limit(top_n)
        st.dataframe(leaders.toPandas())

    st.subheader("Game Outcome Model")
    if st.button("Train / refresh model"):
        with st.spinner("Engineering features and training logistic regression..."):
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


if __name__ == "__main__":
    main()
