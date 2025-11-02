import json
import os
import subprocess
import sys
import time

import pandas as pd
import streamlit as st


RESULTS_FILE = "results/pyspark_vs_hadoop.jsonl"


def load_results() -> pd.DataFrame:
    if not os.path.exists(RESULTS_FILE):
        return pd.DataFrame(columns=["tool", "query", "wall_ms", "rows", "topn"])  # type: ignore
    rows = []
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    return pd.DataFrame(rows)


def run_compare(runs: int, topn: int):
    cmd = [
        sys.executable,
        "scripts/run_bigdata_compare.py",
        "--runs",
        str(runs),
        "--topn",
        str(topn),
    ]
    st.info("Running comparison; this can take a minute…")
    t0 = time.time()
    res = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.time() - t0
    if res.returncode != 0:
        st.error("Execution failed")
        st.code(res.stdout)
        st.code(res.stderr)
    else:
        st.success(f"Done in {dt:.1f}s")


st.set_page_config(page_title="PySpark vs Hadoop Streaming", layout="wide")
st.title("PySpark vs Hadoop Streaming (and Trino)")
st.write("Run and visualize side-by-side performance across workloads.")

with st.sidebar:
    st.subheader("Run")
    runs = st.number_input("Repeat runs", min_value=1, max_value=5, value=1)
    topn = st.number_input("Top N (Q3)", min_value=1, max_value=20, value=3)
    if st.button("Run comparison"):
        run_compare(int(runs), int(topn))
    st.subheader("Trino (optional)")
    host = st.text_input("Trino host", value="localhost")
    port = st.number_input("Trino port", min_value=1, max_value=65535, value=8080)
    user = st.text_input("Trino user", value="lsdm")
    if st.button("Run Trino queries"):
        cmd = [
            sys.executable,
            "scripts/run_trino_bigdata.py",
            "--host",
            host,
            "--port",
            str(int(port)),
            "--user",
            user,
            "--runs",
            str(int(runs)),
            "--topn",
            str(int(topn)),
        ]
        st.info("Running Trino queries…")
        t0 = time.time()
        res = subprocess.run(cmd, capture_output=True, text=True)
        dt = time.time() - t0
        if res.returncode != 0:
            st.error("Trino execution failed")
            st.code(res.stdout)
            st.code(res.stderr)
        else:
            st.success(f"Trino done in {dt:.1f}s")

df = load_results()
if df.empty:
    st.info("No results yet. Click 'Run comparison' to generate.")
else:
    st.subheader("Results")
    # Normalize tool names so we can compare across scripts
    df_norm = df.copy()
    df_norm["tool"] = df_norm["tool"].replace({
        "hadoop_streaming": "hadoop",
        "pyspark_cluster": "pyspark",
    })
    # Aggregate average by tool/query
    summary = (
        df_norm.groupby(["tool", "query"], dropna=False)["wall_ms"].mean().reset_index()
    )
    piv = summary.pivot(index="query", columns="tool", values="wall_ms").reset_index()
    st.dataframe(piv)
    try:
        import altair as alt

        # Bars per tool per query
        melt_cols = [c for c in piv.columns if c not in {"query"}]
        chart = (
            alt.Chart(piv)
            .transform_fold(melt_cols, as_=["tool", "wall_ms"])
            .mark_bar()
            .encode(x="query:N", y="wall_ms:Q", color="tool:N")
        )
        st.altair_chart(chart, use_container_width=True)

        # Speedup vs Hadoop (where both present)
        if "hadoop" in melt_cols and "pyspark" in melt_cols:
            piv["speedup_vs_hadoop"] = piv["hadoop"] / piv["pyspark"]
            st.subheader("Speedup vs Hadoop (higher is better)")
            sp = alt.Chart(piv).mark_bar().encode(x="query:N", y="speedup_vs_hadoop:Q")
            st.altair_chart(sp, use_container_width=True)
            st.write(piv[["query", "speedup_vs_hadoop"]])
    except Exception:
        pass

    # Optional: Spark resource profile (if pyspark available)
    with st.expander("Spark resource profile", expanded=False):
        try:
            from pyspark.sql import SparkSession  # type: ignore

            spark = SparkSession.builder.appName("profile").getOrCreate()
            conf = {k: v for k, v in spark.sparkContext.getConf().getAll()}
            st.json(conf)
            try:
                spark.stop()
            except Exception:
                pass
        except Exception as e:
            st.info(f"PySpark not available for profiling: {e}")
