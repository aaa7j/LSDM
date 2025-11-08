import json
import os
import subprocess
import sys
import time

import numpy as np
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

    # Extended comparative statistics
    st.subheader("Extended Stats")
    try:
        # Enrich dataframe with numeric cols and run index
        df_e = df_norm.copy()
        for c in ("wall_ms", "rows", "topn"):
            if c in df_e.columns:
                df_e[c] = pd.to_numeric(df_e[c], errors="coerce")
        df_e = df_e.reset_index(drop=True)
        grp_keys = [k for k in ["tool", "query", "topn"] if k in df_e.columns]
        if grp_keys:
            df_e["run_idx"] = df_e.groupby(grp_keys).cumcount() + 1

        # Lightweight filters just for extended stats
        cols = st.columns(3)
        with cols[0]:
            q_opts = sorted(df_e["query"].dropna().unique().tolist()) if "query" in df_e.columns else []
            sel_q = st.multiselect("Queries", options=q_opts, default=q_opts)
        with cols[1]:
            t_opts = sorted(df_e["tool"].dropna().unique().tolist()) if "tool" in df_e.columns else []
            sel_t = st.multiselect("Tools", options=t_opts, default=t_opts)
        with cols[2]:
            topn_opts = sorted(df_e["topn"].dropna().unique().astype(int).tolist()) if "topn" in df_e.columns else []
            sel_topn = st.multiselect("TopN (Q3)", options=topn_opts, default=topn_opts) if topn_opts else []

        df_f = df_e.copy()
        if sel_q:
            df_f = df_f[df_f["query"].isin(sel_q)]
        if sel_t:
            df_f = df_f[df_f["tool"].isin(sel_t)]
        if sel_topn:
            df_f = df_f[df_f["topn"].isin(sel_topn)]

        # Summary by tool/query
        if not df_f.empty:
            g = df_f.groupby(["tool", "query"], dropna=False)
            summary_ext = (
                g["wall_ms"].agg(
                    runs="count",
                    mean_ms="mean",
                    median_ms="median",
                    min_ms="min",
                    p95_ms=lambda x: np.nanpercentile(x, 95),
                    max_ms="max",
                    std_ms="std",
                ).reset_index()
            )
            summary_ext["cv"] = (summary_ext["std_ms"] / summary_ext["mean_ms"]).replace([np.inf, -np.inf], np.nan)
            if "rows" in df_f.columns and df_f["rows"].notna().any():
                rows_mean = g["rows"].mean().reset_index(name="rows_mean")
                summary_ext = summary_ext.merge(rows_mean, on=["tool", "query"], how="left")
                summary_ext["throughput_rows_per_s"] = summary_ext.apply(
                    lambda r: (r["rows_mean"] / (r["mean_ms"] / 1000.0)) if pd.notnull(r.get("rows_mean")) and r["mean_ms"] > 0 else np.nan,
                    axis=1,
                )

            st.dataframe(summary_ext)

            # Speedup tables (mean/median)
            piv_mean = summary_ext.pivot(index="query", columns="tool", values="mean_ms").dropna(how="any")
            piv_median = summary_ext.pivot(index="query", columns="tool", values="median_ms").dropna(how="any")
            if {"hadoop", "pyspark"}.issubset(piv_mean.columns):
                sp_mean = (piv_mean["hadoop"] / piv_mean["pyspark"]).reset_index(name="speedup_vs_hadoop")
                st.markdown("Speedup vs Hadoop (mean)")
                st.dataframe(sp_mean)
            if {"hadoop", "pyspark"}.issubset(piv_median.columns):
                sp_median = (piv_median["hadoop"] / piv_median["pyspark"]).reset_index(name="speedup_vs_hadoop_median")
                st.markdown("Speedup vs Hadoop (median)")
                st.dataframe(sp_median)

            try:
                import altair as alt

                # Boxplots by tool per query
                box = (
                    alt.Chart(df_f)
                    .mark_boxplot()
                    .encode(x="tool:N", y="wall_ms:Q", column="query:N", color="tool:N")
                )
                st.altair_chart(box, use_container_width=True)

                # Run trend per query/tool
                if "run_idx" in df_f.columns:
                    trend = (
                        alt.Chart(df_f)
                        .mark_line(point=True)
                        .encode(x="run_idx:Q", y="wall_ms:Q", color="tool:N", facet="query:N")
                    )
                    st.altair_chart(trend, use_container_width=True)

                # Throughput scatter if rows present
                if "rows" in df_f.columns and df_f["rows"].notna().any():
                    df_thr = df_f[df_f["rows"].notna()].copy()
                    df_thr["thr_rows_s"] = df_thr["rows"] / (df_thr["wall_ms"] / 1000.0)
                    scatter = (
                        alt.Chart(df_thr)
                        .mark_circle(size=60, opacity=0.7)
                        .encode(x="wall_ms:Q", y="thr_rows_s:Q", color="tool:N", shape="query:N")
                    )
                    st.altair_chart(scatter, use_container_width=True)
            except Exception:
                pass

            # Overall weighted speedup (sum of means)
            try:
                if {"hadoop", "pyspark"}.issubset(piv_mean.columns):
                    total_h = piv_mean["hadoop"].sum()
                    total_p = piv_mean["pyspark"].sum()
                    if total_p > 0:
                        st.info(f"Overall weighted speedup: {total_h/total_p:.2f}x")
            except Exception:
                pass
    except Exception as e:
        st.warning(f"Extended stats unavailable: {e}")

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
