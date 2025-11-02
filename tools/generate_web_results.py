#!/usr/bin/env python3
"""Convert TSV outputs from Hadoop and PySpark runs into JSON for the UI.

Produces web/data/hadoop.json and web/data/pyspark.json (if corresponding TSVs exist).
"""
import csv
import json
import os
from glob import glob
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
OUT_DIR = os.path.join(ROOT, "web", "data")
os.makedirs(OUT_DIR, exist_ok=True)


def tsv_to_records(path, headers=None):
    recs = []
    # read bytes and detect encoding (handle UTF-8, UTF-8 BOM, UTF-16 LE/BE)
    with open(path, "rb") as bf:
        data = bf.read()
    enc = "utf-8"
    if data.startswith(b"\xff\xfe") or data.startswith(b"\xfe\xff"):
        enc = "utf-16"
    elif data.startswith(b"\xef\xbb\xbf"):
        enc = "utf-8-sig"
    # try decode
    try:
        text = data.decode(enc)
    except Exception:
        try:
            text = data.decode("utf-8", errors="replace")
        except Exception:
            text = data.decode("latin-1", errors="replace")
    # split into lines to feed csv.reader
    lines = text.splitlines()
    reader = csv.reader(lines, delimiter="\t")
    for row in reader:
        if not row:
            continue
        if headers and len(row) < len(headers):
            # pad
            row += [""] * (len(headers) - len(row))
        if headers:
            recs.append(dict(zip(headers, row)))
        else:
            recs.append(row)
    return recs


def find_latest_tsv(prefix):
    # look for outputs/{prefix}*.tsv or outputs/hadoop/{prefix}.tsv
    candidates = glob(os.path.join(ROOT, "outputs", "**", f"{prefix}*.tsv"), recursive=True)
    if not candidates:
        return None
    # pick largest file as heuristic for real output
    candidates.sort(key=lambda p: os.path.getsize(p), reverse=True)
    return candidates[0]


def produce_hadoop_json():
    # expected fields
    q1_path = find_latest_tsv("q1")
    q2_path = find_latest_tsv("q2")
    q3_path = find_latest_tsv("q3")
    out = {"q1": [], "q2": [], "q3": []}
    if q1_path:
        # season, team_id, total_points, avg_points, games
        out["q1"] = tsv_to_records(
            q1_path,
            headers=["season", "team_id", "total_points", "avg_points", "games"],
        )
    if q2_path:
        out["q2"] = tsv_to_records(q2_path, headers=["season", "team_id", "team_name", "total_points", "avg_points", "games"])
    if q3_path:
        # reducer outputs: team_id, season, game_id, points
        out["q3"] = tsv_to_records(q3_path, headers=["team_id", "season", "game_id", "points"])
    with open(os.path.join(OUT_DIR, "hadoop.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print("wrote web/data/hadoop.json")


def produce_pyspark_json():
    # heuristics: look for outputs/pyspark/*.tsv or outputs/*pyspark*.tsv
    candidates = glob(os.path.join(ROOT, "outputs", "**", "*pyspark*.tsv"), recursive=True)
    out = {}
    if candidates:
        # if there are multiple, convert each
        for p in candidates:
            name = os.path.splitext(os.path.basename(p))[0]
            out[name] = tsv_to_records(p)
        with open(os.path.join(OUT_DIR, "pyspark.json"), "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print("wrote web/data/pyspark.json")
        return

    # fallback: maybe pyspark outputs are in reports/ or models/
    print("no pyspark TSVs found; skipping pyspark.json")
    # also try to load outputs/spark parquet directories (if present) using pyspark
    spark_dir = os.path.join(ROOT, 'outputs', 'spark')
    try:
        from pyspark.sql import SparkSession
    except Exception:
        SparkSession = None

    if os.path.isdir(spark_dir) and SparkSession is not None:
        spark = SparkSession.builder.master('local[1]').appName('gen_web_results').getOrCreate()
        for sub in os.listdir(spark_dir):
            subp = os.path.join(spark_dir, sub)
            if not os.path.isdir(subp):
                continue
            # attempt to read as parquet
            try:
                df = spark.read.parquet(subp)
                # convert to records (limit to 10000 rows to be safe)
                pdf = df.limit(10000).toPandas()
                records = pdf.where(pd.notnull(pdf), None).to_dict(orient='records')
                out[sub] = records
            except Exception:
                # ignore read failures
                pass
        try:
            spark.stop()
        except Exception:
            pass

    with open(os.path.join(OUT_DIR, 'pyspark.json'), 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)


def main():
    produce_hadoop_json()
    produce_pyspark_json()


if __name__ == "__main__":
    main()
