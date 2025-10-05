"""
FastAPI search service that keeps a single long‑lived PySpark session.

Notes
- Spark is created on FastAPI startup and stopped on shutdown.
- GLOBAL_* tables are loaded from the local Parquet warehouse and cached.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pyspark.sql import SparkSession

from src.search.engine import run_query


WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "warehouse")

GLOBAL_PARQUET = {
    # Only what search needs for now (faster startup)
    "GLOBAL_PLAYER": "global_player",
    "GLOBAL_TEAM": "global_team",
    "GLOBAL_GAME": "global_game",
}


def _mk_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("LSDM-SearchService")
        .master("local[*]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
    )
    # Respect SPARK_LOCAL_DIRS if provided (Windows temp location)
    local_dirs = os.environ.get("SPARK_LOCAL_DIRS")
    if local_dirs:
        builder = builder.config("spark.local.dir", local_dirs)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _register_views_from_warehouse(spark: SparkSession, base: str = WAREHOUSE_DIR):
    # Load Parquet folders and register temp views
    print(f"[INIT] Registering GLOBAL_* from: {os.path.abspath(base)}")
    for view_name, sub in GLOBAL_PARQUET.items():
        path = os.path.join(base, sub)
        if not os.path.isdir(path):
            print(f"[WARN] Missing path for {view_name}: {path}")
            continue
        print(f"[INIT] -> {view_name} from {path}")
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(view_name)
    # Cache for faster responses
    for view_name in GLOBAL_PARQUET.keys():
        if view_name in [t.name for t in spark.catalog.listTables()]:
            spark.catalog.cacheTable(view_name)
    print("[OK] GLOBAL_* views registered and cached (where available)")


app = FastAPI(title="LSDM Search Service", version="1.0.0")


# Global Spark handle (initialized on startup)
spark: Optional[SparkSession] = None


@app.on_event("startup")
def _on_startup():
    global spark
    spark = _mk_spark()
    _register_views_from_warehouse(spark)
    # Optional warmup to trigger JVM init and cache population
    try:
        spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 1").collect()
        spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 1").collect()
        spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 1").collect()
    except Exception as e:
        print(f"[WARN] Warmup failed: {e}")


@app.on_event("shutdown")
def _on_shutdown():
    global spark
    try:
        if spark is not None:
            spark.stop()
    finally:
        spark = None


class SearchResponse(BaseModel):
    total: int
    items: List[Dict]
    meta: Dict


@app.get("/health")
def health():
    if spark is None:
        return {"status": "starting"}
    cnt = spark.sql("SELECT COUNT(*) AS c FROM GLOBAL_PLAYER").collect()[0]["c"]
    return {"status": "ok", "players": cnt}


@app.get("/warmup")
def warmup():
    if spark is None:
        return {"status": "starting"}
    spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 1").collect()
    return {"status": "warmed"}


@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., description="Search text, e.g. 'michael jordan' or 'lakers 2010'"),
    limit: int = Query(200, ge=1, le=2000, description="candidates per entity"),
):
    if spark is None:
        return {"total": 0, "items": [], "meta": {"query": q, "notes": "service starting"}}
    rows, meta = run_query(spark, q.strip(), top_n=limit)
    return {"total": len(rows), "items": rows, "meta": meta}
