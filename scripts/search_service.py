# scripts/search_service.py
from __future__ import annotations
import os
from typing import List, Dict, Optional

from fastapi import FastAPI, Query
from pydantic import BaseModel

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

# riuso del motore di ricerca già fatto
from src.search.engine import run_query

WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "warehouse")

GLOBAL_PARQUET = {
    "GLOBAL_PLAYER":        "global_player",
    "GLOBAL_TEAM":          "global_team",
    "GLOBAL_GAME":          "global_game",
    "GLOBAL_PLAY_BY_PLAY":  "global_play_by_play",
    "GLOBAL_LINE_SCORE":    "global_line_score",
    "GLOBAL_OTHER_STATS":   "global_other_stats",
    "GLOBAL_OFFICIAL":      "global_official",
    "GLOBAL_GAME_OFFICIAL": "global_game_official",
    "GLOBAL_DRAFT_COMBINE": "global_draft_combine",
    "GLOBAL_DRAFT_HISTORY": "global_draft_history",
}

def _mk_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("LSDM-SearchService")
        # tuning leggero per macchina locale
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # evita troppe stampe
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def _register_views_from_warehouse(spark: SparkSession, base: str = WAREHOUSE_DIR):
    # Carica i parquet salvati e crea le temp views GLOBAL_*
    for view_name, sub in GLOBAL_PARQUET.items():
        path = os.path.join(base, sub)
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(view_name)
    # Cache per risposte più veloci
    for view_name in GLOBAL_PARQUET.keys():
        spark.catalog.cacheTable(view_name)

# --- bootstrap all'avvio del processo ---
spark = _mk_spark()
_register_views_from_warehouse(spark)

# FastAPI app
app = FastAPI(title="LSDM Search Service", version="1.0.0")

class SearchResponse(BaseModel):
    total: int
    items: List[Dict]
    meta: Dict

@app.get("/health")
def health():
    # mini query per verificare le viste
    cnt = spark.sql("SELECT COUNT(*) AS c FROM GLOBAL_PLAYER").collect()[0]["c"]
    return {"status": "ok", "players": cnt}

@app.get("/warmup")
def warmup():
    # riscalda cache e JVM
    spark.sql("SELECT * FROM GLOBAL_PLAYER LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_TEAM   LIMIT 1").collect()
    spark.sql("SELECT * FROM GLOBAL_GAME   LIMIT 1").collect()
    return {"status": "warmed"}

@app.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(..., description="Testo della ricerca, es. 'michael jordan' o 'lakers 2010'"),
    limit: int = Query(200, ge=1, le=2000, description="candidati per entità"),
):
    rows, meta = run_query(spark, q.strip(), top_n=limit)
    return {"total": len(rows), "items": rows, "meta": meta}
