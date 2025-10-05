from fastapi import FastAPI, Query
from pyspark.sql import SparkSession
from src.search.engine import run_query
import os

app = FastAPI(title="NBA Data Search API")

# === Setup Spark una sola volta ===
def get_spark():
    spark = (
        SparkSession.builder
        .appName("NBA Search API")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    warehouse_path = os.path.join(os.getcwd(), "warehouse")
    tables = [t.name for t in spark.catalog.listTables()]
    if not tables:
        print("[INIT] Registering GLOBAL_* views from warehouse ...")
        spark.read.parquet(os.path.join(warehouse_path, "global_player")).createOrReplaceTempView("GLOBAL_PLAYER")
        spark.read.parquet(os.path.join(warehouse_path, "global_team")).createOrReplaceTempView("GLOBAL_TEAM")
        spark.read.parquet(os.path.join(warehouse_path, "global_game")).createOrReplaceTempView("GLOBAL_GAME")
    print("[OK] Spark ready.")
    return spark


spark = get_spark()


# === Endpoint di base ===
@app.get("/health")
def health():
    return {"status": "ok"}


# === Endpoint di ricerca ===
@app.get("/search")
def search(q: str = Query(..., description="Testo da cercare"), limit: int = 10):
    """
    Esegue una ricerca full-text (tipo 'Michael Jordan')
    su tutte le tabelle GLOBAL_* nel warehouse.
    """
    try:
        rows, meta = run_query(spark, q, top_n=limit)
        return {"query": q, "meta": meta, "results": rows}
    except Exception as e:
        return {"error": str(e)}
