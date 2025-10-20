from pyspark.sql import SparkSession

def summarize_row_counts(spark: SparkSession, tables: list[str]) -> dict[str, int]:
    """
    Conta le righe delle viste passate senza usare spark.catalog.listTables()
    """
    counts: dict[str, int] = {}
    for t in tables:
        try:
            n = spark.sql(f"SELECT COUNT(*) AS c FROM {t}").collect()[0]["c"]
            counts[t] = int(n)
            print(f"[QC] {t:<24} -> {n:,}")
        except Exception as e:
            counts[t] = -1
            print(f"[QC] {t:<24} -> ERROR: {e}")
    return counts


def assert_primary_keys(spark: SparkSession, pk_map: dict[str, list[str]]) -> dict[str, bool]:
    """
    Verifica (best-effort) unicità chiavi primarie su viste logiche.
    Esegue un GROUP BY COUNT(*)>1.
    """
    results: dict[str, bool] = {}
    for table, keys in pk_map.items():
        if not keys:
            results[table] = True
            continue
        try:
            cols = ", ".join(keys)
            dup = spark.sql(
                f"""
                SELECT {cols}, COUNT(*) AS cnt
                FROM {table}
                GROUP BY {cols}
                HAVING COUNT(*) > 1
                """
            )
            has_dups = dup.limit(1).count() > 0
            results[table] = not has_dups
            status = "OK" if not has_dups else "DUPLICATES FOUND"
            print(f"[PK ] {table:<24} -> {status}")
        except Exception as e:
            results[table] = False
            print(f"[PK ] {table:<24} -> ERROR: {e}")
    return results
