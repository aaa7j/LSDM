# scripts/run_search.py
import os, sys, argparse, subprocess, pathlib
from pyspark.sql import SparkSession

# ---------------- ENV Windows friendly ----------------
JAVA_HOME = r"C:\Users\angel\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

TMP_DIR = r"C:\spark-tmp"
os.makedirs(TMP_DIR, exist_ok=True)
os.environ["TMP"] = TMP_DIR
os.environ["TEMP"] = TMP_DIR
os.environ["SPARK_LOCAL_DIRS"] = TMP_DIR
os.environ["JAVA_TOOL_OPTIONS"] = f"-Djava.io.tmpdir={TMP_DIR} -Dfile.encoding=UTF-8"

# Spark conf basic
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    f'--conf spark.local.dir={TMP_DIR} '
    f'--conf spark.sql.session.timeZone=UTC '
    'pyspark-shell'
)

# Make project importable as package (src/)
ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.search.engine import run_query  # noqa: E402


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("LSDM-Search")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def ensure_global_views_from_warehouse(spark: SparkSession, warehouse: str):
    """
    Registra le GLOBAL_* come tabelle temporanee leggendo dal tuo Parquet 'warehouse'
    creato dallo script run_gav.py (salvataggio Parquet per ogni GLOBAL_*).
    """
    base = pathlib.Path(warehouse)
    expected = {
        "GLOBAL_PLAYER": "global_player",
        "GLOBAL_TEAM": "global_team",
        "GLOBAL_GAME": "global_game",
        "GLOBAL_PLAY_BY_PLAY": "global_play_by_play",
        "GLOBAL_LINE_SCORE": "global_line_score",
        "GLOBAL_OTHER_STATS": "global_other_stats",
        "GLOBAL_OFFICIAL": "global_official",
        "GLOBAL_GAME_OFFICIAL": "global_game_official",
        "GLOBAL_DRAFT_COMBINE": "global_draft_combine",
        "GLOBAL_DRAFT_HISTORY": "global_draft_history",
    }

    missing = []
    for view, folder in expected.items():
        path = base / folder
        if not path.exists():
            missing.append(str(path))
            continue
        df = spark.read.parquet(str(path))
        df.createOrReplaceTempView(view)

    if missing:
        raise SystemExit(f"Mancano alcune cartelle Parquet nel warehouse:\n" + "\n".join(missing))
    print("[OK] Tutte le GLOBAL_* views sono registrate dal warehouse.")


def main():
    parser = argparse.ArgumentParser(description="Global search stile Basketball Reference")
    parser.add_argument("--warehouse", default="warehouse", help="Cartella Parquet con le GLOBAL_* (default: warehouse)")
    parser.add_argument("--limit", type=int, default=10, help="Max risultati per categoria")
    parser.add_argument("q", nargs="+", help="Query libera (es: 'michael jordan' o 'lakers 2010')")
    args = parser.parse_args()
    query = " ".join(args.q)

    # debug prints brevi
    print(">>> Python exe:", sys.executable)
    print(">>> JAVA_HOME :", os.environ.get("JAVA_HOME"))
    try:
        out = subprocess.check_output(["java","-version"], stderr=subprocess.STDOUT).decode(errors="ignore")
        print(">>> java -version:\n", out)
    except Exception:
        pass

    spark = get_spark()
    ensure_global_views_from_warehouse(spark, args.warehouse)

    rows, meta = run_query(spark, query, top_n=args.limit)

    # Output leggibile
    print("\n=== SEARCH RESULT ===")
    print(f"Query: {meta.get('query')}")
    print(f"Note:  {meta.get('hints')}")
    print(f"Totale risultati (aggregati): {meta.get('returned')}\n")

    # mostra raggruppati per tipo (player/team/game)
    by_type = {"player": [], "team": [], "game": []}
    for r in rows:
        by_type.setdefault(r["_entity"], []).append(r)

    def show_block(title, lst):
        if not lst:
            return
        print(f"--- {title} ---")
        for x in lst:
            if x["_entity"] == "player":
                print(f"[Player] {x['_title']}   (id={x['_id']})   {x['_subtitle'] or ''}")
            elif x["_entity"] == "team":
                sub = f" — {x['_subtitle']}" if x["_subtitle"] else ""
                print(f"[Team]   {x['_title']}{sub}   (id={x['_id']})")
            else:
                extr = f"  [{x['_extra']}]" if x["_extra"] else ""
                print(f"[Game]   {x['_title']}  {x['_subtitle']}{extr}   (id={x['_id']})")
        print()

    show_block("PLAYERS", by_type["player"])
    show_block("TEAMS",   by_type["team"])
    show_block("GAMES",   by_type["game"])

    spark.stop()


if __name__ == "__main__":
    main()
