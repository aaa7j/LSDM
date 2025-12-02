import csv
import json
from pathlib import Path

import psycopg2


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    config_path = ROOT / "db" / "config.json"
    data_path = ROOT / "data" / "Player Play By Play.csv"
    tmp_path = ROOT / "data" / "_player_play_by_play_staging.csv"

    if not config_path.exists():
        raise SystemExit(f"Missing DB config: {config_path}")
    if not data_path.exists():
        raise SystemExit(f"Missing CSV file: {data_path}")

    with config_path.open("r", encoding="utf-8") as f:
        cfg = json.load(f)

    print(f"[prep] Building trimmed CSV at {tmp_path} ...")
    with data_path.open("r", encoding="utf-8", newline="") as src, tmp_path.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        fieldnames = ["season", "lg", "player", "player_id", "age", "team", "pos", "g", "gs", "mp"]
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        rows_in = 0
        for row in reader:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
            rows_in += 1
    print(f"[prep] Trimmed rows: {rows_in}")

    conn = psycopg2.connect(
        host=cfg.get("host", "localhost"),
        port=cfg.get("port", 5432),
        dbname=cfg.get("dbname", "lsdm"),
        user=cfg.get("user", "postgres"),
        password=cfg.get("password", "postgres"),
    )

    try:
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute("SELECT to_regclass('staging.player_play_by_play')")
        exists = cur.fetchone()[0] is not None
        if not exists:
            raise SystemExit(
                "Table staging.player_play_by_play does not exist. "
                "Run db/02_staging_tables.sql first."
            )

        print("[load] Truncating staging.player_play_by_play ...")
        cur.execute("TRUNCATE TABLE staging.player_play_by_play")

        copy_sql = """
COPY staging.player_play_by_play (
  season, lg, player, player_id, age, team, pos, g, gs, mp
)
FROM STDIN WITH (FORMAT CSV, HEADER TRUE)
"""
        print(f"[load] Loading from {tmp_path} ...")
        with tmp_path.open("r", encoding="utf-8") as f_csv:
            cur.copy_expert(copy_sql, f_csv)

        cur.execute("SELECT COUNT(*) FROM staging.player_play_by_play")
        loaded = cur.fetchone()[0]
        print(f"[load] Done. Rows in staging.player_play_by_play: {loaded}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

