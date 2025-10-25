import os
import json
import psycopg2

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DBCFG_PATH = os.path.join(ROOT, 'db', 'config.json')


def load_cfg():
    with open(DBCFG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def main():
    cfg = load_cfg()
    conn = psycopg2.connect(
        host=cfg['host'], port=cfg['port'], dbname=cfg['dbname'], user=cfg['user'], password=cfg['password']
    )
    try:
        with conn.cursor() as cur:
            # Drop tables we are moving to Mongo to avoid duplicates
            for tbl in ['staging.player_totals', 'staging.team_abbrev', 'staging.player_bio', 'staging.team_details']:
                print(f"[DROP] {tbl}")
                cur.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.commit()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
