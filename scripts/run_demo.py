import os
import argparse
import json
import psycopg2
import psycopg2.extras
import csv
import tempfile
import os
from contextlib import contextmanager

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DBCFG_PATH = os.path.join(ROOT, 'db', 'config.json')


def load_cfg():
    if os.path.exists(DBCFG_PATH):
        with open(DBCFG_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {
        'host': os.environ.get('PGHOST', 'localhost'),
        'port': int(os.environ.get('PGPORT', '5432')),
        'dbname': os.environ.get('PGDATABASE', 'lsdm'),
        'user': os.environ.get('PGUSER', 'postgres'),
        'password': os.environ.get('PGPASSWORD', 'postgres'),
    }


@contextmanager
def pg_connect(cfg):
    conn = psycopg2.connect(
        host=cfg['host'], port=cfg['port'], dbname=cfg['dbname'], user=cfg['user'], password=cfg['password']
    )
    try:
        yield conn
    finally:
        conn.close()


def run_sql(conn, path):
    print(f"[SQL] {os.path.relpath(path, ROOT)}")
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def _copy_with_null_token(conn, table, path, null_token):
    with open(path, 'r', encoding='utf-8', errors='ignore') as f, conn.cursor() as cur:
        cur.copy_expert(sql=f"COPY {table} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '{null_token}')", file=f)


def _sanitize_csv(in_path, null_tokens=("NA", "N/A", "NaN", "nan")):
    """Create a sanitized temp CSV where any cell equal to a null_token becomes empty.
    Returns temp file path.
    """
    fd, tmp_path = tempfile.mkstemp(prefix="sanitized_", suffix=".csv")
    os.close(fd)
    # Detect delimiter quickly
    with open(in_path, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
            delim = dialect.delimiter
        except Exception:
            delim = ','
        reader = csv.reader(f, delimiter=delim)
        with open(tmp_path, 'w', encoding='utf-8', newline='') as g:
            writer = csv.writer(g, delimiter=delim)
            for i, row in enumerate(reader):
                if i == 0:
                    writer.writerow(row)
                    continue
                cleaned = [
                    ('' if (c is not None and str(c).strip() in null_tokens) else c)
                    for c in row
                ]
                writer.writerow(cleaned)
    return tmp_path


def copy_csv(conn, table, path):
    print(f"[COPY] {table} <- {os.path.relpath(path, ROOT)}")
    try:
        # First try: tell Postgres to treat 'NA' as NULL
        _copy_with_null_token(conn, table, path, "NA")
        conn.commit()
        return
    except Exception as e:
        print(f"[RETRY] COPY failed with NULL 'NA': {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        print("[RETRY] Sanitizing CSV (NA/N/A/NaN -> empty) and retrying...")
        tmp = _sanitize_csv(path)
        try:
            _copy_with_null_token(conn, table, tmp, '')  # default NULL empty
            conn.commit()
        finally:
            try:
                os.remove(tmp)
            except Exception:
                pass


def load_jsonl_player_bio(conn, path, table, src_label):
    print(f"[JSONL] {table} <- {os.path.relpath(path, ROOT)}")
    with open(path, 'r', encoding='utf-8') as f, conn.cursor() as cur:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            cols = (
                rec.get('player_id'), rec.get('full_name'), rec.get('first_name'), rec.get('last_name'),
                rec.get('birthdate'), rec.get('school') or rec.get('college'), rec.get('country'),
                rec.get('height'), rec.get('weight'), rec.get('position'),
                rec.get('team_id'), rec.get('team_name'), rec.get('team_abbreviation'),
                rec.get('from_year'), rec.get('to_year'), rec.get('draft_year'), rec.get('draft_round'), rec.get('draft_number'),
                src_label
            )
            cur.execute(
                """
                INSERT INTO staging.player_bio (
                  player_id, full_name, first_name, last_name, birthdate, school, country,
                  height, weight, position, team_id, team_name, team_abbreviation,
                  from_year, to_year, draft_year, draft_round, draft_number, src
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                cols,
            )
    conn.commit()


def load_jsonl_team_details(conn, path, table, src_label):
    print(f"[JSONL] {table} <- {os.path.relpath(path, ROOT)}")
    with open(path, 'r', encoding='utf-8') as f, conn.cursor() as cur:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            meta = rec.get('meta') or {}
            arena = meta.get('arena') or {}
            cols = (
                rec.get('team_id'), rec.get('abbreviation'), rec.get('nickname'),
                meta.get('founded_year'), meta.get('city'), arena.get('name'), arena.get('capacity'),
                meta.get('owner'), meta.get('general_manager'), meta.get('head_coach'), meta.get('dleague_affiliation'),
                (rec.get('social') or {}).get('facebook'), (rec.get('social') or {}).get('instagram'), (rec.get('social') or {}).get('twitter'),
                src_label,
            )
            cur.execute(
                """
                INSERT INTO staging.team_details (
                  team_id, abbreviation, nickname, yearfounded, city, arena, arenacapacity,
                  owner, generalmanager, headcoach, dleagueaffiliation, facebook, instagram, twitter, src
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                cols,
            )
    conn.commit()


def load_jsonl_games(conn, path, table):
    print(f"[JSONL] {table} <- {os.path.relpath(path, ROOT)}")
    with open(path, 'r', encoding='utf-8') as f, conn.cursor() as cur:
        for line in f:
            if not line.strip():
                continue
            rec = json.loads(line)
            cols = (
                rec.get('game_id'), rec.get('date'), rec.get('season'),
                (rec.get('home') or {}).get('team_id'), (rec.get('home') or {}).get('team_city'), (rec.get('home') or {}).get('team_name'), (rec.get('home') or {}).get('score'),
                (rec.get('away') or {}).get('team_id'), (rec.get('away') or {}).get('team_city'), (rec.get('away') or {}).get('team_name'), (rec.get('away') or {}).get('score'),
                rec.get('winner'), rec.get('game_type'), rec.get('attendance'), rec.get('arena_id')
            )
            cur.execute(
                """
                INSERT INTO staging.games (
                  game_id, game_date, season_label, home_team_id, home_team_city, home_team_name, home_score,
                  away_team_id, away_team_city, away_team_name, away_score,
                  winner, game_type, attendance, arena_id
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                cols,
            )
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Load CSVs into Postgres staging; optionally JSONL too.")
    parser.add_argument("--csv-only", action="store_true", help="Load only CSV sources; skip JSONL loads")
    args = parser.parse_args()

    cfg = load_cfg()
    data_dir = os.path.join(ROOT, 'data')
    json_dir = os.path.join(data_dir, 'json')
    with pg_connect(cfg) as conn:
        # Schemas + staging DDL
        run_sql(conn, os.path.join(ROOT, 'db', '01_schemas.sql'))
        run_sql(conn, os.path.join(ROOT, 'db', '02_staging_tables.sql'))
        # Remove JSON placeholders: keep JSON only in Mongo
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DROP TABLE IF EXISTS staging.player_totals CASCADE;
                    DROP TABLE IF EXISTS staging.team_abbrev CASCADE;
                    DROP TABLE IF EXISTS staging.team_details CASCADE;
                    DROP TABLE IF EXISTS staging.player_bio CASCADE;
                    DROP TABLE IF EXISTS staging.games CASCADE;
                    -- Drop duplicate/legacy tables in favor of canonical ones
                    DROP TABLE IF EXISTS staging.opponent_stats_per_100 CASCADE;
                    DROP TABLE IF EXISTS staging.team_stats_per_100 CASCADE;
                    DROP TABLE IF EXISTS staging.player_box_score CASCADE;
                    DROP TABLE IF EXISTS staging.advanced CASCADE;
                    """
                )
            conn.commit()
            print("[CLEANUP] Dropped JSON placeholders and duplicate legacy tables from Postgres staging")
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print("[WARN] could not drop JSON placeholders:", e)

        # Load CSVs into staging
        csv_map = [
            # ATTENZIONE: niente duplicati con Mongo. Non carichiamo player_totals/team_abbrev in Postgres.
            ('staging.player_per_game', os.path.join(data_dir, 'Player Per Game.csv')),
            ('staging.player_advanced', os.path.join(data_dir, 'Advanced.csv')),
            ('staging.team_summaries', os.path.join(data_dir, 'Team Summaries.csv')),
            # Altri CSV utili (opzionali):
            ('staging.team_stats_per_game', os.path.join(data_dir, 'Team Stats Per Game.csv')),
            ('staging.team_stats_per_100', os.path.join(data_dir, 'Team Stats Per 100 Poss.csv')),
            ('staging.team_totals', os.path.join(data_dir, 'Team Totals.csv')),
            ('staging.opponent_stats_per_100', os.path.join(data_dir, 'Opponent Stats Per 100 Poss.csv')),
            ('staging.player_shooting', os.path.join(data_dir, 'Player Shooting.csv')),
            ('staging.player_play_by_play', os.path.join(data_dir, 'Player Play By Play.csv')),
            ('staging.seasons_stats', os.path.join(data_dir, 'Seasons_Stats.csv')),
            ('staging.nba_head_coaches', os.path.join(data_dir, 'NBA_head_coaches.csv')),
        ]
        for table, path in csv_map:
            if os.path.exists(path):
                try:
                    copy_csv(conn, table, path)
                except Exception as e:
                    print(f"[SKIP ERROR] {table} from {os.path.relpath(path, ROOT)}: {e}")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
            else:
                print(f"[SKIP] missing {path}")

        # JSONL heterogeneous loads
        if not args.csv_only:
            j1 = os.path.join(json_dir, 'common_player_info.jsonl')
            if os.path.exists(j1):
                load_jsonl_player_bio(conn, j1, 'staging.player_bio', 'json_common')
            else:
                # fallback from CSV
                cpi_csv = os.path.join(data_dir, 'common_player_info.csv')
                if os.path.exists(cpi_csv):
                    copy_csv(conn, 'staging.player_bio', cpi_csv)
            j2 = os.path.join(json_dir, 'team_details.jsonl')
            if os.path.exists(j2):
                load_jsonl_team_details(conn, j2, 'staging.team_details', 'json_team_details')
            else:
                td_csv = os.path.join(data_dir, 'team_details.csv')
                if os.path.exists(td_csv):
                    copy_csv(conn, 'staging.team_details', td_csv)
            j3 = os.path.join(json_dir, 'games.jsonl')
            if os.path.exists(j3):
                load_jsonl_games(conn, j3, 'staging.games')

        # std and gav views
        # Le viste locali possono fallire se alcune tabelle (spostate su Mongo) non esistono. Ignora errori.
        try:
            run_sql(conn, os.path.join(ROOT, 'db', '03_std_views.sql'))
        except Exception as e:
            print('[WARN] std views skipped:', e)
        try:
            run_sql(conn, os.path.join(ROOT, 'db', '04_gav_views.sql'))
        except Exception as e:
            print('[WARN] gav views skipped:', e)

        # Optional snapshots
        try:
            run_sql(conn, os.path.join(ROOT, 'db', '05_wh_matviews.sql'))
        except Exception as e:
            print('[WARN] materialized views step failed (postgres version?):', e)

        # Quick sanity counts
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM gav.global_player_season")
            n_players = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM gav.global_team_season")
            n_teams = cur.fetchone()[0]
        print(f"[OK] GAV ready. players={n_players} rows, teams={n_teams} rows")


if __name__ == '__main__':
    main()
