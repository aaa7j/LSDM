"""
RAW vs GAV query viewer (Streamlit)

Run:
  streamlit run ui_gav.py

Prereqs:
  - Trino at http://localhost:8080 (or change host/port)
  - memory + mongodb + postgresql catalogs configured
  - Apply the GAV views (button in sidebar)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List
import re

import streamlit as st

try:
    import pandas as pd
except Exception:
    raise

try:
    from trino.dbapi import connect as trino_connect  # type: ignore
except Exception:
    trino_connect = None


@dataclass
class QueryCase:
    title: str
    sql_raw: str
    sql_gav: str


def connect_trino(host: str, port: int, user: str, catalog: str = "memory", schema: str = "gav"):
    if trino_connect is None:
        raise RuntimeError("Package 'trino' non installato")
    return trino_connect(host=host, port=port, user=user, http_scheme="http", catalog=catalog, schema=schema)


def _sanitize_sql_for_trino(sql: str) -> str:
    s = sql.strip()
    s = re.sub(r";\s*\Z", "", s, flags=re.S)
    return s


def run_query(sql: str, host: str, port: int, user: str, catalog: str = "memory", schema: str = "gav", max_rows: int = 1000) -> pd.DataFrame:
    sql = _sanitize_sql_for_trino(sql)
    with connect_trino(host, int(port), user, catalog, schema) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(size=max_rows)
        cols = [c[0] for c in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=cols)


def apply_gav_views(host: str, port: int, user: str) -> str:
    path = os.path.join("trino", "views", "gav.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(f"File non trovato: {path}")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # split on semicolons ignoring comments
    statements: List[str] = []
    buf: List[str] = []
    for line in content.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if ";" in line:
            joined = "\n".join(buf)
            parts = joined.split(";")
            for p in parts[:-1]:
                s = p.strip()
                if s:
                    statements.append(s)
            buf = [parts[-1]] if parts[-1] else []
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)

    executed = 0
    with connect_trino(host, int(port), user, catalog="memory", schema="gav") as conn:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
            try:
                cur.fetchall()
            except Exception:
                pass
            executed += 1
    return f"Eseguite {executed} istruzioni da {path}"

def diagnose(host: str, port: int, user: str):
    report = {}
    def q(sql: str, catalog: str = "system", schema: str = "information_schema"):
        with connect_trino(host, int(port), user, catalog=catalog, schema=schema) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            return cur.fetchall()

    try:
        cats = [r[0] for r in q("SHOW CATALOGS")]
        report["catalogs"] = cats
        if "memory" in cats:
            schemas = [r[0] for r in q("SHOW SCHEMAS FROM memory")]
            report["memory.schemas"] = schemas
            if "gav" in schemas:
                report["memory.gav.tables"] = [r[0] for r in q("SHOW TABLES FROM memory.gav")] 
        if "postgresql" in cats:
            pg_schemas = [r[0] for r in q("SHOW SCHEMAS FROM postgresql")]
            report["postgresql.schemas"] = pg_schemas
            if "staging" in pg_schemas:
                report["postgresql.staging.tables"] = [r[0] for r in q("SHOW TABLES FROM postgresql.staging")]
        if "mongodb" in cats:
            mg_schemas = [r[0] for r in q("SHOW SCHEMAS FROM mongodb")]
            report["mongodb.schemas"] = mg_schemas
            if "lsdm" in mg_schemas:
                report["mongodb.lsdm.tables"] = [r[0] for r in q("SHOW TABLES FROM mongodb.lsdm")]
    except Exception as e:
        report["error"] = str(e)
    return report


CASES: List[QueryCase] = [
    QueryCase(
        title="Roster + posizione/fisico",
        sql_raw=(
            """
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))            AS player_name,
  TRY_CAST(pt.season AS INTEGER)                              AS season,
  UPPER(pt.team)                                              AS team_abbr,
  COALESCE(cpi.position, UPPER(pt.pos), UPPER(ppg.pos))       AS position,
  CAST(ROUND(
    COALESCE(
      TRY_CAST(REGEXP_EXTRACT(cpi.height, '^(\\d+)', 1) AS DOUBLE) * 30.48
      + TRY_CAST(REGEXP_EXTRACT(cpi.height, '(\\d+)$', 1) AS DOUBLE) * 2.54,
      NULL
    )
  ) AS INTEGER)                                               AS height_cm,
  CAST(ROUND(
    COALESCE(TRY_CAST(cpi.weight AS DOUBLE) * 0.45359237, NULL)
  ) AS INTEGER)                                               AS weight_kg
FROM mongodb.lsdm.player_totals pt
LEFT JOIN postgresql.staging.player_per_game ppg
  ON ppg.player_id = pt.player_id
 AND TRY_CAST(ppg.season AS INTEGER) = TRY_CAST(pt.season AS INTEGER)
 AND UPPER(ppg.team) = UPPER(pt.team)
LEFT JOIN mongodb.lsdm.common_player_info cpi
  ON cpi.player_id = pt.player_id
WHERE UPPER(pt.team) = 'LAL' AND TRY_CAST(pt.season AS INTEGER) BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name
"""
        ),
        sql_gav=(
            """
SELECT player_name, season, team_abbr, position, height_cm, weight_kg
FROM memory.gav.global_player_season
WHERE team_abbr = 'LAL' AND season BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name
"""
        ),
    ),
    QueryCase(
        title="Triple-doubles per giocatore",
        sql_raw=(
            """
SELECT LOWER(player_name) AS player_name, COUNT(*) AS triple_doubles
FROM postgresql.staging.nba_player_box_score_stats_1950_2022
WHERE TRY_CAST(season AS INTEGER) BETWEEN 2010 AND 2020
  AND TRY_CAST(pts AS INTEGER) >= 10
  AND TRY_CAST(reb AS INTEGER) >= 10
  AND TRY_CAST(ast AS INTEGER) >= 10
GROUP BY LOWER(player_name)
ORDER BY triple_doubles DESC
"""
        ),
        sql_gav=(
            """
SELECT player_name, COUNT(*) AS triple_doubles
FROM memory.gav.player_game_box
WHERE season BETWEEN 2010 AND 2020
  AND pts >= 10 AND reb >= 10 AND ast >= 10
GROUP BY player_name
ORDER BY triple_doubles DESC
"""
        ),
    ),
    QueryCase(
        title="Team meta (coach + arena) per BOS/LAL nel 2022",
        sql_raw=(
            """
SELECT
  TRY_CAST(ts.season AS INTEGER)                                           AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                               AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname))        AS team_name,
  LOWER(COALESCE(nhc.name, td.meta.head_coach))                            AS coach,
  LOWER(COALESCE(ts.arena, td.meta.arena.name))                            AS arena_name
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
LEFT JOIN postgresql.staging.nba_head_coaches nhc
  ON 2022 BETWEEN TRY_CAST(nhc.start_season_short AS INTEGER)
              AND TRY_CAST(nhc.end_season_short   AS INTEGER)
 AND REGEXP_LIKE(UPPER(nhc.teams), CONCAT('(^|[,\\s])', UPPER(COALESCE(ts.abbreviation, ta.abbreviation)), '([,\\s]|$)'))
WHERE COALESCE(ts.abbreviation, ta.abbreviation) IN ('BOS','LAL')
  AND TRY_CAST(ts.season AS INTEGER) = 2022
ORDER BY team_abbr
"""
        ),
        sql_gav=(
            """
SELECT season, team_abbr, team_name, coach, arena_name
FROM memory.gav.dim_team_season
WHERE team_abbr IN ('BOS','LAL') AND season = 2022
ORDER BY team_abbr
"""
        ),
    ),
    QueryCase(
        title="Top 10 PER (2016, CLE)",
        sql_raw=(
            """
SELECT LOWER(pt.player) AS player_name, UPPER(pt.team) AS team_abbr, adv.per, adv.ts_percent, adv.ws, adv.vorp
FROM postgresql.staging.player_advanced adv
JOIN mongodb.lsdm.player_totals pt
  ON adv.player_id = pt.player_id AND TRY_CAST(adv.season AS INTEGER) = TRY_CAST(pt.season AS INTEGER)
WHERE TRY_CAST(adv.season AS INTEGER) = 2016 AND UPPER(pt.team) = 'CLE'
ORDER BY adv.per DESC
LIMIT 10
"""
        ),
        sql_gav=(
            """
SELECT player_name, team_abbr, per, ts_percent, ws, vorp
FROM memory.gav.global_player_season
WHERE season = 2016 AND team_abbr = 'CLE'
ORDER BY per DESC NULLS LAST
"""
        ),
    ),
    QueryCase(
        title="Partite con maggiore affluenza",
        sql_raw=(
            """
SELECT
  g.game_id,
  COALESCE(
    CAST(try(from_iso8601_timestamp(CAST(g.date AS VARCHAR))) AS DATE),
    CAST(TRY_CAST(CAST(g.date AS VARCHAR) AS TIMESTAMP) AS DATE),
    TRY_CAST(SUBSTR(CAST(g.date AS VARCHAR), 1, 10) AS DATE)
  ) AS game_date,
  COALESCE(
    TRY_CAST(g.season AS INTEGER),
    TRY_CAST(SUBSTR(CAST(g.season AS VARCHAR), 1, 4) AS INTEGER),
    year(
      COALESCE(
        CAST(try(from_iso8601_timestamp(CAST(g.date AS VARCHAR))) AS DATE),
        CAST(TRY_CAST(CAST(g.date AS VARCHAR) AS TIMESTAMP) AS DATE),
        TRY_CAST(SUBSTR(CAST(g.date AS VARCHAR), 1, 10) AS DATE)
      )
    )
  ) AS season,
  LOWER(g.home.team_city)  AS home_team_city,
  LOWER(g.home.team_name)  AS home_team_name,
  LOWER(g.away.team_city)  AS away_team_city,
  LOWER(g.away.team_name)  AS away_team_name,
  CASE
    WHEN REGEXP_LIKE(CAST(g.attendance AS VARCHAR), '^\d{1,3}([.,]\d{3})+$')
      THEN TRY_CAST(REPLACE(REPLACE(CAST(g.attendance AS VARCHAR), ',', ''), '.', '') AS INTEGER)
    WHEN REGEXP_LIKE(CAST(g.attendance AS VARCHAR), '^\d+\.\d+$')
      THEN TRY_CAST(REGEXP_EXTRACT(CAST(g.attendance AS VARCHAR), '^(\d+)\.', 1) AS INTEGER)
    ELSE TRY_CAST(CAST(g.attendance AS VARCHAR) AS INTEGER)
  END AS attendance
FROM mongodb.lsdm.games g
WHERE COALESCE(TRY_CAST(g.season AS INTEGER), TRY_CAST(SUBSTR(g.season, 1, 4) AS INTEGER), year(TRY_CAST(g.date AS DATE))) = 2025
  AND g.attendance IS NOT NULL
ORDER BY attendance DESC
"""
        ),
        sql_gav=(
            """
SELECT game_id, game_date, season, home_team_city, home_team_name, away_team_city, away_team_name, attendance
FROM memory.gav.global_game
WHERE season = 2025 AND attendance IS NOT NULL
ORDER BY attendance DESC
"""
        ),
    ),
    QueryCase(
        title="Statistiche avanzate di squadra (LAL vs BOS 2020)",
        sql_raw=(
            """
SELECT
  TRY_CAST(ts.season AS INTEGER) AS season,
  UPPER(ts.abbreviation) AS team_abbr,
  ts.o_rtg, ts.d_rtg, ts.n_rtg, ts.pace,
  COALESCE(
    tpg.pts_per_game,
    CASE WHEN tot.pts IS NOT NULL AND tot.g IS NOT NULL AND TRY_CAST(tot.g AS DOUBLE) <> 0
         THEN TRY_CAST(tot.pts AS DOUBLE) / TRY_CAST(tot.g AS DOUBLE)
    END
  ) AS points_per_game
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_stats_per_game tpg
  ON TRY_CAST(tpg.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tpg.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_totals tot
  ON TRY_CAST(tot.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tot.abbreviation) = UPPER(ts.abbreviation)
WHERE TRY_CAST(ts.season AS INTEGER) = 2020 AND UPPER(ts.abbreviation) IN ('LAL','BOS')
ORDER BY team_abbr
"""
        ),
        sql_gav=(
            """
SELECT season, team_abbr, ROUND(o_rtg,1) AS o_rtg, ROUND(d_rtg,1) AS d_rtg, ROUND(n_rtg,1) AS n_rtg, ROUND(pace,1) AS pace,
       ROUND(COALESCE(points_per_game, (o_rtg * pace)/100.0), 1) AS points_per_game
FROM memory.gav.global_team_season
WHERE season = 2020 AND team_abbr IN ('LAL','BOS')
ORDER BY team_abbr
"""
        ),
    ),
    QueryCase(
        title="Affluenza media stagionale per squadra",
        sql_raw=(
            """
SELECT
  TRY_CAST(ts.season AS INTEGER)                                   AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                       AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname)) AS team_name,
  ts.attend, ts.attend_g
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation)        = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
WHERE TRY_CAST(ts.season AS INTEGER) = 2025
ORDER BY ts.attend DESC NULLS LAST
"""
        ),
        sql_gav=(
            """
SELECT season, team_abbr, team_name, attend, attend_g
FROM memory.gav.global_team_season
WHERE season = 2025
ORDER BY attend DESC NULLS LAST
"""
        ),
    ),
]


def main():
    st.set_page_config(page_title="RAW vs GAV: Trino", layout="wide")
    st.title("Confronto SQL: Sorgenti RAW vs Vista GAV")
    st.caption("Seleziona un caso, confronta le query; esegui solo la GAV e guarda i risultati.")

    with st.sidebar:
        st.subheader("Connessione Trino")
        host = st.text_input("Host", value="localhost")
        port = st.number_input("Port", value=8080, min_value=1, max_value=65535, step=1)
        user = st.text_input("User", value="streamlit")
        if st.button("Applica/aggiorna viste GAV"):
            try:
                msg = apply_gav_views(host, int(port), user)
                st.success(msg)
            except Exception as e:
                st.error(f"Errore nell'applicare le viste: {e}")
        if st.button("Diagnostica"):
            try:
                rep = diagnose(host, int(port), user)
                st.write(rep)
            except Exception as e:
                st.error(f"Errore diagnostica: {e}")

    case_titles = [c.title for c in CASES]
    choice = st.selectbox("Caso", options=case_titles, index=0)
    case = next(c for c in CASES if c.title == choice)

    dyn_raw = case.sql_raw
    dyn_gav = case.sql_gav

    with st.expander("Parametri", expanded=True):
        if "Roster + posizione" in case.title:
            team = st.text_input("Team abbr", value="LAL").upper()
            y1, y2 = st.slider("Intervallo stagioni", 1950, 2025, (2019, 2021))
            dyn_raw = f"""
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player)) AS player_name,
  TRY_CAST(pt.season AS INTEGER) AS season,
  UPPER(pt.team) AS team_abbr,
  COALESCE(cpi.position, UPPER(pt.pos), UPPER(ppg.pos)) AS position,
  CAST(ROUND(
    COALESCE(
      TRY_CAST(REGEXP_EXTRACT(cpi.height, '^(\\d+)', 1) AS DOUBLE) * 30.48
      + TRY_CAST(REGEXP_EXTRACT(cpi.height, '(\\d+)$', 1) AS DOUBLE) * 2.54,
      NULL
    )
  ) AS INTEGER) AS height_cm,
  CAST(ROUND(
    COALESCE(TRY_CAST(cpi.weight AS DOUBLE) * 0.45359237, NULL)
  ) AS INTEGER) AS weight_kg
FROM mongodb.lsdm.player_totals pt
LEFT JOIN postgresql.staging.player_per_game ppg
  ON ppg.player_id = pt.player_id
 AND TRY_CAST(ppg.season AS INTEGER) = TRY_CAST(pt.season AS INTEGER)
 AND UPPER(ppg.team) = UPPER(pt.team)
LEFT JOIN mongodb.lsdm.common_player_info cpi
  ON cpi.player_id = pt.player_id
WHERE UPPER(pt.team) = '{team}' AND TRY_CAST(pt.season AS INTEGER) BETWEEN {y1} AND {y2}
ORDER BY season DESC, player_name"""
            dyn_gav = f"""
SELECT player_name, season, team_abbr, position, height_cm, weight_kg
FROM memory.gav.global_player_season
WHERE team_abbr = '{team}' AND season BETWEEN {y1} AND {y2}
ORDER BY season DESC, player_name"""
        elif "Team meta (coach + arena)" in case.title:
            teams_in = st.text_input("Team abbr (lista)", value="BOS,LAL")
            abbr_list = [re.sub(r"[^A-Za-z]", "", t).upper() for t in teams_in.split(',') if t.strip()]
            abbrs = ",".join([f"'{t}'" for t in abbr_list]) or "'BOS','LAL'"
            dyn_raw = f"""
SELECT
  TRY_CAST(ts.season AS INTEGER)                                           AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                               AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname))        AS team_name,
  LOWER(COALESCE(nhc.name, td.meta.head_coach))                            AS coach,
  LOWER(COALESCE(ts.arena, td.meta.arena.name))                            AS arena_name
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
LEFT JOIN postgresql.staging.nba_head_coaches nhc
  ON 2022 BETWEEN TRY_CAST(nhc.start_season_short AS INTEGER)
              AND TRY_CAST(nhc.end_season_short   AS INTEGER)
 AND REGEXP_LIKE(UPPER(nhc.teams), CONCAT('(^|[,\\s])', UPPER(COALESCE(ts.abbreviation, ta.abbreviation)), '([,\\s]|$)'))
WHERE COALESCE(ts.abbreviation, ta.abbreviation) IN ({abbrs})
  AND TRY_CAST(ts.season AS INTEGER) = 2022
ORDER BY team_abbr"""
            dyn_gav = f"""
SELECT season, team_abbr, team_name, coach, arena_name
FROM memory.gav.dim_team_season
WHERE team_abbr IN ({abbrs}) AND season = 2022
ORDER BY team_abbr"""
        elif "Triple-doubles per giocatore" in case.title:
            start, end = st.slider("Intervallo stagioni", 1950, 2025, (2010, 2020))
            dyn_raw = f"""
SELECT LOWER(player_name) AS player_name, COUNT(*) AS triple_doubles
FROM postgresql.staging.nba_player_box_score_stats_1950_2022
WHERE TRY_CAST(season AS INTEGER) BETWEEN {start} AND {end}
  AND TRY_CAST(pts AS INTEGER) >= 10
  AND TRY_CAST(reb AS INTEGER) >= 10
  AND TRY_CAST(ast AS INTEGER) >= 10
GROUP BY LOWER(player_name)
ORDER BY triple_doubles DESC"""
            dyn_gav = f"""
SELECT player_name, COUNT(*) AS triple_doubles
FROM memory.gav.player_game_box
WHERE season BETWEEN {start} AND {end}
  AND pts >= 10 AND reb >= 10 AND ast >= 10
GROUP BY player_name
ORDER BY triple_doubles DESC"""
        elif "Top 10 PER" in case.title:
            team = st.text_input("Team abbr", value="CLE").upper()
            season = st.slider("Season", min_value=1950, max_value=2025, value=2016, step=1)
            dyn_gav = f"""
SELECT player_name, team_abbr, per, ts_percent, ws, vorp
FROM memory.gav.global_player_season
WHERE season = {int(season)} AND team_abbr = '{team}'
ORDER BY per DESC NULLS LAST
LIMIT 10"""
        elif "Statistiche avanzate di squadra" in case.title:
            season = st.slider("Season", min_value=1961, max_value=2025, value=2020, step=1)
            team1 = re.sub(r"[^A-Za-z]", "", st.text_input("Team A", value="LAL")).upper()
            team2 = re.sub(r"[^A-Za-z]", "", st.text_input("Team B", value="BOS")).upper()
            dyn_raw = f"""
SELECT
  TRY_CAST(ts.season AS INTEGER) AS season,
  UPPER(ts.abbreviation) AS team_abbr,
  ROUND(ts.o_rtg, 1) AS o_rtg,
  ROUND(ts.d_rtg, 1) AS d_rtg,
  ROUND(ts.n_rtg, 1) AS n_rtg,
  ROUND(ts.pace, 1)  AS pace,
  ROUND(
    COALESCE(
      tpg.pts_per_game,
      CASE WHEN tot.pts IS NOT NULL AND tot.g IS NOT NULL AND TRY_CAST(tot.g AS DOUBLE) <> 0
           THEN TRY_CAST(tot.pts AS DOUBLE) / TRY_CAST(tot.g AS DOUBLE)
      END,
      (ts.o_rtg * ts.pace)/100.0
    ), 1
  ) AS points_per_game
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_stats_per_game tpg
  ON TRY_CAST(tpg.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tpg.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_totals tot
  ON TRY_CAST(tot.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER) AND UPPER(tot.abbreviation) = UPPER(ts.abbreviation)
WHERE TRY_CAST(ts.season AS INTEGER) = {int(season)} AND UPPER(ts.abbreviation) IN ('{team1}','{team2}')
ORDER BY team_abbr;"""
            dyn_gav = f"""
SELECT
  season,
  team_abbr,
  ROUND(o_rtg, 1) AS o_rtg,
  ROUND(d_rtg, 1) AS d_rtg,
  ROUND(n_rtg, 1) AS n_rtg,
  ROUND(pace, 1)  AS pace,
  ROUND(COALESCE(points_per_game, (o_rtg * pace)/100.0), 1) AS points_per_game
FROM memory.gav.global_team_season
WHERE season = {int(season)}
  AND team_abbr IN ('{team1}','{team2}')
ORDER BY team_abbr;"""
        elif "Partite con maggiore affluenza" in case.title:
            season_g = st.slider("Season", min_value=1946, max_value=2025, value=2025, step=1)

            dyn_raw = f"""
SELECT
  g.game_id,
  COALESCE(
    CAST(try(from_iso8601_timestamp(CAST(g.date AS VARCHAR))) AS DATE),
    CAST(TRY_CAST(CAST(g.date AS VARCHAR) AS TIMESTAMP) AS DATE),
    TRY_CAST(SUBSTR(CAST(g.date AS VARCHAR), 1, 10) AS DATE)
  ) AS game_date,
  COALESCE(
    TRY_CAST(g.season AS INTEGER),
    TRY_CAST(SUBSTR(CAST(g.season AS VARCHAR), 1, 4) AS INTEGER),
    year(
      COALESCE(
        CAST(try(from_iso8601_timestamp(CAST(g.date AS VARCHAR))) AS DATE),
        CAST(TRY_CAST(CAST(g.date AS VARCHAR) AS TIMESTAMP) AS DATE),
        TRY_CAST(SUBSTR(CAST(g.date AS VARCHAR), 1, 10) AS DATE)
      )
    )
  ) AS season,
  LOWER(g.home.team_city)  AS home_team_city,
  LOWER(g.home.team_name)  AS home_team_name,
  LOWER(g.away.team_city)  AS away_team_city,
  LOWER(g.away.team_name)  AS away_team_name,
  CASE
    WHEN REGEXP_LIKE(CAST(g.attendance AS VARCHAR), '^\d{1,3}([.,]\\d{3})+$')
      THEN TRY_CAST(REPLACE(REPLACE(CAST(g.attendance AS VARCHAR), ',', ''), '.', '') AS INTEGER)
    WHEN REGEXP_LIKE(CAST(g.attendance AS VARCHAR), '^\d+\.\d+$')
      THEN TRY_CAST(REGEXP_EXTRACT(CAST(g.attendance AS VARCHAR), '^(\d+)\.', 1) AS INTEGER)
    ELSE TRY_CAST(CAST(g.attendance AS VARCHAR) AS INTEGER)
  END AS attendance
FROM mongodb.lsdm.games g
WHERE COALESCE(
        TRY_CAST(g.season AS INTEGER),
        TRY_CAST(SUBSTR(CAST(g.season AS VARCHAR), 1, 4) AS INTEGER),
        year(
          COALESCE(
            CAST(try(from_iso8601_timestamp(CAST(g.date AS VARCHAR))) AS DATE),
            CAST(TRY_CAST(CAST(g.date AS VARCHAR) AS TIMESTAMP) AS DATE),
            TRY_CAST(SUBSTR(CAST(g.date AS VARCHAR), 1, 10) AS DATE)
          )
        )
      ) = {int(season_g)}
  AND g.attendance IS NOT NULL
ORDER BY attendance DESC"""

            dyn_gav = f"""
SELECT game_id, game_date, season, home_team_city, home_team_name, away_team_city, away_team_name, attendance
FROM memory.gav.global_game
WHERE season = {int(season_g)} AND attendance IS NOT NULL
ORDER BY attendance DESC"""
        elif "Affluenza media stagionale" in case.title:
            season = st.slider("Season", min_value=1981, max_value=2025, value=2025, step=1)
            dyn_gav = f"""
SELECT season, team_abbr, team_name, attend, attend_g
FROM memory.gav.global_team_season
WHERE season = {int(season)}
ORDER BY attend DESC NULLS LAST"""

    col1, col2 = st.columns(2)
    with col1:
        st.text("Sorgenti raw (senza GAV)")
        st.code((dyn_raw or case.sql_raw).strip(), language="sql")
    with col2:
        st.text("Vista globale (con GAV)")
        st.code((dyn_gav or case.sql_gav).strip(), language="sql")

    st.divider()

    show_limit = True
    if "Top 10 PER" in case.title:
        show_limit = False
    max_rows = 100
    if show_limit:
        max_rows = st.slider("Limite righe", min_value=10, max_value=100, value=100, step=10)
    run = st.button("Esegui SOLO la query GAV")
    if run:
        try:
            with st.spinner("Eseguo la query GAV su Trino..."):
                df = run_query((dyn_gav or case.sql_gav), host, int(port), user, catalog="memory", schema="gav", max_rows=int(max_rows))
            if df.empty:
                st.info("Nessun risultato (tabella vuota).")
            else:
                try:
                    df.index = df.index + 1
                    df.index.name = "#"
                except Exception:
                    pass
                st.dataframe(df)
        except Exception as e:
            try:
                import trino
                if isinstance(e, trino.exceptions.TrinoUserError):  # type: ignore[attr-defined]
                    st.error(f"{e.error_name}: {e.message} (queryId={e.query_id})")  # type: ignore[attr-defined]
                else:
                    st.error(f"Errore esecuzione: {e}")
            except Exception:
                st.error(f"Errore esecuzione: {e}")


if __name__ == "__main__":  # pragma: no cover
    main()


