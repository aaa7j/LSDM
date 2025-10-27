"""
RAW vs GAV query viewer (Streamlit)

Architecture:
- Connects to Trino using the official `trino` Python client
- Dropdown to choose a prepared comparison case
- Shows SQL_RAW and SQL_GAV side by side
- Executes ONLY the GAV query and displays results

Run:
  streamlit run ui_gav.py

Prereqs:
  - Trino running at http://localhost:8080 (or change host/port)
  - memory + mongodb + postgresql catalogs configured as in ./trino/etc/catalog
  - GAV views applied (use the "Apply GAV views" button below)
  - pip install -r requirements.txt (needs streamlit, trino, pandas)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Dict, Optional

import streamlit as st
import re

try:
    import pandas as pd
except Exception as e:  # pragma: no cover
    raise

try:
    import trino  # type: ignore
    from trino.dbapi import connect as trino_connect  # type: ignore
except Exception as e:  # pragma: no cover
    trino = None
    trino_connect = None


@dataclass
class QueryCase:
    title: str
    sql_raw: str
    sql_gav: str


CASES: List[QueryCase] = [
    QueryCase(
        title="Roster + posizione/fisico (LAL, 2019–2021)",
        sql_raw=(
            """
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))            AS player_name,
  TRY_CAST(pt.season AS INTEGER)                              AS season,
  UPPER(pt.team)                                              AS team_abbr,
  COALESCE(cpi.position, UPPER(pt.pos), UPPER(ppg.pos))       AS position,
  cpi.height, cpi.weight,
  CASE WHEN cpi.position IS NOT NULL THEN 'common_player_info'
       WHEN pt.pos IS NOT NULL THEN 'player_totals'
       WHEN ppg.pos IS NOT NULL THEN 'player_per_game'
  END                                                         AS src_position
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
SELECT player_name, season, team_abbr, position, height_cm, weight_kg, src_position
FROM memory.gav.global_player_season
WHERE team_abbr = 'LAL' AND season BETWEEN 2019 AND 2021
ORDER BY season DESC, player_name
"""
        ),
    ),
    QueryCase(
        title="Team meta (coach + arena) per BOS/LAL nel 2020",
        sql_raw=(
            """
SELECT
  TRY_CAST(ts.season AS INTEGER)                                           AS season,
  COALESCE(ts.abbreviation, ta.abbreviation)                               AS team_abbr,
  COALESCE(LOWER(ts.team), LOWER(ta.team_name), LOWER(td.nickname))        AS team_name,
  LOWER(COALESCE(td.meta.head_coach, nhc.name))                            AS coach,
  LOWER(COALESCE(ts.arena, td.meta.arena.name))                            AS arena_name,
  CASE WHEN ts.arena IS NOT NULL THEN 'team_summaries' ELSE 'team_details' END AS src_arena,
  CASE WHEN td.abbreviation IS NOT NULL THEN 'team_details'
       WHEN nhc.name IS NOT NULL THEN 'nba_head_coaches' END               AS src_coach
FROM postgresql.staging.team_summaries ts
LEFT JOIN mongodb.lsdm.team_abbrev ta
  ON TRY_CAST(ta.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(ta.abbreviation) = UPPER(ts.abbreviation)
LEFT JOIN mongodb.lsdm.team_details td
  ON UPPER(td.abbreviation) = UPPER(COALESCE(ts.abbreviation, ta.abbreviation))
LEFT JOIN postgresql.staging.nba_head_coaches nhc
  ON TRY_CAST(ts.season AS INTEGER) BETWEEN TRY_CAST(SUBSTRING(nhc.start_season, 1, 4) AS INTEGER)
                     AND TRY_CAST(SUBSTRING(nhc.end_season,   1, 4) AS INTEGER)
 AND REGEXP_LIKE(UPPER(nhc.teams), CONCAT('(^|[,\\s])', UPPER(ts.abbreviation), '([,\\s]|$)'))
WHERE COALESCE(ts.abbreviation, ta.abbreviation) IN ('BOS','LAL')
  AND TRY_CAST(ts.season AS INTEGER) = 2020
ORDER BY team_abbr;
"""
        ),
        sql_gav=(
            """
SELECT season, team_abbr, team_name, coach, arena_name, src_coach, src_arena
FROM memory.gav.dim_team_season
WHERE team_abbr IN ('BOS','LAL') AND season = 2020
ORDER BY team_abbr;
"""
        ),
    ),
    QueryCase(
        title="Top 10 PER (2016, CLE) con fallback e normalizzazione TS%",
        sql_raw=(
            """
WITH adv AS (
  SELECT player_id,
         TRY_CAST(season AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(bpm)        AS bpm,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.player_advanced
  GROUP BY 1,2
), ss AS (
  SELECT LOWER(player) AS player_name,
         TRY_CAST(year AS INTEGER) AS season,
         AVG(per)        AS per,
         AVG(ts_percent) AS ts_percent,
         AVG(ws)         AS ws,
         AVG(vorp)       AS vorp
  FROM postgresql.staging.seasons_stats
  GROUP BY 1,2
)
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))                        AS player_name,
  UPPER(pt.team)                                                          AS team_abbr,
  COALESCE(adv.per, ss.per)                                               AS per,
  CASE WHEN COALESCE(adv.ts_percent, ss.ts_percent) > 1
       THEN COALESCE(adv.ts_percent, ss.ts_percent) / 100.0
       ELSE COALESCE(adv.ts_percent, ss.ts_percent) END                   AS ts_percent,
  COALESCE(adv.ws, ss.ws)                                                 AS ws,
  COALESCE(adv.vorp, ss.vorp)                                             AS vorp
FROM mongodb.lsdm.player_totals pt
LEFT JOIN adv
  ON adv.player_id = pt.player_id
 AND adv.season    = TRY_CAST(pt.season AS INTEGER)
LEFT JOIN ss
  ON ss.player_name = LOWER(pt.player)
 AND ss.season      = TRY_CAST(pt.season AS INTEGER)
LEFT JOIN mongodb.lsdm.common_player_info cpi
  ON cpi.player_id = pt.player_id
WHERE TRY_CAST(pt.season AS INTEGER) = 2016 AND UPPER(pt.team) = 'CLE'
ORDER BY per DESC NULLS LAST
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
        title="Partite con maggiore affluenza (2025)",
        sql_raw=(
            """
SELECT
  g.game_id,
  TRY_CAST(g.date AS DATE) AS game_date,
  COALESCE(TRY_CAST(g.season AS INTEGER), TRY_CAST(SUBSTR(g.season, 1, 4) AS INTEGER), year(TRY_CAST(g.date AS DATE))) AS season,
  LOWER(g.home.team_city)  AS home_team_city,
  LOWER(g.home.team_name)  AS home_team_name,
  LOWER(g.away.team_city)  AS away_team_city,
  LOWER(g.away.team_name)  AS away_team_name,
  TRY_CAST(g.attendance AS INTEGER) AS attendance
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
WHERE TRY_CAST(season AS INTEGER) = 2025 AND attendance IS NOT NULL
ORDER BY attendance DESC
"""
        ),
    ),
    QueryCase(
        title="Affluenza media stagionale per squadra (2025)",
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
WHERE TRY_CAST(season AS INTEGER) = 2025
ORDER BY attend DESC NULLS LAST
"""
        ),
    ),
    QueryCase(
        title="Triple-doubles per giocatore (2010–2020)",
        sql_raw=(
            """
SELECT LOWER(player_name) AS player_name, COUNT(*) AS triple_doubles
FROM postgresql.staging.nba_player_box_score_stats_1950_2022
WHERE TRY_CAST(season AS INTEGER) BETWEEN 2010 AND 2020
  AND TRY_CAST(pts AS INTEGER) >= 10
  AND TRY_CAST(reb AS DOUBLE)  >= 10
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
        title="Statistiche avanzate di squadra per stagione (LAL vs BOS, 2020)",
        sql_raw=(
            """
SELECT
  TRY_CAST(ts.season AS INTEGER) AS season,
  UPPER(ts.abbreviation)         AS team_abbr,
  LOWER(ts.team)                 AS team_name,
  ts.o_rtg,
  ts.d_rtg,
  ts.n_rtg,
  ts.pace,
  pg.pts AS points_per_game
FROM postgresql.staging.team_summaries ts
LEFT JOIN postgresql.staging.team_stats_per_100_poss t100
  ON TRY_CAST(t100.season AS INTEGER) = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(t100.abbreviation)        = UPPER(ts.abbreviation)
LEFT JOIN postgresql.staging.team_stats_per_game pg
  ON TRY_CAST(pg.season AS INTEGER)   = TRY_CAST(ts.season AS INTEGER)
 AND UPPER(pg.abbreviation)          = UPPER(ts.abbreviation)
WHERE TRY_CAST(ts.season AS INTEGER) = 2020
  AND UPPER(ts.abbreviation) IN ('LAL','BOS')
ORDER BY team_abbr;
"""
        ),
        sql_gav=(
            """
SELECT
  season,
  team_abbr,
  ROUND(o_rtg, 1) AS o_rtg,
  ROUND(d_rtg, 1) AS d_rtg,
  ROUND(n_rtg, 1) AS n_rtg,
  ROUND(pace, 1)  AS pace,
  ROUND(COALESCE(points_per_game, (o_rtg * pace)/100.0), 1) AS points_per_game
FROM memory.gav.global_team_season
WHERE season = 2020
  AND team_abbr IN ('LAL','BOS')
ORDER BY team_abbr;
"""
        ),
    ),
]


def connect_trino(host: str, port: int, user: str, catalog: str = "memory", schema: str = "gav"):
    if trino_connect is None:
        raise RuntimeError("Python package 'trino' non installato. Aggiungi 'trino' a requirements e pip install.")
    return trino_connect(
        host=host,
        port=port,
        user=user,
        http_scheme="http",
        catalog=catalog,
        schema=schema,
        source="ui_gav",
        session_properties={"query_max_run_time": "5m"},
    )


def _sanitize_sql_for_trino(sql: str) -> str:
    """Trino HTTP API expects a single statement without trailing semicolon."""
    s = sql.strip()
    # Drop trailing semicolons (and any trailing whitespace)
    s = re.sub(r";\s*\Z", "", s, flags=re.S)
    return s


def run_query(sql: str, host: str, port: int, user: str, catalog: str = "memory", schema: str = "gav", max_rows: int = 1000) -> pd.DataFrame:
    sql = _sanitize_sql_for_trino(sql)
    with connect_trino(host, port, user, catalog, schema) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchmany(size=max_rows)
        cols = [c[0] for c in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=cols)


def _apply_limit(sql: str, n: int) -> str:
    s = _sanitize_sql_for_trino(sql)
    try:
        # Replace trailing LIMIT <num> or append if missing
        if re.search(r"(?is)\blimit\s+\d+\s*$", s):
            s = re.sub(r"(?is)\blimit\s+\d+\s*$", f"LIMIT {int(n)}", s)
        else:
            s = f"{s} LIMIT {int(n)}"
    except Exception:
        pass
    return s


def apply_gav_views(host: str, port: int, user: str) -> str:
    """Executes trino/views/gav.sql sequentially (splits on ';')."""
    path = os.path.join("trino", "views", "gav.sql")
    if not os.path.exists(path):
        raise FileNotFoundError(f"File non trovato: {path}")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    # Strip comments and split on semicolons
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
    # any tail
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)

    executed = 0
    with connect_trino(host, port, user, catalog="memory", schema="gav") as conn:
        cur = conn.cursor()
        for stmt in statements:
            cur.execute(stmt)
            # fetch/close to ensure execution
            try:
                cur.fetchall()
            except Exception:
                pass
            executed += 1
    return f"Eseguite {executed} istruzioni da {path}"


def reset_memory_gav(host: str, port: int, user: str) -> None:
    """Drop and recreate memory.gav schema to ensure a clean slate."""
    # Try drop with CASCADE
    try:
        with connect_trino(host, int(port), user, catalog="memory", schema="gav") as conn:
            cur = conn.cursor()
            cur.execute("DROP SCHEMA IF EXISTS memory.gav CASCADE")
            try:
                cur.fetchall()
            except Exception:
                pass
    except Exception:
        pass
    # Recreate schema
    with connect_trino(host, int(port), user, catalog="memory", schema="default") as conn:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS memory.gav")
        try:
            cur.fetchall()
        except Exception:
            pass


def ensure_gav_ready(host: str, port: int, user: str) -> None:
    """Idempotently ensure memory.gav views exist and are up to date."""
    if st.session_state.get("gav_ready"):
        return
    reset_memory_gav(host, port, user)
    apply_gav_views(host, port, user)
    st.session_state["gav_ready"] = True


def diagnose(host: str, port: int, user: str) -> Dict[str, List[str]]:
    """Basic connectivity and metadata diagnostics to surface common issues."""
    report: Dict[str, List[str]] = {}
    def q(sql: str, catalog: str | None = None, schema: str | None = None) -> List[str]:
        with connect_trino(host, int(port), user, catalog or "system", schema or "information_schema") as conn:
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return [r[0] if r and len(r) else "" for r in rows]

    try:
        cats = q("SHOW CATALOGS")
        report["catalogs"] = cats
    except Exception as e:  # pragma: no cover
        report["error"] = [f"Connessione fallita: {e}"]
        return report

    # Memory
    if "memory" in report.get("catalogs", []):
        try:
            schemas = q("SHOW SCHEMAS FROM memory")
            report["memory.schemas"] = schemas
            if "gav" in schemas:
                try:
                    tables = q("SHOW TABLES FROM memory.gav")
                    report["memory.gav.tables"] = tables
                except Exception as e:
                    report["memory.gav.error"] = [str(e)]
        except Exception as e:
            report["memory.error"] = [str(e)]

    # PostgreSQL
    if "postgresql" in report.get("catalogs", []):
        try:
            pgs = q("SHOW SCHEMAS FROM postgresql")
            report["postgresql.schemas"] = pgs
            if "staging" in pgs:
                try:
                    tables = q("SHOW TABLES FROM postgresql.staging")
                    report["postgresql.staging.tables"] = tables
                except Exception as e:
                    report["postgresql.staging.error"] = [str(e)]
        except Exception as e:
            report["postgresql.error"] = [str(e)]

    # MongoDB
    if "mongodb" in report.get("catalogs", []):
        try:
            mgs = q("SHOW SCHEMAS FROM mongodb")
            report["mongodb.schemas"] = mgs
            if "lsdm" in mgs:
                try:
                    tables = q("SHOW TABLES FROM mongodb.lsdm")
                    report["mongodb.lsdm.tables"] = tables
                except Exception as e:
                    report["mongodb.lsdm.error"] = [str(e)]
        except Exception as e:
            report["mongodb.error"] = [str(e)]

    return report


def main():
    st.set_page_config(page_title="RAW vs GAV: Trino", layout="wide")
    st.title("Confronto SQL: Sorgenti RAW vs Vista GAV")
    st.caption("Seleziona un caso, confronta le query; esegui solo la GAV e guarda i risultati.")

    with st.sidebar:
        st.subheader("Connessione Trino")
        host = st.text_input("Host", value="localhost")
        port = st.number_input("Port", value=8080, min_value=1, max_value=65535, step=1)
        user = st.text_input("User", value="streamlit")
        st.caption("Catalog/schema di default: memory/gav. Le query sono qualificate completamente.")

        # Auto-apply GAV views once per session
        if not st.session_state.get("gav_ready"):
            try:
                with st.spinner("Inizializzo viste GAV..."):
                    ensure_gav_ready(host, int(port), user)
                st.success("Viste GAV pronte")
            except Exception as e:
                st.warning(f"Init automatico GAV fallito: {e}")
        if st.button("Applica/aggiorna viste GAV"):
            try:
                reset_memory_gav(host, int(port), user)
                msg = apply_gav_views(host, int(port), user)
                st.session_state["gav_ready"] = True
                st.success(msg)
            except Exception as e:
                st.error(f"Errore nell'applicare le viste: {e}")
        if st.button("Diagnostica"):
            try:
                rep = diagnose(host, int(port), user)
                st.write(rep)
                missing = []
                # quick hints
                cats = set(rep.get("catalogs", []))
                if "mongodb" not in cats:
                    missing.append("Catalogo 'mongodb' mancante")
                if "postgresql" not in cats:
                    missing.append("Catalogo 'postgresql' mancante")
                if "memory" not in cats:
                    missing.append("Catalogo 'memory' mancante")
                if missing:
                    st.warning("; ".join(missing))
                # expected core tables/collections
                exp_pg = {"player_per_game","player_advanced","seasons_stats","player_season_info","team_summaries","team_stats_per_game","team_stats_per_100_poss","opponent_stats_per_100_poss","team_totals","nba_head_coaches","nba_player_box_score_stats_1950_2022"}
                got_pg = set(rep.get("postgresql.staging.tables", []))
                missing_pg = sorted(list(exp_pg - got_pg)) if got_pg else []
                if missing_pg:
                    st.warning(f"Tabelle Postgres mancanti in schema staging: {', '.join(missing_pg)}")
                exp_mg = {"common_player_info","team_details","games","team_abbrev","player_totals"}
                got_mg = set(rep.get("mongodb.lsdm.tables", []))
                missing_mg = sorted(list(exp_mg - got_mg)) if got_mg else []
                if missing_mg:
                    st.warning(f"Collezioni Mongo mancanti in db lsdm: {', '.join(missing_mg)}")
            except Exception as e:
                st.error(f"Errore diagnostica: {e}")

    case_titles = [c.title for c in CASES]
    choice = st.selectbox("Caso", options=case_titles, index=0)
    case = next(c for c in CASES if c.title == choice)

    # Optional dynamic parameter UI per case
    def _sanitize_team_abbr(s: str) -> str:
        return re.sub(r"[^A-Za-z]", "", (s or "").upper())

    dyn_raw = case.sql_raw
    dyn_gav = case.sql_gav

    with st.expander("Parametri", expanded=True):
        if "Roster + posizione" in case.title:
            team = _sanitize_team_abbr(st.text_input("Team abbr", value="LAL"))
            y1, y2 = st.slider("Intervallo stagioni", 1950, 2030, (2019, 2021))
            dyn_raw = f"""
SELECT
  COALESCE(LOWER(cpi.full_name), LOWER(pt.player))            AS player_name,
  TRY_CAST(pt.season AS INTEGER)                              AS season,
  UPPER(pt.team)                                              AS team_abbr,
  COALESCE(cpi.position, UPPER(pt.pos), UPPER(ppg.pos))       AS position,
  cpi.height, cpi.weight,
  CASE WHEN cpi.position IS NOT NULL THEN 'common_player_info'
       WHEN pt.pos IS NOT NULL THEN 'player_totals'
       WHEN ppg.pos IS NOT NULL THEN 'player_per_game'
  END                                                         AS src_position
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
SELECT player_name, season, team_abbr, position, height_cm, weight_kg, src_position
FROM memory.gav.global_player_season
WHERE team_abbr = '{team}' AND season BETWEEN {y1} AND {y2}
ORDER BY season DESC, player_name"""
        elif "Team meta (coach + arena)" in case.title:
            season = st.number_input("Season", 1950, 2030, 2020, step=1)
            teams_in = st.text_input("Team abbr (lista separata da virgola)", value="BOS,LAL")
            abbrs = ",".join([f"'{_sanitize_team_abbr(t)}'" for t in teams_in.split(',') if _sanitize_team_abbr(t)]) or "'BOS','LAL'"
            dyn_gav = f"""
SELECT season, team_abbr, team_name, coach, arena_name, src_coach, src_arena
FROM memory.gav.dim_team_season
WHERE team_abbr IN ({abbrs}) AND season = {season}
ORDER BY team_abbr;"""
        elif "Top 10 PER" in case.title:
            team = _sanitize_team_abbr(st.text_input("Team abbr", value="CLE"))
            season = st.number_input("Season", 1950, 2030, 2016, step=1)
            dyn_gav = f"""
SELECT player_name, team_abbr, per, ts_percent, ws, vorp
FROM memory.gav.global_player_season
WHERE season = {season} AND team_abbr = '{team}'
ORDER BY per DESC NULLS LAST"""
        elif "Partite con maggiore affluenza" in case.title:
            season = st.number_input("Season", 1950, 2035, 2025, step=1)
            dyn_gav = f"""
SELECT game_id, game_date, season, home_team_city, home_team_name, away_team_city, away_team_name, attendance
FROM memory.gav.global_game
WHERE season = {season} AND attendance IS NOT NULL
ORDER BY attendance DESC"""
        elif "Triple-doubles per giocatore" in case.title:
            start, end = st.slider("Intervallo stagioni", 1950, 2035, (2010, 2020))
            dyn_gav = f"""
SELECT player_name, COUNT(*) AS triple_doubles
FROM memory.gav.player_game_box
WHERE season BETWEEN {start} AND {end}
  AND pts >= 10 AND reb >= 10 AND ast >= 10
GROUP BY player_name
ORDER BY triple_doubles DESC"""
        elif "Statistiche avanzate di squadra" in case.title:
            season = st.number_input("Season", 1950, 2035, 2020, step=1)
            team1 = _sanitize_team_abbr(st.text_input("Team A", value="LAL"))
            team2 = _sanitize_team_abbr(st.text_input("Team B", value="BOS"))
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
WHERE season = {season}
  AND team_abbr IN ('{team1}','{team2}')
ORDER BY team_abbr;"""
        elif "Affluenza media stagionale" in case.title:
            season = st.number_input("Season", 1950, 2035, 2025, step=1)
            dyn_gav = f"""
SELECT season, team_abbr, team_name, attend, attend_g
FROM memory.gav.global_team_season
WHERE TRY_CAST(season AS INTEGER) = {season}
ORDER BY attend DESC NULLS LAST"""

    col1, col2 = st.columns(2)
    with col1:
        st.text("Sorgenti raw (senza GAV)")
        st.code((dyn_raw or case.sql_raw).strip(), language="sql")
    with col2:
        st.text("Vista globale (con GAV)")
        st.code((dyn_gav or case.sql_gav).strip(), language="sql")

    st.divider()

    max_rows = st.slider("Limite righe", min_value=10, max_value=1000, value=100, step=10)
    run = st.button("Esegui SOLO la query GAV")
    if run:
        try:
            with st.spinner("Eseguo la query GAV su Trino..."):
                df = run_query((dyn_gav or case.sql_gav), host, int(port), user, catalog="memory", schema="gav", max_rows=int(max_rows))
            if df.empty:
                st.info("Nessun risultato (tabella vuota).")
            else:
                # Render with 1-based row index for readability
                try:
                    df.index = df.index + 1
                    df.index.name = "#"
                except Exception:
                    pass
                st.dataframe(df)
        except Exception as e:
            # Surface rich error info if available
            try:
                import trino
                if isinstance(e, trino.exceptions.TrinoUserError):  # type: ignore[attr-defined]
                    msg = f"{e.error_name}: {e.message} (queryId={e.query_id})"  # type: ignore[attr-defined]
                    st.error(msg)
                else:
                    st.error(f"Errore esecuzione: {e}")
            except Exception:
                st.error(f"Errore esecuzione: {e}")


if __name__ == "__main__":  # pragma: no cover
    main()







