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
import json
import subprocess
import sys

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
    # Prefer project-root gav.sql if present, fallback to trino/views/gav.sql
    path = os.path.join("gav.sql")
    if not os.path.exists(path):
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


# -------------------------
# Hadoop vs Spark helpers
# -------------------------
def _load_json_rows(path: str):
    """Load { rows, count } from a JSON file; tolerate UTF-8 BOM."""
    try:
        with open(path, "rb") as f:
            data = f.read()
        if not data:
            return [], None
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]
        obj = json.loads(data.decode("utf-8"))
        return obj.get("rows", []), obj
    except Exception:
        return [], None


def _ps_run(args: List[str]):
    try:
        r = subprocess.run(args, capture_output=True, text=True)
        return r.returncode, r.stdout, r.stderr
    except Exception as e:
        return 1, "", str(e)


def hs_run_hadoop(query: str, topn: int = 3, reuse: bool = True):
    """Run Hadoop Streaming for a single query via PowerShell script, then regenerate JSON."""
    reuse_val = "True" if reuse else "False"
    code, out, err = _ps_run([
        "powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_hadoop_streaming.ps1",
        "-Only", query, "-TopN", str(int(topn)), "-Reuse", reuse_val,
    ])
    gen_code, gen_out, gen_err = _ps_run([
        "powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/generate_web_results.ps1",
    ])
    ok = (code == 0) and (gen_code == 0)
    return ok, (out + "\n" + gen_out), (err + "\n" + gen_err)


def hs_run_spark(query: str, topn: int = 3, reuse: bool = True):
    """Run PySpark for a single query via PowerShell script, then regenerate JSON."""
    reuse_val = "True" if reuse else "False"
    code, out, err = _ps_run([
        "powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/run_spark_cluster.ps1",
        "-Only", query, "-TopN", str(int(topn)), "-Reuse", reuse_val,
    ])
    gen_code, gen_out, gen_err = _ps_run([
        "powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/generate_web_results.ps1",
    ])
    ok = (code == 0) and (gen_code == 0)
    return ok, (out + "\n" + gen_out), (err + "\n" + gen_err)


def _season_key(val: str) -> int:
    try:
        s = str(val)
        return int(s[:4])
    except Exception:
        return -10**9


def _extract_season_year(row, q: str) -> int:
    """Return the start year as int for a row of query q (q1/q2/q3)."""
    try:
        if isinstance(row, dict):
            s = str(row.get("season", ""))
        else:
            idx = 0 if q in ("q1", "q2") else 1  # q3 has season at index 1
            s = str(row[idx]) if len(row) > idx else ""
        return _season_key(s)
    except Exception:
        return -10**9

def _load_team_fullname(side: str) -> dict:
    """Return { team_id: 'City TeamName' or 'TeamName' } mapping.
    Sources tried in order:
      - web/data/*q2*.json (team_name)
      - warehouse/bigdata/teams_dim_tsv/part-00000.tsv (team_city, team_name)
      - data/json/team_details.jsonl (meta.city, nickname/team_name)
    """
    name_map: dict[str, str] = {}
    city_map: dict[str, str] = {}

    # 1) Names from web data (q2)
    paths: list[tuple[str, str]]
    if side == 'hadoop':
        paths = [("web/data/q2.json", "rows"), ("web/data/hadoop.json", "q2")]
    else:
        paths = [("web/data/spark_q2.json", "rows"), ("web/data/pyspark.json", "q2")]
    for path, key in paths:
        rows, obj = _load_json_rows(path)
        data = obj.get(key) if (obj and key in obj) else rows
        if not data:
            continue
        for r in data:
            if isinstance(r, dict):
                tid = str(r.get("team_id", "")).strip()
                name = str(r.get("team_name", "")).strip()
            elif isinstance(r, (list, tuple)):
                tid = str(r[1]).strip() if len(r) > 1 else ""
                name = str(r[2]).strip() if len(r) > 2 else ""
            else:
                continue
            if tid and name and tid not in name_map:
                name_map[tid] = name
        if name_map:
            break

    # 2) City+name from teams_dim_tsv (if enriched)
    try:
        tsv = os.path.join('warehouse','bigdata','teams_dim_tsv','part-00000.tsv')
        with open(tsv, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                parts = line.rstrip('\n').split('\t')
                if len(parts) >= 3:
                    tid = parts[0].strip()
                    city = parts[1].strip()
                    nm = parts[2].strip()
                    if tid:
                        if nm and tid not in name_map:
                            name_map[tid] = nm
                        if city and tid not in city_map:
                            city_map[tid] = city
    except Exception:
        pass

    # 3) team_details.jsonl as last resort
    try:
        td = os.path.join('data','json','team_details.jsonl')
        with open(td, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                tid = str(obj.get('team_id','')).strip()
                if not tid:
                    continue
                if tid not in name_map:
                    nm = (obj.get('team_name') or obj.get('nickname') or '').strip()
                    if nm:
                        name_map[tid] = nm
                if tid not in city_map:
                    meta = obj.get('meta') or {}
                    ct = (meta.get('city') or '').strip()
                    if ct:
                        city_map[tid] = ct
    except Exception:
        pass

    # Build full-name map
    full: dict[str, str] = {}
    for tid, nm in name_map.items():
        city = city_map.get(tid, '').strip()
        full[tid] = f"{city} {nm}".strip() if city else nm
    return full


def _load_game_date_map() -> dict:
    """Return { game_id: 'YYYY-MM-DD' } from data/json/games.jsonl.
    Falls back to empty dict on any error.
    """
    m: dict[str, str] = {}
    try:
        path = os.path.join('data', 'json', 'games.jsonl')
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                gid = str(obj.get('game_id', '')).strip()
                dt = obj.get('date')
                if not gid:
                    continue
                dstr = None
                if isinstance(dt, str) and dt:
                    # take first 10 chars if ISO timestamp
                    dstr = dt[:10]
                if dstr:
                    m[gid] = dstr
    except Exception:
        pass
    return m

# Teams to exclude from results (non-NBA)
_EXCLUDE_TEAM_IDS = {"15016", "15018"}
_EXCLUDE_TEAM_NAMES = {name.lower() for name in ["Phoenix", "Jerusalem B.C."]}


def _filter_non_nba(rows, q: str):
    """Remove rows whose team_id is in the exclude set.
    Supports both list-based rows and dict rows.
    q: 'q1' | 'q2' | 'q3' to resolve index of team_id for list rows.
    """
    out = []
    for r in rows or []:
        try:
            if isinstance(r, dict):
                tid = str(r.get("team_id", "")).strip()
            else:
                idx = 1 if q in ("q1", "q2") else 0  # q3 has team_id at position 0
                tid = str(r[idx]) if len(r) > idx else ""
            if tid and tid in _EXCLUDE_TEAM_IDS:
                continue
        except Exception:
            pass
        out.append(r)
    return out


def hs_render_results(only: str | None = None, limit: int = 100, prefer_names: bool = False):
    """Render side-by-side tables for Hadoop and Spark for q1/q2/q3 from web/data JSON files.
    - only: 'q1' | 'q2' | 'q3' to render a single query; None renders all in tabs.
    - limit: max rows to display per table.
    - prefer_names: when True and only=='q1', replace team_id with team_name if available (via q2 mapping).
    """
    try:
        import pandas as pd  # local import to be safe
    except Exception:
        pd = None  # type: ignore

    HEADERS = {
        "q1": ["season", "team_id", "total_points", "avg_points", "games"],
        # Q2 raw shape: season, team_id, team_name, high_games, total_games, pct_high
        "q2": ["season", "team_id", "team_name", "high_games", "total_games", "pct_high"],
        "q3": ["team_id", "season", "game_id", "points"],
    }
    files = {
        "q1": ("web/data/q1.json", "web/data/spark_q1.json"),
        "q2": ("web/data/q2.json", "web/data/spark_q2.json"),
        "q3": ("web/data/q3.json", "web/data/spark_q3.json"),
    }

    def _normalize(rows, headers):
        out = []
        for r in (rows or []):
            if isinstance(r, dict):
                out.append([r.get(c, "") for c in headers])
            elif isinstance(r, (list, tuple)):
                lst = list(r)
                if len(lst) < len(headers):
                    lst = lst + ([""] * (len(headers) - len(lst)))
                elif len(lst) > len(headers):
                    lst = lst[: len(headers)]
                out.append(lst)
            else:
                # unknown shape; render as single-column string
                out.append([str(r)] + [""] * (len(headers) - 1))
        return out

    queries = ["q1", "q2", "q3"] if not only else [only]
    if only is None:
        tabs = st.tabs(["Q1", "Q2", "Q3"])
    for idx, q in enumerate(queries):
        container = tabs[idx] if only is None else st.container()
        with container:
            h_rows, h_obj = _load_json_rows(files[q][0])
            s_rows, s_obj = _load_json_rows(files[q][1])
            # Fallback: if spark_q*.json missing/empty, try web/data/pyspark.json[{q}]
            if not s_rows:
                try:
                    with open("web/data/pyspark.json", "r", encoding="utf-8") as f:
                        pys = json.load(f)
                    if isinstance(pys, dict) and q in pys and isinstance(pys[q], list):
                        # Ensure list of rows in the same order as HEADERS
                        conv = []
                        for rec in pys[q]:
                            if isinstance(rec, dict):
                                conv.append([rec.get(col, "") for col in HEADERS[q]])
                            else:
                                conv.append(rec)
                        s_rows = conv
                        s_obj = {"rows": s_rows, "count": len(s_rows)}
                except Exception:
                    pass
            h_cnt = (h_obj or {}).get("count", len(h_rows)) if h_obj is not None else 0
            s_cnt = (s_obj or {}).get("count", len(s_rows)) if s_obj is not None else 0
            # Filter out non-NBA teams then recompute counts
            try:
                h_rows = _filter_non_nba(h_rows, q)
                s_rows = _filter_non_nba(s_rows, q)
                h_cnt = len(h_rows)
                s_cnt = len(s_rows)
            except Exception:
                pass
            st.caption(f"Hadoop rows: {h_cnt} — Spark rows: {s_cnt}")
            # Optionally replace team_id with team_name for q1; for q2 hide team_id and show full team name
            headers = HEADERS[q][:]
            if prefer_names and q == "q1":
                map_h = _load_team_fullname('hadoop')
                map_s = _load_team_fullname('spark')
                def _apply_map(rows, m):
                    out = []
                    for r in rows or []:
                        if isinstance(r, dict):
                            rr = [r.get("season"), m.get(str(r.get("team_id")), r.get("team_id")), r.get("total_points"), r.get("avg_points"), r.get("games")]
                        else:
                            lst = list(r)
                            if len(lst) < 5:
                                lst += [""] * (5 - len(lst))
                            lst[1] = m.get(str(lst[1]), lst[1])
                            rr = lst[:5]
                        out.append(rr)
                    return out
                h_rows = _apply_map(h_rows, map_h)
                s_rows = _apply_map(s_rows, map_s)
                headers = ["season", "team", "total_points", "avg_points", "games"]
            elif prefer_names and q == "q2":
                # Map to: season, team, high_games, total_games, pct_high
                map_h = _load_team_fullname('hadoop')
                map_s = _load_team_fullname('spark')
                def _apply_q2(rows, m):
                    out = []
                    for r in rows or []:
                        if isinstance(r, dict):
                            season = r.get("season")
                            tid = str(r.get("team_id"))
                            team = (r.get("team_name") or m.get(tid, tid))
                            high = r.get("high_games")
                            total = r.get("total_games")
                            pct = r.get("pct_high")
                        else:
                            lst = list(r)
                            if len(lst) < 6:
                                lst += [""] * (6 - len(lst))
                            season, tid, tname = lst[0], str(lst[1]), lst[2]
                            team = tname or m.get(tid, tid)
                            high, total, pct = lst[3], lst[4], lst[5]
                        out.append([season, team, high, total, pct])
                    return out
                h_rows = _apply_q2(h_rows, map_h)
                s_rows = _apply_q2(s_rows, map_s)
                headers = ["season", "team", "high_games", "total_games", "pct_high"]
            elif q == "q3":
                # Map team_id -> team name and game_id -> date; columns: season, team, game_date, points
                map_h = _load_team_fullname('hadoop')
                map_s = _load_team_fullname('spark')
                gmap = _load_game_date_map()
                def _apply_q3(rows, m):
                    out = []
                    for r in rows or []:
                        if isinstance(r, dict):
                            tid = str(r.get("team_id"))
                            season = r.get("season")
                            gid = str(r.get("game_id"))
                            team = m.get(tid, tid)
                            gdate = gmap.get(gid, gid)
                            out.append([season, team, gdate, r.get("points")])
                        else:
                            lst = list(r)
                            if len(lst) < 4:
                                lst += [""] * (4 - len(lst))
                            # original order: team_id, season, game_id, points
                            tid = str(lst[0])
                            season = lst[1]
                            gid = str(lst[2])
                            pts = lst[3]
                            team = m.get(tid, tid)
                            gdate = gmap.get(gid, gid)
                            out.append([season, team, gdate, pts])
                    return out
                h_rows = _apply_q3(h_rows, map_h)
                s_rows = _apply_q3(s_rows, map_s)
                # Drop non-NBA exhibition teams by name (do not affect Phoenix Suns)
                try:
                    h_rows = [r for r in (h_rows or []) if str(r[1]).strip().lower() not in _EXCLUDE_TEAM_NAMES]
                    s_rows = [r for r in (s_rows or []) if str(r[1]).strip().lower() not in _EXCLUDE_TEAM_NAMES]
                except Exception:
                    pass
                headers = ["season", "team", "game_date", "points"]

            # Filter out seasons after 2024-25 and sort by season desc (recency)
            MAX_YEAR = 2024
            try:
                h_rows = [r for r in (h_rows or []) if _extract_season_year(r, q) <= MAX_YEAR]
                h_rows = sorted(h_rows, key=lambda r: _extract_season_year(r, q), reverse=True)
            except Exception:
                pass
            try:
                s_rows = [r for r in (s_rows or []) if _extract_season_year(r, q) <= MAX_YEAR]
                s_rows = sorted(s_rows, key=lambda r: _extract_season_year(r, q), reverse=True)
            except Exception:
                pass

            c1, c2 = st.columns(2)
            with c1:
                st.text("Hadoop")
                if h_rows and pd is not None:
                    dfh = pd.DataFrame(_normalize(h_rows[:limit], headers), columns=headers)
                    if "avg_points" in dfh.columns:
                        try:
                            dfh["avg_points"] = pd.to_numeric(dfh["avg_points"], errors="coerce").round(1)
                        except Exception:
                            pass
                    if "pct_high" in dfh.columns:
                        try:
                            dfh["pct_high"] = pd.to_numeric(dfh["pct_high"], errors="coerce").round(3)
                        except Exception:
                            pass
                    try:
                        dfh.index = range(1, len(dfh) + 1)
                        dfh.index.name = "#"
                    except Exception:
                        pass
                    st.dataframe(dfh)
                elif h_rows:
                    st.write(_normalize(h_rows[: min(20, limit) ], headers))
                else:
                    st.info("(vuoto)")
            with c2:
                st.text("PySpark")
                if s_rows and pd is not None:
                    dfs = pd.DataFrame(_normalize(s_rows[:limit], headers), columns=headers)
                    if "avg_points" in dfs.columns:
                        try:
                            dfs["avg_points"] = pd.to_numeric(dfs["avg_points"], errors="coerce").round(1)
                        except Exception:
                            pass
                    if "pct_high" in dfs.columns:
                        try:
                            dfs["pct_high"] = pd.to_numeric(dfs["pct_high"], errors="coerce").round(3)
                        except Exception:
                            pass
                    try:
                        dfs.index = range(1, len(dfs) + 1)
                        dfs.index.name = "#"
                    except Exception:
                        pass
                    st.dataframe(dfs)
                elif s_rows:
                    st.write(_normalize(s_rows[: min(20, limit) ], headers))
                else:
                    st.info("(vuoto)")


def render_hadoop_spark_page():
    """Standalone page: run Hadoop/Spark and display results side-by-side."""
    st.header("Hadoop vs PySpark: Esegui e confronta")
    st.caption("Esegue gli script PowerShell e visualizza i JSON in web/data/.")

    # Query selector
    labels = {
        "Q1 — Aggregazione punti (stagione, squadra)": "q1",
        "Q2 — Quota partite ad alto punteggio (>=120)": "q2",
        "Q3 — Top N partite per squadra": "q3",
    }
    label = st.selectbox("Query", list(labels.keys()), index=0)
    q = labels[label]
    force = st.checkbox("Forza ricalcolo (ignora reuse)", value=False)
    limit = st.slider("Righe da mostrare", min_value=10, max_value=200, value=100, step=10)
    prefer_names = True  # always show team names (City + Name when possible)
    topn = 3
    if q == "q3":
        topn = st.number_input("Top N (Q3)", min_value=1, max_value=20, value=3)

    colA, colB, colC, colD = st.columns(4)
    with colA:
        if st.button("Esegui Hadoop"):
            ok, out, err = hs_run_hadoop(q, topn=int(topn), reuse=(not force))
            if ok:
                st.success("Hadoop OK")
            else:
                st.error("Hadoop fallito")
                st.code(err or out)
    with colB:
        if st.button("Esegui Spark"):
            ok, out, err = hs_run_spark(q, topn=int(topn), reuse=(not force))
            if ok:
                st.success("Spark OK")
            else:
                st.error("Spark fallito")
                st.code(err or out)
    with colC:
        if st.button("Esegui entrambi"):
            ok1, out1, err1 = hs_run_hadoop(q, topn=int(topn), reuse=(not force))
            ok2, out2, err2 = hs_run_spark(q, topn=int(topn), reuse=(not force))
            if ok1 and ok2:
                st.success("Hadoop + Spark OK")
            else:
                if not ok1:
                    st.error("Hadoop fallito")
                    st.code(err1 or out1)
                if not ok2:
                    st.error("Spark fallito")
                    st.code(err2 or out2)
    with colD:
        if st.button("Rigenera JSON"):
            code, out, err = _ps_run(["powershell", "-ExecutionPolicy", "Bypass", "-File", "scripts/generate_web_results.ps1"])
            if code == 0:
                st.success("JSON aggiornati in web/data/")
            else:
                st.error("Rigenerazione fallita")
                st.code(err or out)

    st.subheader("Risultati")
    hs_render_results(only=q, limit=int(limit), prefer_names=True)


def _load_perf_results(path: str = "results/pyspark_vs_hadoop.jsonl"):
    try:
        rows = []
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
        return rows
    except Exception:
        return []


def render_performance_page():
    st.title("Performance: PySpark vs Hadoop Streaming")
    st.caption("Esegue i job e confronta i tempi medi per query.")

    with st.sidebar:
        st.subheader("Esecuzione")
        runs = st.number_input("Ripetizioni", min_value=1, max_value=5, value=1)
        topn = st.number_input("Top N (Q3)", min_value=1, max_value=20, value=3)
        if st.button("Esegui confronto"):
            try:
                cmd = [sys.executable, "scripts/run_bigdata_compare.py", "--runs", str(int(runs)), "--topn", str(int(topn))]
                r = subprocess.run(cmd, capture_output=True, text=True)
                if r.returncode != 0:
                    st.error("Esecuzione fallita")
                    st.code(r.stdout)
                    st.code(r.stderr)
                else:
                    st.success("Confronto completato")
            except Exception as e:
                st.error(f"Errore esecuzione: {e}")

    data = _load_perf_results()
    if not data:
        st.info("Nessun risultato ancora. Premi 'Esegui Confronto' dalla sidebar.")
        return

    try:
        import pandas as pd  # type: ignore
        df = pd.DataFrame(data)
        if df.empty:
            st.info("Nessun risultato valido nel file dei risultati")
            return
        df["tool"] = df["tool"].replace({"hadoop_streaming": "hadoop", "pyspark_cluster": "pyspark"})
        summary = df.groupby(["tool", "query"], dropna=False, as_index=False)["wall_ms"].mean()
        piv = summary.pivot(index="query", columns="tool", values="wall_ms").reset_index()
        st.subheader("Tempi medi per query (ms)")
        st.dataframe(piv)
        try:
            import altair as alt  # type: ignore
            melt_cols = [c for c in piv.columns if c != "query"]
            chart = alt.Chart(piv).transform_fold(melt_cols, as_=["tool", "wall_ms"]).mark_bar().encode(
                x="query:N", y="wall_ms:Q", color="tool:N"
            )
            st.altair_chart(chart, use_container_width=True)
            if "hadoop" in melt_cols and "pyspark" in melt_cols:
                piv["speedup_vs_hadoop"] = piv["hadoop"] / piv["pyspark"]
                st.subheader("Speedup vs Hadoop (maggiore è meglio)")
                sp = alt.Chart(piv).mark_bar().encode(x="query:N", y="speedup_vs_hadoop:Q")
                st.altair_chart(sp, use_container_width=True)
                st.write(piv[["query", "speedup_vs_hadoop"]])
        except Exception:
            pass
    except Exception as e:
        st.error(f"Errore caricamento risultati: {e}")
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

    # Sidebar navigation between pages
    with st.sidebar:
        st.subheader("Pagine")
        _page = st.radio("Seleziona", ["RAW vs GAV", "Hadoop vs PySpark", "Performance"], index=0)

    if _page == "Hadoop vs PySpark":
        render_hadoop_spark_page()
        return
    if _page == "Performance":
        render_performance_page()
        return

    # Default page: RAW vs GAV (unchanged)
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

    # (No Hadoop/Spark content on this page)


if __name__ == "__main__":  # pragma: no cover
    main()


