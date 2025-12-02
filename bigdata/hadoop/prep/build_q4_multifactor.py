import argparse
import csv
import os
from collections import defaultdict
from typing import Dict, Tuple, Any


def _read_per_game(path: str):
    by_player: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    tot_fallback: Dict[Tuple[int, str], Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                season = int(float(row.get("season", "") or 0))
            except Exception:
                continue
            pid = (row.get("player_id") or "").strip()
            team = (row.get("team") or "").strip().upper()
            if not pid:
                continue
            rec = {
                "pts_pg": _to_float(row.get("pts_per_game")),
                "mp_pg": _to_float(row.get("mp_per_game")),
                "team": team,
            }
            by_player[(season, team, pid)] = rec
            if team == "TOT":
                tot_fallback[(season, pid)] = rec
    return by_player, tot_fallback


def _read_advanced(path: str):
    by_player: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    tot_fallback: Dict[Tuple[int, str], Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                season = int(float(row.get("season", "") or 0))
            except Exception:
                continue
            pid = (row.get("player_id") or "").strip()
            team = (row.get("team") or "").strip().upper()
            if not pid:
                continue
            rec = {
                "usg": _to_float(row.get("usg_percent")),
                "ts": _to_float(row.get("ts_percent")),
                "ws": _to_float(row.get("ws")),
                "vorp": _to_float(row.get("vorp")),
                "team": team,
            }
            by_player[(season, team, pid)] = rec
            if team == "TOT":
                tot_fallback[(season, pid)] = rec
    return by_player, tot_fallback


def _read_team_totals(path: str):
    out: Dict[Tuple[int, str], Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                season = int(float(row.get("season", "") or 0))
            except Exception:
                continue
            team = (row.get("abbreviation") or "").strip().upper()
            if not team:
                continue
            out[(season, team)] = {
                "team_pts": _to_float(row.get("pts")),
                "team_mp": _to_float(row.get("mp")),
            }
    return out


def _to_float(val):
    try:
        if val is None:
            return None
        s = str(val).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def build_dataset(play_csv: str, per_game_csv: str, adv_csv: str, team_totals_csv: str, out_tsv: str, min_games: int = 10):
    per_by_team, per_tot = _read_per_game(per_game_csv)
    adv_by_team, adv_tot = _read_advanced(adv_csv)
    team_totals = _read_team_totals(team_totals_csv)

    os.makedirs(os.path.dirname(out_tsv) or ".", exist_ok=True)
    rows = 0
    with open(play_csv, "r", encoding="utf-8", newline="") as f_in, open(out_tsv, "w", encoding="utf-8", newline="") as f_out:
        r = csv.DictReader(f_in)
        for row in r:
            try:
                season = int(float(row.get("season", "") or 0))
            except Exception:
                continue
            team = (row.get("team") or "").strip().upper()
            pid = (row.get("player_id") or "").strip()
            player = (row.get("player") or "").strip()
            pos = (row.get("pos") or "").strip()
            g = row.get("g")
            gs = row.get("gs")
            mp_total = _to_float(row.get("mp"))

            try:
                g_int = int(float(g or 0))
            except Exception:
                continue

            # Skip multi-team aggregate rows like '2TM', '3TM', '4TM'
            if team in {"2TM", "3TM", "4TM"}:
                continue

            if g_int < min_games or mp_total is None:
                continue

            # Lookups
            adv = adv_by_team.get((season, team, pid)) or adv_tot.get((season, pid)) or {}
            per = per_by_team.get((season, team, pid)) or per_tot.get((season, pid)) or {}
            team_meta = team_totals.get((season, team), {})

            usage = adv.get("usg")
            ts = adv.get("ts")
            ws = adv.get("ws")
            vorp = adv.get("vorp")
            pts_pg = per.get("pts_pg")
            mp_pg = per.get("mp_pg")

            pts_per_36 = None
            if pts_pg is not None and mp_pg and mp_pg > 0:
                pts_per_36 = (pts_pg / mp_pg) * 36.0

            team_pts = team_meta.get("team_pts")

            # Write TSV
            def _fmt(x, digits=3):
                if x is None:
                    return ""
                return f"{x:.{digits}f}"

            f_out.write(
                "\t".join(
                    [
                        str(season),
                        team,
                        pid,
                        player,
                        pos,
                        str(g_int),
                        str(gs or ""),
                        _fmt(mp_total, 3),
                        _fmt(usage, 3),
                        _fmt(ts, 3),
                        _fmt(pts_per_36, 3),
                        _fmt(team_pts, 3),
                        _fmt(ws, 3),
                        _fmt(vorp, 3),
                    ]
                )
                + "\n"
            )
            rows += 1
    return rows


def main():
    ap = argparse.ArgumentParser(description="Build enriched TSV for Q4 multifactor ranking.")
    ap.add_argument("--play", default=os.path.join("data", "_player_play_by_play_staging.csv"), help="Play-by-play staging CSV")
    ap.add_argument("--per-game", dest="per_game", default=os.path.join("data", "Player Per Game.csv"), help="Per-game stats CSV")
    ap.add_argument("--advanced", default=os.path.join("data", "Advanced.csv"), help="Advanced stats CSV")
    ap.add_argument("--team-totals", dest="team_totals", default=os.path.join("data", "Team Totals.csv"), help="Team totals CSV")
    ap.add_argument("--out", default=os.path.join("warehouse", "bigdata", "q4_multifactor.tsv"), help="Output TSV path")
    ap.add_argument("--min-games", dest="min_games", type=int, default=10, help="Minimum games played to keep a player")
    args = ap.parse_args()

    rows = build_dataset(args.play, args.per_game, args.advanced, args.team_totals, args.out, min_games=int(args.min_games))
    print(f"Wrote {rows} rows to {args.out}")


if __name__ == "__main__":
    main()
