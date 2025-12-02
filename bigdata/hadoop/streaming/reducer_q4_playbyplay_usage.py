import sys


def flush(current_key, rows, limit=15):
    if not current_key or not rows:
        return
    total_minutes = sum(r[5] for r in rows)
    # team_pts should be identical within the group; take the first non-null
    team_pts = None
    for r in rows:
        if len(r) > 9 and r[9] is not None:
            team_pts = r[9]
            break
    out = []
    for player_id, player_name, pos, g, gs, mp, usage, ts, pts36, tp, ws, vorp in rows:
        minutes_per_game = mp / g if g else 0.0
        pct_team_minutes = (100.0 * mp / total_minutes) if total_minutes else 0.0
        est_pts = (pts36 * mp / 36.0) if pts36 is not None else None
        pct_team_pts = (100.0 * est_pts / tp) if (est_pts is not None and tp) else 0.0
        usage_norm = (usage / 35.0) if usage is not None else 0.0
        ts_norm = ts if ts is not None else 0.0
        pts_norm = (pts36 / 40.0) if pts36 is not None else 0.0
        team_pts_share_norm = (pct_team_pts / 100.0) if pct_team_pts is not None else 0.0
        # Rough normalization for advanced metrics
        ws_norm = 0.0
        if ws is not None:
            ws_norm = max(min(ws / 10.0, 1.0), 0.0)
        vorp_norm = 0.0
        if vorp is not None:
            vorp_norm = max(min(vorp / 8.0, 1.0), 0.0)
        # Minutes factor: penalize very low minutes_per_game and low games played
        # - minutes_weight in [0,1], ~0.0 for <10 mpg, ~1.0 for >=30 mpg
        # - games_weight in [0,1], ~0.0 for 10 games, ~1.0 for 82 games
        base_mpg = 10.0
        max_mpg = 30.0
        minutes_weight = 0.0
        if minutes_per_game > base_mpg:
            minutes_weight = min((minutes_per_game - base_mpg) / (max_mpg - base_mpg), 1.0)
        games_weight = min(max((g - 10.0) / (82.0 - 10.0), 0.0), 1.0)
        minutes_games_factor = 0.5 * minutes_weight + 0.5 * games_weight
        score_core = (
            0.30 * (pct_team_minutes / 100.0)
            + 0.15 * usage_norm
            + 0.15 * ts_norm
            + 0.15 * pts_norm
            + 0.10 * team_pts_share_norm
            + 0.075 * ws_norm
            + 0.075 * vorp_norm
        )
        score = score_core * minutes_games_factor
        out.append(
            (
                -score,
                -pct_team_minutes,
                -mp,
                current_key,
                player_id,
                player_name,
                pos,
                g,
                gs,
                mp,
                minutes_per_game,
                pct_team_minutes,
                pct_team_pts,
                usage,
                ts,
                pts36,
                score,
            )
        )
    out.sort()
    for rank, rec in enumerate(out[:limit], start=1):
        (
            _,
            _,
            _,
            key,
            player_id,
            player_name,
            pos,
            g,
            gs,
            mp,
            mpg,
            pct_min,
            pct_pts,
            usage,
            ts,
            pts36,
            score,
        ) = rec
        season, team = key.split("|", 1)
        def _fmt(val, digits=1):
            try:
                return ("{0:." + str(digits) + "f}").format(float(val))
            except Exception:
                return ""
        print(
            "\t".join(
                [
                    season,
                    team,
                    player_name,
                    pos,
                    str(g),
                    str(gs),
                    _fmt(mp),
                    _fmt(mpg),
                    _fmt(usage, 1),
                    _fmt(ts, 3),
                    _fmt(pts36),
                    _fmt(pct_min),
                    _fmt(pct_pts),
                    _fmt(score, 3),
                    str(rank),
                ]
            )
        )


def main():
    current_key = None
    rows = []
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 13:
            continue
        (
            key,
            player_id,
            player_name,
            pos,
            g,
            gs,
            mp,
            usage_pct,
            ts_percent,
            pts_per_36,
            team_pts,
            ws_raw,
            vorp_raw,
        ) = (parts + [""] * 13)[:13]
        try:
            g_int = int(float(g))
            mp_float = float(mp)
        except Exception:
            continue
        usage = _to_float(usage_pct)
        ts = _to_float(ts_percent)
        pts36 = _to_float(pts_per_36)
        tpts = _to_float(team_pts)
        ws = _to_float(ws_raw)
        vorp = _to_float(vorp_raw)
        if key != current_key:
            flush(current_key, rows)
            current_key = key
            rows = []
        rows.append((player_id, player_name, pos, g_int, gs, mp_float, usage, ts, pts36, tpts, ws, vorp))
    flush(current_key, rows)


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


if __name__ == "__main__":
    main()
