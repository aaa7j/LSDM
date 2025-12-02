import sys


def main():
    """
    Input TSV (built by build_q4_multifactor.py):
      season, team, player_id, player_name, pos, g, gs, mp, usage_pct, ts_percent,
      pts_per_36, team_pts, ws, vorp
    Output to reducer: key=season|team, plus all numeric fields for scoring.
    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 12:
            continue
        (
            season,
            team,
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
            ws,
            vorp,
        ) = (parts + [""] * 14)[:14]
        try:
            g_int = int(float(g or 0))
            mp_float = float(mp)
        except Exception:
            continue
        key = "{}|{}".format(season, team)
        print("\t".join([
            key,
            player_id or "",
            player_name or "",
            pos or "",
            str(g_int),
            str(gs or ""),
            "{:.3f}".format(mp_float),
            usage_pct or "",
            ts_percent or "",
            pts_per_36 or "",
            team_pts or "",
            ws or "",
            vorp or "",
        ]))


if __name__ == "__main__":
    main()
