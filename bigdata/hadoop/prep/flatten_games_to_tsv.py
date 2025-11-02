import argparse
import json
import os


def emit_line(out, season, team_id, points, game_id):
    out.write(f"{season}\t{team_id}\t{points}\t{game_id}\n")


def run(games_path: str, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "part-00000.tsv")
    with open(games_path, "r", encoding="utf-8") as inp, open(out_file, "w", encoding="utf-8") as out:
        for line in inp:
            if not line.strip():
                continue
            obj = json.loads(line)
            season = obj.get("season")
            gid = obj.get("game_id")
            home = obj.get("home") or {}
            away = obj.get("away") or {}
            h_id = home.get("team_id")
            a_id = away.get("team_id")
            h_points = int(home.get("score")) if home.get("score") is not None else None
            a_points = int(away.get("score")) if away.get("score") is not None else None
            if season and gid and h_id and h_points is not None:
                emit_line(out, season, h_id, h_points, gid)
            if season and gid and a_id and a_points is not None:
                emit_line(out, season, a_id, a_points, gid)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    run(args.games, args.out)


if __name__ == "__main__":
    main()

