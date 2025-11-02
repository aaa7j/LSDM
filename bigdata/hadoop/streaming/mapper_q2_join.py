import os
import sys


def main():
    try:
        thr = int(os.environ.get("Q2_THRESHOLD", "120"))
    except Exception:
        thr = 120
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        # Expect TSV from flatten: season \t team_id \t points \t game_id [\t ...]
        if len(parts) < 3:
            continue
        season, team_id, points = parts[0], parts[1], parts[2]
        # Emit two counters per game: total=1, high=(points>=thr)
        key = season + "|" + team_id
        print("\t".join([key, "T", "1"]))
        try:
            p = int(points)
            if p >= thr:
                print("\t".join([key, "H", "1"]))
        except Exception:
            pass


if __name__ == "__main__":
    main()
