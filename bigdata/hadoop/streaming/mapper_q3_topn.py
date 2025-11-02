import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        season, team_id, points, game_id = parts[0], parts[1], parts[2], parts[3]
        try:
            p = int(points)
        except Exception:
            continue
        # emit: team_id \t points \t season \t game_id
        print("\t".join([team_id, str(p), season, game_id]))


if __name__ == "__main__":
    main()
