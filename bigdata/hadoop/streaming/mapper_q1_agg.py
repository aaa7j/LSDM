import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        season, team_id, points = parts[0], parts[1], parts[2]
        try:
            p = int(points)
        except Exception:
            continue
        # Emit a single-field composite key to ensure Hadoop Streaming sorts
        # and groups by BOTH season and team_id (default grouping is on the
        # first field only). We use '|' as a safe separator not present in keys.
        key = season + "|" + team_id
        print(key + "\t" + str(p))


if __name__ == "__main__":
    main()
