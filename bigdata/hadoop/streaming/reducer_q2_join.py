import sys


def flush(key, totals):
    if key is None:
        return
    try:
        season, team_id = key.split("|", 1)
    except ValueError:
        return
    high = totals.get("H", 0)
    tot = totals.get("T", 0)
    pct = (float(high) / tot) if tot else 0.0
    # Output: season \t team_id \t team_name(blank) \t high_games \t total_games \t pct_high
    out = [season, team_id, "", str(high), str(tot), "{0:.3f}".format(pct)]
    print("\t".join(out))


def main():
    cur_key = None
    sums = {"H": 0, "T": 0}
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        # Expect: season|team_id \t (H|T) \t 1
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        key = parts[0]
        tag = parts[1]
        try:
            val = int(parts[2])
        except Exception:
            val = 0
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            flush(cur_key, sums)
            cur_key = key
            sums = {"H": 0, "T": 0}
        if tag in ("H", "T"):
            sums[tag] = sums.get(tag, 0) + val
    flush(cur_key, sums)


if __name__ == "__main__":
    main()
