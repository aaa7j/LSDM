import sys


def main():
    cur_key = None
    s = 0
    c = 0
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t")
        # Expect mapper output as: "season|team_id\tpoints"
        if len(parts) < 2:
            continue
        key = parts[0]
        try:
            p = int(parts[1])
        except Exception:
            continue
        if cur_key is None:
            cur_key = key
        if key != cur_key:
            try:
                season, team_id = cur_key.split("|", 1)
            except ValueError:
                # Fallback: if key not composite, skip emission
                cur_key = key
                s = 0
                c = 0
                continue
            avg = (s / c) if c else 0
            # format avg to 1 decimal place
            print("\t".join([season, team_id, str(s), "{:.1f}".format(avg), str(c)]))
            cur_key = key
            s = 0
            c = 0
        s += p
        c += 1
    if cur_key is not None:
        try:
            season, team_id = cur_key.split("|", 1)
        except ValueError:
            return
        avg = (s / c) if c else 0
        print("\t".join([season, team_id, str(s), "{:.1f}".format(avg), str(c)]))


if __name__ == "__main__":
    main()
