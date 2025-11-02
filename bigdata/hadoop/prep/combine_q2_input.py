import argparse
import glob
import os


def main():
    ap = argparse.ArgumentParser(description="Combine Q1 agg and teams dim into tagged TSV for Hadoop join")
    ap.add_argument("--agg", required=True, help="Path to q1 agg TSV (season\tteam_id\ttotal\tavg\tcount)")
    ap.add_argument("--dim", required=True, help="Dir with teams_dim TSV files (team_id\tcity\tname)")
    ap.add_argument("--out", required=True, help="Output combined TSV path")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    # write with explicit LF newline to avoid CRLF issues on Windows hosts
    with open(args.out, "w", encoding="utf-8", newline="\n") as out:
        # A lines: preserve tabs/empty fields, only strip trailing newline
        with open(args.agg, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                out.write("A\t" + line + "\n")
        # B lines
        # B lines: preserve trailing empty fields (city/name) so join keys remain
        for path in glob.glob(os.path.join(args.dim, "**", "*.tsv"), recursive=True):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    out.write("B\t" + line + "\n")


if __name__ == "__main__":
    main()

