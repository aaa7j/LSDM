import argparse
import os
import json
from collections import OrderedDict
from typing import Optional


def run(inp_dir: str, out_dir: str, team_details_path: Optional[str] = None):
    os.makedirs(out_dir, exist_ok=True)
    out_file = os.path.join(out_dir, "part-00000.tsv")
    seen = OrderedDict()  # team_id -> (city, name)
    # Input TSV lines: season \t team_id \t points \t game_id
    for root, _, files in os.walk(inp_dir):
        for name in files:
            if not name.endswith(".tsv"):
                continue
            with open(os.path.join(root, name), "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 2:
                        continue
                    team_id = parts[1]
                    if team_id and team_id not in seen:
                        seen[team_id] = ("", "")

    # If team_details.jsonl is available, enrich with real city/name
    if team_details_path is None:
        cand = os.path.join("data", "json", "team_details.jsonl")
        if os.path.exists(cand):
            team_details_path = cand
    if team_details_path and os.path.exists(team_details_path):
        try:
            with open(team_details_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    tid = str(obj.get("team_id", "")).strip()
                    if not tid or tid not in seen:
                        continue
                    # prefer explicit team_name if present; otherwise use nickname
                    name = obj.get("team_name") or obj.get("nickname") or ""
                    # city nested under meta.city
                    meta = obj.get("meta") or {}
                    city = meta.get("city") or ""
                    if name or city:
                        seen[tid] = (str(city), str(name))
        except Exception:
            # if enrichment fails, keep placeholders
            pass
    with open(out_file, "w", encoding="utf-8") as out:
        for tid, (city, name) in seen.items():
            out.write(f"{tid}\t{city}\t{name}\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--inp", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--team-details", default=None, help="Path to team_details.jsonl (optional)")
    args = ap.parse_args()
    run(args.inp, args.out, args.team_details)


if __name__ == "__main__":
    main()
