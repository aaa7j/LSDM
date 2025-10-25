import csv, json, os
from datetime import datetime

root = r"C:\\Users\\angel\\LSDM\\data"
out_dir = os.path.join(root, 'json')
os.makedirs(out_dir, exist_ok=True)
summary = {}


def write_jsonl(in_path, out_path, transform=None):
    n=0
    with open(in_path, 'r', encoding='utf-8', errors='ignore', newline='') as f, \
         open(out_path, 'w', encoding='utf-8') as g:
        reader = csv.DictReader(f)
        for row in reader:
            rec = transform(row) if transform else row
            g.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n+=1
    return n

# Helpers

def season_label_from_year_token(tok):
    try:
        y = int(str(tok).strip())
        return f"{y}-{str((y+1))[-2:]}"
    except Exception:
        s = str(tok)
        if '-' in s or '/' in s:
            parts = s.replace('/', '-').split('-')
            if parts and parts[0].isdigit():
                y = int(parts[0])
                return f"{y}-{str((y+1))[-2:]}"
        return None

# 1) Team Abbrev.csv -> json/team_abbrev.jsonl
team_abbrev_csv = os.path.join(root, 'Team Abbrev.csv')
if os.path.exists(team_abbrev_csv):
    def ta_transform(r):
        season = r.get('season') or r.get('Season') or r.get('year')
        return {
            'season': season,
            'season_label': season_label_from_year_token(season),
            'league': r.get('lg') or r.get('league'),
            'team_name': r.get('team') or r.get('Team'),
            'abbreviation': r.get('abbreviation') or r.get('abbr'),
            'playoffs': r.get('playoffs')
        }
    n = write_jsonl(team_abbrev_csv, os.path.join(out_dir, 'team_abbrev.jsonl'), ta_transform)
    summary['team_abbrev.jsonl'] = n

# 2) Player Totals.csv -> json/player_totals.jsonl
player_totals_csv = os.path.join(root, 'Player Totals.csv')
if os.path.exists(player_totals_csv):
    def pt_transform(r):
        # keep original names (with x3p etc.) and add season_label
        season = r.get('season') or r.get('Season') or r.get('year')
        rec = dict(r)
        rec['season_label'] = season_label_from_year_token(season)
        return rec
    n = write_jsonl(player_totals_csv, os.path.join(out_dir, 'player_totals.jsonl'), pt_transform)
    summary['player_totals.jsonl'] = n

for k,v in summary.items():
    print(f"WROTE {k}: {v} records")
