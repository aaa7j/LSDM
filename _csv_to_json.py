import csv, json, os, sys
from datetime import datetime

root = r"C:\\Users\\angel\\LSDM\\data"
out_dir = os.path.join(root, 'json')
os.makedirs(out_dir, exist_ok=True)

summary = {}

def write_jsonl(records, path):
    n=0
    with open(path, 'w', encoding='utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n+=1
    return n

# 1) common_player_info.csv -> common_player_info.jsonl
cpi_csv = os.path.join(root, 'common_player_info.csv')
if os.path.exists(cpi_csv):
    out = []
    with open(cpi_csv, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get('person_id') or row.get('PLAYER_ID') or row.get('id')
            first = row.get('first_name') or row.get('FIRST_NAME') or ''
            last = row.get('last_name') or row.get('LAST_NAME') or ''
            full_name = (row.get('display_first_last') or row.get('display_name') or (first + ' ' + last)).strip()
            rec = {
                'player_id': pid if pid is not None else None,
                'full_name': full_name,
                'first_name': first,
                'last_name': last,
                'birthdate': row.get('birthdate') or row.get('BIRTHDATE'),
                'school': row.get('school') or row.get('SCHOOL'),
                'country': row.get('country') or row.get('COUNTRY'),
                'height': row.get('height') or row.get('HEIGHT'),
                'weight': row.get('weight') or row.get('WEIGHT'),
                'position': row.get('position') or row.get('POSITION'),
                'team_id': row.get('team_id') or row.get('TEAM_ID'),
                'team_name': row.get('team_name') or row.get('TEAM_NAME'),
                'team_abbreviation': row.get('team_abbreviation') or row.get('TEAM_ABBREVIATION'),
                'from_year': row.get('from_year') or row.get('FROM_YEAR'),
                'to_year': row.get('to_year') or row.get('TO_YEAR'),
                'draft_year': row.get('draft_year') or row.get('DRAFT_YEAR'),
                'draft_round': row.get('draft_round') or row.get('DRAFT_ROUND'),
                'draft_number': row.get('draft_number') or row.get('DRAFT_NUMBER')
            }
            out.append(rec)
    count = write_jsonl(out, os.path.join(out_dir, 'common_player_info.jsonl'))
    summary['common_player_info.jsonl'] = count

# 2) team_details.csv -> team_details.jsonl (nested)
td_csv = os.path.join(root, 'team_details.csv')
if os.path.exists(td_csv):
    out = []
    with open(td_csv, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rec = {
                'team_id': row.get('team_id') or row.get('TEAM_ID'),
                'abbreviation': row.get('abbreviation') or row.get('ABBREVIATION'),
                'nickname': row.get('nickname') or row.get('NICKNAME'),
                'meta': {
                    'founded_year': row.get('yearfounded') or row.get('YEARFOUNDED'),
                    'city': row.get('city') or row.get('CITY'),
                    'arena': {
                        'name': row.get('arena') or row.get('ARENA'),
                        'capacity': row.get('arenacapacity') or row.get('ARENACAPACITY')
                    },
                    'owner': row.get('owner') or row.get('OWNER'),
                    'general_manager': row.get('generalmanager') or row.get('GENERALMANAGER'),
                    'head_coach': row.get('headcoach') or row.get('HEADCOACH'),
                    'dleague_affiliation': row.get('dleagueaffiliation') or row.get('DLEAGUEAFFILIATION')
                },
                'social': {
                    'facebook': row.get('facebook') or row.get('FACEBOOK'),
                    'instagram': row.get('instagram') or row.get('INSTAGRAM'),
                    'twitter': row.get('twitter') or row.get('TWITTER')
                }
            }
            out.append(rec)
    count = write_jsonl(out, os.path.join(out_dir, 'team_details.jsonl'))
    summary['team_details.jsonl'] = count

# 3) Games.csv -> games.jsonl with computed season label
from math import isnan

def compute_season(date_str):
    if not date_str:
        return None
    # try common formats
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            y = dt.year
            m = dt.month
            start = y if m >= 10 else y - 1
            end = start + 1
            return f"{start}-{str(end)[-2:]}"
        except Exception:
            continue
    # fallback: extract year tokens
    try:
        y = int(date_str[:4])
        m = 12
        start = y
        end = y+1
        return f"{start}-{str(end)[-2:]}"
    except Exception:
        return None

g_csv = os.path.join(root, 'Games.csv')
if os.path.exists(g_csv):
    out = []
    with open(g_csv, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            gameDate = row.get('gameDate') or row.get('date') or row.get('game_date')
            season = compute_season(gameDate)
            rec = {
                'game_id': row.get('gameId') or row.get('game_id') or row.get('id'),
                'date': gameDate,
                'season': season,
                'home': {
                    'team_id': row.get('hometeamId') or row.get('home_team_id') or row.get('home_team'),
                    'team_city': row.get('hometeamCity') or row.get('home_team_city') or row.get('home_city'),
                    'team_name': row.get('hometeamName') or row.get('home_team_name') or row.get('home_name'),
                    'score': row.get('homeScore') or row.get('home_score')
                },
                'away': {
                    'team_id': row.get('awayteamId') or row.get('away_team_id') or row.get('away_team'),
                    'team_city': row.get('awayteamCity') or row.get('away_team_city') or row.get('away_city'),
                    'team_name': row.get('awayteamName') or row.get('away_team_name') or row.get('away_name'),
                    'score': row.get('awayScore') or row.get('away_score')
                },
                'winner': row.get('winner'),
                'game_type': row.get('gameType') or row.get('game_type'),
                'attendance': row.get('attendance'),
                'arena_id': row.get('arenaId') or row.get('arena_id'),
                'labels': {
                    'label': row.get('gameLabel') or row.get('label'),
                    'sub_label': row.get('gameSubLabel') or row.get('sub_label'),
                    'series_game_number': row.get('seriesGameNumber') or row.get('series_game_number')
                }
            }
            out.append(rec)
    count = write_jsonl(out, os.path.join(out_dir, 'games.jsonl'))
    summary['games.jsonl'] = count

# Print summary
for k,v in summary.items():
    print(f"WROTE {k}: {v} records")
