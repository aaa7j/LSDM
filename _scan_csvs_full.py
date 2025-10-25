import os, re, json, csv
from collections import defaultdict
from datetime import datetime

ROOT = r"C:\\Users\\angel\\LSDM\\data"
REPORT_JSON = r"C:\\Users\\angel\\LSDM\\reports\\csv_audit_full.json"
REPORT_TXT  = r"C:\\Users\\angel\\LSDM\\reports\\selection_plan_full.txt"

SYN = {
  'players': {
    'player_id': {'player_id','playerid','id','pid','person_id','nba_id'},
    'first_name': {'first_name','firstname'},
    'last_name': {'last_name','lastname'},
    'full_name': {'full_name','name','player','player_name','player_full_name','playerfullname','display_name','playerdisplayname'},
    'birth_date': {'birth_date','birthdate','dob','date_of_birth','birth'},
    'team_abbr': {'team_abbr','team','tm','teamcode','team_code','team_id','franchise','franchise_abbr','team_short','abbreviation'},
    'position': {'position','pos'},
    'height_cm': {'height_cm'},
    'height_feet': {'height_feet','feet','ft','height_feet_inches'},
    'height_inches': {'height_inches','inches','inch','in'},
    'height': {'height'},
    'weight_kg': {'weight_kg'},
    'weight_lb': {'weight_lb','weight_pounds','weight','wt'},
    'season': {'season','season_id','season_year','year','yr'},
    'updated_at': {'updated_at','last_update','last_modified','last_updated'}
  },
  'teams': {
    'team_abbr': {'team_abbr','team','tm','team_code','team_id','franchise','franchise_abbr','team_short','abbreviation'},
    'team_name': {'team_name','team','franchise','name','nickname'},
    'coach': {'coach','head_coach','coach_name'},
    'arena_name': {'arena','arena_name','stadium','venue'},
    'city': {'city','location','market'},
    'team_colors': {'team_colors','colors','primary_hex','secondary_hex','color1','color2'},
    'season': {'season','season_id','season_year','year','yr'},
    'updated_at': {'updated_at','last_update','last_modified'}
  },
  'games': {
    'game_id': {'game_id','gameid','gid','id'},
    'date': {'date','game_date','gamedate','datetime'},
    'home_team': {'home_team','home','home_team_abbr','home_abbr','home_team_id','hometeamname','hometeamid','hometeam'},
    'away_team': {'away_team','away','away_team_abbr','away_abbr','away_team_id','awayteamname','awayteamid','awayteam'},
    'season': {'season','season_id','season_year','year','yr'},
    'attendance': {'attendance'},
    'tv_network': {'tv','tv_network','network','broadcast'}
  },
  'stats': {
    'player_id': {'player_id','playerid','id','pid','person_id'},
    'player_name': {'player_name','name','player','full_name','player'},
    'team_abbr': {'team_abbr','team','tm','team_code','team_id','abbreviation'},
    'season': {'season','season_id','season_year','year','yr'},
    'pts': {'pts','points','pts_per_game'},
    'ast': {'ast','assists','ast_per_game'},
    'reb': {'reb','trb','rebounds','trb_per_game'},
    'min': {'min','minutes','mp','mp_per_game'},
    'stl': {'stl','steals'},
    'blk': {'blk','blocks'},
    'fg_pct': {'fg%','fg_pct','fgp'},
    'fg3_pct': {'3p%','3p_pct','fg3_pct'},
    'ft_pct': {'ft%','ft_pct'},
    'per': {'per'},
    'ts_percent': {'ts_percent','ts%'},
    'ws': {'ws'},
    'bpm': {'bpm'},
    'vorp': {'vorp'}
  },
}

CAT_HINTS = {
  'players': {'players','player'},
  'teams': {'teams','team','coach','arena','color'},
  'games': {'games','game','schedule'},
  'stats': {'stats','boxscore','box','advanced'},
  'meta': {'alias','mapping','abbrev'}
}


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+","_", s.strip().lower())


def infer_category(path):
    p = path.replace('\\','/').lower()
    for cat, hints in CAT_HINTS.items():
        if any(h in p for h in hints):
            return cat
    parts = p.split('/')
    for part in parts:
        if part in SYN:
            return part
    return 'meta'

YEAR_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")
# pair not followed by another '-' (to avoid YYYY-MM-DD)
SEASON_PAIR_RE = re.compile(r"(?<!\d)(19\d{2}|20\d{2})\s*[-/\\_]\s*(\d{2}|19\d{2}|20\d{2})(?!-)\b")


def detect_delimiter(header_line: str):
    try:
        dialect = csv.Sniffer().sniff(header_line)
        return dialect.delimiter
    except Exception:
        for d in [',',';','\t','|']:
            if d in header_line:
                return d
        return ','


def read_header(fp):
    try:
        with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
            return f.readline()
    except Exception:
        return ''


def parse_header_cols(header_line, delim):
    return [c.strip() for c in header_line.strip().split(delim) if c.strip()]


def normalize_season_pair(y1: int, y2_token: str) -> str:
    y1 = int(y1)
    if len(y2_token) == 2:
        y2 = int(y2_token)
        # treat values 1..12 as likely months unless column is season-like (handled upstream)
        century = (y1 // 100) * 100
        y2_full = century + y2
        if y2_full < y1:
            y2_full += 100
    else:
        y2_full = int(y2_token)
    return f"{y1}-{y2_full}"


def extract_seasons_from_value(val: str, allow_pairs: bool):
    seasons = []
    if not val:
        return seasons
    if allow_pairs:
        for m in SEASON_PAIR_RE.finditer(val):
            y1, y2 = m.group(1), m.group(2)
            # filter obvious month pairs: if two-digit y2 <= 12 and string contains ':' or looks like time/date
            if len(y2) == 2 and int(y2) <= 12 and (':' in val or re.search(r"\b\d{2}:\d{2}\b", val)):
                continue
            seasons.append(normalize_season_pair(y1, y2))
    for m in YEAR_RE.finditer(val):
        seasons.append(m.group(1))
    return seasons

SEASON_COLS = {'season','season_id','season_year','year','yr'}
DATE_COLS = {'date','game_date','gamedate','datetime'}


def scan_file(fp, cols_norm, delim):
    seasons = set()
    rows = 0
    try:
        with open(fp, 'r', encoding='utf-8', errors='ignore', newline='') as f:
            reader = csv.reader(f, delimiter=delim)
            next(reader, None)
            idx = {c:i for i,c in enumerate(cols_norm)}
            have_season = [c for c in SEASON_COLS if c in idx]
            have_date = [c for c in DATE_COLS if c in idx]
            for row in reader:
                rows += 1
                for c in have_season:
                    j = idx[c]
                    if j < len(row):
                        seasons.update(extract_seasons_from_value(str(row[j]), allow_pairs=True))
                for c in have_date:
                    j = idx[c]
                    if j < len(row):
                        seasons.update(extract_seasons_from_value(str(row[j]), allow_pairs=False))
    except Exception:
        pass
    return seasons, rows


def coverage_for_category(cat, cols_norm):
    syn = SYN.get(cat, {})
    covered = {}
    for target, names in syn.items():
        hit = False
        for n in names:
            if n == 'first_name+last_name':
                if 'first_name' in cols_norm and 'last_name' in cols_norm:
                    hit = True
                    break
            if n in cols_norm:
                hit = True
                break
        covered[target] = hit
    if cat == 'players':
        h_ok = covered.get('height_cm') or (covered.get('height_feet') and covered.get('height_inches')) or covered.get('height')
        covered['height_any'] = bool(h_ok)
        w_ok = covered.get('weight_kg') or covered.get('weight_lb')
        covered['weight_any'] = bool(w_ok)
        name_ok = covered.get('full_name') or (covered.get('first_name') and covered.get('last_name'))
        covered['name_any'] = bool(name_ok)
    return covered

entries = []
for dirpath, _, filenames in os.walk(ROOT):
    for fn in filenames:
        if not fn.lower().endswith('.csv'):
            continue
        fp = os.path.join(dirpath, fn)
        rel = os.path.relpath(fp, ROOT).replace('\\','/')
        header = read_header(fp)
        delim = detect_delimiter(header)
        cols = parse_header_cols(header, delim)
        cols_norm = [norm(c) for c in cols]
        cat = infer_category(rel)
        seasons, rows = scan_file(fp, cols_norm, delim)
        seasons.update(extract_seasons_from_value(os.path.basename(fp), allow_pairs=True))
        seasons_list = sorted(set(seasons))
        cov = coverage_for_category(cat, cols_norm)
        richness = sum(1 for v in cov.values() if v)
        if cov.get('player_id') or cov.get('team_abbr') or cov.get('game_id'):
            richness += 1
        if any(s in cols_norm for s in SEASON_COLS):
            richness += 1
        entries.append({
            'path': rel,
            'category': cat,
            'delimiter': delim,
            'n_columns': len(cols),
            'rows_estimate': rows,
            'columns': cols,
            'columns_norm': cols_norm,
            'seasons_found': seasons_list,
            'seasons_count': len(seasons_list),
            'season_min': seasons_list[0] if seasons_list else None,
            'season_max': seasons_list[-1] if seasons_list else None,
            'coverage': cov,
            'richness_score': richness,
        })

by_cat = defaultdict(list)
for e in entries:
    by_cat[e['category']].append(e)

selection = {}
for cat, arr in by_cat.items():
    cov_sorted = sorted(arr, key=lambda x: (x['seasons_count'], x['richness_score'], x['rows_estimate']), reverse=True)
    rich_sorted = sorted(arr, key=lambda x: (x['richness_score'], x['seasons_count'], x['rows_estimate']), reverse=True)
    selection[cat] = {
        'best_coverage_top': cov_sorted[:5],
        'best_richness_top': rich_sorted[:5],
    }

out = {
  'root': ROOT,
  'generated_at': datetime.now().isoformat(),
  'total_csv': len(entries),
  'entries': entries,
  'selection': selection,
}

os.makedirs(os.path.dirname(REPORT_JSON), exist_ok=True)
with open(REPORT_JSON, 'w', encoding='utf-8') as f:
    json.dump(out, f, indent=2, ensure_ascii=False)

lines = []
lines.append(f"CSV AUDIT (full) @ {out['generated_at']}")
lines.append("")
for cat in ['players','teams','games','stats','meta']:
    arr = by_cat.get(cat, [])
    lines.append(f"== {cat.upper()} ({len(arr)} files) ==")
    show = sorted(arr, key=lambda x: (x['seasons_count'], x['richness_score']), reverse=True)[:20]
    for e in show:
        seas = e['seasons_count']
        rng = f"{e['season_min']} → {e['season_max']}" if seas else "-"
        lines.append(f"- {e['path']} | cols={e['n_columns']}, rows≈{e['rows_estimate']} | seasons={seas} ({rng}) | score={e['richness_score']}")
    lines.append("")
    picks = selection.get(cat, {})
    if picks:
        lines.append("  Suggested picks:")
        for tag, lst in picks.items():
            for e in lst:
                seas = e['seasons_count']
                rng = f"{e['season_min']} → {e['season_max']}" if seas else "-"
                lines.append(f"    - ({tag}) {e['path']} -> seasons={seas} ({rng}), score={e['richness_score']}")
    lines.append("")

with open(REPORT_TXT, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print('OK')
