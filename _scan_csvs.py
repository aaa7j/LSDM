import os, re, json, csv
from collections import defaultdict
from datetime import datetime

root = r"C:\\Users\\angel\\LSDM\\data"
report_json = r"C:\\Users\\angel\\LSDM\\reports\\csv_audit.json"
report_txt  = r"C:\\Users\\angel\\LSDM\\reports\\selection_plan.txt"

SYN = {
  'players': {
    'player_id': {'player_id','playerid','id','pid','person_id','nba_id'},
    'full_name': {'full_name','name','player','player_name','player_full_name','playerfullname','display_name','playerdisplayname', 'first_name+last_name'},
    'birth_date': {'birth_date','birthdate','dob','date_of_birth','birth'},
    'team_abbr': {'team_abbr','team','tm','teamcode','team_code','team_id','franchise','franchise_abbr','team_short'},
    'position': {'position','pos'},
    'height_cm': {'height_cm','height'},
    'height_feet': {'height_feet','feet','ft'},
    'height_inches': {'height_inches','inches','inch','in'},
    'weight_kg': {'weight_kg'},
    'weight_lb': {'weight_lb','weight_pounds','weight','wt'},
    'season': {'season','season_id','season_year','year'},
    'updated_at': {'updated_at','last_update','last_modified','last_updated'}
  },
  'teams': {
    'team_abbr': {'team_abbr','team','tm','team_code','team_id','franchise','franchise_abbr','team_short'},
    'team_name': {'team_name','team','franchise','name'},
    'coach': {'coach','head_coach','coach_name'},
    'arena_name': {'arena','arena_name','stadium','venue'},
    'city': {'city','location','market'},
    'team_colors': {'team_colors','colors','primary_hex','secondary_hex','color1','color2'},
    'season': {'season','season_id','season_year','year'},
    'updated_at': {'updated_at','last_update','last_modified'}
  },
  'games': {
    'game_id': {'game_id','gameid','gid','id'},
    'date': {'date','game_date','gamedate','datetime'},
    'home_team': {'home_team','home','home_team_abbr','home_abbr','home_team_id'},
    'away_team': {'away_team','away','away_team_abbr','away_abbr','away_team_id'},
    'season': {'season','season_id','season_year','year'},
    'attendance': {'attendance'},
    'tv_network': {'tv','tv_network','network','broadcast'}
  },
  'stats': {
    'player_id': {'player_id','playerid','id','pid','person_id'},
    'player_name': {'player_name','name','player','full_name'},
    'team_abbr': {'team_abbr','team','tm','team_code','team_id'},
    'season': {'season','season_id','season_year','year'},
    'pts': {'pts','points'},
    'ast': {'ast','assists'},
    'reb': {'reb','trb','rebounds'},
    'min': {'min','minutes','mp'},
    'stl': {'stl','steals'},
    'blk': {'blk','blocks'},
    'fg_pct': {'fg%','fg_pct','fgp'},
    'fg3_pct': {'3p%','3p_pct','fg3_pct'},
    'ft_pct': {'ft%','ft_pct'}
  },
}

CAT_HINTS = {
  'players': {'players','player'},
  'teams': {'teams','team','coach','arena','color'},
  'games': {'games','game','schedule'},
  'stats': {'stats','boxscore','box','advanced'},
  'meta': {'alias','mapping'}
}


def norm(s: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]+","_", s.strip().lower())


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


def detect_delimiter(header_line):
    try:
        dialect = csv.Sniffer().sniff(header_line)
        return dialect.delimiter
    except Exception:
        for d in [',',';','\t','|']:
            if d in header_line:
                return d
        return ','


def read_header_and_delim(fp):
    cols, delim = [], ','
    try:
        with open(fp, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline()
        delim = detect_delimiter(header)
        cols = [c.strip() for c in header.strip().split(delim)]
    except Exception:
        pass
    return cols, delim


def quick_season_scan(fp, cols_norm, delim, limit=10000):
    seasons = set()
    idx = {c:i for i,c in enumerate(cols_norm)}
    target_cols = [c for c in ['season','season_id','season_year','year','date','game_date','gamedate','datetime'] if c in idx]
    if not target_cols:
        # fallback to filename
        base = os.path.basename(fp)
        seasons.update(re.findall(r"(20\d{2})(?:\D(20\d{2}))?", base))
        return simplify_seasons(seasons)
    # stream rows
    try:
        with open(fp, 'r', encoding='utf-8', errors='ignore', newline='') as f:
            reader = csv.reader(f, delimiter=delim)
            next(reader, None)  # skip header
            for i, row in enumerate(reader):
                if i >= limit:
                    break
                for c in target_cols:
                    j = idx[c]
                    if j < len(row):
                        val = str(row[j]).strip()
                        if not val:
                            continue
                        if c in {'season','season_id','season_year','year'}:
                            # patterns: 2020, 2019-20, 2019/2020
                            m = re.findall(r"(20\d{2})(?:\D(20\d{2}))?", val)
                            for g in m:
                                seasons.add(g)
                        else:
                            m = re.findall(r"(20\d{2})", val)
                            for yy in m:
                                seasons.add((yy,''))
    except Exception:
        pass
    return simplify_seasons(seasons)


def simplify_seasons(raw):
    seasons = set()
    for g in raw:
        if isinstance(g, tuple):
            a,b = g
            if b:
                seasons.add(f"{a}-{b}")
            else:
                seasons.add(a)
        else:
            s = str(g)
            m = re.findall(r"(20\d{2})(?:\D(20\d{2}))?", s)
            for a,b in m:
                seasons.add(f"{a}-{b}" if b else a)
    return sorted(seasons)


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
        h_ok = covered.get('height_cm') or (covered.get('height_feet') and covered.get('height_inches'))
        covered['height'] = bool(h_ok)
        w_ok = covered.get('weight_kg') or covered.get('weight_lb')
        covered['weight'] = bool(w_ok)
    return covered

entries = []
for dirpath, _, filenames in os.walk(root):
    for fn in filenames:
        if not fn.lower().endswith('.csv'):
            continue
        fp = os.path.join(dirpath, fn)
        rel = os.path.relpath(fp, root)
        cols, delim = read_header_and_delim(fp)
        cols_norm = [norm(c) for c in cols]
        cat = infer_category(rel)
        seasons = quick_season_scan(fp, cols_norm, delim, limit=15000)
        cov = coverage_for_category(cat, cols_norm)
        richness = sum(1 for v in cov.values() if v)
        if cov.get('player_id') or cov.get('team_abbr') or cov.get('game_id'):
            richness += 1
        if cov.get('season'):
            richness += 1
        entries.append({
            'path': rel.replace('\\','/'),
            'category': cat,
            'delimiter': delim,
            'n_columns': len(cols),
            'columns': cols,
            'columns_norm': cols_norm,
            'seasons_found': seasons,
            'seasons_count': len(set(seasons)),
            'coverage': cov,
            'richness_score': richness,
        })

by_cat = defaultdict(list)
for e in entries:
    by_cat[e['category']].append(e)

selection = {}
for cat, arr in by_cat.items():
    arr_sorted_cov = sorted(arr, key=lambda x: (x['seasons_count'], x['richness_score']), reverse=True)
    arr_sorted_rich = sorted(arr, key=lambda x: (x['richness_score'], x['seasons_count']), reverse=True)
    selection[cat] = {
        'best_coverage': arr_sorted_cov[:3],
        'best_richness': arr_sorted_rich[:3],
    }

out = {
  'root': root,
  'total_csv': len(entries),
  'entries': entries,
  'selection': selection,
}
with open(report_json, 'w', encoding='utf-8') as f:
    json.dump(out, f, indent=2, ensure_ascii=False)

lines = []
lines.append(f"CSV AUDIT @ {datetime.now().isoformat()}")
lines.append("")
for cat in ['players','teams','games','stats','meta']:
    arr = by_cat.get(cat, [])
    lines.append(f"== {cat.upper()} ({len(arr)} files) ==")
    for e in sorted(arr, key=lambda x: (x['richness_score'], x['seasons_count']), reverse=True)[:15]:
        seas = ','.join(e['seasons_found'][:6]) + ('' if e['seasons_count']<=6 else ', ...')
        lines.append(f"- {e['path']} | cols={e['n_columns']} | seasons={e['seasons_count']} [{seas}] | score={e['richness_score']}")
    lines.append("")
    picks = selection.get(cat, {})
    if picks:
        lines.append("  Suggested picks:")
        for tag, lst in picks.items():
            for e in lst:
                seas = ','.join(e['seasons_found'][:6]) + ('' if e['seasons_count']<=6 else ', ...')
                lines.append(f"    - ({tag}) {e['path']} -> seasons={e['seasons_count']} [{seas}], score={e['richness_score']}")
    lines.append("")

with open(report_txt, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines))

print('OK')
