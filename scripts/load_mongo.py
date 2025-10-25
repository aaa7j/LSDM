import os
import json
import csv
from typing import Iterable, Dict
from pymongo import MongoClient
import glob
import re
import argparse

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MONGO_CFG = os.path.join(ROOT, 'db', 'mongo_config.json')


def load_cfg():
    with open(MONGO_CFG, 'r', encoding='utf-8') as f:
        return json.load(f)


def sanitize_name(name: str) -> str:
    base = os.path.splitext(os.path.basename(name))[0]
    base = base.strip().lower()
    base = re.sub(r'[^a-z0-9]+', '_', base)
    base = re.sub(r'_+', '_', base).strip('_')
    return base or 'collection'


def iter_jsonl(path: str) -> Iterable[Dict]:
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                # skip bad lines
                continue


def to_number(val: str):
    if val is None:
        return None
    s = str(val).strip()
    if s == '' or s.upper() in {'NA', 'N/A', 'NAN', 'NULL'}:
        return None
    try:
        if '.' in s:
            return float(s.replace(',', ''))
        return int(s.replace(',', ''))
    except Exception:
        return s


def iter_csv(path: str) -> Iterable[Dict]:
    # Robust CSV reader: sniff delimiter; drop empty first header if present
    with open(path, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
            delim = dialect.delimiter
        except Exception:
            delim = ','
        reader = csv.reader(f, delimiter=delim)
        header = next(reader)
        if header and (header[0] is None or header[0].strip() == ''):
            header = header[1:]
        header = [h.strip() if h else '' for h in header]
        for row in reader:
            if not row:
                continue
            if len(row) > len(header):
                # trim extra cells
                row = row[: len(header)]
            doc = {}
            for k, v in zip(header, row):
                key = k or 'col'
                doc[key] = to_number(v)
            yield doc


def bulk_load(coll, docs: Iterable[Dict], batch_size: int = 5000):
    batch = []
    n = 0
    for d in docs:
        batch.append(d)
        if len(batch) >= batch_size:
            coll.insert_many(batch, ordered=False)
            n += len(batch)
            batch = []
    if batch:
        coll.insert_many(batch, ordered=False)
        n += len(batch)
    return n


def main():
    parser = argparse.ArgumentParser(description="Load all JSON/JSONL from data/json into MongoDB.")
    parser.add_argument("--force-all", action="store_true", help="Do not skip any JSON even if a CSV counterpart exists")
    args = parser.parse_args()

    cfg = load_cfg()
    client = MongoClient(cfg['uri'])
    db = client[cfg['database']]

    data_dir = os.path.join(ROOT, 'data')
    json_dir = os.path.join(data_dir, 'json')

    # Generic: import all *.jsonl and *.json under data/json
    json_files = list(glob.glob(os.path.join(json_dir, '*.jsonl'))) + list(glob.glob(os.path.join(json_dir, '*.json')))
    # Build set of CSV basenames to avoid duplicates across DBs
    csv_bases = set()
    if not args.force_all:
        csv_dir = os.path.join(ROOT, 'data')
        for f in os.listdir(csv_dir):
            if f.lower().endswith('.csv'):
                b = re.sub(r'[^a-z0-9]+', '_', os.path.splitext(f)[0].strip().lower())
                b = re.sub(r'_+', '_', b).strip('_')
                csv_bases.add(b)
        if csv_bases:
            print(f"[INFO] CSV datasets detected in Postgres (will skip matching JSON): {sorted(list(csv_bases))[:5]}{' ...' if len(csv_bases)>5 else ''}")
    for path in sorted(json_files):
        coll_name = sanitize_name(os.path.basename(path))
        if coll_name.endswith('_json'):
            coll_name = coll_name[:-5]
        # Skip if a CSV with equivalent basename exists to avoid duplicates
        if csv_bases and coll_name in csv_bases:
            print(f"[SKIP duplicate] {coll_name} has CSV counterpart in Postgres")
            continue
        coll = db[coll_name]
        print(f"[LOAD] {coll_name} <- {os.path.relpath(path, ROOT)}")
        coll.drop()
        n = bulk_load(coll, iter_jsonl(path))
        print(f"[OK] {coll_name}: {n} docs")

    client.close()


if __name__ == '__main__':
    main()
