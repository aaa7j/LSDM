#!/usr/bin/env python3
import csv
import json
import os

OUT_DIR = os.path.join('web','data')
os.makedirs(OUT_DIR, exist_ok=True)

TSV_FILES = {
    'q1': os.path.join('outputs','hadoop','q1.tsv'),
    'q2': os.path.join('outputs','hadoop','q2.tsv'),
    'q3': os.path.join('outputs','hadoop','q3.tsv'),
}

for key, path in TSV_FILES.items():
    outpath = os.path.join(OUT_DIR, key + '.json')
    items = []
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                # skip empty rows
                if not any(cell.strip() for cell in row):
                    continue
                items.append(row)
    with open(outpath, 'w', encoding='utf-8') as o:
        json.dump({ 'rows': items, 'count': len(items) }, o, ensure_ascii=False, indent=2)
    print('Wrote', outpath)

print('Done writing q1/q2/q3 json files')
