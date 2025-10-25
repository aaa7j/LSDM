import os
import csv
import json
import re
import tempfile
import psycopg2
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DBCFG = os.path.join(ROOT, 'db', 'config.json')


def load_cfg():
    with open(DBCFG, 'r', encoding='utf-8') as f:
        return json.load(f)


def sanitize_name(name: str) -> str:
    base = os.path.splitext(os.path.basename(name))[0]
    base = base.strip().lower()
    base = re.sub(r'[^a-z0-9]+', '_', base)
    base = re.sub(r'_+', '_', base).strip('_')
    if not base:
        base = 'table'
    return base


def read_header(path: str):
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
        cols = []
        seen = set()
        for h in header:
            h = (h or '').strip().lower()
            h = re.sub(r'[^a-z0-9]+', '_', h)
            h = re.sub(r'_+', '_', h).strip('_') or 'col'
            # deduplicate
            orig = h
            k = 2
            while h in seen:
                h = f"{orig}_{k}"
                k += 1
            seen.add(h)
            cols.append(h)
        return cols, delim


def sanitize_csv(path: str, delim: str, null_tokens=("NA", "N/A", "NaN", "nan", "NULL")):
    fd, tmp = tempfile.mkstemp(prefix='san_all_', suffix='.csv')
    os.close(fd)
    with open(path, 'r', encoding='utf-8', errors='ignore', newline='') as f, \
         open(tmp, 'w', encoding='utf-8', newline='') as g:
        reader = csv.reader(f, delimiter=delim)
        writer = csv.writer(g, delimiter=',')  # write as comma CSV
        for i, row in enumerate(reader):
            if i == 0:
                writer.writerow(row)
                continue
            cleaned = []
            for c in row:
                if c is None:
                    cleaned.append('')
                    continue
                s = str(c).strip()
                if s.upper() in {t.upper() for t in null_tokens}:
                    cleaned.append('')
                else:
                    cleaned.append(s)
            writer.writerow(cleaned)
    return tmp


def is_int(s: str) -> bool:
    try:
        int(s)
        return True
    except Exception:
        return False


def is_float(s: str) -> bool:
    try:
        float(s)
        return True
    except Exception:
        return False


def is_bool(s: str) -> bool:
    # Consider only textual booleans, not numeric 0/1 (to avoid misclassifying numeric columns)
    return s.lower() in {"true", "false", "t", "f", "yes", "no", "y", "n"}


def is_date(s: str) -> bool:
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            datetime.strptime(s, fmt)
            return True
        except Exception:
            continue
    return False


def infer_types(path: str, delim: str, cols: list, sample_rows: int = 2000):
    # Returns list of Postgres types aligned to cols
    counts = [{"int": 0, "float": 0, "bool": 0, "date": 0, "text": 0, "nonnull": 0} for _ in cols]
    null_set = {"", "na", "n/a", "nan", "null"}
    with open(path, 'r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.reader(f, delimiter=delim)
        next(reader, None)
        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            # align length
            if len(row) < len(cols):
                row = row + [None] * (len(cols) - len(row))
            elif len(row) > len(cols):
                row = row[: len(cols)]
            for j, raw in enumerate(row):
                if raw is None:
                    continue
                s = str(raw).strip()
                if s.lower() in null_set:
                    continue
                counts[j]["nonnull"] += 1
                # strip percent symbols and commas for numeric detection
                s_norm = s.replace('%', '').replace(',', '')
                if is_bool(s):
                    counts[j]["bool"] += 1
                elif is_int(s_norm):
                    counts[j]["int"] += 1
                elif is_float(s_norm):
                    counts[j]["float"] += 1
                elif is_date(s):
                    counts[j]["date"] += 1
                else:
                    counts[j]["text"] += 1
    types = []
    for j, c in enumerate(counts):
        if c["nonnull"] == 0:
            types.append("text")
            continue
        # precedence: int -> float -> date -> boolean -> text
        if c["int"] == c["nonnull"]:
            types.append("int")
        elif (c["int"] + c["float"]) == c["nonnull"]:
            types.append("double precision")
        elif c["date"] == c["nonnull"]:
            types.append("date")
        elif c["bool"] == c["nonnull"]:
            types.append("boolean")
        else:
            types.append("text")
    return types


def main():
    cfg = load_cfg()
    conn = psycopg2.connect(
        host=cfg['host'], port=cfg['port'], dbname=cfg['dbname'], user=cfg['user'], password=cfg['password']
    )
    try:
        base = os.path.join(ROOT, 'data')
        files = [
            os.path.join(base, f) for f in os.listdir(base)
            if f.lower().endswith('.csv')
        ]
        # ensure schema exists
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS staging")
            conn.commit()
        for fp in files:
            cols, delim = read_header(fp)
            if not cols:
                print(f"[SKIP] empty header: {os.path.basename(fp)}")
                continue
            base = sanitize_name(fp)
            # Normalize specific names to avoid duplicates (align with existing staging tables)
            if base == 'advanced':
                base = 'player_advanced'
            tbl = f"staging.{base}"
            types = infer_types(fp, delim, cols)
            col_defs = ", ".join(f'"{c}" {t}' for c, t in zip(cols, types))
            ddl = f"DROP TABLE IF EXISTS {tbl} CASCADE; CREATE TABLE {tbl} ({col_defs});"
            print(f"[DDL] {tbl}")
            with conn.cursor() as cur:
                cur.execute(ddl)
                conn.commit()
            tmp = sanitize_csv(fp, delim)
            try:
                with open(tmp, 'r', encoding='utf-8') as f, conn.cursor() as cur:
                    cur.copy_expert(sql=f"COPY {tbl} FROM STDIN WITH (FORMAT csv, HEADER true, NULL '')", file=f)
                conn.commit()
                print(f"[OK] {tbl} <- {os.path.basename(fp)}")
            finally:
                try:
                    os.remove(tmp)
                except Exception:
                    pass
        print("[DONE] All CSVs loaded into staging.* (typed)")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
