import argparse
import os
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


DIR_PATTERNS_DEFAULT = [
    "**/__pycache__",
    "**/.pytest_cache",
    "**/.mypy_cache",
    "**/.ipynb_checkpoints",
    "spark-warehouse",
    "outputs",
    str(Path("tools") / "__pycache__"),
]

FILE_PATTERNS_DEFAULT = [
    "**/*.pyc",
    "**/*.pyo",
    "**/*.log",
    "tmp_hp.html",
    "ui_gav_dump.txt",
    "_pdf_text.txt",
    "_csv_to_json.py",
    "_csv_to_json_more.py",
    "_read_pdf_tmp.py",
    "_scan_csvs.py",
    "_scan_csvs_full.py",
    "app.py",
    "app_pyspark_vs_hadoop.py",
]


SAFE_KEEP_DIRS = {
    "src",
    "scripts",
    "bigdata",
    "data",
    "warehouse",
    "web",
    "trino",
    "tools",
}


def rm_tree(p: Path):
    if not p.exists():
        return False
    shutil.rmtree(p, ignore_errors=True)
    return not p.exists()


def rm_file(p: Path):
    try:
        p.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser(description="Clean repository from caches and build artifacts")
    ap.add_argument("--execute", action="store_true", help="Actually delete files (otherwise dry-run)")
    ap.add_argument("--aggressive", action="store_true", help="Also remove results and generated web data")
    args = ap.parse_args()

    to_delete_dirs = []
    for pat in DIR_PATTERNS_DEFAULT:
        to_delete_dirs.extend(sorted(ROOT.glob(pat)))

    to_delete_files = []
    for pat in FILE_PATTERNS_DEFAULT:
        to_delete_files.extend(sorted(ROOT.glob(pat)))

    # Aggressive mode: clear results JSON (keeps folder), and any tmp under web/data
    if args.aggressive:
        to_delete_files.extend(sorted(ROOT.glob("results/pyspark_vs_hadoop.jsonl")))
        to_delete_dirs.extend(sorted(ROOT.glob("web/data/tmp*")))
        # Project folders considered non-essential for core Streamlit flows
        for d in ["metadata", "models", "pages", "reports"]:
            p = ROOT / d
            if p.exists():
                to_delete_dirs.append(p)
        # Trino runtime artifacts and optional examples
        for d in [
            ROOT / "trino" / "queries",
            ROOT / "trino" / "data" / "trino" / "var",
            ROOT / "trino" / "data" / "trino" / "etc",
            ROOT / "trino" / "data" / "trino" / "plugin",
            ROOT / "trino" / "data" / "trino" / "secrets-plugin",
        ]:
            if d.exists():
                to_delete_dirs.append(d)

    # Filter out anything under SAFE_KEEP_DIRS only if it matches exact keep root
    def is_safe(p: Path) -> bool:
        """Never delete the project roots in SAFE_KEEP_DIRS themselves."""
        try:
            rel = p.relative_to(ROOT)
        except Exception:
            return False
        parts = rel.parts
        return len(parts) == 1 and parts[0] in SAFE_KEEP_DIRS

    # Deduplicate and skip venv
    seen = set()
    dirs = []
    for d in to_delete_dirs:
        try:
            if ROOT.joinpath('.venv') in d.parents or d == ROOT.joinpath('.venv'):
                continue
        except Exception:
            pass
        if is_safe(d):
            continue
        if d.exists():
            key = d.resolve()
            if key not in seen:
                seen.add(key)
                dirs.append(d)

    files = []
    for f in to_delete_files:
        try:
            if ROOT.joinpath('.venv') in f.parents or f == ROOT.joinpath('.venv'):
                continue
        except Exception:
            pass
        if is_safe(f):
            continue
        if f.exists():
            key = f.resolve()
            if key not in seen:
                seen.add(key)
                files.append(f)

    print("[clean] Candidates (dirs):", len(dirs))
    for d in dirs:
        print("  DIR ", d)
    print("[clean] Candidates (files):", len(files))
    for f in files:
        print("  FILE", f)

    if not args.execute:
        print("[clean] Dry-run complete. Re-run with --execute to apply.")
        return

    failures = 0
    for d in dirs:
        if not rm_tree(d):
            failures += 1
            print("[clean][WARN] Could not remove dir:", d)
    for f in files:
        if not rm_file(f):
            failures += 1
            print("[clean][WARN] Could not remove file:", f)

    if failures:
        print(f"[clean] Completed with {failures} warnings.")
    else:
        print("[clean] Completed successfully.")


if __name__ == "main__":  # fallback if mis-invoked
    main()

if __name__ == "__main__":
    main()
