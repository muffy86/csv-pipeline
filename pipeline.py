#!/usr/bin/env python3
"""
CSV Pipeline Processor — Multi-stage data pipeline (Python Edition)

Processes CSV files through a 4-stage pipeline:
1. Ingest:     Read raw CSV(s) from incoming/ directory
2. Validate:   Check for missing values, data types
3. Transform:  Clean, normalize, enrich data
4. Export:     Output to CSV, JSON, and generate summary report

Equivalent of the original csv-pipeline.ps1 PowerShell script.
"""

import argparse
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas", file=sys.stderr)
    sys.exit(1)


# ── ANSI helpers ──────────────────────────────────────────────────────────────

class Color:
    """Minimal ANSI color codes — falls back to empty strings if output is piped."""
    _enable = sys.stdout.isatty()

    @classmethod
    def _c(cls, code, text):
        return f"\033[{code}m{text}\033[0m" if cls._enable else str(text)

    @classmethod
    def cyan(cls, text):    return cls._c(36, text)
    @classmethod
    def green(cls, text):   return cls._c(32, text)
    @classmethod
    def yellow(cls, text):  return cls._c(33, text)
    @classmethod
    def magenta(cls, text): return cls._c(35, text)
    @classmethod
    def red(cls, text):     return cls._c(31, text)
    @classmethod
    def darkgray(cls, text): return cls._c(90, text)


# ── STAGE 1: Ingest ───────────────────────────────────────────────────────────

def stage_ingest(file_path: str) -> pd.DataFrame:
    """Read a CSV file and return a DataFrame."""
    print(f"\n{Color.yellow('[STAGE 1/4] Ingest')}")
    try:
        df = pd.read_csv(file_path)
        print(f"  {Color.green('✓')} Loaded {len(df)} rows from {Path(file_path).name}")
        return df
    except Exception as exc:
        print(f"  {Color.red('✗')} Failed to read CSV: {exc}")
        return None


# ── STAGE 2: Validate ─────────────────────────────────────────────────────────

def stage_validate(df: pd.DataFrame) -> list:
    """Check for missing values and data-quality issues. Returns a list of issue strings."""
    print(f"\n{Color.yellow('[STAGE 2/4] Validate')}")
    print(f"  Columns detected: {', '.join(df.columns)}")

    issues = []
    null_counts = df.isnull().sum()
    total_nulls = int(null_counts.sum())

    if total_nulls > 0:
        print(f"  {Color.yellow('⚠')} Found {total_nulls} missing values across dataset")
        for col in null_counts.index:
            count = int(null_counts[col])
            if count > 0:
                issues.append(f"Missing values in '{col}': {count}")
                print(f"    {Color.darkgray('·')} '{col}': {count} missing")
    else:
        print(f"  {Color.green('✓')} All values present")

    return issues


# ── STAGE 3: Transform ────────────────────────────────────────────────────────

def stage_transform(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Normalize types, trim whitespace, add metadata columns."""
    print(f"\n{Color.yellow('[STAGE 3/4] Transform')}")

    tdf = df.copy()

    # Strip whitespace from all string columns
    for col in tdf.select_dtypes(include="object").columns:
        tdf[col] = tdf[col].astype(str).str.strip()
        # Restore actual NaN values that were converted to string "nan"
        tdf[col] = tdf[col].replace("nan", pd.NA)

    # Auto-detect and convert numeric columns (strings like "95,000" → 95000.0)
    for col in tdf.columns:
        if tdf[col].dtype == "object":
            # Try to extract a numeric pattern (allows commas and decimals)
            sample = tdf[col].dropna()
            if len(sample) > 0:
                # Heuristic: if >60% of non-null values look numeric, convert
                numeric_count = sample.apply(
                    lambda v: bool(re.match(r'^[\d,]+\.?\d*$', str(v).strip()))
                ).sum()
                if numeric_count > 0.6 * len(sample):
                    tdf[col] = (
                        tdf[col]
                        .astype(str)
                        .str.replace(",", "", regex=False)
                        .str.strip()
                        .replace("nan", pd.NA)
                    )
                    tdf[col] = pd.to_numeric(tdf[col], errors="coerce")

    # Add metadata columns
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tdf["processed_at"] = now_str
    tdf["source_file"] = Path(source_file).name

    print(f"  {Color.green('✓')} Transformed {len(tdf)} rows (types normalized, metadata added)")
    return tdf


# ── STAGE 4: Export ───────────────────────────────────────────────────────────

def stage_export(df: pd.DataFrame, output_dir: str, source_file: str, issues: list, original_rows: int):
    """Write processed data to CSV, JSON, and generate a summary report."""
    print(f"\n{Color.yellow('[STAGE 4/4] Export')}")

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    base_name = Path(source_file).stem
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

    # ── CSV export ──
    csv_path = output_path / f"{base_name}_processed_{timestamp}.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    csv_size_kb = csv_path.stat().st_size / 1024
    print(f"  {Color.green('✓')} CSV: {csv_path} ({csv_size_kb:.1f} KB)")

    # ── JSON export ──
    json_path = output_path / f"{base_name}_processed_{timestamp}.json"
    df.to_json(json_path, orient="records", indent=2, force_ascii=False)
    print(f"  {Color.green('✓')} JSON: {json_path}")

    # ── Summary report ──
    summary = {
        "PipelineRun": datetime.now(timezone.utc).isoformat(),
        "SourceFile": Path(source_file).name,
        "RowsIngested": original_rows,
        "RowsExported": len(df),
        "Columns": list(df.columns),
        "ValidationIssues": issues,
        "Outputs": {"CSV": str(csv_path), "JSON": str(json_path)},
    }
    summary_path = output_path / f"{base_name}_summary_{timestamp}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"  {Color.green('✓')} Summary: {summary_path}")

    return str(summary_path)


# ── Full Pipeline ─────────────────────────────────────────────────────────────

def run_pipeline(file_path: str, output_dir: str, archive_dir: str) -> bool:
    """Execute the 4-stage pipeline on a single CSV file."""
    print(f"\n{Color.magenta('=========================================')}")
    print(f"{Color.cyan('PROCESSING:')} {file_path}")
    print(f"{Color.magenta('=========================================')}")

    # Stage 1
    df = stage_ingest(file_path)
    if df is None:
        return False
    original_rows = len(df)

    if df.empty:
        print(f"  {Color.yellow('⚠')} Empty CSV file — skipping")
        return False

    # Stage 2
    issues = stage_validate(df)

    # Stage 3
    df = stage_transform(df, file_path)

    # Stage 4
    stage_export(df, output_dir, file_path, issues, original_rows)

    # Archive source file
    Path(archive_dir).mkdir(parents=True, exist_ok=True)
    shutil.move(file_path, str(Path(archive_dir) / Path(file_path).name))
    print(f"  {Color.darkgray('✓')} Archived source to {archive_dir}")

    # Report
    print(f"\n{Color.green('=== PIPELINE RESULT ===')}")
    print(f"{Color.green('✓')} Processed {original_rows} rows → {len(df)} rows")
    if issues:
        print(f"{Color.yellow('⚠')} {len(issues)} validation issue(s)")

    return True


# ── Sample Data ───────────────────────────────────────────────────────────────

SAMPLE_EMPLOYEES_CSV = """\
Name,Email,Department,Salary,StartDate,HireLocation
Alice Johnson,alice@example.com,Engineering,95000,2023-01-15,New York
Bob Smith,bob@example.com,Marketing,72000,2022-06-01,San Francisco
Charlie Brown,charlie@example.com,Sales,85000,2024-03-10,Chicago
Diana Prince,diana@example.com,Engineering,105000,2021-09-20,Seattle
Evan Williams,evan@example.com,HR,65000,2023-11-05,Boston
Fiona Green,fiona@example.com,Engineering,,2024-07-01,Austin
George Harris,george@example.com,Sales,78000,,Denver
"""

SAMPLE_SALES_CSV = """\
OrderID,Product,Quantity,Price,OrderDate,Customer
ORD-001,Laptop,2,1299.99,2026-07-20,Acme Corp
ORD-002,Mouse,10,49.99,2026-07-21,Globex Inc
ORD-003,Monitor,3,399.99,2026-07-22,Initech
ORD-004,Keyboard,5,89.99,2026-07-22,Acme Corp
ORD-005,Webcam,8,79.99,2026-07-23,Globex Inc
"""


def create_sample_data(input_dir: str):
    """Write sample CSV files into the input directory."""
    input_path = Path(input_dir)
    input_path.mkdir(parents=True, exist_ok=True)

    (input_path / "employees.csv").write_text(SAMPLE_EMPLOYEES_CSV, encoding="utf-8")
    (input_path / "sales.csv").write_text(SAMPLE_SALES_CSV, encoding="utf-8")

    print(f"  {Color.green('✓')} Created sample: {input_path / 'employees.csv'}")
    print(f"  {Color.green('✓')} Created sample: {input_path / 'sales.csv'}")


# ── Watch Mode ────────────────────────────────────────────────────────────────

def watch_directory(input_dir: str, output_dir: str, archive_dir: str, interval: int):
    """Watch a directory for new CSV files and process them as they appear."""
    print(f"\n{Color.yellow(f'Watching {input_dir} for new CSV files every {interval}s... (Ctrl+C to stop)')}")
    seen = set()

    # Seed with already-present files
    for f in Path(input_dir).glob("*.csv"):
        seen.add(f.name)

    try:
        while True:
            for f in sorted(Path(input_dir).glob("*.csv")):
                if f.name not in seen:
                    seen.add(f.name)
                    time.sleep(1)  # Wait for the file to finish writing
                    run_pipeline(str(f), output_dir, archive_dir)
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\n{Color.darkgray('Watch mode terminated.')}")


# ── CLI Entry Point ───────────────────────────────────────────────────────────

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="CSV Pipeline Processor — Ingest → Validate → Transform → Export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pipeline.py                        # Process incoming/*.csv\n"
            "  python pipeline.py --watch                # Watch for new files\n"
            "  python pipeline.py --input /data/csv      # Custom input dir\n"
        ),
    )
    parser.add_argument("--input-dir", "-i", default="./incoming",
                        help="Directory containing CSV files to process (default: ./incoming)")
    parser.add_argument("--output-dir", "-o", default="./processed",
                        help="Directory for processed output files (default: ./processed)")
    parser.add_argument("--archive-dir", "-a", default="./archive",
                        help="Directory to archive processed source files (default: ./archive)")
    parser.add_argument("--watch", "-w", action="store_true",
                        help="Watch input directory for new CSV files")
    parser.add_argument("--interval", "-n", type=int, default=10,
                        help="Watch interval in seconds (default: 10)")
    return parser.parse_args(argv)


def main():
    args = parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir
    archive_dir = args.archive_dir

    # ── Banner ──
    print(Color.cyan("""\
┌──────────────────────────────────────┐
│       CSV PIPELINE PROCESSOR         │
│   Stage: Ingest → Validate →         │
│          Transform → Export          │
└──────────────────────────────────────┘
"""))

    print(f"Input:   {input_dir}")
    print(f"Output:  {output_dir}")
    print(f"Archive: {archive_dir}")

    # Ensure directories exist
    for d in (input_dir, output_dir, archive_dir):
        Path(d).mkdir(parents=True, exist_ok=True)

    if args.watch:
        watch_directory(input_dir, output_dir, archive_dir, args.interval)
        return

    # Find CSV files
    csv_files = sorted(Path(input_dir).glob("*.csv"))

    if not csv_files:
        print(f"\n{Color.yellow('No CSV files found in ' + str(Path(input_dir).resolve()))}")
        print(f"{Color.darkgray('Drop .csv files into the input directory and re-run, or use:')}")
        print(f"{Color.darkgray('  python pipeline.py --watch')}")
        print(f"\n{Color.cyan('Creating sample CSV files for testing...')}")

        create_sample_data(input_dir)

        print(f"\n{Color.darkgray('Re-run to process them!')}")
        return

    # Process each file
    results = []
    for csv_file in csv_files:
        ok = run_pipeline(str(csv_file), output_dir, archive_dir)
        results.append({"File": csv_file.name, "Success": ok})

    # Final summary
    print(f"\n{Color.magenta('=== FINAL SUMMARY ===')}")
    success_count = sum(1 for r in results if r["Success"])
    total_count = len(results)

    header = f"{'File':<30} {'Success':<10}"
    sep = "-" * 40
    print(header)
    print(sep)
    for r in results:
        status = Color.green("✓") if r["Success"] else Color.red("✗")
        print(f"{r['File']:<30} {status:<10}")
    print(sep)
    print(f"{Color.green('Pipeline complete:')} {success_count}/{total_count} files processed successfully")


if __name__ == "__main__":
    main()
