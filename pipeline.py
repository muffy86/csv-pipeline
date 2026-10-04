"""Enhanced CSV Pipeline — v2 with aggregation, reporting, webhooks."""
import os
import sys
import csv
import json
import time
import shutil
import hashlib
import logging
import argparse
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path
from collections import Counter
import pandas as pd

# Configuration
WATCH_INTERVAL = int(os.getenv("PIPELINE_INTERVAL", "10"))
REDIS_URL = os.getenv("REDIS_URL", "")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# Logging — stdout only; Docker's json-file driver handles capture + rotation.
# (A FileHandler inside the container writes to the container layer and is
#  both lossy across restarts and unbounded, so it is deliberately omitted.)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
    force=True,
)
log = logging.getLogger("pipeline")


def stage_ingest(file_path):
    """Read CSV file into DataFrame."""
    try:
        df = pd.read_csv(file_path)
        log.info(f"Ingested {len(df)} rows from {Path(file_path).name}")
        return df
    except Exception as e:
        log.error(f"Ingest failed for {file_path}: {e}")
        return None


def stage_validate(df):
    """Validate data quality."""
    issues = []
    for col in df.columns:
        nulls = df[col].isna().sum()
        if nulls > 0:
            issues.append({"column": col, "nulls": int(nulls), "pct": round(nulls / len(df) * 100, 1)})
    if issues:
        log.warning(f"Validation: {len(issues)} issue(s) found")
    return issues


def stage_transform(df, source_file):
    """Clean and transform data."""
    original_count = len(df)
    for col in df.select_dtypes(include=["object", "str"]).columns:
        df[col] = df[col].str.strip()
    df = df.dropna(how="all")
    df = df.drop_duplicates()
    df = df.fillna("N/A")
    changes = original_count - len(df)
    if changes > 0:
        log.info(f"Transform: removed {changes} problematic rows")
    return df


def stage_aggregate(df):
    """Generate summary statistics and aggregations."""
    report = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "dtypes": {str(k): str(v) for k, v in df.dtypes.items()},
        "numeric_summary": {},
        "value_counts": {},
    }
    for col in df.select_dtypes(include="number").columns:
        report["numeric_summary"][col] = {
            "min": float(df[col].min()),
            "max": float(df[col].max()),
            "mean": round(float(df[col].mean()), 2),
            "median": round(float(df[col].median()), 2),
        }
    for col in df.select_dtypes(include=["object", "str"]).columns[:3]:
        counts = df[col].value_counts().head(5).to_dict()
        report["value_counts"][col] = {str(k): int(v) for k, v in counts.items()}
    log.info(f"Aggregation complete for {len(df)} rows x {len(df.columns)} cols")
    return report


def stage_notify(file_name, status, summary, issues, report):
    """Send webhook notification about pipeline completion."""
    if not WEBHOOK_URL:
        return False
    payload = {
        "event": "pipeline.complete",
        "file": str(file_name),
        "status": status,
        "timestamp": datetime.now().isoformat(),
        "summary": summary,
        "issues": issues,
        "aggregations": report,
    }
    try:
        data = json.dumps(payload).encode()
        req = urllib.request.Request(
            WEBHOOK_URL, data=data,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
        log.info(f"Webhook sent to {WEBHOOK_URL}")
        return True
    except Exception as e:
        log.warning(f"Webhook failed: {e}")
        return False


def stage_export(df, output_dir, source_file, issues, report, original_rows):
    """Export processed data to JSON/CSV with metadata."""
    os.makedirs(output_dir, exist_ok=True)
    base = Path(source_file).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"{base}_{timestamp}.json")
    result = {
        "metadata": {
            "source": Path(source_file).name,
            "processed_at": datetime.now().isoformat(),
            "original_rows": original_rows,
            "final_rows": len(df),
            "issues": issues,
            "aggregations": report,
            "status": "success" if len(issues) == 0 else "warning",
        },
        "data": df.to_dict(orient="records"),
    }
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, default=str)
    log.info(f"Exported {len(df)} rows to {output_file}")
    return output_file


def run_pipeline(file_path, output_dir, archive_dir):
    """Execute the full pipeline with aggregation and notifications."""
    file_name = Path(file_path).name
    log.info(f"Processing: {file_name}")
    df = stage_ingest(file_path)
    if df is None:
        return False
    original_rows = len(df)
    issues = stage_validate(df)
    df = stage_transform(df, file_path)
    if df is None:
        return False
    report = stage_aggregate(df)
    export_path = stage_export(df, output_dir, file_path, issues, report, original_rows)
    os.makedirs(archive_dir, exist_ok=True)
    # NOTE: use shutil.move, NOT os.rename — incoming/ and archive/ are
    # separate bind mounts (different devices), and os.rename raises
    # OSError [Errno 18] Invalid cross-device link across devices.
    shutil.move(str(file_path), os.path.join(archive_dir, file_name))
    log.info(f"Archived: {file_name}")
    summary = {"original_rows": original_rows, "final_rows": len(df), "removed": original_rows - len(df), "issues": len(issues)}
    stage_notify(file_name, "success", summary, issues, report)
    log.info(f"Complete: {file_name} -> {export_path}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced CSV Pipeline")
    parser.add_argument("files", nargs="*", help="CSV files to process")
    parser.add_argument("--watch", action="store_true", help="Watch incoming directory")
    parser.add_argument("--interval", type=int, default=WATCH_INTERVAL, help="Watch interval (s)")
    parser.add_argument("--generate-sample", action="store_true", help="Generate sample data")
    args = parser.parse_args()
    base_dir = Path(__file__).parent
    incoming = base_dir / "incoming"
    processed = base_dir / "processed"
    archive = base_dir / "archive"
    incoming.mkdir(exist_ok=True)
    processed.mkdir(exist_ok=True)
    archive.mkdir(exist_ok=True)
    if args.generate_sample:
        log.info("Generating sample data...")
        df = pd.DataFrame({
            "name": ["Alice Johnson", "Bob Smith", " Charlie Brown ", "", "Diana Prince"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com", "diana@example.com", ""],
            "age": [32, 45, 28, None, 35],
            "department": ["Engineering", "Marketing", "Engineering", "Engineering", "Leadership"],
            "salary": [95000, 82000, 78000, 120000, 150000],
        })
        df.to_csv(incoming / "sample_data.csv", index=False)
        log.info(f"Sample data saved")
    if args.watch:
        log.info(f"Watching {incoming} every {args.interval}s...")
        seen = set()
        while True:
            # Forget names that have left incoming/ (i.e. archived OK) so a
            # file re-dropped later under the same name is processed again.
            seen &= {p.name for p in incoming.glob("*.csv")}
            for f in sorted(incoming.glob("*.csv")):
                if f.name in seen:
                    continue
                try:
                    run_pipeline(str(f), str(processed), str(archive))
                except Exception:
                    # Never let one bad file kill the watcher. An unhandled
                    # exception here previously crashed the process, so the
                    # container restart-looped and re-processed the same file
                    # on every boot (duplicate outputs).
                    log.exception(
                        f"Pipeline failed for {f.name}; leaving it in incoming/ for review"
                    )
                seen.add(f.name)
            time.sleep(args.interval)
    for f in args.files:
        run_pipeline(f, str(processed), str(archive))
    if not args.files and not args.watch and not args.generate_sample:
        parser.print_help()
