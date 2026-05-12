#!/usr/bin/env python3
"""
Thygenie ETL Relay — process_trendlyne.py

Reads a Trendlyne CSV committed to data/trendlyne_*.csv,
converts rows to JSON, and POSTs in batches to the import endpoint.

This runs on GitHub Actions after a push to data/*.csv.
The user downloads the CSV in their browser (bypassing WAF),
commits it to the repo, and this script handles the rest.

Usage (GitHub Actions):
  python3 .github/scripts/process_trendlyne.py

Environment variables:
  THYGENIE_SITE_URL  — override base URL (default: https://thygenie.in/equity-invest)
  THYGENIE_API_KEY   — override API key (default: thygenie_cron_2024)
"""

import csv
import json
import os
import sys
import glob
import time
import urllib.request
import urllib.error

SITE_URL  = os.environ.get("THYGENIE_SITE_URL", "https://thygenie.in/equity-invest")
API_KEY   = os.environ.get("THYGENIE_API_KEY",  "thygenie_cron_2024")
ENDPOINT  = f"{SITE_URL}/trendlyne_fetcher.php"
BATCH_SZ  = 500   # rows per POST — keeps request body under ~500KB


def find_csvs():
    """Return all CSV paths in data/, sorted oldest-first so financials import before DVM."""
    seen = set()
    results = []
    for pattern in ["data/trendlyne_*.csv", "data/*.csv", "*.csv"]:
        for f in glob.glob(pattern):
            if f not in seen:
                seen.add(f)
                results.append(f)
    return sorted(results, key=os.path.getmtime)


def parse_csv(path):
    """Parse CSV, strip BOM, return list of dicts."""
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items() if k})
    return rows


def post_batch(rows, task="import_json"):
    payload = json.dumps({"key": API_KEY, "rows": rows}).encode("utf-8")
    url     = f"{ENDPOINT}?task={task}&key={API_KEY}"
    req     = urllib.request.Request(
        url,
        data    = payload,
        headers = {
            "Content-Type": "application/json",
            "User-Agent":   "Thygenie-ETL/2.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def process_one(csv_path):
    """Process a single CSV file. Returns True on success."""
    print(f"\n{'='*60}")
    print(f"CSV file : {csv_path}")
    rows = parse_csv(csv_path)
    print(f"Rows     : {len(rows)}")
    if not rows:
        print("WARNING: CSV parsed to 0 rows — skipping (check delimiter or encoding)")
        return True

    headers = list(rows[0].keys())
    print(f"Headers  : {', '.join(headers[:8])}{'...' if len(headers) > 8 else ''}")

    totals        = {"matched": 0, "unmatched": 0, "fund_saved": 0, "anal_saved": 0, "tech_saved": 0}
    total_batches = (len(rows) + BATCH_SZ - 1) // BATCH_SZ
    file_type     = None

    for idx in range(0, len(rows), BATCH_SZ):
        batch     = rows[idx : idx + BATCH_SZ]
        batch_num = idx // BATCH_SZ + 1
        print(f"Batch {batch_num}/{total_batches} ({len(batch)} rows)... ", end="", flush=True)
        try:
            result = post_batch(batch)
            for k in totals:
                totals[k] += result.get(k, 0)
            if file_type is None:
                file_type = result.get("file_type", "unknown")
            print(
                f"matched={result.get('matched',0)} "
                f"fund={result.get('fund_saved',0)} "
                f"anal={result.get('anal_saved',0)} "
                f"tech={result.get('tech_saved',0)}"
            )
            if result.get("error"):
                print(f"  WARNING: {result['error']}")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:300]
            print(f"HTTP {e.code}: {body}")
            return False
        except Exception as e:
            print(f"ERROR: {e}")
            return False

        if idx + BATCH_SZ < len(rows):
            time.sleep(2)

    print(
        f"\nComplete [{file_type}]: matched={totals['matched']} "
        f"fund_saved={totals['fund_saved']} "
        f"anal_saved={totals['anal_saved']} "
        f"tech_saved={totals['tech_saved']}"
    )
    if file_type == "unknown":
        print(f"  NOTE: File type unrecognised — no data saved. First 8 headers above.")
    if totals["unmatched"] > 0:
        print(f"  Unmatched (no stock found): {totals['unmatched']} rows")
    return True


def main():
    csv_paths = find_csvs()
    if not csv_paths:
        event = os.environ.get("GITHUB_EVENT_NAME", "push")
        if event == "workflow_dispatch":
            print("ERROR: No CSV found. Commit a file to data/trendlyne_*.csv first.")
            sys.exit(1)
        else:
            print("No CSV found in data/ — skipping ETL (push did not include a CSV)")
            sys.exit(0)

    print(f"Found {len(csv_paths)} CSV file(s) to process")
    for path in csv_paths:
        ok = process_one(path)
        if not ok:
            sys.exit(1)


if __name__ == "__main__":
    main()
