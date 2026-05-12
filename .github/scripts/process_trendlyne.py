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


def find_csv():
    """Return path to the most recently modified CSV in data/."""
    patterns = ["data/trendlyne_*.csv", "data/*.csv"]
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            return max(files, key=os.path.getmtime)
    # Also check repo root for convenience
    root_csvs = glob.glob("*.csv")
    return max(root_csvs, key=os.path.getmtime) if root_csvs else None


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


def main():
    csv_path = find_csv()
    if not csv_path:
        event = os.environ.get("GITHUB_EVENT_NAME", "push")
        if event == "workflow_dispatch":
            print("ERROR: No CSV found. Commit a file to data/trendlyne_*.csv first.")
            sys.exit(1)
        else:
            print("No CSV found in data/ — skipping ETL (push did not include a CSV)")
            sys.exit(0)

    print(f"CSV file : {csv_path}")
    rows = parse_csv(csv_path)
    print(f"Rows     : {len(rows)}")
    if not rows:
        print("ERROR: CSV parsed to 0 rows — check delimiter or encoding")
        sys.exit(1)

    # Show first 3 headers for debug
    headers = list(rows[0].keys())
    print(f"Headers  : {', '.join(headers[:6])}{'...' if len(headers) > 6 else ''}")

    totals      = {"matched": 0, "unmatched": 0, "fund_saved": 0, "anal_saved": 0, "tech_saved": 0}
    total_batches = (len(rows) + BATCH_SZ - 1) // BATCH_SZ

    for idx in range(0, len(rows), BATCH_SZ):
        batch     = rows[idx : idx + BATCH_SZ]
        batch_num = idx // BATCH_SZ + 1
        print(f"Batch {batch_num}/{total_batches} ({len(batch)} rows)... ", end="", flush=True)
        try:
            result = post_batch(batch)
            for k in totals:
                totals[k] += result.get(k, 0)
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
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: {e}")
            sys.exit(1)

        # Brief pause between batches to avoid overwhelming shared hosting
        if idx + BATCH_SZ < len(rows):
            time.sleep(2)

    print(
        f"\nComplete: matched={totals['matched']} "
        f"fund_saved={totals['fund_saved']} "
        f"anal_saved={totals['anal_saved']} "
        f"tech_saved={totals['tech_saved']}"
    )
    if totals["unmatched"] > 0:
        print(f"Unmatched (no stock found): {totals['unmatched']} rows")


if __name__ == "__main__":
    main()
