#!/usr/bin/env python3
"""
EU Gas Storage Level Fetcher
Downloads EU aggregate gas storage fill levels (% full) from the AGSI+ API
and stores as local JSON for the power prices dashboard.

Usage:
    python fetch_storage.py                    # Fetch from 2015-01-01 to today (incremental)
    python fetch_storage.py --start 2020-01-01 # Custom start date
    python fetch_storage.py --full             # Force full re-fetch

Data is saved to data/EU-Storage.json:
{unix_seconds: [...], fill_pct: [...]}
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "EU-Storage.json")
API_BASE = "https://agsi.gie.eu/api"
API_KEY = "d4566b3b4b7b7b1d7e8d39398037c07c"


def load_existing():
    """Load existing local data file if present."""
    if not os.path.exists(OUTPUT_FILE):
        return None
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "unix_seconds" in data and "fill_pct" in data and len(data["unix_seconds"]) > 0:
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: corrupt local file, will re-fetch: {e}")
    return None


def merge_data(existing, new_ts, new_pcts):
    """Merge new data into existing, deduplicating by timestamp."""
    ts_map = {}
    if existing:
        for i, ts in enumerate(existing["unix_seconds"]):
            ts_map[ts] = existing["fill_pct"][i]
    for i, ts in enumerate(new_ts):
        ts_map[ts] = new_pcts[i]

    sorted_ts = sorted(ts_map.keys())
    return {
        "unix_seconds": sorted_ts,
        "fill_pct": [ts_map[ts] for ts in sorted_ts],
    }


def save_data(data):
    """Save data to local JSON file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"  Saved {OUTPUT_FILE} ({size_kb:.1f} KB, {len(data['unix_seconds']):,} points)")


def fetch_page(start_date, end_date, page=1, retries=3):
    """Fetch a single page of EU storage data from AGSI+ API."""
    url = (
        f"{API_BASE}?type=eu"
        f"&from={start_date}&to={end_date}"
        f"&size=300&page={page}"
    )
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "x-key": API_KEY,
                "User-Agent": "EnergyDashboard/1.0",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  Retry {attempt + 1}/{retries} ({e}) -- waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Failed after {retries} attempts: {e}")
                return None


def fetch_storage(start_date, end_date, existing=None):
    """Fetch EU storage levels from AGSI+ API with pagination."""
    print(f"  Fetching EU storage data from {start_date} to {end_date}...")

    all_ts = []
    all_pcts = []
    page = 1

    while True:
        print(f"  Page {page}...", end=" ", flush=True)
        result = fetch_page(start_date, end_date, page=page)

        if result is None:
            print("FAIL")
            break

        # The API returns data in result["data"] (list of objects)
        entries = result.get("data", [])
        if not entries:
            print("no more data")
            break

        count = 0
        for entry in entries:
            full_str = entry.get("full")
            date_str = entry.get("gasDayStart")
            if full_str is None or date_str is None:
                continue
            try:
                pct = float(full_str)
            except (ValueError, TypeError):
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                ts = int(dt.timestamp())
            except ValueError:
                continue
            all_ts.append(ts)
            all_pcts.append(round(pct, 2))
            count += 1

        print(f"OK {count} entries")

        # Check if there are more pages
        last_page = result.get("last_page", 1)
        if page >= last_page:
            break
        page += 1

        # Rate limiting between pages
        time.sleep(0.5)

    print(f"  Total fetched: {len(all_ts)} data points")

    if not all_ts:
        return existing

    return merge_data(existing, all_ts, all_pcts)


def main():
    parser = argparse.ArgumentParser(
        description="Download EU gas storage levels for the power prices dashboard"
    )
    parser.add_argument(
        "--start", default="2015-01-01",
        help="Start date (YYYY-MM-DD). Default: 2015-01-01"
    )
    parser.add_argument(
        "--full", action="store_true",
        help="Force full re-fetch (ignore existing data)"
    )
    args = parser.parse_args()

    start_date = args.start
    end_date = datetime.now().strftime("%Y-%m-%d")

    print(f"EU Gas Storage Level Fetcher")
    print(f"{'=' * 40}")
    print(f"API: AGSI+ (agsi.gie.eu)")
    print(f"Range: {start_date} -> {end_date}")
    print(f"Data directory: {DATA_DIR}")

    existing = None if args.full else load_existing()

    if existing:
        last_ts = max(existing["unix_seconds"])
        last_date = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d")
        first_ts = min(existing["unix_seconds"])
        first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d")
        n_points = len(existing["unix_seconds"])
        print(f"\n  Existing local data: {first_date} to {last_date} ({n_points:,} points)")

        # Incremental: fetch from 2 days before last stored data
        incremental_start = datetime.fromtimestamp(last_ts) - timedelta(days=2)
        actual_start = max(
            incremental_start,
            datetime.strptime(start_date, "%Y-%m-%d")
        ).strftime("%Y-%m-%d")
        print(f"  Incremental update from {actual_start}")
    else:
        actual_start = start_date
        print(f"\n  No existing data, full fetch from {start_date}")

    result = fetch_storage(actual_start, end_date, existing)

    if result and len(result["unix_seconds"]) > 0:
        save_data(result)
        first = datetime.fromtimestamp(min(result["unix_seconds"])).strftime("%Y-%m-%d")
        last = datetime.fromtimestamp(max(result["unix_seconds"])).strftime("%Y-%m-%d")
        print(f"\n{'=' * 40}")
        print(f"Done! {len(result['unix_seconds']):,} total points ({first} to {last})")
    else:
        print(f"\n{'=' * 40}")
        print("No data available.")


if __name__ == "__main__":
    main()
