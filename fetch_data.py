#!/usr/bin/env python3
"""
Energy-Charts Data Fetcher
Downloads day-ahead power price data from the Energy-Charts API (Fraunhofer ISE)
and stores it as local JSON files for fast dashboard loading.

Usage:
    python fetch_data.py                        # Fetch all markets from 2015 to today
    python fetch_data.py --market DE-LU         # Fetch only Germany/Luxembourg
    python fetch_data.py --market DE-LU FR      # Fetch multiple markets
    python fetch_data.py --start 2020-01-01     # Custom start date
    python fetch_data.py --all-history          # Fetch from 2011 (earliest available)

Data is saved to data/{MARKET}.json and supports incremental updates —
re-running the script only fetches new data since the last stored timestamp.
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

ALL_MARKETS = [
    "DE-LU", "FR", "NL", "BE", "AT", "CH",
    "DK1", "DK2", "NO1", "NO2", "SE1", "SE2", "SE3", "SE4", "FI",
    "ES", "PT",
    "IT-North", "IT-South",
    "PL", "CZ", "HU", "SK", "SI", "HR", "RO", "BG",
    "EE", "LV", "LT",
    "GR",
]

API_BASE = "https://api.energy-charts.info/price"


def fetch_chunk(market, start, end, retries=3):
    """Fetch a date range from the API with retries."""
    url = f"{API_BASE}?bzn={market}&start={start}&end={end}"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "EnergyDashboard/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                if "unix_seconds" in data and "price" in data:
                    return data
                print(f"  Warning: unexpected response format for {market} {start}–{end}")
                return None
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  Retry {attempt + 1}/{retries} for {market} ({e}) — waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Failed after {retries} attempts for {market} {start}–{end}: {e}")
                return None


def load_existing(market):
    """Load existing local data file if present."""
    path = os.path.join(DATA_DIR, f"{market}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "unix_seconds" in data and "price" in data and len(data["unix_seconds"]) > 0:
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: corrupt local file for {market}, will re-fetch: {e}")
    return None


def save_data(market, data):
    """Save data to local JSON file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{market}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"  Saved {path} ({size_mb:.1f} MB, {len(data['unix_seconds']):,} points)")


def merge_data(existing, new_data):
    """Merge new data into existing, deduplicating by timestamp."""
    if existing is None:
        return new_data
    if new_data is None:
        return existing

    ts_map = {}
    for i, ts in enumerate(existing["unix_seconds"]):
        ts_map[ts] = existing["price"][i]
    for i, ts in enumerate(new_data["unix_seconds"]):
        ts_map[ts] = new_data["price"][i]

    sorted_ts = sorted(ts_map.keys())
    return {
        "unix_seconds": sorted_ts,
        "price": [ts_map[ts] for ts in sorted_ts],
    }


def date_chunks(start_date, end_date, chunk_days=365):
    """Split a date range into chunks."""
    chunks = []
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days), end_date)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end
    return chunks


def fetch_market(market, start_date, end_date):
    """Fetch data for a single market with incremental update support."""
    print(f"\n[{market}]")

    existing = load_existing(market)
    actual_start = start_date

    if existing:
        last_ts = max(existing["unix_seconds"])
        last_date = datetime.fromtimestamp(last_ts).strftime("%Y-%m-%d")
        n_points = len(existing["unix_seconds"])
        first_ts = min(existing["unix_seconds"])
        first_date = datetime.fromtimestamp(first_ts).strftime("%Y-%m-%d")
        print(f"  Existing local data: {first_date} to {last_date} ({n_points:,} points)")

        # Only fetch from the day after the last stored data
        incremental_start = datetime.fromtimestamp(last_ts) - timedelta(days=1)
        if incremental_start > start_date:
            actual_start = incremental_start
            print(f"  Incremental update from {actual_start.strftime('%Y-%m-%d')}")

        # Also backfill if the requested start is before existing data
        if start_date < datetime.fromtimestamp(first_ts):
            backfill_end = datetime.fromtimestamp(first_ts) + timedelta(days=1)
            print(f"  Backfilling {start_date.strftime('%Y-%m-%d')} to {backfill_end.strftime('%Y-%m-%d')}...")
            backfill_chunks = date_chunks(start_date, backfill_end)
            for sd, ed in backfill_chunks:
                chunk = fetch_chunk(market, sd, ed)
                if chunk:
                    existing = merge_data(existing, chunk)
    else:
        print(f"  No existing data, full fetch from {start_date.strftime('%Y-%m-%d')}")

    # Fetch from actual_start to end_date
    chunks = date_chunks(actual_start, end_date)
    total_chunks = len(chunks)

    if total_chunks == 0:
        print("  Already up to date.")
        return

    fetched_any = False
    for idx, (sd, ed) in enumerate(chunks):
        print(f"  Chunk {idx + 1}/{total_chunks}: {sd} -> {ed}...", end=" ", flush=True)
        chunk = fetch_chunk(market, sd, ed)
        if chunk:
            n = len(chunk["unix_seconds"])
            print(f"OK {n:,} points")
            existing = merge_data(existing, chunk)
            fetched_any = True
        else:
            print("FAIL failed")

        # Rate limiting: small delay between chunks
        if idx < total_chunks - 1:
            time.sleep(0.5)

    if existing and fetched_any:
        save_data(market, existing)
    elif existing and not fetched_any:
        print("  No new data fetched (API may be unavailable)")
    else:
        print("  No data available for this market")


def main():
    parser = argparse.ArgumentParser(
        description="Download Energy-Charts price data for local dashboard use"
    )
    parser.add_argument(
        "--market", nargs="+", default=None,
        help="Market codes to fetch (e.g., DE-LU FR). Default: all markets"
    )
    parser.add_argument(
        "--start", default="2015-01-01",
        help="Start date (YYYY-MM-DD). Default: 2015-01-01"
    )
    parser.add_argument(
        "--all-history", action="store_true",
        help="Fetch from 2011-01-01 (earliest available data)"
    )
    args = parser.parse_args()

    markets = args.market if args.market else ALL_MARKETS
    start_str = "2011-01-01" if args.all_history else args.start
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.now() + timedelta(days=1)

    print(f"Energy-Charts Data Fetcher")
    print(f"{'=' * 40}")
    print(f"Markets: {', '.join(markets)}")
    print(f"Range: {start_str} -> {end_date.strftime('%Y-%m-%d')}")
    print(f"Data directory: {DATA_DIR}")

    os.makedirs(DATA_DIR, exist_ok=True)

    for market in markets:
        fetch_market(market, start_date, end_date)

    print(f"\n{'=' * 40}")
    print("Done! Dashboard will automatically load from local data.")


if __name__ == "__main__":
    main()
