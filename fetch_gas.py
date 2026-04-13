#!/usr/bin/env python3
"""
TTF Natural Gas Price Fetcher
Downloads TTF front-month gas futures (EUR/MWh) from Yahoo Finance via yfinance
and stores as local JSON for the power prices dashboard.

Usage:
    python fetch_gas.py              # Fetch from 2015-01-01 to today (incremental)
    python fetch_gas.py --start 2020-01-01  # Custom start date
    python fetch_gas.py --full       # Force full re-fetch

Data is saved to data/TTF.json in the same format as power data:
{unix_seconds: [...], price: [...]}
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "TTF.json")
TICKER = "TTF=F"


def load_existing():
    """Load existing local data file if present."""
    if not os.path.exists(OUTPUT_FILE):
        return None
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "unix_seconds" in data and "price" in data and len(data["unix_seconds"]) > 0:
            return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: corrupt local file, will re-fetch: {e}")
    return None


def merge_data(existing, new_ts, new_prices):
    """Merge new data into existing, deduplicating by timestamp."""
    ts_map = {}
    if existing:
        for i, ts in enumerate(existing["unix_seconds"]):
            ts_map[ts] = existing["price"][i]
    for i, ts in enumerate(new_ts):
        ts_map[ts] = new_prices[i]

    sorted_ts = sorted(ts_map.keys())
    return {
        "unix_seconds": sorted_ts,
        "price": [ts_map[ts] for ts in sorted_ts],
    }


def save_data(data):
    """Save data to local JSON file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"  Saved {OUTPUT_FILE} ({size_kb:.1f} KB, {len(data['unix_seconds']):,} points)")


def fetch_ttf(start_date, end_date, existing=None):
    """Fetch TTF gas prices from Yahoo Finance."""
    try:
        import yfinance as yf
    except ImportError:
        print("Error: yfinance not installed. Run: pip install yfinance")
        sys.exit(1)

    print(f"  Fetching {TICKER} from {start_date} to {end_date}...")
    ticker = yf.Ticker(TICKER)

    # yfinance download — daily close prices
    df = ticker.history(start=start_date, end=end_date, interval="1d")

    if df.empty:
        print("  Warning: no data returned from Yahoo Finance")
        return existing

    # Convert to unix_seconds and price arrays
    new_ts = []
    new_prices = []
    for idx, row in df.iterrows():
        # Use midnight UTC of the trading date as the timestamp
        ts = int(idx.timestamp())
        close = row["Close"]
        if close is not None and close == close:  # not NaN
            new_ts.append(ts)
            new_prices.append(round(float(close), 2))

    print(f"  Got {len(new_ts)} daily prices from Yahoo Finance")

    if not new_ts:
        return existing

    return merge_data(existing, new_ts, new_prices)


def main():
    parser = argparse.ArgumentParser(
        description="Download TTF gas prices for the power prices dashboard"
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
    end_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"TTF Gas Price Fetcher")
    print(f"{'=' * 40}")
    print(f"Ticker: {TICKER}")
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

    result = fetch_ttf(actual_start, end_date, existing)

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
