#!/usr/bin/env python3
"""
Fast Export Generator for Auto Market Index.

Generates CSV and JSON exports of:
  - AUTO INDEX history (average of 25 model median listing prices per week)
  - Per-model median listing prices for all 25 tracked models

Listing prices are asking prices on active used-car listings via Marketcheck API.
These are NOT transaction (sold) prices.

Usage:
  python generate_exports.py

Outputs (written to data/exports/):
  auto-markets-history.csv
  auto-markets-history.json
  auto-markets-latest.csv
  auto-markets-latest.json
  daily/<YYYY-MM-DD>.csv
  daily/<YYYY-MM-DD>.json
"""

import sqlite3
import json
import csv
import os
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR / "data" / "auto_markets.db"
EXPORT_DIR = SCRIPT_DIR / "data" / "exports"
DAILY_DIR  = EXPORT_DIR / "daily"

DAILY_DIR.mkdir(parents=True, exist_ok=True)

# 25 tracked models (same order as server.js)
MODELS = [
    # Pickups
    ("Ford",          "F-150"),
    ("Chevrolet",     "Silverado 1500"),
    ("Toyota",        "Tacoma"),
    ("Ram",           "1500"),
    ("Toyota",        "Tundra"),
    # Sedans
    ("Toyota",        "Camry"),
    ("Honda",         "Accord"),
    ("Toyota",        "Corolla"),
    ("Honda",         "Civic"),
    ("Nissan",        "Altima"),
    ("Hyundai",       "Elantra"),
    # SUVs
    ("Toyota",        "RAV4"),
    ("Honda",         "CR-V"),
    ("Ford",          "Explorer"),
    ("Chevrolet",     "Equinox"),
    ("Toyota",        "Highlander"),
    ("Jeep",          "Grand Cherokee"),
    ("Ford",          "Escape"),
    ("Honda",         "Pilot"),
    # Luxury
    ("Tesla",         "Model 3"),
    ("Tesla",         "Model Y"),
    ("BMW",           "3 Series"),
    ("Mercedes-Benz", "C-Class"),
    # Minivan / Commercial
    ("Honda",         "Odyssey"),
    ("Ford",          "Transit"),
]

# Column names for CSV (safe for spreadsheets)
def col_name(make, model):
    safe = f"{make}_{model}".replace(" ", "_").replace("-", "_").replace("/", "_")
    return f"{safe}_median"

CSV_FIELDS = ["date", "auto_index"] + [col_name(m, n) for m, n in MODELS]


def get_db():
    if not DB_PATH.exists():
        print(f"  WARNING: database not found at {DB_PATH}")
        return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def write_csv(filepath, rows, fieldnames):
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {filepath} ({len(rows)} rows)")


def write_json(filepath, data):
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Written: {filepath}")


def main():
    print(f"[{datetime.utcnow().isoformat()}] Generating Auto Market Index exports...")

    conn = get_db()
    if conn is None:
        print("  Exiting: no database found. Run the server first to populate data.")
        return

    # ── 1. Fetch all AUTO INDEX readings (one per week) ──
    index_rows = conn.execute("""
        SELECT date(timestamp) as date, index_value
        FROM auto_index
        ORDER BY date ASC
    """).fetchall()

    print(f"  Found {len(index_rows)} AUTO INDEX readings")

    # ── 2. Build per-model median maps: {(make, model): {date: median_price}} ──
    model_maps = {}
    for make, model in MODELS:
        rows = conn.execute("""
            SELECT date(timestamp) as date, median_price
            FROM auto_prices
            WHERE make = ? AND model = ?
            ORDER BY date ASC
        """, (make, model)).fetchall()
        model_maps[(make, model)] = {r["date"]: r["median_price"] for r in rows}

    print(f"  Loaded price data for {len(MODELS)} models")

    # ── 3. Build output rows ──
    csv_rows  = []
    json_data = []

    for row in index_rows:
        d = row["date"]

        csv_entry  = {"date": d, "auto_index": round(row["index_value"], 2) if row["index_value"] else ""}
        json_entry = {"date": d, "auto_index": round(row["index_value"], 2) if row["index_value"] else None,
                      "models": {}}

        for make, model in MODELS:
            price = model_maps[(make, model)].get(d)
            col   = col_name(make, model)
            csv_entry[col]                         = round(price, 2) if price else ""
            json_entry["models"][f"{make} {model}"] = round(price, 2) if price else None

        csv_rows.append(csv_entry)
        json_data.append(json_entry)

    # ── 4. Write history files ──
    write_csv(EXPORT_DIR / "auto-markets-history.csv",  csv_rows, CSV_FIELDS)
    write_json(EXPORT_DIR / "auto-markets-history.json", {
        "export_date":  datetime.utcnow().isoformat(),
        "record_count": len(json_data),
        "description":  "Auto Market Index — 25 US models, weekly median listing prices via Marketcheck API. Listing prices only (not sold prices).",
        "models_tracked": [f"{m} {n}" for m, n in MODELS],
        "data": json_data,
    })

    # ── 5. Latest snapshot ──
    if csv_rows:
        write_csv(EXPORT_DIR  / "auto-markets-latest.csv",  [csv_rows[-1]], CSV_FIELDS)
        write_json(EXPORT_DIR / "auto-markets-latest.json", {
            "export_date": datetime.utcnow().isoformat(),
            **json_data[-1],
        })

    # ── 6. Today's daily snapshot ──
    today      = datetime.utcnow().strftime("%Y-%m-%d")
    today_csv  = [r for r in csv_rows  if r["date"] == today]
    today_json = [r for r in json_data if r["date"] == today]

    if today_csv:
        write_csv(DAILY_DIR  / f"{today}.csv",  today_csv,  CSV_FIELDS)
        write_json(DAILY_DIR / f"{today}.json", today_json[-1])
    else:
        print(f"  No data for today ({today}) — daily snapshot skipped")

    conn.close()
    print("Done!")
    print(f"\nExports written to: {EXPORT_DIR}")


if __name__ == "__main__":
    main()
