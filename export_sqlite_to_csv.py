#!/usr/bin/env python3
"""
export_sqlite_to_csv.py
-----------------------
Exports all Zepz compliance audit tables from SQLite to CSV files,
ready for upload to Databricks DBFS.

Usage:
    python export_sqlite_to_csv.py --db /path/to/your/zepz_audit.db

Output:
    Creates a folder ./zepz_csvs/ containing one CSV per table.
    Upload each CSV to Databricks via:
        Workspace UI → Data → Add Data → DBFS → Upload to /FileStore/zepz/
"""

import sqlite3
import csv
import os
import argparse
import sys
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────

TABLES = [
    "customers",
    "transactions",
    "disclosures",
    "violations",
    "audit_events",
]

OUTPUT_DIR = Path("./zepz_csvs")

# ── Main ───────────────────────────────────────────────────────────────────────

def export_table(conn: sqlite3.Connection, table: str, output_dir: Path) -> int:
    """Export a single SQLite table to CSV. Returns row count."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table}")
    columns = [description[0] for description in cursor.description]
    rows = cursor.fetchall()

    out_path = output_dir / f"{table}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(columns)
        writer.writerows(rows)

    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Export Zepz SQLite DB to CSVs for Databricks")
    parser.add_argument(
        "--db",
        required=True,
        help="Path to the SQLite database file (e.g. ~/zepz_audit.db)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help=f"Output directory for CSV files (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {out_dir.resolve()}\n")

    conn = sqlite3.connect(db_path)
    try:
        total_rows = 0
        for table in TABLES:
            try:
                count = export_table(conn, table, out_dir)
                total_rows += count
                print(f"  ✓  {table:<20} {count:>7,} rows  →  {out_dir}/{table}.csv")
            except sqlite3.OperationalError as e:
                print(f"  ✗  {table:<20} SKIPPED ({e})")
    finally:
        conn.close()

    print(f"\nDone. {total_rows:,} total rows exported to {out_dir.resolve()}/")
    print("\n─── Next Steps ───────────────────────────────────────────────────────────────")
    print("1. Open your Databricks workspace in a browser")
    print("2. Go to: Catalog > (DBFS icon in top right) or Data > Add Data > DBFS")
    print("3. Upload all CSV files from ./zepz_csvs/ to: /FileStore/zepz/")
    print("4. Import and run the notebook:  zepz_audit_databricks_setup.py")
    print("─────────────────────────────────────────────────────────────────────────────")


if __name__ == "__main__":
    main()
