"""
Upload SQLite tables to Databricks Unity Catalog as managed Delta tables.

Reads from the local SQLite DB, converts each table to Parquet,
uploads via the Databricks Files API (to a UC volume), then
creates tables with CREATE TABLE ... USING PARQUET.
"""

import os
import sqlite3
import tempfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

SQLITE_PATH = "/Users/arthursherman/audit_trail_generator/data/zepz.db"
CATALOG = "workspace"
SCHEMA = "zepz"

HOST = os.environ["DATABRICKS_HOST"].rstrip("/")
TOKEN = os.environ["DATABRICKS_TOKEN"]
WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

HEADERS = {"Authorization": f"Bearer {TOKEN}"}
SQL_URL = f"{HOST}/api/2.0/sql/statements"
FILES_URL = f"{HOST}/api/2.0/fs/files"

TABLES = ["customers", "transactions", "disclosures", "audit_events", "violations"]


def run_sql(statement: str, wait=True) -> dict:
    """Execute SQL via the Statement Execution API."""
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "50s" if wait else "0s",
    }
    resp = requests.post(SQL_URL, headers=HEADERS, json=payload)
    if not resp.ok:
        print(f"  HTTP {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    result = resp.json()
    status = result.get("status", {}).get("state", "")
    if status == "FAILED":
        error = result.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed: {error.get('message', result)}")
    return result


def upload_to_volume(local_path: str, volume_file_path: str):
    """Upload a file to a Unity Catalog volume via the Files API."""
    # The Files API expects: /api/2.0/fs/files/Volumes/<catalog>/<schema>/<volume>/<path>
    url = f"{FILES_URL}/Volumes/{volume_file_path}"
    with open(local_path, "rb") as f:
        resp = requests.put(
            url,
            headers={**HEADERS, "Content-Type": "application/octet-stream"},
            data=f,
        )
    if not resp.ok:
        print(f"  Upload error: {resp.status_code} {resp.text}")
    resp.raise_for_status()
    print(f"  Uploaded to /Volumes/{volume_file_path}")


def main():
    conn = sqlite3.connect(SQLITE_PATH)

    # 0. List available catalogs
    print("Listing catalogs ...")
    result = run_sql("SHOW CATALOGS")
    print(f"  Available catalogs: {result.get('result', {}).get('data_array', [])}")

    # 1. Create schema and volume
    print(f"Creating schema {CATALOG}.{SCHEMA} ...")
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

    volume_name = "staging"
    print(f"Creating volume {CATALOG}.{SCHEMA}.{volume_name} ...")
    run_sql(
        f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{volume_name}"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        for table in TABLES:
            print(f"\n--- {table} ---")

            # Read from SQLite
            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            print(f"  Read {len(df)} rows from SQLite")

            # Write to local Parquet
            parquet_path = Path(tmpdir) / f"{table}.parquet"
            df.to_parquet(parquet_path, index=False)
            print(f"  Wrote Parquet ({parquet_path.stat().st_size / 1024:.0f} KB)")

            # Upload Parquet to UC volume
            volume_path = f"{CATALOG}/{SCHEMA}/{volume_name}/{table}.parquet"
            upload_to_volume(str(parquet_path), volume_path)

            # Create managed Delta table via COPY INTO
            fqn = f"{CATALOG}.{SCHEMA}.{table}"
            run_sql(f"DROP TABLE IF EXISTS {fqn}")

            # Create empty table with schema inferred from a CTAS + COPY INTO
            copy_sql = (
                f"CREATE TABLE IF NOT EXISTS {fqn} "
                f"AS SELECT * FROM parquet.`/Volumes/{CATALOG}/{SCHEMA}/{volume_name}/{table}.parquet`"
            )
            run_sql(copy_sql)
            print(f"  Created table {fqn}")

    conn.close()

    # Verify
    print("\n=== Verification ===")
    for table in TABLES:
        result = run_sql(f"SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.{table}")
        rows = result.get("result", {}).get("data_array", [])
        count = rows[0][0] if rows else "?"
        print(f"  {CATALOG}.{SCHEMA}.{table}: {count} rows")

    print("\nDone!")


if __name__ == "__main__":
    main()
