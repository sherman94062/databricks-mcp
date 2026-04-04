#!/usr/bin/env python3
"""
sqlite_to_databricks.py
-----------------------
Migrates all Zepz compliance audit tables from SQLite into Databricks
as managed Delta tables under main.zepz_audit.

Requirements:
    pip3 install databricks-sdk

Environment variables required (set before running):
    DATABRICKS_HOST   e.g. https://community.cloud.databricks.com
    DATABRICKS_TOKEN  your personal access token

Usage:
    python3 sqlite_to_databricks.py --db ./zepz_audit.db

What it does:
    1. Reads each table from SQLite
    2. Writes a CSV to a local temp file
    3. Uploads the CSV to DBFS at /FileStore/zepz/<table>.csv
    4. Runs SQL to CREATE the Delta table and COPY INTO from the CSV
"""

import os
import sys
import csv
import time
import sqlite3
import argparse
import tempfile
from pathlib import Path

# ── Check for databricks-sdk ───────────────────────────────────────────────────
try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
except ImportError:
    print("ERROR: databricks-sdk not installed.")
    print("Run: pip3 install databricks-sdk")
    sys.exit(1)

# ── Configuration ──────────────────────────────────────────────────────────────

CATALOG   = "main"
SCHEMA    = "zepz_audit"
DBFS_DIR  = "/FileStore/zepz"

TABLES = [
    "customers",
    "transactions",
    "disclosures",
    "violations",
    "audit_events",
]

# DDL for each table — explicit types rather than inferring from CSV
DDL = {
    "customers": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers (
            customer_id          STRING,
            registration_country STRING NOT NULL,
            kyc_tier             STRING NOT NULL,
            risk_level           STRING NOT NULL,
            registration_date    DATE   NOT NULL,
            created_at           TIMESTAMP
        ) USING DELTA
    """,
    "transactions": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.transactions (
            tx_id                STRING,
            customer_id          STRING NOT NULL,
            corridor             STRING NOT NULL,
            send_country         STRING NOT NULL,
            receive_country      STRING NOT NULL,
            send_amount          DECIMAL(12,2) NOT NULL,
            send_currency        STRING NOT NULL,
            receive_amount       DECIMAL(12,2) NOT NULL,
            receive_currency     STRING NOT NULL,
            disclosed_fee        DECIMAL(8,2)  NOT NULL,
            charged_fee          DECIMAL(8,2)  NOT NULL,
            disclosed_fx_rate    DECIMAL(12,6) NOT NULL,
            applied_fx_rate      DECIMAL(12,6) NOT NULL,
            transfer_method      STRING NOT NULL,
            promised_delivery_ts TIMESTAMP NOT NULL,
            actual_delivery_ts   TIMESTAMP,
            status               STRING NOT NULL,
            created_at           TIMESTAMP NOT NULL
        ) USING DELTA
    """,
    "disclosures": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.disclosures (
            disclosure_id             STRING,
            tx_id                     STRING NOT NULL,
            disclosed_at              TIMESTAMP NOT NULL,
            disclosed_fee             DECIMAL(8,2)  NOT NULL,
            disclosed_fx_rate         DECIMAL(12,6) NOT NULL,
            disclosed_receive_amount  DECIMAL(12,2) NOT NULL,
            disclosed_delivery_window STRING NOT NULL,
            receipt_sent_at           TIMESTAMP,
            cancellation_deadline     TIMESTAMP NOT NULL,
            created_at                TIMESTAMP
        ) USING DELTA
    """,
    "violations": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.violations (
            violation_id  STRING,
            tx_id         STRING NOT NULL,
            rule_id       STRING NOT NULL,
            severity      STRING NOT NULL,
            disclosed_value STRING NOT NULL,
            actual_value  STRING NOT NULL,
            delta         DECIMAL(12,6),
            delta_pct     DECIMAL(8,4),
            description   STRING NOT NULL,
            detected_at   TIMESTAMP
        ) USING DELTA
    """,
    "audit_events": """
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.audit_events (
            event_id      STRING,
            tx_id         STRING NOT NULL,
            event_type    STRING NOT NULL,
            actor         STRING NOT NULL,
            wall_clock_ts TIMESTAMP NOT NULL,
            sequence_num  INTEGER NOT NULL,
            payload       STRING NOT NULL,
            created_at    TIMESTAMP
        ) USING DELTA
    """,
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def check_env():
    host  = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if not host or not token:
        print("ERROR: Missing environment variables.")
        print("  export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com")
        print("  export DATABRICKS_TOKEN=your-token")
        sys.exit(1)
    return host, token


def export_table_to_csv(conn: sqlite3.Connection, table: str, path: Path) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    return len(rows)


def upload_to_dbfs(w: WorkspaceClient, local_path: Path, dbfs_path: str):
    """Upload a local file to DBFS using the SDK's dbfs.upload helper."""
    with open(local_path, "rb") as f:
        w.dbfs.upload(dbfs_path, f, overwrite=True)


def run_sql(w: WorkspaceClient, statement: str, warehouse_id: str = None):
    """
    Execute SQL via the Statement Execution API.
    Falls back gracefully if no warehouse is available (Community Edition).
    """
    kwargs = {"statement": statement, "catalog": CATALOG, "schema": SCHEMA}
    if warehouse_id:
        kwargs["warehouse_id"] = warehouse_id

    response = w.statement_execution.execute_statement(**kwargs)

    # Poll until done
    stmt_id = response.statement_id
    for _ in range(60):  # up to 60s timeout
        status = w.statement_execution.get_statement(stmt_id)
        state  = status.status.state
        if state in (StatementState.SUCCEEDED,):
            return status
        if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
            err = status.status.error
            raise RuntimeError(f"SQL failed [{state}]: {err.message if err else 'unknown error'}")
        time.sleep(1)
    raise TimeoutError(f"SQL statement timed out: {statement[:80]}...")


def find_warehouse(w: WorkspaceClient) -> str | None:
    """Return the first running SQL warehouse ID, or None."""
    try:
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.state and wh.state.value in ("RUNNING", "STARTING"):
                return wh.id
        # If none running, return first available (it'll auto-start)
        if warehouses:
            return warehouses[0].id
    except Exception:
        pass
    return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Migrate Zepz SQLite → Databricks Delta")
    parser.add_argument("--db", required=True, help="Path to zepz_audit.db SQLite file")
    parser.add_argument("--catalog", default=CATALOG)
    parser.add_argument("--schema",  default=SCHEMA)
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"ERROR: SQLite file not found: {db_path}")
        sys.exit(1)

    host, token = check_env()
    print(f"\nConnecting to Databricks at {host}...")

    w = WorkspaceClient(host=host, token=token)

    # Verify connection
    try:
        me = w.current_user.me()
        print(f"Authenticated as: {me.user_name}")
    except Exception as e:
        print(f"ERROR: Could not connect to Databricks: {e}")
        sys.exit(1)

    warehouse_id = find_warehouse(w)
    if warehouse_id:
        print(f"Using SQL warehouse: {warehouse_id}")
    else:
        print("WARNING: No SQL warehouse found. Will attempt without warehouse_id.")

    # Create schema
    print(f"\nCreating schema {args.catalog}.{args.schema}...")
    try:
        run_sql(w, f"CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema}", warehouse_id)
        print("  ✓ Schema ready")
    except Exception as e:
        print(f"  ✗ Schema creation failed: {e}")
        print("  Continuing — schema may already exist or permissions differ on Community Edition.")

    conn = sqlite3.connect(db_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        total_rows = 0

        for table in TABLES:
            print(f"\n── {table} ──────────────────────────")

            # 1. Export SQLite → local CSV
            csv_path   = tmp / f"{table}.csv"
            row_count  = export_table_to_csv(conn, table, csv_path)
            total_rows += row_count
            print(f"  ✓ Exported {row_count:,} rows from SQLite")

            # 2. Upload CSV → DBFS
            dbfs_path = f"{DBFS_DIR}/{table}.csv"
            try:
                upload_to_dbfs(w, csv_path, dbfs_path)
                print(f"  ✓ Uploaded to dbfs:{dbfs_path}")
            except Exception as e:
                print(f"  ✗ DBFS upload failed: {e}")
                print("    Skipping this table.")
                continue

            # 3. Create Delta table
            ddl = DDL[table].format(catalog=args.catalog, schema=args.schema)
            try:
                run_sql(w, ddl, warehouse_id)
                print(f"  ✓ Delta table created")
            except Exception as e:
                print(f"  ✗ CREATE TABLE failed: {e}")
                continue

            # 4. Truncate (idempotent re-run support)
            try:
                run_sql(w, f"TRUNCATE TABLE {args.catalog}.{args.schema}.{table}", warehouse_id)
            except Exception:
                pass  # Table may be empty on first run, that's fine

            # 5. COPY INTO from DBFS CSV
            copy_sql = f"""
                COPY INTO {args.catalog}.{args.schema}.{table}
                FROM 'dbfs:{dbfs_path}'
                FILEFORMAT = CSV
                FORMAT_OPTIONS (
                    'header' = 'true',
                    'inferSchema' = 'false',
                    'timestampFormat' = 'yyyy-MM-dd HH:mm:ss',
                    'nullValue' = ''
                )
                COPY_OPTIONS ('force' = 'true')
            """
            try:
                run_sql(w, copy_sql, warehouse_id)
                print(f"  ✓ COPY INTO complete")
            except Exception as e:
                print(f"  ✗ COPY INTO failed: {e}")
                print("    The table was created but may be empty. Check Databricks UI.")

    conn.close()

    print(f"\n{'─'*60}")
    print(f"Migration complete. {total_rows:,} total rows processed.")
    print(f"\nYour tables are at: {args.catalog}.{args.schema}.*")
    print("  customers, transactions, disclosures, violations, audit_events")
    print(f"\nVerify in Databricks: SELECT COUNT(*) FROM {args.catalog}.{args.schema}.violations")
    print('─'*60)


if __name__ == "__main__":
    main()
