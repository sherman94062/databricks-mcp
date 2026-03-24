"""
client/mock.py - Full stub implementation of DatabricksClientBase.

Returns realistic-looking data so you can develop and demo the MCP server
without a Databricks account. Every public method is implemented; none touch
the network. Swap this for RealClient by setting DATABRICKS_MOCK=false.
"""

from __future__ import annotations
import asyncio
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from .base import DatabricksClientBase


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# Seed data

_CATALOGS = [
    {"name": "main", "type": "MANAGED", "comment": "Primary Unity Catalog", "owner": "admin"},
    {"name": "hive_metastore", "type": "HIVE_METASTORE", "comment": "Legacy Hive metastore", "owner": "admin"},
    {"name": "samples", "type": "MANAGED", "comment": "Databricks sample datasets", "owner": "databricks"},
]

_SCHEMAS: dict[str, list[dict]] = {
    "main": [
        {"name": "default", "catalog": "main", "owner": "admin", "comment": ""},
        {"name": "bronze", "catalog": "main", "owner": "etl_svc", "comment": "Raw ingestion layer"},
        {"name": "silver", "catalog": "main", "owner": "etl_svc", "comment": "Cleansed / normalized"},
        {"name": "gold", "catalog": "main", "owner": "analytics", "comment": "Business-ready aggregates"},
    ],
    "hive_metastore": [
        {"name": "default", "catalog": "hive_metastore", "owner": "admin", "comment": ""},
        {"name": "legacy_sales", "catalog": "hive_metastore", "owner": "admin", "comment": ""},
    ],
    "samples": [
        {"name": "tpch", "catalog": "samples", "owner": "databricks", "comment": "TPC-H benchmark"},
        {"name": "nyctaxi", "catalog": "samples", "owner": "databricks", "comment": "NYC Taxi trips"},
    ],
}

_TABLES: dict[str, list[dict]] = {
    "main.bronze": [
        {"name": "events_raw", "type": "MANAGED", "format": "DELTA"},
        {"name": "orders_raw", "type": "MANAGED", "format": "DELTA"},
        {"name": "users_raw", "type": "EXTERNAL", "format": "PARQUET"},
    ],
    "main.silver": [
        {"name": "events", "type": "MANAGED", "format": "DELTA"},
        {"name": "orders", "type": "MANAGED", "format": "DELTA"},
        {"name": "users", "type": "MANAGED", "format": "DELTA"},
        {"name": "products", "type": "MANAGED", "format": "DELTA"},
    ],
    "main.gold": [
        {"name": "daily_revenue", "type": "MANAGED", "format": "DELTA"},
        {"name": "user_lifetime_value", "type": "MANAGED", "format": "DELTA"},
        {"name": "funnel_summary", "type": "MANAGED", "format": "DELTA"},
    ],
    "samples.tpch": [
        {"name": "customer", "type": "MANAGED", "format": "DELTA"},
        {"name": "lineitem", "type": "MANAGED", "format": "DELTA"},
        {"name": "orders", "type": "MANAGED", "format": "DELTA"},
        {"name": "part", "type": "MANAGED", "format": "DELTA"},
    ],
    "samples.nyctaxi": [
        {"name": "trips", "type": "MANAGED", "format": "DELTA"},
    ],
}

_TABLE_DETAILS: dict[str, dict] = {
    "main.silver.orders": {
        "full_name": "main.silver.orders",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "owner": "etl_svc",
        "comment": "Cleansed orders with status and totals",
        "row_count": 4_872_301,
        "size_bytes": 2_147_483_648,
        "created_at": "2024-03-15T00:00:00Z",
        "updated_at": "2025-03-23T06:00:00Z",
        "columns": [
            {"name": "order_id", "type": "BIGINT", "nullable": False, "comment": "PK"},
            {"name": "user_id", "type": "BIGINT", "nullable": False, "comment": "FK to users"},
            {"name": "product_id", "type": "BIGINT", "nullable": False, "comment": ""},
            {"name": "order_date", "type": "DATE", "nullable": False, "comment": ""},
            {"name": "status", "type": "STRING", "nullable": False, "comment": "pending|shipped|delivered|cancelled"},
            {"name": "amount", "type": "DOUBLE", "nullable": True, "comment": "USD"},
            {"name": "updated_at", "type": "TIMESTAMP", "nullable": False, "comment": ""},
        ],
        "partition_columns": ["order_date"],
        "properties": {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
    },
    "main.gold.daily_revenue": {
        "full_name": "main.gold.daily_revenue",
        "table_type": "MANAGED",
        "data_source_format": "DELTA",
        "owner": "analytics",
        "comment": "Daily rolled-up revenue by channel",
        "row_count": 1_460,
        "size_bytes": 524_288,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2025-03-24T01:00:00Z",
        "columns": [
            {"name": "revenue_date", "type": "DATE", "nullable": False, "comment": ""},
            {"name": "channel", "type": "STRING", "nullable": False, "comment": "web|mobile|api"},
            {"name": "gross_revenue", "type": "DOUBLE", "nullable": False, "comment": ""},
            {"name": "net_revenue", "type": "DOUBLE", "nullable": False, "comment": ""},
            {"name": "order_count", "type": "BIGINT", "nullable": False, "comment": ""},
        ],
        "partition_columns": ["revenue_date"],
        "properties": {},
    },
}

_CLUSTERS = [
    {
        "cluster_id": "1234-567890-abc1def",
        "cluster_name": "Data Engineering (Shared)",
        "state": "RUNNING",
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 4,
        "autoscale": {"min_workers": 2, "max_workers": 8},
        "creator_user_name": "admin@example.com",
        "start_time": int((_utcnow() - timedelta(hours=3)).timestamp() * 1000),
    },
    {
        "cluster_id": "2345-678901-bcd2efg",
        "cluster_name": "ML Training GPU",
        "state": "TERMINATED",
        "spark_version": "14.3.x-gpu-ml-scala2.12",
        "node_type_id": "p3.2xlarge",
        "num_workers": 2,
        "autoscale": None,
        "creator_user_name": "mleng@example.com",
        "start_time": None,
    },
    {
        "cluster_id": "3456-789012-cde3fgh",
        "cluster_name": "Interactive Analytics",
        "state": "RUNNING",
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "m5.2xlarge",
        "num_workers": 2,
        "autoscale": {"min_workers": 1, "max_workers": 4},
        "creator_user_name": "analyst@example.com",
        "start_time": int((_utcnow() - timedelta(hours=1)).timestamp() * 1000),
    },
]

_JOBS = [
    {"job_id": 101, "name": "Bronze Ingest - Events", "creator": "etl_svc", "schedule": "0 */1 * * *", "state": "ACTIVE"},
    {"job_id": 102, "name": "Silver Transform - Orders", "creator": "etl_svc", "schedule": "0 2 * * *", "state": "ACTIVE"},
    {"job_id": 103, "name": "Gold Aggregations", "creator": "analytics", "schedule": "0 3 * * *", "state": "ACTIVE"},
    {"job_id": 104, "name": "ML Feature Pipeline", "creator": "mleng", "schedule": "0 4 * * 1", "state": "ACTIVE"},
    {"job_id": 105, "name": "Data Quality Checks", "creator": "etl_svc", "schedule": "0 6 * * *", "state": "PAUSED"},
]

_DBFS_TREE = {
    "/": [
        {"path": "/FileStore", "is_dir": True, "file_size": 0},
        {"path": "/databricks-datasets", "is_dir": True, "file_size": 0},
        {"path": "/user", "is_dir": True, "file_size": 0},
        {"path": "/mnt", "is_dir": True, "file_size": 0},
    ],
    "/FileStore": [
        {"path": "/FileStore/tables", "is_dir": True, "file_size": 0},
        {"path": "/FileStore/plots", "is_dir": True, "file_size": 0},
    ],
    "/mnt": [
        {"path": "/mnt/bronze", "is_dir": True, "file_size": 0},
        {"path": "/mnt/silver", "is_dir": True, "file_size": 0},
        {"path": "/mnt/gold", "is_dir": True, "file_size": 0},
        {"path": "/mnt/checkpoints", "is_dir": True, "file_size": 0},
    ],
    "/mnt/bronze": [
        {"path": "/mnt/bronze/events/2025-03-24.parquet", "is_dir": False, "file_size": 1_048_576},
        {"path": "/mnt/bronze/events/2025-03-23.parquet", "is_dir": False, "file_size": 983_040},
        {"path": "/mnt/bronze/orders/2025-03-24.parquet", "is_dir": False, "file_size": 524_288},
    ],
}


def _mock_sql_result(statement: str, max_rows: int) -> dict[str, Any]:
    """Generate plausible mock result rows based on keywords in the statement."""
    stmt = statement.lower()

    if "daily_revenue" in stmt or "revenue" in stmt:
        columns = ["revenue_date", "channel", "gross_revenue", "net_revenue", "order_count"]
        base = datetime(2025, 3, 1, tzinfo=timezone.utc)
        all_rows = [
            [(base + timedelta(days=i)).strftime("%Y-%m-%d"), ch,
             round(random.uniform(50000, 150000), 2), round(random.uniform(40000, 120000), 2),
             random.randint(500, 5000)]
            for i in range(7) for ch in ["web", "mobile", "api"]
        ]
        rows = all_rows[:max_rows]
    elif "orders" in stmt:
        columns = ["order_id", "user_id", "status", "amount", "order_date"]
        rows = [
            [1000 + i, 200 + i, random.choice(["shipped", "delivered", "pending"]),
             round(random.uniform(10, 500), 2), "2025-03-24"]
            for i in range(min(10, max_rows))
        ]
    elif "show tables" in stmt or "information_schema" in stmt:
        columns = ["table_catalog", "table_schema", "table_name", "table_type"]
        rows = [["main", "silver", "orders", "BASE TABLE"], ["main", "gold", "daily_revenue", "BASE TABLE"]]
    elif "select 1" in stmt:
        columns = ["1"]
        rows = [[1]]
    else:
        columns = ["result"]
        rows = [["(mock) Query executed successfully"]]

    truncated_rows = rows[:max_rows]
    return {
        "columns": columns,
        "rows": truncated_rows,
        "row_count": len(truncated_rows),
        "truncated": len(rows) > len(truncated_rows),
        "warehouse_id": "mock-warehouse-01",
        "statement_id": "mock-stmt-00000001",
    }


class MockClient(DatabricksClientBase):
    """Drop-in stub. All calls return realistic-looking data. No network I/O."""

    async def list_catalogs(self) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        return _CATALOGS

    async def list_schemas(self, catalog: str) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        return _SCHEMAS.get(catalog, [])

    async def list_tables(self, catalog: str, schema: str) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        key = f"{catalog}.{schema}"
        tables = _TABLES.get(key, [])
        return [{**t, "catalog": catalog, "schema": schema} for t in tables]

    async def describe_table(self, catalog: str, schema: str, table: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        key = f"{catalog}.{schema}.{table}"
        if key in _TABLE_DETAILS:
            return _TABLE_DETAILS[key]
        return {
            "full_name": key,
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "owner": "admin",
            "comment": f"(mock) {table}",
            "row_count": random.randint(1000, 1_000_000),
            "size_bytes": random.randint(1_000_000, 1_000_000_000),
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False, "comment": "PK"},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False, "comment": ""},
                {"name": "updated_at", "type": "TIMESTAMP", "nullable": False, "comment": ""},
            ],
            "partition_columns": [],
            "properties": {},
        }

    async def execute_sql(
        self,
        statement: str,
        warehouse_id: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        max_rows: int = 100,
    ) -> dict[str, Any]:
        await asyncio.sleep(0)
        return _mock_sql_result(statement, max_rows)

    async def list_clusters(self) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        return _CLUSTERS

    async def get_cluster(self, cluster_id: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        for c in _CLUSTERS:
            if c["cluster_id"] == cluster_id:
                return c
        raise ValueError(f"Cluster not found: {cluster_id}")

    async def start_cluster(self, cluster_id: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"cluster_id": cluster_id, "state": "PENDING", "message": "(mock) Start initiated"}

    async def terminate_cluster(self, cluster_id: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"cluster_id": cluster_id, "state": "TERMINATING", "message": "(mock) Terminate initiated"}

    async def list_jobs(self, limit: int = 25) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        return _JOBS[:limit]

    async def run_job(self, job_id: int, params: dict[str, Any] | None = None) -> dict[str, Any]:
        await asyncio.sleep(0)
        run_id = random.randint(100_000, 999_999)
        return {
            "run_id": run_id,
            "job_id": job_id,
            "run_page_url": f"https://mock.azuredatabricks.net/#job/{job_id}/run/{run_id}",
            "params": params or {},
        }

    async def get_job_run(self, run_id: int) -> dict[str, Any]:
        await asyncio.sleep(0)
        states = ["RUNNING", "RUNNING", "SUCCESS", "FAILED"]
        state = random.choice(states)
        return {
            "run_id": run_id,
            "state": {"life_cycle_state": state, "result_state": state if state in ("SUCCESS", "FAILED") else None},
            "start_time": int((_utcnow() - timedelta(minutes=5)).timestamp() * 1000),
            "end_time": int(_utcnow().timestamp() * 1000) if state in ("SUCCESS", "FAILED") else None,
            "run_page_url": f"https://mock.azuredatabricks.net/#job/run/{run_id}",
        }

    async def list_dbfs(self, path: str) -> list[dict[str, Any]]:
        await asyncio.sleep(0)
        normalized = path.rstrip("/") or "/"
        return _DBFS_TREE.get(normalized, [])

    async def get_dbfs_file_info(self, path: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        for entries in _DBFS_TREE.values():
            for entry in entries:
                if entry["path"] == path:
                    return entry
        return {"path": path, "is_dir": False, "file_size": 0, "note": "(mock) path not in stub data"}
