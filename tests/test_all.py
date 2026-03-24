"""
tests/test_all.py - pytest suite for databricks-mcp.
All 25 tests run against MockClient (no Databricks account needed).
"""

import os
os.environ["DATABRICKS_MOCK"] = "true"  # must be set before any imports

import pytest
from databricks_mcp.client.mock import MockClient


@pytest.fixture
def client() -> MockClient:
    return MockClient()


class TestCatalog:
    async def test_list_catalogs_returns_list(self, client):
        result = await client.list_catalogs()
        assert isinstance(result, list)
        assert len(result) > 0
        assert all("name" in c for c in result)

    async def test_list_schemas_main(self, client):
        schemas = await client.list_schemas("main")
        names = [s["name"] for s in schemas]
        assert "bronze" in names
        assert "silver" in names
        assert "gold" in names

    async def test_list_schemas_unknown_catalog(self, client):
        schemas = await client.list_schemas("nonexistent_catalog")
        assert schemas == []

    async def test_list_tables_silver(self, client):
        tables = await client.list_tables("main", "silver")
        names = [t["name"] for t in tables]
        assert "orders" in names
        assert "users" in names

    async def test_describe_table_known(self, client):
        detail = await client.describe_table("main", "silver", "orders")
        assert detail["full_name"] == "main.silver.orders"
        assert isinstance(detail["columns"], list)
        assert detail["row_count"] > 0

    async def test_describe_table_generic_fallback(self, client):
        """Unknown tables should return a generic stub, not raise."""
        detail = await client.describe_table("main", "bronze", "some_unknown_table")
        assert "columns" in detail
        assert detail["full_name"] == "main.bronze.some_unknown_table"


class TestSQL:
    async def test_execute_select_1(self, client):
        result = await client.execute_sql("SELECT 1")
        assert "columns" in result
        assert "rows" in result
        assert result["row_count"] >= 1

    async def test_execute_revenue_query(self, client):
        result = await client.execute_sql("SELECT * FROM main.gold.daily_revenue")
        assert "revenue_date" in result["columns"]

    async def test_max_rows_respected(self, client):
        result = await client.execute_sql("SELECT * FROM main.gold.daily_revenue", max_rows=3)
        assert result["row_count"] <= 3

    async def test_result_structure(self, client):
        result = await client.execute_sql("SELECT * FROM main.silver.orders LIMIT 5")
        assert set(result.keys()) >= {"columns", "rows", "row_count", "truncated"}


class TestClusters:
    async def test_list_clusters(self, client):
        clusters = await client.list_clusters()
        assert len(clusters) > 0
        assert all("cluster_id" in c for c in clusters)
        assert all("state" in c for c in clusters)

    async def test_get_cluster_known(self, client):
        clusters = await client.list_clusters()
        cid = clusters[0]["cluster_id"]
        detail = await client.get_cluster(cid)
        assert detail["cluster_id"] == cid

    async def test_get_cluster_unknown_raises(self, client):
        with pytest.raises(ValueError, match="not found"):
            await client.get_cluster("nonexistent-cluster-id")

    async def test_start_cluster(self, client):
        result = await client.start_cluster("any-cluster-id")
        assert result["state"] in ("PENDING", "RUNNING")

    async def test_terminate_cluster(self, client):
        result = await client.terminate_cluster("any-cluster-id")
        assert result["state"] in ("TERMINATING", "TERMINATED")


class TestJobs:
    async def test_list_jobs(self, client):
        jobs = await client.list_jobs()
        assert len(jobs) > 0
        assert all("job_id" in j for j in jobs)

    async def test_list_jobs_limit(self, client):
        jobs = await client.list_jobs(limit=2)
        assert len(jobs) <= 2

    async def test_run_job(self, client):
        result = await client.run_job(job_id=101)
        assert "run_id" in result
        assert "run_page_url" in result

    async def test_run_job_with_params(self, client):
        result = await client.run_job(job_id=102, params={"env": "prod"})
        assert result["params"]["env"] == "prod"

    async def test_get_job_run(self, client):
        result = await client.get_job_run(run_id=99999)
        assert "state" in result
        assert "run_id" in result


class TestDBFS:
    async def test_list_root(self, client):
        entries = await client.list_dbfs("/")
        paths = [e["path"] for e in entries]
        assert "/mnt" in paths
        assert "/FileStore" in paths

    async def test_list_mnt(self, client):
        entries = await client.list_dbfs("/mnt")
        assert len(entries) > 0
        assert all("path" in e for e in entries)

    async def test_list_unknown_path(self, client):
        entries = await client.list_dbfs("/nonexistent/path")
        assert entries == []

    async def test_get_file_info_known(self, client):
        info = await client.get_dbfs_file_info("/mnt/bronze/events/2025-03-24.parquet")
        assert info["is_dir"] is False
        assert info["file_size"] > 0

    async def test_get_file_info_unknown(self, client):
        info = await client.get_dbfs_file_info("/nonexistent/file.parquet")
        assert "path" in info
