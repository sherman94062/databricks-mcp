"""
client/base.py - Abstract interface that both MockClient and RealClient must satisfy.

Add a method here first, then implement it in mock.py and real.py.
The tools layer only ever imports DatabricksClientBase - never a concrete class directly.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class DatabricksClientBase(ABC):

    # Unity Catalog

    @abstractmethod
    async def list_catalogs(self) -> list[dict[str, Any]]:
        """Return list of catalog objects: [{name, type, comment, owner}]"""

    @abstractmethod
    async def list_schemas(self, catalog: str) -> list[dict[str, Any]]:
        """Return list of schema objects within a catalog."""

    @abstractmethod
    async def list_tables(self, catalog: str, schema: str) -> list[dict[str, Any]]:
        """Return list of table summaries within a schema."""

    @abstractmethod
    async def describe_table(self, catalog: str, schema: str, table: str) -> dict[str, Any]:
        """Return full table metadata: columns, partitions, owner, row count, etc."""

    # SQL Execution

    @abstractmethod
    async def execute_sql(
        self,
        statement: str,
        warehouse_id: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        max_rows: int = 100,
    ) -> dict[str, Any]:
        """{columns: [str], rows: [[Any]], row_count: int, truncated: bool}"""

    # Clusters

    @abstractmethod
    async def list_clusters(self) -> list[dict[str, Any]]:
        """Return list of cluster summaries."""

    @abstractmethod
    async def get_cluster(self, cluster_id: str) -> dict[str, Any]:
        """Return full cluster detail."""

    @abstractmethod
    async def start_cluster(self, cluster_id: str) -> dict[str, Any]:
        """Start a terminated cluster. Returns {cluster_id, state}."""

    @abstractmethod
    async def terminate_cluster(self, cluster_id: str) -> dict[str, Any]:
        """Terminate a running cluster. Returns {cluster_id, state}."""

    # Jobs

    @abstractmethod
    async def list_jobs(self, limit: int = 25) -> list[dict[str, Any]]:
        """Return list of job summaries."""

    @abstractmethod
    async def run_job(self, job_id: int, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Trigger a job run. Returns {run_id, run_page_url}."""

    @abstractmethod
    async def get_job_run(self, run_id: int) -> dict[str, Any]:
        """Return status + result for a job run."""

    # DBFS

    @abstractmethod
    async def list_dbfs(self, path: str) -> list[dict[str, Any]]:
        """List files/directories at a DBFS path."""

    @abstractmethod
    async def get_dbfs_file_info(self, path: str) -> dict[str, Any]:
        """Return metadata for a single DBFS path."""
