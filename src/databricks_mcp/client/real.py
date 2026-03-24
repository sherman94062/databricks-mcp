"""
client/real.py - Live Databricks SDK client.

Requires:  pip install databricks-sdk
           DATABRICKS_HOST and DATABRICKS_TOKEN env vars

All methods are stubbed with clear TODO markers so you know exactly where
to drop in the SDK calls once you have an account. The class structure
mirrors MockClient 1-for-1, making the swap trivial.
"""

from __future__ import annotations
from typing import Any

from .base import DatabricksClientBase


class RealClient(DatabricksClientBase):
    """
    Live client backed by the Databricks SDK.
    Instantiation wires up the SDK WorkspaceClient.
    Replace each TODO block with the corresponding SDK call.
    """

    def __init__(self, host: str, token: str, warehouse_id: str = "", default_catalog: str = "main") -> None:
        # TODO: uncomment after `pip install databricks-sdk`
        # from databricks.sdk import WorkspaceClient
        # self._ws = WorkspaceClient(host=host, token=token)
        self._host = host
        self._token = token
        self._warehouse_id = warehouse_id
        self._default_catalog = default_catalog

    async def list_catalogs(self) -> list[dict[str, Any]]:
        # TODO: return [c.as_dict() for c in self._ws.catalogs.list()]
        raise NotImplementedError("Install databricks-sdk and uncomment SDK calls")

    async def list_schemas(self, catalog: str) -> list[dict[str, Any]]:
        # TODO: return [s.as_dict() for s in self._ws.schemas.list(catalog_name=catalog)]
        raise NotImplementedError

    async def list_tables(self, catalog: str, schema: str) -> list[dict[str, Any]]:
        # TODO: return [t.as_dict() for t in self._ws.tables.list(catalog_name=catalog, schema_name=schema)]
        raise NotImplementedError

    async def describe_table(self, catalog: str, schema: str, table: str) -> dict[str, Any]:
        # TODO: return self._ws.tables.get(f"{catalog}.{schema}.{table}").as_dict()
        raise NotImplementedError

    async def execute_sql(
        self,
        statement: str,
        warehouse_id: str | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        max_rows: int = 100,
    ) -> dict[str, Any]:
        # TODO:
        # wh_id = warehouse_id or self._warehouse_id
        # resp = self._ws.statement_execution.execute_statement(
        #     statement=statement, warehouse_id=wh_id,
        #     catalog=catalog or self._default_catalog, schema=schema,
        #     disposition="INLINE", format="JSON_ARRAY",
        # )
        # columns = [col.name for col in resp.manifest.schema.columns]
        # rows = resp.result.data_array or []
        # return {"columns": columns, "rows": rows[:max_rows], "row_count": len(rows),
        #         "truncated": len(rows) >= max_rows, "statement_id": resp.statement_id}
        raise NotImplementedError

    async def list_clusters(self) -> list[dict[str, Any]]:
        # TODO: return [c.as_dict() for c in self._ws.clusters.list()]
        raise NotImplementedError

    async def get_cluster(self, cluster_id: str) -> dict[str, Any]:
        # TODO: return self._ws.clusters.get(cluster_id).as_dict()
        raise NotImplementedError

    async def start_cluster(self, cluster_id: str) -> dict[str, Any]:
        # TODO: self._ws.clusters.start(cluster_id); return {"cluster_id": cluster_id, "state": "PENDING"}
        raise NotImplementedError

    async def terminate_cluster(self, cluster_id: str) -> dict[str, Any]:
        # TODO: self._ws.clusters.delete(cluster_id); return {"cluster_id": cluster_id, "state": "TERMINATING"}
        raise NotImplementedError

    async def list_jobs(self, limit: int = 25) -> list[dict[str, Any]]:
        # TODO: return [j.as_dict() for j in self._ws.jobs.list(limit=limit)]
        raise NotImplementedError

    async def run_job(self, job_id: int, params: dict[str, Any] | None = None) -> dict[str, Any]:
        # TODO:
        # run = self._ws.jobs.run_now(job_id=job_id, notebook_params=params)
        # return {"run_id": run.run_id, "run_page_url": run.run_page_url}
        raise NotImplementedError

    async def get_job_run(self, run_id: int) -> dict[str, Any]:
        # TODO: return self._ws.jobs.get_run(run_id=run_id).as_dict()
        raise NotImplementedError

    async def list_dbfs(self, path: str) -> list[dict[str, Any]]:
        # TODO: return [f.as_dict() for f in self._ws.dbfs.list(path=path)]
        raise NotImplementedError

    async def get_dbfs_file_info(self, path: str) -> dict[str, Any]:
        # TODO: return self._ws.dbfs.get_status(path=path).as_dict()
        raise NotImplementedError
