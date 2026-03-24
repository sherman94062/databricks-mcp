"""
tools/sql.py - MCP tool for SQL warehouse query execution.
"""

from __future__ import annotations
import json
from mcp.server.fastmcp import FastMCP
from databricks_mcp.client import get_client
from databricks_mcp.config import settings


def register(mcp: FastMCP) -> None:
    client = get_client()

    @mcp.tool()
    async def execute_sql(
        statement: str,
        warehouse_id: str = "",
        catalog: str = "",
        schema: str = "",
        max_rows: int = 100,
    ) -> str:
        """
        Execute a SQL statement on a Databricks SQL warehouse and return results.

        Args:
            statement:    SQL to execute (SELECT, SHOW, DESCRIBE, etc.)
            warehouse_id: SQL warehouse ID. Omit to use the workspace default.
            catalog:      Default catalog context.
            schema:       Default schema context.
            max_rows:     Maximum rows to return (default 100, max 1000).

        Returns:
            JSON with: columns, rows, row_count, truncated, statement_id.
        """
        result = await client.execute_sql(
            statement=statement,
            warehouse_id=warehouse_id or settings.warehouse_id or None,
            catalog=catalog or settings.default_catalog or None,
            schema=schema or settings.default_schema or None,
            max_rows=min(max_rows, 1000),
        )
        return json.dumps(result, indent=2, default=str)
