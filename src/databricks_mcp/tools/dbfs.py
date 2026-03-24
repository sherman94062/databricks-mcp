"""
tools/dbfs.py - MCP tools for Databricks File System (DBFS) browsing.
"""

from __future__ import annotations
import json
from mcp.server.fastmcp import FastMCP
from databricks_mcp.client import get_client


def register(mcp: FastMCP) -> None:
    client = get_client()

    @mcp.tool()
    async def list_dbfs(path: str = "/") -> str:
        """
        List files and directories at a DBFS path.

        Args:
            path: DBFS path to list (e.g. "/mnt/bronze", "/FileStore/tables").
                  Defaults to root "/".
        """
        return json.dumps(await client.list_dbfs(path), indent=2)

    @mcp.tool()
    async def get_dbfs_file_info(path: str) -> str:
        """
        Return metadata for a single DBFS path.

        Args:
            path: Full DBFS path (e.g. "/mnt/bronze/events/2025-03-24.parquet")
        """
        return json.dumps(await client.get_dbfs_file_info(path), indent=2)
