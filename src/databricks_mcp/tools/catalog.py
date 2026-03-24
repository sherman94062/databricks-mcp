"""
tools/catalog.py - MCP tools for Unity Catalog browsing.
"""

from __future__ import annotations
import json
from mcp.server.fastmcp import FastMCP
from databricks_mcp.client import get_client


def register(mcp: FastMCP) -> None:
    client = get_client()

    @mcp.tool()
    async def list_catalogs() -> str:
        """List all Unity Catalog catalogs in the Databricks workspace."""
        return json.dumps(await client.list_catalogs(), indent=2)

    @mcp.tool()
    async def list_schemas(catalog: str) -> str:
        """
        List all schemas within a Unity Catalog catalog.

        Args:
            catalog: Catalog name (e.g. "main", "samples")
        """
        return json.dumps(await client.list_schemas(catalog), indent=2)

    @mcp.tool()
    async def list_tables(catalog: str, schema: str) -> str:
        """
        List all tables within a schema.

        Args:
            catalog: Catalog name (e.g. "main")
            schema:  Schema name (e.g. "silver")
        """
        return json.dumps(await client.list_tables(catalog, schema), indent=2)

    @mcp.tool()
    async def describe_table(catalog: str, schema: str, table: str) -> str:
        """
        Return full metadata for a table: columns, partition columns, row count, size, owner.

        Args:
            catalog: Catalog name
            schema:  Schema name
            table:   Table name
        """
        return json.dumps(await client.describe_table(catalog, schema, table), indent=2)
