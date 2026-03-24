"""
tools/clusters.py - MCP tools for Databricks cluster management.
"""

from __future__ import annotations
import json
from mcp.server.fastmcp import FastMCP
from databricks_mcp.client import get_client


def register(mcp: FastMCP) -> None:
    client = get_client()

    @mcp.tool()
    async def list_clusters() -> str:
        """List all Databricks clusters with state, Spark version, node type, and worker count."""
        return json.dumps(await client.list_clusters(), indent=2, default=str)

    @mcp.tool()
    async def get_cluster(cluster_id: str) -> str:
        """
        Return full detail for a single Databricks cluster.

        Args:
            cluster_id: The cluster ID (e.g. "1234-567890-abc1def")
        """
        return json.dumps(await client.get_cluster(cluster_id), indent=2, default=str)

    @mcp.tool()
    async def start_cluster(cluster_id: str) -> str:
        """
        Start a terminated Databricks cluster.

        Args:
            cluster_id: The cluster ID to start
        """
        return json.dumps(await client.start_cluster(cluster_id), indent=2)

    @mcp.tool()
    async def terminate_cluster(cluster_id: str) -> str:
        """
        Terminate a running Databricks cluster.
        Note: interrupts any running Spark jobs on the cluster.

        Args:
            cluster_id: The cluster ID to terminate
        """
        return json.dumps(await client.terminate_cluster(cluster_id), indent=2)
