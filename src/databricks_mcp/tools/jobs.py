"""
tools/jobs.py - MCP tools for Databricks Jobs / Workflows.
"""

from __future__ import annotations
import json
from typing import Optional
from mcp.server.fastmcp import FastMCP
from databricks_mcp.client import get_client


def register(mcp: FastMCP) -> None:
    client = get_client()

    @mcp.tool()
    async def list_jobs(limit: int = 25) -> str:
        """
        List Databricks Jobs in the workspace.

        Args:
            limit: Maximum number of jobs to return (default 25)
        """
        return json.dumps(await client.list_jobs(limit=limit), indent=2)

    @mcp.tool()
    async def run_job(job_id: int, params: Optional[str] = None) -> str:
        """
        Trigger a Databricks Job run.

        Args:
            job_id: The integer Job ID (visible in the Jobs UI)
            params: Optional JSON string of notebook parameters,
                    e.g. '{"start_date": "2025-03-01", "env": "prod"}'

        Returns:
            JSON with run_id and run_page_url for monitoring.
        """
        parsed_params: dict | None = None
        if params:
            try:
                parsed_params = json.loads(params)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"params must be valid JSON: {e}"})
        return json.dumps(await client.run_job(job_id=job_id, params=parsed_params), indent=2)

    @mcp.tool()
    async def get_job_run(run_id: int) -> str:
        """
        Return the current status and result of a Databricks Job run.

        Args:
            run_id: The run ID returned by run_job or visible in the Jobs UI
        """
        return json.dumps(await client.get_job_run(run_id=run_id), indent=2, default=str)
