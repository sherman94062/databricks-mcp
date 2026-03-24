"""
server.py - Databricks MCP server entry point.

Instantiates a FastMCP server, registers all tool groups,
and starts the stdio transport (used by Claude Desktop / Claude Code).

Run:
    python -m databricks_mcp.server
    # or after pip install -e .:
    databricks-mcp
"""

from mcp.server.fastmcp import FastMCP

from databricks_mcp.config import settings
from databricks_mcp.tools import catalog, sql, clusters, jobs, dbfs


def build_server() -> FastMCP:
    mode = "mock" if settings.mock else f"live ({settings.host})"
    mcp = FastMCP(
        name="databricks",
        instructions=(
            "Databricks MCP server. Provides tools for Unity Catalog browsing, "
            "SQL execution, cluster management, job orchestration, and DBFS navigation. "
            f"Running in {mode} mode."
        ),
    )
    catalog.register(mcp)
    sql.register(mcp)
    clusters.register(mcp)
    jobs.register(mcp)
    dbfs.register(mcp)
    return mcp


def main() -> None:
    mcp = build_server()
    mode = "MOCK" if settings.mock else f"LIVE -> {settings.host}"
    print(f"[databricks-mcp] Starting | mode={mode} | tools=14")
    mcp.run()


if __name__ == "__main__":
    main()
