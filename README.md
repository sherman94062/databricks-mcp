# Databricks MCP Server

A [Model Context Protocol](https://modelcontextprotocol.io) server for Databricks, built in Python.
Exposes Databricks capabilities as MCP tools consumable by Claude Desktop, Claude Code, or any MCP-compatible client.

## Quickstart (mock mode — no Databricks account needed)

```bash
git clone https://github.com/sherman94062/databricks-mcp
cd databricks-mcp
chmod +x setup.sh && ./setup.sh
```

The setup script checks Python 3.11+, creates a `.venv`, installs deps, runs 25 tests, and prints the Claude Desktop config block with correct absolute paths filled in.

## Manual install

```bash
pip install -e ".[dev]"
DATABRICKS_MOCK=true python -m databricks_mcp.server
```

## Claude Desktop config

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "databricks": {
      "command": "/path/to/databricks-mcp/.venv/bin/python",
      "args": ["-m", "databricks_mcp.server"],
      "cwd": "/path/to/databricks-mcp",
      "env": { "DATABRICKS_MOCK": "true" }
    }
  }
}
```

Run `./setup.sh` to get this block with your real paths.

## Live Mode (when you have a Databricks account)

```bash
cp .env.example .env
# Edit .env: set DATABRICKS_MOCK=false, fill in HOST + TOKEN
pip install databricks-sdk
# Uncomment TODO blocks in src/databricks_mcp/client/real.py
```

## Available Tools (14 total)

| Tool | Description |
|------|-------------|
| `list_catalogs` | List Unity Catalog catalogs |
| `list_schemas` | List schemas within a catalog |
| `list_tables` | List tables within a schema |
| `describe_table` | Full schema + stats for a table |
| `execute_sql` | Run SQL on a warehouse, return rows as JSON |
| `list_clusters` | List all compute clusters + state |
| `get_cluster` | Details for a single cluster |
| `start_cluster` | Start a terminated cluster |
| `terminate_cluster` | Terminate a running cluster |
| `list_jobs` | List Databricks Jobs |
| `run_job` | Trigger a job run |
| `get_job_run` | Status + result for a job run |
| `list_dbfs` | Browse DBFS directory |
| `get_dbfs_file_info` | Metadata for a single DBFS path |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABRICKS_MOCK` | `true` | Use mock client (no creds needed) |
| `DATABRICKS_HOST` | — | Workspace URL, e.g. `https://adb-xxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | — | Personal access token or service principal token |
| `DATABRICKS_WAREHOUSE_ID` | — | Default SQL warehouse ID |
| `DATABRICKS_CATALOG` | `main` | Default Unity Catalog catalog |

## Architecture

```
src/databricks_mcp/
├── server.py          # FastMCP entry point & tool registration
├── config.py          # Env var / .env loader
├── client/
│   ├── base.py        # Abstract DatabricksClient interface
│   ├── mock.py        # Stub client — full mock data, no network
│   └── real.py        # Live Databricks SDK client (TODOs marked)
└── tools/
    ├── catalog.py     # Unity Catalog tools
    ├── sql.py         # SQL execution
    ├── clusters.py    # Cluster management
    ├── jobs.py        # Job orchestration
    └── dbfs.py        # DBFS browsing
```

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v   # 25 tests, all mock, all green
```

## Extending

1. Add method to `DatabricksClientBase` in `client/base.py`
2. Implement in both `client/mock.py` and `client/real.py`
3. Add `@mcp.tool()` function in the appropriate `tools/` module
4. Import/register in `server.py`
