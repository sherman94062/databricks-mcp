#!/usr/bin/env bash
# setup.sh - one-shot local setup for databricks-mcp on macOS
# Usage:  chmod +x setup.sh && ./setup.sh

set -euo pipefail

VENV_DIR=".venv"
CLAUDE_DESKTOP_CONFIG="$HOME/Library/Application Support/Claude/claude_desktop_config.json"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
ok()   { echo -e "${GREEN}checkmark${NC} $*"; }
warn() { echo -e "${YELLOW}warning${NC}  $*"; }
err()  { echo -e "${RED}error${NC} $*"; exit 1; }

echo ""
echo "======================================="
echo "  databricks-mcp  - Mac setup"
echo "======================================="
echo ""

# 1. Check Python
if ! command -v python3 &>/dev/null; then
    err "python3 not found. Install via: brew install python@3.12"
fi

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_OK=$(python3 -c "import sys; print('yes' if sys.version_info >= (3,11) else 'no')")
if [[ "$PYTHON_OK" != "yes" ]]; then
    err "Python 3.11+ required (found $PYTHON_VER). Run: brew install python@3.12"
fi
ok "Python $PYTHON_VER"

# 2. Create / reuse virtual environment
if [[ ! -d "$VENV_DIR" ]]; then
    python3 -m venv "$VENV_DIR"
    ok "Created virtual environment at $VENV_DIR"
else
    ok "Using existing virtual environment at $VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# 3. Install package
pip install -q --upgrade pip
pip install -q -e ".[dev]"
ok "Package installed (dev mode)"

# 4. Create .env if missing
if [[ ! -f ".env" ]]; then
    cp .env.example .env
    ok "Created .env from .env.example (mock mode enabled)"
else
    ok ".env already exists - skipping"
fi

# 5. Run tests
echo ""
echo "Running test suite..."
python -m pytest tests/ -v --tb=short
echo ""

# 6. Smoke-test server import
python -c "from databricks_mcp.server import build_server; build_server(); print('Server OK')"
ok "Server import smoke test passed"

# 7. Print Claude Desktop config
echo ""
VENV_PYTHON="$(pwd)/$VENV_DIR/bin/python"
SERVER_DIR="$(pwd)"

echo -e "${YELLOW}=======================================${NC}"
echo "  Claude Desktop config"
echo -e "${YELLOW}=======================================${NC}"
echo ""

if [[ -f "$CLAUDE_DESKTOP_CONFIG" ]]; then
    echo "Config file: $CLAUDE_DESKTOP_CONFIG"
    echo "Add this inside the mcpServers key:"
else
    warn "Claude Desktop config not found at: $CLAUDE_DESKTOP_CONFIG"
    echo "Create the file with:"
    echo '  { "mcpServers": { ... paste below ... } }'
fi

cat <<EOF

{
  "databricks": {
    "command": "$VENV_PYTHON",
    "args": ["-m", "databricks_mcp.server"],
    "cwd": "$SERVER_DIR",
    "env": {
      "DATABRICKS_MOCK": "true"
    }
  }
}

EOF

echo -e "${GREEN}=======================================${NC}"
echo -e "${GREEN}  Setup complete!${NC}"
echo -e "${GREEN}=======================================${NC}"
echo ""
echo "Start server: source $VENV_DIR/bin/activate && python -m databricks_mcp.server"
echo ""
echo "Go live later:"
echo "  1. Edit .env: DATABRICKS_MOCK=false + credentials"
echo "  2. pip install databricks-sdk"
echo "  3. Uncomment TODOs in src/databricks_mcp/client/real.py"
echo ""
