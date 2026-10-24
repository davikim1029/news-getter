#!/usr/bin/env bash
# Prepares news-getter to run on a new machine: installs uv (the Python package manager
# this repo uses) if missing, checks that the shared library checkout ../options-shared
# exists, installs this service's dependencies into .venv, and confirms the shared
# library imports. Safe to re-run; it changes nothing that is already in place.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SERVICE_NAME="$(basename "$SERVICE_ROOT")"
SHARED_DIR="$SERVICE_ROOT/../options-shared"

echo ""
echo "=== Setting up: $SERVICE_NAME ==="
echo ""

# UV installs to ~/.local/bin — ensure it's on PATH for this session
export PATH="$HOME/.local/bin:$PATH"

# --- Step 1: Install UV if missing ---
if command -v uv &>/dev/null; then
    echo "[1/3] UV already installed — $(uv --version)"
else
    echo "[1/3] Installing UV..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    echo "      Installed: $(uv --version)"
fi

# --- Step 2: Verify options-shared is present ---
echo "[2/3] Checking options-shared..."
if [ ! -d "$SHARED_DIR" ]; then
    echo ""
    echo "ERROR: options-shared not found at $SHARED_DIR"
    echo "The full monorepo must be cloned — options-shared must exist as a sibling directory."
    exit 1
fi
echo "      Found at $SHARED_DIR"

# --- Step 3: Install dependencies from lockfile ---
echo "[3/3] Syncing dependencies (uv sync)..."
cd "$SERVICE_ROOT"
uv sync

# --- Verify shared_options editable install ---
echo ""
if uv run python -c "from shared_options.log.logger_singleton import getLogger" 2>/dev/null; then
    echo "✓  shared_options import: OK"
else
    echo "✗  shared_options import FAILED"
    echo "   Check that options-shared/pyproject.toml exists and re-run this script."
    exit 1
fi

PYTHON_VERSION=$(uv run python --version)

echo ""
echo "================================================"
echo " $SERVICE_NAME is ready."
echo " Python: $PYTHON_VERSION (.venv/bin/python)"
echo "================================================"
echo ""
echo "Run the service:"
echo "  uv run python main.py                     # interactive mode"
echo "  uv run python main.py --mode start-server # start in the background"
echo "  uv run python main.py --mode monitor      # run, restarting on crash"
echo "  uv run python main.py --mode stop         # stop"
echo ""
echo "Manage dependencies:"
echo "  uv sync                # re-sync after pulling changes"
echo "  uv add <package>       # add a dependency"
echo "  uv remove <package>    # remove a dependency"
echo "  uv lock --upgrade      # upgrade all deps within constraints"
echo ""
