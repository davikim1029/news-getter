# Setup — news-getter

This service uses [UV](https://docs.astral.sh/uv/) for dependency management.

## First-time setup on a new machine

```bash
chmod +x setup.sh
./setup.sh
```

The script will:
1. Install UV if not already present (via `curl`)
2. Verify `options-shared` exists at `../options-shared`
3. Create `.venv/` and install all dependencies pinned in `uv.lock`
4. Confirm the `shared_options` package is importable

**Requirements:** `curl` must be available. The full monorepo must be cloned so that `options-shared` exists as a sibling directory.

> **Note:** This service includes `torch` and `transformers`. First-time sync will download ~1–2 GB of model weights and wheel files.

## After pulling changes

If `pyproject.toml` or `uv.lock` changed upstream, re-sync:

```bash
uv sync
```

## Common commands

| Command | Description |
|---|---|
| `uv run python main.py` | Interactive mode |
| `uv run python main.py --mode start-server` | Start the news server |
| `uv run python main.py --mode monitor` | Monitor loop (auto-restart) |
| `uv run python main.py --mode stop` | Stop the server |
| `uv run python main.py --mode check` | Check server status |
| `uv run python main.py --mode logs` | Tail last 20 log lines |
| `source .venv/bin/activate` | Activate venv manually |

## Dependency files

| File | Commit? | Purpose |
|---|---|---|
| `pyproject.toml` | ✅ Yes | Declares direct dependencies |
| `uv.lock` | ✅ Yes | Pinned lockfile for reproducible installs |
| `.venv/` | ❌ No | Local virtualenv — created by `uv sync` |
