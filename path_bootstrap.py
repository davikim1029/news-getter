"""Runtime path bootstrap for monorepo editable dependencies."""

from __future__ import annotations

import sys
import os
from pathlib import Path


def ensure_shared_options_path() -> None:
    shared_root = Path(__file__).resolve().parent.parent / "options-shared"
    if shared_root.exists():
        shared_path = str(shared_root)
        if shared_path not in sys.path:
            sys.path.insert(0, shared_path)


def is_schema_export() -> bool:
    return os.getenv("OPENAPI_SCHEMA_EXPORT") == "1"
