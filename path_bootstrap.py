"""Makes the shared library importable, and tells code when only the API schema is wanted.

Every service in this monorepo imports `shared_options` from the sibling `options-shared`
checkout. The service's virtualenv normally provides it as an editable install; putting
the checkout on `sys.path` as well means imports still work when that install is missing.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path


def ensure_shared_options_path() -> None:
    """Put `../options-shared` on `sys.path`. Call before importing `shared_options`."""
    shared_root = Path(__file__).resolve().parent.parent / "options-shared"
    if shared_root.exists():
        shared_path = str(shared_root)
        if shared_path not in sys.path:
            sys.path.insert(0, shared_path)


def is_schema_export() -> bool:
    """True while `tools/openapi_artifact_check.py` imports the app just to read its routes.

    In that mode, code skips side effects such as creating cache files on disk.
    """
    return os.getenv("OPENAPI_SCHEMA_EXPORT") == "1"
