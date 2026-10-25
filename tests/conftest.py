"""Test setup for news-getter: every file the service would write goes to a scratch folder.

This runs before any test module imports the service. That matters because importing
news_server opens the rate-limit cache file, and the database paths are fixed when
database/database.py is first imported.
"""
# Before ANY import that could build the shared logger. Its default folder is the
# cwd-relative `logs/`, and the nightly test gate runs from this service's folder, so
# without this, fixture output would land in the production log (TESTLOG-01).
import atexit as _atexit
import os as _os
import shutil as _shutil
import tempfile as _tempfile

_SCRATCH = _tempfile.mkdtemp(prefix="news-getter-tests-")
_atexit.register(_shutil.rmtree, _SCRATCH, True)
if not _os.environ.get("OPTIONS_LOG_DIR"):
    _os.environ["OPTIONS_LOG_DIR"] = _os.path.join(_SCRATCH, "logs")
# The service's own database, the read-only options.db it lists tickers from, and the
# back-off file: all scratch, so no test can read or change the real ones.
_os.environ["NEWS_DB_PATH"] = _os.path.join(_SCRATCH, "news.db")
_os.environ["NEWS_OPTIONS_DB_PATH"] = _os.path.join(_SCRATCH, "options.db")
_os.environ["NEWS_RATE_LIMIT_CACHE_PATH"] = _os.path.join(_SCRATCH, "cache", "ratelimit.json")
_os.environ["NEWS_IMPORT_LEGACY_ON_INIT"] = "false"
# The keyword scorer is what production runs (USE_TRANSFORMERS is not deployed), and it
# needs no model download.
_os.environ["USE_TRANSFORMERS"] = "false"

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture
def news_db():
    """A freshly migrated, empty news.db, plus the `news_server` module bound to it."""
    import news_server
    from database.database import SessionLocal, init_database

    init_database()
    with SessionLocal() as db:
        for table in ("news_articles", "symbol_sentiment", "tickers", "sentiment_requests"):
            db.execute(news_server.text(f"DELETE FROM {table}"))
        db.commit()
    news_server.app_state.failed_fetches.clear()
    news_server.app_state.request_stamped.clear()
    return news_server


@pytest.fixture(autouse=True)
def _no_admin_key(monkeypatch):
    """Run with no admin key configured, which is development mode: admin routes are open."""
    for name in ("NEWS_ADMIN_API_KEY", "ANALYSIS_API_KEY", "APP_ENV", "ENV"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(f"{name}_PREVIOUS", raising=False)
