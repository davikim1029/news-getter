import os
import sqlite3
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# ===========================
# Database Setup
# ===========================
BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_NEWS_DB_PATH = BASE_DIR / "database" / "news.db"
DEFAULT_OPTIONS_DB_PATH = BASE_DIR.parent / "option-file-server" / "database" / "options.db"

NEWS_DB_PATH = Path(os.getenv("NEWS_DB_PATH", str(DEFAULT_NEWS_DB_PATH))).expanduser()
OPTIONS_DB_PATH = Path(os.getenv("NEWS_OPTIONS_DB_PATH", str(DEFAULT_OPTIONS_DB_PATH))).expanduser()

DATABASE_URL = f"sqlite:///{NEWS_DB_PATH}"

# SQLite-specific engine configuration
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite
    pool_pre_ping=True
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ===========================

def init_database():
    """Initialize news-getter-owned schema via versioned migrations."""
    from database.migrations import run_migrations
    run_migrations(str(NEWS_DB_PATH))
    _import_legacy_news_tables_if_needed()
    logger.logMessage(
        f"[DB] Database initialization complete (news_db={NEWS_DB_PATH}, options_db={OPTIONS_DB_PATH})"
    )


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    try:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except sqlite3.Error:
        return 0


def _import_legacy_news_tables_if_needed() -> None:
    """One-time bootstrap from legacy news tables previously stored in options.db.

    DATA-02 moved the writable news-owned tables out of option-file-server's
    ``options.db``. To make the cutover operationally boring, startup copies
    legacy rows only when the new news DB is empty. Disable with
    ``NEWS_IMPORT_LEGACY_ON_INIT=false``.
    """
    if os.getenv("NEWS_IMPORT_LEGACY_ON_INIT", "true").strip().lower() in {"0", "false", "no"}:
        return
    news_path = NEWS_DB_PATH.resolve()
    options_path = OPTIONS_DB_PATH.resolve()
    if news_path == options_path or not options_path.exists():
        return

    conn = sqlite3.connect(str(news_path), timeout=30)
    try:
        if _table_count(conn, "news_articles") or _table_count(conn, "symbol_sentiment"):
            return
        conn.execute("ATTACH DATABASE ? AS legacy", (str(options_path),))
        legacy_tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM legacy.sqlite_master WHERE type='table'"
            ).fetchall()
        }
        copied: dict[str, int] = {}
        if "news_articles" in legacy_tables:
            conn.execute(
                """
                INSERT OR IGNORE INTO news_articles
                    (id, symbol, source, title, description, url, published_at, fetched_at)
                SELECT id, symbol, source, title, description, url, published_at, fetched_at
                FROM legacy.news_articles
                """
            )
            copied["news_articles"] = conn.total_changes
        if "symbol_sentiment" in legacy_tables:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO symbol_sentiment
                    (id, symbol, symbol_name, sentiment_score, article_count, last_updated, source_breakdown)
                SELECT id, symbol, symbol_name, sentiment_score, article_count, last_updated, source_breakdown
                FROM legacy.symbol_sentiment
                """
            )
            copied["symbol_sentiment"] = conn.total_changes - before
        if "tickers" in legacy_tables:
            before = conn.total_changes
            conn.execute(
                """
                INSERT OR IGNORE INTO tickers (symbol, name, timestamp)
                SELECT symbol, name, timestamp FROM legacy.tickers
                """
            )
            copied["tickers"] = conn.total_changes - before
        conn.commit()
        if copied:
            logger.logMessage(f"[DB] Imported legacy news rows from options.db: {copied}")
    except Exception as e:
        conn.rollback()
        logger.logMessage(f"[DB] Legacy news import skipped/failed: {e}")
    finally:
        conn.close()


def list_option_symbols(*, skip: int = 0, limit: int | None = None) -> list[str]:
    """Read distinct option symbols from option-file-server's DB without writing to it."""
    if not OPTIONS_DB_PATH.exists():
        logger.logMessage(f"[DB] options DB not found for symbol discovery: {OPTIONS_DB_PATH}")
        return []
    sql = "SELECT DISTINCT symbol FROM option_lifetimes WHERE symbol IS NOT NULL ORDER BY symbol"
    params: list[int] = []
    if limit is not None:
        sql += " LIMIT ? OFFSET ?"
        params.extend([int(limit), int(skip)])
    conn = sqlite3.connect(f"file:{OPTIONS_DB_PATH}?mode=ro", uri=True, timeout=30)
    try:
        return [row[0] for row in conn.execute(sql, params).fetchall() if row[0]]
    except sqlite3.Error as e:
        logger.logMessage(f"[DB] Failed to list option symbols from {OPTIONS_DB_PATH}: {e}")
        return []
    finally:
        conn.close()


def count_option_symbols() -> int:
    """Return the distinct option symbol count from the read-only upstream options DB."""
    if not OPTIONS_DB_PATH.exists():
        return 0
    conn = sqlite3.connect(f"file:{OPTIONS_DB_PATH}?mode=ro", uri=True, timeout=30)
    try:
        return int(conn.execute(
            "SELECT COUNT(DISTINCT symbol) FROM option_lifetimes WHERE symbol IS NOT NULL"
        ).fetchone()[0] or 0)
    except sqlite3.Error as e:
        logger.logMessage(f"[DB] Failed to count option symbols from {OPTIONS_DB_PATH}: {e}")
        return 0
    finally:
        conn.close()


# ===========================
# Dependency Injection
# ===========================
def get_db():
    """Database session dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
