import sqlite3
from datetime import datetime
from pathlib import Path

from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# Uses a separate table from option-file-server's `schema_migrations` since both
# services write to the same shared database and need isolated version namespaces.
_MIGRATION_TABLE = "news_migrations"


def _migration_v1_base_schema(conn):
    """
    v1: Create news_articles, symbol_sentiment, and tickers tables with indexes.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS news_articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol VARCHAR(10) NOT NULL,
            source VARCHAR(50) NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            url TEXT,
            published_at VARCHAR(50),
            fetched_at DATETIME
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS symbol_sentiment (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol VARCHAR(10) NOT NULL UNIQUE,
            symbol_name VARCHAR(255),
            sentiment_score REAL,
            article_count INTEGER,
            last_updated DATETIME,
            source_breakdown TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS tickers (
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            PRIMARY KEY (symbol)
        )
    """)

    conn.execute("CREATE INDEX IF NOT EXISTS ix_news_articles_symbol ON news_articles(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_news_articles_fetched_at ON news_articles(fetched_at)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uix_symbol_url ON news_articles(symbol, url)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_fetched ON news_articles(symbol, fetched_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_url ON news_articles(url)")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_symbol_sentiment_symbol ON symbol_sentiment(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sentiment_updated ON symbol_sentiment(last_updated DESC)")


_MIGRATIONS = [
    (1, _migration_v1_base_schema),
    # (2, _migration_v2_...),  # add future migrations here
]


def run_migrations(db_path: str):
    """Apply all pending news-getter schema migrations. Safe to call on every startup."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_MIGRATION_TABLE} (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
        """)
        conn.commit()

        applied = {row[0] for row in conn.execute(f"SELECT version FROM {_MIGRATION_TABLE}")}
        for version, fn in _MIGRATIONS:
            if version in applied:
                continue
            try:
                conn.execute("BEGIN IMMEDIATE")
                fn(conn)
                conn.execute(
                    f"INSERT INTO {_MIGRATION_TABLE} (version, applied_at) VALUES (?, ?)",
                    (version, datetime.now().isoformat()),
                )
                conn.commit()
                logger.logMessage(f"[Migrations] news-getter: Applied migration v{version}")
            except Exception as e:
                conn.rollback()
                logger.logMessage(f"[Migrations] news-getter: Migration v{version} failed: {e}")
                raise
    finally:
        conn.close()
