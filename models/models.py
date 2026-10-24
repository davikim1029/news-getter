"""Data shapes used by news-getter: process state, API request/response models, and DB tables.

The two SQLAlchemy tables (`news_articles`, `symbol_sentiment`) are only *mapped* here so the
code can query them. They are created and changed by `database/migrations.py`, so adding a
column here alone does not add it to the database.
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pydantic import BaseModel, Field
from database.database import Base
from path_bootstrap import is_schema_export
from services.core.cache_manager import RateLimitCache
from sqlalchemy import Column, DateTime, Float, Index, Integer, String, Text, UniqueConstraint

# ===========================
# Global State
# ===========================
class AppState:
    """Process-wide state shared by the scheduler and the API routes (one instance, in news_server.py)."""

    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        # Skipped during a schema export so importing the app creates no cache file.
        self.rate_cache = None if is_schema_export() else RateLimitCache()
        # True while a full all-tickers run is in progress; stops two runs overlapping.
        self.is_processing = False


# ===========================
# Pydantic Models
# ===========================
class SentimentResponse(BaseModel):
    """Response body of POST /sentiment/{symbol}/score."""

    symbol: str
    symbol_name: Optional[str]
    sentiment_score: float
    article_count: int
    articles: List[Dict[str, Any]]
    source_breakdown: Dict[str, int]
    last_updated: datetime
    from_cache: bool

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class SchedulerConfig(BaseModel):
    """Request body of POST /scheduler/configure: how often the all-tickers run repeats."""

    interval_minutes: int = Field(default=60, ge=5, le=1440)
    enabled: bool = True


class NewsArticle(Base):
    """One fetched headline for one ticker. Rows older than a day are purged on that ticker's next fetch."""
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    source = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    description = Column(Text)
    url = Column(Text)
    published_at = Column(String(50))  # as the provider sent it: ISO for NewsAPI, RFC 822 for Google
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Prevent duplicate articles by URL per symbol
    __table_args__ = (
        UniqueConstraint('symbol', 'url', name='uix_symbol_url'),
        Index('idx_symbol_fetched', 'symbol', 'fetched_at'),
    )


class SymbolSentiment(Base):
    """The latest sentiment score for one ticker, overwritten on every successful fetch."""
    __tablename__ = "symbol_sentiment"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True, unique=True)
    symbol_name = Column(String(255))
    sentiment_score = Column(Float)
    article_count = Column(Integer)
    last_updated = Column(DateTime, default=datetime.utcnow, index=True)
    source_breakdown = Column(Text)  # JSON object: provider name -> headline count


def sentiment_to_dict(
    record: SymbolSentiment,
    articles: Optional[List[Dict[str, Any]]] = None,
    from_cache: bool = True
) -> dict:
    """
    Turn a SymbolSentiment row into a dict with SentimentResponse's fields.

    `articles` is attached as given (empty if omitted); missing numbers become 0 and a
    missing timestamp becomes "now".
    """
    return {
        "symbol": record.symbol,
        "symbol_name": record.symbol_name,
        "sentiment_score": record.sentiment_score or 0.0,
        "article_count": record.article_count or 0,
        "articles": articles or [],
        "source_breakdown": json.loads(record.source_breakdown or "{}"),
        "last_updated": record.last_updated or datetime.utcnow(),
        "from_cache": from_cache,
    }
