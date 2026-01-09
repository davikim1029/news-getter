from sqlalchemy import Column, String, Float, DateTime, Text, Integer, UniqueConstraint, Index
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass
from datetime import datetime
from database.database import Base
from services.core.cache_manager import RateLimitCache, HeadlineCache
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ===========================
# Global State
# ===========================
class AppState:
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.rate_cache = RateLimitCache()
        self.headline_cache = HeadlineCache()
        self.is_processing = False


# ===========================
# Pydantic Models
# ===========================
class SentimentResponse(BaseModel):
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


class MigrationRequest(BaseModel):
    json_file_path: str = Field(..., description="Path to JSON file with news data")
    backup_existing: bool = Field(default=True, description="Backup existing data before migration")


class TickerRequest(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=10, description="Stock symbol")
    symbol_name: Optional[str] = None
    force_refresh: bool = Field(default=False, description="Force new aggregation even if cached")


class SchedulerConfig(BaseModel):
    interval_minutes: int = Field(default=60, ge=5, le=1440)
    enabled: bool = True



@dataclass
class TickerSentiment:
    ticker: str
    ticker_name: Optional[str]
    sentiment_score: float
    headlines: str              # JSON-serialized string
    headline_count: int
    source_breakdown: str       # JSON-serialized string
    last_updated: datetime

class NewsArticle(Base):
    """Stores individual news articles for reference"""
    __tablename__ = "news_articles"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    source = Column(String(50), nullable=False)
    title = Column(Text, nullable=False)
    description = Column(Text)
    url = Column(Text)
    published_at = Column(String(50))  # Store as ISO string
    fetched_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Prevent duplicate articles by URL per symbol
    __table_args__ = (
        UniqueConstraint('symbol', 'url', name='uix_symbol_url'),
        Index('idx_symbol_fetched', 'symbol', 'fetched_at'),
    )


class SymbolSentiment(Base):
    """Stores computed sentiment scores for symbols"""
    __tablename__ = "symbol_sentiment"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True, unique=True)
    symbol_name = Column(String(255))
    sentiment_score = Column(Float)
    article_count = Column(Integer)
    last_updated = Column(DateTime, default=datetime.utcnow, index=True)
    source_breakdown = Column(Text) 


class SymbolSentimentOut(BaseModel):
    symbol: str
    symbol_name: Optional[str]
    sentiment_score: float
    article_count: int
    source_breakdown: dict
    last_updated: datetime

    # Allow constructing from ORM objects
    model_config = {"from_attributes": True}


class OptionLifetime(Base):
    """Maps to existing option_lifetimes table - read only"""
    __tablename__ = "option_lifetimes"

    osiKey = Column("osiKey", Text, primary_key=True)
    timestamp = Column(Text, nullable=False)
    symbol = Column(Text, nullable=False, index=True)


import json

# Step 1: Convert ORM → dict before caching
def sentiment_to_dict(record: SymbolSentiment):
    return {
        "symbol": record.symbol,
        "symbol_name": record.symbol_name,
        "sentiment_score": record.sentiment_score or 0.0,
        "article_count": record.article_count or 0,
        "source_breakdown": json.loads(record.source_breakdown or "{}"),
        "last_updated": record.last_updated.isoformat() if record.last_updated else None,
        "from_cache": True,
    }
