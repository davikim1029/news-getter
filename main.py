# main.py
"""
FastAPI News Aggregator Service

Features:
1. Background task scheduler for automated news aggregation
2. JSON data migration to database
3. REST API for on-demand news retrieval
"""

from fastapi import FastAPI
from typing import Optional
import asyncio as redis
import asyncio
import os
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from database.database import init_database, SessionLocal
from services.processor import process_all_tickers

from services.core.cache_manager import RateLimitCache, HeadlineCache
from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# ===========================
# Global State
# ===========================
class AppState:
    def __init__(self):
        self.scheduler: Optional[AsyncIOScheduler] = None
        self.rate_cache = RateLimitCache()
        self.headline_cache = HeadlineCache()
        self.is_processing = False


app_state = AppState()

# ===========================
# Scheduler Setup
# ===========================
async def start_scheduler():
    """Initialize and start the background scheduler"""
    app_state.scheduler = AsyncIOScheduler()
    
    # Default: run every 60 minutes
    interval_minutes = int(os.getenv("AGGREGATION_INTERVAL_MINUTES", "60"))
    
    app_state.scheduler.add_job(
        process_all_tickers,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="news_aggregation",
        name="Aggregate news for all tickers",
        replace_existing=True,
        kwargs={"db": SessionLocal()}
    )
    
    app_state.scheduler.start()
    logger.logMessage(f"[Scheduler] Started with {interval_minutes}min interval")


async def stop_scheduler():
    """Shutdown the scheduler gracefully"""
    if app_state.scheduler:
        app_state.scheduler.shutdown()
        logger.logMessage("[Scheduler] Stopped")


# ===========================
# Lifespan Management
# ===========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.logMessage("[App] Starting up...")
    
    app.state.redis = redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379"),
    decode_responses=True,  # strings instead of bytes
    )

    # Optional: verify connection
    await app.state.redis.ping()
    logger.logMessage("[App] Redis connected")
    
    # Initialize database (create tables if needed)
    init_database()
    
    # Warm up transformer model if enabled
    if os.getenv("USE_TRANSFORMERS", "false").lower() == "true":
        logger.logMessage("[App] Initializing FinBERT model...")
        from services.news_aggregator import _load_transformer_pipeline
        await asyncio.to_thread(_load_transformer_pipeline)
    
    await start_scheduler()
    
    yield
    
    # Shutdown
    logger.logMessage("[App] Shutting down...")
    await stop_scheduler()
    
    await app.state.redis.close()
    logger.logMessage("[App] Redis connection closed")


# ===========================
# FastAPI App
# ===========================
app = FastAPI(
    title="News Aggregator Service",
    description="Automated news aggregation and sentiment analysis for stock tickers",
    version="1.0.0",
    lifespan=lifespan
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )