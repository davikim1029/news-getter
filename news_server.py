# news_server.py
"""
FastAPI News Aggregator Service

Features:
1. Background task scheduler for automated news aggregation
2. JSON data migration to database
3. REST API for on-demand news retrieval
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Query
from typing import Optional, List, Dict, Any
from dateutil.parser import parse as parse_datetime
from datetime import datetime, timezone
import asyncio
import os
import json
from contextlib import asynccontextmanager
from models.models import (
    SymbolSentiment,
    SymbolSentimentOut,
    NewsArticle,
    OptionLifetime,
    SentimentResponse,
    MigrationRequest,
    SchedulerConfig,
    AppState,
    TickerSentiment,
    sentiment_to_dict
)
from database.database import (engine, SessionLocal, init_database, get_db)
from sqlalchemy.orm import sessionmaker, Session
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import json

# Import your existing news aggregator
from services.news_aggregator import (
    aggregate_headlines_smart,
    Headline
)
from services.core.cache_manager import RateLimitCache, HeadlineCache
from shared_options.log.logger_singleton import getLogger

logger = getLogger()

app_state = AppState()


# ===========================
# Core Business Logic
# ===========================
from services.news_aggregator import (
    RateLimitedException,
    AggregatorException)

async def aggregate_and_store_ticker(
    ticker: str,
    ticker_name: str | None,
    db: Session,
    force_refresh: bool = False,
) -> dict:
    """
    Production-ready async aggregation pipeline.
    
    Returns a dict compatible with SentimentResponse.

    Guarantees:
    - Always returns a dict on success
    - Raises RateLimitedException if all sources are rate-limited
    - Raises AggregatorException if all sources fail
    """
    ticker = ticker.upper()

    try:
        # ============================
        # 1. Cache check
        # ============================
        if not force_refresh:
            existing: SymbolSentiment | None = await asyncio.to_thread(
                lambda: db.query(SymbolSentiment).filter(SymbolSentiment.symbol == ticker).first()
            )
            if existing:
                if existing.last_updated.tzinfo is None:
                    last_updated_aware = existing.last_updated.replace(tzinfo=timezone.utc)
                else:
                    last_updated_aware = existing.last_updated

                age = datetime.now(timezone.utc) - last_updated_aware

                if age.total_seconds() < 3600:
                    logger.logMessage(f"[API] Cache hit for {ticker} (age={age.total_seconds():.0f}s)")
                    # Fetch recent articles for response
                    articles = await asyncio.to_thread(
                        lambda: [
                            {
                                "source": a.source,
                                "title": a.title,
                                "description": a.description,
                                "url": a.url,
                                "published_at": (
                                    a.published_at.isoformat()
                                    if isinstance(a.published_at, datetime)
                                    else parse_datetime(a.published_at).isoformat()
                                    if a.published_at
                                    else None
                                ),
                                "fetched_at": a.fetched_at.isoformat() if a.fetched_at else None
                            }
                            for a in db.query(NewsArticle)
                                .filter(NewsArticle.symbol == ticker)
                                .order_by(NewsArticle.fetched_at.desc())
                                .limit(50)
                                .all()
                        ]
                    )
                    out = sentiment_to_dict(existing, articles=articles, from_cache=True)
                    logger.logMessage(f"[API] Returning cached sentiment for {ticker}: {out}")
                    return out

        logger.logMessage(f"[API] Aggregating news for {ticker}")

        # ============================
        # 2. Fetch headlines
        # ============================
        headlines: list[Headline] = await asyncio.to_thread(
            aggregate_headlines_smart, ticker, ticker_name or "", app_state.rate_cache
        )

        if headlines is None:
            raise AggregatorException(f"Aggregator returned None for {ticker}")

        # ============================
        # 3. Store articles
        # ============================
        async def store_articles() -> List[Dict[str, Any]]:
            def _store() -> int:
                stored_count = 0
                from datetime import timedelta
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
                deleted = db.query(NewsArticle).filter(
                    NewsArticle.symbol == ticker,
                    NewsArticle.fetched_at < cutoff_date
                ).delete()
                if deleted:
                    logger.logMessage(f"[DB] Removed {deleted} stale articles for {ticker}")

                for h in headlines:
                    if h.url:
                        existing = db.query(NewsArticle).filter(NewsArticle.url == h.url).first()
                        if existing:
                            existing.fetched_at = datetime.now(timezone.utc)
                            continue
                    db.add(NewsArticle(
                        symbol=ticker,
                        source=h.source or "unknown",
                        title=h.title or "",
                        description=h.description,
                        url=h.url,
                        published_at=h.published_at if h.published_at else None,
                        fetched_at=datetime.now(timezone.utc)
                    ))
                    stored_count += 1

                db.commit()
                return stored_count

            stored_count = await asyncio.to_thread(_store)

            return [
                {
                    "source": h.source or "unknown",
                    "title": h.title or "",
                    "description": h.description,
                    "url": h.url,
                    "published_at": (
                        h.published_at.isoformat()
                        if isinstance(h.published_at, datetime)
                        else parse_datetime(h.published_at).isoformat()
                        if h.published_at
                        else None
                    ),
                    "fetched_at": datetime.now(timezone.utc).isoformat()
                } for h in headlines
            ]

        articles_data = await store_articles()

        # ============================
        # 4. Compute sentiment
        # ============================
        from services.news_aggregator import compute_headlines_sentiment
        sentiment_score = await asyncio.to_thread(compute_headlines_sentiment, headlines)

        # ============================
        # 5. Source breakdown
        # ============================
        source_breakdown: dict[str, int] = {}
        for h in headlines:
            source_breakdown[h.source] = source_breakdown.get(h.source, 0) + 1

        # ============================
        # 6. Upsert sentiment record
        # ============================
        def upsert() -> SymbolSentiment:
            record = db.query(SymbolSentiment).filter(SymbolSentiment.symbol == ticker).first()
            if record:
                if ticker_name is not None:
                  record.symbol_name = ticker_name
                record.sentiment_score = sentiment_score
                record.article_count = len(headlines)
                record.source_breakdown = json.dumps(source_breakdown)
                record.last_updated = datetime.now(timezone.utc)
            else:
                record = SymbolSentiment(
                    symbol=ticker,
                    symbol_name=ticker_name,
                    sentiment_score=sentiment_score,
                    article_count=len(headlines),
                    source_breakdown=json.dumps(source_breakdown),
                    last_updated=datetime.now(timezone.utc)
                )
                db.add(record)
            db.commit()
            db.refresh(record)
            return record

        sentiment_record = await asyncio.to_thread(upsert)

        logger.logMessage(
            f"[API] {ticker}: sentiment={sentiment_score:.3f}, "
            f"articles={len(headlines)}, stored={len(articles_data)}"
        )

        return sentiment_to_dict(sentiment_record, articles=articles_data, from_cache=False)

    except RateLimitedException:
        logger.logMessage(f"[API] Rate-limited for {ticker}")
        raise
    except AggregatorException:
        logger.logMessage(f"[API] Aggregation failed for {ticker}")
        raise
    except Exception as e:
        logger.logMessage(f"[API] Fatal error for {ticker}: {e}")
        db.rollback()
        raise


async def store_articles(symbol: str, headlines: List[Headline], db: Session) -> int:
    """
    Store individual articles in the news_articles table.
    - Prevents duplicates based on URL
    - Removes stale articles (older than 30 days)
    - Returns count of newly stored articles
    """
    try:
        stored_count = 0
        
        # Clean up stale articles first (older than 30 days)
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)
        deleted = db.query(NewsArticle).filter(
            NewsArticle.symbol == symbol,
            NewsArticle.fetched_at < cutoff_date
        ).delete()
        
        if deleted > 0:
            logger.logMessage(f"[DB] Removed {deleted} stale articles for {symbol}")
        
        # Store new articles
        for h in headlines:
            # Check if article already exists (by URL)
            if h.url:
                existing = db.query(NewsArticle).filter(
                    NewsArticle.url == h.url
                ).first()
                
                if existing:
                    # Update fetched_at to keep it fresh
                    existing.fetched_at = datetime.now(timezone.utc)
                    continue
            
            # Create new article
            article = NewsArticle(
                symbol=symbol,
                source=h.source or "unknown",
                title=h.title or "",
                description=h.description,
                url=h.url,
                published_at=h.published_at if h.published_at else None,
                fetched_at=datetime.now(timezone.utc)
            )
            db.add(article)
            stored_count += 1
        
        db.commit()
        return stored_count
        
    except Exception as e:
        logger.logMessage(f"[DB] Error storing articles for {symbol}: {e}")
        db.rollback()
        return 0


import asyncio
from sqlalchemy.orm import Session

BATCH_SIZE = 20  # process 20 tickers concurrently

async def process_all_tickers(db: Session):
    """
    Process all unique symbols in batches concurrently.
    Prevents blocking the event loop and keeps the API responsive.
    """
    if app_state.is_processing:
        logger.logMessage("[Scheduler] Already processing, skipping...")
        return

    symbol_map = load_symbol_map(db)

    # Get unique symbols
    from sqlalchemy import distinct
    symbols = db.query(distinct(OptionLifetime.symbol)).all()
    unique_symbols = [s[0] for s in symbols if s[0]]

    if not unique_symbols:
        logger.logMessage("[Scheduler] No symbols to process")
        return

    logger.logMessage(f"[Scheduler] Processing {len(unique_symbols)} unique symbols")

    app_state.is_processing = True
    processed = 0
    errors = 0

    try:
        # Split symbols into batches
        for i in range(0, len(unique_symbols), BATCH_SIZE):
            batch = unique_symbols[i:i+BATCH_SIZE]

            # Create tasks for this batch
            tasks = [
                process_single_ticker(symbol, symbol_map.get(symbol))
                for symbol in batch
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Count successes/errors
            for r in results:
                if isinstance(r, Exception):
                    errors += 1
                    logger.logMessage(f"[Scheduler] Error in batch: {r}")
                elif r:
                    processed += 1
                else:
                    errors += 1

            # Yield to event loop to prevent blocking
            await asyncio.sleep(0.1)

        logger.logMessage(
            f"[Scheduler] Batch complete: {processed} processed, {errors} errors"
        )

    finally:
        app_state.is_processing = False


async def process_single_ticker(symbol: str, ticker_name: str | None):
    db = SessionLocal()
    try:
        return await aggregate_and_store_ticker(symbol, ticker_name, db)
    finally:
        db.close()



from sqlalchemy import text

def load_symbol_map(db: Session) -> Dict[str, str]:
    result = db.execute(
        text("SELECT symbol, name FROM tickers")
    )
    return {row.symbol: row.name for row in result}


def migrate_json_to_db(json_path: str, db: Session, backup: bool = True) -> Dict[str, Any]:
    """
    Migrate existing JSON news data to database.
    
    Expected JSON format:
    {
        "AAPL": {
            "sentiment": 0.5,
            "headlines": [...],
            "timestamp": "2024-01-01T00:00:00Z"
        },
        ...
    }
    """
    try:
        # Backup existing data if requested
        if backup:
            backup_data = db.query(TickerSentiment).all()
            backup_file = f"sentiment_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(backup_file, 'w') as f:
                json.dump(
                    [
                        {
                            "ticker": r.ticker,
                            "sentiment_score": r.sentiment_score,
                            "headlines": json.loads(r.headlines) if r.headlines else [],  # Deserialize for backup
                            "last_updated": r.last_updated.isoformat()
                        }
                        for r in backup_data
                    ],
                    f,
                    indent=2
                )
            logger.logMessage(f"[Migration] Backup saved to {backup_file}")
        
        # Load JSON data
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        migrated = 0
        errors = 0
        
        for ticker, ticker_data in data.items():
            try:
                headlines = ticker_data.get("headlines", [])
                
                # Count by source
                source_breakdown = {}
                for h in headlines:
                    source = h.get("source", "unknown")
                    source_breakdown[source] = source_breakdown.get(source, 0) + 1
                
                # Parse timestamp
                timestamp_str = ticker_data.get("timestamp")
                timestamp = None
                if timestamp_str:
                    try:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    except:
                        timestamp = datetime.now(timezone.utc)
                else:
                    timestamp = datetime.now(timezone.utc)          
                
                record = TickerSentiment(
                    ticker=ticker.upper(),
                    ticker_name=ticker_data.get("ticker_name"),
                    sentiment_score=ticker_data.get("sentiment", 0.0),
                    headlines=json.dumps(headlines),  # Serialize to JSON string
                    headline_count=len(headlines),
                    source_breakdown=json.dumps(source_breakdown),  # Serialize to JSON string
                    last_updated=timestamp
                )
                
                db.add(record)
                migrated += 1
                
            except Exception as e:
                logger.logMessage(f"[Migration] Error migrating {ticker}: {e}")
                errors += 1
        
        db.commit()
        
        result = {
            "status": "success",
            "migrated": migrated,
            "errors": errors,
            "total": len(data)
        }
        
        logger.logMessage(f"[Migration] Complete: {result}")
        return result
        
    except Exception as e:
        logger.logMessage(f"[Migration] Failed: {e}")
        db.rollback()
        raise


# ===========================
# Scheduler Setup
# ===========================
async def start_scheduler():
    """Initialize and start the background scheduler"""
    app_state.scheduler = AsyncIOScheduler()
    
    # Default: run every 60 minutes
    interval_minutes = int(os.getenv("AGGREGATION_INTERVAL_MINUTES", "60"))
    
    #Run once at startup
    print ("[Scheduler] Running initial ticker save at startup...")
    await save_tickers_to_db(db=SessionLocal())
    print ("[Scheduler] Initial ticker save complete.")
    app_state.scheduler.add_job(
        job_wrapper,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="news_aggregation",
        name="Aggregate news for all tickers",
        replace_existing=True,
        kwargs={"job_func": process_all_tickers}  # job_wrapper now calls this
    )
    
    app_state.scheduler.add_job(
        job_wrapper,
        trigger=IntervalTrigger(weeks=2),
        id="ticker_saver",
        name="Fetch and save US tickers to database",
        replace_existing=True,
        kwargs={"job_func": save_tickers_to_db}
    )
        
    app_state.scheduler.start()
    logger.logMessage(f"[Scheduler] Started with {interval_minutes}min interval")
    

async def save_tickers_to_db(db: Session = None):
    """Fetch and save US tickers to the database"""
    
    logger.logMessage("[Tickers] Fetching and saving US tickers to database...")
    from shared_options.models.tickers import fetch_us_tickers_from_finnhub
    if db is None:
        db = SessionLocal()
    try:
        ticker_dict = fetch_us_tickers_from_finnhub(None)
        for symbol, name in ticker_dict.items():
            existing = db.execute(
                text("SELECT 1 FROM tickers WHERE symbol = :symbol"),
                {"symbol": symbol},
            ).fetchone()
            if not existing:
                db.execute(
                    text(
                        "INSERT INTO tickers (symbol, name, timestamp) "
                        "VALUES (:symbol, :name, :timestamp)"
                    ),
                    {
                        "symbol": symbol,
                        "name": name,
                        "timestamp": datetime.now(timezone.utc),
                    },
                )
        db.commit()
        logger.logMessage(f"[Tickers] Saved {len(ticker_dict)} US tickers to database")
    except Exception as e:
        logger.logMessage(f"[Tickers] Error saving tickers: {e}")
        db.rollback()
    finally:
        db.close()


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
    from dotenv import load_dotenv
    load_dotenv()  # reads .env into os.environ
    
    logger.logMessage("[App] Starting up...")
    
    # Initialize database (create tables if needed)
    init_database()
    
    # Warm up transformer model if enabled
    if True or os.getenv("USE_TRANSFORMERS", "false").lower() == "true":
        logger.logMessage("[App] Initializing FinBERT model...")
        from services.news_aggregator import _load_transformer_pipeline
        await asyncio.to_thread(_load_transformer_pipeline)
    
    await start_scheduler()
    
    yield
    
    # Shutdown
    logger.logMessage("[App] Shutting down...")
    await stop_scheduler()
        


# ===========================
# FastAPI App
# ===========================
app = FastAPI(
    title="News Aggregator Service",
    description="Automated news aggregation and sentiment analysis for stock tickers",
    version="1.0.0",
    lifespan=lifespan
)


# ===========================
# API Endpoints
# ===========================

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "News Aggregator API",
        "scheduler_running": app_state.scheduler.running if app_state.scheduler else False,
        "is_processing": app_state.is_processing
    }
    
from models.models import SentimentResponse

@app.post("/sentiment/{symbol}/score", response_model=SentimentResponse)
async def get_sentiment_score(
    symbol: str,
    include_articles: bool = Query(False, description="Include full article info"),
    force_refresh: bool = False,
    db: Session = Depends(get_db)
):
    """
    Fetch sentiment for a given ticker symbol.
    Returns cached data if available and not force_refresh.
    Includes the last 50 articles.
    """
    symbol = symbol.upper()

    try:
        # aggregate_and_store_ticker now returns a dict compatible with SentimentResponse
        sentiment_data = await aggregate_and_store_ticker(
            ticker=symbol,
            ticker_name=load_symbol_map(db).get(symbol),
            db=db,
            force_refresh=force_refresh
        )

    except RateLimitedException as e:
        raise HTTPException(status_code=429, detail=str(e))
    except AggregatorException as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    # Ensure the articles key exists
    if "articles" not in sentiment_data:
        sentiment_data["articles"] = []

    # Set from_cache properly
    sentiment_data["from_cache"] = not force_refresh

    if not include_articles:
        sentiment_data["articles"] = []
        
    # Construct Pydantic response
    response = SentimentResponse(**sentiment_data)

    logger.logMessage(f"[API] Sentiment response for {symbol}: {response}")
    return response


@app.post("/migrate")
async def migrate_json_data(
    request: MigrationRequest,
    db: Session = Depends(get_db)
):
    """
    Migrate existing JSON news data to database.
    
    Expects JSON file with structure:
    {
        "TICKER": {
            "sentiment": float,
            "headlines": [...],
            "timestamp": "ISO-8601"
        }
    }
    """
    try:
        result = migrate_json_to_db(
            request.json_file_path,
            db,
            request.backup_existing
        )
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="JSON file not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Migration failed: {str(e)}")


@app.post("/scheduler/trigger")
async def trigger_batch_processing(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Manually trigger batch processing of all tickers"""
    if app_state.is_processing:
        return {"status": "already_running", "message": "Batch processing already in progress"}
    
    background_tasks.add_task(process_all_tickers, db)
    return {"status": "triggered", "message": "Batch processing started"}


@app.get("/scheduler/status")
async def get_scheduler_status():
    """Get current scheduler status"""
    if not app_state.scheduler:
        return {"status": "not_initialized"}
    
    jobs = []
    for job in app_state.scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time if job.next_run_time else None
        })
    
    return {
        "running": app_state.scheduler.running,
        "is_processing": app_state.is_processing,
        "jobs": jobs
    }


@app.post("/scheduler/configure")
async def configure_scheduler(config: SchedulerConfig):
    """Update scheduler configuration"""
    if not app_state.scheduler:
        raise HTTPException(status_code=500, detail="Scheduler not initialized")
    
    job = app_state.scheduler.get_job("news_aggregation")
    
    if config.enabled:
        if job:
            # Update existing job
            app_state.scheduler.reschedule_job(
                "news_aggregation",
                trigger=IntervalTrigger(minutes=config.interval_minutes)
            )
        else:
            # Add new job
            app_state.scheduler.add_job(
                process_all_tickers,
                trigger=IntervalTrigger(minutes=config.interval_minutes),
                id="news_aggregation",
                name="Aggregate news for all tickers",
                replace_existing=True,
                kwargs={"db": SessionLocal()}
            )
        message = f"Scheduler configured: {config.interval_minutes}min interval"
    else:
        if job:
            app_state.scheduler.remove_job("news_aggregation")
        message = "Scheduler disabled"
    
    return {"status": "success", "message": message, "config": config}


@app.get("/stats")
async def get_statistics(db: Session = Depends(get_db)):
    """Get overall statistics about sentiment data"""
    from sqlalchemy import func
    
    total_symbols = db.query(func.count(SymbolSentiment.id)).scalar()
    avg_sentiment = db.query(func.avg(SymbolSentiment.sentiment_score)).scalar()
    total_articles = db.query(func.count(NewsArticle.id)).scalar()
    
    recent_updates = db.query(SymbolSentiment).order_by(
        SymbolSentiment.last_updated.desc()
    ).limit(10).all()
    
    # Get unique symbols from option_lifetimes
    from sqlalchemy import distinct
    total_option_symbols = db.query(func.count(distinct(OptionLifetime.symbol))).scalar()
    
    return {
        "total_symbols_tracked": total_symbols,
        "total_option_symbols": total_option_symbols,
        "average_sentiment": float(avg_sentiment) if avg_sentiment else 0.0,
        "total_articles": total_articles or 0,
        "recent_updates": [
            {
                "symbol": r.symbol,
                "sentiment": r.sentiment_score,
                "article_count": r.article_count,
                "last_updated": r.last_updated
            }
            for r in recent_updates
        ]
    }


@app.get("/symbols")
async def list_symbols(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """List all symbols from option_lifetimes table"""
    from sqlalchemy import distinct
    
    symbols = db.query(distinct(OptionLifetime.symbol)).offset(skip).limit(limit).all()
    symbol_list = [s[0] for s in symbols if s[0]]
    
    return {
        "symbols": symbol_list,
        "count": len(symbol_list),
        "skip": skip,
        "limit": limit
    }


@app.get("/articles/{symbol}")
async def get_symbol_articles(
    symbol: str,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """Get all stored articles for a specific symbol"""
    symbol = symbol.upper()
    
    articles = db.query(NewsArticle).filter(
        NewsArticle.symbol == symbol
    ).order_by(NewsArticle.fetched_at.desc()).limit(limit).all()
    
    if not articles:
        raise HTTPException(
            status_code=404,
            detail=f"No articles found for {symbol}"
        )
    
    return {
        "symbol": symbol,
        "article_count": len(articles),
        "articles": [
            {
                "id": a.id,
                "source": a.source,
                "title": a.title,
                "description": a.description,
                "url": a.url,
                "published_at": (
                    a.published_at.isoformat()
                    if isinstance(a.published_at, datetime)
                    else parse_datetime(a.published_at).isoformat()
                    if a.published_at
                    else None
                ),
                "fetched_at": a.fetched_at.isoformat() if a.fetched_at else None
            }
            for a in articles
        ]
    }


@app.delete("/articles/cleanup")
async def cleanup_stale_articles(
    days: int = 30,
    db: Session = Depends(get_db)
):
    """Remove articles older than specified days"""
    from datetime import timedelta
    
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    
    deleted = db.query(NewsArticle).filter(
        NewsArticle.fetched_at < cutoff_date
    ).delete()
    
    db.commit()
    
    return {
        "status": "success",
        "deleted_count": deleted,
        "cutoff_date": cutoff_date.isoformat(),
        "message": f"Removed {deleted} articles older than {days} days"
    }
    
    
from typing import Callable
from sqlalchemy.orm import Session
import asyncio

async def job_wrapper(job_func: Callable[..., asyncio.Future], **kwargs):
    """
    Generic wrapper for scheduler jobs.
    - Creates a fresh DB session per run
    - Runs the job in an async-safe way
    - Ensures the DB session is closed even if job fails
    - Runs blocking sync functions in a thread if needed
    """
    # Create a fresh DB session if not provided
    db: Session = kwargs.get("db") or SessionLocal()
    kwargs["db"] = db  # ensure job_func gets a db param
    
    try:
        if asyncio.iscoroutinefunction(job_func):
            await job_func(**kwargs)
        else:
            # If the job_func is synchronous (blocking), run it in a thread
            await asyncio.to_thread(job_func, **kwargs)
    except Exception as e:
        logger.logMessage(f"[Scheduler] Error in job {job_func.__name__}: {e}")
    finally:
        db.close()



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=9000,
        reload=True,
        log_level="info"
    )