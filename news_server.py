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
    compute_headlines_sentiment,
    Headline,
)
from services.core.cache_manager import RateLimitCache, HeadlineCache
from shared_options.log.logger_singleton import getLogger
from shared_options.services.monitor_status import MonitorStatus

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
    force_refresh: bool = False,
) -> dict:
    ticker = ticker.upper()

    try:
        # ============================
        # 1. Cache check (DB READ)
        # ============================
        if not force_refresh:
            with db_session() as db:
                existing: SymbolSentiment | None = (
                    db.query(SymbolSentiment)
                    .filter(SymbolSentiment.symbol == ticker)
                    .first()
                )

                if existing:
                    last_updated = (
                        existing.last_updated.replace(tzinfo=timezone.utc)
                        if existing.last_updated.tzinfo is None
                        else existing.last_updated
                    )

                    age = datetime.now(timezone.utc) - last_updated
                    if age.total_seconds() < 3600:
                        logger.logMessage(
                            f"[API] Cache hit for {ticker} (age={age.total_seconds():.0f}s)"
                        )

                        articles = [
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
                                "fetched_at": a.fetched_at.isoformat() if a.fetched_at else None,
                            }
                            for a in (
                                db.query(NewsArticle)
                                .filter(NewsArticle.symbol == ticker)
                                .order_by(NewsArticle.fetched_at.desc())
                                .limit(50)
                                .all()
                            )
                        ]

                        return sentiment_to_dict(existing, articles=articles, from_cache=True)

        logger.logMessage(f"[API] Aggregating news for {ticker}")

        # ============================
        # 2. Fetch headlines (NON-DB)
        # ============================
        headlines: list[Headline] = await asyncio.to_thread(
            aggregate_headlines_smart,
            ticker,
            ticker_name or "",
            app_state.rate_cache,
        )

        if headlines is None:
            raise AggregatorException(f"Aggregator returned None for {ticker}")

        # ============================
        # 3. Store articles (DB WRITE)
        # ============================
        with db_session() as db:
            from datetime import timedelta

            now = datetime.now(timezone.utc)
            cutoff_date = now - timedelta(days=1)

            deleted = (
                db.query(NewsArticle)
                .filter(
                    NewsArticle.symbol == ticker,
                    NewsArticle.fetched_at < cutoff_date,
                )
                .delete()
            )

            if deleted:
                logger.logMessage(f"[DB] Removed {deleted} stale articles for {ticker}")

            # Bulk-fetch all existing URLs for this ticker — one query instead of N
            incoming_urls = {h.url for h in headlines if h.url}
            existing_urls: set[str] = set()
            if incoming_urls:
                existing_urls = {
                    row.url
                    for row in db.query(NewsArticle.url)
                    .filter(NewsArticle.url.in_(incoming_urls))
                    .all()
                }
                # Touch fetched_at for duplicates in one update
                stale_urls = incoming_urls & existing_urls
                if stale_urls:
                    db.query(NewsArticle).filter(
                        NewsArticle.url.in_(stale_urls)
                    ).update({"fetched_at": now}, synchronize_session=False)

            articles_data = []
            for h in headlines:
                if h.url and h.url in existing_urls:
                    continue

                article = NewsArticle(
                    symbol=ticker,
                    source=h.source or "unknown",
                    title=h.title or "",
                    description=h.description,
                    url=h.url,
                    published_at=h.published_at if h.published_at else None,
                    fetched_at=now,
                )
                db.add(article)

                articles_data.append(
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
                        "fetched_at": now.isoformat(),
                    }
                )

            db.commit()

        # ============================
        # 4. Compute sentiment (NON-DB)
        # ============================
        sentiment_score = await asyncio.to_thread(
            compute_headlines_sentiment, headlines
        )

        # ============================
        # 5. Source breakdown
        # ============================
        source_breakdown: dict[str, int] = {}
        for h in headlines:
            source_breakdown[h.source] = source_breakdown.get(h.source, 0) + 1

        # ============================
        # 6. Upsert sentiment (DB WRITE)
        # ============================
        with db_session() as db:
            record = (
                db.query(SymbolSentiment)
                .filter(SymbolSentiment.symbol == ticker)
                .first()
            )

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
                    last_updated=datetime.now(timezone.utc),
                )
                db.add(record)

            db.commit()
            db.refresh(record)

        logger.logMessage(
            f"[API] {ticker}: sentiment={sentiment_score:.3f}, "
            f"articles={len(headlines)}"
        )

        return sentiment_to_dict(record, articles=articles_data, from_cache=False)

    except RateLimitedException:
        logger.logMessage(f"[API] Rate-limited for {ticker}")
        raise
    except AggregatorException:
        logger.logMessage(f"[API] Aggregation failed for {ticker}")
        raise
    except Exception as e:
        logger.logMessage(f"[API] Fatal error for {ticker}: {e}")
        raise


import asyncio
from sqlalchemy.orm import Session

BATCH_SIZE = 20  # process 20 tickers concurrently

async def process_all_tickers():
    if app_state.is_processing:
        logger.logMessage("[Scheduler] Already processing, skipping...")
        return

    with db_session() as db:
        symbol_map = load_symbol_map(db)
        symbols = db.query(OptionLifetime.symbol).distinct().all()

    unique_symbols = [s[0] for s in symbols if s[0]]

    if not unique_symbols:
        logger.logMessage("[Scheduler] No symbols to process")
        return

    ms = MonitorStatus.instance()
    total = len(unique_symbols)
    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    with ms._lock:
        cycles = ms._data["counters"].get("Cycles", 0) + 1
        total_processed_so_far = ms._data["counters"].get("Tickers Processed", 0)
        ms._data["counters"]["Cycles"] = cycles
    ms.set_progress(0, total, "tickers")
    ms.set_step(f"Processing {total} tickers in {num_batches} batch(es)")

    app_state.is_processing = True
    done = 0
    try:
        for i in range(0, total, BATCH_SIZE):
            batch = unique_symbols[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            ms.set_step(f"Batch {batch_num}/{num_batches} — {batch[0]}..{batch[-1]}")

            tasks = [
                process_single_ticker(symbol, symbol_map.get(symbol))
                for symbol in batch
            ]

            await asyncio.gather(*tasks, return_exceptions=True)
            done += len(batch)
            ms.set_progress(done, total, "tickers")
            ms.set_counter("Tickers Processed", total_processed_so_far + done)
            await asyncio.sleep(0.1)

    finally:
        app_state.is_processing = False
        ms.set_progress(done, total, "tickers")
        ms.set_step("Idle — waiting for next cycle")
        ms.set_last_event(f"Completed {done}/{total} tickers")


DB_CONCURRENCY_LIMIT = 4
db_semaphore = asyncio.Semaphore(DB_CONCURRENCY_LIMIT)


async def process_single_ticker(symbol: str, ticker_name: Optional[str]):
    async with db_semaphore:
        return await aggregate_and_store_ticker(
            ticker=symbol,
            ticker_name=ticker_name,
        )




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
async def _evict_caches():
    if app_state.rate_cache:
        n = app_state.rate_cache.evict_expired()
        if n:
            logger.logMessage(f"[Cache] Evicted {n} expired rate-limit entries")

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

    app_state.scheduler.add_job(
        job_wrapper,
        trigger=IntervalTrigger(minutes=30),
        id="cache_eviction",
        name="Evict expired in-memory cache entries",
        replace_existing=True,
        kwargs={"job_func": _evict_caches}
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
        # Bulk-fetch all existing symbols in one query
        existing_symbols = {
            row[0]
            for row in db.execute(text("SELECT symbol FROM tickers")).fetchall()
        }
        new_symbols = {sym: meta for sym, meta in ticker_dict.items() if sym not in existing_symbols}
        if new_symbols:
            now = datetime.now(timezone.utc)
            db.execute(
                text(
                    "INSERT INTO tickers (symbol, name, timestamp) "
                    "VALUES (:symbol, :name, :timestamp)"
                ),
                [
                    {"symbol": sym, "name": meta.get("name"), "timestamp": now}
                    for sym, meta in new_symbols.items()
                ],
            )
        db.commit()
        logger.logMessage(
            f"[Tickers] {len(new_symbols)} new tickers inserted ({len(ticker_dict)} fetched)"
        )
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


from contextlib import contextmanager

@contextmanager
def db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ===========================
# Lifespan Management
# ===========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    from dotenv import load_dotenv
    load_dotenv()  # reads .env into os.environ

    ms = MonitorStatus.instance("news-getter")
    ms.set_step("Starting up")
    logger.logMessage("[App] Starting up...")
    
    # Initialize database (create tables if needed)
    init_database()
    
    # Warm up transformer model if enabled
    if os.getenv("USE_TRANSFORMERS", "false").lower() == "true":
        logger.logMessage("[App] Initializing FinBERT model...")
        from services.news_aggregator import _load_transformer_pipeline
        await asyncio.to_thread(_load_transformer_pipeline)
    
    await start_scheduler()
    ms.set_step("Idle — waiting for next cycle")

    yield

    # Shutdown
    ms.set_step("Shutting down")
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


@app.get("/sentiment/aggregate")
async def get_aggregate_sentiment(
    tickers: Optional[str] = Query(
        None,
        description="Comma-separated ticker list. Omit for market-wide average."
    ),
    db: Session = Depends(get_db),
):
    """
    Return the average sentiment score across a set of tickers.

    - No `tickers` param → market-wide average of all tracked symbols.
    - `tickers=AAPL,MSFT,...` → average only for those symbols.

    Only includes symbols that have a recorded sentiment score.
    Returns 0.0 with ticker_count=0 when no data is available.
    """
    from sqlalchemy import func

    query = db.query(
        func.avg(SymbolSentiment.sentiment_score).label("avg_score"),
        func.count(SymbolSentiment.id).label("cnt"),
    )

    if tickers:
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        if ticker_list:
            query = query.filter(SymbolSentiment.symbol.in_(ticker_list))

    row = query.one()
    avg_score = float(row.avg_score) if row.avg_score is not None else 0.0
    ticker_count = int(row.cnt) if row.cnt else 0

    scope = "market" if not tickers else f"custom({ticker_count} symbols)"

    return {
        "scope": scope,
        "sentiment_score": avg_score,
        "ticker_count": ticker_count,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


@app.delete("/articles/cleanup")
async def cleanup_stale_articles(
    days: int = 1,
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

async def job_wrapper(job_func, **kwargs):
    try:
        if asyncio.iscoroutinefunction(job_func):
            await job_func(**kwargs)
        else:
            await asyncio.to_thread(job_func, **kwargs)
    except Exception as e:
        logger.logMessage(f"[Scheduler] Error in job {job_func.__name__}: {e}")



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=9000,
        reload=True,
        log_level="info"
    )