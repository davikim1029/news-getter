# main.py
"""
FastAPI News Aggregator Service

Features:
1. Background task scheduler for automated news aggregation
2. JSON data migration to database
3. REST API for on-demand news retrieval
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone
import asyncio
import os
import json
from contextlib import asynccontextmanager
from models.models import (
    SymbolSentiment,
    NewsArticle,
    OptionLifetime,
    SentimentResponse,
    MigrationRequest,
    SchedulerConfig,
    AppState,
    TickerSentiment
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
async def aggregate_and_store_ticker(
    ticker: str,
    ticker_name: Optional[str],
    db: Session,
    force_refresh: bool = False
) -> Optional[SymbolSentiment]:
    """
    Aggregate news for a ticker and store in database.
    Returns the stored/updated sentiment record.
    """
    try:
        # Check if we should use cached data
        if not force_refresh:
            existing = db.query(SymbolSentiment).filter(
                SymbolSentiment.symbol == ticker.upper()
            ).first()
            
            if existing:
                age = datetime.utcnow() - existing.last_updated
                if age.total_seconds() < 3600:  # Less than 1 hour old
                    logger.logMessage(f"[API] Using cached data for {ticker} (age: {age})")
                    return existing

        logger.logMessage(f"[API] Aggregating news for {ticker}")
        
        # Fetch headlines
        headlines = aggregate_headlines_smart(
            ticker.upper(),
            ticker_name or "",
            rate_cache=app_state.rate_cache
        )
        
        if headlines is None:
            logger.logMessage(f"[API] Rate limited for {ticker}")
            return None
        
        # Store individual articles in news_articles table
        stored_articles = await store_articles(ticker.upper(), headlines, db)
        
        # Compute sentiment
        sentiment_score = 0.0
        if headlines:
            from services.news_aggregator import compute_headlines_sentiment
            sentiment_score = compute_headlines_sentiment(headlines)
        
        # Count by source
        source_breakdown = {}
        for h in headlines:
            source_breakdown[h.source] = source_breakdown.get(h.source, 0) + 1
        
        # Create or update sentiment record
        sentiment_record = db.query(SymbolSentiment).filter(
            SymbolSentiment.symbol == ticker.upper()
        ).first()
        
        if sentiment_record:
            # Update existing
            sentiment_record.symbol_name = ticker_name
            sentiment_record.sentiment_score = sentiment_score
            sentiment_record.article_count = len(headlines)
            sentiment_record.source_breakdown = json.dumps(source_breakdown)
            sentiment_record.last_updated = datetime.utcnow()
        else:
            # Create new
            sentiment_record = SymbolSentiment(
                symbol=ticker.upper(),
                symbol_name=ticker_name,
                sentiment_score=sentiment_score,
                article_count=len(headlines),
                source_breakdown=json.dumps(source_breakdown),
                last_updated=datetime.utcnow()
            )
            db.add(sentiment_record)
        
        db.commit()
        db.refresh(sentiment_record)
        
        logger.logMessage(
            f"[API] Stored sentiment for {ticker}: "
            f"score={sentiment_score:.3f}, articles={len(headlines)}, stored={stored_articles}"
        )
        
        return sentiment_record
        
    except Exception as e:
        logger.logMessage(f"[API] Error processing {ticker}: {e}")
        db.rollback()
        return None


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
        cutoff_date = datetime.utcnow() - timedelta(days=30)
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
                    existing.fetched_at = datetime.utcnow()
                    continue
            
            # Create new article
            article = NewsArticle(
                symbol=symbol,
                source=h.source or "unknown",
                title=h.title or "",
                description=h.description,
                url=h.url,
                published_at=h.published_at,
                fetched_at=datetime.utcnow()
            )
            db.add(article)
            stored_count += 1
        
        db.commit()
        return stored_count
        
    except Exception as e:
        logger.logMessage(f"[DB] Error storing articles for {symbol}: {e}")
        db.rollback()
        return 0


async def process_all_tickers(db: Session):
    """Background task: Process all unique symbols from option_lifetimes table"""
    if app_state.is_processing:
        logger.logMessage("[Scheduler] Already processing, skipping...")
        return

    symbol_map = load_symbol_map(db)

    
    try:
        app_state.is_processing = True
        logger.logMessage("[Scheduler] Starting batch processing")
        
        # Get unique symbols from option_lifetimes table
        from sqlalchemy import distinct
        symbols = db.query(distinct(OptionLifetime.symbol)).all()
        unique_symbols = [s[0] for s in symbols if s[0]]
        total = len(unique_symbols)
        
        logger.logMessage(f"[Scheduler] Processing {total} unique symbols")
        
        processed = 0
        errors = 0
        
        for symbol in unique_symbols:
            try:
                ticker_name = symbol_map.get(symbol) 
                result = await aggregate_and_store_ticker(
                    symbol,
                    ticker_name,
                    db,
                    force_refresh=False
                )
                
                if result:
                    processed += 1
                else:
                    errors += 1
                
                # Small delay to avoid hammering APIs
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.logMessage(f"[Scheduler] Error processing {symbol}: {e}")
                errors += 1
        
        logger.logMessage(
            f"[Scheduler] Batch complete: {processed} processed, {errors} errors"
        )
        
    except Exception as e:
        logger.logMessage(f"[Scheduler] Batch processing error: {e}")
    finally:
        app_state.is_processing = False

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
                        # Convert to naive datetime for SQLite
                        timestamp = timestamp.replace(tzinfo=None)
                    except:
                        timestamp = datetime.utcnow()
                else:
                    timestamp = datetime.utcnow()
                
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
    await save_tickers_to_db(db=SessionLocal())
    
    app_state.scheduler.add_job(
        process_all_tickers,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="news_aggregation",
        name="Aggregate news for all tickers",
        replace_existing=True,
        kwargs={"db": SessionLocal()}
    )
    app_state.scheduler.add_job(
        save_tickers_to_db,
        trigger=IntervalTrigger(weeks=2),
        id="ticker_saver",
        name="Fetch and save US tickers to database",
        replace_existing=True,
        kwargs={"db": SessionLocal()}
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
                "SELECT 1 FROM tickers WHERE symbol = :symbol",
                {"symbol": symbol}
            ).fetchone()
            if not existing:
                db.execute(
                    "INSERT INTO tickers (symbol, name, timestamp) VALUES (:symbol, :name, :timestamp)",
                    {
                        "symbol": symbol,
                        "name": name,
                        "timestamp": datetime.utcnow().isoformat()
                    }
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
    if os.getenv("USE_TRANSFORMERS", "false").lower() == "true":
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


@app.post("/sentiment/{symbol}", response_model=SentimentResponse)
async def get_symbol_sentiment(
    symbol: str,
    force_refresh: bool = False,
    db: Session = Depends(get_db)
):
    """
    Get sentiment data for a specific symbol.
    
    - Checks database cache first (if not force_refresh)
    - Aggregates new data if needed
    - Stores and returns results
    """
    symbol = symbol.upper()
    
    # Get or create sentiment data
    sentiment = await aggregate_and_store_ticker(
        symbol, None, db, force_refresh
    )
    
    if sentiment is None:
        raise HTTPException(
            status_code=429,
            detail="Rate limited. Please try again later."
        )
    
    # Get recent articles for this symbol
    articles = db.query(NewsArticle).filter(
        NewsArticle.symbol == symbol
    ).order_by(NewsArticle.fetched_at.desc()).limit(50).all()
    
    articles_data = [
        {
            "source": a.source,
            "title": a.title,
            "description": a.description,
            "url": a.url,
            "published_at": a.published_at,
            "fetched_at": a.fetched_at.isoformat()
        }
        for a in articles
    ]
    
    return SentimentResponse(
        symbol=sentiment.symbol,
        symbol_name=sentiment.symbol_name,
        sentiment_score=sentiment.sentiment_score,
        article_count=sentiment.article_count,
        articles=articles_data,
        source_breakdown=json.loads(sentiment.source_breakdown) if sentiment.source_breakdown else {},
        last_updated=sentiment.last_updated,
        from_cache=not force_refresh
    )


@app.get("/sentiment/{symbol}/latest", response_model=SentimentResponse)
async def get_cached_sentiment(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    Get the most recent cached sentiment data for a symbol.
    Does NOT trigger new aggregation.
    """
    symbol = symbol.upper()
    
    sentiment = db.query(SymbolSentiment).filter(
        SymbolSentiment.symbol == symbol
    ).first()
    
    if not sentiment:
        raise HTTPException(
            status_code=404,
            detail=f"No sentiment data found for {symbol}"
        )
    
    # Get recent articles for this symbol
    articles = db.query(NewsArticle).filter(
        NewsArticle.symbol == symbol
    ).order_by(NewsArticle.fetched_at.desc()).limit(50).all()
    
    articles_data = [
        {
            "source": a.source,
            "title": a.title,
            "description": a.description,
            "url": a.url,
            "published_at": a.published_at,
            "fetched_at": a.fetched_at.isoformat()
        }
        for a in articles
    ]
    
    return SentimentResponse(
        symbol=sentiment.symbol,
        symbol_name=sentiment.symbol_name,
        sentiment_score=sentiment.sentiment_score,
        article_count=sentiment.article_count,
        articles=articles_data,
        source_breakdown=json.loads(sentiment.source_breakdown) if sentiment.source_breakdown else {},
        last_updated=sentiment.last_updated,
        from_cache=True
    )


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
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None
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
                "last_updated": r.last_updated.isoformat()
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
                "published_at": a.published_at,
                "fetched_at": a.fetched_at.isoformat()
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
    
    cutoff_date = datetime.utcnow() - timedelta(days=days)
    
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=9000,
        reload=True,
        log_level="info"
    )