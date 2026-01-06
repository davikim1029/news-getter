from fastapi import HTTPException, BackgroundTasks, Depends
from datetime import datetime
from datetime import datetime
import json
from sqlalchemy.orm import Session
from services.processor import aggregate_and_store_ticker, process_all_tickers, migrate_json_to_db 
from database.database import SessionLocal, get_db
from apscheduler.triggers.interval import IntervalTrigger
import json

from models.models import (
    SentimentResponse,
    MigrationRequest,
    SymbolSentiment,
    SchedulerConfig,
    OptionLifetime,
    NewsArticle)

# app/api/sentiment.py
from fastapi import APIRouter, Depends
from services.core.deps import get_app_state

router = APIRouter(prefix="/api", tags=["api"])

# ===========================
# API Endpoints
# ===========================

@router.get("/")
async def root(app_state = Depends(get_app_state)):
    """Health check endpoint"""
    return {
        "status": "running",
        "service": "News Aggregator API",
        "scheduler_running": app_state.scheduler.running if app_state.scheduler else False,
        "is_processing": app_state.is_processing
    }


@router.post("/sentiment/{symbol}", response_model=SentimentResponse)
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


@router.get("/sentiment/{symbol}/latest", response_model=SentimentResponse)
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


@router.post("/migrate")
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


@router.post("/scheduler/trigger")
async def trigger_batch_processing(
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    app_state = Depends(get_app_state)
):
    """Manually trigger batch processing of all tickers"""
    if app_state.is_processing:
        return {"status": "already_running", "message": "Batch processing already in progress"}
    
    background_tasks.add_task(process_all_tickers, db)
    return {"status": "triggered", "message": "Batch processing started"}


@router.get("/scheduler/status")
async def get_scheduler_status(app_state = Depends(get_app_state)):
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


@router.post("/scheduler/configure")
async def configure_scheduler(config: SchedulerConfig, app_state = Depends(get_app_state)):
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


@router.get("/stats")
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


@router.get("/symbols")
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


@router.get("/articles/{symbol}")
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


@router.delete("/articles/cleanup")
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