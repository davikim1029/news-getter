from typing import Optional, List, Dict, Any
from datetime import datetime
from datetime import datetime
import asyncio
import json
from sqlalchemy.orm import Session
from shared_options.log.logger_singleton import getLogger
from fastapi import Depends
from services.core.deps import get_app_state
from models.models import SymbolSentiment, NewsArticle, TickerSentiment, OptionLifetime
import json

# Import your existing news aggregator
from services.news_aggregator import (
    get_sentiment_signal,
    aggregate_headlines_smart,
    Headline
)
from services.core.cache_manager import RateLimitCache, HeadlineCache
from shared_options.log.logger_singleton import getLogger

logger = getLogger()


# ===========================
# Core Business Logic
# ===========================
async def aggregate_and_store_ticker(
    ticker: str,
    ticker_name: Optional[str],
    db: Session,
    force_refresh: bool = False,
    app_state = Depends(get_app_state)
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


async def process_all_tickers(db: Session, app_state = Depends(get_app_state)):
    """Background task: Process all unique symbols from option_lifetimes table"""
    if app_state.is_processing:
        logger.logMessage("[Scheduler] Already processing, skipping...")
        return
    
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
                result = await aggregate_and_store_ticker(
                    symbol,
                    None,  # We don't have company names in option_lifetimes
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

