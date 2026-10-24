"""The news-getter web service: keeps a news sentiment score for every ticker the scanner trades.

Every hour (AGGREGATION_INTERVAL_MINUTES), a background job takes each ticker that has
option history in option-file-server's database, fetches its recent headlines, stores
them, and saves a score from -1 (bad news) to 1 (good news). The scanner (option-getter)
reads these scores through GET /sentiment/aggregate, for market and sector mood, and
POST /sentiment/{symbol}/score, for one ticker. Other routes inspect stored data or
control the schedule.

uvicorn runs this as `news_server:app`; main.py starts it. Routes that change or delete
data require an X-API-Key header; read routes are open to the private network.
"""

from path_bootstrap import ensure_shared_options_path

ensure_shared_options_path()

import asyncio
import json
import os
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dateutil.parser import parse as parse_datetime
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from database.database import (
    SessionLocal,
    count_option_symbols,
    get_db,
    init_database,
    list_option_symbols,
)
from models.models import (
    AppState,
    NewsArticle,
    SchedulerConfig,
    SentimentResponse,
    SymbolSentiment,
    sentiment_to_dict,
)
from services.news_aggregator import (
    AggregatorException,
    Headline,
    RateLimitedException,
    aggregate_headlines_smart,
    compute_headlines_sentiment,
)
from shared_options.log.logger import LogType
from shared_options.log.logger_singleton import getLogger
from shared_options.services.key_auth import (
    accepted_secrets,
    is_one_of,
    register_env_path,
)
from shared_options.services.monitor_status import MonitorStatus

logger = getLogger()

app_state = AppState()

# An all-tickers run starts 20 tickers at a time, but at most 4 fetch-and-store at once,
# which bounds both concurrent provider requests and concurrent SQLite writers.
BATCH_SIZE = 20
DB_CONCURRENCY_LIMIT = 4
db_semaphore = asyncio.Semaphore(DB_CONCURRENCY_LIMIT)


def _is_production() -> bool:
    return os.getenv("APP_ENV", os.getenv("ENV", "development")).strip().lower() in {
        "prod",
        "production",
    }


def _accepted_admin_keys() -> list[str]:
    """API keys that unlock admin routes: NEWS_ADMIN_API_KEY, else ANALYSIS_API_KEY (each with its rotation predecessor)."""
    keys = accepted_secrets("NEWS_ADMIN_API_KEY")
    if not keys:
        keys = accepted_secrets("ANALYSIS_API_KEY")
    return keys


def _require_admin_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """FastAPI dependency: reject the request with 401 unless X-API-Key is an accepted key."""
    accepted = _accepted_admin_keys()
    if not accepted:
        # No key configured: development mode, so admin routes stay open. Production
        # can't reach this, because startup refuses to run without a key.
        return
    if not is_one_of(x_api_key, accepted):
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


def _require_production_auth_config() -> None:
    """Refuse to start in production (APP_ENV=production) when no admin key is configured."""
    if _is_production() and not _accepted_admin_keys():
        raise RuntimeError(
            "Production news-getter requires NEWS_ADMIN_API_KEY or ANALYSIS_API_KEY"
        )


@contextmanager
def db_session():
    """A news.db session for code outside a request (the scheduler); always closed afterwards."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _published_iso(value) -> str | None:
    """A publish date, in whatever format the provider used, as an ISO string (None if absent)."""
    if isinstance(value, datetime):
        return value.isoformat()
    return parse_datetime(value).isoformat() if value else None


def _article_to_dict(article: NewsArticle, include_id: bool = False) -> dict:
    """A stored article as the JSON shape the API returns."""
    row = {
        "source": article.source,
        "title": article.title,
        "description": article.description,
        "url": article.url,
        "published_at": _published_iso(article.published_at),
        "fetched_at": article.fetched_at.isoformat() if article.fetched_at else None,
    }
    return {"id": article.id, **row} if include_id else row


def load_symbol_map(db: Session) -> Dict[str, str]:
    """Every known ticker mapped to its company name, from the `tickers` table."""
    result = db.execute(
        text("SELECT symbol, name FROM tickers")
    )
    return {row.symbol: row.name for row in result}


def _symbol_name(db: Session, symbol: str) -> str | None:
    """The company name for one ticker, or None if it isn't in the `tickers` table."""
    return db.execute(
        text("SELECT name FROM tickers WHERE symbol = :symbol"), {"symbol": symbol}
    ).scalar()


# ===========================
# Core Business Logic
# ===========================
async def aggregate_and_store_ticker(
    ticker: str,
    ticker_name: str | None,
    force_refresh: bool = False,
) -> dict:
    """
    Return a ticker's sentiment, recomputing it from fresh headlines when needed.

    A score less than an hour old is returned as is, unless `force_refresh`. Otherwise:
    fetch headlines, store the new ones, score them, and save the score. Returns a dict
    shaped like SentimentResponse. Raises RateLimitedException or AggregatorException
    when no headlines could be fetched at all.

    Providers are searched by `ticker_name` (the company name) when one is known; a bare
    ticker such as "ON" or "ALL" would also match ordinary words.
    """
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
                            f"[API] Cache hit for {ticker} (age={age.total_seconds():.0f}s)",
                            log_type=LogType.Server,
                        )

                        articles = [
                            _article_to_dict(a)
                            for a in (
                                db.query(NewsArticle)
                                .filter(NewsArticle.symbol == ticker)
                                .order_by(NewsArticle.fetched_at.desc())
                                .limit(50)
                                .all()
                            )
                        ]

                        return sentiment_to_dict(existing, articles=articles, from_cache=True)

        logger.logMessage(f"[API] Aggregating news for {ticker}", log_type=LogType.Server)

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

            # Find which incoming URLs are already stored, in one query instead of one per
        # headline. (Matches across all tickers, not just this one: NEWSGET-01.)
            incoming_urls = {h.url for h in headlines if h.url}
            existing_urls: set[str] = set()
            if incoming_urls:
                existing_urls = {
                    row.url
                    for row in db.query(NewsArticle.url)
                    .filter(NewsArticle.url.in_(incoming_urls))
                    .all()
                }
                # Seen again today: refresh fetched_at so the 1-day purge above keeps them.
                stale_urls = incoming_urls & existing_urls
                if stale_urls:
                    db.query(NewsArticle).filter(
                        NewsArticle.url.in_(stale_urls)
                    ).update({"fetched_at": now}, synchronize_session=False)

            # Insert only articles we haven't stored, and collect the same rows for the response.
            articles_data = []
            for h in headlines:
                if h.url and h.url in existing_urls:
                    continue

                db.add(NewsArticle(
                    symbol=ticker,
                    source=h.source or "unknown",
                    title=h.title or "",
                    description=h.description,
                    url=h.url,
                    published_at=h.published_at if h.published_at else None,
                    fetched_at=now,
                ))

                articles_data.append(
                    {
                        "source": h.source or "unknown",
                        "title": h.title or "",
                        "description": h.description,
                        "url": h.url,
                        "published_at": _published_iso(h.published_at),
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
        # 5. Source breakdown: headline count per provider
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
            f"articles={len(headlines)}",
            log_type=LogType.Server,
        )

        return sentiment_to_dict(record, articles=articles_data, from_cache=False)

    except RateLimitedException:
        logger.logMessage(f"[API] Rate-limited for {ticker}", log_type=LogType.Server)
        raise
    except AggregatorException:
        logger.logMessage(f"[API] Aggregation failed for {ticker}", log_type=LogType.Server)
        raise
    except Exception as e:
        logger.logMessage(f"[API] Fatal error for {ticker}: {e}", log_type=LogType.Server)
        raise


async def process_all_tickers():
    """
    Refresh the sentiment of every ticker with option history. The hourly scheduled job.

    Runs in batches of BATCH_SIZE and publishes progress to MonitorStatus, which
    process-monitor and the iOS app display. Skips if a run is already in progress.
    One ticker's failure doesn't stop the run.
    """
    if app_state.is_processing:
        logger.logMessage("[Scheduler] Already processing, skipping...")
        return

    with db_session() as db:
        symbol_map = load_symbol_map(db)

    unique_symbols = list_option_symbols()

    if not unique_symbols:
        logger.logMessage("[Scheduler] No symbols to process")
        return

    ms = MonitorStatus.instance()
    total = len(unique_symbols)
    num_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    # Read the running totals and bump the cycle count in one locked step.
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

            # return_exceptions: a failed ticker becomes a result value instead of
            # cancelling the rest of the batch. Its error was already logged.
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


async def process_single_ticker(symbol: str, ticker_name: Optional[str]):
    """Refresh one ticker, waiting for one of the DB_CONCURRENCY_LIMIT slots first."""
    async with db_semaphore:
        return await aggregate_and_store_ticker(
            ticker=symbol,
            ticker_name=ticker_name,
        )


# ===========================
# Scheduler Setup
# ===========================
async def job_wrapper(job_func, **kwargs):
    """Run a scheduled job (async or not) and log any error instead of letting it escape."""
    try:
        if asyncio.iscoroutinefunction(job_func):
            await job_func(**kwargs)
        else:
            await asyncio.to_thread(job_func, **kwargs)
    except Exception as e:
        logger.logMessage(f"[Scheduler] Error in job {job_func.__name__}: {e}")


async def _evict_caches():
    """Scheduled every 30 minutes: drop rate-limit back-offs that have expired."""
    if app_state.rate_cache:
        n = app_state.rate_cache.evict_expired()
        if n:
            logger.logMessage(f"[Cache] Evicted {n} expired rate-limit entries")


async def save_tickers_to_db(db: Session = None):
    """Add newly listed US tickers (symbol and company name) from Finnhub to the `tickers` table.

    Existing rows are left alone, so a company that changes its name keeps the old one.
    Closes `db` when done. Errors are logged, not raised.
    """

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


async def start_scheduler():
    """Load ticker names once, then start the three repeating jobs: sentiment, ticker names, cache eviction."""
    app_state.scheduler = AsyncIOScheduler()

    # Default: run every 60 minutes
    interval_minutes = int(os.getenv("AGGREGATION_INTERVAL_MINUTES", "60"))

    # Run once at startup
    logger.logMessage("[Scheduler] Running initial ticker save at startup...")
    await save_tickers_to_db(db=SessionLocal())
    logger.logMessage("[Scheduler] Initial ticker save complete.")
    app_state.scheduler.add_job(
        job_wrapper,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id="news_aggregation",
        name="Aggregate news for all tickers",
        replace_existing=True,
        kwargs={"job_func": process_all_tickers}
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


async def stop_scheduler():
    """Stop the scheduler (called on shutdown)."""
    if app_state.scheduler:
        app_state.scheduler.shutdown()
        logger.logMessage("[Scheduler] Stopped")


# ===========================
# Lifespan Management
# ===========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Everything that happens when the server starts, and again when it stops (after `yield`)."""
    # Startup
    from dotenv import load_dotenv
    load_dotenv(override=True)  # reads .env into os.environ
    # Point the key checks at the .env file itself, so a rotated API key takes effect
    # without a restart.
    register_env_path(os.path.join(os.path.dirname(__file__), ".env"))
    _require_production_auth_config()

    ms = MonitorStatus.instance("news-getter")
    ms.set_step("Starting up")
    logger.logMessage("[App] Starting up...")

    # Initialize database (create tables if needed)
    init_database()

    # Load FinBERT now rather than on first use, which would otherwise stall for the
    # model download and load.
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
    """Health check: is the server up, is the scheduler running, is a run in progress."""
    return {
        "status": "running",
        "service": "News Aggregator API",
        "scheduler_running": app_state.scheduler.running if app_state.scheduler else False,
        "is_processing": app_state.is_processing
    }


@app.post("/sentiment/{symbol}/score", response_model=SentimentResponse)
async def get_sentiment_score(
    symbol: str,
    include_articles: bool = Query(False, description="Include full article info"),
    force_refresh: bool = False,
    db: Session = Depends(get_db)
):
    """
    Sentiment for one ticker, refreshed from the news if the stored score is over an hour old.

    Articles are included only when `include_articles=true` (up to 50). Upstream rate
    limiting answers 429, and every provider failing answers 502.
    """
    symbol = symbol.upper()

    try:
        sentiment_data = await aggregate_and_store_ticker(
            ticker=symbol,
            ticker_name=_symbol_name(db, symbol),
            force_refresh=force_refresh
        )

    except RateLimitedException as e:
        raise HTTPException(status_code=429, detail=str(e)) from e
    except AggregatorException as e:
        raise HTTPException(status_code=502, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}") from e

    # Ensure the articles key exists
    if "articles" not in sentiment_data:
        sentiment_data["articles"] = []

    # Overwrites the accurate value set above, so a cache miss reports true (NEWSGET-01).
    sentiment_data["from_cache"] = not force_refresh

    if not include_articles:
        sentiment_data["articles"] = []

    # Construct Pydantic response
    response = SentimentResponse(**sentiment_data)

    logger.logMessage(f"[API] Sentiment response for {symbol}: {response}", log_type=LogType.Server)
    return response


@app.post("/scheduler/trigger")
async def trigger_batch_processing(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_admin_key),
):
    """Start an all-tickers refresh now, in the background (unless one is already running)."""
    if app_state.is_processing:
        return {"status": "already_running", "message": "Batch processing already in progress"}

    background_tasks.add_task(process_all_tickers)
    return {"status": "triggered", "message": "Batch processing started"}


@app.get("/scheduler/status")
async def get_scheduler_status():
    """Whether the scheduler is running, and each job with its next run time."""
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
async def configure_scheduler(
    config: SchedulerConfig,
    _: None = Depends(_require_admin_key),
):
    """Change how often the all-tickers refresh runs, or turn it off. Not kept across restarts."""
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
            )
        message = f"Scheduler configured: {config.interval_minutes}min interval"
    else:
        if job:
            app_state.scheduler.remove_job("news_aggregation")
        message = "Scheduler disabled"

    return {"status": "success", "message": message, "config": config}


@app.get("/stats")
async def get_statistics(db: Session = Depends(get_db)):
    """Counts and averages over the stored data, plus the ten most recently scored tickers."""
    total_symbols = db.query(func.count(SymbolSentiment.id)).scalar()
    avg_sentiment = db.query(func.avg(SymbolSentiment.sentiment_score)).scalar()
    total_articles = db.query(func.count(NewsArticle.id)).scalar()

    recent_updates = db.query(SymbolSentiment).order_by(
        SymbolSentiment.last_updated.desc()
    ).limit(10).all()

    total_option_symbols = count_option_symbols()

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
    """One page of the tickers that have option history in option-file-server's database."""
    symbol_list = list_option_symbols(skip=skip, limit=limit)

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
    """The most recently fetched stored articles for one ticker; 404 if there are none."""
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
        "articles": [_article_to_dict(a, include_id=True) for a in articles]
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
    db: Session = Depends(get_db),
    _: None = Depends(_require_admin_key),
):
    """Delete stored articles, for every ticker, fetched more than `days` days ago."""
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "news_server:app",
        host="0.0.0.0",
        port=9000,
        reload=True,
        log_level="info"
    )
