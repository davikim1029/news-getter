# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Cross-Service API Reference

Before writing any code that calls another service, **read that service's `docs/API.md` first**. Each service maintains an API reference documenting every owned endpoint and every external call it makes.

| Service | Port | API Docs |
|---|---|---|
| option-file-server | 8000 | [../option-file-server/docs/API.md](../option-file-server/docs/API.md) |
| **news-getter** (this service) | 9000 | [docs/API.md](docs/API.md) |
| options-ai | 8100 | [../options-ai/docs/API.md](../options-ai/docs/API.md) |
| option-getter | 8001 | [../option-getter/docs/API.md](../option-getter/docs/API.md) |
| option-analysis | 9200 | [../option-analysis/docs/API.md](../option-analysis/docs/API.md) |
| process-monitor | 9100 | [../process-monitor/docs/API.md](../process-monitor/docs/API.md) |

---

## Service Overview

`news-getter` is a FastAPI news aggregator service (port 9000) that fetches and scores news sentiment per ticker using NLP models. It uses APScheduler to run background aggregation tasks and exposes a REST API for on-demand sentiment retrieval.

This service is part of a larger monorepo — see the parent `CLAUDE.md` at `../CLAUDE.md` for full architecture context.

## Setup & Running

```bash
cd news-getter
uv sync   # creates .venv, resolves deps from pyproject.toml
```

```bash
python main.py --mode start-server  # start uvicorn once
python main.py --mode monitor       # monitor loop (auto-restart on crash)
python main.py --mode stop          # stop server via PID
python main.py --mode check         # check if server is running
python main.py --mode stats         # interactive SQL query runner (analytics/sql/*.sql)
python main.py --mode logs          # tail last N lines of log
python main.py                      # interactive menu
```

Process files:
- PID: `news_server.pid`
- Monitor PID: `news_monitor.pid`
- Stop flag: `news_server.stop`
- Log: `news_server.log`

## Architecture

### Entry Points

- `news_server.py` — FastAPI app (`news_server:app`), uvicorn target. Manages APScheduler lifecycle via `@asynccontextmanager lifespan`.
- `main.py` — CLI wrapper that spawns uvicorn as a subprocess and implements the monitor loop.
- `api.py` — REST API route definitions.

### Services

- `services/news_aggregator.py` — `aggregate_headlines_smart()`: fetches RSS/API headlines, deduplicates, and scores sentiment.
- `services/processor.py` — batch processing of headline sentiment.
- `services/core/cache_manager.py` — `RateLimitCache` and `HeadlineCache` for deduplication and rate-limit management.
- `services/core/deps.py` — FastAPI dependency injection helpers.

### Database

SQLAlchemy-managed SQLite database in `database/`. Models in `models/models.py` include:
- `SymbolSentiment` — aggregated sentiment score per ticker
- `NewsArticle` — individual article records
- `TickerSentiment` — per-ticker sentiment timeseries

### Background Scheduling

APScheduler (`AsyncIOScheduler`) runs periodic sentiment aggregation. Scheduler config is exposed via the REST API (`SchedulerConfig` model).

## Key Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `GET /sentiment/{ticker}` | GET | Per-ticker sentiment score |
| `GET /sentiment/aggregate` | GET | Aggregate sentiment across tickers. Optional `?tickers=AAPL,MSFT` to scope to specific tickers. Returns `{scope, sentiment_score, ticker_count, computed_at}`. No DB writes — pure aggregation from `SymbolSentiment`. Used by `option-getter` to prefetch market-wide and sector-level sentiment once per scan cycle. |

## Key Files

| File | Purpose |
|---|---|
| `news_server.py` | FastAPI app with APScheduler lifecycle + `GET /sentiment/aggregate` endpoint |
| `api.py` | REST API endpoints |
| `services/news_aggregator.py` | Headline fetch + sentiment scoring |
| `services/core/cache_manager.py` | Rate-limit + headline deduplication cache |
| `database/database.py` | SQLAlchemy engine, session, and `init_database()` |
| `models/models.py` | Pydantic/SQLAlchemy data models |

## Environment Variables

Loaded from `.env` at startup via `python-dotenv`. API keys for news sources should be set there.

## Git Commit Policy

**Never create a git commit without explicit user confirmation.** Before every commit:
1. Show the proposed file list and commit message
2. Ask the user to confirm and provide or approve the message
3. Only proceed after explicit approval

This applies across all repos in this monorepo.