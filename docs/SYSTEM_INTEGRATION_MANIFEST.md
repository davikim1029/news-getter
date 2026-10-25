# System Integration Manifest — news-getter

Verified against the repository implementation on 2026-07-14. This manifest summarizes integration contracts and risks; it complements rather than replaces `API.md` (endpoint contract) and `architecture.md` (internal runtime/data flow).

## Exposed APIs, entry points, and public interfaces

The deployed application is `news_server:app`, served on `0.0.0.0:9000` by `main.py`. The CLI supports `--mode start-server|monitor|stop|check|stats|logs`, with an interactive menu when no mode is supplied. Process integration also includes PID files (`news_server.pid`, `news_monitor.pid`), stop flag (`news_server.stop`), monitor log (`logs/monitor/log.log`), FastAPI OpenAPI (`/openapi.json`), and Swagger UI (`/docs`).

Live REST routes are: `GET /` health; `POST /sentiment/{symbol}/score`; `GET /sentiment/aggregate`; `GET /stats`; `GET /symbols`; `GET /articles/{symbol}`; `DELETE /articles/cleanup`; and scheduler trigger/status/configure routes. See `API.md` for inputs and known route defects. Administrative/destructive routes require `X-API-Key`; read routes remain public on the trusted service network.

Python-level interfaces intentionally consumed inside the service include `aggregate_headlines_smart()`, `compute_headlines_sentiment()`, `SessionLocal`/`get_db()`, `RateLimitCache`, and `shared-options` logging/monitor/key-check/PID-check/ticker APIs.

## Critical external dependencies and project linkages

- **option-file-server (upstream, structural):** `news-getter` directly opens `../option-file-server/database/options.db` read-only for distinct symbols from `option_lifetimes`. News-owned tables now live in `news-getter/database/news.db` by default, removing the second writer from `options.db` while retaining a local high-throughput symbol-discovery boundary.
- **option-getter (downstream):** consumes aggregate sentiment for market-wide and sector-level scan context; per-symbol scoring is also available.
- **options-shared (build/runtime):** editable path dependency `../options-shared`; owns logging, monitor status, key and PID checks, and Finnhub ticker retrieval.
- **News providers (upstream):** NewsAPI (`NEWSAPI_KEY`), NewsData (`NEWSDATA_KEY`), and unauthenticated Google News RSS provide headlines.
- **Model provider/runtime:** Transformers, PyTorch CPU, and optional Hugging Face `ProsusAI/finbert` artifacts are used when `USE_TRANSFORMERS=true`; otherwise local heuristic scoring applies.
- **Framework/runtime:** FastAPI/Uvicorn, SQLAlchemy, APScheduler, feedparser, requests (transitive/currently imported), python-dotenv (transitive/currently imported), SQLite, and psutil (currently imported by the CLI) are operational dependencies. Some imported runtime packages are not declared directly in `pyproject.toml`, relying on transitive/shared dependency resolution.

## Structural risks, security bottlenecks, and architectural debt

1. Read endpoints are unauthenticated while the server binds every interface; administrative routes are API-key protected and production refuses startup without a configured admin key.
2. The symbol-discovery read still depends on a local `options.db` path and `option_lifetimes` schema. News persistence/migrations no longer share the file-server writer lock.
3. Provider requests/feed parsing now use bounded connect/read timeouts, but provider-wide retries and circuit-breaker cooldowns remain intentionally minimal. Repeated upstream degradation can still consume aggregation capacity until the rate-limit cache or scheduler cadence backs off.
4. APScheduler and the processing guard are process-local. Multiple workers/replicas duplicate jobs and bypass mutual exclusion; scheduler configuration disappears on restart.
5. APScheduler configuration remains process-local and non-persistent. Runtime reconfiguration works for the current process only and is rebuilt from environment/defaults on restart.
6. `news_server.py` itself contains route, scheduling, persistence, and business logic. This encourages documentation and implementation drift.
7. Relative cache/PID/log paths require a particular working directory (run from the news-getter folder); database paths resolve from the module path.

## State management and data storage assumptions

The canonical news store is now `news-getter/database/news.db` (or `NEWS_DB_PATH`). `news-getter` owns migrations for `news_articles`, `symbol_sentiment`, `tickers`, `sentiment_requests` (v2), and `news_migrations`; it assumes `option_lifetimes` exists in the upstream options DB and remains readable. On first startup after cutover, legacy news rows are copied from `options.db` only if the new news tables are empty.

Ticker sentiment is considered fresh for three hours, and `/sentiment/aggregate` counts only scores refreshed in the last 24 hours. Per-symbol articles older than one day are deleted during refresh; the cleanup endpoint can apply another caller-selected retention. The ticker-name table refreshes at startup and every two weeks. Aggregation defaults to every 60 minutes via `AGGREGATION_INTERVAL_MINUTES`.

The rate-limit cache lives in memory and in `cache/ratelimit_sentiment.json`, which is loaded at startup and rewritten on every new back-off, so back-offs survive a restart. The failed-fetch memory (30-minute fail-fast per ticker), scheduler jobs, `is_processing`, the DB semaphore, HTTP session, and optional model pipeline are process-local. The design assumes one service instance, a stable repository-relative directory layout, writable local disk, and trusted network access.

## Documentation disposition

- `API.md` remains necessary as the detailed consumer contract and external-call inventory.
- `architecture.md` remains necessary for maintainers because it explains lifecycle, flow, state placement, and failure behavior.
- This manifest is the cross-service review and risk register. It consolidates integration boundaries but should not replace either focused document.
