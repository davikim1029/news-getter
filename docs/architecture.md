# Architecture — news-getter (port 9000)

## Runtime shape

`main.py` is the process-control CLI. It starts `uvicorn news_server:app --host 0.0.0.0 --port 9000`, manages PID/stop files, optionally monitors and restarts the server, and exposes check/stats/log modes. `news_server.py` owns the live FastAPI application, lifespan, scheduler, and routes.

At startup the lifespan loads `.env`, applies news schema migrations to the news-owned SQLite database, optionally imports legacy news rows from `option-file-server/database/options.db` when the new DB is empty, optionally loads FinBERT, synchronously refreshes the ticker-name table through `shared-options`/Finnhub, and starts an in-process `AsyncIOScheduler`.

## Processing flow

1. The hourly aggregation job reads distinct symbols from `option_lifetimes`, a table owned/populated by `option-file-server`, through a read-only SQLite connection.
2. Symbols run in batches of 20. A semaphore limits the complete per-ticker operation to four concurrent tasks.
3. `aggregate_and_store_ticker()` accepts a DB cache record younger than one hour unless forced. On a miss, it fetches NewsAPI, NewsData, and Google News RSS in a worker thread.
4. Articles older than one day for that symbol are deleted. New URLs are inserted and duplicate URLs have `fetched_at` refreshed.
5. Sentiment is computed by optional FinBERT (`USE_TRANSFORMERS=true`) or a keyword/source/recency heuristic. `symbol_sentiment` is then upserted.
6. API consumers read ticker, filtered aggregate, article, and operational views from the news-owned database; `/symbols` and `total_option_symbols` still read the upstream options DB read-only.

Additional jobs refresh ticker metadata every two weeks and evict expired rate-limit cache entries every 30 minutes. Scheduler state and configuration live only in the server process.

## State and storage

| State | Location and ownership | Assumptions |
|---|---|---|
| News SQLite data | `database/news.db` by default; override with `NEWS_DB_PATH` | `news-getter` is the only writer for `news_articles`, `symbol_sentiment`, `tickers`, and `news_migrations`. Startup can copy legacy rows from the old shared DB once when the news DB is empty; disable with `NEWS_IMPORT_LEGACY_ON_INIT=false`. |
| Upstream options symbols | `../option-file-server/database/options.db` by default; override with `NEWS_OPTIONS_DB_PATH` | Opened read-only for `option_lifetimes` symbol discovery and stats. News writes no longer share `options.db`'s SQLite writer lock. |
| Rate-limit cache | `cache/ratelimit_sentiment.json` plus memory; one-day TTL | Loaded at module import and written on registered shutdown; scheduled eviction changes memory but is not itself an autosave loop. |
| Headline cache | `cache/headlines.json` plus memory | Instantiated in `AppState`, but the live aggregation path does not consult it; DB URL uniqueness performs effective deduplication. |
| Scheduler/processing flag/model | Process memory | Lost on restart and not coordinated between multiple Uvicorn workers/instances. The deployment assumes one application process. |
| Process control | `news_server.pid`, `news_monitor.pid`, `news_server.stop`, `logs/monitor/log.log` | Paths are relative to the working directory; monitor logs rotate daily with seven-day retention. |

## Integration boundaries

- Upstream: `option-file-server` supplies scan symbols through the shared SQLite `option_lifetimes` table; NewsAPI, NewsData, Google News, and Finnhub supply network data; Hugging Face supplies optional FinBERT artifacts; `shared-options` supplies logging, monitor state, shutdown registration, ticker retrieval, and shared models.
- Downstream: `option-getter` calls `GET /sentiment/aggregate` for market and sector context and may call the per-symbol scoring endpoint. Other internal clients can consume the unauthenticated REST API.
- Coupling: there is still a direct local read boundary from `news-getter` to `option-file-server` for `option_lifetimes` symbol discovery, but news retention/migrations no longer write to `options.db`.

## Failure behavior and known debt

- Individual news-provider errors generally degrade to fewer/no headlines; provider 429 state is cached. Optional transformer failure permanently falls back to heuristic scoring for that process.
- Provider HTTP/RSS calls use explicit connect/read timeouts (`NEWS_HTTP_CONNECT_TIMEOUT_SECONDS`, default 3s; `NEWS_HTTP_READ_TIMEOUT_SECONDS`, default 10s). A single stalled provider should degrade a ticker result instead of indefinitely occupying one of the four aggregation slots.
- Scheduler jobs run in-process. Multiple server instances would duplicate ticker refresh and aggregation, while the boolean processing guard is not distributed.
- `api.py` and `services/processor.py` duplicate older API/processing logic but are not wired into the application, increasing drift risk.
- PID files, cache files, migration backups, and logs depend on the launch working directory; database paths are now resolved from the module path unless overridden.
- Administrative writes, destructive cleanup, arbitrary server-local migration paths, and scheduler controls require `X-API-Key` (`NEWS_ADMIN_API_KEY`, falling back to `ANALYSIS_API_KEY` for compatibility). `APP_ENV=production` refuses startup unless one of those keys is configured.
