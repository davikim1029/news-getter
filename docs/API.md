# API Reference — news-getter (port 9000)

Implementation source of truth: `news_server.py` (`news_server:app`). FastAPI also exposes generated OpenAPI at `/openapi.json` and Swagger UI at `/docs` while the service is running.

**Authentication:** read endpoints remain public on the trusted service network. Administrative and destructive operations require `X-API-Key` matching `NEWS_ADMIN_API_KEY` (or `ANALYSIS_API_KEY` as a compatibility fallback). Production mode (`APP_ENV=production`) refuses to start unless one of those keys is configured.

**Storage:** news-owned tables live in `database/news.db` by default (`NEWS_DB_PATH` override). `/symbols` and the option-symbol count in `/stats` read `option-file-server/database/options.db` through a read-only connection (`NEWS_OPTIONS_DB_PATH` override). Startup can copy legacy news rows from the old shared DB when the new DB is empty; disable with `NEWS_IMPORT_LEGACY_ON_INIT=false`.

## Owned endpoints

| Method and path | Inputs | Response / behavior |
|---|---|---|
| `GET /` | None | Health summary: `status`, `service`, `scheduler_running`, `is_processing`. |
| `POST /sentiment/{symbol}/score` | Path `symbol`; query `include_articles=false`, `force_refresh=false` | Returns `SentimentResponse`. Uses a DB record less than one hour old unless refresh is forced (`from_cache: true`); otherwise fetches news, stores articles, computes sentiment, and upserts the symbol aggregate (`from_cache: false`). A provider that was searched and found nothing yields a stored 0.0 with `article_count: 0`. When no provider could be searched, answers 429 if any is rate-limiting us and 502 otherwise, leaves the stored score unchanged, and answers that same error for the ticker for 30 minutes unless `force_refresh=true`. |
| `GET /sentiment/aggregate` | Optional comma-separated query `tickers` | Average of available `symbol_sentiment.sentiment_score` records. Returns `scope`, `sentiment_score`, `ticker_count`, `computed_at`; performs no write. |
| `GET /stats` | None | Counts, average sentiment, and ten most recently updated symbols. |
| `GET /symbols` | Query `skip=0`, `limit=100` | Page of distinct symbols read from the upstream `option_lifetimes` table via read-only SQLite. |
| `GET /articles/{symbol}` | Query `limit=50` | Most recently fetched stored articles; 404 when none exist. |
| `DELETE /articles/cleanup` | Header `X-API-Key`; query `days=1` | Deletes `news_articles` older than the cutoff and commits immediately. |
| `POST /scheduler/trigger` | Header `X-API-Key` | Enqueues a full ticker sentiment run if one is not already active. |
| `GET /scheduler/status` | None | Scheduler running state, processing flag, and jobs with next-run timestamps. |
| `POST /scheduler/configure` | Header `X-API-Key`; JSON `{"interval_minutes":60,"enabled":true}`; interval 5–1440 | Reschedules or removes the in-process aggregation job. Configuration is not persisted across restart. |

`SentimentResponse` contains `symbol`, nullable `symbol_name`, `sentiment_score`, `article_count`, `articles`, `source_breakdown`, `last_updated`, and `from_cache`.

## External calls made

| Target | Interface | Configuration | Purpose |
|---|---|---|---|
| NewsAPI | `GET https://newsapi.org/v2/everything` | `NEWSAPI_KEY`; `NEWS_HTTP_CONNECT_TIMEOUT_SECONDS`; `NEWS_HTTP_READ_TIMEOUT_SECONDS`; optional `NEWS_HTTP_USER_AGENT` | Up to 50 English articles from configured financial sources. |
| NewsData | `GET https://newsdata.io/api/1/news` | `NEWSDATA_KEY`; shared `NEWS_HTTP_*` timeout/user-agent settings | Business and technology articles. |
| Google News | RSS search URL under `news.google.com` | shared `NEWS_HTTP_*` timeout/user-agent settings | Fallback/additional headlines. |
| Finnhub, through editable `shared-options` | `fetch_us_tickers_from_finnhub(None)` | Credentials are managed by the shared library/environment | Refresh the local `tickers` table at startup and every two weeks. |
| Hugging Face model hub | model `ProsusAI/finbert` | `USE_TRANSFORMERS=true` | Downloads/loads optional transformer artifacts; otherwise keyword scoring is used. |

HTTP clients use bounded request timeouts by default: 3 seconds to connect and 10 seconds to read. Override with `NEWS_HTTP_CONNECT_TIMEOUT_SECONDS` and `NEWS_HTTP_READ_TIMEOUT_SECONDS` when provider/network conditions require it. Consult each provider’s terms and quota behavior before changing polling volume.
