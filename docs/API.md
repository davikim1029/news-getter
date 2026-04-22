# API Reference — news-getter (port 9000)

> Before calling any endpoint here, verify the path against this file.
> Auth: No authentication required on any endpoint (internal network only).

---

## Owned Endpoints

### GET /
- **File:** `news_server.py:602`
- **Auth:** None
- **Purpose:** Health check and scheduler status.
- **Input:** None
- **Example output:**
  ```json
  {
    "status": "running",
    "service": "News Aggregator API",
    "scheduler_running": true,
    "is_processing": false
  }
  ```

---

### POST /sentiment/{symbol}/score
- **File:** `news_server.py:614`
- **Auth:** None
- **Purpose:** Primary sentiment endpoint. Fetches/scores news for a ticker. Returns cached data unless `force_refresh=true`. Used by option-getter per-ticker during scan.
- **Input:** Path param `symbol` (ticker). Query params: `include_articles=false`, `force_refresh=false`
- **Example output (`SentimentResponse`):**
  ```json
  {
    "symbol": "AAPL",
    "sentiment_score": 0.62,
    "article_count": 14,
    "from_cache": true,
    "computed_at": "2026-04-21T13:45:00Z",
    "articles": []
  }
  ```

---

### GET /sentiment/aggregate
- **File:** `news_server.py:858`
- **Auth:** None
- **Purpose:** Aggregate sentiment across all tracked tickers, or scoped to a subset. **Primary endpoint used by option-getter** for market-wide and sector-level prefetch at the start of each scan cycle. No DB writes — pure in-memory aggregation from `SymbolSentiment`.
- **Input:** Query param `tickers` (optional, comma-separated, e.g. `?tickers=AAPL,MSFT,GOOG`)
- **Example output:**
  ```json
  {
    "scope": "market",
    "sentiment_score": 0.31,
    "ticker_count": 87,
    "computed_at": "2026-04-21T14:00:00Z"
  }
  ```
  With `?tickers=AAPL,MSFT`:
  ```json
  {
    "scope": "filtered",
    "sentiment_score": 0.55,
    "ticker_count": 2,
    "computed_at": "2026-04-21T14:00:00Z"
  }
  ```

---

### GET /sentiment/{symbol}/latest
- **File:** `api.py:95`
- **Auth:** None
- **Purpose:** Latest cached sentiment for a symbol without triggering a fetch.
- **Input:** Path param `symbol`
- **Example output:** Same shape as `SentimentResponse` above.

---

### POST /sentiment/{symbol}
- **File:** `api.py:40`
- **Auth:** None
- **Purpose:** Compute and store sentiment for a symbol (alias for the score endpoint via router).
- **Input:** Path param `symbol`
- **Example output:** Same shape as `SentimentResponse`.

---

### GET /stats
- **File:** `news_server.py:761`
- **Auth:** None
- **Purpose:** Database stats — article counts, sentiment record counts.
- **Input:** None
- **Example output:**
  ```json
  {
    "total_articles": 42800,
    "total_sentiments": 3200,
    "symbols_tracked": 87,
    "oldest_article": "2026-01-01T00:00:00Z",
    "newest_article": "2026-04-21T14:00:00Z"
  }
  ```

---

### GET /symbols
- **File:** `news_server.py:795`
- **Auth:** None
- **Purpose:** List all tracked ticker symbols.
- **Input:** None
- **Example output:**
  ```json
  { "symbols": ["AAPL", "MSFT", "NVDA", "GOOGL"] }
  ```

---

### GET /articles/{symbol}
- **File:** `news_server.py:815`
- **Auth:** None
- **Purpose:** Recent articles for a specific symbol.
- **Input:** Path param `symbol`. Query param `limit` (optional, default 50)
- **Example output:**
  ```json
  {
    "symbol": "AAPL",
    "articles": [
      { "title": "Apple reports record revenue", "url": "https://...", "published_at": "2026-04-20T10:00:00Z", "sentiment": 0.8 }
    ]
  }
  ```

---

### DELETE /articles/cleanup
- **File:** `news_server.py:901`
- **Auth:** None
- **Purpose:** Purge articles older than a configured retention window.
- **Input:** None
- **Example output:**
  ```json
  { "deleted": 1200 }
  ```

---

### POST /migrate
- **File:** `news_server.py:661`
- **Auth:** None
- **Purpose:** Migrate existing JSON news data files into the database.
- **Input:**
  ```json
  { "json_file_path": "/path/to/news_cache.json", "backup_existing": true }
  ```
- **Example output:**
  ```json
  { "migrated": 420, "skipped": 12, "errors": 0 }
  ```

---

### POST /scheduler/trigger
- **File:** `news_server.py:693`
- **Auth:** None
- **Purpose:** Manually trigger a background batch processing cycle for all tickers.
- **Input:** None
- **Example output:**
  ```json
  { "status": "triggered" }
  ```

---

### GET /scheduler/status
- **File:** `news_server.py:706`
- **Auth:** None
- **Purpose:** Background APScheduler status and next run time.
- **Input:** None
- **Example output:**
  ```json
  {
    "running": true,
    "next_run": "2026-04-21T15:00:00Z",
    "interval_minutes": 60
  }
  ```

---

### POST /scheduler/configure
- **File:** `news_server.py:727`
- **Auth:** None
- **Purpose:** Update scheduler interval or settings.
- **Input:**
  ```json
  { "interval_minutes": 30 }
  ```
- **Example output:**
  ```json
  { "status": "ok", "interval_minutes": 30 }
  ```

---

## External Calls Made

| Target | Method | URL | Purpose | Source File |
|---|---|---|---|---|
| News/RSS feeds | GET | Various external news APIs | Headline fetch for sentiment scoring | `services/news_aggregator.py` |
