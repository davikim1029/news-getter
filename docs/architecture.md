# Architecture — news-getter (port 9000)

## High-Level Workflow

1. **Startup** — `lifespan()` initializes SQLAlchemy DB tables, starts `AsyncIOScheduler` with sentiment aggregation tasks
2. **Headline collection** — background scheduler calls `aggregate_headlines_smart()` per ticker on a configurable interval; deduplicates via `HeadlineCache`
3. **Sentiment scoring** — each headline scored by NLP model; per-ticker aggregate written to `SymbolSentiment`
4. **API** — option-getter fetches `GET /sentiment/aggregate` once per scan cycle (market-wide + per-sector); iOS app may query per-ticker sentiment directly

---

## Workflow Breakdown

### Step 1: Startup and scheduling
Entry point: `lifespan()` → `news_server.py`

```
lifespan()
  → init_database()         create tables via SQLAlchemy
  → AsyncIOScheduler.start()
      → schedules aggregate_headlines_smart() per ticker
```

### Step 2: Headline aggregation (background, recurring)
Entry point: `aggregate_headlines_smart()` → `services/news_aggregator.py`

```
aggregate_headlines_smart(ticker)
  → HeadlineCache.deduplicate()   filter already-seen headlines
  → RateLimitCache.check()        back off if approaching API limit
  → fetch RSS / external API      fetch headlines for ticker
  → NLP sentiment model           score each headline
  → write SymbolSentiment + NewsArticle to DB
```

### Step 3: Sentiment query
Entry point: `GET /sentiment/aggregate` → `api.py`

```
GET /sentiment/aggregate[?tickers=...]
  → SQLAlchemy query on SymbolSentiment
  → aggregate: bullish/bearish/neutral counts, composite score
  → return JSON
```

---

## Function Details

### `lifespan()` (startup context manager)
- **File:** `news_server.py`
- **Called by:** FastAPI framework
- **Calls:** `init_database()`, `AsyncIOScheduler.start()`
- **Side effects:** Creates DB tables, starts scheduler

### `aggregate_headlines_smart(ticker)`
- **File:** `services/news_aggregator.py`
- **Called by:** Background scheduler
- **Calls:** `HeadlineCache.deduplicate()`, `RateLimitCache.check()`, external RSS/API, NLP model
- **Side effects:** Writes to `SymbolSentiment`, `NewsArticle` tables

### `GET /sentiment/aggregate`
- **File:** `api.py`
- **Called by:** option-getter (once per scan cycle — market-wide then per-sector), iOS app
- **Calls:** SQLAlchemy query on `SymbolSentiment`
- **Side effects:** None (read-only)

### `HeadlineCache.deduplicate()`
- **File:** `services/core/cache_manager.py`
- **Called by:** `aggregate_headlines_smart()`
- **Side effects:** In-memory dict update; prevents duplicate scoring across fetches

### `RateLimitCache`
- **File:** `services/core/cache_manager.py`
- **Called by:** `aggregate_headlines_smart()`
- **Side effects:** In-memory rate-limit state; sleeps if approaching limit

---

## Data Stores

| Store | Location | Owned | Tables |
|---|---|---|---|
| Sentiment DB | `database/` (SQLAlchemy SQLite) | Yes | `SymbolSentiment`, `NewsArticle`, `TickerSentiment` |
| HeadlineCache | In-memory | Yes | Deduplication dict |
| RateLimitCache | In-memory | Yes | Per-source rate-limit state |

---

## Background Threads

| Task | Schedule | Purpose |
|---|---|---|
| `aggregate_headlines_smart()` | Configurable interval (per ticker) | Fetch + score headlines |

---

## External Dependencies

| Service | Direction | Call | Purpose |
|---|---|---|---|
| `option-getter` (8001) | inbound | `GET /sentiment/aggregate` | Prefetch market + sector sentiment at scan start |
| External news APIs / RSS | outbound | HTTP | Source of headlines |
| NLP sentiment model | internal | Library call | Score each headline |

---

## Failure Handling

- **External API unavailable** — `RateLimitCache` backs off; cached sentiment returned if fresh
- **Rate limit hit** — `RateLimitCache` sleeps before next request; graceful degradation
- **NLP model error** — headline skipped; aggregate scores from remaining articles
