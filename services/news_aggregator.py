"""Fetches recent headlines for one ticker and turns them into a sentiment score.

`aggregate_headlines_smart()` asks three providers (NewsAPI, NewsData, Google News RSS)
and combines whatever comes back. `compute_headlines_sentiment()` scores a list of
headlines from -1 (bad news) to 1 (good news): with the FinBERT language model when
USE_TRANSFORMERS=true, otherwise with a keyword heuristic. news_server.py calls both, once
per ticker per cycle.

A provider that answers "too many requests" is put on a back-off in the RateLimitCache
and skipped until it expires. Fetching raises when no provider could be searched at all,
so an outage is never mistaken for "no news"; scoring never raises.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from threading import Lock
from typing import List, Optional

from dateutil.parser import parse as parse_datetime

import os
import math
import re
import feedparser
import requests

from shared_options.log.logger_singleton import getLogger
from services.core.cache_manager import RateLimitCache

logger = getLogger()

_http_session = requests.Session()


def _float_env(name: str, default: float) -> float:
    """Read a positive number from environment variable `name`, or fall back to `default`."""
    try:
        value = float(os.getenv(name, str(default)))
        if value <= 0:
            raise ValueError
        return value
    except (TypeError, ValueError):
        logger.logMessage(f"[NewsHTTP] Invalid {name}; using default {default}s")
        return default


HTTP_CONNECT_TIMEOUT_SECONDS = _float_env("NEWS_HTTP_CONNECT_TIMEOUT_SECONDS", 3.0)
HTTP_READ_TIMEOUT_SECONDS = _float_env("NEWS_HTTP_READ_TIMEOUT_SECONDS", 10.0)
# Bounded timeouts, so one stalled provider can't hold an aggregation slot indefinitely.
HTTP_TIMEOUT = (HTTP_CONNECT_TIMEOUT_SECONDS, HTTP_READ_TIMEOUT_SECONDS)
HTTP_USER_AGENT = os.getenv(
    "NEWS_HTTP_USER_AGENT",
    "options-news-getter/1.0 (+https://github.com/daviskim/options)"
)

class RateLimitedException(Exception):
    """Raised when no provider could be searched and at least one is rate-limiting us."""
    pass

class AggregatorException(Exception):
    """Raised when no provider could be searched: each one errored or has no API key."""
    pass


class ProviderRateLimited(Exception):
    """A provider refused us: it answered 429 just now (`just_now`), or an earlier back-off is still running."""

    def __init__(self, provider: str, just_now: bool):
        super().__init__(f"{provider} is rate-limiting us")
        self.just_now = just_now


class ProviderNotConfigured(Exception):
    """A provider can't be asked because its API key isn't set."""
    pass

# -------------------------------------------------------
# Headline model
# -------------------------------------------------------
@dataclass
class Headline:
    """One headline as a provider returned it. `source` is the provider, not the publisher."""

    source: str
    title: str
    description: Optional[str] = None
    url: Optional[str] = None
    published_at: Optional[str] = None

    def combined_text(self) -> str:
        """Title and description joined into the one text that gets scored."""
        if self.description:
            return f"{self.title}. {self.description}"
        return self.title


# -------------------------------------------------------
# Provider clients. Each fetch() returns the provider's headlines, which may be none at
# all, and raises when the provider could not be searched: ProviderRateLimited,
# ProviderNotConfigured, or the request's own error.
# -------------------------------------------------------
class NewsClientBase:
    """Common shape of a news provider client."""

    def __init__(self, rate_cache: Optional[RateLimitCache], logger):
        self.rate_cache = rate_cache
        self.logger = logger

    def fetch(self, ticker: str, ticker_name: str) -> List[Headline]:
        """Headlines about `ticker`, searched by company name when one is known."""
        raise NotImplementedError


class NewsAPIClient(NewsClientBase):
    """newsapi.org: up to 50 English articles from a fixed list of financial publishers."""

    def __init__(self, rate_cache, logger):
        super().__init__(rate_cache, logger)
        self.api_key = os.getenv("NEWSAPI_KEY")

    def fetch(self, ticker: str, ticker_name: str) -> List[Headline]:
        if not self.api_key:
            self.logger.logMessage("[NewsAPI] API key missing; skipping NewsAPI")
            raise ProviderNotConfigured("NEWSAPI_KEY is not set")

        if self.rate_cache is not None and self.rate_cache.is_limited("NewsAPI"):
            raise ProviderRateLimited("NewsAPI", just_now=False)

        news_sources_str = "bloomberg,reuters,the-wall-street-journal,cnbc,marketwatch,the-new-york-times,financial-times,forbes,business-insider,yahoo-news"

        url = "https://newsapi.org/v2/everything"
       
        keyword = ticker_name
        if ticker_name == "" or ticker_name is None:
            keyword = ticker
            
        params = {
            "q": f'{keyword}',  # not quoted, so the words may match separately (NewsData's are quoted)
            "language": "en",
            "sources": news_sources_str,
            "sortBy": "publishedAt",
            "pageSize": 50,
            "apiKey": self.api_key
        }
        resp = _http_session.get(
            url,
            params=params,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": HTTP_USER_AGENT},
        )
        # NewsAPI's free tier allows a fixed number of requests per day, so a 429
        # means "stop for the day": back off for 24 hours.
        if resp.status_code == 429:
            self.logger.logMessage("[NewsAPI] Rate limited (429)")
            if self.rate_cache is not None:
                self.rate_cache.add("NewsAPI", 3600 * 24)
            raise ProviderRateLimited("NewsAPI", just_now=True)
        resp.raise_for_status()
        data = resp.json().get("articles", []) or []
        out = []
        for it in data:
            out.append(Headline(
                source="newsapi",
                title=it.get("title") or "",
                description=it.get("description"),
                url=it.get("url"),
                published_at=it.get("publishedAt")
            ))
        return out


class NewsDataClient(NewsClientBase):
    """newsdata.io: English business and technology articles matching the exact name."""

    def __init__(self, rate_cache, logger):
        super().__init__(rate_cache, logger)
        self.api_key = os.getenv("NEWSDATA_KEY")

    def fetch(self, ticker: str, ticker_name: str) -> List[Headline]:
        if not self.api_key:
            self.logger.logMessage("[NewsData] API key missing; skipping NewsData")
            raise ProviderNotConfigured("NEWSDATA_KEY is not set")

        if self.rate_cache is not None and self.rate_cache.is_limited("NewsData"):
            raise ProviderRateLimited("NewsData", just_now=False)

        categories = [
            "business",
            "technology",
        ]
        categories_str = ",".join(categories)
        
        url = "https://newsdata.io/api/1/news"
        keyword = ticker_name
        if ticker_name == "" or ticker_name is None:
            keyword = ticker
            
        params = {
            "apikey": self.api_key,
            "q": f'"{keyword}"',  # safely quoted
            "language": "en",
            "category": categories_str,

        }
        resp = _http_session.get(
            url,
            params=params,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": HTTP_USER_AGENT},
        )
        if resp.status_code == 429:  # back off for an hour
            self.logger.logMessage("[NewsData] Rate limited (429)")
            if self.rate_cache is not None:
                self.rate_cache.add("NewsData", 3600)
            raise ProviderRateLimited("NewsData", just_now=True)
        resp.raise_for_status()
        items = resp.json().get("results", []) or []
        out = []
        for it in items:
            out.append(Headline(
                source="newsdata",
                title=it.get("title") or "",
                description=it.get("description"),
                url=it.get("link"),
                published_at=it.get("pubDate") or it.get("date")
            ))
        return out


class GoogleNewsClient(NewsClientBase):
    """Google News RSS search. Needs no API key, so it runs even when the others have none."""

    def __init__(self, rate_cache, logger):
        super().__init__(rate_cache, logger)

    def _build_query_url(self, keywords: List[str]) -> str:
        """RSS search URL matching any of `keywords` (US English edition)."""
        query = "+OR+".join([k.replace(" ", "+") for k in keywords if k])
        return f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

    def fetch(self, ticker: str, ticker_name: str) -> List[Headline]:
        if self.rate_cache is not None and self.rate_cache.is_limited("GoogleNews"):
            raise ProviderRateLimited("GoogleNews", just_now=False)

        query = ticker_name or ticker
        keywords = [query, "stock", "finance"]
        url = self._build_query_url(keywords)
        resp = _http_session.get(
            url,
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": HTTP_USER_AGENT},
        )
        if resp.status_code == 429:  # back off for an hour
            self.logger.logMessage("[GoogleNews] Rate limited (429)")
            if self.rate_cache is not None:
                self.rate_cache.add("GoogleNews", 3600)
            raise ProviderRateLimited("GoogleNews", just_now=True)
        resp.raise_for_status()

        feed = feedparser.parse(resp.content)
        out = []
        for entry in feed.entries:
            out.append(Headline(
                source="google",
                title=entry.title,
                description=clean_description(getattr(entry, "summary", None)),
                url=getattr(entry, "link", None),
                published_at=getattr(entry, "published", None) or getattr(entry, "updated", None)
            ))
        return out


# -------------------------------------------------------
# Aggregator public function
# -------------------------------------------------------
def aggregate_headlines_smart(ticker: str, ticker_name: str, rate_cache: RateLimitCache = None) -> List[Headline]:
    """
    Fetch headlines for one ticker from every provider and return them combined.

    Providers that fail are logged and skipped, so any headlines at all are a success.
    An empty list means at least one provider was searched and found nothing, which is
    a real "no recent news". When no provider could be searched at all, this raises
    instead (RateLimitedException if any of them is rate-limiting us, else
    AggregatorException), so the caller never stores an outage as a neutral score.
    """
    clients = [
        ("newsapi", NewsAPIClient(rate_cache, logger)),
        ("newsdata", NewsDataClient(rate_cache, logger)),
        ("google", GoogleNewsClient(rate_cache, logger)),
    ]

    aggregated: List[Headline] = []
    searched: list[str] = []
    exceptions: list[tuple[str, Exception]] = []
    rate_limited_sources: list[str] = []   # answered 429 on this call
    backing_off_sources: list[str] = []    # skipped: an earlier 429's back-off is running
    not_configured: list[str] = []

    # Ask the providers one after another; each failure is recorded, not fatal.
    for name, client in clients:
        try:
            items = client.fetch(ticker, ticker_name)
        except ProviderRateLimited as e:
            (rate_limited_sources if e.just_now else backing_off_sources).append(name)
            continue
        except ProviderNotConfigured:
            not_configured.append(name)
            continue
        except Exception as e:
            logger.logMessage(f"[Aggregator] {name} client failed for {ticker}: {e}")
            exceptions.append((name, e))
            continue
        searched.append(name)
        aggregated.extend(items)

    if aggregated:
        # Return whatever we managed to collect. A back-off that was already running
        # isn't news, so only this call's failures are worth a log line.
        if exceptions or rate_limited_sources:
            logger.logMessage(
                f"[Aggregator] Partial failures for {ticker}: "
                f"exceptions={[(n,type(e).__name__) for n,e in exceptions]}, "
                f"rate_limited={rate_limited_sources}, "
                f"returning {len(aggregated)} headlines"
            )
        return aggregated

    if searched:
        return []

    # No provider could be searched, so "no headlines" would be a guess: raise instead.
    if rate_limited_sources or backing_off_sources:
        raise RateLimitedException(
            f"No source could be searched for {ticker}; rate-limited: "
            f"{rate_limited_sources + backing_off_sources}"
        )
    raise AggregatorException(
        f"All sources failed for {ticker}: "
        + ", ".join(
            [f"{n}: {type(e).__name__}" for n, e in exceptions]
            + [f"{n}: no API key" for n in not_configured]
        )
    )



# -------------------------------------------------------
# Sentiment scoring, in [-1.0, 1.0]
# -------------------------------------------------------
# Heuristic weights: how much one keyword moves a headline's score. Hand-picked, not fitted.
KEYWORD_WEIGHTS = {
    "earnings": 0.18, "beat": 0.15, "miss": -0.2, "lawsuit": -0.35, "layoff": -0.2,
    "upgrade": 0.25, "downgrade": -0.25, "acquisition": 0.10, "bankruptcy": -0.45,
    "guidance": 0.08, "investigation": -0.25, "recall": -0.25, "partnership": 0.08,
    "growth": 0.12, "surge": 0.12, "drop": -0.12, "rally": 0.10, "decline": -0.12
}
# How much to trust each source. Headline.source is the provider name, never the
# publisher, so only the "newsapi", "google" and "default" entries are ever used.
SOURCE_WEIGHTS = {
    "newsapi": 1.2, "reuters": 1.2, "bloomberg": 1.2, "the-wall-street-journal": 1.1,
    "cnbc": 1.0, "marketwatch": 0.95, "financial-times": 1.1, "forbes": 0.9,
    "business-insider": 0.9, "yahoo-news": 0.8, "google": 0.9, "default": 1.0
}

# Optional transformer support — controlled via USE_TRANSFORMERS env var (default false)
USE_TRANSFORMERS = os.getenv("USE_TRANSFORMERS", "false").lower() == "true"

_transformer_pipeline = None
_transformer_failed = False
_transformer_lock = Lock()


def _load_transformer_pipeline():
    """
    Load the FinBERT model the first time it is needed and return it; None if unavailable.

    The download and load take a while, so it happens once per process, guarded by a
    lock because scoring runs in worker threads. A failed load is remembered and never
    retried, and scoring then uses the keyword heuristic for the life of the process.
    """
    global _transformer_pipeline, _transformer_failed

    # Fast exit paths
    if _transformer_pipeline is not None:
        return _transformer_pipeline
    if _transformer_failed:
        return None

    with _transformer_lock:
        # Re-check inside lock
        if _transformer_pipeline is not None:
            return _transformer_pipeline
        if _transformer_failed:
            return None

        try:
            from transformers import (
                AutoTokenizer,
                AutoModelForSequenceClassification,
                pipeline,
            )
            import torch

            model_name = "ProsusAI/finbert"

            tokenizer = AutoTokenizer.from_pretrained(model_name)

            # Batched scoring pads short texts, which needs a padding token.
            if tokenizer.pad_token is None:
                tokenizer.pad_token = tokenizer.eos_token

            model = AutoModelForSequenceClassification.from_pretrained(
                model_name,
                torch_dtype=torch.float32,
                low_cpu_mem_usage=True,
                device_map=None,
            )

            model.eval()

            _transformer_pipeline = pipeline(
                "sentiment-analysis",
                model=model,
                tokenizer=tokenizer,
                device=-1,  # force CPU
            )

            logger.logMessage("[Sentiment] Transformer pipeline loaded")

        except Exception as e:
            _transformer_failed = True
            _transformer_pipeline = None
            logger.logMessage(f"[Sentiment] Transformer permanently disabled: {e}")

        return _transformer_pipeline



def compute_headlines_sentiment(headlines: List[Headline]) -> float:
    """
    Score a list of headlines from -1.0 (bad news) to 1.0 (good news).

    Uses FinBERT when USE_TRANSFORMERS=true and the model loaded, otherwise the keyword
    heuristic below. Returns neutral 0.0 for an empty list or on any error.
    """
    try:
        if not headlines:
            return 0.0

        texts = [h.combined_text()[:400] for h in headlines]

        # FinBERT path: each headline gets a label and a confidence; positive counts as
        # +confidence, negative as -confidence, neutral as 0, and the score is the mean.
        if USE_TRANSFORMERS:
            pipeline = _load_transformer_pipeline()
            if pipeline:
                try:
                    # Process in batches of 32 for better performance
                    batch_size = 32
                    results = pipeline(
                        texts, 
                        truncation=True, 
                        batch_size=batch_size,
                        max_length=512
                    )
                    vals = []
                    for r in results:
                        label = r.get("label", "").upper()
                        score = float(r.get("score", 0.0))
                        if label == "POSITIVE":
                            vals.append(score)
                        elif label == "NEGATIVE":
                            vals.append(-score)
                        else:
                            vals.append(0.0)
                    if vals:
                        mean = sum(vals) / len(vals)
                        # ensure range [-1,1]
                        return float(max(-1.0, min(1.0, mean)))
                except Exception as e:
                    logger.logMessage(f"[Sentiment] Transformer scoring failed: {e}")

        # Heuristic path: per headline, keyword points + polarity points, then the mean
        # over headlines weighted by source trust and recency.
        weighted_scores = []
        total_weight = 0.0
        for h in headlines:
            text = h.combined_text().lower()
            kw_score = 0.0
            for kw, w in KEYWORD_WEIGHTS.items():
                if kw in text:
                    kw_score += w

            # Extra polarity points. Matched as substrings, so "up" also counts in "update".
            pos_hits = sum(1 for w in ("up", "gain", "beat", "rise", "surge", "rally") if w in text)
            neg_hits = sum(1 for w in ("down", "miss", "loss", "drop", "decline", "slump") if w in text)
            lex_score = 0.05 * (pos_hits - neg_hits)

            # source weight
            src = (h.source or "").lower()
            sw = SOURCE_WEIGHTS.get(src, SOURCE_WEIGHTS["default"])

            # Recency: a headline's weight halves roughly every two days (e^(-days/3)),
            # counted in whole days. An unreadable or missing date keeps weight 1.0.
            rec_w = 1.0
            pub = published_datetime(h.published_at)
            if pub is not None:
                age_days = max(0.0, (datetime.now(timezone.utc) - pub).days)
                rec_w = math.exp(-age_days / 3.0)

            combined = (kw_score + lex_score) * sw * rec_w
            weighted_scores.append(combined)
            total_weight += abs(sw * rec_w)

        if not weighted_scores or total_weight == 0:
            return 0.0

        # Dividing by the summed weights (not the headline count) makes this a true
        # weighted mean: the weights decide how much each headline counts, without also
        # shrinking a set of old or low-trust headlines toward zero.
        avg = sum(weighted_scores) / total_weight
        # Clamp to [-1, 1]; typical scores are far smaller.
        if avg > 1.0: avg = 1.0
        if avg < -1.0: avg = -1.0
        return float(avg)

    except Exception as e:
        logger.logMessage(f"[Sentiment] compute_headlines_sentiment error: {e}")
        return 0.0


def published_datetime(value: Optional[str]) -> Optional[datetime]:
    """A headline's publish date as a timezone-aware UTC datetime, or None if it can't be read.

    Each provider writes dates its own way: NewsAPI as ISO 8601 ("2026-10-24T13:05:00Z"),
    NewsData as "2026-10-24 13:05:00" with no zone (it is UTC), and Google News as
    RFC 822 ("Fri, 24 Oct 2026 13:05:00 GMT").
    """
    if not value:
        return None
    try:
        parsed = parse_datetime(value)
    except (ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def clean_description(html_text: str) -> str:
    """Remove HTML tags, decode entities, and normalize whitespace."""
    if not html_text:
        return ""

    no_tags = re.sub(r"<.*?>", "", html_text)
    text = unescape(no_tags)

    # Replace non-breaking spaces and normalize multiple spaces
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text
