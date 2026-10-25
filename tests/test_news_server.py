"""The per-ticker score route and the store path behind it, with the providers stubbed out."""
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from services.news_aggregator import AggregatorException, Headline, RateLimitedException


def _stub_fetch(monkeypatch, news_server, outcome):
    """Replace the provider fetch: return `outcome`'s headlines, or raise it. Returns a call log."""
    calls = []

    def fake(ticker, ticker_name, rate_cache):
        calls.append(ticker)
        if isinstance(outcome, Exception):
            raise outcome
        return list(outcome)

    monkeypatch.setattr(news_server, "aggregate_headlines_smart", fake)
    return calls


def _client(news_server):
    # Not used as a context manager, so the app's startup (scheduler, .env) doesn't run.
    return TestClient(news_server.app)


def _stored(news_server, symbol):
    from database.database import SessionLocal
    from models.models import NewsArticle, SymbolSentiment

    with SessionLocal() as db:
        record = db.query(SymbolSentiment).filter(SymbolSentiment.symbol == symbol).first()
        urls = sorted(a.url for a in db.query(NewsArticle).filter(NewsArticle.symbol == symbol))
        return record, urls


def _age_score(news_server, symbol, hours):
    from database.database import SessionLocal
    from models.models import SymbolSentiment

    with SessionLocal() as db:
        record = db.query(SymbolSentiment).filter(SymbolSentiment.symbol == symbol).first()
        record.last_updated = datetime.now(timezone.utc) - timedelta(hours=hours)
        db.commit()


GOOD_NEWS = [Headline(source="newsapi", title="Analyst upgrade", url="https://example.com/a")]


def test_from_cache_says_whether_the_providers_were_asked(news_db, monkeypatch):
    _stub_fetch(monkeypatch, news_db, GOOD_NEWS)
    client = _client(news_db)

    assert client.post("/sentiment/AAPL/score").json()["from_cache"] is False
    assert client.post("/sentiment/AAPL/score").json()["from_cache"] is True
    assert client.post("/sentiment/AAPL/score?force_refresh=true").json()["from_cache"] is False


def test_one_article_about_two_tickers_is_stored_for_each(news_db, monkeypatch):
    _stub_fetch(monkeypatch, news_db, GOOD_NEWS)
    client = _client(news_db)

    client.post("/sentiment/AAPL/score")
    client.post("/sentiment/MSFT/score")

    assert _stored(news_db, "AAPL")[1] == ["https://example.com/a"]
    assert _stored(news_db, "MSFT")[1] == ["https://example.com/a"]


def test_the_same_story_from_two_providers_is_stored_once(news_db, monkeypatch):
    twice = GOOD_NEWS + [Headline(source="google", title="Analyst upgrade", url="https://example.com/a")]
    _stub_fetch(monkeypatch, news_db, twice)

    response = _client(news_db).post("/sentiment/AAPL/score")

    assert response.status_code == 200
    assert _stored(news_db, "AAPL")[1] == ["https://example.com/a"]


def test_a_failed_fetch_keeps_the_last_real_score_and_then_fails_fast(news_db, monkeypatch):
    _stub_fetch(monkeypatch, news_db, GOOD_NEWS)
    client = _client(news_db)
    client.post("/sentiment/AAPL/score")
    real_score = _stored(news_db, "AAPL")[0].sentiment_score
    assert real_score > 0
    _age_score(news_db, "AAPL", hours=2)  # stale, so the next request asks the providers

    calls = _stub_fetch(monkeypatch, news_db, AggregatorException("All sources failed for AAPL"))
    assert client.post("/sentiment/AAPL/score").status_code == 502
    assert client.post("/sentiment/AAPL/score").status_code == 502
    assert calls == ["AAPL"], "the second request should fail fast without asking the providers"

    record, _ = _stored(news_db, "AAPL")
    assert record.sentiment_score == real_score
    assert record.article_count == 1

    # force_refresh skips the fail-fast window.
    client.post("/sentiment/AAPL/score?force_refresh=true")
    assert calls == ["AAPL", "AAPL"]


def test_rate_limited_answers_429_and_stores_nothing(news_db, monkeypatch):
    _stub_fetch(monkeypatch, news_db, RateLimitedException("No source could be searched"))

    assert _client(news_db).post("/sentiment/AAPL/score").status_code == 429
    assert _stored(news_db, "AAPL") == (None, [])


def test_no_news_from_a_searched_provider_is_stored_as_neutral(news_db, monkeypatch):
    _stub_fetch(monkeypatch, news_db, [])

    body = _client(news_db).post("/sentiment/AAPL/score").json()

    assert (body["sentiment_score"], body["article_count"]) == (0.0, 0)


def test_re_enabling_the_schedule_keeps_the_error_logging_wrapper(news_db, monkeypatch):
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    monkeypatch.setattr(news_db.app_state, "scheduler", AsyncIOScheduler())
    client = _client(news_db)

    client.post("/scheduler/configure", json={"enabled": False, "interval_minutes": 60})
    client.post("/scheduler/configure", json={"enabled": True, "interval_minutes": 30})

    job = news_db.app_state.scheduler.get_job("news_aggregation")
    assert job.func is news_db.job_wrapper
    assert job.kwargs == {"job_func": news_db.process_all_tickers}
