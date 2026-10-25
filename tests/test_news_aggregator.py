"""How headlines are fetched from the providers and turned into one score."""
from datetime import datetime, timedelta, timezone

import pytest

from services import news_aggregator as na
from services.news_aggregator import Headline


class _Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def _providers(monkeypatch, newsapi, newsdata, google):
    """Make each provider's fetch() do what its argument says: a list, or an exception to raise."""
    for cls, outcome in ((na.NewsAPIClient, newsapi), (na.NewsDataClient, newsdata), (na.GoogleNewsClient, google)):
        def fetch(self, ticker, name, outcome=outcome):
            if isinstance(outcome, Exception):
                raise outcome
            return outcome
        monkeypatch.setattr(cls, "fetch", fetch)


def test_an_outage_raises_instead_of_looking_like_no_news(monkeypatch):
    _providers(monkeypatch, ConnectionError("down"), ConnectionError("down"), TimeoutError("slow"))

    with pytest.raises(na.AggregatorException):
        na.aggregate_headlines_smart("AAPL", "Apple")


def test_every_provider_backing_off_raises_rate_limited(monkeypatch):
    backing_off = na.ProviderRateLimited("x", just_now=False)
    _providers(monkeypatch, backing_off, backing_off, backing_off)

    with pytest.raises(na.RateLimitedException):
        na.aggregate_headlines_smart("AAPL", "Apple")


def test_no_api_keys_and_a_failing_google_is_an_error_not_no_news(monkeypatch):
    missing = na.ProviderNotConfigured("no key")
    _providers(monkeypatch, missing, missing, ConnectionError("down"))

    with pytest.raises(na.AggregatorException, match="no API key"):
        na.aggregate_headlines_smart("AAPL", "Apple")


def test_every_configured_provider_finding_nothing_means_no_news(monkeypatch):
    _providers(monkeypatch, na.ProviderNotConfigured("no key"), [], [])

    assert na.aggregate_headlines_smart("AAPL", "Apple") == []


def test_no_headlines_while_a_provider_failed_is_an_error_not_no_news(monkeypatch):
    _providers(monkeypatch, na.ProviderNotConfigured("no key"), ConnectionError("down"), [])

    with pytest.raises(na.AggregatorException):
        na.aggregate_headlines_smart("AAPL", "Apple")


def test_no_headlines_while_google_is_refusing_is_rate_limited_not_no_news(monkeypatch):
    # NewsAPI searches only ten publishers, so its empty answer for a small company says
    # nothing; with Google refusing, the honest answer is "could not tell".
    _providers(monkeypatch, [], na.ProviderRateLimited("NewsData", just_now=False),
               na.ProviderRateLimited("GoogleNews", just_now=True))

    with pytest.raises(na.RateLimitedException):
        na.aggregate_headlines_smart("ACME", "Acme Corp")


def test_any_headlines_are_returned_even_when_other_providers_fail(monkeypatch):
    headline = Headline(source="google", title="Apple rallies")
    _providers(monkeypatch, ConnectionError("down"), na.ProviderRateLimited("x", just_now=True), [headline])

    assert na.aggregate_headlines_smart("AAPL", "Apple") == [headline]


def test_a_failed_request_raises_out_of_the_client(monkeypatch):
    def broken_get(*args, **kwargs):
        raise ConnectionError("no route to host")
    monkeypatch.setattr(na._http_session, "get", broken_get)

    with pytest.raises(ConnectionError):
        na.GoogleNewsClient(None, na.logger).fetch("AAPL", "Apple")


def test_a_429_records_a_backoff_and_raises(monkeypatch, tmp_path):
    from services.core.cache_manager import RateLimitCache

    cache = RateLimitCache(filepath=str(tmp_path / "ratelimit.json"))
    monkeypatch.setattr(na._http_session, "get", lambda *a, **k: _Response(429))
    monkeypatch.setenv("NEWSDATA_KEY", "test-key")

    with pytest.raises(na.ProviderRateLimited) as raised:
        na.NewsDataClient(cache, na.logger).fetch("AAPL", "Apple")
    assert raised.value.just_now
    assert cache.is_limited("NewsData")

    # The next call skips the provider without a request.
    monkeypatch.setattr(na._http_session, "get", lambda *a, **k: pytest.fail("called a backing-off provider"))
    with pytest.raises(na.ProviderRateLimited) as raised:
        na.NewsDataClient(cache, na.logger).fetch("AAPL", "Apple")
    assert not raised.value.just_now


@pytest.mark.parametrize("status", [429, 503])
def test_a_google_refusal_backs_off_for_an_hour(monkeypatch, tmp_path, status):
    from services.core.cache_manager import RateLimitCache

    cache = RateLimitCache(filepath=str(tmp_path / "ratelimit.json"))
    monkeypatch.setattr(na._http_session, "get", lambda *a, **k: _Response(status))

    with pytest.raises(na.ProviderRateLimited) as raised:
        na.GoogleNewsClient(cache, na.logger).fetch("AAPL", "Apple")
    assert raised.value.just_now
    assert cache.is_limited("GoogleNews")

    monkeypatch.setattr(na._http_session, "get", lambda *a, **k: pytest.fail("called a backing-off provider"))
    with pytest.raises(na.ProviderRateLimited):
        na.GoogleNewsClient(cache, na.logger).fetch("AAPL", "Apple")


@pytest.mark.parametrize("value", [
    "2026-10-24T13:05:00Z",            # NewsAPI
    "2026-10-24 13:05:00",             # NewsData: no zone, UTC
    "Sat, 24 Oct 2026 13:05:00 GMT",   # Google News
])
def test_every_providers_date_format_is_read_as_utc(value):
    assert na.published_datetime(value) == datetime(2026, 10, 24, 13, 5, tzinfo=timezone.utc)


def test_an_unreadable_date_is_none():
    assert na.published_datetime("yesterday-ish") is None
    assert na.published_datetime(None) is None


def _rfc822(moment: datetime) -> str:
    return moment.strftime("%a, %d %b %Y %H:%M:%S GMT")


def test_one_headline_scores_its_own_points_whatever_its_weight():
    # "upgrade" is worth 0.25, and it also contains the polarity word "up" (+0.05).
    headline = Headline(source="newsapi", title="Analyst upgrade")

    assert na.compute_headlines_sentiment([headline]) == pytest.approx(0.30)


def test_an_old_google_headline_counts_far_less_than_a_fresh_one():
    now = datetime.now(timezone.utc)
    fresh = Headline(source="google", title="Analyst upgrade", published_at=_rfc822(now))
    month_old = Headline(source="google", title="Analyst downgrade",
                         published_at=_rfc822(now - timedelta(days=30)))

    # The month-old headline's weight is e^-10, so the mean is all but the fresh one's 0.30.
    assert na.compute_headlines_sentiment([fresh, month_old]) == pytest.approx(0.30, abs=1e-3)
