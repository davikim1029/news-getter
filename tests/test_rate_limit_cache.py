"""The rate-limit cache must remember a provider's back-off across a restart."""
from services.core.cache_manager import RateLimitCache


def test_backoff_is_saved_when_added_and_survives_a_restart(tmp_path):
    path = str(tmp_path / "cache" / "ratelimit.json")
    RateLimitCache(filepath=path).add("NewsAPI", 3600 * 24)

    # A new instance stands in for the process after a restart.
    assert RateLimitCache(filepath=path).is_limited("NewsAPI")


def test_backoff_ends_when_its_window_has_passed(tmp_path):
    cache = RateLimitCache(filepath=str(tmp_path / "ratelimit.json"))
    cache.add("NewsData", 0)

    assert not cache.is_limited("NewsData")
    assert not cache.is_limited("GoogleNews")
