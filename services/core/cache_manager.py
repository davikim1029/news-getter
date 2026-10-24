"""Remembers which news providers have rate-limited us, and until when.

When a provider answers HTTP 429 ("too many requests"), the fetch code records a back-off
here, and every later fetch checks it first so we stop calling that provider until the
back-off expires. `CacheManager` is a small generic key/value store with expiry and a JSON
file; `RateLimitCache` is the one cache this service uses.

Caution: the JSON file is loaded at startup but only written by a shutdown callback, and
news-getter never fires shutdown callbacks, so in practice back-offs are lost on restart
(NEWSGET-01).
"""
import json
import os
import tempfile
from datetime import datetime, timedelta
from threading import RLock
from shared_options.services.shutdown_handler import ShutdownManager
from shared_options.log.logger_singleton import getLogger


class CacheManager:

    """
    Thread-safe key/value cache whose entries expire after a time-to-live (TTL).

    Each entry is stored as {"Value": ..., "Timestamp": <when it was added>}. The cache
    is loaded from `filepath` on creation and saved back to it on shutdown.
    """

    def __init__(self,
                 name: str,
                 filepath: str,
                 ttl_days: float = None,
                 ttl_hours: float = None,
                 ttl_minutes: float = None):
        """Create the cache, register its save-on-shutdown hook, and load the file."""
        self._cache = {}
        self._lock = RLock()

        self.name = name
        self.filepath = filepath
        self.ttl_days = ttl_days
        self.ttl_hours = ttl_hours
        self.ttl_minutes = ttl_minutes
        self.logger = getLogger()

        # Defensive retry: initialise the shutdown manager if registering fails.
        try:
            ShutdownManager.register(self.name, lambda reason=None: self._save_cache())
        except TypeError:
            ShutdownManager.init(error_logger=self.logger.logMessage)
            ShutdownManager.register(self.name, lambda reason=None: self._save_cache())
        self.logger.logMessage(f"Loading {self.name}...")
        self._load_cache()
        self.logger.logMessage(f"Successfully loaded {self.name}.")

    # ----------------------------
    # Cache Persistence
    # ----------------------------
    def _load_cache(self):
        """Read entries from the JSON file, skipping expired ones. Creates an empty file if missing."""
        if not os.path.exists(self.filepath):
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            with open(self.filepath, "w") as f:
                json.dump({}, f)
            return

        try:
            with open(self.filepath, "r") as f:
                raw = json.load(f)
            with self._lock:
                for key, data in raw.items():
                    ts_str = data.get("Timestamp")
                    value = data.get("Value")
                    if ts_str is None:
                        continue
                    ts = datetime.fromisoformat(ts_str)
                    if not self.is_expired(ts):
                        self._cache[key] = {"Value": value, "Timestamp": ts}
        except json.JSONDecodeError:
            self.logger.logMessage(f"[{self.name}] Cache file empty or corrupted, starting fresh")
        except Exception as e:
            self.logger.logMessage(f"[{self.name}] Failed to load cache: {e}")

    def _save_cache(self):
        """Write all entries to the JSON file. Logs and continues on failure."""
        try:
            # Copy under lock
            with self._lock:
                cache_copy = dict(self._cache)

            serializable = {
                k: {"Value": v["Value"], "Timestamp": v["Timestamp"].isoformat()}
                for k, v in cache_copy.items()
            }

            # Write to a temporary file and rename it over the real one, so a crash
            # mid-write can never leave a half-written cache file behind.
            dir_name = os.path.dirname(self.filepath)
            with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, encoding="utf-8") as tmp:
                json.dump(serializable, tmp, indent=2, default=str)
                tmp.flush()
                os.fsync(tmp.fileno())

            os.replace(tmp.name, self.filepath)
        except Exception as e:
            self.logger.logMessage(f"[{self.name}] Failed to save cache: {e}")

    # ----------------------------
    # TTL / Expiration
    # ----------------------------
    def is_expired(self, timestamp):
        """True if an entry added at `timestamp` is older than this cache's TTL (30 days if none set)."""
        days = self.ttl_days if self.ttl_days is not None else 0
        hours = self.ttl_hours if self.ttl_hours is not None else 0
        minutes = self.ttl_minutes if self.ttl_minutes is not None else 0

        if days == 0 and hours == 0 and minutes == 0:
            days = 30  # default 30 days

        ttl = timedelta(days=days, hours=hours, minutes=minutes)
        return datetime.now().astimezone() - timestamp > ttl

    # ----------------------------
    # Public Cache Methods
    # ----------------------------
    def add(self, key, value):
        """Store `value` under `key`, stamped with the current time."""
        with self._lock:
            self._cache[key] = {
                "Value": value,
                "Timestamp": datetime.now().astimezone()
            }

    def evict_expired(self) -> int:
        """Drop every expired entry from memory and return how many were dropped."""
        with self._lock:
            expired = [k for k, v in self._cache.items() if self.is_expired(v["Timestamp"])]
            for k in expired:
                del self._cache[k]
        return len(expired)


class RateLimitCache(CacheManager):
    """Per-provider back-offs. Key = provider name, value = back-off length in seconds."""

    def __init__(self):
        super().__init__("RateLimit Cache", "cache/ratelimit_sentiment.json", ttl_days=1)

    def is_limited(self, key: str) -> bool:
        """
        True while provider `key` is still inside its back-off window.

        An entry whose window has passed is removed on the way out.
        """
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return False  # not cached at all

            reset_seconds = item.get("Value")
            timestamp = item.get("Timestamp")

            if reset_seconds is None or timestamp is None:
                return False  # malformed entry, treat as expired

            # The back-off ends `reset_seconds` after the 429 was recorded.
            reset_time = timestamp + timedelta(seconds=reset_seconds)
            if datetime.now().astimezone() >= reset_time:
                # expired, remove from cache
                del self._cache[key]
                return False

            return True
