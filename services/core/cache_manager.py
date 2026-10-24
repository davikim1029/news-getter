# services/core/cache_manager.py
import json
import os
import tempfile
from datetime import datetime, timedelta
from threading import RLock
from shared_options.services.shutdown_handler import ShutdownManager
from shared_options.log.logger_singleton import getLogger


class CacheManager:

    """
    Thread-safe cache manager with TTL, autosave, and JSON persistence.
    """

    def __init__(self,
                 name: str,
                 filepath: str,
                 ttl_days: float = None,
                 ttl_hours: float = None,
                 ttl_minutes: float = None):
        self._cache = {}
        self._lock = RLock()

        self.name = name
        self.filepath = filepath
        self.ttl_days = ttl_days
        self.ttl_hours = ttl_hours
        self.ttl_minutes = ttl_minutes
        self.logger = getLogger()

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
        try:
            # Copy under lock
            with self._lock:
                cache_copy = dict(self._cache)

            serializable = {
                k: {"Value": v["Value"], "Timestamp": v["Timestamp"].isoformat()}
                for k, v in cache_copy.items()
            }

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
        with self._lock:
            self._cache[key] = {
                "Value": value,
                "Timestamp": datetime.now().astimezone()
            }

    def evict_expired(self) -> int:
        with self._lock:
            expired = [k for k, v in self._cache.items() if self.is_expired(v["Timestamp"])]
            for k in expired:
                del self._cache[k]
        return len(expired)


class RateLimitCache(CacheManager):
    def __init__(self):
        super().__init__("RateLimit Cache", "cache/ratelimit_sentiment.json", ttl_days=1)

    def is_limited(self, key: str) -> bool:
        """
        Check if a rate limit for `key` is still active.
        Returns True if still limited, False if expired.
        """
        with self._lock:
            item = self._cache.get(key)
            if not item:
                return False  # not cached at all

            reset_seconds = item.get("Value")
            timestamp = item.get("Timestamp")

            if reset_seconds is None or timestamp is None:
                return False  # malformed entry, treat as expired

            reset_time = timestamp + timedelta(seconds=reset_seconds)
            if datetime.now().astimezone() >= reset_time:
                # expired, remove from cache
                del self._cache[key]
                return False

            return True
