# services/core/cache_manager.py
import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from threading import RLock
from shared_options.services.shutdown_handler import ShutdownManager
from shared_options.log.logger_singleton import getLogger
import shutil
from pathlib import Path

class CacheManager:
    
    """
    Thread-safe cache manager with TTL, autosave, and JSON persistence.
    """

    def __init__(self,
                 name: str,
                 filepath: str,
                 ttl_days: float = None,
                 ttl_hours: float = None,
                 ttl_minutes: float = None,
                 autosave_interval: int = 60):
        self._cache = {}
        self._lock = RLock()

        self.name = name
        self.filepath = filepath
        self.ttl_days = ttl_days
        self.ttl_hours = ttl_hours
        self.ttl_minutes = ttl_minutes
        self.autosave_interval = autosave_interval
        self.logger = getLogger()
        
        
        # ------------------------
        # New global scanner config
        # ------------------------
        self.scanner_config = {
            "parallel": os.environ.get("BUY_PARALLEL", "1") == "1",
            "max_workers": int(os.environ.get("BUY_MAX_WORKERS", "8")),
            "min_volume": int(os.environ.get("MIN_VOLUME", "50")),
            "min_ask_cents": int(os.environ.get("MIN_ASK_CENTS", "5")),
            "max_ask_cents": int(os.environ.get("MAX_ASK_CENTS", "50")),
            "strike_range_pct": int(os.environ.get("STRIKE_RANGE_PCT", "20"))
        }


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


    def autosave_loop(self, stop_event):
        while not stop_event.is_set():
            self._save_cache()
            stop_event.wait(self.autosave_interval)

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
                "Value": self._convert_nested_tuples(value),
                "Timestamp": datetime.now().astimezone()
            }

    def get(self, key):
        if self.is_cached(key):
            return self._cache[key]["Value"]
        return None

    def is_cached(self, key):
        with self._lock:
            item = self._cache.get(key)
            if item:
                if self.is_expired(item["Timestamp"]):
                    del self._cache[key]
                    return False
                return True
            return False
    
    def delete_key(self,key):
        with self._lock:
            item = self._cache.get(key)
            if item:
                del self._cache[key]

    def clear(self):
        with self._lock:
            self._cache.clear()
        self._save_cache()

    def is_empty(self):
        return not bool(self._cache)

    # ----------------------------
    # Utility Methods
    # ----------------------------
    def _convert_nested_tuples(self, value):
        if not isinstance(value, dict):
            return value
        nested = {}
        for key, val in value.items():
            if isinstance(key, tuple):
                current = nested
                for k in key[:-1]:
                    current = current.setdefault(k, {})
                current[key[-1]] = val
            else:
                nested[key] = val
        return nested

    def maybe_convert_tuples(self, value):
        if isinstance(value, dict) and any(isinstance(k, tuple) for k in value.keys()):
            return self._convert_nested_tuples(value)
        return value
    
    
    def copy_cache_to_file(self, backup_folder: str = "data/ticker_eval",filename: str = ""):
        """
        Make a timestamped copy of a Cache file.
        """
        
        timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        if filename == "":
            filename = f"{self.name.replace(' ', '_')}_{timestamp}.json"
        if not filename.endswith(".json"):
            filename = f"{filename}.json"
            
        backup_dir = Path(backup_folder)
        backup_dir.mkdir(parents=True, exist_ok=True)

        src = Path(self.filepath)
        dst = backup_dir / f"{filename}"

        try:
            shutil.copy2(src, dst)
            self.logger.logMessage(f"[{self.name} Backup] Saved backup to {dst}")
        except Exception as e:
            self.logger.logMessage(f"[{self.name} Backup] Failed to backup: {e}")


# ----------------------------
# Concrete Caches
# ----------------------------
class IgnoreTickerCache(CacheManager):
    def __init__(self):
        super().__init__("IgnoreTicker Cache", "cache/ignore_tickers.json", ttl_days=30, autosave_interval=60)


class BoughtTickerCache(CacheManager):
    def __init__(self):
        super().__init__("BoughtTicker Cache", "cache/bought_tickers.json", ttl_hours=6, autosave_interval=60)


class NewsApiCache(CacheManager):
    def __init__(self):
        super().__init__("NewsApi Cache", "cache/newsapi_sentiment.json", ttl_hours=6, autosave_interval=60)
        
class HeadlineCache(CacheManager):
    def __init__(self):
        super().__init__("Headline Cache", "cache/headlines.json", ttl_hours=6, autosave_interval=60)


class RateLimitCache(CacheManager):
    def __init__(self):
        super().__init__("RateLimit Cache", "cache/ratelimit_sentiment.json", ttl_days=1, autosave_interval=60)

class YFinanceTickerCache(CacheManager):
    def __init__(self):
        super().__init__("YFinance Ticker Cache", "cache/yfinance_ticker.json", ttl_days=30, autosave_interval=60)


class TickerCache(CacheManager):
    def __init__(self):
        super().__init__("Ticker Cache", "cache/tickers.json", ttl_days=30)


class EvalCache(CacheManager):
    def __init__(self):
        super().__init__("Eval Cache", "cache/evaluated.json", ttl_minutes=30)


class LastTickerCache(CacheManager):
    def __init__(self):
        super().__init__("LastTicker Cache", "cache/last_ticker.json", ttl_days=1)
        
class TickerMetadata(CacheManager):
    def __init__(self):
        super().__init__("TickerMetadata Cache","cache/ticker_metadata.json",ttl_days=5)


# ----------------------------
# Unified Cache Container
# ----------------------------
class Caches:
    """
    One container for all caches to simplify function signatures and ThreadManager integration.
    """
    def __init__(self):
        self.ignore = IgnoreTickerCache()
        self.bought = BoughtTickerCache()
        self.news = NewsApiCache()
        self.rate = RateLimitCache()
        self.ticker = TickerCache()
        self.eval = EvalCache()
        self.yfin = YFinanceTickerCache()
        self.last_seen = LastTickerCache()
        self.ticker_metadata = TickerMetadata()
        self.headlines = HeadlineCache()

    # Return list of all caches (for loops in scanner)
    def all_caches(self):
        return [
            self.ignore,
            self.bought,
            self.news,
            self.rate,
            self.ticker,
            self.eval,
            self.yfin,
            self.last_seen,
            self.ticker_metadata,
            self.headlines
        ]

    # Return tuples for autosave loops (for ThreadManager)
    def all_autosave_loops(self):
        return [
            (self.ignore.autosave_loop, "Ignore Cache Autosave"),
            (self.bought.autosave_loop, "Bought Cache Autosave"),
            (self.news.autosave_loop, "NewsAPI Cache Autosave"),
            (self.rate.autosave_loop, "RateLimit Cache Autosave"),
            (self.last_seen.autosave_loop, "Last Ticker Cache Autosave"),
            (self.ticker_metadata.autosave_loop,"Ticker Metadata Cache Autosave"),
            (self.eval.autosave_loop,"Evaluation Cache Autosave"),
            (self.yfin.autosave_loop, "YFinance Cache Autosave"),
            (self.headlines.autosave_loop, "Headline Cache Autosave")
        ]

    # Clear all caches
    def clear_all(self):
        for cache in self.all_caches():
            cache.clear()


