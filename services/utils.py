# scanner_utils.py

import yfinance as yf
import time as pyTime
import os
import json
from shared_options.models.option import OptionContract
from shared_options.log.logger_singleton import getLogger
from shared_options.models.tickers import fetch_us_tickers_from_finnhub
from services.core.cache_manager import TickerCache,RateLimitCache
from datetime import datetime, timedelta, time
import sqlite3
from datetime import datetime, timedelta
from shared_options.models.option import OptionContract,OptionGreeks,Product, ProductId, Quick
import pandas_market_calendars as mcal
import pytz
from typing import List

################################ TICKER CACHE ####################################

def get_active_tickers(ticker_cache:TickerCache = None):
    if ticker_cache is not None:
        ticker_cache._load_cache()
        if ticker_cache.is_empty():
            tickers = fetch_us_tickers_from_finnhub(ticker_cache=ticker_cache)
        else:
            tickers = ticker_cache._cache.keys()
    else:
        tickers = fetch_us_tickers_from_finnhub(ticker_cache=ticker_cache)
    return tickers

def get_next_run_date(seconds_to_wait: int) -> str:
    """
    Returns the next run time as a string in 12-hour format (AM/PM),
    adding seconds_to_wait to the current time while rolling over AM/PM half-days.
    Fully timezone-aware.
    """
    HALF_DAY = 12 * 60 * 60  # 43,200 seconds

    now = datetime.now().astimezone()  # aware datetime
    tz = now.tzinfo  # preserve timezone info

    # Seconds into current 12-hour half (0–43,199)
    seconds_in_half = (now.hour % 12) * 3600 + now.minute * 60 + now.second

    # Total seconds after wait
    total_seconds = seconds_in_half + seconds_to_wait

    # How many half-days to roll over, remainder seconds
    carry_halves, rem_seconds = divmod(total_seconds, HALF_DAY)

    # Determine current AM/PM half: 0 = AM, 1 = PM
    current_half = 0 if now.hour < 12 else 1
    new_half = (current_half + carry_halves) % 2

    # Anchor base datetime at midnight (AM) or noon (PM) in same tz
    base_time = time(0, 0) if new_half == 0 else time(12, 0)
    base = datetime.combine(now.date(), base_time, tzinfo=tz)

    # Add remaining seconds
    next_run = base + timedelta(seconds=rem_seconds)

    return next_run.strftime("%I:%M %p")


from datetime import datetime, timedelta

def is_rate_limited(cache: RateLimitCache, key: str) -> bool:
    """
    Check if a rate limit for `key` is still active.
    Returns True if still limited, False if expired.
    """
    item = cache._cache.get(key)
    if not item:
        return False  # not cached at all

    reset_seconds = item.get("Value")
    timestamp = item.get("Timestamp")

    if reset_seconds is None or timestamp is None:
        return False  # malformed entry, treat as expired

    reset_time = timestamp + timedelta(seconds=reset_seconds)
    if datetime.now().astimezone() >= reset_time:
        # expired, remove from cache
        with cache._lock:
            del cache._cache[key]
        return False

    return True


def wait_rate_limit(cache: RateLimitCache, key: str):
    """
    If the rate limit for `key` is still active, wait the remaining time.
    Removes the cache entry if expired.
    """
    item = cache._cache.get(key)
    if not item:
        return  # no limit, proceed

    reset_seconds = item.get("Value")
    timestamp = item.get("Timestamp")

    if reset_seconds is None or timestamp is None:
        return  # malformed entry, treat as expired

    # Ensure timestamp is a datetime object
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)

    reset_time = timestamp + timedelta(seconds=reset_seconds)
    now = datetime.now().astimezone()

    if now >= reset_time:
        # expired, remove from cache
        with cache._lock:
            del cache._cache[key]
        return

    # Calculate remaining wait time in seconds
    remaining = (reset_time - now).total_seconds()
    print(f"[RateLimit] Waiting {remaining:.1f} seconds for {key}...")
    pyTime.sleep(remaining)

    # Once slept, remove entry
    with cache._lock:
        cache._cache.pop(key, None)



def get_active_option_contracts(db_path: str, min_days_remaining: int = 5) -> List[OptionContract]:
    """
    Retrieves the latest snapshot per OSI key that:
      - Has never expired (no snapshot with daysToExpiration = 0)
      - Has more than `min_days_remaining` days left until expiration
    Converts each into an OptionContract dataclass with full Product info.
    """
    query = """
        SELECT s.*
            FROM option_snapshots s
            JOIN (
                SELECT osiKey, MAX(timestamp) AS max_ts
                FROM option_snapshots
                WHERE timestamp >= datetime('now','localtime','start of day','utc')
                AND timestamp <  datetime('now','localtime','start of day','+1 day','utc')
                AND osiKey NOT IN (
                    SELECT osiKey
                    FROM option_snapshots
                    WHERE daysToExpiration = 0
                        AND timestamp >= datetime('now','localtime','start of day','utc')
                        AND timestamp <  datetime('now','localtime','start of day','+1 day','utc')
                )
                GROUP BY osiKey
            ) latest
            ON s.osiKey = latest.osiKey
            AND s.timestamp = latest.max_ts
            WHERE s.daysToExpiration > ?;
    """

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, (min_days_remaining,))
        rows = cursor.fetchall()

    contracts = []
    for row in rows:
        # Calculate expiry date
        expiry_date = None
        if row["daysToExpiration"] and row["daysToExpiration"] > 0:
            expiry_date = datetime.now().astimezone() + timedelta(days=row["daysToExpiration"])

        greeks = OptionGreeks(
            delta=row["delta"] if row["delta"] is not None else None,
            gamma=row["gamma"] if row["gamma"] is not None else None,
            theta=row["theta"] if row["theta"] is not None else None,
            vega=row["vega"] if row["vega"] is not None else None,
            rho=row["rho"] if row["rho"] is not None else None,
            iv=row["iv"] if row["iv"] is not None else None
        )

        product = Product(
            symbol=row["symbol"],
            securityType="OPTION",
            callPut="CALL" if row["optionType"] == 1 else "PUT",
            expiryYear=expiry_date.year if expiry_date else None,
            expiryMonth=expiry_date.month if expiry_date else None,
            expiryDay=expiry_date.day if expiry_date else None,
            strikePrice=row["strikePrice"],
            productId=ProductId(symbol=row["symbol"], typeCode="OPTION")
        )

        quick = Quick(
            lastTrade=row["lastPrice"] if row["lastPrice"] is not None else None,
            volume=row["volume"] if row["volume"] is not None else None
        )

        contract = OptionContract(
            symbol=row["symbol"],
            optionType="CALL" if row["optionType"] == 1 else "PUT",
            strikePrice=row["strikePrice"],
            displaySymbol=row["symbol"],
            osiKey=row["osiKey"],
            bid=row["bid"] if row["bid"] is not None else None,
            ask=row["ask"] if row["ask"] is not None else None,
            bidSize=int(row["bidSize"]) if row["bidSize"] is not None else None,
            askSize=int(row["askSize"]) if row["askSize"] is not None else None,
            inTheMoney="Y" if row["inTheMoney"] == 1 else "N",
            volume=int(row["volume"]) if row["volume"] is not None else None,
            openInterest=int(row["openInterest"]) if row["openInterest"] is not None else None,
            lastPrice=row["lastPrice"] if row["lastPrice"] is not None else None,
            OptionGreeks=greeks,
            quick=quick,
            product=product,
            expiryDate=expiry_date,
            nearPrice=row["nearPrice"] if row["nearPrice"] is not None else None
        )


        contracts.append(contract)

    return contracts
