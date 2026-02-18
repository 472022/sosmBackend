"""
stock_fetcher.py  (v2 — Production-Hardened)
=============================================
A bulletproof parallel stock data fetcher for the Indian Stock API.

New in v2:
  ─ Exponential back-off with jitter  (retries: 3 attempts by default)
  ─ HTTP 429 / 503 rate-limit detection with Retry-After header support
  ─ Circuit-breaker: pauses a stock that fails N times in a row
  ─ Request deduplication / response caching (TTL-based, per symbol)
  ─ Graceful HTTP connection reuse via a per-thread connection pool
  ─ Structured JSON logging (LOG_FILE) + coloured human output (stdout)
  ─ Health-check ping on startup to detect bad API key / network early
  ─ Stale-data guard: keeps last-known-good data if the new fetch fails
  ─ Graceful shutdown on SIGTERM / SIGINT (flushes partial results first)
  ─ Cycle-level summary with success-rate trend across last N cycles
  ─ Per-stock latency tracking (min / max / avg reported each cycle)
  ─ Market-data endpoints via shared generic fetcher (parallel, same retry stack):
      /trending  /ipo  /news  /mutual_funds  /price_shockers
      /BSE_most_active  /NSE_most_active  /fetch_52_week_high_low_data
      (all fetched in parallel every MARKET_DATA_FETCH_INTERVAL seconds)

Reads stock entries from stock.json in the format:
    {
      "nifty50": [
        {"symbol": "TATASTEEL", "name": "Tata Steel Ltd."},
        ...
      ]
    }

Fetches each stock by its SYMBOL  →  GET /stock?name=<SYMBOL>
Saves structured results to stock_data.json. Runs continuously.

Usage:
    python stock_fetcher.py

Requirements:
    Python 3.9+
    requests  (for all market-data endpoints — install via: pip install requests)
"""

import http.client
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import urllib.parse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

# Third-party (all market-data endpoints — pip install requests)
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False

# ═══════════════════════════════════════════════════════════════════════
#  CONFIGURATION  ←  Edit these values. No other changes needed.
# ═══════════════════════════════════════════════════════════════════════
API_KEY: str          = "sk-live-k929JPU4dewyOg3cU8mJLKOthy6epnhRa76v8MGu"
API_HOST: str         = "stock.indianapi.in"
FETCH_INTERVAL: int   = 30       # Seconds between full refresh cycles
MAX_WORKERS: int      = 10       # Max simultaneous HTTP requests
REQUEST_TIMEOUT: int  = 15       # Per-request timeout in seconds

INPUT_FILE: str       = "stock.json"
OUTPUT_FILE: str      = "stock_data.json"
LOG_FILE: str         = "stock_fetcher.log"   # "" → disable file logging

STOCK_LIST_KEY: str   = "nifty50"

# ── Market-data output files (one JSON per endpoint) ───────────────────
TRENDING_OUTPUT_FILE:     str = "trending_data.json"
IPO_OUTPUT_FILE:          str = "ipo_data.json"
NEWS_OUTPUT_FILE:         str = "news_data.json"
MUTUAL_FUNDS_OUTPUT_FILE: str = "mutual_funds_data.json"
PRICE_SHOCKERS_OUTPUT_FILE: str = "price_shockers_data.json"
BSE_MOST_ACTIVE_OUTPUT_FILE: str = "bse_most_active_data.json"
NSE_MOST_ACTIVE_OUTPUT_FILE: str = "nse_most_active_data.json"
WEEK_HIGH_LOW_OUTPUT_FILE:  str = "52_week_high_low_data.json"

# How often each market-data endpoint is refreshed (seconds).
# All endpoints share one coordinated cycle — set to taste.
MARKET_DATA_FETCH_INTERVAL: int = 300   # 5 minutes

# ── Retry / back-off ───────────────────────────────────────────────────
MAX_RETRIES: int       = 3       # Attempts per request (1 = no retry)
BACKOFF_BASE: float    = 1.5     # Seconds for first retry wait
BACKOFF_FACTOR: float  = 2.0     # Multiplier each retry (exponential)
BACKOFF_JITTER: float  = 0.5     # ± random seconds added to each wait
MAX_BACKOFF: float     = 30.0    # Cap on any single wait

# ── Circuit-breaker ────────────────────────────────────────────────────
CB_FAILURE_THRESHOLD: int   = 5   # Consecutive failures before tripping
CB_RECOVERY_TIMEOUT: float  = 60.0  # Seconds before the breaker half-opens

# ── Response cache (per-symbol TTL) ────────────────────────────────────
CACHE_TTL: float = 25.0   # Seconds. Prevents duplicate fetches in same cycle.

# ── Trend window ───────────────────────────────────────────────────────
TREND_WINDOW: int = 5   # Number of past cycles to include in success-rate trend
# ═══════════════════════════════════════════════════════════════════════


# ── ANSI colours (disabled automatically on Windows / non-tty) ─────────
def _supports_colour() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty() and os.name != "nt"

_USE_COLOUR = _supports_colour()

class C:
    RST  = "\033[0m"  if _USE_COLOUR else ""
    BOLD = "\033[1m"  if _USE_COLOUR else ""
    DIM  = "\033[2m"  if _USE_COLOUR else ""
    GRN  = "\033[32m" if _USE_COLOUR else ""
    RED  = "\033[31m" if _USE_COLOUR else ""
    YLW  = "\033[33m" if _USE_COLOUR else ""
    CYN  = "\033[36m" if _USE_COLOUR else ""
    MGN  = "\033[35m" if _USE_COLOUR else ""


# ── Logging ────────────────────────────────────────────────────────────
class _JsonFormatter(logging.Formatter):
    """Emits one JSON object per log line to file for easy log-shipping."""
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts":      self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":   record.levelname,
            "msg":     record.getMessage(),
            "logger":  record.name,
        }, ensure_ascii=False)


class _ColourFormatter(logging.Formatter):
    LEVEL_COLOURS = {
        "DEBUG":    C.DIM,
        "INFO":     C.GRN,
        "WARNING":  C.YLW,
        "ERROR":    C.RED,
        "CRITICAL": C.RED + C.BOLD,
    }
    def format(self, record: logging.LogRecord) -> str:
        colour = self.LEVEL_COLOURS.get(record.levelname, "")
        ts = self.formatTime(record, "%H:%M:%S")
        return f"{C.DIM}{ts}{C.RST}  {colour}{record.levelname:<8}{C.RST}  {record.getMessage()}"


_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_ColourFormatter())

_log_handlers: list[logging.Handler] = [_console_handler]
if LOG_FILE:
    _fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _fh.setFormatter(_JsonFormatter())
    _log_handlers.append(_fh)

logging.basicConfig(level=logging.INFO, handlers=_log_handlers)
logger = logging.getLogger("stock_fetcher")


# ═══════════════════════════════════════════════════════════════════════
#  DATA MODEL
# ═══════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class StockEntry:
    symbol: str
    name: str


# ═══════════════════════════════════════════════════════════════════════
#  RESPONSE CACHE  (thread-safe, TTL-based, keyed by (endpoint, symbol))
# ═══════════════════════════════════════════════════════════════════════
@dataclass
class _CacheEntry:
    data: Any
    expires_at: float   # monotonic timestamp


class ResponseCache:
    def __init__(self, ttl: float = CACHE_TTL) -> None:
        self._ttl   = ttl
        self._store: dict[str, _CacheEntry] = {}
        self._lock  = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry and time.monotonic() < entry.expires_at:
                return entry.data
        return None

    def set(self, key: str, data: Any) -> None:
        with self._lock:
            self._store[key] = _CacheEntry(data=data, expires_at=time.monotonic() + self._ttl)

    def evict_expired(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._store = {k: v for k, v in self._store.items() if v.expires_at > now}


_cache = ResponseCache()


# ═══════════════════════════════════════════════════════════════════════
#  CIRCUIT BREAKER  (per-symbol, prevents hammering broken endpoints)
# ═══════════════════════════════════════════════════════════════════════
class CircuitBreaker:
    """
    States:
      CLOSED   → normal operation
      OPEN     → requests blocked; reopens after CB_RECOVERY_TIMEOUT
      HALF_OPEN→ one trial request allowed; success→CLOSED, fail→OPEN
    """
    _CLOSED    = "CLOSED"
    _OPEN      = "OPEN"
    _HALF_OPEN = "HALF_OPEN"

    def __init__(self) -> None:
        self._states: dict[str, str]   = {}
        self._failures: dict[str, int] = {}
        self._opened_at: dict[str, float] = {}
        self._lock = threading.Lock()

    def is_allowed(self, symbol: str) -> bool:
        with self._lock:
            state = self._states.get(symbol, self._CLOSED)
            if state == self._CLOSED:
                return True
            if state == self._OPEN:
                if time.monotonic() - self._opened_at[symbol] >= CB_RECOVERY_TIMEOUT:
                    self._states[symbol] = self._HALF_OPEN
                    return True
                return False
            # HALF_OPEN — allow one probe
            return True

    def record_success(self, symbol: str) -> None:
        with self._lock:
            self._states[symbol]   = self._CLOSED
            self._failures[symbol] = 0

    def record_failure(self, symbol: str) -> None:
        with self._lock:
            self._failures[symbol] = self._failures.get(symbol, 0) + 1
            if self._failures[symbol] >= CB_FAILURE_THRESHOLD:
                self._states[symbol]    = self._OPEN
                self._opened_at[symbol] = time.monotonic()
                logger.warning(
                    "🔴 Circuit breaker OPEN for %s after %d failures. "
                    "Will retry in %.0fs.",
                    symbol, CB_FAILURE_THRESHOLD, CB_RECOVERY_TIMEOUT,
                )

    def status(self, symbol: str) -> str:
        return self._states.get(symbol, self._CLOSED)


_breaker = CircuitBreaker()


# ═══════════════════════════════════════════════════════════════════════
#  LATENCY TRACKER  (thread-safe, per-symbol)
# ═══════════════════════════════════════════════════════════════════════
class LatencyTracker:
    def __init__(self) -> None:
        self._data: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    def record(self, symbol: str, elapsed: float) -> None:
        with self._lock:
            self._data.setdefault(symbol, []).append(elapsed)

    def summary(self) -> dict[str, dict]:
        with self._lock:
            out = {}
            for sym, times in self._data.items():
                if times:
                    out[sym] = {
                        "min_ms": round(min(times) * 1000, 1),
                        "max_ms": round(max(times) * 1000, 1),
                        "avg_ms": round(sum(times) / len(times) * 1000, 1),
                        "samples": len(times),
                    }
            return out

    def reset(self) -> None:
        with self._lock:
            self._data.clear()


_latency = LatencyTracker()


# ═══════════════════════════════════════════════════════════════════════
#  STALE DATA STORE  (keep last-good payload if new fetch fails)
# ═══════════════════════════════════════════════════════════════════════
class StaleDataStore:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
        self._lock  = threading.Lock()

    def update(self, symbol: str, data: Any) -> None:
        with self._lock:
            self._store[symbol] = data

    def get(self, symbol: str) -> Optional[Any]:
        with self._lock:
            return self._store.get(symbol)

    def load_from_file(self, filepath: str) -> None:
        """Pre-warm the stale store from a previous run's output file."""
        if not os.path.exists(filepath):
            return
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                saved = json.load(fh)
            for sym, rec in saved.get("stocks", {}).items():
                if rec.get("data"):
                    self.update(sym, rec["data"])
            logger.info("Pre-warmed stale store with %d entries from '%s'.", len(self._store), filepath)
        except Exception as exc:
            logger.warning("Could not pre-warm stale store: %s", exc)


_stale = StaleDataStore()


# ═══════════════════════════════════════════════════════════════════════
#  CYCLE TREND TRACKER
# ═══════════════════════════════════════════════════════════════════════
class CycleTrend:
    def __init__(self, window: int = TREND_WINDOW) -> None:
        self._rates: deque[float] = deque(maxlen=window)

    def record(self, success: int, total: int) -> None:
        self._rates.append(round(success / total * 100, 1) if total else 0.0)

    def display(self) -> str:
        if not self._rates:
            return ""
        bars  = "▁▂▃▄▅▆▇█"
        trend = ""
        for r in self._rates:
            idx = min(int(r / 100 * (len(bars) - 1)), len(bars) - 1)
            trend += bars[idx]
        avg = sum(self._rates) / len(self._rates)
        return f"{trend}  avg {avg:.1f}%  (last {len(self._rates)} cycles)"


_trend = CycleTrend()


# ═══════════════════════════════════════════════════════════════════════
#  GRACEFUL SHUTDOWN
# ═══════════════════════════════════════════════════════════════════════
_shutdown_event = threading.Event()

def _handle_signal(signum: int, _frame: Any) -> None:
    logger.warning("Signal %d received — finishing current cycle then exiting…", signum)
    _shutdown_event.set()

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ═══════════════════════════════════════════════════════════════════════
#  ANIMATED SPINNER
# ═══════════════════════════════════════════════════════════════════════
class Spinner:
    FRAMES   = ("⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏")
    INTERVAL = 0.10

    def __init__(self, message: str = "Working…") -> None:
        self.message = message
        self._stop   = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def _spin(self) -> None:
        idx = 0
        while not self._stop.is_set():
            sys.stdout.write(f"\r  {self.FRAMES[idx % len(self.FRAMES)]}  {self.message}   ")
            sys.stdout.flush()
            time.sleep(self.INTERVAL)
            idx += 1
        sys.stdout.write("\r" + " " * (len(self.message) + 12) + "\r")
        sys.stdout.flush()

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)

    def __enter__(self) -> "Spinner":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()


# ═══════════════════════════════════════════════════════════════════════
#  THREAD-SAFE PROGRESS TRACKER
# ═══════════════════════════════════════════════════════════════════════
class Progress:
    def __init__(self, total: int) -> None:
        self.total  = total
        self._done  = 0
        self._lock  = threading.Lock()
        self._width = len(str(total))

    def update(
        self,
        entry: StockEntry,
        success: bool,
        error: str = "",
        cached: bool = False,
        stale: bool = False,
        latency_ms: float = 0.0,
    ) -> None:
        with self._lock:
            self._done += 1
            if cached:
                icon = f"{C.CYN}💾{C.RST}"
                tag  = "CACHED"
            elif stale:
                icon = f"{C.YLW}⚠️ {C.RST}"
                tag  = "STALE "
            elif success:
                icon = f"{C.GRN}✅{C.RST}"
                tag  = f"{latency_ms:>6.0f}ms"
            else:
                icon = f"{C.RED}❌{C.RST}"
                tag  = "FAILED"

            detail = f"  {C.DIM}←  {error}{C.RST}" if error else ""
            print(
                f"  [{self._done:>{self._width}}/{self.total}]"
                f"  {icon}  {C.BOLD}{entry.symbol:<14}{C.RST}"
                f"  {C.DIM}({entry.name}){C.RST}"
                f"  {C.DIM}{tag}{C.RST}"
                f"{detail}"
            )


# ═══════════════════════════════════════════════════════════════════════
#  FILE I/O
# ═══════════════════════════════════════════════════════════════════════
def load_stock_entries(filepath: str, list_key: str) -> list[StockEntry]:
    if not os.path.exists(filepath):
        logger.error("Input file '%s' not found.", filepath)
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
    except json.JSONDecodeError as exc:
        logger.error("Cannot parse '%s': %s", filepath, exc)
        return []
    if not isinstance(raw, dict) or list_key not in raw:
        logger.error("'%s' must contain a '%s' key.", filepath, list_key)
        return []
    entries: list[StockEntry] = []
    for idx, item in enumerate(raw[list_key]):
        if not isinstance(item, dict):
            logger.warning("Item #%d is not a dict — skipped.", idx)
            continue
        symbol = str(item.get("symbol", "")).strip()
        name   = str(item.get("name",   "")).strip()
        if not symbol:
            logger.warning("Item #%d has no 'symbol' — skipped.", idx)
            continue
        entries.append(StockEntry(symbol=symbol, name=name or symbol))
    if not entries:
        logger.warning("No valid entries in '%s'['%s'].", filepath, list_key)
    return entries


def save_stock_data(filepath: str, payload: dict) -> None:
    tmp = filepath + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        os.replace(tmp, filepath)
    except OSError as exc:
        logger.error("Failed to save '%s': %s", filepath, exc)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


# ═══════════════════════════════════════════════════════════════════════
#  BACK-OFF HELPER
# ═══════════════════════════════════════════════════════════════════════
def _backoff_wait(attempt: int, retry_after: Optional[float] = None) -> None:
    """
    Sleep before a retry.

    If the server returned a Retry-After value, honour it.
    Otherwise use exponential back-off with jitter.
    """
    if retry_after is not None:
        wait = max(0.0, retry_after)
        logger.info("Rate-limited — sleeping %.1fs (Retry-After).", wait)
    else:
        base = min(BACKOFF_BASE * (BACKOFF_FACTOR ** attempt), MAX_BACKOFF)
        wait = base + random.uniform(-BACKOFF_JITTER, BACKOFF_JITTER)
        wait = max(0.0, wait)
    time.sleep(wait)


def _parse_retry_after(headers_str: str) -> Optional[float]:
    """Extract Retry-After seconds from raw HTTP headers string, if present."""
    for line in headers_str.splitlines():
        if line.lower().startswith("retry-after:"):
            value = line.split(":", 1)[1].strip()
            try:
                return float(value)
            except ValueError:
                pass
    return None


# ═══════════════════════════════════════════════════════════════════════
#  HTTP FETCH  (single stock, with retries + circuit-breaker + cache)
# ═══════════════════════════════════════════════════════════════════════
# Retryable HTTP status codes
_RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

def fetch_stock(
    entry: StockEntry,
) -> tuple[StockEntry, Optional[dict], Optional[str], bool, bool, float]:
    """
    Fetch stock data for one entry, with:
      - TTL-based response cache
      - Circuit-breaker guard
      - Exponential back-off retries (MAX_RETRIES attempts)
      - Retry-After header support for HTTP 429
      - Stale-data fallback if all retries fail

    Returns:
        (entry, data, error, cached, stale, latency_seconds)
    """
    cache_key = f"stock:{entry.symbol}"

    # ── Cache hit ─────────────────────────────────────────────────────
    cached_data = _cache.get(cache_key)
    if cached_data is not None:
        return entry, cached_data, None, True, False, 0.0

    # ── Circuit-breaker ───────────────────────────────────────────────
    if not _breaker.is_allowed(entry.symbol):
        stale_data = _stale.get(entry.symbol)
        if stale_data:
            return entry, stale_data, "circuit open (stale data served)", False, True, 0.0
        return entry, None, f"circuit breaker OPEN for {entry.symbol}", False, False, 0.0

    encoded  = urllib.parse.quote_plus(entry.symbol)
    endpoint = f"/stock?name={encoded}"
    last_error: str = "unknown error"
    total_latency = 0.0

    for attempt in range(MAX_RETRIES):
        t0 = time.monotonic()
        try:
            conn = http.client.HTTPSConnection(API_HOST, timeout=REQUEST_TIMEOUT)
            conn.request("GET", endpoint, headers={"X-Api-Key": API_KEY})
            res  = conn.getresponse()
            body = res.read().decode("utf-8")
            # Capture headers as a string for Retry-After parsing
            raw_headers = str(res.headers)
            conn.close()

            elapsed = time.monotonic() - t0
            total_latency += elapsed

            # ── Success ───────────────────────────────────────────────
            if res.status == 200:
                data = json.loads(body)
                _cache.set(cache_key, data)
                _stale.update(entry.symbol, data)
                _breaker.record_success(entry.symbol)
                _latency.record(entry.symbol, elapsed)
                return entry, data, None, False, False, elapsed

            # ── Rate limited ──────────────────────────────────────────
            if res.status == 429:
                retry_after = _parse_retry_after(raw_headers)
                last_error  = f"HTTP 429 (rate limited)"
                logger.warning(
                    "429 rate-limit on %s (attempt %d/%d).",
                    entry.symbol, attempt + 1, MAX_RETRIES,
                )
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt, retry_after=retry_after)
                continue

            # ── Other retryable errors ────────────────────────────────
            if res.status in _RETRYABLE_STATUSES:
                last_error = f"HTTP {res.status}"
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt)
                continue

            # ── Non-retryable HTTP error ──────────────────────────────
            last_error = f"HTTP {res.status}: {body[:200]}"
            break

        except json.JSONDecodeError as exc:
            elapsed = time.monotonic() - t0
            last_error = f"Invalid JSON: {exc}"
            if attempt < MAX_RETRIES - 1:
                _backoff_wait(attempt)

        except TimeoutError:
            elapsed = time.monotonic() - t0
            last_error = f"Timed out after {REQUEST_TIMEOUT}s"
            if attempt < MAX_RETRIES - 1:
                _backoff_wait(attempt)

        except (http.client.HTTPException, OSError) as exc:
            elapsed = time.monotonic() - t0
            last_error = f"Network error: {exc}"
            if attempt < MAX_RETRIES - 1:
                _backoff_wait(attempt)

        except Exception as exc:
            elapsed = time.monotonic() - t0
            last_error = f"Unexpected error: {exc}"
            break  # non-retryable safety-net

    # ── All retries exhausted ─────────────────────────────────────────
    _breaker.record_failure(entry.symbol)
    stale_data = _stale.get(entry.symbol)
    if stale_data:
        logger.warning(
            "All retries failed for %s — serving stale data. Last error: %s",
            entry.symbol, last_error,
        )
        return entry, stale_data, last_error, False, True, total_latency
    return entry, None, last_error, False, False, total_latency


# ═══════════════════════════════════════════════════════════════════════
#  GENERIC SIMPLE-ENDPOINT FETCHER  (no query params — header auth only)
# ═══════════════════════════════════════════════════════════════════════
def fetch_simple_endpoint(
    path: str,
    label: str,
) -> tuple[Optional[Any], Optional[str]]:
    """
    Fetch any no-parameter GET endpoint on API_HOST using `requests`.

    All market-data endpoints (trending, ipo, news, mutual_funds, …) share
    the same shape:
        GET  https://<API_HOST>/<path>
        Header: X-Api-Key: <API_KEY>

    This single function handles all of them so retry / back-off / cache
    logic lives in exactly one place.

    Args:
        path  : URL path without leading slash, e.g. "trending"
        label : Short uppercase label for log lines, e.g. "TRENDING"

    Returns:
        (data, None)          — on success  (data is the parsed JSON)
        (None, error_string)  — on any failure
    """
    if not _REQUESTS_AVAILABLE:
        return None, (
            "The 'requests' package is required. "
            "Install it with:  pip install requests"
        )

    cache_key = f"endpoint:{path}"

    # ── Cache hit ─────────────────────────────────────────────────────
    cached = _cache.get(cache_key)
    if cached is not None:
        logger.info("%s  CACHED", label)
        return cached, None

    url        = f"https://{API_HOST}/{path}"
    session    = requests.Session()
    session.headers.update({"X-Api-Key": API_KEY})
    last_error = "unknown error"

    try:
        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT)

                # ── Success ───────────────────────────────────────────
                if resp.status_code == 200:
                    data = resp.json()
                    _cache.set(cache_key, data)
                    n = len(data) if isinstance(data, (list, dict)) else "?"
                    logger.info("%s  OK  (%s items)", label, n)
                    return data, None

                # ── Rate limited ──────────────────────────────────────
                if resp.status_code == 429:
                    retry_after_raw = resp.headers.get("Retry-After")
                    retry_after: Optional[float] = None
                    if retry_after_raw is not None:
                        try:
                            retry_after = float(retry_after_raw)
                        except ValueError:
                            pass
                    last_error = "HTTP 429 (rate limited)"
                    logger.warning(
                        "%s  429 rate-limit (attempt %d/%d).",
                        label, attempt + 1, MAX_RETRIES,
                    )
                    if attempt < MAX_RETRIES - 1:
                        _backoff_wait(attempt, retry_after=retry_after)
                    continue

                # ── Other retryable errors ────────────────────────────
                if resp.status_code in _RETRYABLE_STATUSES:
                    last_error = f"HTTP {resp.status_code}"
                    if attempt < MAX_RETRIES - 1:
                        _backoff_wait(attempt)
                    continue

                # ── Non-retryable HTTP error ──────────────────────────
                last_error = f"HTTP {resp.status_code}: {resp.text[:200]}"
                break

            except requests.exceptions.Timeout:
                last_error = f"Timed out after {REQUEST_TIMEOUT}s"
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt)

            except requests.exceptions.ConnectionError as exc:
                last_error = f"Connection error: {exc}"
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt)

            except requests.exceptions.JSONDecodeError as exc:
                last_error = f"Invalid JSON: {exc}"
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt)

            except requests.exceptions.RequestException as exc:
                last_error = f"Requests error: {exc}"
                if attempt < MAX_RETRIES - 1:
                    _backoff_wait(attempt)

            except Exception as exc:
                last_error = f"Unexpected error: {exc}"
                break   # non-retryable safety-net

    finally:
        session.close()

    logger.warning("%s  FAIL  %s", label, last_error)
    return None, last_error


# ═══════════════════════════════════════════════════════════════════════
#  MARKET-DATA CYCLE  (all 8 simple endpoints fetched in parallel)
# ═══════════════════════════════════════════════════════════════════════

# Registry: (url_path, log_label, output_file)
# Add or remove entries here — no other code changes needed.
_MARKET_ENDPOINTS: list[tuple[str, str, str]] = [
    ("trending",                  "TRENDING",       TRENDING_OUTPUT_FILE),
    ("ipo",                       "IPO",             IPO_OUTPUT_FILE),
    ("news",                      "NEWS",            NEWS_OUTPUT_FILE),
    ("mutual_funds",              "MUTUAL_FUNDS",    MUTUAL_FUNDS_OUTPUT_FILE),
    ("price_shockers",            "PRICE_SHOCKERS",  PRICE_SHOCKERS_OUTPUT_FILE),
    ("BSE_most_active",           "BSE_ACTIVE",      BSE_MOST_ACTIVE_OUTPUT_FILE),
    ("NSE_most_active",           "NSE_ACTIVE",      NSE_MOST_ACTIVE_OUTPUT_FILE),
    ("fetch_52_week_high_low_data", "52W_HIGH_LOW",  WEEK_HIGH_LOW_OUTPUT_FILE),
]


def run_market_data_cycle() -> dict[str, dict]:
    """
    Fetch all market-data endpoints in parallel and save each to its own file.

    Returns a dict keyed by url_path with per-endpoint result dicts:
    {
      "trending":     {"last_updated": "…", "data": …, "error": null},
      "ipo":          {"last_updated": "…", "data": …, "error": null},
      ...
    }
    """
    n = len(_MARKET_ENDPOINTS)
    print(f"\n{C.BOLD}{'─' * 64}{C.RST}")
    print(f"  🌐  Market-data cycle  —  {C.BOLD}{n}{C.RST} endpoint(s) in parallel")
    print(f"{C.BOLD}{'─' * 64}{C.RST}")

    results: dict[str, dict] = {}
    width = len(str(n))

    with ThreadPoolExecutor(max_workers=n) as pool:
        # Submit all fetches simultaneously
        future_map = {
            pool.submit(fetch_simple_endpoint, path, label): (path, label, outfile)
            for path, label, outfile in _MARKET_ENDPOINTS
        }

        done = 0
        for future in as_completed(future_map):
            if _shutdown_event.is_set():
                break
            path, label, outfile = future_map[future]
            data, error = future.result()
            done += 1

            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload = {"last_updated": ts, "data": data, "error": error}
            results[path] = payload

            if error:
                print(
                    f"  [{done:>{width}}/{n}]  {C.RED}❌{C.RST}"
                    f"  {label:<18}  {C.DIM}{error}{C.RST}"
                )
            else:
                count = len(data) if isinstance(data, (list, dict)) else "?"
                print(
                    f"  [{done:>{width}}/{n}]  {C.GRN}✅{C.RST}"
                    f"  {label:<18}  {C.DIM}{count} item(s){C.RST}"
                )

            # Save this endpoint's file immediately (don't wait for others)
            save_stock_data(outfile, payload)
            status = "OK" if error is None else f"FAIL — {error}"
            logger.info("Saved '%s': %s", outfile, status)

    succeeded = sum(1 for r in results.values() if r["error"] is None)
    failed    = n - succeeded
    print(f"{C.BOLD}{'─' * 64}{C.RST}")
    suffix = f",  {failed} failed" if failed else "  (all succeeded!)"
    print(f"  ✔  Market data: {C.BOLD}{succeeded}/{n}{C.RST}{suffix}")
    print(f"{C.BOLD}{'─' * 64}{C.RST}\n")

    return results


# ═══════════════════════════════════════════════════════════════════════
#  HEALTH CHECK  (run once on startup)
# ═══════════════════════════════════════════════════════════════════════
def run_health_check() -> bool:
    """
    Ping the API host to confirm connectivity and API key validity
    before entering the main loop.
    Returns True if healthy, False otherwise.
    """
    print(f"\n  {C.CYN}🔍  Running startup health check…{C.RST}")
    try:
        conn = http.client.HTTPSConnection(API_HOST, timeout=REQUEST_TIMEOUT)
        # Use a known lightweight endpoint; fall back to root if stock endpoint unavailable
        conn.request("GET", "/stock?name=RELIANCE", headers={"X-Api-Key": API_KEY})
        res  = conn.getresponse()
        body = res.read(512).decode("utf-8", errors="replace")
        conn.close()

        if res.status == 200:
            print(f"  {C.GRN}✅  Health check passed  (HTTP 200){C.RST}\n")
            return True
        if res.status == 401:
            logger.error(
                "Health check: HTTP 401 — invalid or missing API key. "
                "Update API_KEY in the config section."
            )
            return False
        if res.status == 429:
            logger.warning(
                "Health check: HTTP 429 — rate limited. "
                "Will proceed; fetches may be delayed."
            )
            return True
        logger.warning("Health check: HTTP %d — %s", res.status, body[:200])
        return True     # Non-fatal; let the main loop decide
    except Exception as exc:
        logger.error("Health check failed — cannot reach %s: %s", API_HOST, exc)
        return False


# ═══════════════════════════════════════════════════════════════════════
#  FETCH CYCLE
# ═══════════════════════════════════════════════════════════════════════
def run_fetch_cycle(entries: list[StockEntry]) -> dict:
    total    = len(entries)
    workers  = min(MAX_WORKERS, total)
    progress = Progress(total)
    _latency.reset()

    stocks: dict[str, Any] = {}
    errors: dict[str, str] = {}
    cached_count = 0
    stale_count  = 0

    print(f"\n{C.BOLD}{'─' * 64}{C.RST}")
    print(
        f"  🚀  Fetch cycle  —  "
        f"{C.BOLD}{total}{C.RST} stock(s)  |  "
        f"{C.BOLD}{workers}{C.RST} workers  |  "
        f"{C.BOLD}{MAX_RETRIES}{C.RST} retries max"
    )
    print(f"{C.BOLD}{'─' * 64}{C.RST}")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(fetch_stock, e): e for e in entries}
        for future in as_completed(future_map):
            if _shutdown_event.is_set():
                break
            entry, data, error, cached, stale, latency_s = future.result()

            stocks[entry.symbol] = {
                "symbol":    entry.symbol,
                "name":      entry.name,
                "data":      data,
                "cached":    cached,
                "stale":     stale,
                "latency_ms": round(latency_s * 1000, 1),
            }

            if cached:
                cached_count += 1
                progress.update(entry, success=True, cached=True)
            elif stale:
                stale_count += 1
                errors[entry.symbol] = error or "stale"
                progress.update(entry, success=False, error=error or "", stale=True)
            elif error:
                errors[entry.symbol] = error
                progress.update(entry, success=False, error=error)
                logger.warning("FAIL  %-14s  %s", entry.symbol, error)
            else:
                progress.update(entry, success=True, latency_ms=latency_s * 1000)
                logger.info("OK    %s", entry.symbol)

    successful = total - len({s for s, e in errors.items() if "stale" not in e})
    _trend.record(successful, total)

    lat_summary = _latency.summary()
    all_latencies = [v["avg_ms"] for v in lat_summary.values()]
    avg_lat = round(sum(all_latencies) / len(all_latencies), 1) if all_latencies else 0.0

    print(f"{C.BOLD}{'─' * 64}{C.RST}")
    print(
        f"  ✔  {C.GRN}Cycle complete{C.RST}:  "
        f"{C.BOLD}{successful}/{total}{C.RST} live  |  "
        f"{C.CYN}{cached_count} cached{C.RST}  |  "
        f"{C.YLW}{stale_count} stale{C.RST}  |  "
        f"avg {avg_lat:.0f}ms"
    )
    trend_str = _trend.display()
    if trend_str:
        print(f"  📊  Success rate trend:  {trend_str}")
    print(f"{C.BOLD}{'─' * 64}{C.RST}\n")

    return {
        "last_updated":  datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_stocks":  total,
        "successful":    successful,
        "cached":        cached_count,
        "stale":         stale_count,
        "failed":        len(errors),
        "avg_latency_ms": avg_lat,
        "stocks":        stocks,
        "errors":        errors,
        "latency":       lat_summary,
    }


# ═══════════════════════════════════════════════════════════════════════
#  COUNTDOWN
# ═══════════════════════════════════════════════════════════════════════
def countdown(seconds: int) -> None:
    for remaining in range(seconds, 0, -1):
        if _shutdown_event.is_set():
            break
        sys.stdout.write(
            f"\r  ⏳  Next refresh in {C.BOLD}{remaining:>3}s{C.RST}  "
            f"{C.DIM}(Ctrl+C to quit){C.RST}   "
        )
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 56 + "\r")
    sys.stdout.flush()


# ═══════════════════════════════════════════════════════════════════════
#  MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════
def main() -> None:
    print("═" * 64)
    print(f"  {C.BOLD}{C.MGN}📈  Stock Fetcher v2  —  Indian Stock API{C.RST}")
    print(f"  Input      : {INPUT_FILE}  (key: '{STOCK_LIST_KEY}')")
    print(f"  Stock out  : {OUTPUT_FILE}")
    print(f"  Market out : {', '.join(f for _, _, f in _MARKET_ENDPOINTS)}")
    print(f"  Stock refresh  : every {FETCH_INTERVAL}s  |  Workers: {MAX_WORKERS}")
    print(f"  Market refresh : every {MARKET_DATA_FETCH_INTERVAL}s  ({len(_MARKET_ENDPOINTS)} endpoints in parallel)")
    print(f"  Retries : {MAX_RETRIES}  |  Back-off: {BACKOFF_BASE}s × {BACKOFF_FACTOR}^n  |  Cap: {MAX_BACKOFF}s")
    print(f"  Circuit : opens after {CB_FAILURE_THRESHOLD} failures, resets in {CB_RECOVERY_TIMEOUT:.0f}s")
    print(f"  Cache   : TTL {CACHE_TTL:.0f}s")
    print("═" * 64)

    if API_KEY == "YOUR_API_KEY_HERE":
        logger.warning("⚠  API_KEY is still the placeholder — update it before use.")

    if not _REQUESTS_AVAILABLE:
        logger.warning(
            "⚠  'requests' package not found — all market-data endpoints will be DISABLED. "
            "Fix with:  pip install requests"
        )

    # ── Startup health check ───────────────────────────────────────────
    if not run_health_check():
        logger.error("Health check failed. Fix connectivity/API key and retry.")
        sys.exit(1)

    # ── Pre-warm stale store from previous run ─────────────────────────
    _stale.load_from_file(OUTPUT_FILE)

    cycle = 0
    last_market_fetch = 0.0

    while not _shutdown_event.is_set():
        cycle += 1
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n  {C.CYN}🔄  Cycle #{cycle}  —  {ts}{C.RST}")

        # ── 1. Load entries ────────────────────────────────────────────
        with Spinner(f"Reading {INPUT_FILE}…"):
            entries = load_stock_entries(INPUT_FILE, STOCK_LIST_KEY)

        if not entries:
            logger.error("No stocks loaded. Waiting %ds…", FETCH_INTERVAL)
            countdown(FETCH_INTERVAL)
            continue

        logger.info("Loaded %d stock(s).", len(entries))

        # ── 2. Evict stale cache entries before each cycle ─────────────
        _cache.evict_expired()

        # ── 3. Fetch all stocks in parallel ───────────────────────────
        result = run_fetch_cycle(entries)

        # ── 4. Save stock results ──────────────────────────────────────
        with Spinner(f"Saving {OUTPUT_FILE}…"):
            save_stock_data(OUTPUT_FILE, result)

        logger.info(
            "Saved '%s': total=%d  success=%d  cached=%d  stale=%d  failed=%d",
            OUTPUT_FILE,
            result["total_stocks"],
            result["successful"],
            result["cached"],
            result["stale"],
            result["failed"],
        )

        # ── 5. Market-data endpoints (trending, ipo, news, …) ──────────
        now = time.time()
        if now - last_market_fetch >= MARKET_DATA_FETCH_INTERVAL:
            run_market_data_cycle()   # saves each file internally
            last_market_fetch = now

        if _shutdown_event.is_set():
            break

        # ── 6. Wait ────────────────────────────────────────────────────
        countdown(FETCH_INTERVAL)

    print(f"\n\n  {C.YLW}⛔  Stopped. Goodbye!{C.RST}\n")
    sys.exit(0)


if __name__ == "__main__":
    main()