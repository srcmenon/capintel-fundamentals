"""
CapIntel Fundamentals Server — Oracle VM
========================================
FastAPI + yfinance + SQLite

Architecture:
  - SQLite persistent cache (7-day TTL, survives restarts)
  - Sequential fetching with 2s delay (avoids single-IP rate limits)
  - Stale-on-error fallback (429 → return old data, never null if cached)
  - Background weekly refresh (Sunday 2am IST, all tickers pre-warmed)
  - indianapi.in as secondary source for Indian stocks (optional)

Run:  uvicorn main:app --host 0.0.0.0 --port 3001
PM2:  pm2 start "uvicorn main:app --host 0.0.0.0 --port 3001" --name capintel-fundamentals
"""

import sqlite3
import json
import time
import logging
import os
from datetime import datetime, timezone
from typing import Optional
from contextlib import asynccontextmanager

import yfinance as yf
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────
DB_PATH        = os.path.join(os.path.dirname(__file__), "cache.db")
CACHE_TTL_DAYS = 7
FETCH_DELAY_S  = 2      # seconds between Yahoo requests
STALE_TTL_DAYS = 30     # serve stale data up to this age on error

# ── SQLite cache ─────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                ticker      TEXT PRIMARY KEY,
                data        TEXT NOT NULL,
                fetched_at  INTEGER NOT NULL,
                source      TEXT DEFAULT 'yahoo'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS refresh_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at      INTEGER NOT NULL,
                tickers     INTEGER,
                success     INTEGER,
                failed      INTEGER
            )
        """)
        conn.commit()
    log.info(f"[db] initialised at {DB_PATH}")

def cache_get(ticker: str, allow_stale=False) -> Optional[dict]:
    """Return cached data if fresh. If allow_stale, return even if expired."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT data, fetched_at FROM fundamentals WHERE ticker=?", (ticker,)
        ).fetchone()
    if not row:
        return None
    age_days = (time.time() - row["fetched_at"]) / 86400
    if age_days <= CACHE_TTL_DAYS:
        data = json.loads(row["data"])
        data["fromCache"] = True
        data["cacheAgeDays"] = round(age_days, 1)
        return data
    if allow_stale and age_days <= STALE_TTL_DAYS:
        data = json.loads(row["data"])
        data["fromCache"] = True
        data["stale"] = True
        data["cacheAgeDays"] = round(age_days, 1)
        log.warning(f"[cache] {ticker} stale ({age_days:.0f}d) — returning anyway")
        return data
    return None

def cache_set(ticker: str, data: dict, source="yahoo"):
    """Write or update cache entry."""
    payload = {k: v for k, v in data.items()
               if k not in ("fromCache", "stale", "cacheAgeDays")}
    with get_db() as conn:
        conn.execute("""
            INSERT INTO fundamentals (ticker, data, fetched_at, source)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                data=excluded.data,
                fetched_at=excluded.fetched_at,
                source=excluded.source
        """, (ticker, json.dumps(payload), int(time.time()), source))
        conn.commit()

# ── yfinance fetch ────────────────────────────────────────────
def fetch_yahoo(ticker: str) -> Optional[dict]:
    """
    Fetch fundamentals via yfinance.
    yfinance manages its own session/cookies — no separate crumb needed.
    Returns None on any failure so caller can use stale cache.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info
        if not info or info.get("regularMarketPrice") is None and info.get("currentPrice") is None:
            log.warning(f"[yf] {ticker} — empty info response")
            return None

        def n(v):
            try:
                f = float(v)
                return f if (f == f and abs(f) < 1e15) else None  # filter NaN/Inf
            except (TypeError, ValueError):
                return None

        result = {
            "trailingPE":       n(info.get("trailingPE")),
            "priceToBook":      n(info.get("priceToBook")),
            "roe":              n(info.get("returnOnEquity")),
            "roa":              n(info.get("returnOnAssets")),
            "profitMargins":    n(info.get("profitMargins")),
            "operatingMargins": n(info.get("operatingMargins")),
            "grossMargins":     n(info.get("grossMargins")),
            "debtToEquity":     n(info.get("debtToEquity")),
            "currentRatio":     n(info.get("currentRatio")),
            "revenueGrowth":    n(info.get("revenueGrowth")),
            "earningsGrowth":   n(info.get("earningsGrowth")),
            "trailingEps":      n(info.get("trailingEps")),
            "forwardEps":       n(info.get("forwardEps")),
            "sector":           info.get("sector")   or None,
            "industry":         info.get("industry") or None,
            "marketCap":        n(info.get("marketCap")),
            "recommendationKey":       info.get("recommendationKey") or None,
            "numberOfAnalystOpinions": n(info.get("numberOfAnalystOpinions")),
            "targetMeanPrice":         n(info.get("targetMeanPrice")),
        }

        # At least PE or ROE must be present to be useful
        has_data = any(result[k] is not None for k in ("trailingPE", "roe", "priceToBook"))
        if not has_data:
            log.warning(f"[yf] {ticker} — all key fields null")
            return None

        log.info(f"[yf] {ticker} pe={result['trailingPE']} roe={result['roe']} pb={result['priceToBook']}")
        return result

    except Exception as e:
        msg = str(e)
        if "429" in msg or "Too Many Requests" in msg:
            log.error(f"[yf] {ticker} — 429 rate limited")
        else:
            log.error(f"[yf] {ticker} — {msg}")
        return None

# ── Fetch one ticker with full fallback logic ─────────────────
def fetch_with_fallback(ticker: str) -> Optional[dict]:
    """
    1. Try Yahoo (yfinance)
    2. On failure → return stale cache if available
    3. Return None only if no cache exists at all
    """
    fresh = cache_get(ticker, allow_stale=False)
    if fresh:
        return fresh

    result = fetch_yahoo(ticker)
    if result:
        cache_set(ticker, result, source="yahoo")
        result["fromCache"] = False
        return result

    # Yahoo failed — try stale cache
    stale = cache_get(ticker, allow_stale=True)
    if stale:
        return stale

    return None

# ── Background weekly refresh ─────────────────────────────────
_known_tickers: list[str] = []

def background_refresh():
    """
    Runs weekly (Sunday 2am IST).
    Sequentially refreshes all known tickers with FETCH_DELAY_S delay.
    Uses stale cache as fallback if Yahoo fails for any ticker.
    """
    if not _known_tickers:
        log.info("[refresh] no tickers registered yet — skipping")
        return

    log.info(f"[refresh] starting weekly refresh for {len(_known_tickers)} tickers")
    success, failed = 0, 0
    start = time.time()

    for ticker in _known_tickers:
        try:
            # Force bypass cache for refresh
            result = fetch_yahoo(ticker)
            if result:
                cache_set(ticker, result, source="yahoo")
                success += 1
            else:
                failed += 1
                log.warning(f"[refresh] {ticker} — failed, keeping existing cache")
        except Exception as e:
            failed += 1
            log.error(f"[refresh] {ticker} — {e}")
        time.sleep(FETCH_DELAY_S)

    elapsed = round(time.time() - start, 1)
    with get_db() as conn:
        conn.execute(
            "INSERT INTO refresh_log (run_at, tickers, success, failed) VALUES (?,?,?,?)",
            (int(time.time()), len(_known_tickers), success, failed)
        )
        conn.commit()
    log.info(f"[refresh] done — {success} ok, {failed} failed, {elapsed}s elapsed")

# ── App lifecycle ─────────────────────────────────────────────
scheduler = BackgroundScheduler(timezone="Asia/Kolkata")

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Weekly refresh: Sunday 2am IST
    scheduler.add_job(background_refresh, "cron", day_of_week="sun", hour=2, minute=0)
    scheduler.start()
    log.info("[scheduler] weekly refresh scheduled — Sunday 2:00 IST")
    yield
    scheduler.shutdown()

app = FastAPI(title="CapIntel Fundamentals", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

# ── Routes ────────────────────────────────────────────────────
@app.get("/health")
def health():
    with get_db() as conn:
        count = conn.execute("SELECT COUNT(*) FROM fundamentals").fetchone()[0]
        last_refresh = conn.execute(
            "SELECT run_at, success, failed FROM refresh_log ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "status": "ok",
        "cache": count,
        "lastRefresh": {
            "at": datetime.fromtimestamp(last_refresh["run_at"], tz=timezone.utc).isoformat()
                  if last_refresh else None,
            "success": last_refresh["success"] if last_refresh else None,
            "failed":  last_refresh["failed"]  if last_refresh else None,
        }
    }

class BatchRequest(BaseModel):
    tickers: list[str]

@app.post("/fundamentals/batch")
def fundamentals_batch(req: BatchRequest):
    global _known_tickers

    tickers = [t.strip() for t in req.tickers if t.strip()]
    if not tickers:
        return {"error": "tickers array required"}, 400

    # Register tickers for background refresh
    for t in tickers:
        if t not in _known_tickers:
            _known_tickers.append(t)

    results = {}
    needs_fetch = []

    # Pass 1: serve from cache immediately
    for ticker in tickers:
        cached = cache_get(ticker, allow_stale=False)
        if cached:
            results[ticker] = cached
        else:
            needs_fetch.append(ticker)

    # Pass 2: fetch missing tickers sequentially with delay
    for i, ticker in enumerate(needs_fetch):
        result = fetch_with_fallback(ticker)
        results[ticker] = result
        # Delay between fetches (skip delay after last one)
        if i < len(needs_fetch) - 1:
            time.sleep(FETCH_DELAY_S)

    return {
        "results": results,
        "computedAt": datetime.now(tz=timezone.utc).isoformat(),
        "cached": len(tickers) - len(needs_fetch),
        "fetched": len(needs_fetch),
    }

@app.post("/refresh/trigger")
def trigger_refresh():
    """Manual refresh trigger — useful after quarterly results."""
    import threading
    t = threading.Thread(target=background_refresh, daemon=True)
    t.start()
    return {"status": "refresh started", "tickers": len(_known_tickers)}

@app.get("/cache/status")
def cache_status():
    """Show cache freshness for all stored tickers."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ticker, fetched_at, source FROM fundamentals ORDER BY fetched_at DESC"
        ).fetchall()
    now = time.time()
    return {
        "entries": [
            {
                "ticker": r["ticker"],
                "ageDays": round((now - r["fetched_at"]) / 86400, 1),
                "fresh": (now - r["fetched_at"]) / 86400 <= CACHE_TTL_DAYS,
                "source": r["source"],
                "fetchedAt": datetime.fromtimestamp(r["fetched_at"], tz=timezone.utc).isoformat(),
            }
            for r in rows
        ]
    }
"""Entry point so PM2 can run: python3 -u main.py"""
import uvicorn
from main import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3001, log_level="info")
