#!/usr/bin/env python3
"""
Wolfe Wave CLOUD SCANNER — runs 24/7 on Koyeb free tier.
Scans all NSE F&O stocks during market hours.
Sends Telegram + ntfy.sh alerts on new Point 5 patterns.
"""

import os
import sys
import time
import json
import logging
import datetime
import threading
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# =====================================================================
# CREDENTIALS (set via Koyeb environment variables)
# =====================================================================
DHAN_CLIENT_ID = os.environ.get("DHAN_CLIENT_ID", "1104219000")
DHAN_ACCESS_TOKEN = os.environ.get("DHAN_ACCESS_TOKEN", "")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "8826992303:AAFTQZS35wSKlSj4Lf1r04Iwzik5nsLWMys")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "5366704688")

NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "wolfe-wave-alerts")

# =====================================================================
# LOGGING
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("wolfe")

# =====================================================================
# NSE F&O UNIVERSE
# =====================================================================
NSE_FO_STOCKS = [
    "ABB","ABBOTINDIA","ABCAPITAL","ABFRL","ACC",
    "ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS",
    "ALKEM","AMBUJACEM","ANGELONE","APLAPOLLO","APOLLOHOSP",
    "APOLLOTYRE","ASHOKLEY","ASIANPAINT","ASTRAL","ATUL",
    "AUBANK","AUROPHARMA","AXISBANK","BAJAJ-AUTO","BAJAJFINSV",
    "BAJFINANCE","BALKRISIND","BANDHANBNK","BANKBARODA","BEL",
    "BHARATFORG","BHEL","BIOCON","BPCL","BRITANNIA",
    "CANBK","CHOLAFIN","CIPLA","COALINDIA","COFORGE",
    "CONCOR","CROMPTON","CUMMINSIND","DABUR","DLF",
    "DRREDDY","EICHERMOT","GAIL","GLENMARK","GMRAIRPORT",
    "GODREJCP","GRASIM","HAL","HAVELLS","HCLTECH",
    "HDFCBANK","HDFCLIFE","HEROMOTOCO","HINDALCO","HINDUNILVR",
    "ICICIBANK","ICICIGI","ICICIPRULI","IDEA","IDFCFIRSTB",
    "IEX","IGL","INDHOTEL","INDIGO","INDUSINDBK",
    "INDUSTOWER","INFY","IOC","IRFC","ITC",
    "JINDALSTEL","JIOFIN","JSWENERGY","JSWSTEEL","KOTAKBANK",
    "LICHSGFIN","LT","LTIM","LUPIN","M&M",
    "MANAPPURAM","MARICO","MARUTI","MAXHEALTH","MCX",
    "MOTHERSON","MPHASIS","NAUKRI","NBCC","NMDC",
    "NTPC","OBEROIRLTY","OFSS","ONGC","PATANJALI",
    "PERSISTENT","PETRONET","PFC","PIDILITIND","POLYCAB",
    "POWERGRID","PREMIERENE","PVRINOX","RAMCOCEM","RBLBANK",
    "RECLTD","RELIANCE","SAIL","SAMMAANCAP","SBICARD",
    "SBILIFE","SBIN","SHREECEM","SIEMENS","SUNPHARMA",
    "SWIGGY","TATACOMM","TATACONSUM","TATAMOTORS","TATAPOWER",
    "TATASTEEL","TCS","TECHM","TITAN","TORNTPHARM",
    "TRENT","TVSMOTOR","ULTRACEMCO","UNOMINDA","VEDL",
    "VOLTAS","WAAREEENER","WIPRO","ZOMATO",
]
TIMEFRAMES = ["5m", "15m"]

# =====================================================================
# DEDUP CACHE (in-memory, resets on restart — acceptable for cloud)
# =====================================================================
_seen = set()

def is_duplicate(symbol, tf, direction, p5_price):
    key = f"{symbol}_{tf}_{direction}_{p5_price:.2f}"
    return key in _seen

def mark_seen(symbol, tf, direction, p5_price):
    key = f"{symbol}_{tf}_{direction}_{p5_price:.2f}"
    _seen.add(key)
    if len(_seen) > 5000:
        lst = list(_seen)
        _seen.clear()
        _seen.update(lst[-3000:])

# =====================================================================
# TELEGRAM + NTFY
# =====================================================================
_telegram_down = False

def send_telegram(msg):
    global _telegram_down
    if _telegram_down:
        _send_ntfy(msg)
        return
    def _post():
        global _telegram_down
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": msg[:4000], "parse_mode": "HTML"},
                timeout=10,
            )
            if r.status_code != 200:
                log.warning(f"Telegram HTTP {r.status_code}")
        except Exception as e:
            if not _telegram_down:
                log.warning(f"Telegram unreachable ({e.__class__.__name__}); trying ntfy.sh")
            _telegram_down = True
            _send_ntfy(msg)
    threading.Thread(target=_post, daemon=True).start()

def _send_ntfy(msg):
    import re
    text = re.sub(r"<[^>]+>", "", msg).strip()
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=text.encode("utf-8"),
            headers={"Tags": "rotating_light,wolf"},
            timeout=10,
        )
    except Exception:
        pass

# =====================================================================
# DHAN API
# =====================================================================
_dhan = None
_inst_map = {}

def init_dhan():
    global _dhan
    if not DHAN_ACCESS_TOKEN:
        log.error("No DHAN_ACCESS_TOKEN set")
        return False
    try:
        from dhanhq import DhanContext, dhanhq
        ctx = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
        _dhan = dhanhq(ctx)
        log.info("Dhan API connected")
        return True
    except Exception as e:
        log.error(f"Dhan init failed: {e}")
        return False

def load_instruments():
    global _inst_map
    try:
        log.info("Downloading instrument list...")
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        df = pd.read_csv(url, low_memory=False)
        nse_eq = df[
            (df["SEM_EXM_EXCH_ID"] == "NSE") &
            (df["SEM_INSTRUMENT_NAME"] == "EQUITY") &
            (df["SEM_SERIES"] == "EQ")
        ]
        for _, row in nse_eq.iterrows():
            sym = str(row.get("SEM_TRADING_SYMBOL", "")).strip().upper()
            sid = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
            if sym and sid:
                _inst_map[sym] = sid
        log.info(f"Loaded {len(_inst_map)} instruments")
    except Exception as e:
        log.error(f"Instrument download failed: {e}")

def get_security_id(symbol):
    return _inst_map.get(symbol.upper())

def fetch_candles(symbol, tf):
    if not _dhan:
        return None
    sec_id = get_security_id(symbol)
    if not sec_id:
        return None
    tf_config = {
        "5m":  ("5",  True,  60),
        "15m": ("15", True,  60),
    }
    interval, intraday, days_back = tf_config.get(tf, ("5", True, 60))
    try:
        now = datetime.datetime.now()
        start = now - datetime.timedelta(days=days_back)
        data = _dhan.intraday_minute_data(
            security_id=sec_id, exchange_segment="NSE_EQ",
            instrument_type="EQUITY", interval=interval,
            from_date=start.strftime("%Y-%m-%d 09:15:00"),
            to_date=now.strftime("%Y-%m-%d 15:30:00"),
        )
        if not data or not isinstance(data, dict):
            return None
        inner = data.get("data", data)
        df = pd.DataFrame({
            "open": inner.get("open", []),
            "high": inner.get("high", []),
            "low": inner.get("low", []),
            "close": inner.get("close", []),
            "volume": inner.get("volume", []),
        })
        for c in ["open", "high", "low", "close"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close"])
        return df
    except Exception:
        return None

# =====================================================================
# WOLFE WAVE DETECTION
# =====================================================================
PIVOT_LEFT = 5
PIVOT_RIGHT = 5

def detect_pivots(df, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    highs = df["high"].values
    lows = df["low"].values
    n = len(df)
    pivots = []
    for i in range(left, n - right):
        wh = highs[i - left: i + right + 1]
        if highs[i] == wh.max() and np.sum(wh == highs[i]) == 1:
            pivots.append({"idx": i, "price": float(highs[i]), "type": 1})
        wl = lows[i - left: i + right + 1]
        if lows[i] == wl.min() and np.sum(wl == lows[i]) == 1:
            pivots.append({"idx": i, "price": float(lows[i]), "type": -1})
    pivots.sort(key=lambda p: p["idx"])
    return _dedup(pivots)

def _dedup(pivots):
    if not pivots:
        return []
    result = [pivots[0]]
    for p in pivots[1:]:
        last = result[-1]
        if p["type"] == last["type"]:
            if p["type"] == 1 and p["price"] > last["price"]:
                result[-1] = p
            elif p["type"] == -1 and p["price"] < last["price"]:
                result[-1] = p
        else:
            result.append(p)
    return result

def check_wolfe(p0, p1, p2, p3, p4, p5, t0):
    d = 1 if t0 == 1 else -1
    rA = (p3 < p1) if d == 1 else (p3 > p1)
    rB = (p5 < p3) if d == 1 else (p5 > p3)
    rC = (p0 >= p3) if d == 1 else (p0 <= p3)
    lo4, hi4 = min(p1, p2), max(p1, p2)
    rD = lo4 <= p4 <= hi4
    rE = (p4 < p2) if d == 1 else (p4 > p2)
    rF = abs(p4 - p3) < abs(p2 - p1)
    rG = (p0 > p2) if d == 1 else (p0 < p2)
    return all([rA, rB, rC, rD, rE, rF, rG]), d

def scan_symbol(symbol, tf, df):
    pivots = detect_pivots(df)
    if len(pivots) < 6:
        return None
    pts = pivots[-6:]
    if pts[-1]["idx"] + PIVOT_RIGHT >= len(df):
        return None
    prices = [p["price"] for p in pts]
    bars = [p["idx"] for p in pts]
    valid, direction = check_wolfe(*prices, pts[0]["type"])
    if not valid:
        return None
    epa = prices[1]
    return direction, epa, list(zip(bars, prices))

# =====================================================================
# MARKET HOURS
# =====================================================================
def is_market_open():
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo
    now = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close

# =====================================================================
# SCAN CYCLE
# =====================================================================
def scan_once():
    new_count = 0
    for sym in NSE_FO_STOCKS:
        for tf in TIMEFRAMES:
            df = fetch_candles(sym, tf)
            if df is None or len(df) < 30:
                continue
            result = scan_symbol(sym, tf, df)
            if result is None:
                continue
            direction, epa, points = result
            d = "BULL" if direction == 1 else "BEAR"
            p5_price = points[-1][1]
            if is_duplicate(sym, tf, d, p5_price):
                continue
            mark_seen(sym, tf, d, p5_price)
            new_count += 1
            emoji = "🟢" if direction == 1 else "🔴"
            tf_label = {"5m": "5-Min", "15m": "15-Min"}.get(tf, tf)
            log.info(f"  {emoji} WOLFE {d} — {sym} {tf_label} EPA: {epa:.2f}")
            msg = (
                f"{emoji} <b>WOLFE WAVE {d}</b> — <b>{sym}</b> {tf_label}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"P0: {points[0][1]:.2f}\n"
                f"P1: {points[1][1]:.2f}\n"
                f"P2: {points[2][1]:.2f}\n"
                f"P3: {points[3][1]:.2f}\n"
                f"P4: {points[4][1]:.2f}\n"
                f"P5: {points[5][1]:.2f}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"🎯 EPA Target: ₹{epa:.2f}\n"
                f"⏰ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            send_telegram(msg)
    return new_count

# =====================================================================
# MAIN LOOP
# =====================================================================
def main():
    log.info("=" * 50)
    log.info("WOLFE WAVE CLOUD SCANNER — Starting")
    log.info("=" * 50)

    if not init_dhan():
        log.error("Cannot start without Dhan API")
        sys.exit(1)

    load_instruments()
    send_telegram("🚀 <b>Wolfe Wave Cloud Scanner started</b>")

    while True:
        if not is_market_open():
            log.info("Market closed. Sleeping 60s...")
            time.sleep(60)
            continue

        log.info(f"Market open. Scanning {len(NSE_FO_STOCKS)} stocks...")
        t0 = time.time()
        new = scan_once()
        elapsed = time.time() - t0
        log.info(f"Scan done in {elapsed:.1f}s — {new} new signals")

        wait = max(30, 120 - elapsed)
        log.info(f"Next scan in {wait:.0f}s")
        time.sleep(wait)


if __name__ == "__main__":
    main()
