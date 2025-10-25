\
import math
import aiohttp

# Helpers
def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

async def fetch_json(session, url, params=None, headers=None):
    async with session.get(url, params=params, headers=headers, timeout=30) as r:
        r.raise_for_status()
        return await r.json()

# ===== BINANCE FUTURES =====
async def binance_futures(session):
    """Returns list of dicts with keys: exchange, market, base_volume, quote_volume_usd"""
    out = []
    # USDT-margined perpetual
    try:
        data = await fetch_json(session, "https://fapi.binance.com/fapi/v1/ticker/24hr", params={"symbol":"ETHUSDT"})
        quote = safe_float(data.get("quoteVolume"))
        out.append({"exchange":"binance_futures", "market":"ETHUSDT_PERP", "base_volume": safe_float(data.get("volume")), "quote_volume_usd": quote})
    except Exception:
        pass
    # COIN-margined perpetual
    try:
        data = await fetch_json(session, "https://dapi.binance.com/dapi/v1/ticker/24hr", params={"symbol":"ETHUSD_PERP"})
        # This endpoint returns base volume in contracts (ETH). Multiply by last price to get USD notional
        last = safe_float(data.get("lastPrice"))
        vol = safe_float(data.get("volume"))
        quote = last * vol if last and vol else None
        out.append({"exchange":"binance_coin_futures", "market":"ETHUSD_PERP", "base_volume": vol, "quote_volume_usd": quote})
    except Exception:
        pass
    return out

# ===== BYBIT =====
async def bybit(session):
    out = []
    try:
        # linear (USDT)
        data = await fetch_json(session, "https://api.bybit.com/v5/market/tickers", params={"category":"linear", "symbol":"ETHUSDT"})
        items = data.get("result", {}).get("list", []) or data.get("result", {}).get("category", [])
        if items:
            it = items[0]
            turnover24h = safe_float(it.get("turnover24h")) or safe_float(it.get("turnover24h", 0))
            out.append({"exchange":"bybit", "market":"ETHUSDT_PERP", "base_volume": None, "quote_volume_usd": turnover24h})
    except Exception:
        pass
    try:
        # inverse (USD)
        data = await fetch_json(session, "https://api.bybit.com/v5/market/tickers", params={"category":"inverse", "symbol":"ETHUSD"})
        items = data.get("result", {}).get("list", [])
        if items:
            it = items[0]
            turnover24h = safe_float(it.get("turnover24h"))
            out.append({"exchange":"bybit", "market":"ETHUSD_PERP", "base_volume": None, "quote_volume_usd": turnover24h})
    except Exception:
        pass
    return out

# ===== OKX =====
async def okx(session):
    out = []
    for inst in ["ETH-USDT-SWAP", "ETH-USD-SWAP"]:
        try:
            data = await fetch_json(session, "https://www.okx.com/api/v5/market/ticker", params={"instId":inst})
            arr = data.get("data", [])
            if arr:
                it = arr[0]
                # OKX returns volCcy24h as 24h volume in quote ccy (USDT or USD)
                quote = safe_float(it.get("volCcy24h"))
                out.append({"exchange":"okx", "market":inst, "base_volume": safe_float(it.get("vol24h")), "quote_volume_usd": quote})
        except Exception:
            pass
    return out

# ===== DERIBIT =====
async def deribit(session):
    out = []
    # Deribit futures, aggregate all ETH futures
    try:
        data = await fetch_json(session, "https://www.deribit.com/api/v2/public/get_book_summary_by_currency", params={"currency":"ETH","kind":"future"})
        arr = data.get("result", [])
        for it in arr:
            # 'volume' is 24h base volume (ETH); 'last' is last price in USD
            base = safe_float(it.get("volume"))
            last = safe_float(it.get("last"))
            quote = base * last if base and last else None
            out.append({"exchange":"deribit", "market": it.get("instrument_name","ETH-FUT"), "base_volume": base, "quote_volume_usd": quote})
    except Exception:
        pass
    return out

# ===== BITMEX =====
async def bitmex(session):
    out = []
    try:
        data = await fetch_json(session, "https://www.bitmex.com/api/v1/instrument", params={"symbol":"ETHUSDT","columns":"symbol,volume24h,turnover24h,lastPrice"})
        if isinstance(data, list) and data:
            it = data[0]
            last = safe_float(it.get("lastPrice"))
            # turnover24h is in XBt? On ETHUSDT, turnover is quote ccy; prefer turnover
            turnover = safe_float(it.get("turnover24h"))
            if turnover:
                out.append({"exchange":"bitmex", "market":"ETHUSDT_PERP", "base_volume": None, "quote_volume_usd": turnover})
            else:
                base = safe_float(it.get("volume24h"))
                quote = base * last if base and last else None
                out.append({"exchange":"bitmex", "market":"ETHUSDT_PERP", "base_volume": base, "quote_volume_usd": quote})
    except Exception:
        pass
    return out

async def fetch_all_exchanges(session):
    results = []
    # Order matters for readability only
    for fn in [binance_futures, bybit, okx, deribit, bitmex]:
        try:
            rows = await fn(session)
            results.extend(rows)
        except Exception:
            pass
    return results
