\
import os
import asyncio
import time
from dataclasses import dataclass
from typing import List, Dict, Optional

import aiohttp
from dotenv import load_dotenv

from storage import init_db, insert_snapshot, insert_total, last_two_totals
from exchanges import fetch_all_exchanges
from notifier import send_telegram

load_dotenv()  # loads .env if present

ALERT_CHANGE_PCT = float(os.getenv("ALERT_CHANGE_PCT", "10"))
ALERT_CHANGE_USD = float(os.getenv("ALERT_CHANGE_USD", "0"))
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() == "true"

def pct_change(new: float, old: float) -> Optional[float]:
    if old is None or old == 0:
        return None
    return (new - old) / old * 100.0

async def take_snapshot(session) -> Dict:
    ts = int(time.time())
    rows = await fetch_all_exchanges(session)
    total = 0.0
    for r in rows:
        insert_snapshot(ts, r["exchange"], r["market"], r.get("base_volume"), r.get("quote_volume_usd"), r)
        if r.get("quote_volume_usd"):
            total += float(r["quote_volume_usd"])
    insert_total(ts, total)
    return {"ts": ts, "rows": rows, "total_usd": total}

def format_summary(snapshot: Dict, last_total: Optional[float], prev_total: Optional[float]) -> str:
    lines = []
    ts = snapshot["ts"]
    total = snapshot["total_usd"]
    lines.append(f"<b>ETH Futures Volume (24h)</b> — <i>{ts}</i>")
    lines.append(f"Total notional: <b>${total:,.0f}</b>")
    if last_total is not None:
        ch = total - last_total
        pc = pct_change(total, last_total)
        lines.append(f"Δ vs last: {('+' if ch>=0 else '')}{ch:,.0f} USD ({'+' if (pc or 0)>=0 else ''}{(pc or 0):.2f}%)")
    if prev_total is not None:
        ch2 = total - prev_total
        pc2 = pct_change(total, prev_total)
        lines.append(f"Δ vs prev: {('+' if ch2>=0 else '')}{ch2:,.0f} USD ({'+' if (pc2 or 0)>=0 else ''}{(pc2 or 0):.2f}%)")
    lines.append("— breakdown —")
    for r in snapshot["rows"]:
        q = r.get("quote_volume_usd")
        if q:
            lines.append(f"{r['exchange']}: {r['market']} → ${q:,.0f}")
    return "\n".join(lines)

async def maybe_alert(snapshot: Dict):
    last, prev = last_two_totals()
    last_total = last[1] if last else None
    prev_total = prev[1] if prev else None
    summary = format_summary(snapshot, last_total, prev_total)

    should_alert = False
    if last_total is not None:
        pc = pct_change(snapshot["total_usd"], last_total) or 0.0
        if abs(pc) >= ALERT_CHANGE_PCT:
            should_alert = True
    if not should_alert and last_total is not None:
        usd_change = abs(snapshot["total_usd"] - last_total)
        if usd_change >= ALERT_CHANGE_USD:
            should_alert = True

    print(summary)  # always print to stdout
    if should_alert:
        await send_telegram(summary)

async def runner():
    init_db()
    async with aiohttp.ClientSession() as session:
        while True:
            snap = await take_snapshot(session)
            await maybe_alert(snap)
            if RUN_ONCE:
                break
            await asyncio.sleep(POLL_SECONDS)

if __name__ == "__main__":
    asyncio.run(runner())
