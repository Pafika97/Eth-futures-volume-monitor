\
import sqlite3
from pathlib import Path
from typing import Optional, Tuple
import time

DB_PATH = Path(__file__).with_name("eth_futures_volume.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS volume_snapshots (
    ts INTEGER NOT NULL,
    exchange TEXT NOT NULL,
    market TEXT NOT NULL,
    base_volume REAL,          -- in ETH when provided
    quote_volume_usd REAL,     -- normalized to USD
    raw JSON
);
CREATE INDEX IF NOT EXISTS idx_ts ON volume_snapshots(ts);
CREATE INDEX IF NOT EXISTS idx_exchange_ts ON volume_snapshots(exchange, ts);
CREATE TABLE IF NOT EXISTS totals (
    ts INTEGER PRIMARY KEY,
    total_quote_volume_usd REAL NOT NULL
);
"""

def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.executescript(SCHEMA)

def insert_snapshot(ts:int, exchange:str, market:str, base_volume:Optional[float], quote_volume_usd:Optional[float], raw:dict):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(
            "INSERT INTO volume_snapshots (ts, exchange, market, base_volume, quote_volume_usd, raw) VALUES (?,?,?,?,?,json(?))",
            (ts, exchange, market, base_volume, quote_volume_usd, __import__("json").dumps(raw)),
        )

def insert_total(ts:int, total_quote_volume_usd:float):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("INSERT OR REPLACE INTO totals (ts, total_quote_volume_usd) VALUES (?,?)", (ts, total_quote_volume_usd))

def last_two_totals() -> Tuple[Optional[Tuple[int, float]], Optional[Tuple[int, float]]]:
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute("SELECT ts, total_quote_volume_usd FROM totals ORDER BY ts DESC LIMIT 2")
        rows = cur.fetchall()
        if not rows:
            return None, None
        if len(rows) == 1:
            return rows[0], None
        return rows[0], rows[1]
