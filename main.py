# main.py
import os
import json
import time
import signal
import sys
from typing import List, Dict, Tuple

import gspread
from google.oauth2.service_account import Credentials

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

# ----------------- Config -----------------
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")                    # preferred (from the Sheet URL)
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Trading Log") # fallback if no ID provided
WORKSHEET_NAME = os.getenv("PAPERTRADER_SHEET", "Papertrader")

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))   # loop interval
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "150"))      # batch size for Alpaca

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("ERROR: Missing ALPACA_API_KEY or ALPACA_SECRET_KEY", file=sys.stderr)
    sys.exit(1)

# ----------------- Graceful shutdown -----------------
_running = True
def _handle_sigterm(signum, frame):
    global _running
    _running = False

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ----------------- Google Sheets helpers -----------------
def _get_gspread_client():
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    if not creds_json:
        print("ERROR: Missing GOOGLE_CREDS_JSON", file=sys.stderr)
        sys.exit(1)

    info = json.loads(creds_json)
    # Optional: log which service account is used (helps with sharing/permissions)
    print(f"Using service account: {info.get('client_email','(unknown)')}", flush=True)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)

def _open_worksheet(gc):
    if SHEET_ID:
        sh = gc.open_by_key(SHEET_ID)
    else:
        sh = gc.open(SHEET_NAME)

    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=5)
        # Only A and C matter for this bot; add light headers so A1/C1 are obvious.
        ws.update("A1:C1", [["asset", "", "current price"]])
    return ws

# ----------------- Row-preserving read/write -----------------
def _get_rows_and_symbols(ws) -> List[Tuple[int, str]]:
    """
    Reads Column A and returns a list of (row_index, SYMBOL) for all non-empty cells,
    preserving original row numbers. Row 1 is header; data starts at row 2.
    """
    col_a = ws.col_values(1)  # up to last non-empty A cell
    out: List[Tuple[int, str]] = []
    for row_idx, raw in enumerate(col_a[1:], start=2):  # skip header row
        sym = (raw or "").strip().upper()
        if sym:
            # allow a leading '$' or stray spaces
            if sym.startswith("$"):
                sym = sym[1:]
            out.append((row_idx, sym))
    return out

def _write_prices_exact_rows(ws, rows_with_symbols: List[Tuple[int, str]], price_map: Dict[str, float]) -> None:
    """
    Writes each price into the exact row's Column C (C{row_idx}).
    Only touches Column C, per user's requirement.
    """
    if not rows_with_symbols:
        return

    updates = []
    for row_idx, sym in rows_with_symbols:
        price = price_map.get(sym)
        updates.append({
            "range": f"C{row_idx}",
            "values": [[price if price is not None else ""]],
        })

    # Batch non-contiguous cell updates
    if updates:
        ws.batch_update(updates, value_input_option="RAW")

# ----------------- Alpaca price fetch -----------------
def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _fetch_latest_prices(symbols: List[str], client: StockHistoricalDataClient) -> Dict[str, float]:
    """
    Returns dict: { 'AAPL': 172.34, ... } for symbols with available trades.
    """
    prices: Dict[str, float] = {}
    if not symbols:
        return prices

    # Use a set to avoid redundant requests for duplicates in Column A
    unique_symbols = list(dict.fromkeys(symbols))  # preserve order while deduping

    for batch in _chunks(unique_symbols, CHUNK_SIZE):
        try:
            req = StockLatestTradeRequest(symbol_or_symbols=batch)
            result = client.get_stock_latest_trade(req)
        except Exception as e:
            print(f"Alpaca request failed for batch of size {len(batch)}: {e}", file=sys.stderr)
            continue

        # result is mapping-like: {'AAPL': Trade(...), ...}
        for sym, trade in (result or {}).items():
            try:
                prices[sym] = float(trade.price)
            except Exception:
                # If trade.price missing or unparsable, skip gracefully
                pass

    return prices

# ----------------- Main loop -----------------
def main():
    print("Papertrader updater starting…", flush=True)

    gc = _get_gspread_client()
    ws = _open_worksheet(gc)

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    while _running:
        try:
            rows_syms = _get_rows_and_symbols(ws)     # [(row_idx, 'AAPL'), ...]
            symbols   = [s for _, s in rows_syms]     # ['AAPL', 'SPY', ...]

            if not symbols:
                print("No tickers in Column A. Sleeping…", flush=True)
                time.sleep(POLL_SECONDS)
                continue

            price_map = _fetch_latest_prices(symbols, client)
            _write_prices_exact_rows(ws, rows_syms, price_map)

            updated = sum(1 for _, s in rows_syms if s in price_map)
            print(f"Updated {updated} of {len(rows_syms)} rows. Sleeping {POLL_SECONDS}s…", flush=True)

        except Exception as e:
            print(f"Loop error: {e}", file=sys.stderr)

        time.sleep(POLL_SECONDS)

    print("Papertrader updater shutting down.")

if __name__ == "__main__":
    main()
