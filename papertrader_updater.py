import os
import json
import time
import signal
import sys
from typing import List, Dict

import gspread
from google.oauth2.service_account import Credentials

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest

# ------------- Config -------------
SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Trading Log")  # change if needed
WORKSHEET_NAME = os.getenv("PAPERTRADER_SHEET", "Papertrader")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "150"))  # symbols per batch request

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")

if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
    print("ERROR: Missing ALPACA_API_KEY or ALPACA_SECRET_KEY", file=sys.stderr)
    sys.exit(1)

# ------------- Graceful shutdown -------------
_running = True
def _handle_sigterm(signum, frame):
    global _running
    _running = False
signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ------------- Google Sheets client -------------
def _get_gspread_client():
    creds_json = os.getenv("GOOGLE_CREDS_JSON")
    if not creds_json:
        print("ERROR: Missing GOOGLE_CREDS_JSON", file=sys.stderr)
        sys.exit(1)
    info = json.loads(creds_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(credentials)

def _open_worksheet(gc):
    sh = gc.open(SHEET_NAME)
    try:
        ws = sh.worksheet(WORKSHEET_NAME)
    except gspread.WorksheetNotFound:
        # Create if missing; basic headers
        ws = sh.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=5)
        ws.update("A1:C1", [["Ticker", "Last Updated (UTC)", "Price"]])
    return ws

# ------------- Ticker + price helpers -------------
def _get_tickers(ws) -> List[str]:
    # Read from A2 downward, ignore blanks and headers
    tickers = ws.col_values(1)[1:]  # skip header
    return [t.strip().upper() for t in tickers if t.strip()]

def _chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _fetch_latest_prices(symbols: List[str], client: StockHistoricalDataClient) -> Dict[str, float]:
    """
    Returns dict: { 'AAPL': 172.34, ... } (missing keys if no data)
    """
    prices = {}
    if not symbols:
        return prices

    # Batch using alpaca-py latest trade endpoint
    for batch in _chunks(symbols, CHUNK_SIZE):
        req = StockLatestTradeRequest(symbol_or_symbols=batch)
        try:
            result = client.get_stock_latest_trade(req)
        except Exception as e:
            print(f"Alpaca request failed for batch {batch[:5]}... ({len(batch)} symbols): {e}", file=sys.stderr)
            continue

        # result is mapping-like: {'AAPL': Trade(...), ...}
        for sym, trade in (result or {}).items():
            try:
                prices[sym] = float(trade.price)
            except Exception:
                # Some symbols might not have trades (halted, bad symbol, etc.)
                pass
    return prices

def _write_prices(ws, symbols: List[str], price_map: Dict[str, float]):
    """
    Writes:
      - Column B: timestamp
      - Column C: price
    Keeps row alignment with tickers in column A.
    """
    if not symbols:
        return

    # Build update ranges
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    updates_colB = []
    updates_colC = []
    # Rows start at 2 because row 1 is header
    for idx, sym in enumerate(symbols, start=2):
        price = price_map.get(sym)
        # Always stamp Column B so you can see freshness; only write price if available
        updates_colB.append([timestamp])
        updates_colC.append([price if price is not None else ""])

    ws.update(f"B2:B{len(symbols)+1}", updates_colB, value_input_option="RAW")
    ws.update(f"C2:C{len(symbols)+1}", updates_colC, value_input_option="RAW")

def main():
    print("Papertrader updater starting…", flush=True)
    gc = _get_gspread_client()
    ws = _open_worksheet(gc)

    client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_SECRET_KEY)

    while _running:
        try:
            symbols = _get_tickers(ws)
            if not symbols:
                print("No tickers in column A. Sleeping…", flush=True)
                time.sleep(POLL_SECONDS)
                continue

            price_map = _fetch_latest_prices(symbols, client)
            _write_prices(ws, symbols, price_map)

            print(f"Updated {len(price_map)} of {len(symbols)} tickers at {time.strftime('%H:%M:%S', time.gmtime())}", flush=True)

        except Exception as e:
            print(f"Loop error: {e}", file=sys.stderr)

        time.sleep(POLL_SECONDS)

    print("Papertrader updater shutting down.")

if __name__ == "__main__":
    main()
