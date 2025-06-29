#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
strategy.py – Lazy batched yfinance download
Downloads all tickers in a single request the first time the
web page is hit, then re‑uses the data for faster responses.
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# ── CONFIG ────────────────────────────────────────────────────────────
THRESHOLD_PERCENT = 20
START_DATE = (datetime.today() - timedelta(days=3 * 365)).strftime('%Y-%m-%d')
END_DATE   = datetime.today().strftime('%Y-%m-%d')

# Full NSE ticker list (191 symbols)
all_stocks = [
    "3MINDIA", "AHLUCONT", "AIAENG", "AJANTPHARM", "AKZOINDIA", "ALKEM", "ANANDRATHI",
    "ANGELONE", "APARINDS", "APOLLOHOSP", "ARCHEM", "ASIANPAINT", "ASTRAL", "ASTRAZEN", "AWL",
    "AVANTIFEED", "BAJAJ-AUTO", "BAJAJHLDNG", "BASF", "BAYERCROP", "BEL", "BERGEPAINT", "BIKAJI",
    "BLUESTARCO", "BOSCHLTD", "BSOFT", "BSE", "CAPLIPOINT", "CARBORUNIV", "CAMS", "CASTROLIND",
    "CELLO", "CERA", "CHAMBLFERT", "CIPLA", "CMSINFO", "COALINDIA", "COCHINSHIP", "COLPAL",
    "CONCORDBIO", "COROMANDEL", "CROMPTON", "CRISIL", "CUMMINSIND", "DABUR", "DBCORP", "DEEPAKNTR",
    "DHANUKA", "DIXON", "DMART", "DRREDDY", "ECLERX", "EICHERMOT", "EIDPARRY", "EIHOTEL", "ELECON",
    "EMAMILTD", "ENGINERSIN", "ERIS", "FINEORG", "FORCEMOT", "FORTIS", "GANESHHOUC", "GARFIBRES",
    "GHCL", "GILLETTE", "GLAXO", "GODFRYPHLP", "GODREJCP", "GODREJIND", "GRINDWELL", "GRSE", "GSPL",
    "GUJGASLTD", "HAL", "HAPPYFORGE", "HAVELLS", "HCLTECH", "HEROMOTOCO", "HINDUNILVR", "HONAUT",
    "ICICIGI", "IEX", "IGL", "IMFA", "INDHOTEL", "INDIAMART", "INFY", "INGERRAND", "INTELLECT",
    "IONEXCHANG", "IRCTC", "ITC", "JBCHEPHARM", "JAIBALAJI", "JIOFIN", "JWL", "JYOTHYLAB",
    "JYOTICNC", "KAJARIACER", "KAMS", "KFINTECH", "KEI", "KIRLOSBROS", "KPIGREEN", "KPITTECH",
    "KPRMILL", "KSCL", "LALPATHLAB", "LICI", "LTIM", "LTTS", "MAHAPEXLTD", "MAHSEAMLES", "MANKIND",
    "MANINFRA", "MARICO", "MARUTI", "MCX", "MCDHOLDING", "MEDANTA", "MGL", "MISHTANN", "MPHASIS",
    "MRF", "MSUMI", "NAM-INDIA", "NATCOPHARM", "NBCC", "NEULANDLAB", "NEWGEN", "NESCO", "NIITLTD",
    "NMDC", "OFSS", "PAGEIND", "PETRONET", "PFIZER", "PGHH", "PGHL", "PIDILITIND", "PIIND", "POLYCAB",
    "POLYMED", "RADICO", "RAILTEL", "RATNAMANI", "RELAXO", "RITES", "ROUTE", "SANOFI", "SCHAEFFLER",
    "SEQUENT", "SHARDAMOTR", "SHAREINDIA", "SHRIPISTON", "SIEMENS", "SKFINDIA", "STYRENIX",
    "SUMICHEM", "SUNTV", "SUPREMEIND", "SURYAROSNI", "TANLA", "TATAELXSI", "TATAMOTORS", "TATATECH",
    "TBOTEK", "TEAMLEASE", "TECHM", "TIINDIA", "TIMKEN", "TITAGARH", "TRITURBINE", "UBL",
    "ULTRACEMCO", "UNITDSPR", "UPL", "URJAGLO", "USHAMART", "UTIAMC", "VBL", "VESUVIUS", "VOLTAMP",
    "VSTIND", "WSTCSTPAPR", "ZENSARTECH", "ZFCVINDIA", "ZENTEC"
]

# ── Lazy bulk download cache ──────────────────────────────────────────
_bulk_cache = None


def _bulk_download() -> pd.DataFrame:
    """Download all tickers in one call (only once per runtime)."""
    tickers = [s + ".NS" for s in all_stocks]
    return yf.download(
        tickers=tickers,
        start=START_DATE,
        end=END_DATE,
        group_by="ticker",
        threads=False
    )


def _get_bulk() -> pd.DataFrame:
    global _bulk_cache
    if _bulk_cache is None:
        _bulk_cache = _bulk_download()
    return _bulk_cache


# ── Helpers ───────────────────────────────────────────────────────────
def get_df(symbol: str) -> pd.DataFrame | None:
    bulk = _get_bulk()
    try:
        df = bulk[f"{symbol}.NS"].dropna()
    except KeyError:
        return None

    if df.empty:
        return None

    df = df[['Open', 'High', 'Low', 'Close']].copy()
    df['MA200'] = df['Close'].rolling(window=200).mean()
    return df


def find_v20_signals(df: pd.DataFrame):
    signals = []
    latest_close = df['Close'].iloc[-1]
    streak_low = streak_high = None

    for idx in range(1, len(df)):
        cur = df.iloc[idx]

        # Green candle
        if cur['Close'] > cur['Open']:
            streak_low = cur['Low'] if streak_low is None else min(streak_low, cur['Low'])
            streak_high = cur['High'] if streak_high is None else max(streak_high, cur['High'])
            continue

        # Red candle ends streak
        if streak_low and streak_high:
            pct_move = (streak_high - streak_low) / streak_low * 100
            if pct_move >= THRESHOLD_PERCENT and streak_low < cur.get('MA200', float('inf')):
                proximity = abs(latest_close - streak_low) / streak_low * 100
                signals.append((
                    df.index[idx].date(), round(streak_low, 2), round(streak_high, 2),
                    round(pct_move, 2), round(latest_close, 2), round(proximity, 2)
                ))
        streak_low = streak_high = None
    return signals


# ── Main function called by Flask ─────────────────────────────────────
def scan_stocks():
    results = []
    for sym in all_stocks:
        df = get_df(sym)
        if df is None:
            continue
        for sig in find_v20_signals(df):
            sig_date, buy, sell, pct, close, prox = sig
            results.append({
                'SignalDate': str(sig_date),
                'Symbol': sym,
                'BuyAt': buy,
                'SellAt': sell,
                '%Move': pct,
                'Close': close,
                'Proximity%': prox
            })

    # Newest date first, then nearest proximity
    results.sort(key=lambda x: (x['SignalDate'], x['Proximity%']), reverse=True)
    return results
