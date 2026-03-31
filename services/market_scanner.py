import requests
import yfinance as yf
from datetime import datetime, timedelta, timezone


REQUEST_TIMEOUT = 12


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def get_candidate_symbols():
    """
    Lista fija temporal para que el scanner no dependa del FTP de NASDAQ.
    """
    return [
        "ELAB", "EEIQ", "ASTC", "BFRG", "UGRO", "GLND", "QNTM",
        "RBNE", "JCSE", "WISA", "AGRI", "XELA", "BBLG", "GROM",
        "HUBC", "TOP", "TPST", "ICU", "IMPP", "SILO",
        "MULN", "FFIE", "NKLA", "TTOO", "APRN"
    ]


def flatten_columns(df):
    """
    A veces yfinance devuelve MultiIndex en columnas.
    Esto lo aplana para poder usar df['Close'], df['Open'], etc.
    """
    try:
        if hasattr(df.columns, "levels"):
            df.columns = df.columns.get_level_values(0)
    except Exception:
        pass
    return df


def get_fast_snapshot(symbol):
    try:
        df = yf.download(
            symbol,
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
        )

        if df is None or df.empty:
            print(f"NO DAILY DATA: {symbol}")
            return None

        df = flatten_columns(df)

        needed_cols = ["Close", "Open", "High", "Low", "Volume"]
        missing = [col for col in needed_cols if col not in df.columns]
        if missing:
            print(f"ERROR snapshot {symbol}: missing columns {missing}")
            return None

        df = df.dropna(subset=needed_cols)

        if df.empty:
            print(f"EMPTY DAILY AFTER DROPNA: {symbol}")
            return None

        last = df.iloc[-1]
        prev_close = df["Close"].iloc[-2] if len(df) >= 2 else last["Close"]

        return {
            "symbol": symbol,
            "price": safe_float(last["Close"]),
            "prev_close": safe_float(prev_close),
            "volume": safe_int(last["Volume"]),
            "open": safe_float(last["Open"]),
            "day_high": safe_float(last["High"]),
            "day_low": safe_float(last["Low"]),
            "market_cap": None,
            "float": None,
        }

    except Exception as e:
        print(f"ERROR snapshot {symbol}: {e}")
        return None


def enrich_with_info(symbol, row):
    """
    Intenta sacar float, avg volume, market cap y nombre.
    Si falla, no rompe el screener.
    """
    try:
        t = yf.Ticker(symbol)
        info = t.info

        float_shares = info.get("floatShares")
        avg_volume = info.get("averageVolume")
        market_cap = info.get("marketCap")
        short_name = info.get("shortName") or info.get("longName") or symbol

        if float_shares:
            row["float"] = safe_float(float_shares, row.get("float"))

        if market_cap:
            row["market_cap"] = safe_float(market_cap, row.get("market_cap"))

        row["avg_volume"] = safe_int(avg_volume)
        row["name"] = short_name

        return row

    except Exception as e:
        print(f"INFO FAIL {symbol}: {e}")
        row["avg_volume"] = None
        row["name"] = symbol
        return row


def get_recent_momentum(symbol):
    """
    Momentum intradía 1m, 5m y 10m.
    """
    try:
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=20)

        df = yf.download(
            symbol,
            interval="1m",
            start=start,
            end=end,
            progress=False,
            auto_adjust=False,
            threads=False,
            prepost=True,
        )

        if df is None or df.empty:
            print(f"NO 1M DATA: {symbol}")
            return None

        df = flatten_columns(df)

        if "Close" not in df.columns:
            print(f"NO CLOSE COLUMN 1M: {symbol}")
            return None

        closes = df["Close"].dropna()

        if closes is None or len(closes) < 2:
            print(f"NOT ENOUGH 1M BARS: {symbol}")
            return None

        last = float(closes.iloc[-1])
        close_1m = float(closes.iloc[-2])
        close_5m = float(closes.iloc[-6]) if len(closes) >= 6 else float(closes.iloc[0])
        close_10m = float(closes.iloc[-11]) if len(closes) >= 11 else float(closes.iloc[0])

        change_1m = ((last - close_1m) / close_1m) * 100 if close_1m else 0
        change_5m = ((last - close_5m) / close_5m) * 100 if close_5m else 0
        change_10m = ((last - close_10m) / close_10m) * 100 if close_10m else 0

        return {
            "change_1m": round(change_1m, 2),
            "change_5m": round(change_5m, 2),
            "change_10m": round(change_10m, 2),
        }

    except Exception as e:
        print(f"ERROR get_recent_momentum {symbol}: {e}")
        return None


def passes_primary_filters(row):
    price = row.get("price")
    prev_close = row.get("prev_close")
    market_cap = row.get("market_cap")
    fl = row.get("float")
    volume = row.get("volume")

    if not price or not prev_close or not volume:
        return False

    if price < 0.5 or price > 20:
        return False

    pct_day = ((price - prev_close) / prev_close) * 100
    row["pct_day"] = round(pct_day, 2)

    if pct_day < 5:
        return False

    if market_cap and market_cap > 2_000_000_000:
        return False

    if fl and fl > 120_000_000:
        return False

    if volume < 75_000:
        return False

    return True


def add_relative_volume(row):
    volume = row.get("volume")
    avg_volume = row.get("avg_volume")

    if volume and avg_volume and avg_volume > 0:
        row["rvol"] = round(volume / avg_volume, 2)
    else:
        row["rvol"] = None

    return row


def compute_score(row):
    pct_day = row.get("pct_day") or 0
    change_1m = row.get("change_1m") or 0
    change_5m = row.get("change_5m") or 0
    change_10m = row.get("change_10m") or 0
    rvol = row.get("rvol") or 0
    volume = row.get("volume") or 0

    volume_factor = min(volume / 300000, 20)

    score = (
        pct_day * 1.2
        + change_1m * 1.5
        + change_5m * 2.2
        + change_10m * 1.0
        + rvol * 2.0
        + volume_factor
    )

    row["momentum_score"] = round(score, 2)
    return row


def format_row(row):
    return {
        "symbol": row.get("symbol"),
        "name": row.get("name"),
        "price": round(row.get("price", 0), 2) if row.get("price") is not None else None,
        "pct_day": row.get("pct_day"),
        "volume": row.get("volume"),
        "avg_volume": row.get("avg_volume"),
        "rvol": row.get("rvol"),
        "market_cap": row.get("market_cap"),
        "float": row.get("float"),
        "change_1m": row.get("change_1m", 0),
        "change_5m": row.get("change_5m", 0),
        "change_10m": row.get("change_10m", 0),
        "momentum_score": row.get("momentum_score", 0),
    }


def scan_market():
    symbols = get_candidate_symbols()
    print("TOTAL SYMBOLS:", len(symbols))

    if not symbols:
        return []

    results = []

    for i, sym in enumerate(symbols, start=1):
        print(f"[{i}/{len(symbols)}] SCANNING {sym}")

        snap = get_fast_snapshot(sym)
        if not snap:
            continue

        if not passes_primary_filters(snap):
            continue

        snap = enrich_with_info(sym, snap)
        snap = add_relative_volume(snap)

        mom = get_recent_momentum(sym)
        if mom:
            snap.update(mom)
        else:
            snap["change_1m"] = 0
            snap["change_5m"] = 0
            snap["change_10m"] = 0

        snap = compute_score(snap)
        results.append(format_row(snap))

    results.sort(key=lambda x: x["momentum_score"], reverse=True)

    if not results:
        print("NO RESULTS → relaxing filters")

        for sym in symbols[:10]:
            snap = get_fast_snapshot(sym)
            if not snap:
                continue

            snap["name"] = sym
            snap["avg_volume"] = None
            snap["rvol"] = None
            snap["pct_day"] = 0
            snap["change_1m"] = 0
            snap["change_5m"] = 0
            snap["change_10m"] = 0
            snap["momentum_score"] = 0

            results.append(format_row(snap))

    print("FINAL RESULTS:", len(results))
    return results[:25]
