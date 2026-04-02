import time
import yfinance as yf


CANDIDATE_SYMBOLS = [
    "TURB", "TMDE", "SELX", "BATL", "TPET", "EONR", "ANNA",
    "GDXD", "USEG", "RBNE"
]

CACHE = {
    "gainers": {"data": [], "timestamp": 0},
    "momentum": {"data": [], "timestamp": 0},
}

CACHE_SECONDS = 10


def safe_scalar(value):
    try:
        if hasattr(value, "iloc"):
            return value.iloc[0]
        return value
    except Exception:
        return value


def flatten_columns(df):
    try:
        if hasattr(df.columns, "levels"):
            df.columns = df.columns.get_level_values(0)
    except Exception:
        pass
    return df


def safe_pct_change(new, old):
    try:
        if old in (0, None):
            return 0
        return round(((new - old) / old) * 100, 2)
    except Exception:
        return 0


def passes_basic_filters(price, volume):
    if price is None:
        return False
    if price < 0.5 or price > 25:
        return False
    if volume is None or volume < 50000:
        return False
    return True


def format_float_value(value):
    try:
        if value is None:
            return None
        value = float(value)
        return round(value / 1_000_000, 2)
    except Exception:
        return None


def get_company_info(symbol):
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}

        float_shares = info.get("floatShares")
        avg_volume = info.get("averageVolume")
        name = info.get("shortName") or info.get("longName") or symbol

        return {
            "name": name,
            "companyName": name,
            "float_shares": float_shares,
            "avg_volume": avg_volume,
        }
    except Exception as e:
        print(f"INFO FAIL {symbol}: {e}")
        return {
            "name": symbol,
            "companyName": symbol,
            "float_shares": None,
            "avg_volume": None,
        }


def get_daily_snapshot(symbol):
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
            return None

        df = flatten_columns(df)

        needed_cols = ["Close", "Open", "Volume"]
        for col in needed_cols:
            if col not in df.columns:
                return None

        df = df.dropna(subset=needed_cols)
        if df.empty:
            return None

        last = df.iloc[-1]
        prev_close = safe_scalar(df["Close"].iloc[-2]) if len(df) >= 2 else safe_scalar(last["Close"])

        price = round(float(safe_scalar(last["Close"])), 4)
        prev_close = round(float(prev_close), 4)
        open_price = round(float(safe_scalar(last["Open"])), 4)
        volume = int(float(safe_scalar(last["Volume"])))
        day_change = safe_pct_change(price, prev_close)

        return {
            "symbol": symbol,
            "price": price,
            "prev_close": prev_close,
            "open_price": open_price,
            "volume": volume,
            "day_change": day_change,
            "changePercent": day_change,   # clave compatible con gainers.html
        }

    except Exception as e:
        print(f"DAILY SNAPSHOT FAIL {symbol}: {e}")
        return None


def get_intraday_momentum(symbol):
    try:
        df = yf.download(
            symbol,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=False,
            threads=False,
            prepost=True,
        )

        if df is None or df.empty:
            return {
                "change_1m": 0,
                "change_5m": 0,
            }

        df = flatten_columns(df)

        if "Close" not in df.columns:
            return {
                "change_1m": 0,
                "change_5m": 0,
            }

        closes = df["Close"].dropna()

        if len(closes) < 2:
            return {
                "change_1m": 0,
                "change_5m": 0,
            }

        last = float(safe_scalar(closes.iloc[-1]))
        close_1m_ago = float(safe_scalar(closes.iloc[-2]))
        close_5m_ago = float(safe_scalar(closes.iloc[-6])) if len(closes) >= 6 else float(safe_scalar(closes.iloc[0]))

        return {
            "change_1m": safe_pct_change(last, close_1m_ago),
            "change_5m": safe_pct_change(last, close_5m_ago),
        }

    except Exception as e:
        print(f"INTRADAY FAIL {symbol}: {e}")
        return {
            "change_1m": 0,
            "change_5m": 0,
        }


def build_gainers():
    now = time.time()
    if now - CACHE["gainers"]["timestamp"] < CACHE_SECONDS:
        return CACHE["gainers"]["data"]

    results = []

    for symbol in CANDIDATE_SYMBOLS:
        snap = get_daily_snapshot(symbol)
        if not snap:
            continue

        price = snap["price"]
        prev_close = snap["prev_close"]
        volume = snap["volume"]
        day_change = snap["day_change"]

        if not passes_basic_filters(price, volume):
            continue

        mom = get_intraday_momentum(symbol)
        info = get_company_info(symbol)

        avg_volume = info.get("avg_volume")
        rvol = round(volume / avg_volume, 2) if avg_volume not in (None, 0) else None

        results.append({
            "symbol": symbol,
            "name": info.get("name"),
            "companyName": info.get("companyName"),
            "price": price,
            "prev_close": prev_close,
            "day_change": day_change,
            "changePercent": day_change,   # esta es la clave importante
            "percent_change": day_change,  # compatibilidad extra
            "change_pct": day_change,      # compatibilidad extra
            "change_5m": mom["change_5m"],
            "change_1m": mom["change_1m"],
            "volume": volume,
            "float_shares": info.get("float_shares"),
            "float_m": format_float_value(info.get("float_shares")),
            "avg_volume": avg_volume,
            "rvol": rvol,
        })

    results.sort(key=lambda x: (x["day_change"], x["change_5m"], x["volume"]), reverse=True)
    final_data = results[:8]

    CACHE["gainers"] = {
        "data": final_data,
        "timestamp": time.time(),
    }
    return final_data


def build_momentum():
    now = time.time()
    if now - CACHE["momentum"]["timestamp"] < CACHE_SECONDS:
        return CACHE["momentum"]["data"]

    results = []

    for symbol in CANDIDATE_SYMBOLS:
        snap = get_daily_snapshot(symbol)
        if not snap:
            continue

        price = snap["price"]
        volume = snap["volume"]

        if not passes_basic_filters(price, volume):
            continue

        mom = get_intraday_momentum(symbol)

        if mom["change_1m"] > 0 or mom["change_5m"] > 0:
            results.append({
                "symbol": symbol,
                "price": price,
                "change_1m": mom["change_1m"],
                "change_5m": mom["change_5m"],
                "volume": volume,
            })

    results.sort(key=lambda x: (x["change_1m"], x["change_5m"], x["volume"]), reverse=True)
    final_data = results[:8]

    CACHE["momentum"] = {
        "data": final_data,
        "timestamp": time.time(),
    }
    return final_data
