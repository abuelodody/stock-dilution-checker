import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

POLYGON_API_KEY = "u9ZalpowcehPoQyZUh1gpPkNS_d9VUmy"

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


def get_period_string(period_key: str) -> str:
    mapping = {
        "6m": "6mo",
        "1y": "1y",
        "2y": "2y",
        "5y": "5y",
    }
    return mapping.get(period_key, "1y")


def round_or_none(value, digits=2):
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except Exception:
        return None


def avg(values):
    nums = [float(x) for x in values if x is not None]
    if not nums:
        return None
    return sum(nums) / len(nums)


def median(values):
    nums = sorted([float(x) for x in values if x is not None])
    if not nums:
        return None
    n = len(nums)
    mid = n // 2
    if n % 2 == 0:
        return (nums[mid - 1] + nums[mid]) / 2
    return nums[mid]


def pct(new, old):
    try:
        if new is None or old in (None, 0):
            return None
        return ((new - old) / old) * 100
    except Exception:
        return None


def get_day_path_from_polygon(ticker, date_str, prev_close):
    try:
        date_only = date_str.split(" ")[0]

        url = (
            f"https://api.polygon.io/v2/aggs/ticker/"
            f"{ticker}/range/1/minute/"
            f"{date_only}/{date_only}"
        )

        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": POLYGON_API_KEY
        }

        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        results = data.get("results", [])
        if not results:
            return {
                "pm_high": None,
                "pm_low": None,
                "pm_high_pct": None,
                "pm_low_pct": None,
                "path_points": []
            }

        pm_bars = []
        market_bars = []

        for bar in results:
            ts = bar.get("t")
            if ts is None:
                continue

            dt_utc = datetime.fromtimestamp(ts / 1000, tz=ZoneInfo("UTC"))
            dt_ny = dt_utc.astimezone(ZoneInfo("America/New_York"))

            total_minutes = dt_ny.hour * 60 + dt_ny.minute

            item = {
                "time": dt_ny.strftime("%H:%M"),
                "open": bar.get("o"),
                "high": bar.get("h"),
                "low": bar.get("l"),
                "close": bar.get("c"),
            }

            # Premarket 04:00 - 09:29 NY
            if 240 <= total_minutes <= 569:
                pm_bars.append(item)

            # Regular Market 09:30 - 16:00 NY
            if 570 <= total_minutes <= 960:
                market_bars.append(item)

        def pct_from_prev(price):
            try:
                if price is None or prev_close in (None, 0):
                    return None
                return round(((float(price) - float(prev_close)) / float(prev_close)) * 100, 2)
            except Exception:
                return None

        pm_high_bar = max(pm_bars, key=lambda x: x["high"]) if pm_bars else None
        pm_low_bar = min(pm_bars, key=lambda x: x["low"]) if pm_bars else None
        market_high_bar = max(market_bars, key=lambda x: x["high"]) if market_bars else None
        market_low_bar = min(market_bars, key=lambda x: x["low"]) if market_bars else None

        open_bar = market_bars[0] if market_bars else None
        close_bar = market_bars[-1] if market_bars else None
        # ORB 5 minutos: 09:30 - 09:34 NY
        orb_5_bars = market_bars[:5] if len(market_bars) >= 5 else market_bars

        orb_high = None
        orb_low = None
        orb_high_break_time = None
        orb_low_break_time = None
        orb_high_broken = False
        orb_low_broken = False

        if orb_5_bars:
            orb_high = max([b["high"] for b in orb_5_bars if b["high"] is not None], default=None)
            orb_low = min([b["low"] for b in orb_5_bars if b["low"] is not None], default=None)

            after_orb_bars = market_bars[5:] if len(market_bars) > 5 else []

            if orb_high is not None:
                for bar in after_orb_bars:
                    if bar["high"] is not None and float(bar["high"]) > float(orb_high):
                        orb_high_broken = True
                        orb_high_break_time = bar["time"]
                        break

            if orb_low is not None:
                for bar in after_orb_bars:
                    if bar["low"] is not None and float(bar["low"]) < float(orb_low):
                        orb_low_broken = True
                        orb_low_break_time = bar["time"]
                        break
        pm_high_break_time = None
        pm_low_break_time = None
        pm_high_broken = False
        pm_low_broken = False

        if pm_high_bar and market_bars:
            pm_high_value = pm_high_bar["high"]

            for bar in market_bars:
                if bar["high"] is not None and float(bar["high"]) > float(pm_high_value):
                    pm_high_broken = True
                    pm_high_break_time = bar["time"]
                    break

        if pm_low_bar and market_bars:
            pm_low_value = pm_low_bar["low"]

            for bar in market_bars:
                if bar["low"] is not None and float(bar["low"]) < float(pm_low_value):
                    pm_low_broken = True
                    pm_low_break_time = bar["time"]
                    break

        close_above_pm_high = False
        close_below_pm_low = False

        if close_bar and pm_high_bar:
            close_above_pm_high = float(close_bar["close"]) > float(pm_high_bar["high"])

        if close_bar and pm_low_bar:
            close_below_pm_low = float(close_bar["close"]) < float(pm_low_bar["low"])

        path_points = []

        path_points.append({
            "label": "Prev Close",
            "time": "Prev",
            "pct": 0
        })

        raw_points = []

        if pm_low_bar:
            raw_points.append({
                "label": "PM Low",
                "time": pm_low_bar["time"],
                "pct": pct_from_prev(pm_low_bar["low"])
            })

        if pm_high_bar:
            raw_points.append({
                "label": "PM High",
                "time": pm_high_bar["time"],
                "pct": pct_from_prev(pm_high_bar["high"])
            })

        if open_bar:
            raw_points.append({
                "label": "Open",
                "time": open_bar["time"],
                "pct": pct_from_prev(open_bar["open"])
            })

        if market_low_bar:
            raw_points.append({
                "label": "MKT Low",
                "time": market_low_bar["time"],
                "pct": pct_from_prev(market_low_bar["low"])
            })

        if market_high_bar:
            raw_points.append({
                "label": "MKT High",
                "time": market_high_bar["time"],
                "pct": pct_from_prev(market_high_bar["high"])
            })

        if close_bar:
            raw_points.append({
                "label": "Close",
                "time": close_bar["time"],
                "pct": pct_from_prev(close_bar["close"])
            })

        def minutes_from_label(p):
            if p["time"] == "Prev":
                return -1
            h, m = p["time"].split(":")
            return int(h) * 60 + int(m)

        raw_points = [p for p in raw_points if p["pct"] is not None]
        raw_points = sorted(raw_points, key=minutes_from_label)

        path_points.extend(raw_points)

        return {
            "pm_high": pm_high_bar["high"] if pm_high_bar else None,
            "pm_low": pm_low_bar["low"] if pm_low_bar else None,
            "pm_high_pct": pct_from_prev(pm_high_bar["high"]) if pm_high_bar else None,
            "pm_low_pct": pct_from_prev(pm_low_bar["low"]) if pm_low_bar else None,

            "pm_high_time": pm_high_bar["time"] if pm_high_bar else None,
            "pm_low_time": pm_low_bar["time"] if pm_low_bar else None,
            "market_high_time": market_high_bar["time"] if market_high_bar else None,
            "market_low_time": market_low_bar["time"] if market_low_bar else None,
            "open_time": open_bar["time"] if open_bar else None,
            "close_time": close_bar["time"] if close_bar else None,

            "orb_high": orb_high,
            "orb_low": orb_low,
            "orb_high_broken": orb_high_broken,
            "orb_high_break_time": orb_high_break_time,
            "orb_low_broken": orb_low_broken,
            "orb_low_break_time": orb_low_break_time,

            "pm_high_broken": pm_high_broken,
            "pm_high_break_time": pm_high_break_time,
            "close_above_pm_high": close_above_pm_high,

            "pm_low_broken": pm_low_broken,
            "pm_low_break_time": pm_low_break_time,
            "close_below_pm_low": close_below_pm_low,

            "path_points": path_points
                    }

    except Exception as e:
        print("ERROR get_day_path_from_polygon:", e)
        return {
            "pm_high": None,
            "pm_low": None,
            "pm_high_pct": None,
            "pm_low_pct": None,
            "path_points": []
        }


def get_bucket_name(abs_gap_pct):
    if abs_gap_pct is None:
        return None
    if 5 <= abs_gap_pct < 10:
        return "5-10%"
    if 10 <= abs_gap_pct < 20:
        return "10-20%"
    if abs_gap_pct >= 20:
        return "20%+"
    return None


def build_bucket_summary(events):
    bucket_names = ["5-10%", "10-20%", "20%+"]
    summary = []

    for bucket in bucket_names:
        bucket_events = [e for e in events if e.get("gap_bucket") == bucket]
        total = len(bucket_events)

        green = len([e for e in bucket_events if e["color"] == "green"])
        red = len([e for e in bucket_events if e["color"] == "red"])
        flat = len([e for e in bucket_events if e["color"] == "flat"])
        holds = len([e for e in bucket_events if e["holds_gap"]])
        fills = len([e for e in bucket_events if e["fills_gap"]])

        summary.append({
            "bucket": bucket,
            "count": total,
            "green_pct": round_or_none((green / total) * 100, 2) if total else 0,
            "red_pct": round_or_none((red / total) * 100, 2) if total else 0,
            "flat_pct": round_or_none((flat / total) * 100, 2) if total else 0,
            "hold_gap_pct": round_or_none((holds / total) * 100, 2) if total else 0,
            "fill_gap_pct": round_or_none((fills / total) * 100, 2) if total else 0,
            "avg_open_to_close_pct": round_or_none(avg([e["open_to_close_pct"] for e in bucket_events]), 2),
            "avg_close_vs_prev_close_pct": round_or_none(avg([e["close_vs_prev_close_pct"] for e in bucket_events]), 2),
        })

    return summary


def compute_bias(stats):
    gap_type = stats.get("gap_type", "up")
    gap_days = stats.get("gap_days", 0)

    if gap_days < 3:
        return {
            "label": "Inconclusive",
            "class": "bias-neutral",
            "reason": "Muy pocos eventos para sacar una conclusión fiable."
        }

    green_pct = stats.get("green_pct", 0) or 0
    red_pct = stats.get("red_pct", 0) or 0
    hold_gap_pct = stats.get("hold_gap_pct", 0) or 0
    fill_gap_pct = stats.get("fill_gap_pct", 0) or 0
    avg_open_to_close = stats.get("avg_open_to_close_pct", 0) or 0

    if gap_type == "up":
        long_score = 0
        short_score = 0

        if green_pct >= 55:
            long_score += 1
        if hold_gap_pct >= 55:
            long_score += 1
        if avg_open_to_close > 0:
            long_score += 1

        if red_pct >= 55:
            short_score += 1
        if fill_gap_pct >= 55:
            short_score += 1
        if avg_open_to_close < 0:
            short_score += 1

        if long_score >= 2 and long_score > short_score:
            return {
                "label": "Long Bias",
                "class": "bias-long",
                "reason": "Tras gap up, suele mantener el gap y cerrar con comportamiento alcista."
            }

        if short_score >= 2 and short_score > long_score:
            return {
                "label": "Short Bias",
                "class": "bias-short",
                "reason": "Tras gap up, suele hacer fade, rellenar gap o cerrar débil."
            }

    else:
        long_score = 0
        short_score = 0

        if red_pct >= 55:
            long_score += 1
        if fill_gap_pct >= 55:
            long_score += 1
        if avg_open_to_close > 0:
            long_score += 1

        if green_pct >= 55:
            short_score += 1
        if hold_gap_pct >= 55:
            short_score += 1
        if avg_open_to_close < 0:
            short_score += 1

        if long_score >= 2 and long_score > short_score:
            return {
                "label": "Long Bias",
                "class": "bias-long",
                "reason": "Tras gap down, suele rebotar o recuperar parte del movimiento."
            }

        if short_score >= 2 and short_score > long_score:
            return {
                "label": "Short Bias",
                "class": "bias-short",
                "reason": "Tras gap down, suele seguir cayendo y mantener debilidad."
            }

    return {
        "label": "Mixed",
        "class": "bias-mixed",
        "reason": "El comportamiento histórico está mezclado y no muestra una ventaja clara."
    }


def build_gap_stats(ticker: str, gap_percent: float = 5, period_key: str = "1y", gap_type: str = "up"):
    ticker = (ticker or "").upper().strip()
    gap_type = (gap_type or "up").lower().strip()

    if not ticker:
        return {"error": "Debes introducir un ticker."}

    period = get_period_string(period_key)

    try:
        df = yf.download(
            ticker,
            period=period,
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
    except Exception as e:
        return {"error": f"Error descargando datos: {e}"}

    if df is None or df.empty:
        return {"error": "No se pudieron obtener datos históricos."}

    df = flatten_columns(df)

    needed_cols = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed_cols:
        if col not in df.columns:
            return {"error": f"Falta la columna {col} en los datos descargados."}

    df = df.dropna(subset=needed_cols)
    if len(df) < 2:
        return {"error": "No hay suficientes datos para calcular estadísticas."}

    events = []

    similarity_matches = []

    for i in range(1, len(df)):
        prev_close = safe_scalar(df["Close"].iloc[i - 1])
        row = df.iloc[i]

        open_price = safe_scalar(row["Open"])
        high_price = safe_scalar(row["High"])
        low_price = safe_scalar(row["Low"])
        close_price = safe_scalar(row["Close"])
        volume = safe_scalar(row["Volume"])

        if prev_close in (None, 0) or open_price is None:
            continue

        gap_pct = pct(open_price, prev_close)
        if gap_pct is None:
            continue

        if gap_type == "up":
            if gap_pct < float(gap_percent):
                continue
        else:
            if gap_pct > -float(gap_percent):
                continue

        open_to_close = pct(close_price, open_price)
        open_to_high = pct(high_price, open_price)
        open_to_low = pct(low_price, open_price)
        close_vs_prev_close = pct(close_price, prev_close)

        event_date = df.index[i].strftime("%Y-%m-%d")

        day_path = get_day_path_from_polygon(ticker, event_date, prev_close)

        pm_high = day_path.get("pm_high")
        pm_low = day_path.get("pm_low")
        pm_high_pct = day_path.get("pm_high_pct")
        pm_low_pct = day_path.get("pm_low_pct")
        path_points = day_path.get("path_points", [])
        pm_high_time = day_path.get("pm_high_time")
        pm_low_time = day_path.get("pm_low_time")
        market_high_time = day_path.get("market_high_time")
        market_low_time = day_path.get("market_low_time")
        open_time = day_path.get("open_time")
        close_time = day_path.get("close_time")
        orb_high = day_path.get("orb_high")
        orb_low = day_path.get("orb_low")
        orb_high_broken = day_path.get("orb_high_broken", False)
        orb_high_break_time = day_path.get("orb_high_break_time")
        orb_low_broken = day_path.get("orb_low_broken", False)
        orb_low_break_time = day_path.get("orb_low_break_time")
        pm_high_broken = day_path.get("pm_high_broken", False)
        pm_high_break_time = day_path.get("pm_high_break_time")
        close_above_pm_high = day_path.get("close_above_pm_high", False)

        pm_low_broken = day_path.get("pm_low_broken", False)
        pm_low_break_time = day_path.get("pm_low_break_time")
        close_below_pm_low = day_path.get("close_below_pm_low", False)

        if gap_type == "up":
            color = "green" if close_price > open_price else "red" if close_price < open_price else "flat"
            holds_gap = close_price > prev_close
            fills_gap = low_price <= prev_close
        else:
            color = "green" if close_price < open_price else "red" if close_price > open_price else "flat"
            holds_gap = close_price < prev_close
            fills_gap = high_price >= prev_close

        abs_gap_pct = abs(gap_pct)
        gap_bucket = get_bucket_name(abs_gap_pct)

        pattern_type = "Mixed"
        pattern_score = 50

        market_high_time_bucket = None
        market_low_time_bucket = None

        def classify_time_bucket(time_str):
            if not time_str or ":" not in time_str:
                return None

            try:
                h, m = time_str.split(":")
                total = int(h) * 60 + int(m)

                if 570 <= total < 600:
                    return "early"

                if 600 <= total < 780:
                    return "mid"

                if 780 <= total <= 960:
                    return "late"

            except Exception:
                return None

        market_high_time_bucket = classify_time_bucket(market_high_time)
        market_low_time_bucket = classify_time_bucket(market_low_time)

        pm_extension_vs_pm_high = None

        if pm_high_pct is not None and gap_pct is not None:
            pm_extension_vs_pm_high = pm_high_pct - gap_pct

        market_high_before_low = False
        market_low_before_high = False

        if market_high_time and market_low_time:
            market_high_before_low = market_high_time < market_low_time
            market_low_before_high = market_low_time < market_high_time

        # =========================
        # GAP & GO
        # =========================

        if (
                pm_high_broken
                and pm_extension_vs_pm_high is not None
                and pm_extension_vs_pm_high >= 30
                and color == "green"
                and close_vs_prev_close > gap_pct
        ):
            pattern_type = "Gap & Go"
            pattern_score = 95

        # =========================
        # GAP & EXTENSION
        # =========================

        elif (
                not pm_high_broken
                and market_high_before_low
                and color == "green"
                and open_to_high is not None
                and open_to_high > 10
        ):
            pattern_type = "Gap & Extension"
            pattern_score = 82

        # =========================
        # PM HIGH FAIL FADE
        # =========================

        elif (
                pm_high_broken
                and pm_extension_vs_pm_high is not None
                and pm_extension_vs_pm_high < 15
                and market_high_before_low
                and color == "red"
        ):
            pattern_type = "PM High Fail Fade"
            pattern_score = 92

        # =========================
        # PM SPIKE DUMP
        # =========================

        elif (
                pm_high_pct is not None
                and gap_pct is not None
                and pm_high_pct > (gap_pct * 2)
                and color == "red"
        ):
            pattern_type = "PM Spike Dump"
            pattern_score = 88

        # =========================
        # RUNNER ALL DAY
        # =========================

        elif (
                pm_high_pct is not None
                and pm_high_pct < 15
                and market_low_before_high
                and market_low_time_bucket == "early"
                and market_high_time_bucket == "late"
                and color == "green"
        ):
            pattern_type = "Runner All Day"
            pattern_score = 94

        # =========================
        # EARLY SPIKE RUNNER FADE
        # =========================

        elif (
                market_high_time_bucket == "early"
                and color == "green"
                and close_vs_prev_close is not None
                and open_to_high is not None
                and close_vs_prev_close < open_to_high
        ):
            pattern_type = "Early Spike Runner Fade"
            pattern_score = 76

        # =========================
        # OPEN FLUSH REVERSAL
        # =========================

        elif (
                open_to_low is not None
                and open_to_low < -10
                and color == "green"
        ):
            pattern_type = "Open Flush Reversal"
            pattern_score = 80

        # =========================
        # GAP FADE
        # =========================

        elif (
                fills_gap
                and color == "red"
        ):
            pattern_type = "Gap Fade"
            pattern_score = 85

        # =========================
        # BULL TRAP
        # =========================

        elif (
                pm_high_broken
                and color == "red"
                and close_above_pm_high is False
        ):
            pattern_type = "Bull Trap"
            pattern_score = 90

        event = {
            "date": event_date,
            "prev_close": round_or_none(prev_close, 4),
            "open": round_or_none(open_price, 4),
            "high": round_or_none(high_price, 4),
            "low": round_or_none(low_price, 4),
            "close": round_or_none(close_price, 4),
            "volume": int(volume) if volume is not None else None,
            "gap_pct": round_or_none(gap_pct, 2),
            "abs_gap_pct": round_or_none(abs_gap_pct, 2),
            "open_to_close_pct": round_or_none(open_to_close, 2),
            "open_to_high_pct": round_or_none(open_to_high, 2),
            "open_to_low_pct": round_or_none(open_to_low, 2),
            "close_vs_prev_close_pct": round_or_none(close_vs_prev_close, 2),
            "pm_high": round_or_none(pm_high, 4),
            "pm_low": round_or_none(pm_low, 4),
            "pm_high_pct": round_or_none(pm_high_pct, 2),
            "pm_low_pct": round_or_none(pm_low_pct, 2),
            "path_points": path_points,
            "pm_high_time": pm_high_time,
            "pm_low_time": pm_low_time,
            "market_high_time": market_high_time,
            "market_low_time": market_low_time,
            "open_time": open_time,
            "close_time": close_time,
            "orb_high": round_or_none(orb_high, 4),
            "orb_low": round_or_none(orb_low, 4),
            "orb_high_broken": orb_high_broken,
            "orb_high_break_time": orb_high_break_time,
            "orb_low_broken": orb_low_broken,
            "orb_low_break_time": orb_low_break_time,
            "pm_high_broken": pm_high_broken,
            "pm_high_break_time": pm_high_break_time,
            "close_above_pm_high": close_above_pm_high,

            "pm_low_broken": pm_low_broken,
            "pm_low_break_time": pm_low_break_time,
            "close_below_pm_low": close_below_pm_low,
            "color": color,
            "holds_gap": holds_gap,
            "fills_gap": fills_gap,
            "gap_bucket": gap_bucket,
            "pattern_type": pattern_type,
            "pattern_score": pattern_score,
        }

        events.append(event)

        # =========================
        # SIMILARITY ENGINE
        # =========================

        similarity_matches = []

        if events:
            current_event = events[-1]

            for old_event in events[:-1]:

                score = 0

                try:
                    # GAP %
                    gap_diff = abs(
                        (current_event.get("gap_pct") or 0) -
                        (old_event.get("gap_pct") or 0)
                    )

                    score += max(0, 100 - gap_diff)

                    # PM HIGH %
                    pmh_diff = abs(
                        (current_event.get("pm_high_pct") or 0) -
                        (old_event.get("pm_high_pct") or 0)
                    )

                    score += max(0, 100 - pmh_diff)

                    # O→C
                    oc_diff = abs(
                        (current_event.get("open_to_close_pct") or 0) -
                        (old_event.get("open_to_close_pct") or 0)
                    )

                    score += max(0, 100 - oc_diff)

                    # O→H
                    oh_diff = abs(
                        (current_event.get("open_to_high_pct") or 0) -
                        (old_event.get("open_to_high_pct") or 0)
                    )

                    score += max(0, 100 - oh_diff)

                    # O→L
                    ol_diff = abs(
                        (current_event.get("open_to_low_pct") or 0) -
                        (old_event.get("open_to_low_pct") or 0)
                    )

                    score += max(0, 100 - ol_diff)

                    final_score = round(score / 5, 2)

                    similarity_matches.append({
                        "date": old_event.get("date"),
                        "pattern": old_event.get("pattern_type"),
                        "score": final_score,
                        "gap_pct": old_event.get("gap_pct"),
                        "close_vs_prev_close_pct": old_event.get("close_vs_prev_close_pct"),
                        "holds_gap": old_event.get("holds_gap"),
                        "fills_gap": old_event.get("fills_gap"),
                    })

                except Exception:
                    pass

            similarity_matches = sorted(
                similarity_matches,
                key=lambda x: x["score"],
                reverse=True
            )[:5]

    total_days = len(df)
    gap_days = len(events)

    green_events = [e for e in events if e["color"] == "green"]
    red_events = [e for e in events if e["color"] == "red"]
    flat_events = [e for e in events if e["color"] == "flat"]
    hold_events = [e for e in events if e["holds_gap"]]
    fill_events = [e for e in events if e["fills_gap"]]

    open_to_close_values = [e["open_to_close_pct"] for e in events]
    open_to_high_values = [e["open_to_high_pct"] for e in events]
    open_to_low_values = [e["open_to_low_pct"] for e in events]
    close_vs_prev_close_values = [e["close_vs_prev_close_pct"] for e in events]
    gap_values = [e["gap_pct"] for e in events]

    bucket_summary = build_bucket_summary(events)

    pm_high_broken_events = [e for e in events if e.get("pm_high_broken")]
    close_above_pm_high_events = [e for e in events if e.get("close_above_pm_high")]
    orb_high_broken_events = [e for e in events if e.get("orb_high_broken")]

    pm_high_broken_pct = round_or_none((len(pm_high_broken_events) / gap_days) * 100, 2) if gap_days else 0
    close_above_pm_high_pct = round_or_none((len(close_above_pm_high_events) / gap_days) * 100, 2) if gap_days else 0
    orb_high_broken_pct = round_or_none((len(orb_high_broken_events) / gap_days) * 100, 2) if gap_days else 0

    def hour_bucket(time_str):
        if not time_str or ":" not in time_str:
            return None

        try:
            h, m = time_str.split(":")
            total = int(h) * 60 + int(m)

            if 570 <= total < 600:
                return "09:30-10:00"
            if 600 <= total < 660:
                return "10:00-11:00"
            if 660 <= total < 720:
                return "11:00-12:00"
            if 720 <= total < 780:
                return "12:00-13:00"
            if 780 <= total < 840:
                return "13:00-14:00"
            if 840 <= total < 900:
                return "14:00-15:00"
            if 900 <= total <= 960:
                return "15:00-16:00"

            return "Other"
        except Exception:
            return None

    def build_time_histogram(time_field):
        labels = [
            "09:30-10:00",
            "10:00-11:00",
            "11:00-12:00",
            "12:00-13:00",
            "13:00-14:00",
            "14:00-15:00",
            "15:00-16:00"
        ]

        total = len([e for e in events if e.get(time_field)])

        rows = []

        for label in labels:
            count = len([
                e for e in events
                if hour_bucket(e.get(time_field)) == label
            ])

            rows.append({
                "bucket": label,
                "count": count,
                "pct": round_or_none((count / total) * 100, 2) if total else 0
            })

        return rows

    high_time_histogram = build_time_histogram("market_high_time")
    low_time_histogram = build_time_histogram("market_low_time")

    avg_pm_low = avg([
        e.get("pm_low_pct")
        for e in events
    ])

    avg_pm_high = avg([
        e.get("pm_high_pct")
        for e in events
    ])

    avg_open = avg([
        e.get("gap_pct")
        for e in events
    ])

    avg_market_low = avg([
        e.get("open_to_low_pct")
        for e in events
    ])

    avg_market_high = avg([
        e.get("open_to_high_pct")
        for e in events
    ])

    avg_close = avg([
        e.get("close_vs_prev_close")
        for e in events
    ])

    average_path = {
        "labels": [
            "Prev Close",
            "Avg PM Low",
            "Avg PM High",
            "Avg Open",
            "Avg MKT Low",
            "Avg MKT High",
            "Avg Close"
        ],

        "values": [
            0,
            round_or_none(avg_pm_low, 2),
            round_or_none(avg_pm_high, 2),
            round_or_none(avg_open, 2),
            round_or_none(avg_market_low, 2),
            round_or_none(avg_market_high, 2),
            round_or_none(avg_close, 2),
        ]
    }

    stats = {
        "ticker": ticker,
        "period_key": period_key,
        "period_label": {
            "6m": "6 meses",
            "1y": "1 año",
            "2y": "2 años",
            "5y": "5 años",
        }.get(period_key, "1 año"),
        "gap_percent": float(gap_percent),
        "gap_type": gap_type,
        "gap_type_label": "Gap Up" if gap_type == "up" else "Gap Down",
        "days_analyzed": total_days,
        "gap_days": gap_days,
        "green_closes": len(green_events),
        "red_closes": len(red_events),
        "flat_closes": len(flat_events),
        "hold_gap_count": len(hold_events),
        "fill_gap_count": len(fill_events),
        "green_pct": round_or_none((len(green_events) / gap_days) * 100, 2) if gap_days else 0,
        "red_pct": round_or_none((len(red_events) / gap_days) * 100, 2) if gap_days else 0,
        "flat_pct": round_or_none((len(flat_events) / gap_days) * 100, 2) if gap_days else 0,
        "hold_gap_pct": round_or_none((len(hold_events) / gap_days) * 100, 2) if gap_days else 0,
        "fill_gap_pct": round_or_none((len(fill_events) / gap_days) * 100, 2) if gap_days else 0,
        "avg_gap_pct": round_or_none(avg(gap_values), 2),
        "avg_open_to_close_pct": round_or_none(avg(open_to_close_values), 2),
        "median_open_to_close_pct": round_or_none(median(open_to_close_values), 2),
        "avg_open_to_high_pct": round_or_none(avg(open_to_high_values), 2),
        "avg_open_to_low_pct": round_or_none(avg(open_to_low_values), 2),
        "avg_close_vs_prev_close_pct": round_or_none(avg(close_vs_prev_close_values), 2),
        "median_close_vs_prev_close_pct": round_or_none(median(close_vs_prev_close_values), 2),
    }

    continuation_score = 0

    continuation_score += min((stats.get("hold_gap_pct", 0) or 0) * 0.25, 25)
    continuation_score += min((stats.get("green_pct", 0) or 0) * 0.25, 25)
    continuation_score += min((pm_high_broken_pct or 0) * 0.20, 20)
    continuation_score += min((close_above_pm_high_pct or 0) * 0.15, 15)
    continuation_score += min((orb_high_broken_pct or 0) * 0.15, 15)

    continuation_score = round_or_none(continuation_score, 1)

    if continuation_score >= 70:
        continuation_label = "Strong Continuation"
        continuation_class = "bias-long"
    elif continuation_score >= 50:
        continuation_label = "Moderate Continuation"
        continuation_class = "bias-mixed"
    else:
        continuation_label = "Weak Continuation / Fade Risk"
        continuation_class = "bias-short"

    continuation = {
        "score": continuation_score,
        "label": continuation_label,
        "class": continuation_class,
        "pm_high_broken_pct": pm_high_broken_pct,
        "close_above_pm_high_pct": close_above_pm_high_pct,
        "orb_high_broken_pct": orb_high_broken_pct,
    }

    bias = compute_bias(stats)

    return {
        "stats": stats,
        "bias": bias,
        "continuation": continuation,
        "events": sorted(events, key=lambda x: x["date"], reverse=True),
        "bucket_summary": bucket_summary,
        "average_path": average_path,
        "high_time_histogram": high_time_histogram,
        "low_time_histogram": low_time_histogram,
        "similarity_matches": similarity_matches,
    }
