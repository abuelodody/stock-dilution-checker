import yfinance as yf


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
        if old in (None, 0):
            return None
        return ((new - old) / old) * 100
    except Exception:
        return None


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

        event = {
            "date": df.index[i].strftime("%Y-%m-%d"),
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
            "color": color,
            "holds_gap": holds_gap,
            "fills_gap": fills_gap,
            "gap_bucket": gap_bucket,
        }
        events.append(event)

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

    bias = compute_bias(stats)

    return {
        "stats": stats,
        "bias": bias,
        "events": sorted(events, key=lambda x: x["date"], reverse=True),
        "bucket_summary": bucket_summary,
    }
