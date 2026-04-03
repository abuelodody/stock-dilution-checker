import sqlite3
import json
import os
import re
import math
import csv
import io
from flask import render_template
from datetime import datetime, timedelta
from html import escape

import requests
import yfinance as yf
from bs4 import BeautifulSoup
from flask import Flask, request, redirect, url_for, render_template, jsonify
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import check_password_hash, generate_password_hash

from services.screener_service import build_gainers, build_momentum
from services.market_scanner import scan_market
from services.gap_stats_service import build_gap_stats

TWELVEDATA_API_KEY = "18ba8881934b4b84b18577fb193d0524"

app = Flask(__name__)

DB_FILE = "database.db"

def get_db():
    return sqlite3.connect(DB_FILE)

def init_trades_table():
    conn = get_db()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        symbol TEXT,
        side TEXT,
        shares REAL,
        entry REAL,
        exit REAL,
        pnl REAL,
        fee REAL,
        setup TEXT,
        notes TEXT
    )
    """)

    conn.commit()
    conn.close()

# IMPORTANTE: crear tabla al iniciar
init_trades_table()

app.secret_key = os.environ.get("SECRET_KEY", "dev-key")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"

SEC_HEADERS = {
    "User-Agent": "SergioMomentumBot/1.0 (contact: tu_correo@example.com)",
    "Accept-Encoding": "gzip, deflate",
}

FINVIZ_HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

REQUEST_TIMEOUT = 20

PERSISTENT_DIR = os.environ.get("RENDER_DISK_PATH", "/var/data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)

DB_FILE = os.environ.get("DB_FILE", os.path.join(PERSISTENT_DIR, "app.db"))
STORAGE_FILE = os.environ.get("STORAGE_FILE", os.path.join(PERSISTENT_DIR, "storage.json"))

print("🔥 PERSISTENT_DIR =", PERSISTENT_DIR)
print("🔥 DB_FILE =", DB_FILE)
print("🔥 STORAGE_FILE =", STORAGE_FILE)


# LOGIN SIMPLE
class User(UserMixin):
    def __init__(self, user_id, username, is_admin=False):
        self.id = str(user_id)
        self.username = username
        self.is_admin = is_admin


@login_manager.user_loader
def load_user(user_id):
    conn = get_db_connection()
    row = conn.execute(
        "SELECT id, username, is_admin FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()
    conn.close()

    if row:
        return User(row["id"], row["username"], bool(row["is_admin"]))

    return None


# =========================
# STORAGE
# =========================
def load_storage():
    default_data = {
        "history": [],
        "favorites": [],
        "notes": {}
    }

    if not os.path.exists(STORAGE_FILE):
        return default_data.copy()

    try:
        with open(STORAGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            return default_data.copy()

        if "history" not in data or not isinstance(data["history"], list):
            data["history"] = []

        if "favorites" not in data or not isinstance(data["favorites"], list):
            data["favorites"] = []

        if "notes" not in data or not isinstance(data["notes"], dict):
            data["notes"] = {}

        return data
    except Exception:
        return default_data.copy()


def save_storage(data):
    safe_data = {
        "history": data.get("history", []),
        "favorites": data.get("favorites", []),
        "notes": data.get("notes", {})
    }

    with open(STORAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(safe_data, f, ensure_ascii=False, indent=2)


def init_storage():
    if not os.path.exists(STORAGE_FILE):
        save_storage({
            "history": [],
            "favorites": [],
            "notes": {}
        })


def toggle_favorite(ticker):
    storage = load_storage()
    favorites = [x.upper() for x in storage["favorites"]]
    ticker = ticker.upper()

    if ticker in favorites:
        favorites.remove(ticker)
    else:
        favorites.insert(0, ticker)

    storage["favorites"] = favorites[:30]
    save_storage(storage)


def is_favorite(ticker):
    storage = load_storage()
    return ticker.upper() in [x.upper() for x in storage["favorites"]]


def get_note(ticker):
    storage = load_storage()
    return storage.get("notes", {}).get(ticker.upper(), "")


def save_note(ticker, note):
    storage = load_storage()
    notes = storage.get("notes", {})
    ticker = ticker.upper().strip()

    if note.strip():
        notes[ticker] = note.strip()
    else:
        notes.pop(ticker, None)

    storage["notes"] = notes
    save_storage(storage)


# =========================
# HELPERS
# =========================
def safe_float(v, default=0):
    try:
        return float(v)
    except Exception:
        return default


def safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default


def compute_score(item):
    pct = safe_float(item.get("change_percent", 0))
    volume = safe_int(item.get("volume", 0))
    volume_factor = math.log10(volume + 1) * 10 if volume > 0 else 0
    return round((pct * 0.7) + (volume_factor * 0.3), 1)


def format_market_cap(value):
    if value in [None, "N/A"]:
        return "N/A"
    try:
        value = float(value)
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"{value / 1_000:.2f}K"
        return f"{value:,.0f}"
    except Exception:
        return str(value)


def get_db_connection():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.commit()
    conn.close()


def create_user(username, password, is_admin=False):
    password_hash = generate_password_hash(password)

    conn = get_db_connection()
    conn.execute(
        "INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)",
        (username, password_hash, int(is_admin))
    )
    conn.commit()
    conn.close()


init_db()
init_storage()

try:
    create_user("admin", "Ss02s52n1975o-!", True)
except sqlite3.IntegrityError:
    pass


def format_number(value):
    if value in [None, "N/A"]:
        return "N/A"
    try:
        value = float(value)
        if value >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        if value >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        if value >= 1_000:
            return f"{value / 1_000:.2f}K"
        return f"{value:,.0f}"
    except Exception:
        return str(value)


def format_percent(value):
    if value in [None, "N/A"]:
        return "N/A"
    try:
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return str(value)


def format_price(value):
    if value in [None, "", "N/A"]:
        return "N/A"
    try:
        return f"${float(value):.4f}"
    except Exception:
        return str(value)


def build_company_summary(text, max_chars=300):
    if not text or text == "N/A":
        return "No company description available."

    text = re.sub(r"\s+", " ", str(text)).strip()

    if len(text) <= max_chars:
        return text

    return text[:max_chars].rsplit(" ", 1)[0] + "..."


def safe_scalar(value):
    try:
        if hasattr(value, "iloc"):
            return value.iloc[0]
        return value
    except Exception:
        return value


def risk_badge_class(risk_level: str):
    risk_level = (risk_level or "").upper()
    if risk_level == "HIGH":
        return "risk-high"
    if risk_level == "MEDIUM":
        return "risk-medium"
    return "risk-low"


def get_max_volume_5y(ticker: str):
    try:
        stock = yf.Ticker(ticker)
        hist_5y = stock.history(period="5y", interval="1d", auto_adjust=False)

        if hist_5y is None or hist_5y.empty:
            return {
                "max_volume_5y": None,
                "max_volume_5y_date": None,
            }

        if "Volume" not in hist_5y.columns:
            return {
                "max_volume_5y": None,
                "max_volume_5y_date": None,
            }

        hist_5y = hist_5y.dropna(subset=["Volume"])
        if hist_5y.empty:
            return {
                "max_volume_5y": None,
                "max_volume_5y_date": None,
            }

        # Excluir la última fila para comparar contra el récord previo
        if len(hist_5y) > 1:
            hist_compare = hist_5y.iloc[:-1].copy()
        else:
            hist_compare = hist_5y.copy()

        if hist_compare.empty:
            return {
                "max_volume_5y": None,
                "max_volume_5y_date": None,
            }

        max_idx = hist_compare["Volume"].idxmax()
        max_volume = hist_compare.loc[max_idx, "Volume"]
        max_date = max_idx.strftime("%Y-%m-%d")

        return {
            "max_volume_5y": int(max_volume),
            "max_volume_5y_date": max_date,
        }

    except Exception as e:
        print("ERROR get_max_volume_5y:", e)
        return {
            "max_volume_5y": None,
            "max_volume_5y_date": None,
        }

        # Excluimos la última barra para comparar el volumen de hoy contra el récord previo
        if len(hist_5y) > 1:
            hist_compare = hist_5y.iloc[:-1].copy()
        else:
            hist_compare = hist_5y.copy()

        if hist_compare.empty:
            return {
                "max_volume_5y": None,
                "max_volume_5y_date": None,
            }

        max_idx = hist_compare["Volume"].idxmax()
        max_volume = safe_scalar(hist_compare.loc[max_idx, "Volume"])

        if hasattr(max_idx, "strftime"):
            max_date = max_idx.strftime("%Y-%m-%d")
        else:
            max_date = str(max_idx)

        return {
            "max_volume_5y": int(max_volume),
            "max_volume_5y_date": max_date,
        }

    except Exception:
        return {
            "max_volume_5y": None,
            "max_volume_5y_date": None,
        }


def calculate_daily_vwap_overhead(ticker: str, period="1y"):
    try:
        stock = yf.Ticker(ticker)

        hist = stock.history(period=period, interval="1d")

        if hist is None or hist.empty:
            return []

        hist["tp"] = (hist["High"] + hist["Low"] + hist["Close"]) / 3
        hist["cum_vol"] = hist["Volume"].cumsum()
        hist["cum_tp_vol"] = (hist["tp"] * hist["Volume"]).cumsum()
        hist["vwap"] = hist["cum_tp_vol"] / hist["cum_vol"]

        vwap_values = hist["vwap"].dropna().values

        if len(vwap_values) == 0:
            return []

        sorted_vwap = sorted(vwap_values, reverse=True)

        levels = []
        threshold = 0.03

        for v in sorted_vwap:
            if not levels:
                levels.append(v)
            else:
                if all(abs(v - x) / x > threshold for x in levels):
                    levels.append(v)

            if len(levels) >= 3:
                break

        return levels

    except Exception as e:
        print("ERROR OVERHEAD:", e)
        return []


def build_trader_conclusion(dilution_result, sec_status, news, price_detection):
    risk = dilution_result.get("risk_level", "LOW")
    flags = dilution_result.get("flags", [])

    has_offering_news = any(item.get("possible_offering") for item in news)
    has_today_news = any(item.get("is_today") for item in news)
    has_fresh_news = any(item.get("is_fresh") for item in news)

    has_atm = sec_status.get("has_atm", False)
    has_warrants = sec_status.get("has_warrants", False)
    has_resale = sec_status.get("has_resale", False)
    has_convertible = sec_status.get("has_convertible", False)
    has_equity_line = sec_status.get("has_equity_line", False)
    has_shelf = sec_status.get("has_shelf", False)
    has_prospectus = sec_status.get("has_prospectus", False)

    parts = []

    if risk == "HIGH":
        parts.append("High dilution risk.")
    elif risk == "MEDIUM":
        parts.append("Medium dilution risk.")
    else:
        parts.append("Lower dilution risk based on current signals.")

    structure = []
    if has_atm:
        structure.append("ATM")
    if has_warrants:
        structure.append("warrants")
    if has_resale:
        structure.append("resale")
    if has_convertible:
        structure.append("convertibles")
    if has_equity_line:
        structure.append("equity line")
    if has_shelf:
        structure.append("shelf")
    if has_prospectus:
        structure.append("prospectus")

    if structure:
        parts.append("SEC structure suggests paper overhead via " + ", ".join(structure) + ".")

    if has_offering_news:
        parts.append("Recent news also contains possible offering language.")

    if has_fresh_news:
        parts.append("Fresh news detected.")
    elif has_today_news:
        parts.append("News today detected.")

    price_bits = []
    if price_detection.get("offering_price"):
        price_bits.append(f"offering {format_price(price_detection['offering_price'])}")
    if price_detection.get("warrant_exercise_price"):
        price_bits.append(f"warrants {format_price(price_detection['warrant_exercise_price'])}")
    if price_detection.get("conversion_price"):
        price_bits.append(f"conversion {format_price(price_detection['conversion_price'])}")

    if price_bits:
        parts.append("Detected price references: " + ", ".join(price_bits) + ".")

    if not flags:
        parts.append("No major paper-risk flags were detected from the current scan.")

    return " ".join(parts)


def build_quick_flags(news, sec_status, dilution_result, price_detection):
    flags = []

    if dilution_result.get("risk_level") == "HIGH":
        flags.append(("HIGH RISK", "pill-red"))
    elif dilution_result.get("risk_level") == "MEDIUM":
        flags.append(("MEDIUM RISK", "pill-yellow"))
    else:
        flags.append(("LOW RISK", "pill-green"))

    if any(item.get("is_fresh") for item in news):
        flags.append(("FRESH NEWS", "pill-blue"))

    if any(item.get("is_today") for item in news):
        flags.append(("NEWS TODAY", "pill-blue"))

    if any(item.get("possible_offering") for item in news):
        flags.append(("OFFERING NEWS?", "pill-red"))

    if sec_status.get("has_atm"):
        flags.append(("ATM", "pill-red"))

    if sec_status.get("has_warrants"):
        flags.append(("WARRANTS", "pill-yellow"))

    if sec_status.get("has_resale"):
        flags.append(("RESALE", "pill-yellow"))

    if sec_status.get("has_convertible"):
        flags.append(("CONVERTIBLE", "pill-red"))

    if sec_status.get("has_equity_line"):
        flags.append(("EQUITY LINE", "pill-red"))

    if sec_status.get("has_shelf"):
        flags.append(("SHELF", "pill-yellow"))

    if sec_status.get("has_prospectus"):
        flags.append(("PROSPECTUS", "pill-yellow"))

    if price_detection.get("offering_price"):
        flags.append((f"OFFER ${float(price_detection['offering_price']):.2f}", "pill-red"))

    if price_detection.get("warrant_exercise_price"):
        flags.append((f"WARRANT ${float(price_detection['warrant_exercise_price']):.2f}", "pill-yellow"))

    if price_detection.get("conversion_price"):
        flags.append((f"CONV ${float(price_detection['conversion_price']):.2f}", "pill-red"))

    return flags


# =========================
# DATOS DE LA ACCIÓN
# =========================
def get_stock_data(ticker: str):
    try:
        ticker = ticker.upper().strip()

        hist = yf.download(
            ticker,
            period="5d",
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
        )

        if hist is None or hist.empty:
            return {"error": "No pude obtener histórico de Yahoo Finance."}

        last_close = safe_scalar(hist["Close"].iloc[-1])
        prev_close = safe_scalar(hist["Close"].iloc[-2]) if len(hist) >= 2 else "N/A"
        volume = safe_scalar(hist["Volume"].iloc[-1])
        open_price = safe_scalar(hist["Open"].iloc[-1])
        high = safe_scalar(hist["High"].iloc[-1])
        low = safe_scalar(hist["Low"].iloc[-1])

        company_name = ticker
        market_cap = "N/A"
        sector = "N/A"
        industry = "N/A"
        country = "N/A"
        business_summary = "No company description available."
        float_shares = "N/A"
        shares_outstanding = "N/A"
        institutional_ownership = "N/A"
        insider_ownership = "N/A"
        avg_volume = "N/A"
        rvol = "N/A"
        country_risk_class = "country-us"

        try:
            stock = yf.Ticker(ticker)
            info = stock.get_info()

            company_name = info.get("longName", ticker)
            market_cap = info.get("marketCap", "N/A")
            sector = info.get("sector", "N/A")
            industry = info.get("industry", "N/A")
            float_shares = info.get("floatShares", "N/A")
            shares_outstanding = info.get("sharesOutstanding", "N/A")
            institutional_ownership = info.get("heldPercentInstitutions", "N/A")
            insider_ownership = info.get("heldPercentInsiders", "N/A")
            avg_volume = info.get("averageVolume", "N/A")
            country = info.get("country", "N/A")
            business_summary = info.get("longBusinessSummary", "No company description available.")
            try:
                if avg_volume not in [None, "N/A", 0]:
                    rvol = round(float(volume) / float(avg_volume), 2)
            except Exception:
                rvol = "N/A"

        except Exception:
            pass

        max_vol_data = get_max_volume_5y(ticker)
        max_volume_5y = max_vol_data.get("max_volume_5y")
        max_volume_5y_date = max_vol_data.get("max_volume_5y_date")

        is_record_volume = False
        try:
            if volume not in [None, "N/A"] and max_volume_5y not in [None, "N/A"]:
                is_record_volume = float(volume) > float(max_volume_5y)
        except Exception:
            is_record_volume = False
        danger_countries = {
            "China": "country-danger",
            "Hong Kong": "country-danger",
            "Singapore": "country-danger",
            "Taiwan": "country-danger",
            "Cayman Islands": "country-danger"
        }

        if country == "United States":
            country_risk_class = "country-us"
        elif country in danger_countries:
            country_risk_class = "country-danger"
        elif country not in ["N/A", None, ""]:
            country_risk_class = "country-non-us"
        else:
            country_risk_class = "country-unknown"

        return {
            "symbol": ticker,
            "companyName": company_name,
            "price": round(float(last_close), 4),
            "prevClose": round(float(prev_close), 4) if prev_close != "N/A" else "N/A",
            "open": round(float(open_price), 4),
            "high": round(float(high), 4),
            "low": round(float(low), 4),
            "volume": int(volume),
            "avgVolume": avg_volume,
            "rvol": rvol,
            "marketCap": market_cap,
            "sector": sector,
            "industry": industry,
            "floatShares": float_shares,
            "sharesOutstanding": shares_outstanding,
            "institutionalOwnership": institutional_ownership,
            "insiderOwnership": insider_ownership,
            "maxVolume5Y": max_volume_5y,
            "maxVolume5YFormatted": format_number(max_volume_5y),
            "maxVolume5YDate": max_volume_5y_date,
            "isRecordVolume": is_record_volume,
            "country": country,
            "businessSummary": business_summary,
            "countryRiskClass": country_risk_class,
        }

    except Exception as e:
        return {"error": str(e)}


def get_twelvedata_intraday(symbol="AAPL", interval="1min", outputsize=120):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
        "prepost": "true"  # 🔥 ESTA ES LA CLAVE
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        if data.get("status") != "ok":
            return {"ok": False, "error": data.get("message", "Error con Twelve Data")}

        values = data.get("values", [])
        if not values:
            return {"ok": False, "error": "No hay datos"}

        return {"ok": True, "data": data}

    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_intraday_snapshot(symbol="AAPL"):
    result = get_twelvedata_intraday(symbol=symbol, interval="1min", outputsize=120)

    # 🔥 Precio realtime SIEMPRE
    realtime_price = get_realtime_price(symbol)

    # ❌ Si Twelve Data falla → fallback Yahoo
    if not result["ok"] or not result.get("data", {}).get("values"):

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1d", interval="1m")

            if hist is not None and not hist.empty:
                intraday_volume = int(hist["Volume"].sum())
                bars = len(hist)
            else:
                intraday_volume = "N/A"
                bars = 0

        except:
            intraday_volume = "N/A"
            bars = 0

        return {
            "price": realtime_price,
            "intraday_volume": intraday_volume,
            "bars": bars,
            "error": "Fallback Yahoo"
        }

    # ✅ Si Twelve Data funciona
    data = result["data"]
    values = data["values"]

    total_volume = 0
    for bar in values:
        try:
            total_volume += int(float(bar.get("volume", 0)))
        except:
            pass

    return {
        "price": realtime_price,
        "intraday_volume": total_volume,
        "bars": len(values),
        "error": None
    }


def get_realtime_price(symbol="AAPL"):
    url = "https://api.twelvedata.com/price"
    params = {
        "symbol": symbol,
        "apikey": TWELVEDATA_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        if "price" in data:
            return data["price"]

        return "N/A"

    except Exception:
        return "N/A"


# =========================
# FINVIZ NEWS
# =========================
def parse_finviz_datetime(raw_text: str):
    raw_text = raw_text.strip()
    now = datetime.now()

    try:
        lower = raw_text.lower()

        if lower.startswith("today"):
            time_part = raw_text.split()[-1]
            dt = datetime.strptime(time_part, "%I:%M%p")
            return now.replace(hour=dt.hour, minute=dt.minute, second=0, microsecond=0)

        if lower.startswith("yesterday"):
            time_part = raw_text.split()[-1]
            dt = datetime.strptime(time_part, "%I:%M%p")
            yesterday = now - timedelta(days=1)
            return yesterday.replace(hour=dt.hour, minute=dt.minute, second=0, microsecond=0)

        return datetime.strptime(raw_text, "%b-%d-%y %I:%M%p")
    except Exception:
        return None


def get_stock_news(ticker: str, limit: int = 3):
    try:
        url = f"https://finviz.com/quote.ashx?t={ticker}"
        response = requests.get(url, headers=FINVIZ_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        news_table = soup.find(id="news-table") or soup.find("table", class_="news-table")
        if not news_table:
            return []

        results = []
        current_date_label = ""

        for row in news_table.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) < 2:
                continue

            date_text = cols[0].get_text(" ", strip=True)
            link_tag = cols[1].find("a")
            if not link_tag:
                continue

            title = link_tag.get_text(" ", strip=True)
            link = link_tag.get("href", "").strip()

            if link.startswith("/"):
                link = f"https://finviz.com{link}"

            parts = date_text.split()
            if len(parts) == 2:
                current_date_label = parts[0]
                final_date_text = date_text
            elif len(parts) == 1:
                final_date_text = f"{current_date_label} {parts[0]}" if current_date_label else parts[0]
            else:
                final_date_text = date_text

            parsed_dt = parse_finviz_datetime(final_date_text)

            is_today = False
            is_fresh = False
            pretty_date = final_date_text

            if parsed_dt:
                now = datetime.now()
                is_today = parsed_dt.date() == now.date()
                is_fresh = (now - parsed_dt) <= timedelta(hours=2)
                pretty_date = parsed_dt.strftime("%d-%m-%Y %H:%M")

            offering_keywords = [
                "offering", "atm", "warrant", "direct offering",
                "registered direct", "prospectus", "shelf", "resale",
                "convertible", "equity line", "purchase agreement",
                "sales agreement", "prospectus supplement"
            ]

            title_lower = title.lower()
            possible_offering = any(word in title_lower for word in offering_keywords)

            results.append({
                "title": title,
                "link": link,
                "publisher": "Finviz",
                "date": pretty_date,
                "is_today": is_today,
                "is_fresh": is_fresh,
                "possible_offering": possible_offering,
            })

            if len(results) >= limit:
                break

        return results

    except Exception:
        return []


# =========================
# SEC
# =========================
def get_cik_from_ticker(ticker: str):
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        response = requests.get(url, headers=SEC_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        ticker_upper = ticker.upper().strip()

        for _, company in data.items():
            sec_ticker = str(company.get("ticker", "")).upper()
            if sec_ticker == ticker_upper:
                cik_str = str(company.get("cik_str", "")).zfill(10)
                title = company.get("title", "")
                return {"cik": cik_str, "title": title}

        return None
    except Exception:
        return None


def get_recent_sec_filings(ticker: str, limit: int = 20):
    cik_data = get_cik_from_ticker(ticker)
    if not cik_data:
        return None

    cik = cik_data["cik"]

    try:
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(url, headers=SEC_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        dates = recent.get("filingDate", [])
        accession_numbers = recent.get("accessionNumber", [])
        primary_documents = recent.get("primaryDocument", [])
        primary_doc_desc = recent.get("primaryDocDescription", [])

        results = []
        max_len = min(
            len(forms),
            len(dates),
            len(accession_numbers),
            len(primary_documents),
            limit
        )

        for i in range(max_len):
            accession = accession_numbers[i]
            accession_clean = accession.replace("-", "")

            filing_link = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{accession_clean}/{primary_documents[i]}"
            )

            index_link = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{accession_clean}/{accession}-index.htm"
            )

            results.append({
                "form": forms[i],
                "date": dates[i],
                "accession": accession,
                "primaryDocument": primary_documents[i],
                "description": primary_doc_desc[i] if i < len(primary_doc_desc) else "",
                "link": filing_link,
                "index_link": index_link,
            })

        return results

    except Exception:
        return []


def fetch_filing_text(url: str):
    try:
        response = requests.get(url, headers=SEC_HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        text = response.text
        soup = BeautifulSoup(text, "html.parser")
        cleaned_text = soup.get_text(" ", strip=True)
        cleaned_text = re.sub(r"\s+", " ", cleaned_text)
        return cleaned_text.lower()

    except Exception:
        return ""


def extract_price_near_keywords(text: str, keywords, window=240, max_matches=8):
    if not text:
        return []

    text_lower = text.lower()
    prices = []

    price_pattern = re.compile(
        r"""
        \$\s?(\d+(?:\.\d{1,4})?)
        |
        (\d+(?:\.\d{1,4})?)\s?(?:per\s+share|a\s+share|exercise\s+price|conversion\s+price)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    for keyword in keywords:
        for match in re.finditer(re.escape(keyword.lower()), text_lower):
            start = max(0, match.start() - window)
            end = min(len(text_lower), match.end() + window)
            chunk = text[start:end]

            for price_match in price_pattern.finditer(chunk):
                raw_price = price_match.group(1) or price_match.group(2)
                if not raw_price:
                    continue
                try:
                    val = float(raw_price)
                    if 0 < val < 100000:
                        prices.append(val)
                except Exception:
                    pass

            if len(prices) >= max_matches:
                return prices

    return prices


def pick_best_price(prices):
    if not prices:
        return None
    try:
        prices = sorted(prices)
        return prices[0]
    except Exception:
        return prices[0]


def analyze_sec_offering_status(filings, max_docs_to_scan: int = 6):
    status = {
        "has_relevant_filings": False,
        "has_shelf": False,
        "has_prospectus": False,
        "has_effect": False,
        "has_atm": False,
        "has_warrants": False,
        "has_resale": False,
        "has_sales_agreement": False,
        "has_convertible": False,
        "has_equity_line": False,
        "risk_flags": [],
        "relevant_filings": [],
        "text_hits": [],
        "scanned_texts": []
    }

    if not filings:
        return status

    relevant_forms = {
        "S-1", "S-1/A", "S-3", "S-3/A", "S-3ASR", "S-3MEF",
        "F-1", "F-1/A", "F-3", "F-3/A", "F-3ASR",
        "424B1", "424B2", "424B3", "424B4", "424B5", "424B7", "424B8",
        "POS AM", "EFFECT", "RW", "8-K", "6-K"
    }

    shelf_forms = {
        "S-1", "S-1/A", "S-3", "S-3/A", "S-3ASR", "S-3MEF",
        "F-1", "F-1/A", "F-3", "F-3/A", "F-3ASR"
    }

    prospectus_forms = {"424B1", "424B2", "424B3", "424B4", "424B5", "424B7", "424B8"}

    for filing in filings:
        form = filing.get("form", "")
        if form in relevant_forms:
            status["has_relevant_filings"] = True
            status["relevant_filings"].append(filing)

        if form in shelf_forms:
            status["has_shelf"] = True

        if form in prospectus_forms:
            status["has_prospectus"] = True

        if form == "EFFECT":
            status["has_effect"] = True

    keyword_map = {
        "ATM": [
            "at the market offering",
            "at-the-market offering",
            "at the market",
            "at-the-market",
            "atm program"
        ],
        "WARRANTS": ["warrant", "warrants"],
        "RESALE": ["resale", "resale prospectus"],
        "SALES AGREEMENT": [
            "sales agreement",
            "equity distribution agreement",
            "distribution agreement"
        ],
        "CONVERTIBLE": [
            "convertible note",
            "convertible notes",
            "convertible debenture",
            "convertible preferred",
            "conversion price"
        ],
        "EQUITY LINE": [
            "equity line",
            "common stock purchase agreement",
            "committed equity facility"
        ],
        "PURCHASE AGREEMENT": [
            "purchase agreement",
            "securities purchase agreement"
        ],
    }

    scanned = 0
    for filing in status["relevant_filings"]:
        if scanned >= max_docs_to_scan:
            break

        filing_text = fetch_filing_text(filing["link"])

        if not filing_text:
            filing_text = fetch_filing_text(filing["index_link"])

        if not filing_text:
            continue

        scanned += 1
        local_hits = []

        for label, patterns in keyword_map.items():
            for pattern in patterns:
                if pattern in filing_text:
                    local_hits.append(label)
                    break

        if "ATM" in local_hits:
            status["has_atm"] = True
        if "WARRANTS" in local_hits:
            status["has_warrants"] = True
        if "RESALE" in local_hits:
            status["has_resale"] = True
        if "SALES AGREEMENT" in local_hits:
            status["has_sales_agreement"] = True
        if "CONVERTIBLE" in local_hits:
            status["has_convertible"] = True
        if "EQUITY LINE" in local_hits:
            status["has_equity_line"] = True
        if "PURCHASE AGREEMENT" in local_hits:
            status["has_equity_line"] = True

        if local_hits:
            status["text_hits"].append({
                "form": filing["form"],
                "date": filing["date"],
                "link": filing["link"],
                "index_link": filing["index_link"],
                "hits": local_hits
            })

        status["scanned_texts"].append({
            "form": filing["form"],
            "date": filing["date"],
            "link": filing["link"],
            "index_link": filing["index_link"],
            "text": filing_text
        })

    if status["has_atm"]:
        status["risk_flags"].append("ATM language found")
    if status["has_sales_agreement"]:
        status["risk_flags"].append("Sales agreement found")
    if status["has_shelf"]:
        status["risk_flags"].append("Shelf / registration filing found")
    if status["has_prospectus"]:
        status["risk_flags"].append("Prospectus / supplement filing found")
    if status["has_effect"]:
        status["risk_flags"].append("Effective registration found")
    if status["has_warrants"]:
        status["risk_flags"].append("Warrants language found")
    if status["has_resale"]:
        status["risk_flags"].append("Resale language found")
    if status["has_convertible"]:
        status["risk_flags"].append("Convertible language found")
    if status["has_equity_line"]:
        status["risk_flags"].append("Equity line / purchase agreement found")

    return status


def detect_price_levels_from_sec(sec_status):
    result = {
        "offering_price": None,
        "warrant_exercise_price": None,
        "conversion_price": None,
        "sources": []
    }

    if not sec_status:
        return result

    offering_keywords = [
        "offering price",
        "public offering price",
        "price to the public",
        "purchase price",
        "registered direct offering",
        "offering"
    ]

    warrant_keywords = [
        "exercise price",
        "warrants",
        "pre-funded warrants",
        "common warrants"
    ]

    conversion_keywords = [
        "conversion price",
        "convertible note",
        "convertible notes",
        "convertible preferred",
        "conversion"
    ]

    for doc in sec_status.get("scanned_texts", []):
        text = doc.get("text", "")

        offering_prices = extract_price_near_keywords(text, offering_keywords)
        warrant_prices = extract_price_near_keywords(text, warrant_keywords)
        conversion_prices = extract_price_near_keywords(text, conversion_keywords)

        local_source = {
            "form": doc.get("form", ""),
            "date": doc.get("date", ""),
            "link": doc.get("link", ""),
            "index_link": doc.get("index_link", ""),
            "offering_prices": offering_prices[:5],
            "warrant_prices": warrant_prices[:5],
            "conversion_prices": conversion_prices[:5],
        }

        if offering_prices or warrant_prices or conversion_prices:
            result["sources"].append(local_source)

        if result["offering_price"] is None and offering_prices:
            result["offering_price"] = pick_best_price(offering_prices)

        if result["warrant_exercise_price"] is None and warrant_prices:
            result["warrant_exercise_price"] = pick_best_price(warrant_prices)

        if result["conversion_price"] is None and conversion_prices:
            result["conversion_price"] = pick_best_price(conversion_prices)

    return result


def detect_dilution(data, news, filings, sec_status, price_detection):
    flags = []
    score = 0

    float_shares = data.get("floatShares")
    institutional = data.get("institutionalOwnership")
    shares_outstanding = data.get("sharesOutstanding")

    try:
        if float_shares not in [None, "N/A"] and float_shares > 50_000_000:
            flags.append("High float")
            score += 1
    except Exception:
        pass

    try:
        if shares_outstanding not in [None, "N/A"] and shares_outstanding > 100_000_000:
            flags.append("High shares outstanding")
            score += 1
    except Exception:
        pass

    try:
        if institutional not in [None, "N/A"] and float(institutional) < 0.10:
            flags.append("Low institutional ownership")
            score += 1
    except Exception:
        pass

    for item in news:
        if item.get("possible_offering"):
            flags.append("Possible offering news")
            score += 2

    if sec_status:
        for risk in sec_status.get("risk_flags", []):
            flags.append(risk)
            score += 2

    if price_detection.get("offering_price"):
        flags.append(f"Offering price detected: {format_price(price_detection['offering_price'])}")
        score += 2

    if price_detection.get("warrant_exercise_price"):
        flags.append(f"Warrant exercise price detected: {format_price(price_detection['warrant_exercise_price'])}")
        score += 1

    if price_detection.get("conversion_price"):
        flags.append(f"Conversion price detected: {format_price(price_detection['conversion_price'])}")
        score += 2

    suspicious_forms = {
        "S-1", "S-1/A", "S-3", "S-3/A", "S-3ASR", "S-3MEF",
        "424B1", "424B2", "424B3", "424B4", "424B5", "424B7", "424B8",
        "POS AM", "EFFECT", "RW", "F-1", "F-1/A", "F-3", "F-3/A", "F-3ASR", "6-K", "8-K"
    }

    if filings:
        for filing in filings[:10]:
            form = filing.get("form", "")
            if form in suspicious_forms:
                flags.append(f"SEC filing: {form}")
                score += 1

    unique_flags = []
    seen = set()
    for flag in flags:
        if flag not in seen:
            unique_flags.append(flag)
            seen.add(flag)

    if score >= 8:
        risk_level = "HIGH"
    elif score >= 4:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    return {
        "flags": unique_flags,
        "score": score,
        "risk_level": risk_level
    }


# =========================
# HTML HELPERS
# =========================
def render_sidebar(current_ticker=""):
    storage = load_storage()
    favorites = storage["favorites"]
    notes = storage.get("notes", {})

    fav_html = ""
    if favorites:
        for t in favorites:
            active = " active-ticker" if t.upper() == current_ticker.upper() else ""
            note = notes.get(t.upper(), "")
            note_html = f'<div class="fav-note">{escape(note)}</div>' if note else ""
            fav_html += f'''
            <a class="side-link{active}" href="/?ticker={escape(t)}">
                <div class="fav-ticker">⭐ {escape(t)}</div>
                {note_html}
            </a>
            '''
    else:
        fav_html = '<div class="empty-mini">No favorites yet.</div>'

    return fav_html


def test_twelvedata(symbol="AAPL"):
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": "1min",
        "outputsize": 5,
        "apikey": TWELVEDATA_API_KEY
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        return data
    except Exception as e:
        return {"error": str(e)}


def render_note_box(ticker):
    note = get_note(ticker)
    return f"""
    <div class="section-card">
        <div class="section-title">Notes / Watchlist Tag</div>
        <form method="POST" action="/save_note">
            <input type="hidden" name="ticker" value="{escape(ticker)}">
            <textarea name="note" class="note-textarea" placeholder="Ej: Chinese / dangerous, dilution watch, clean, short bias...">{escape(note)}</textarea>
            <button type="submit" class="secondary-btn">Guardar nota</button>
        </form>
    </div>
    """


def render_news(news):
    if not news:
        return '<div class="empty-box">No encontré noticias en Finviz.</div>'

    html = ""
    for item in news:
        labels = []

        if item.get("is_today"):
            labels.append('<span class="mini-tag mini-blue">TODAY</span>')
        if item.get("is_fresh"):
            labels.append('<span class="mini-tag mini-blue">FRESH</span>')
        if item.get("possible_offering"):
            labels.append('<span class="mini-tag mini-red">OFFERING?</span>')

        labels_html = " ".join(labels)

        html += f"""
        <div class="list-card">
            <div class="list-card-title">
                <a href="{item['link']}" target="_blank">{escape(item['title'])}</a>
            </div>
            <div class="list-card-meta">
                <span>{escape(item['date'])}</span>
                <span>{labels_html}</span>
            </div>
        </div>
        """
    return html


def render_filings(filings):
    if filings is None:
        return '<div class="empty-box">Este ticker no parece tener mapeo SEC.</div>'

    if not filings:
        return '<div class="empty-box">No encontré filings recientes.</div>'

    html = ""
    for filing in filings[:6]:
        desc = filing.get("description", "") or filing.get("primaryDocument", "")
        html += f"""
        <div class="list-card">
            <div class="filing-top">
                <span class="form-pill">{filing['form']}</span>
                <span class="filing-date">{filing['date']}</span>
            </div>
            <div class="filing-desc">{escape(desc)}</div>
            <div class="filing-links">
                <a href="{filing['link']}" target="_blank">Open filing</a>
                <a href="{filing['index_link']}" target="_blank">SEC index</a>
            </div>
        </div>
        """
    return html


def render_sec_status(sec_status):
    if not sec_status or not sec_status.get("has_relevant_filings"):
        return '<div class="empty-box">No relevant offering-related filings found.</div>'

    checks = [
        ("ATM", sec_status["has_atm"]),
        ("Shelf / Registration", sec_status["has_shelf"]),
        ("Prospectus / Supplement", sec_status["has_prospectus"]),
        ("EFFECT", sec_status["has_effect"]),
        ("Sales Agreement", sec_status["has_sales_agreement"]),
        ("Warrants", sec_status["has_warrants"]),
        ("Resale", sec_status["has_resale"]),
        ("Convertible", sec_status["has_convertible"]),
        ("Equity Line", sec_status["has_equity_line"]),
    ]

    checks_html = ""
    for label, value in checks:
        dot_class = "dot-yes" if value else "dot-no"
        value_text = "YES" if value else "NO"
        checks_html += f"""
        <div class="status-row">
            <span>{escape(label)}</span>
            <span class="status-value"><span class="dot {dot_class}"></span>{value_text}</span>
        </div>
        """

    flags_html = ""
    if sec_status.get("risk_flags"):
        flags_html += '<div class="card-block"><div class="subsection-title">SEC Risk Flags</div><ul class="nice-list">'
        for flag in sec_status["risk_flags"]:
            flags_html += f"<li>{escape(flag)}</li>"
        flags_html += "</ul></div>"

    hits_html = ""
    if sec_status.get("text_hits"):
        hits_html += '<div class="card-block"><div class="subsection-title">Text hits found in filings</div>'
        for item in sec_status["text_hits"][:3]:
            hits_text = ", ".join(item["hits"])
            hits_html += f"""
            <div class="list-card">
                <div><b>{item['form']}</b> | {item['date']}</div>
                <div>{escape(hits_text)}</div>
                <div class="filing-links">
                    <a href="{item['link']}" target="_blank">Direct filing</a>
                    <a href="{item['index_link']}" target="_blank">SEC index</a>
                </div>
            </div>
            """
        hits_html += "</div>"

    return f"""
    <div class="card-block">
        <div class="subsection-title">SEC Offering Status</div>
        <div class="status-grid">
            {checks_html}
        </div>
    </div>
    {flags_html}
    {hits_html}
    """


def render_price_detection(price_detection):
    has_any = any([
        price_detection.get("offering_price"),
        price_detection.get("warrant_exercise_price"),
        price_detection.get("conversion_price"),
    ])

    if not has_any:
        return '<div class="empty-box">No pude detectar precios claros de offering, warrants o conversion en los filings escaneados.</div>'

    rows = []

    rows.append(f"""
    <div class="status-row">
        <span>Offering Price</span>
        <span class="status-value">{format_price(price_detection.get("offering_price"))}</span>
    </div>
    """)

    rows.append(f"""
    <div class="status-row">
        <span>Warrant Exercise Price</span>
        <span class="status-value">{format_price(price_detection.get("warrant_exercise_price"))}</span>
    </div>
    """)

    rows.append(f"""
    <div class="status-row">
        <span>Conversion Price</span>
        <span class="status-value">{format_price(price_detection.get("conversion_price"))}</span>
    </div>
    """)

    sources_html = ""
    if price_detection.get("sources"):
        sources_html += '<div class="card-block"><div class="subsection-title">Detected from filings</div>'
        for src in price_detection["sources"][:4]:
            parts = []
            if src.get("offering_prices"):
                parts.append("Offering: " + ", ".join(format_price(x) for x in src["offering_prices"][:3]))
            if src.get("warrant_prices"):
                parts.append("Warrants: " + ", ".join(format_price(x) for x in src["warrant_prices"][:3]))
            if src.get("conversion_prices"):
                parts.append("Conversion: " + ", ".join(format_price(x) for x in src["conversion_prices"][:3]))

            sources_html += f"""
            <div class="list-card">
                <div><b>{escape(src.get('form', ''))}</b> | {escape(src.get('date', ''))}</div>
                <div class="filing-desc">{escape(' | '.join(parts))}</div>
                <div class="filing-links">
                    <a href="{src.get('link', '#')}" target="_blank">Direct filing</a>
                    <a href="{src.get('index_link', '#')}" target="_blank">SEC index</a>
                </div>
            </div>
            """
        sources_html += "</div>"

    return f"""
    <div class="card-block">
        <div class="subsection-title">Price Detection</div>
        <div class="status-grid">
            {''.join(rows)}
        </div>
    </div>
    {sources_html}
    """


def render_summary(data, dilution_result, news, sec_status, price_detection, intraday_data=None):
    risk_level = dilution_result["risk_level"]
    badge_class = risk_badge_class(risk_level)
    quick_flags = build_quick_flags(news, sec_status, dilution_result, price_detection)
    conclusion = build_trader_conclusion(dilution_result, sec_status, news, price_detection)
    ticker = data["symbol"]
    favorite_text = "★ Remove favorite" if is_favorite(ticker) else "☆ Add favorite"
    note = get_note(ticker)

    company_summary = build_company_summary(data.get("businessSummary", ""))
    country = data.get("country", "N/A")
    country_class = data.get("countryRiskClass", "country-unknown")

    context_lines = []

    try:
        float_shares = data.get("floatShares")
        if float_shares not in [None, "N/A"] and float(float_shares) < 20_000_000:
            context_lines.append("Low float stock")
    except Exception:
        pass

    if data.get("countryRiskClass") == "country-danger":
        context_lines.append("High risk country stock")
    elif data.get("countryRiskClass") == "country-non-us":
        context_lines.append("Non-US stock")

    if dilution_result.get("risk_level") == "HIGH":
        context_lines.append("High dilution risk")
    elif dilution_result.get("risk_level") == "MEDIUM":
        context_lines.append("Medium dilution risk")

    context_html = ""
    if context_lines:
        context_html = "".join(
            f"<div class='context-line'>• {escape(line)}</div>" for line in context_lines
        )

    intraday_price = "N/A"
    intraday_volume = "N/A"
    intraday_bars = "N/A"

    if intraday_data:
        intraday_price = intraday_data.get("price", "N/A")
        intraday_volume = intraday_data.get("intraday_volume", "N/A")
        intraday_bars = intraday_data.get("bars", "N/A")

    quick_flags_html = ""
    for text, cls in quick_flags:
        quick_flags_html += f'<span class="big-pill {cls}">{escape(text)}</span>'

    score_flags_html = ""
    if dilution_result["flags"]:
        score_flags_html = "<ul class='nice-list'>" + "".join(
            f"<li>{escape(flag)}</li>" for flag in dilution_result["flags"][:12]
        ) + "</ul>"

    note_banner = ""
    if note:
        note_banner = f"""
        <div class="watchlist-note">
            <div class="subsection-title">Saved note</div>
            <div>{escape(note)}</div>
        </div>
        """

    volume_value_class = "metric-value metric-alert-volume" if data.get("isRecordVolume") else "metric-value"
    volume_label_class = "metric-title metric-alert-label" if data.get("isRecordVolume") else "metric-title"

    record_banner = ""
    if data.get("isRecordVolume"):
        record_banner = """
        <div class="record-volume-banner">
            🚨 ALERTA DE VOLUMEN: El volumen de hoy supera el máximo diario histórico de los últimos 5 años
        </div>
        """

    max_vol_date = f" ({escape(str(data['maxVolume5YDate']))})" if data.get("maxVolume5YDate") else ""

    return f"""
    <div class="hero-card">
        <div class="hero-top">
            <div>
                <div class="ticker-line">
                    {escape(str(data['symbol']))}
                    <span class="country-badge {country_class}">{escape(str(country))}</span>
                </div>

                <div class="company-line">{escape(str(data['companyName']))}</div>

                <div class="company-summary">
                    {escape(company_summary)}
                </div>
            </div>

            <div class="hero-actions">
                <a class="favorite-btn" href="/toggle_favorite/{escape(ticker)}">{favorite_text}</a>
                <div class="risk-badge {badge_class}">
                    <div class="risk-label">Dilution Risk</div>
                    <div class="risk-level">{risk_level}</div>
                    <div class="risk-score">Score: {dilution_result['score']}</div>
                </div>
            </div>
        </div>

        {note_banner}

        {record_banner}

        <div class="pill-row">
            {quick_flags_html}
        </div>

        <div class="conclusion-box">
            <div class="subsection-title">Quick trader read</div>
            <div>{escape(conclusion)}</div>
        </div>

        {f'''
        <div class="context-box">
            <div class="subsection-title">Context</div>
            {context_html}
        </div>
        ''' if context_html else ''}
    </div>

    <div class="metrics-grid">
        <div class="metric-card"><div class="metric-title">Price</div><div class="metric-value">{data['price']}</div></div>
        <div class="metric-card"><div class="metric-title">Intraday Price</div><div class="metric-value">{intraday_price}</div></div>
        <div class="metric-card"><div class="metric-title">Prev Close</div><div class="metric-value">{data['prevClose']}</div></div>
        <div class="metric-card"><div class="metric-title">Open</div><div class="metric-value">{data['open']}</div></div>
        <div class="metric-card"><div class="metric-title">High</div><div class="metric-value">{data['high']}</div></div>
        <div class="metric-card"><div class="metric-title">Low</div><div class="metric-value">{data['low']}</div></div>

        <div class="metric-card">
            <div class="{volume_label_class}">Volume</div>
            <div class="{volume_value_class}">{format_number(data['volume'])}</div>
        </div>

        <div class="metric-card"><div class="metric-title">Intraday Volume</div><div class="metric-value">{format_number(intraday_volume)}</div></div>
        <div class="metric-card"><div class="metric-title">Intraday Bars</div><div class="metric-value">{intraday_bars}</div></div>
        <div class="metric-card"><div class="metric-title">Avg Volume</div><div class="metric-value">{format_number(data['avgVolume'])}</div></div>
        <div class="metric-card"><div class="metric-title">RVOL</div><div class="metric-value">{data['rvol']}</div></div>

        <div class="metric-card">
            <div class="metric-title">Max Vol 5Y</div>
            <div class="metric-value metric-historical-volume">{data.get('maxVolume5YFormatted', 'N/A')}</div>
            <div class="metric-subvalue">{max_vol_date}</div>
        </div>

        <div class="metric-card"><div class="metric-title">Market Cap</div><div class="metric-value">{format_market_cap(data['marketCap'])}</div></div>
        <div class="metric-card"><div class="metric-title">Float</div><div class="metric-value">{format_number(data['floatShares'])}</div></div>
        <div class="metric-card"><div class="metric-title">Shares O/S</div><div class="metric-value">{format_number(data['sharesOutstanding'])}</div></div>
        <div class="metric-card"><div class="metric-title">Institutional</div><div class="metric-value">{format_percent(data['institutionalOwnership'])}</div></div>
        <div class="metric-card"><div class="metric-title">Insiders</div><div class="metric-value">{format_percent(data['insiderOwnership'])}</div></div>
        <div class="metric-card"><div class="metric-title">Sector</div><div class="metric-value small-text">{escape(str(data['sector']))}</div></div>
        <div class="metric-card"><div class="metric-title">Industry</div><div class="metric-value small-text">{escape(str(data['industry']))}</div></div>
    </div>

    <div class="card-block">
        <div class="subsection-title">Score flags</div>
        {score_flags_html if score_flags_html else '<div class="empty-box">No major paper-risk flags detected.</div>'}
    </div>
    """


def render_main_menu(active_page="analyzer"):
    items = [
        ("Gainers", url_for("gainers_page"), active_page == "gainers"),
        ("Analyzer", url_for("home"), active_page == "analyzer"),
        ("Gap Stats", url_for("gap_stats_page"), active_page == "gap_stats"),

        ("Import Trades", url_for("import_trades"), active_page == "import_trades"),
        ("Trade History", url_for("trade_history"), active_page == "trade_history"),
    ]

    if current_user.is_authenticated and current_user.is_admin:
        items.append(("Create User", url_for("create_user_route"), active_page == "create_user"))

    items.append(("Logout", url_for("logout"), False))

    html = ""

    for label, link, active in items:
        active_class = " main-nav-active" if active else ""
        html += f'<a class="main-nav-link{active_class}" href="{link}">{escape(label)}</a>'

    return html


def render_overhead_block(data, overheads):
    if not overheads:
        return ""

    current_price = data.get("price", 0)

    rows = ""

    labels = ["PRIMARY", "SECONDARY", "TERTIARY"]
    colors = ["overhead-primary", "overhead-secondary", "overhead-tertiary"]

    for i, level in enumerate(overheads):
        try:
            distance = ((level - current_price) / current_price) * 100
        except:
            distance = 0

        rows += f"""
        <div class="overhead-row {colors[i]}">
            <div class="overhead-label">{labels[i]}</div>
            <div class="overhead-price">{round(level, 2)}</div>
            <div class="overhead-distance">{distance:.1f}%</div>
        </div>
        """

    return f"""
    <div class="overhead-box">
        <div class="overhead-title">🚧 OVERHEAD LEVELS (VWAP)</div>
        {rows}
    </div>
    """


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("gainers_page"))

    error = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        conn = get_db_connection()
        row = conn.execute(
            "SELECT id, username, password_hash, is_admin FROM users WHERE username = ?",
            (username,)
        ).fetchone()
        conn.close()

        if row and check_password_hash(row["password_hash"], password):
            user = User(row["id"], row["username"], bool(row["is_admin"]))
            login_user(user)
            return redirect(url_for("gainers_page"))
        else:
            error = "Usuario o contraseña incorrectos"

    return render_template("login.html", error=error)

@app.route("/import-trades", methods=["GET", "POST"])
def import_trades():
    if request.method == "POST":
        file = request.files["file"]

        if not file:
            return "No file uploaded"

        stream = io.StringIO(file.stream.read().decode("UTF8"), newline=None)
        reader = csv.DictReader(stream)

        conn = get_db()
        c = conn.cursor()

        for row in reader:
            try:
                symbol = row.get("Symbol", "")
                side = row.get("Side", "")
                shares = float(row.get("Shares", 0))
                entry = float(row.get("Price", 0))
                exit_price = float(row.get("Exit Price", 0))
                pnl = float(row.get("P&L", 0))

                c.execute("""
                    INSERT INTO trades (date, symbol, side, shares, entry, exit, pnl, fee, setup, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row.get("Date"),
                    symbol,
                    side,
                    shares,
                    entry,
                    exit_price,
                    pnl,
                    0,
                    "",
                    ""
                ))

            except:
                continue

        conn.commit()
        conn.close()

        return redirect(url_for("trade_history"))

    html = """
    <h2>Import Trades</h2>

    <form method="POST" enctype="multipart/form-data" style="margin-top:20px;">
        <input type="file" name="file" required>
        <button type="submit">Upload CSV</button>
    </form>
    """

    main_menu_html = render_main_menu("import_trades")

    return render_template(
        "index.html",
        ticker="",
        content=html,
        sidebar_html=render_sidebar(""),
        main_menu_html=main_menu_html
    )

@app.route("/trade-history")
def trade_history():
    conn = get_db()
    c = conn.cursor()

    trades = c.execute("""
        SELECT date, symbol, side, shares, entry, exit, pnl
        FROM trades
        ORDER BY date DESC
    """).fetchall() or []

    conn.close()

    rows = ""
    rows = ""
    for t in trades:
        try:
            pnl = float(t[6]) if t[6] is not None else 0
        except:
            pnl = 0

        color = "#00ff9c" if pnl > 0 else "#ff4d4d"

        side_class = "green" if t[2] == "LONG" else "red"

        rows += f"""
        <tr>
            <td>{t[0] or ''}</td>
            <td>{t[1] or ''}</td>
            <td class="{side_class}">{t[2] or ''}</td>
            <td>{t[3] or ''}</td>
            <td>{t[4] or ''}</td>
            <td>{t[5] or ''}</td>
            <td style="color:{color}">{pnl}</td>
        </tr>
        """

    html = f"""
    <h2>Trade History</h2>

    <table style="width:100%; margin-top:20px;">
    <tr>
        <th>Date</th>
        <th>Symbol</th>
        <th>Side</th>
        <th>Shares</th>
        <th>Entry</th>
        <th>Exit</th>
        <th>PnL</th>
    </tr>

    {rows}

    </table>
    """

    main_menu_html = render_main_menu("trade_history")

    return render_template(
        "index.html",
        ticker="",
        content=html,
        sidebar_html=render_sidebar(""),
        main_menu_html=main_menu_html
    )

@app.route("/test")
def test_api():
    data = test_twelvedata("AAPL")
    return jsonify(data)


@app.route("/test_intraday")
def test_intraday():
    data = get_intraday_snapshot("AAPL")
    return jsonify(data)


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


@app.route("/toggle_favorite/<ticker>")
@login_required
def toggle_favorite_route(ticker):
    toggle_favorite(ticker.upper())
    return redirect(url_for("home", ticker=ticker.upper()))


@app.route("/save_note", methods=["POST"])
@login_required
def save_note_route():
    ticker = request.form.get("ticker", "").strip().upper()
    note = request.form.get("note", "")
    if ticker:
        save_note(ticker, note)
    return redirect(url_for("home", ticker=ticker))


@app.route("/create_user", methods=["GET", "POST"])
@login_required
def create_user_route():
    if not current_user.is_admin:
        return "No autorizado", 403

    error = None
    success = None

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if not username or not password:
            error = "Faltan datos"
        else:
            try:
                create_user(username, password)
                success = "Usuario creado correctamente"
            except sqlite3.IntegrityError:
                error = "El usuario ya existe"

    main_menu_html = render_main_menu("create_user")
    return render_template("create_user.html", error=error, success=success, main_menu_html=main_menu_html)


@app.route("/", methods=["GET", "POST"])
@login_required
def home():
    ticker = request.args.get("ticker", "").strip().upper()
    content = ""

    if request.method == "POST":
        ticker = request.form.get("ticker", "").strip().upper()

    if ticker:
        data = get_stock_data(ticker)
        intraday_data = get_intraday_snapshot(ticker)

        if "error" in data:
            content = f'<div class="error-box"><b>Error:</b> {escape(data["error"])}</div>'
        else:
            news = get_stock_news(ticker, limit=3)
            filings = get_recent_sec_filings(ticker, limit=20)
            sec_status = analyze_sec_offering_status(filings, max_docs_to_scan=6)
            price_detection = detect_price_levels_from_sec(sec_status)
            dilution_result = detect_dilution(data, news, filings or [], sec_status, price_detection)

            summary_html = render_summary(data, dilution_result, news, sec_status, price_detection, intraday_data)
            note_box_html = render_note_box(ticker)
            news_html = render_news(news)
            filings_html = render_filings(filings)
            sec_status_html = render_sec_status(sec_status)
            price_detection_html = render_price_detection(price_detection)
            overheads = calculate_daily_vwap_overhead(ticker)
            overhead_html = render_overhead_block(data, overheads)

            content = f"""
            {summary_html}

            {overhead_html}

            {note_box_html}

            <div class="two-col">
                <div class="section-card">
                    <div class="section-title">Latest News</div>
                    {news_html}
                </div>
                <div class="section-card">
                    <div class="section-title">SEC Status</div>
                    {sec_status_html}
                </div>
            </div>

            <div class="section-card">
                <div class="section-title">Offering / Warrant / Conversion Prices</div>
                {price_detection_html}
            </div>

            <div class="section-card">
                <div class="section-title">Recent SEC Filings</div>
                {filings_html}
            </div>
            """

    sidebar_html = render_sidebar(ticker)
    main_menu_html = render_main_menu("analyzer")

    return render_template(
        "index.html",
        ticker=ticker,
        content=content,
        sidebar_html=sidebar_html,
        main_menu_html=main_menu_html
    )


@app.route("/gainers")
@login_required
def gainers_page():
    main_menu_html = render_main_menu("gainers")
    return render_template(
        "gainers.html",
        main_menu_html=main_menu_html,
        chart_symbol="AMEX:SPY"
    )


@app.route("/momentum")
@login_required
def momentum_page():
    main_menu_html = render_main_menu("momentum")
    return render_template("momentum.html", main_menu_html=main_menu_html)


@app.route("/gap-stats", methods=["GET", "POST"])
@login_required
def gap_stats_page():
    ticker = request.form.get("ticker", "").strip().upper() if request.method == "POST" else ""
    gap_percent = request.form.get("gap_percent", "5") if request.method == "POST" else "5"
    period_key = request.form.get("period_key", "1y") if request.method == "POST" else "1y"
    gap_type = request.form.get("gap_type", "up") if request.method == "POST" else "up"

    result = None
    error = None

    if request.method == "POST":
        result = build_gap_stats(
            ticker=ticker,
            gap_percent=float(gap_percent),
            period_key=period_key,
            gap_type=gap_type
        )

        if result and result.get("error"):
            error = result["error"]
            result = None

    main_menu_html = render_main_menu("gap_stats")

    return render_template(
        "gap_stats.html",
        main_menu_html=main_menu_html,
        ticker=ticker,
        gap_percent=gap_percent,
        period_key=period_key,
        gap_type=gap_type,
        result=result,
        error=error,
    )


@app.route("/api/gainers")
@login_required
def api_gainers():
    data = build_gainers()

    cleaned = []
    for x in data:
        item = {
            "symbol": x.get("symbol") or x.get("ticker") or "N/A",
            "company": x.get("company") or x.get("companyName") or "",
            "price": safe_float(x.get("price", x.get("last_price", 0)), 0),
            "change_percent": safe_float(
                x.get("change_percent", x.get("percent_change", x.get("changePct", 0))),
                0
            ),
            "volume": safe_int(x.get("volume", x.get("current_volume", 0)), 0),
        }
        item["score"] = compute_score(item)
        cleaned.append(item)

    cleaned.sort(key=lambda x: x["change_percent"], reverse=True)

    for i, item in enumerate(cleaned, start=1):
        item["rank"] = i

    print("GAINERS DATA CLEANED:", cleaned)
    return jsonify(cleaned)


@app.route("/api/momentum")
@login_required
def api_momentum():
    data = build_momentum()

    cleaned = []
    for x in data:
        item = {
            "symbol": x.get("symbol") or x.get("ticker") or "N/A",
            "company": x.get("company") or x.get("companyName") or "",
            "price": safe_float(x.get("price", x.get("last_price", 0)), 0),
            "change_percent": safe_float(
                x.get("change_percent", x.get("percent_change", x.get("changePct", 0))),
                0
            ),
            "volume": safe_int(x.get("volume", x.get("current_volume", 0)), 0),
        }
        item["score"] = compute_score(item)
        cleaned.append(item)

    cleaned.sort(key=lambda x: x["score"], reverse=True)

    for i, item in enumerate(cleaned, start=1):
        item["rank"] = i

    print("MOMENTUM DATA CLEANED:", cleaned)
    return jsonify(cleaned)


@app.route("/scanner")
@login_required
def scanner():
    data = scan_market()
    return render_template("scanner.html", data=data, main_menu_html=render_main_menu("scanner"))


if __name__ == "__main__":
    app.run(debug=True)
