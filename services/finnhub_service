import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestBarRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

load_dotenv()

API_KEY = os.getenv("APCA_API_KEY_ID")
API_SECRET = os.getenv("APCA_API_SECRET_KEY")

client = StockHistoricalDataClient(API_KEY, API_SECRET)


def get_latest_bars(symbols):
    request = StockLatestBarRequest(
        symbol_or_symbols=symbols,
        feed=DataFeed.IEX
    )
    return client.get_stock_latest_bar(request)


def get_recent_bars(symbols, minutes=10):
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=minutes + 5)

    request = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Minute,
        start=start_time,
        end=end_time,
        feed=DataFeed.IEX
    )

    bars = client.get_stock_bars(request)
    return bars.data
