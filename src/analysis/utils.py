from datetime import datetime
import pandas as pd
import requests
from tqdm import tqdm

API_URL = "http://localhost:8000"
CORR_MATRIX_PATH = "../output/correlation_matrix.csv"
CLUSTER_LABELS_PATH = "../output/cluster_labels.csv"
TRADE_PATH = "../output/mean_reversion_trades.csv"

TEST_PATH = "../output/test.csv"


def get_all_tickers_via_api() -> list[str]:
    response = requests.get(f"{API_URL}/all_tickers")
    response.raise_for_status()

    return response.json()["tickers"]


def load_close_price_via_api(ticker: str, start: datetime = None, end: datetime = None) -> pd.Series:
    params = {
        "ticker": ticker,
        "fields": ["date", "close"]
    }
    if start:
        params["start"] = start.strftime("%Y-%m-%d")
    if end:
        params["end"] = end.strftime("%Y-%m-%d")

    response = requests.get(f"{API_URL}/historical_data", params=params)
    response.raise_for_status()
    data = response.json()

    if not data:
        return pd.Series(dtype=float)

    df = pd.DataFrame(data)
    df = df.sort_values("date").drop_duplicates("date")
    df["date"] = pd.to_datetime(df["date"])
    df.set_index("date", inplace=True)

    return df["close"]


def load_all_close_price_via_api(tickers: list[str], start: datetime, end: datetime) -> pd.DataFrame:
    close_data = {}
    for ticker in tqdm(tickers, desc="📥 Loading close prices via API"):
        series = load_close_price_via_api(ticker, start, end)
        if not series.empty:
            close_data[ticker] = series

    return pd.DataFrame(close_data)
