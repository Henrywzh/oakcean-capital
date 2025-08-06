import pandas as pd
import akshare as ak
from src.config.settings import get_collection


def upload_all():
    collection = get_collection()
    stock_list = ak.stock_info_a_code_name()
    sse_stocks = stock_list[stock_list["code"].str.startswith("6")].reset_index(drop=True)
    bulk_records = []

    for i, row in sse_stocks.iterrows():
        code, name = row["code"], row["name"]
        ticker = f"{code}.SS"
        if collection.find_one({"ticker": ticker}): continue

        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20100101", adjust="qfq")
            for _, r in df.iterrows():
                bulk_records.append({
                    "ticker": ticker,
                    "name": name,
                    "date": pd.to_datetime(r["日期"]),
                    "open": float(r["开盘"]),
                    "high": float(r["最高"]),
                    "low": float(r["最低"]),
                    "close": float(r["收盘"]),
                    "volume": int(r["成交量"]),
                    "amount": float(r["成交额"])
                })

            if len(bulk_records) >= 10_000:
                collection.insert_many(bulk_records)
                bulk_records.clear()

        except Exception as e:
            print(f"❌ {ticker} failed: {e}")

    if bulk_records:
        collection.insert_many(bulk_records)


if __name__ == "__main__":
    upload = False

    if upload:
        upload_all()