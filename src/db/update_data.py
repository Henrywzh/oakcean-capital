import pandas as pd
import akshare as ak
from datetime import datetime
from src.config.settings import get_collection


def update_all():
    collection = get_collection()
    tickers = collection.distinct("ticker")

    for i, ticker in enumerate(tickers):
        code = ticker.split(".")[0]  # "600000.SS" → "600000"
        name_doc = collection.find_one({"ticker": ticker}, {"name": 1})
        name = name_doc.get("name", "") if name_doc else ""

        # Get latest date in DB
        latest_doc = collection.find_one(
            {"ticker": ticker},
            sort=[("date", -1)],
            projection={"date": 1}
        )

        if not latest_doc:
            print(f"⚠️ No existing data for {ticker}, skipping...")
            continue

        latest_date = latest_doc["date"].strftime("%Y%m%d")
        today = datetime.today().strftime("%Y%m%d")

        if latest_date >= today:
            print(f"⏩ {ticker} is already up-to-date")
            continue

        print(f"🔄 {i+1}/{len(tickers)} Updating {ticker} from {latest_date} → {today}")

        try:
            df = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=latest_date,
                adjust="qfq"
            )
            # Skip if already included
            df = df[df["日期"] > latest_doc["date"].strftime("%Y-%m-%d")]

            if df.empty:
                print(f"✅ No new data for {ticker}")
                continue

            records = []
            for _, r in df.iterrows():
                records.append({
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

            collection.insert_many(records)
            print(f"✅ Inserted {len(records)} new rows for {ticker}")

        except Exception as e:
            print(f"❌ Error updating {ticker}: {e}")

