import pandas as pd
import akshare as ak
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread
from tqdm import tqdm
from src.config.settings import get_collection

# Constants
INSERT_BATCH_SIZE = 5000
INSERT_WORKER_DELAY = 0.5  # seconds (not needed with blocking queue)
MAX_WORKERS = 10

# Shared queue for passing records between threads
record_queue = Queue()
collection = get_collection()


def update_ticker_to_queue(ticker: str) -> str:
    code = ticker.split(".")[0]
    name_doc = collection.find_one({"ticker": ticker}, {"name": 1})
    name = name_doc.get("name", "") if name_doc else ""

    latest_doc = collection.find_one(
        {"ticker": ticker},
        sort=[("date", -1)],
        projection={"date": 1}
    )

    if not latest_doc:
        return f"⚠️ {ticker}: no existing data, skipping"

    latest_date = latest_doc["date"].strftime("%Y%m%d")
    today = datetime.today().strftime("%Y%m%d")

    if latest_date >= today:
        return f"⏩ {ticker} is already up-to-date"

    try:
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=latest_date,
            adjust="qfq"
        )
        df = df[pd.to_datetime(df["日期"]) > latest_doc["date"].strftime("%Y-%m-%d")]

        if df.empty:
            return f"✅ {ticker}: no new data"

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
                "amount": float(r["成交额"]),
            })

        record_queue.put(records)
        return f"✅ {ticker}: {len(records)} rows fetched"

    except Exception as e:
        return f"❌ {ticker} error: {e}"


def mongo_insert_worker():
    buffer = []
    while True:
        records = record_queue.get()
        if records == "STOP":
            break

        buffer.extend(records)

        if len(buffer) >= INSERT_BATCH_SIZE:
            try:
                collection.insert_many(buffer, ordered=False)
                buffer.clear()
            except Exception as e:
                print(f"❌ Bulk insert failed: {e}")
                buffer.clear()

    # Insert remaining records
    if buffer:
        try:
            collection.insert_many(buffer, ordered=False)
            print(f"📝 Final flush: Inserted {len(buffer)} records")
        except Exception as e:
            print(f"❌ Final insert failed: {e}")


def update_all_parallel_with_batch_insert():
    tickers = collection.distinct("ticker")
    print(f"🚀 Starting threaded update for {len(tickers)} tickers...")

    # Start insert worker thread
    insert_thread = Thread(target=mongo_insert_worker)
    insert_thread.start()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(update_ticker_to_queue, ticker): ticker for ticker in tickers}
        with tqdm(total=len(tickers), desc="Updating tickers") as pbar:
            for future in as_completed(futures):
                result = future.result()
                tqdm.write(result)
                pbar.update(1)

    # Stop the insert worker
    record_queue.put("STOP")
    insert_thread.join()
    print("✅ All done.")


if __name__ == "__main__":
    update_all_parallel_with_batch_insert()
