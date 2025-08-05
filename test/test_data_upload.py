import pandas as pd
from src.config.settings import get_collection


def test_upload_insert_sample():
    collection = get_collection()

    # Insert one fake row (simulate one record from AkShare)
    dummy = {
        "ticker": "000000.SS",
        "name": "测试股份",
        "date": pd.to_datetime("2020-01-01"),
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 1000,
        "amount": 1500.0
    }

    collection.insert_one(dummy)
    fetched = collection.find_one({"ticker": "000000.SS", "date": dummy["date"]})
    assert fetched["close"] == 1.5

    collection.delete_one({"_id": fetched["_id"]})
