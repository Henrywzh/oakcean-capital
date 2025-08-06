from fastapi import FastAPI, Query
from typing import List, Optional
from datetime import datetime
from fastapi.responses import JSONResponse
from src.config.settings import get_collection

app = FastAPI()
collection = get_collection()


@app.get("/historical_data_bulk")
def get_historical_data_bulk(
    tickers: List[str] = Query(...),
    start: Optional[str] = None,
    end: Optional[str] = None,
    fields: List[str] = Query(default=["date", "close"])
):
    start_date = None
    end_date = None

    try:
        if start:
            start_date = datetime.strptime(start, "%Y-%m-%d")
        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid date format"})

    query = {
        "ticker": {"$in": tickers}
    }
    if start_date and end_date:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    elif start_date:
        query["date"] = {"$gte": start_date}
    elif end_date:
        query["date"] = {"$lte": end_date}

    projection = {field: 1 for field in fields}
    projection["ticker"] = 1
    projection["date"] = 1
    projection["_id"] = 0

    results = list(collection.find(query, projection))
    return results


@app.get("/historical_data")
def get_historical_data(
        ticker: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        fields: List[str] = Query(default=["date", "close", "volume"])
):
    query = {"ticker": ticker}
    start_date = None
    end_date = None

    try:
        if start:
            start_date = datetime.strptime(start, "%Y-%m-%d")
        if end:
            end_date = datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid date format"})

    if start and end:
        query["date"] = {"$gte": start_date, "$lte": end_date}
    elif start:
        query["date"] = {"$gte": start_date}
    elif end:
        query["date"] = {"$lte": end_date}

    projection = {field: 1 for field in fields}
    projection["date"] = 1
    projection["_id"] = 0

    results = list(collection.find(query, projection))
    return results


@app.get("/all_tickers")
def get_all_tickers():
    try:
        tickers = collection.distinct("ticker")
        return {"tickers": sorted(tickers)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/upload_bulk")
async def upload_bulk(data: List[dict]):
    if not data:
        return {"message": "No data provided"}
    try:
        collection.insert_many(data, ordered=False)
        return {"message": f"{len(data)} records inserted."}
    except Exception as e:
        return {"error": str(e)}

