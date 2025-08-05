from fastapi import FastAPI, Query
from typing import List
from datetime import datetime
from fastapi.responses import JSONResponse
from src.config.settings import get_collection

app = FastAPI()
collection = get_collection()

@app.get("/historical_data")
def get_historical_data(
    ticker: str,
    start: str,
    end: str,
    fields: List[str] = Query(default=["date", "close", "volume"])
):
    try:
        start_date = datetime.strptime(start, "%Y-%m-%d")
        end_date = datetime.strptime(end, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid date format"})

    projection = {field: 1 for field in fields}
    projection["date"] = 1
    projection["_id"] = 0

    query = {
        "ticker": ticker,
        "date": {"$gte": start_date, "$lte": end_date}
    }

    results = list(collection.find(query, projection))
    return results
