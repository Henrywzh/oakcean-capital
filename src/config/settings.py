import certifi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os


# Load from .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")


def get_mongo_client():
    # return MongoClient(MONGO_URI, server_api=ServerApi('1'), tls=True, tlsCAFile=certifi.where())
    return MongoClient(MONGO_URI)


def get_collection(use_yfinance=False):
    client = get_mongo_client()
    if use_yfinance:
        db = client["sse_yfinance"]
    else:
        db = client["sse_local"]
    return db["daily_prices"]
