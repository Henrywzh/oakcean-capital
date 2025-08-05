import certifi
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os

# Load from .env
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")


def get_mongo_client():
    return MongoClient(MONGO_URI, server_api=ServerApi('1'), tls=True, tlsCAFile=certifi.where())


def get_collection():
    client = get_mongo_client()
    db = client["sse_database"]
    return db["daily_prices"]
