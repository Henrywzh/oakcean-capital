from src.config.settings import get_collection


def test_mongo_connection():
    collection = get_collection()
    assert collection.name == "daily_prices"
    assert collection.database.name == "sse_database"
