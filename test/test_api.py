from fastapi.testclient import TestClient
from src.api.main import app

client = TestClient(app)

def test_api_response_fields():
    resp = client.get(
        "/historical_data",
        params={
            "ticker": "600000.SS",
            "start": "2020-01-01",
            "end": "2020-12-31",
            "fields": ["close", "volume"]
        }
    )

    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)

    if data:
        assert "close" in data[0]
        assert "volume" in data[0]
        assert "date" in data[0]
