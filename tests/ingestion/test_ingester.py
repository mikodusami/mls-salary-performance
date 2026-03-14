import pytest
import pandas as pd
import requests
from unittest.mock import patch, MagicMock
from ingestion.ingester import Ingester

class MockIngester(Ingester):
    source_name = "test_source"
    def fetch_data(self) -> pd.DataFrame:
        return pd.DataFrame([{"player": "Cucho", "goals": 10}])

@pytest.fixture
def ingester(tmp_path):
    # tmp_path is a built-in pytest fixture that gives you a
    # temporary directory that's cleaned up after the test
    return MockIngester(delay=0, raw_data_dir=str(tmp_path))

def test_source_name(ingester):
    assert ingester.source_name == "test_source"

def test_default_headers_contains_user_agent(ingester):
    headers = ingester._default_headers()
    assert "User-Agent" in headers
    assert "Mozilla" in headers["User-Agent"]

def test_get_returns_response_on_success(ingester):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None

    with patch.object(ingester.session, "get", return_value=mock_response) as mock_get:
        response = ingester.get("https://fake-url.com")
        assert response.status_code == 200
        mock_get.assert_called_once()

def test_get_retries_on_failure(ingester):
    # Simulate two failures then a success
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch.object(ingester.session, "get") as mock_get:
        mock_get.side_effect = [
            requests.ConnectionError("timeout"),
            requests.ConnectionError("timeout"),
            mock_response,
        ]
        result = ingester.get("https://fake-url.com")
        assert mock_get.call_count == 3

def test_get_raises_after_max_retries(ingester):
    with patch.object(ingester.session, "get", side_effect=requests.ConnectionError("down")):
        with pytest.raises(requests.ConnectionError):
            ingester.get("https://fake-url.com")


def test_save_raw_writes_csv(ingester, tmp_path):
    df = pd.DataFrame([{"player": "Cucho", "salary": 1_000_000}])
    ingester.save_raw(df)

    expected_path = tmp_path / "test_source_raw.csv"
    assert expected_path.exists()

    loaded = pd.read_csv(expected_path)
    assert len(loaded) == 1
    assert "player" in loaded.columns

def test_save_raw_does_not_write_empty_dataframe(ingester, tmp_path):
    ingester.save_raw(pd.DataFrame())
    assert not any(tmp_path.iterdir())  # nothing was written


def test_run_returns_dataframe(ingester):
    with patch.object(ingester, "save_raw"):  # skip actual disk write
        result = ingester.run()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1