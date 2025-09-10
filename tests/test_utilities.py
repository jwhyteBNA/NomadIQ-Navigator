import os
import pytest
import polars as pl
from unittest import mock
from src import utilities

@mock.patch.dict(os.environ, {}, clear=True)
def test_get_minio_client_missing_env():
    with pytest.raises(Exception):
        utilities.get_minio_client()

@mock.patch("src.utilities.requests.get")
def test_fetch_all_nps_data_bad_response(mock_get):
    mock_response = mock.Mock()
    mock_response.raise_for_status.side_effect = Exception("API error")
    mock_response.json.return_value = {}
    mock_get.return_value = mock_response
    with pytest.raises(Exception):
        utilities.fetch_all_nps_data("fake_key", "http://fakeurl")

def test_convert_to_csv_large_data():
    data = [{"a": i} for i in range(100000)]
    buffer = utilities.convert_to_csv(data)
    assert hasattr(buffer, "getvalue")
    csv_str = buffer.getvalue().decode()
    assert csv_str.startswith("a")
    assert "0" in csv_str

def test_remove_old_files_idempotency():
    files = ["file1", "file2", "file3"]
    latest = ["file2", "file3"]
    with mock.patch("os.remove") as mock_remove:
        utilities.remove_old_files(files, latest)
        mock_remove.assert_called_once_with("file1")

def test_convert_json_to_parquet_empty():
    result = utilities.convert_json_to_parquet([])
    assert result is None

@mock.patch("src.utilities.duckdb.connect", side_effect=Exception("DB error"))
def test_duckdb_setup_error(mock_connect):
    with pytest.raises(Exception):
        utilities.duckdb_setup()
