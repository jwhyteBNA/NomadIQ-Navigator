import io
import pytest
import requests
import polars as pl
from src.logger import logger_setup
from src.utilities import convert_to_csv, convert_json_to_parquet, fetch_all_nps_data, save_to_minio, get_minio_client

def test_convert_to_csv_simple():
    data = [{'a': 1, 'b': 2}]
    buffer = convert_to_csv(data)
    assert isinstance(buffer, io.BytesIO)
    assert buffer.getbuffer().nbytes > 0
    df = pl.read_csv(buffer)
    assert 'a' in df.columns and 'b' in df.columns

def test_convert_to_parquet_simple():
    data = [{'a': 1, 'b': {'c': 2}}]
    buffer = convert_json_to_parquet(data)
    assert isinstance(buffer, io.BytesIO)
    assert buffer.getbuffer().nbytes > 0
    df = pl.read_parquet(buffer)
    assert 'a' in df.columns

def test_table_name_extraction():
    file_name = 'alerts_data_2025-09-03.parquet'
    table_name = file_name.split('_data_')[0].upper()
    assert table_name == 'ALERTS'

def test_save_to_minio_error(monkeypatch):
    class DummyMinio:
        def put_object(self, *args, **kwargs):
            raise Exception("MinIO error")
    monkeypatch.setattr('src.utilities.get_minio_client', lambda: DummyMinio())
    buffer = io.BytesIO(b"test")
    save_to_minio(buffer, 'bucket', 'file.parquet')

def test_fetch_all_nps_data(monkeypatch):
    class DummyResponse:
        def __init__(self):
            self.status_code = 200
        def json(self):
            return {"data": [{"a": 1}], "total": 1}
        def raise_for_status(self):
            pass
    def dummy_get(url, params):
        return DummyResponse()
    monkeypatch.setattr(requests, 'get', dummy_get)
    result = fetch_all_nps_data('key', 'url')
    assert isinstance(result, list)
    assert result[0]['a'] == 1
