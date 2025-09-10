import os
import duckdb
import pytest
from unittest.mock import patch
from src.utilities import duckdb_setup, ducklake_init, ducklake_connect_minio
from src.setup_ducklake import setup_ducklake

def test_duckdb_setup_creates_connection(tmp_path, monkeypatch):
    db_path = tmp_path / "test_ducklake.db"
    original_connect = duckdb.connect
    monkeypatch.setattr("duckdb.connect", lambda database: original_connect(str(db_path)))
    
    conn = duckdb_setup()
    assert isinstance(conn, duckdb.DuckDBPyConnection)
    assert db_path.exists()
    conn.close()

def test_ducklake_catalog_and_schema_creation(tmp_path, monkeypatch):
    db_path = tmp_path / "test_ducklake_catalog_and_schema_creation.db"
    original_connect = duckdb.connect
    monkeypatch.setattr("duckdb.connect", lambda database='ducklake.db': original_connect(str(db_path)))
    conn = duckdb_setup()
    data_path = str(tmp_path / "data")
    catalog_path = str(tmp_path / "catalog")
    ducklake_init(conn, data_path, catalog_path)
    result = conn.execute("PRAGMA database_list").fetchall()
    assert any("my_ducklake" in row for row in result)
    conn.close()

def test_setup_ducklake_sql_calls():
    class DummyConn:
        def __init__(self):
            self.calls = []
        def execute(self, query):
            self.calls.append(query)
    dummy_conn = DummyConn()
    data_path = "/dummy/data"
    catalog_path = "/dummy/catalog"
    ducklake_init(dummy_conn, data_path, catalog_path)
    assert any("ATTACH 'ducklake:" in call for call in dummy_conn.calls)
    assert any("USE my_ducklake" in call for call in dummy_conn.calls)

def test_ducklake_connect_minio(monkeypatch):
    class DummyConn:
        def __init__(self):
            self.calls = []
        def execute(self, query):
            self.calls.append(query)
    dummy_conn = DummyConn()
    with patch("src.setup_ducklake.os.getenv", side_effect=lambda k: f"dummy_{k}"):
        ducklake_connect_minio(dummy_conn)
    assert any("SET s3_access_key_id" in call for call in dummy_conn.calls)
    assert any("SET s3_secret_access_key" in call for call in dummy_conn.calls)
    assert any("SET s3_endpoint" in call for call in dummy_conn.calls)

def test_ducklake_init_with_invalid_path(tmp_path, monkeypatch):
    db_path = tmp_path / "test_ducklake_init_with_invalid_path.db"
    original_connect = duckdb.connect
    monkeypatch.setattr("duckdb.connect", lambda database='ducklake.db': original_connect(str(db_path)))
    conn = duckdb_setup()
    data_path = "/invalid/path"
    catalog_path = "/invalid/catalog"
    with pytest.raises(Exception):
        ducklake_init(conn, data_path, catalog_path)
    conn.close()

def test_duckdb_install_extension(tmp_path, monkeypatch):
    monkeypatch.setattr("duckdb.install_extension", lambda ext: None)
    monkeypatch.setattr("duckdb.load_extension", lambda ext: None)
    db_path = tmp_path / "test_duckdb_install_extension.db"
    original_connect = duckdb.connect
    monkeypatch.setattr("duckdb.connect", lambda database='ducklake.db': original_connect(str(db_path)))
    duckdb_setup()