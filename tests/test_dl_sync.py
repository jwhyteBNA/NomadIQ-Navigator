import os
import pytest
from unittest import mock
from src.dl_sync import ducklake_sync

@mock.patch("src.dl_sync.duckdb_setup")
@mock.patch("src.dl_sync.ducklake_init")
@mock.patch("src.dl_sync.ducklake_connect_minio")
@mock.patch("src.dl_sync.sync_tables")
@mock.patch("src.dl_sync.cleanup_db_folders")
@mock.patch("src.dl_sync.logger")
def test_ducklake_sync_runs(
	mock_logger,
	mock_cleanup_db_folders,
	mock_sync_tables,
	mock_ducklake_connect_minio,
mock_ducklake_init,
	mock_duckdb_setup,
):
	mock_duckdb_setup.return_value = mock.Mock()
	os.environ["MINIO_BUCKET_NAME"] = "test-bucket"
	ducklake_sync()
	mock_logger.info.assert_any_call("Starting DuckLake sync flow")
	assert mock_duckdb_setup.called
	assert mock_ducklake_init.called
	assert mock_ducklake_connect_minio.called
	assert mock_sync_tables.call_count == 3
	assert mock_cleanup_db_folders.call_count == 3

def test_ducklake_sync_missing_env(monkeypatch):
	monkeypatch.delenv("MINIO_BUCKET_NAME", raising=False)
	with mock.patch("src.dl_sync.duckdb_setup"), \
		 mock.patch("src.dl_sync.ducklake_init"), \
		 mock.patch("src.dl_sync.ducklake_connect_minio"), \
		 mock.patch("src.dl_sync.sync_tables"), \
		 mock.patch("src.dl_sync.cleanup_db_folders"), \
		 mock.patch("src.dl_sync.logger") as mock_logger:
		ducklake_sync()
		# Should still log start, but source_folder will be malformed
		mock_logger.info.assert_any_call("Starting DuckLake sync flow")

@mock.patch("src.dl_sync.duckdb_setup", side_effect=Exception("DB error"))
@mock.patch("src.dl_sync.logger")
def test_ducklake_sync_db_error(mock_logger, mock_duckdb_setup):
	os.environ["MINIO_BUCKET_NAME"] = "test-bucket"
	with pytest.raises(Exception):
		ducklake_sync()

@mock.patch("src.dl_sync.duckdb_setup")
@mock.patch("src.dl_sync.ducklake_init")
@mock.patch("src.dl_sync.ducklake_connect_minio")
@mock.patch("src.dl_sync.sync_tables")
@mock.patch("src.dl_sync.cleanup_db_folders")
@mock.patch("src.dl_sync.logger")
def test_ducklake_sync_empty_folders(
	mock_logger,
	mock_cleanup_db_folders,
	mock_sync_tables,
	mock_ducklake_connect_minio,
mock_ducklake_init,
	mock_duckdb_setup,
):
	mock_duckdb_setup.return_value = mock.Mock()
	os.environ["MINIO_BUCKET_NAME"] = "test-bucket"
	with mock.patch("os.path.join", return_value=""):
		ducklake_sync()
	assert mock_sync_tables.call_count == 3
