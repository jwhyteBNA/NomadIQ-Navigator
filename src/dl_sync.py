import os
import sys
import time
from prefect import task, flow
from dotenv import load_dotenv
from src.logger import logger_setup
from src.data_validation import data_quality_checks
from src.utilities import duckdb_setup, ducklake_init, ducklake_connect_minio, sync_tables, cleanup_db_folders

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

logger = logger_setup("dl_sync.log")
load_dotenv()

 
def ducklake_sync():
    logger.info("Starting DuckLake sync flow")
    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    raw_ducklake_folder = os.path.join(data_path, "RAW")
    source_folder = f"s3://{os.getenv('MINIO_BUCKET_NAME')}"
    start_time = time.time()
    
    conn = duckdb_setup()
    ducklake_init(conn, data_path, catalog_path)
    ducklake_connect_minio(conn)

    sync_tables(conn, logger, source_folder, schema="RAW", mode="ingest")
    cleanup_db_folders(raw_ducklake_folder)
    data_quality_checks()

    transform_folder = os.path.join(parent_path, "sql")
    staged_sql_folder = os.path.join(transform_folder, "staged")
    staged_ducklake_folder = os.path.join(data_path, "STAGED")
    sync_tables(conn, logger, staged_sql_folder, schema="STAGED", mode="transform")
    cleanup_db_folders(staged_ducklake_folder)

    # curated_folder = os.path.join(sql_folder, "curated")
    # sync_tables(conn, logger, curated_folder, schema="CURATED", mode="transform")

    conn.close()
    logger.info("DuckLake connection closed")

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Data processing completed in {duration:.2f} seconds")

if __name__ == "__main__":
    ducklake_sync()