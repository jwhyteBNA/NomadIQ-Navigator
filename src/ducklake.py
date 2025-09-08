import os
import sys
import time
from dotenv import load_dotenv
from logger import logger_setup
from utilities import duckdb_setup, ducklake_setup, ducklake_connect_minio, ducklake_schema_creation, table_creation

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)

load_dotenv()
logger = logger_setup("ducklake.log")

def main():
    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")
    minio_bucket = os.getenv('MINIO_BUCKET_NAME')
    start_time = time.time()
    
    conn = duckdb_setup()
    ducklake_setup(conn, data_path, catalog_path)
    ducklake_connect_minio(conn)
    ducklake_schema_creation(conn)
    table_creation(conn, logger, minio_bucket)

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Data processing completed in {duration:.2f} seconds")

if __name__ == "__main__":
    main()