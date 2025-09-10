import os
from dotenv import load_dotenv
from src.logger import logger_setup
from src.utilities import duckdb_setup, ducklake_init, ducklake_connect_minio, ducklake_schema_creation

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))

load_dotenv()
logger = logger_setup("ducklake.log")

def setup_ducklake():
    logger.info("Starting DuckLake setup")

    data_path = os.path.join(parent_path, "data")
    catalog_path = os.path.join(parent_path, "catalog.ducklake")

    conn = duckdb_setup()
    ducklake_init(conn, data_path, catalog_path)
    ducklake_connect_minio(conn)
    ducklake_schema_creation(conn)
    conn.close()

    logger.info("Success! DuckLake setup complete.")

if __name__ == "__main__":
    setup_ducklake()