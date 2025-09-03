import os
import io
import sys
import time
import duckdb
from dotenv import load_dotenv
from logger import logger_setup

current_path = os.path.dirname(os.path.abspath(__file__))
parent_path = os.path.abspath(os.path.join(current_path, ".."))
sys.path.append(parent_path)


load_dotenv()
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
logger = logger_setup("ducklake.log")

def duckdb_setup():
    logger.info("Setting up DuckDB connection")
    duckdb.install_extension("ducklake")
    duckdb.install_extension("httpfs")
    duckdb.load_extension("ducklake")
    duckdb.load_extension("httpfs")
    logger.info("DuckDB extensions loaded successfully")

    conn = duckdb.connect(database='ducklake.db')
    logger.info("DuckDB database connected")
    return conn

def ducklake_setup(conn, data_path, catalog_path):
    logger.info(f"Setting up DuckLake connection with data path: {data_path} and catalog path: {catalog_path}")
    conn.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
    conn.execute("USE my_ducklake")
    return conn

def ducklake_connect_minio(conn):
    logger.info("Connecting to MinIO")
    conn.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
    conn.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
    conn.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
    conn.execute("SET s3_use_ssl = false")
    conn.execute("SET s3_region = 'us-east-1'")
    conn.execute("SET s3_url_style = 'path'")
    logger.info("MinIO connection settings applied to DuckDB")

def ducklake_schema_creation(conn):
    logger.info("Creating database schemas")
    conn.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    conn.execute("CREATE SCHEMA IF NOT EXISTS STAGED")
    conn.execute("CREATE SCHEMA IF NOT EXISTS CURATED")
    logger.info("DuckLake schema created successfully")

def table_creation(con, logger, bucket_name): 
    logger.info("Refreshing database with the most current data")
    file_list = f"SELECT * FROM glob('s3://{bucket_name}/*.parquet')"

    try:
        batched_files = con.execute(file_list).fetchall()
        file_paths = []
        for row in batched_files:
            file_paths.append(row[0])
        
        logger.info(f"Found {len(file_paths)} files in MinIO bucket")
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path).replace('.parquet', '')
            table_name = file_name.split('_data_')[0].upper()

            logger.info(f"Processing file: {file_path} -> table: {table_name}")

            query = f"""
            CREATE OR REPLACE TABLE RAW.{table_name} AS
            SELECT *,
                '{file_name}' AS _source_file,
                CURRENT_TIMESTAMP AS _ingestion_timestamp,
                ROW_NUMBER() OVER () AS _record_id
            FROM read_parquet('{file_path}');
            """
            
            con.execute(query)
            logger.info(f"Successfully created or updated {table_name}")

    except Exception as e:
        logger.error(f"Error processing files from MinIO: {e}")
        raise

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