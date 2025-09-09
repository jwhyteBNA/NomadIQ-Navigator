import os
import io
import duckdb
import requests
import polars as pl
from minio import Minio
from prefect import task
from datetime import datetime
from dotenv import load_dotenv
from logger import logger_setup

load_dotenv()
logger = logger_setup("utilities.log")

def get_minio_client():
    return Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )

@task
def fetch_all_nps_data(api_key, base_url):
    batch_size = 50
    all_data = []
    start = 0
    total = None
    try:
        logger.info("Starting NPS data fetch...")
        while total is None or len(all_data) < total:
            params = {
                "api_key": api_key,
                "limit": batch_size,
                "start": start
            }
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            result = response.json()
            data = result.get("data", [])
            all_data.extend(data)
            if total is None:
                total = int(result.get("total", len(data)))
            start += batch_size
        logger.info(f"Fetched {len(all_data)} records from {base_url}")
        return all_data
    except Exception as e:
        logger.error(f"Error fetching NPS data: {e}")

@task
def convert_to_csv(data):
    df = pl.json_normalize(data)
    buffer = io.BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)
    return buffer

@task
def convert_json_to_parquet(data):
    try:
        logger.info("Converting JSON data to Parquet format")
        if not data:
            raise ValueError("No data provided for conversion")
        data = pl.json_normalize(data)
        buffer = io.BytesIO()
        data.write_parquet(buffer)
        buffer.seek(0)
        return buffer
    except Exception as e:
        logger.error(f"Error converting JSON to Parquet: {e}")
        return None

@task
def save_to_minio(buffer, bucket_name, object_name):
    minio_client = get_minio_client()
    try:
        ext = object_name.split('.')[-1]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamped_filename = f"{object_name.split('.')[0]}_{timestamp}.{ext}"
        data_bytes = buffer.getvalue()
        minio_client.put_object(
            bucket_name,
            timestamped_filename,
            io.BytesIO(data_bytes),
            length=len(data_bytes)
        )
        logger.info(f"Successfully uploaded {timestamped_filename} to MinIO bucket {bucket_name}")
    except Exception as e:
        logger.error(f"Failed to upload {timestamped_filename} to MinIO: {e}")

@task
def duckdb_setup():
    try:
        logger.info("Setting up DuckDB connection")
        duckdb.install_extension("ducklake")
        duckdb.install_extension("httpfs")
        duckdb.load_extension("ducklake")
        duckdb.load_extension("httpfs")
        logger.info("DuckDB extensions loaded successfully")

        conn = duckdb.connect(database='ducklake.db')
        logger.info("DuckDB database connected")
        return conn
    except Exception as e:
        logger.error(f"DuckDB setup failed: {e}")
        raise

@task
def ducklake_setup(conn, data_path, catalog_path):
    try:
        logger.info(f"Setting up DuckLake connection with data path: {data_path} and catalog path: {catalog_path}")
        conn.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
        conn.execute("USE my_ducklake")
        return conn
    except Exception as e:
        logger.error(f"DuckLake setup failed: {e}")
        raise

@task
def ducklake_connect_minio(conn):
    try:
        logger.info("Connecting to MinIO")
        conn.execute(f"SET s3_access_key_id = '{os.getenv('MINIO_ACCESS_KEY')}'")
        conn.execute(f"SET s3_secret_access_key = '{os.getenv('MINIO_SECRET_KEY')}'")
        conn.execute(f"SET s3_endpoint = '{os.getenv('MINIO_EXTERNAL_URL')}'")
        conn.execute("SET s3_use_ssl = false")
        conn.execute("SET s3_region = 'us-east-1'")
        conn.execute("SET s3_url_style = 'path'")
        logger.info("MinIO connection settings applied to DuckDB")
    except Exception as e:
        logger.error(f"Failed to connect to MinIO: {e}")
        raise

@task
def ducklake_schema_creation(conn):
    logger.info("Creating database schemas")
    conn.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    conn.execute("CREATE SCHEMA IF NOT EXISTS STAGED")
    conn.execute("CREATE SCHEMA IF NOT EXISTS CURATED")
    logger.info("DuckLake schema created successfully")

@task
def table_creation(conn, logger, bucket_name): 
    logger.info("Refreshing database with the most current data")
    file_list = f"SELECT * FROM glob('s3://{bucket_name}/*.parquet')"

    try:
        batched_files = conn.execute(file_list).fetchall()
        file_paths = []
        for row in batched_files:
            file_paths.append(row[0])
        
        logger.info(f"Found {len(file_paths)} files in MinIO bucket")
        
        for file_path in file_paths:
            file_name = os.path.basename(file_path).replace('.parquet', '')
            table_name = file_name.split('_data_')[0].upper()

            logger.info(f"Processing file: {file_path} -> table: {table_name}")
            #Factor this out
            query = f"""
            CREATE OR REPLACE TABLE RAW.{table_name} AS
            SELECT *,
                '{file_name}' AS _source_file,
                CURRENT_TIMESTAMP AS _ingestion_timestamp,
                ROW_NUMBER() OVER () AS _record_id
            FROM read_parquet('{file_path}');
            """

            conn.execute(query)
            logger.info(f"Successfully created or updated {table_name}")

    except Exception as e:
        logger.error(f"Error processing files from MinIO: {e}")
        raise