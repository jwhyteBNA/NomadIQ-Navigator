import os
import io
import duckdb
import requests
import polars as pl
from minio import Minio
from prefect import task
from prefect.cache_policies import NO_CACHE
from datetime import datetime
from dotenv import load_dotenv
from src.logger import logger_setup

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
        raise

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
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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

def ducklake_init(conn, data_path, catalog_path):
    try:
        logger.info(f"Setting up DuckLake connection with data path: {data_path} and catalog path: {catalog_path}")
        conn.execute(f"ATTACH 'ducklake:{catalog_path}' AS my_ducklake (DATA_PATH '{data_path}')")
        conn.execute("USE my_ducklake")
        return conn
    except Exception as e:
        logger.error(f"DuckLake setup failed: {e}")
        raise


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


def ducklake_schema_creation(conn):
    logger.info("Creating database schemas")
    conn.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    conn.execute("CREATE SCHEMA IF NOT EXISTS STAGED")
    conn.execute("CREATE SCHEMA IF NOT EXISTS CURATED")
    logger.info("DuckLake schema created successfully")

@task
def get_latest_minio_files(file_paths):
    sources = {}
    for path in file_paths:
        file_name = os.path.basename(path).replace('.parquet', '')
        parts = file_name.split('_')
        prefix = '_'.join(parts[:2]) 
        timestamp = '_'.join(parts[2:])  
        if prefix not in sources or timestamp > sources[prefix][1]:
            sources[prefix] = (path, timestamp)
    return [info[0] for info in sources.values()]

@task
def remove_old_files(file_paths, latest_files):
    old_files = set(file_paths) - set(latest_files)
    for file_path in old_files:
        try:
            os.remove(file_path)
            print(f"Removed old file: {file_path}")
        except Exception as e:
            print(f"Failed to remove {file_path}: {e}")


def sync_tables(conn, logger, source_folder, schema="RAW", mode=None):
    logger.info(f"Syncing tables from files in {source_folder} to schema {schema}")
    if source_folder and str(source_folder).startswith("s3://"):
        mode = "ingest"
    elif source_folder and os.path.isdir(source_folder):
        mode = "transform"
    else:
        logger.error("Invalid source_folder or unable to determine mode.")
        return

    if mode == "ingest":
        file_list_query = f"SELECT * FROM glob('{source_folder}/*.parquet')"
        batched_files = conn.execute(file_list_query).fetchall()
        file_paths = [row[0] for row in batched_files]
        logger.info(f"Total files found: {len(file_paths)}")
        latest_files = get_latest_minio_files(file_paths)
        logger.info(f"Number of files processed (latest): {len(latest_files)}")
        for file_path in latest_files:
            file_name = os.path.basename(file_path).replace('.parquet', '')
            source_name = file_name.split('_data')[0].upper()
            table_name = f"{schema}.{source_name}"
            query = f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT *,
                '{file_name}' AS _source_file,
                CURRENT_TIMESTAMP AS _ingestion_timestamp,
                ROW_NUMBER() OVER () AS _record_id
            FROM read_parquet('{file_path}');
            """
            conn.execute(query)
            logger.info(f"Successfully created or updated {table_name}")
    elif mode == "transform":
        sql_files = [f for f in os.listdir(source_folder) if f.lower().endswith('.sql')]
        logger.info(f"Total SQL files found: {len(sql_files)} in {source_folder}")
        if not sql_files:
            logger.warning(f"No .SQL files found in {source_folder}")
        for sql_file in sql_files:
            table_name = sql_file.replace('.SQL', '').replace('.sql', '')
            sql_path = os.path.join(source_folder, sql_file)
            with open(sql_path, 'r') as f:
                sql_script = f.read()
            conn.execute(sql_script)
            logger.info(f"Ran transformation for {table_name}")
    else:
        logger.error("Invalid mode or missing sql_folder for transformation.")

@task
def cleanup_db_folders(folder):
    for subfolder in os.listdir(folder):
        subfolder_path = os.path.join(folder, subfolder)
        if os.path.isdir(subfolder_path):
            file_paths = [
                os.path.join(subfolder_path, f)
                for f in os.listdir(subfolder_path)
                if f.endswith('.parquet')
            ]
            if not file_paths:
                continue
            file_paths.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            for file_path in file_paths[1:]:
                try:
                    os.remove(file_path)
                    print(f"Removed old file: {file_path}")
                except Exception as e:
                    print(f"Failed to remove {file_path}: {e}")