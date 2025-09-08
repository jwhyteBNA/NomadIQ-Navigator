import os
import io
import time
import requests
import polars as pl
from minio import Minio
from datetime import datetime
from dotenv import load_dotenv
from logger import logger_setup
from prefect import task, flow
from prefect.client.schemas.schedules import CronSchedule

logger = logger_setup("data_ingestion.log")
load_dotenv()


def get_minio_client():
    return Minio(
        os.getenv('MINIO_EXTERNAL_URL'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )

MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

PARKS_URL = os.getenv('NPS_PARKS_ENDPOINT')
ALERTS_URL = os.getenv('NPS_ALERTS_ENDPOINT')
NPS_API_KEY = os.getenv('NPS_API_KEY')

@task
def fetch_all_nps_data(api_key, base_url):
    batch_size = 50
    all_data = []
    start = 0
    total = None
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
    return all_data

@task
def convert_to_csv(data):
    df = pl.json_normalize(data)
    buffer = io.BytesIO()
    df.write_csv(buffer)
    buffer.seek(0)
    return buffer

@task
def convert_to_parquet(data):
    data = pl.json_normalize(data)
    buffer = io.BytesIO()
    data.write_parquet(buffer)
    buffer.seek(0)
    return buffer

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

@flow()
def data_ingestion_flow():
    start_time = time.time()
    logger.info("Starting data ingestion process at %s.", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    parks_data = fetch_all_nps_data(NPS_API_KEY, PARKS_URL)
    parks_data_parquet = convert_to_parquet(parks_data)
    save_to_minio(parks_data_parquet, MINIO_BUCKET_NAME, "parks_data.parquet")
    alerts_data = fetch_all_nps_data(NPS_API_KEY, ALERTS_URL)
    alerts_data_parquet = convert_to_parquet(alerts_data)
    save_to_minio(alerts_data_parquet, MINIO_BUCKET_NAME, "alerts_data.parquet")

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Ingestion process completed in {duration:.2f} seconds.")


if __name__ == "__main__":
    data_ingestion_flow.serve(
        name="data_ingestion_flow",
        schedule=CronSchedule(
            cron="0 2 * * *",
            timezone="UTC"
        ),
        tags=["Pipeline", "Ingestion"]
    )
