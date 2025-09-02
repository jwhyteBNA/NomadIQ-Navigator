import os
import io
import time
import requests
import polars as pl
import pandas as pd
from minio import Minio
from datetime import datetime
from dotenv import load_dotenv
from logger import logger_setup

logger = logger_setup("data_ingestion.log")
load_dotenv()

MINIO_EXTERNAL_URL = os.getenv('MINIO_EXTERNAL_URL')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
minio_client = Minio(
    MINIO_EXTERNAL_URL,
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    secure=False
)

PARKS_URL = os.getenv('NPS_PARKS_ENDPOINT')
ALERTS_URL = os.getenv('NPS_ALERTS_ENDPOINT')
NPS_API_KEY = os.getenv('NPS_API_KEY')


def get_raw_parks_data():
    response = requests.get(PARKS_URL, headers={"Authorization": f"Bearer {NPS_API_KEY}"})
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Failed to retrieve parks data: {response.status_code}")
        return {"error": "Failed to retrieve parks data"}


def fetch_all_nps_data(api_key, base_url):
    batch_size = 50
    all_data = []
    start = 0
    total = None
    while True:
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
        if len(all_data) >= total:
            break
    return all_data

def convert_to_parquet(data):
    data = pd.json_normalize(data)
    buffer = io.BytesIO()
    data.to_parquet(buffer, index=False)
    buffer.seek(0)
    return buffer

def save_to_minio(buffer, bucket_name, object_name):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamped_filename = f"{object_name.split('.')[0]}_{timestamp}.parquet"
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


def main():

    parks_data = fetch_all_nps_data(NPS_API_KEY, PARKS_URL)
    parks_data_parquet = convert_to_parquet(parks_data)
    save_to_minio(parks_data_parquet, MINIO_BUCKET_NAME, "parks_data.parquet")
    alerts_data = fetch_all_nps_data(NPS_API_KEY, ALERTS_URL)
    alerts_data_parquet = convert_to_parquet(alerts_data)
    save_to_minio(alerts_data_parquet, MINIO_BUCKET_NAME, "alerts_data.parquet")


if __name__ == "__main__":
    main()
