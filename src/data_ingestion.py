import os
import time
from datetime import datetime
from dotenv import load_dotenv
from prefect import task, flow
from src.logger import logger_setup
from src.utilities import fetch_all_nps_data, convert_json_to_parquet, save_to_minio

logger = logger_setup("data_ingestion.log")
load_dotenv()

MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

NPS_API_KEY = os.getenv('NPS_API_KEY')
PARKS_URL = os.getenv('NPS_PARKS_ENDPOINT')
ALERTS_URL = os.getenv('NPS_ALERTS_ENDPOINT')

@task
def fetch_nps_data_task(api_key, url):
    return fetch_all_nps_data(api_key, url)

@task
def convert_to_parquet_task(data):
    return convert_json_to_parquet(data)

@task
def save_parquet_to_minio_task(parquet_data, bucket, filename):
    return save_to_minio(parquet_data, bucket, filename)

@flow
def data_ingestion():
    start_time = time.time()
    logger.info("Starting data ingestion process at %s.", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    try:
        parks_data = fetch_nps_data_task(NPS_API_KEY, PARKS_URL)
        parks_data_parquet = convert_to_parquet_task(parks_data)
        save_parquet_to_minio_task(parks_data_parquet, MINIO_BUCKET_NAME, "parks_data.parquet")

        alerts_data = fetch_nps_data_task(NPS_API_KEY, ALERTS_URL)
        alerts_data_parquet = convert_to_parquet_task(alerts_data)
        save_parquet_to_minio_task(alerts_data_parquet, MINIO_BUCKET_NAME, "alerts_data.parquet")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Ingestion process completed in {duration:.2f} seconds.")
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")


