import os
import time
from datetime import datetime
from dotenv import load_dotenv
from logger import logger_setup
from prefect import task, flow
from prefect.client.schemas.schedules import CronSchedule
from utilities import fetch_all_nps_data, convert_json_to_parquet, save_to_minio

logger = logger_setup("data_ingestion.log")
load_dotenv()

MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')

NPS_API_KEY = os.getenv('NPS_API_KEY')
PARKS_URL = os.getenv('NPS_PARKS_ENDPOINT')
ALERTS_URL = os.getenv('NPS_ALERTS_ENDPOINT')

@flow
def data_ingestion_flow():
    try:
        start_time = time.time()
        logger.info("Starting data ingestion process at %s.", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        parks_data = fetch_all_nps_data(NPS_API_KEY, PARKS_URL)
        parks_data_parquet = convert_json_to_parquet(parks_data)
        save_to_minio(parks_data_parquet, MINIO_BUCKET_NAME, "parks_data.parquet")
        alerts_data = fetch_all_nps_data(NPS_API_KEY, ALERTS_URL)
        alerts_data_parquet = convert_json_to_parquet(alerts_data)
        save_to_minio(alerts_data_parquet, MINIO_BUCKET_NAME, "alerts_data.parquet")

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Ingestion process completed in {duration:.2f} seconds.")
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")


if __name__ == "__main__":
    data_ingestion_flow.serve(
        name="data_ingestion_flow",
        schedule=CronSchedule(
            cron="0 2 * * *",
            timezone="America/Chicago"
        ),
        tags=["Pipeline", "Ingestion"]
    )
