import time
from prefect import flow
from src.logger import logger_setup
from src.dl_sync import ducklake_sync
from src.data_ingestion import data_ingestion
from prefect.client.schemas.schedules import CronSchedule

logger = logger_setup("ducklake_pipeline.log")

@flow
def pipeline_flow():
    start_time = time.time()

    data_ingestion()
    ducklake_sync()

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Full pipeline process completed in {duration:.2f} seconds.")


if __name__ == "__main__":
	pipeline_flow.serve(
		name="nomadiq_parks_pipeline",
		schedule=CronSchedule(
			cron="0 2 * * *",
			timezone="America/Chicago"
		),
		tags=["Pipeline", "Master"]
	)
