from dagster import define_asset_job, ScheduleDefinition
from orchestration.marketeer.ingest.assets import marketeer_process_ingest_queue_batch

# Define a job that processes the queue
process_queue_job = define_asset_job(
    name="process_sp500_ingest_queue",
    selection=[marketeer_process_ingest_queue_batch],
    description="Process a batch of S&P 500 historical data ingestion tasks",
)

# Schedule to run daily
daily_queue_processing_schedule = ScheduleDefinition(
    job=process_queue_job,
    cron_schedule="*/1 * * * *",
    name="daily_sp500_ingest_queue_processing",
)
