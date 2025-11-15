"""Dagster jobs for database testing."""

from dagster import (
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)


# Define a job that materializes all database test assets
database_tests_job = define_asset_job(
    name="database_connectivity_tests",
    selection=AssetSelection.all(),
    description="Test connectivity to all local databases",
)


# Schedule to run every day at 8 AM
daily_database_tests_schedule = ScheduleDefinition(
    job=database_tests_job,
    cron_schedule="0 8 * * *",  # Every day at 8:00 AM
    name="daily_database_tests",
    description="Run database connectivity tests daily at 8 AM",
)


# Schedule to run every hour
hourly_database_tests_schedule = ScheduleDefinition(
    job=database_tests_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
    name="hourly_database_tests",
    description="Run database connectivity tests every hour",
)


# Schedule to run every 5 minutes (useful for development/testing)
frequent_database_tests_schedule = ScheduleDefinition(
    job=database_tests_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    name="frequent_database_tests",
    description="Run database connectivity tests every 5 minutes (dev/test only)",
)