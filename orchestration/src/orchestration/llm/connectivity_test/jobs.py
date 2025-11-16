"""Dagster jobs for database testing."""

from dagster import (
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)

from orchestration.llm.connectivity_test.assets import LLM_CONNECTIVITY_TEST_GROUP_NAME

# Define a job that materializes all database test assets
llm_tests_job = define_asset_job(
    name="llm_connectivity_tests",
    selection=AssetSelection.groups(LLM_CONNECTIVITY_TEST_GROUP_NAME),
    description="Test connectivity to local llm",
)

# Schedule to run every 5 minutes (useful for development/testing)
frequent_llm_tests_schedule = ScheduleDefinition(
    job=llm_tests_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    name="frequent_llm_tests",
    description="Run llm tests every 5 minutes (dev/test only)",
)
