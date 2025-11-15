"""Dagster definitions."""

from dagster import Definitions, load_assets_from_modules

from .db.connectivity_test import assets as db_assets

from .llm.connectivity_test import assets as llm_assets

from orchestration.db.resources import (
    PostgresResource,
    PgVectorResource,
    MongoResource,
    RedisResource,
    DynamoDBResource,
)
from orchestration.db.connectivity_test.jobs import (
    database_tests_job,
    daily_database_tests_schedule,
    hourly_database_tests_schedule,
    frequent_database_tests_schedule,
)

all_assets = load_assets_from_modules([db_assets, llm_assets])

definitions = Definitions(
    assets=all_assets,
    jobs=[database_tests_job],
    schedules=[
        daily_database_tests_schedule,
        hourly_database_tests_schedule,  # Uncomment to enable
        frequent_database_tests_schedule,  # Uncomment for dev/testing
    ],
    resources={
        "postgres": PostgresResource(),
        "pgvector": PgVectorResource(),
        "mongo": MongoResource(),
        "redis": RedisResource(),
        "dynamodb": DynamoDBResource(),
    },
)