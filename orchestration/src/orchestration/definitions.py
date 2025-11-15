"""Dagster definitions."""

from dagster import Definitions, load_assets_from_modules

from . import assets
from .resources import (
    PostgresResource,
    PgVectorResource,
    MongoResource,
    RedisResource,
    DynamoDBResource,
)
from .jobs import (
    database_tests_job,
    daily_database_tests_schedule,
    hourly_database_tests_schedule,
    frequent_database_tests_schedule,
)

all_assets = load_assets_from_modules([assets])

defs = Definitions(
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