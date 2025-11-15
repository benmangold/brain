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

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres": PostgresResource(),
        "pgvector": PgVectorResource(),
        "mongo": MongoResource(),
        "redis": RedisResource(),
        "dynamodb": DynamoDBResource(),
    },
)