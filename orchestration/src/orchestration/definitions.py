from dagster import Definitions, load_assets_from_modules

from .db.connectivity_test import assets as db_assets
from orchestration.llm.connectivity_test import assets as llm_assets
from orchestration.llm.connectivity_test.jobs import frequent_llm_tests_schedule
from orchestration.llm.resources import OllamaResource, DataProcessingResource

from orchestration.db.resources import (
    PostgresResource,
    PgVectorResource,
    MongoResource,
    RedisResource,
    DynamoDBResource,
)

from orchestration.db.connectivity_test.jobs import (
    database_tests_job,
    frequent_database_tests_schedule,
)


all_assets = load_assets_from_modules([db_assets, llm_assets])

definitions = Definitions(
    assets=all_assets,
    jobs=[database_tests_job],
    schedules=[frequent_database_tests_schedule, frequent_llm_tests_schedule],
    resources={
        "postgres": PostgresResource(),
        "pgvector": PgVectorResource(),
        "mongo": MongoResource(),
        "redis": RedisResource(),
        "dynamodb": DynamoDBResource(),
        "ollama": OllamaResource(
            primary_model="llama3.2:1b",
            fallback_model="llama3.2:3b",  # Optional fallback
            temperature=0.7,
            max_tokens=500,
            timeout=30,
            num_ctx=2048,
        ),
    },
)
