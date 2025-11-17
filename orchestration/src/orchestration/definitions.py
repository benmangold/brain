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

from orchestration.marketeer.ingest.jobs import (
    process_queue_job,
    daily_queue_processing_schedule,
)

from orchestration.marketeer import assets as marketeer_assets
from orchestration.marketeer.ingest import assets as marketeer_ingest_assets
from orchestration.marketeer.persistence import assets as marketeer_persistence_assets


all_assets = load_assets_from_modules(
    [
        db_assets,
        llm_assets,
        marketeer_assets,
        marketeer_ingest_assets,
        marketeer_persistence_assets,
    ]
)

definitions = Definitions(
    assets=all_assets,
    jobs=[database_tests_job, process_queue_job],
    schedules=[
        frequent_database_tests_schedule,
        frequent_llm_tests_schedule,
        daily_queue_processing_schedule,
    ],
    resources={
        "postgres": PostgresResource(),
        "pgvector": PgVectorResource(),
        "marketeer_pg": PostgresResource(database="marketeer"),
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
        "data_processing": DataProcessingResource(
            batch_size=5,
            save_outputs=True,
            output_dir="./dagster_home/tmp/dagster_ollama_outputs",
        ),
    },
)
