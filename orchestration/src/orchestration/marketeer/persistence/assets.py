import psycopg2
from dagster import asset, AssetExecutionContext
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from orchestration.db.resources import PostgresResource

MARKETEER_PERSISTENCE_GROUP_NAME = "marketeer_persistence"

MARKETEER_DB_NAME = "marketeer"
INGEST_SCHEMA_NAME = "ingest"
SP500_MEMBERS_TABLE_NAME = "sp500_members"
SP500_MEMBERS_DAILY_TABLE_NAME = "sp500_member_daily"
SP500_MEMBERS_DAILY_QUEUE_NAME = "sp500_member_daily_queue"


@asset(group_name=MARKETEER_PERSISTENCE_GROUP_NAME)
def marketeer_database(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            try:
                cur.execute(f"CREATE DATABASE {MARKETEER_DB_NAME}")
                context.log.info(f"Database '{MARKETEER_DB_NAME}' created successfully")
            except psycopg2.errors.DuplicateDatabase:
                context.log.info(
                    f"Database '{MARKETEER_DB_NAME}' already exists, skipping creation"
                )


@asset(group_name=MARKETEER_PERSISTENCE_GROUP_NAME, deps=[marketeer_database])
def marketeer_ingest_schema(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"CREATE SCHEMA {INGEST_SCHEMA_NAME};")
            except psycopg2.errors.DuplicateSchema:
                context.log.info(
                    f"Schema '{INGEST_SCHEMA_NAME}' already exists, skipping creation"
                )


@asset(group_name=MARKETEER_PERSISTENCE_GROUP_NAME, deps=[marketeer_ingest_schema])
def marketeer_ingest_sp500_members_table(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_TABLE_NAME}(
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    json json NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL
                );
            """
            )
            conn.commit()


@asset(group_name=MARKETEER_PERSISTENCE_GROUP_NAME, deps=[marketeer_ingest_schema])
def marketeer_ingest_sp500_member_historical_data_table(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_TABLE_NAME}(
                    date date NOT NULL,
                    symbol TEXT NOT NULL,
                    json json NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL
                );
            """
            )
            conn.commit()


@asset(group_name=MARKETEER_PERSISTENCE_GROUP_NAME, deps=[marketeer_ingest_schema])
def marketeer_ingest_sp500_member_historical_data_queue(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}(
                    id SERIAL PRIMARY KEY,
                    stock_symbol VARCHAR(10) NOT NULL,
                    period VARCHAR(10) NOT NULL,
                    interval VARCHAR(10) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',  -- pending, processing, completed, failed
                    priority INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 3
                );
                
                CREATE INDEX IF NOT EXISTS idx_queue_status ON {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}(status);
                CREATE INDEX IF NOT EXISTS idx_queue_priority ON {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}(priority DESC, created_at ASC);
            """
            )
            conn.commit()
