from dagster import asset, AssetExecutionContext
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # <-- ADD THIS LINE
import psycopg2
from orchestration.db.resources import PostgresResource
from orchestration.marketeer.ingest.sp500_members import SP500Members

MARKETEER_ASSETS_GROUP_NAME = "marketeer_ingest"


@asset(group_name=MARKETEER_ASSETS_GROUP_NAME)
def marketeer_database(context: AssetExecutionContext, postgres: PostgresResource):
    with postgres.get_connection() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cur:
            try:
                cur.execute("CREATE DATABASE marketeer")
                context.log.info("Database 'marketeer' created successfully")
            except psycopg2.errors.DuplicateDatabase:
                context.log.info(
                    "Database 'marketeer' already exists, skipping creation"
                )


@asset(group_name=MARKETEER_ASSETS_GROUP_NAME, deps=[marketeer_database])
def marketeer_ingest_schema(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("CREATE SCHEMA ingest;")
            except psycopg2.errors.DuplicateSchema:
                context.log.info("Schema 'ingest' already exists, skipping creation")


@asset(group_name=MARKETEER_ASSETS_GROUP_NAME, deps=[marketeer_ingest_schema])
def marketeer_ingest_sp500members_table(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS ingest.sp500_members(
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    json json NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    source TEXT NOT NULL
                );
            """
            )
            conn.commit()


@asset(
    group_name=MARKETEER_ASSETS_GROUP_NAME, deps=[marketeer_ingest_sp500members_table]
)
def marketeer_ingest_sp500members(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    """Create the ingest schema if it doesn't exist"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            stock_list = SP500Members.ingest(cur, conn)
            return stock_list
