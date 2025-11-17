from dagster import asset, AssetExecutionContext, Config, MaterializeResult
from orchestration.marketeer.ingest.assets import marketeer_ingest_sp500_members
from orchestration.marketeer.persistence.assets import (
    marketeer_ingest_sp500_member_historical_data_queue,
)
from orchestration.db.resources import PostgresResource

MARKETEER_ASSETS_GROUP_NAME = "marketeer"


@asset(deps=[marketeer_ingest_sp500_members], group_name=MARKETEER_ASSETS_GROUP_NAME)
def sp500_members(context: AssetExecutionContext, marketeer_pg: PostgresResource):
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "select distinct symbol from ingest.sp500_members order by symbol asc;"
            )
            symbols = cur.fetchall()

            # Extract just the symbols from the tuples
            symbol_list = [row[0] for row in symbols]

            # Log the results
            context.log.info(f"Retrieved {len(symbol_list)} S&P 500 symbols")

            # Return as MaterializeResult with metadata
            return MaterializeResult(
                metadata={"num_symbols": len(symbol_list), "symbols": symbol_list}
            )


@asset(
    group_name=MARKETEER_ASSETS_GROUP_NAME,
    deps=[marketeer_ingest_sp500_member_historical_data_queue],
)
def sp500_member_historical_member_data_ingest_queue_status(
    context: AssetExecutionContext,
    marketeer_pg: PostgresResource,
) -> dict:
    """Get current status of the ingestion queue"""

    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    status,
                    COUNT(*) as count,
                    AVG(retry_count) as avg_retries
                FROM ingest.sp500_member_historical_data_queue
                GROUP BY status
            """
            )

            status_counts = {}
            for row in cur.fetchall():
                status, count, avg_retries = row
                status_counts[status] = {
                    "count": count,
                    "avg_retries": float(avg_retries) if avg_retries else 0,
                }

    context.log.info(f"Queue status: {status_counts}")
    return status_counts
