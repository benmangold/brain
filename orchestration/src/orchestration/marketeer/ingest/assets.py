from dagster import asset, AssetExecutionContext, Config
from dataclasses import dataclass
from typing import List, Optional


from orchestration.db.resources import PostgresResource
from orchestration.marketeer.ingest.sp500_members import SP500Members
from orchestration.marketeer.ingest.sp500_member_historical_data import (
    SP500MemberHistoricalData,
)
from pydantic import Field

from orchestration.marketeer.persistence.assets import (
    marketeer_ingest_sp500_members_table,
    marketeer_ingest_sp500_member_historical_data_table,
    marketeer_ingest_sp500_member_historical_data_queue,
    MARKETEER_DB_NAME,
    INGEST_SCHEMA_NAME,
    SP500_MEMBERS_TABLE_NAME,
    SP500_MEMBERS_DAILY_TABLE_NAME,
    SP500_MEMBERS_DAILY_QUEUE_NAME,
)

MARKETEER_INGEST_ASSETS_GROUP_NAME = "marketeer_ingest"
MARKETEER_ASSETS_GROUP_NAME = "marketeer_ingest"


@dataclass
class QueueItem:
    id: int
    stock_symbol: str
    period: str
    interval: str
    status: str
    priority: int
    retry_count: int
    max_retries: int
    error_message: Optional[str] = None


@asset(
    group_name=MARKETEER_INGEST_ASSETS_GROUP_NAME,
    deps=[marketeer_ingest_sp500_members_table],
)
def marketeer_ingest_sp500_members(
    context: AssetExecutionContext, marketeer_pg: PostgresResource
):
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            stock_list = SP500Members.ingest(cur, conn)
            return stock_list


class SP500MemberDailyIngestConfig(Config):
    stock_list: list[str] = Field(
        default=["MSFT"], description="List of stock symbols to ingest"
    )
    period: str = Field(default="max", description="Time period for historical data")
    interval: str = Field(default="1d", description="Data interval")


@asset(
    group_name=MARKETEER_ASSETS_GROUP_NAME,
    deps=[
        marketeer_ingest_sp500_member_historical_data_queue,
        marketeer_ingest_sp500_members,
    ],
)
def enqueue_sp500_daily_full_history(
    context: AssetExecutionContext,
    marketeer_pg: PostgresResource,
) -> int:
    """Enqueue ingestion tasks for all S&P 500 members"""
    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"select distinct symbol from {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_TABLE_NAME} order by symbol asc;"
            )
            symbols = cur.fetchall()

            # Extract just the symbols from the tuples
            stock_list = [row[0] for row in symbols]

            queued_count = 0
            # enqueue d
            for stock_symbol in stock_list:
                cur.execute(
                    f"""
                    INSERT INTO {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME} 
                    (stock_symbol, period, interval, status, priority)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (stock_symbol, "max", "1d", "pending", 0),
                )
                queued_count += 1

            conn.commit()
            context.log.info(f"Queued {queued_count} new ingestion tasks")

    return queued_count


@asset(
    group_name=MARKETEER_ASSETS_GROUP_NAME,
    deps=[marketeer_ingest_sp500_member_historical_data_table],
)
def marketeer_process_ingest_queue_batch(
    context: AssetExecutionContext,
    marketeer_pg: PostgresResource,
) -> dict:
    """Process a batch of items from the ingestion queue"""

    processed = 0
    failed = 0
    batch_size: int = 5

    with marketeer_pg.get_connection() as conn:
        with conn.cursor() as cur:
            # Fetch pending items
            cur.execute(
                f"""
                SELECT id, stock_symbol, period, interval, retry_count, max_retries
                FROM {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}
                WHERE status = 'pending'
                ORDER BY priority DESC, created_at ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            """,
                (batch_size,),
            )

            items = cur.fetchall()
            context.log.info(f"Processing {len(items)} queue items")

            for item in items:
                item_id, stock_symbol, period, interval, retry_count, max_retries = item

                try:
                    # Mark as processing
                    cur.execute(
                        f"""
                        UPDATE {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}
                        SET status = 'processing', started_at = NOW()
                        WHERE id = %s
                    """,
                        (item_id,),
                    )
                    conn.commit()

                    # Process the ingestion
                    context.log.info(f"Ingesting data for {stock_symbol}")
                    SP500MemberHistoricalData.ingest(
                        cur,
                        conn,
                        SP500_MEMBERS_DAILY_TABLE_NAME,
                        stock_list=[stock_symbol],
                        period=period,
                        interval=interval,
                    )

                    # Mark as completed
                    cur.execute(
                        f"""
                        UPDATE {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}
                        SET status = 'completed', 
                            completed_at = NOW(),
                            error_message = NULL
                        WHERE id = %s
                    """,
                        (item_id,),
                    )
                    conn.commit()
                    processed += 1

                except Exception as e:
                    error_msg = str(e)
                    context.log.error(f"Failed to ingest {stock_symbol}: {error_msg}")

                    # Update retry count and status
                    if retry_count + 1 >= max_retries:
                        new_status = "failed"
                    else:
                        new_status = "pending"

                    cur.execute(
                        f"""
                        UPDATE {INGEST_SCHEMA_NAME}.{SP500_MEMBERS_DAILY_QUEUE_NAME}
                        SET status = %s,
                            retry_count = retry_count + 1,
                            error_message = %s,
                            started_at = NULL
                        WHERE id = %s
                    """,
                        (new_status, error_msg, item_id),
                    )
                    conn.commit()
                    failed += 1

    result = {"processed": processed, "failed": failed, "total": len(items)}
    context.log.info(f"Batch processing complete: {result}")
    return result


class ManualEnqueueConfig(Config):
    stock_symbols: List[str] = Field(description="List of stock symbols to enqueue")
    period: str = Field(default="1y")
    interval: str = Field(default="1d")
    priority: int = Field(default=0)
