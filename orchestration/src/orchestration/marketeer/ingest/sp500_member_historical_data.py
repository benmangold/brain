import yfinance as yf

from datetime import datetime, date, timezone

import pandas as pd

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


class SP500MemberHistoricalData:
    @staticmethod
    def ingest(
        cur, conn, table, stock_list=["MMM", "MSFT"], period="1y", interval="1d"
    ):
        data = yf.download(
            tickers=stock_list,
            period=period,
            interval=interval,
            group_by="ticker",
            auto_adjust=False,
            prepost=False,
            threads=False,
            proxy=None,
        )
        hist = data.T
        now = datetime.now(timezone.utc)
        for t in stock_list:

            # printing name
            print(t)
            print("\n")

            # used data.loc as it takes only index
            # labels and returns dataframe
            symbol_data = hist.loc[t]
            for column in symbol_data:
                cur.execute(
                    f"""
                    INSERT INTO {INGEST_SCHEMA_NAME}.{table} (date, symbol, json, created_at, source) VALUES (%s, %s, %s, %s, %s);
                    """,
                    (
                        pd.to_datetime(column.value),
                        str(t).replace(".", "-"),
                        symbol_data[column].to_json(),
                        now,
                        "yahoo",
                    ),
                )
                conn.commit()
