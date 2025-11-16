import pandas as pd
from datetime import datetime, timezone
import json
import requests
from dagster import asset, AssetExecutionContext
from orchestration.db.resources import PostgresResource

from orchestration.db.resources import PostgresResource


class SP500Members:
    def ingest(self, cur, conn):
        try:
            url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }

            response = requests.get(url, headers=headers)
            table = None
            # Check if the request was successful (status code 200)
            # Pass the HTML content from the response to pd.read_html
            # pd.read_html returns a list of DataFrames, one for each table found
            table = pd.read_html(response.text)
            print("TABLE")
            # You can then access the desired table from the list, e.g., the first one:

            stock_list = table[1]
            stock_list_data = stock_list["Symbol"].to_list()

            for stock_symbol in stock_list_data:
                data = {"stockSymbol": stock_symbol}

                cur.execute(
                    "SET search_path TO ingest; INSERT INTO sp500_members (symbol, json,  created_at, source) VALUES (%s, %s, %s, %s);",
                    (
                        str(stock_symbol),
                        json.dumps(data),
                        datetime.now(timezone.utc),
                        "wikipedia",
                    ),
                )
                conn.commit()

            cur.execute("SELECT * FROM ingest.sp500_members")
            records = cur.fetchall()
            print(f"Ingested {len(records)} SP500 members")

        except Exception as e:
            print(e)


if __name__ == "__main__":
    marketeer_pg = PostgresResource(database="marketeer")

    sp500_members = SP500Members()
    stock_list = sp500_members.ingest(None, None)
    print(stock_list)
