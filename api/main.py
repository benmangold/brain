from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

from src.db import PostgresResource
from datetime import datetime

app = FastAPI()
postgres = PostgresResource()
# postgres.setup_for_execution(None)


class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}


@app.get("/test")
def test():
    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS brain_test (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert test record
            test_message = f"Dagster test at {datetime.now()}"
            cur.execute(
                "INSERT INTO brain_test (message) VALUES (%s) RETURNING id",
                (test_message,),
            )
            record_id = cur.fetchone()[0]

            # Query back
            cur.execute("SELECT COUNT(*) FROM brain_test")
            count = cur.fetchone()[0]
            return {
                "record_id": record_id,
                "total_records": count,
                "message": test_message,
            }
