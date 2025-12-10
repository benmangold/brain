import os
from contextlib import contextmanager
from typing import Any

from psycopg2 import pool


class PostgresResource:
    """PostgreSQL connection resource."""

    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    user: str = os.getenv("POSTGRES_USER", "brain")
    password: str = os.getenv("POSTGRES_PASSWORD", "brain_dev_password")
    database: str = os.getenv("POSTGRES_DB", "brain")
    _pool: Any = None

    def __init__(self) -> None:
        self._pool = pool.SimpleConnectionPool(
            1,
            10,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def teardown_after_execution(self, context) -> None:
        self._pool.closeall()

    @contextmanager
    def get_connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._pool.putconn(conn)
