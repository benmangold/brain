"""Database resources for Dagster."""

import os
from contextlib import contextmanager
from typing import Any

import boto3
import pymongo
import redis
from dagster import ConfigurableResource
from dotenv import load_dotenv
from psycopg2 import pool
from pymongo import MongoClient

# Load environment variables
load_dotenv()


class PostgresResource(ConfigurableResource):
    """PostgreSQL connection resource."""

    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    user: str = os.getenv("POSTGRES_USER", "brain")
    password: str = os.getenv("POSTGRES_PASSWORD", "brain_dev_password")
    database: str = os.getenv("POSTGRES_DB", "brain")
    _pool: Any = None

    def setup_for_execution(self, context) -> None:
        self._pool = pool.SimpleConnectionPool(
            1, 10,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
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


class PgVectorResource(ConfigurableResource):
    """pgvector connection resource."""

    host: str = os.getenv("PGVECTOR_HOST", "localhost")
    port: int = int(os.getenv("PGVECTOR_PORT", "5433"))
    user: str = os.getenv("PGVECTOR_USER", "brain")
    password: str = os.getenv("PGVECTOR_PASSWORD", "brain_dev_password")
    database: str = os.getenv("PGVECTOR_DB", "brain_vectors")
    _pool: Any = None

    def setup_for_execution(self, context) -> None:
        self._pool = pool.SimpleConnectionPool(
            1, 10,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
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


class MongoResource(ConfigurableResource):
    """MongoDB connection resource."""

    host: str = os.getenv("MONGO_HOST", "localhost")
    port: int = int(os.getenv("MONGO_PORT", "27017"))
    user: str = os.getenv("MONGO_USER", "brain")
    password: str = os.getenv("MONGO_PASSWORD", "brain_dev_password")
    database: str = os.getenv("MONGO_DB", "brain")
    auth_source: str = os.getenv("MONGO_AUTH_SOURCE", "admin")
    _client: Any = None
    _db: Any = None

    def setup_for_execution(self, context) -> None:
        connection_string = (
            f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"{self.database}?authSource={self.auth_source}"
        )
        self._client = MongoClient(connection_string)
        self._db = self._client[self.database]

    def teardown_after_execution(self, context) -> None:
        self._client.close()

    @property
    def client(self):
        return self._client

    @property
    def db(self):
        return self._db


class RedisResource(ConfigurableResource):
    """Redis connection resource."""

    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))

    def setup_for_execution(self, context) -> None:
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            decode_responses=True
        )

    def teardown_after_execution(self, context) -> None:
        self._client.close()

    @property
    def client(self):
        return self._client


class DynamoDBResource(ConfigurableResource):
    """DynamoDB Local connection resource."""

    endpoint_url: str = os.getenv("DYNAMODB_ENDPOINT", "http://localhost:8000")
    region_name: str = os.getenv("DYNAMODB_REGION", "us-east-1")
    _client: Any = None
    _resource: Any = None

    def setup_for_execution(self, context) -> None:
        self._client = boto3.client(
            'dynamodb',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy")
        )
        self._resource = boto3.resource(
            'dynamodb',
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy")
        )

    def teardown_after_execution(self, context) -> None:
        pass  # boto3 clients don't need explicit cleanup

    @property
    def client(self):
        return self._client

    @property
    def resource(self):
        return self._resource