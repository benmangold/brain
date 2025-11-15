"""Test assets for database connectivity."""

from datetime import datetime

from dagster import asset, AssetExecutionContext

from orchestration.db.resources import (
    PostgresResource,
    PgVectorResource,
    MongoResource,
    RedisResource,
    DynamoDBResource,
)

DB_CONNECTIVITY_TEST_GROUP_NAME = "database_connectivity"


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME)
def test_postgres_connection(
        context: AssetExecutionContext,
        postgres: PostgresResource
) -> dict:
    """Test PostgreSQL connection and create a test table."""

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            # Create test table if not exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS brain_test (
                    id SERIAL PRIMARY KEY,
                    message TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Insert test record
            test_message = f"Dagster test at {datetime.now()}"
            cur.execute(
                "INSERT INTO brain_test (message) VALUES (%s) RETURNING id",
                (test_message,)
            )
            record_id = cur.fetchone()[0]

            # Query back
            cur.execute("SELECT COUNT(*) FROM brain_test")
            count = cur.fetchone()[0]

            context.log.info(f"PostgreSQL: Inserted record {record_id}, total records: {count}")

            return {
                "database": "postgres",
                "status": "connected",
                "record_id": record_id,
                "total_records": count,
            }


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME)
def test_pgvector_connection(
        context: AssetExecutionContext,
        pgvector: PgVectorResource
) -> dict:
    """Test pgvector connection and create a test table with vector column."""

    with pgvector.get_connection() as conn:
        with conn.cursor() as cur:
            # Enable pgvector extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Create test table with vector column
            cur.execute("""
                CREATE TABLE IF NOT EXISTS brain_embeddings (
                    id SERIAL PRIMARY KEY,
                    content TEXT,
                    embedding vector(3),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Insert test record with a simple 3D vector
            test_vector = [0.1, 0.2, 0.3]
            cur.execute(
                "INSERT INTO brain_embeddings (content, embedding) VALUES (%s, %s) RETURNING id",
                (f"Test embedding at {datetime.now()}", test_vector)
            )
            record_id = cur.fetchone()[0]

            # Query back
            cur.execute("SELECT COUNT(*) FROM brain_embeddings")
            count = cur.fetchone()[0]

            context.log.info(f"pgvector: Inserted record {record_id}, total records: {count}")

            return {
                "database": "pgvector",
                "status": "connected",
                "record_id": record_id,
                "total_records": count,
            }


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME)
def test_mongo_connection(
        context: AssetExecutionContext,
        mongo: MongoResource
) -> dict:
    """Test MongoDB connection and insert a test document."""

    collection = mongo.db.brain_test

    # Insert test document
    test_doc = {
        "message": f"Dagster test at {datetime.now()}",
        "timestamp": datetime.now(),
        "source": "dagster_asset"
    }
    result = collection.insert_one(test_doc)

    # Count documents
    count = collection.count_documents({})

    context.log.info(f"MongoDB: Inserted document {result.inserted_id}, total documents: {count}")

    return {
        "database": "mongodb",
        "status": "connected",
        "document_id": str(result.inserted_id),
        "total_documents": count,
    }


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME)
def test_redis_connection(
        context: AssetExecutionContext,
        redis: RedisResource
) -> dict:
    """Test Redis connection and set/get a key."""

    # Set a test key
    test_key = "brain:test:dagster"
    test_value = f"Test at {datetime.now()}"
    redis.client.set(test_key, test_value, ex=3600)  # Expire in 1 hour

    # Get it back
    retrieved = redis.client.get(test_key)

    # Increment a counter
    counter_key = "brain:test:counter"
    counter = redis.client.incr(counter_key)

    # Get info
    info = redis.client.info("server")

    context.log.info(f"Redis: Set key '{test_key}', counter at {counter}")

    return {
        "database": "redis",
        "status": "connected",
        "test_key": test_key,
        "test_value": retrieved,
        "counter": counter,
        "redis_version": info.get("redis_version"),
    }


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME)
def test_dynamodb_connection(
        context: AssetExecutionContext,
        dynamodb: DynamoDBResource
) -> dict:
    """Test DynamoDB Local connection and create a test table."""

    table_name = "brain_test"

    # List existing tables
    existing_tables = dynamodb.client.list_tables()["TableNames"]

    # Create table if it doesn't exist
    if table_name not in existing_tables:
        table = dynamodb.resource.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        table.wait_until_exists()
        context.log.info(f"Created table: {table_name}")
    else:
        table = dynamodb.resource.Table(table_name)

    # Put an item
    test_id = f"test-{datetime.now().timestamp()}"
    table.put_item(
        Item={
            "id": test_id,
            "message": f"Dagster test at {datetime.now()}",
            "timestamp": str(datetime.now()),
        }
    )

    # Scan to count items
    response = table.scan(Select="COUNT")
    count = response["Count"]

    context.log.info(f"DynamoDB: Inserted item {test_id}, total items: {count}")

    return {
        "database": "dynamodb",
        "status": "connected",
        "item_id": test_id,
        "total_items": count,
        "tables": existing_tables + [table_name] if table_name not in existing_tables else existing_tables,
    }


@asset(group_name=DB_CONNECTIVITY_TEST_GROUP_NAME,
       deps=[
           test_postgres_connection,
           test_pgvector_connection,
           test_mongo_connection,
           test_redis_connection,
           test_dynamodb_connection,
       ]
       )
def all_databases_tested(context: AssetExecutionContext) -> dict:
    """Summary asset that depends on all database tests."""

    context.log.info("All database connectivity tests completed successfully!")

    return {
        "status": "all_databases_connected",
        "timestamp": datetime.now().isoformat(),
        "databases": ["postgres", "pgvector", "mongodb", "redis", "dynamodb"],
    }
