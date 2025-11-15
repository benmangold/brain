# Brain Persistence Layer

Local database infrastructure for the brain project, managed via Docker Compose.

## Services

- **PostgreSQL** (port 5432) - Primary relational database
- **pgvector** (port 5433) - Dedicated PostgreSQL with vector extension for embeddings
- **MongoDB** (port 27017) - Document store
- **DynamoDB Local** (port 8000) - Local DynamoDB for testing
- **Redis** (port 6379) - In-memory cache and message broker

## Quick Start

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs -f

# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```

## Connection Strings

### PostgreSQL
```
postgresql://brain:brain_dev_password@localhost:5432/brain
```

### pgvector
```
postgresql://brain:brain_dev_password@localhost:5433/brain_vectors
```

### MongoDB
```
mongodb://brain:brain_dev_password@localhost:27017/brain?authSource=admin
```

### DynamoDB Local
```
endpoint: http://localhost:8000
region: us-east-1 (can be any region for local)
credentials: dummy (any values work for local)
```

### Redis
```
redis://localhost:6379
```

## Data Persistence

All data is stored in the `data/` directory:
- `data/postgres/` - PostgreSQL data
- `data/pgvector/` - pgvector data
- `data/mongo/` - MongoDB data
- `data/mongo-config/` - MongoDB config
- `data/dynamodb/` - DynamoDB Local data
- `data/redis/` - Redis persistence (AOF)

**Note:** The `data/` directory is gitignored by default. Back up this directory if you need to preserve data.

## Managing Individual Services

```bash
# Start specific service
docker-compose up -d postgres

# Restart a service
docker-compose restart mongo

# View logs for specific service
docker-compose logs -f pgvector

# Stop specific service
docker-compose stop dynamodb
```

## Accessing Databases

### PostgreSQL/pgvector CLI
```bash
docker exec -it brain-postgres psql -U brain -d brain
docker exec -it brain-pgvector psql -U brain -d brain_vectors
```

### MongoDB CLI
```bash
docker exec -it brain-mongo mongosh -u brain -p brain_dev_password --authenticationDatabase admin
```

### DynamoDB Local CLI (requires AWS CLI)
```bash
aws dynamodb list-tables --endpoint-url http://localhost:8000 --region us-east-1
```

### Redis CLI
```bash
docker exec -it brain-redis redis-cli
# or from your host if redis-cli is installed
redis-cli
```

## Security Note

These credentials are for **local development only**. Never use these credentials in production or commit actual production credentials to version control.