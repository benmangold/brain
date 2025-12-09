# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

"brain" is a personal data processing and orchestration system organized into "folds" - dedicated layers of the stack. The project combines data persistence, orchestration, and analytics capabilities.

### Architecture

The system has two main components:

1. **persistence/** - Docker-managed multi-database layer providing:
   - PostgreSQL (port 5432) - primary relational database
   - pgvector (port 5433) - vector embeddings store
   - MongoDB (port 27017) - document database
   - DynamoDB Local (port 8000) - local DynamoDB instance
   - Redis (port 6379) - cache and message broker

2. **orchestration/** - Dagster-based data orchestration built with Python 3.10-3.13:
   - Runs scheduled jobs and data pipelines
   - Manages data ingestion and processing
   - Integrates with local Ollama LLMs
   - Contains dbt analytics project at `orchestration/analytics/`

## Development Commands

### Persistence Layer

```bash
# From the persistence/ directory
docker-compose up -d           # Start all databases
docker-compose ps              # Check service status
docker-compose logs -f         # View logs
docker-compose down            # Stop services
docker-compose down -v         # Stop and delete all data (WARNING)

# Individual services
docker-compose up -d postgres  # Start specific service
docker-compose restart mongo   # Restart service
docker-compose logs -f pgvector # View service logs
```

Database access:
```bash
# PostgreSQL CLI
docker exec -it brain-postgres psql -U brain -d brain

# pgvector CLI
docker exec -it brain-pgvector psql -U brain -d brain_vectors

# MongoDB CLI
docker exec -it brain-mongo mongosh -u brain -p brain_dev_password --authenticationDatabase admin

# Redis CLI
docker exec -it brain-redis redis-cli
```

### Orchestration Layer

```bash
# From the orchestration/ directory

# Setup (first time)
uv sync                        # Install dependencies with uv (preferred)
# OR
python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"

# Activate virtual environment
source .venv/bin/activate      # MacOS/Linux
# OR
.venv\Scripts\activate         # Windows

# Run Dagster
dg dev                         # Start Dagster UI (http://localhost:3000)

# Code formatting
black .                        # Format Python code
```

### dbt Analytics

```bash
# From the orchestration/analytics/ directory
dbt run                        # Run dbt models
dbt test                       # Run dbt tests
dbt compile                    # Compile dbt models
dbt clean                      # Clean target directory
```

The dbt profile "analytics" connects to PostgreSQL at localhost:5432/brain. Profile configuration is in `~/.dbt/profiles.yml`.

### Pre-commit Hooks

```bash
# From orchestration/ directory
pre-commit run --all-files     # Run all hooks manually
```

Hooks configured:
- `uv-lock` - Keep uv.lock in sync with pyproject.toml
- `black` - Python code formatting

## Code Structure

### Dagster Resources

The orchestration layer uses Dagster's ConfigurableResource pattern for all external integrations:

**Database Resources** (`orchestration/db/resources.py`):
- `PostgresResource` - Connection pooling for PostgreSQL (configurable database)
- `PgVectorResource` - Connection pooling for pgvector
- `MongoResource` - MongoDB client with auth
- `RedisResource` - Redis client
- `DynamoDBResource` - DynamoDB Local client

**LLM Resources** (`orchestration/llm/resources.py`):
- `OllamaResource` - Ollama LLM integration with model fallback support
- `DataProcessingResource` - Batch processing utilities for LLM workflows

All resources are configured in `orchestration/src/orchestration/definitions.py` and injected into assets/jobs via Dagster's dependency injection.

### Module Organization

```
orchestration/src/orchestration/
├── definitions.py              # Central Dagster definitions (assets, jobs, schedules, resources)
├── db/                         # Database connectivity
│   ├── resources.py            # Database resource definitions
│   └── connectivity_test/      # Database health check assets/jobs
├── llm/                        # LLM integration
│   ├── resources.py            # Ollama and data processing resources
│   └── connectivity_test/      # LLM health check assets/jobs
└── marketeer/                  # Market data ingestion pipeline
    ├── ingest/                 # Data ingestion logic
    │   ├── jobs.py             # Queue processing job
    │   ├── sp500_members.py    # SP500 member list scraping
    │   └── sp500_member_historical_data.py  # Historical price data
    └── persistence/            # Data persistence assets
```

### Key Patterns

1. **Resource Access**: Assets receive resources via dependency injection. Access database connections using context managers:
   ```python
   def my_asset(context, postgres: PostgresResource):
       with postgres.get_connection() as conn:
           cur = conn.cursor()
           # Use cursor
   ```

2. **Environment Configuration**: All connection details are loaded from `orchestration/.env` using python-dotenv. Resources default to local development values.

3. **Job Scheduling**: Schedules are defined in module-specific `jobs.py` files and registered in `definitions.py`:
   - `frequent_database_tests_schedule` - Regular database health checks
   - `frequent_llm_tests_schedule` - Regular LLM connectivity tests
   - `daily_queue_processing_schedule` - Daily market data ingestion

4. **Market Data Pipeline**: The marketeer module follows a pattern of:
   - Ingest data from external sources (Wikipedia, Yahoo Finance)
   - Store raw data in PostgreSQL `ingest.*` schema
   - Process with dbt in analytics layer

## Important Files

- `orchestration/pyproject.toml` - Python dependencies (managed by uv or pip)
- `orchestration/uv.lock` - Locked dependency versions
- `orchestration/.env` - Database and service connection strings (gitignored, see `persistence/example.env`)
- `orchestration/src/orchestration/definitions.py` - Dagster configuration entry point
- `persistence/docker-compose.yaml` - Database infrastructure definitions
- `persistence/data/` - Database volumes (gitignored, back up for data preservation)

## Connection Strings

All services use local development credentials (see `persistence/README.md` for details):
- PostgreSQL: `postgresql://brain:brain_dev_password@localhost:5432/brain`
- pgvector: `postgresql://brain:brain_dev_password@localhost:5433/brain_vectors`
- MongoDB: `mongodb://brain:brain_dev_password@localhost:27017/brain?authSource=admin`
- DynamoDB: endpoint `http://localhost:8000`, region `us-east-1`
- Redis: `redis://localhost:6379`

**Note**: These are development-only credentials. Never use in production.

## Testing

Run Dagster connectivity test jobs via the UI (http://localhost:3000) or materialize the test assets:
- Database tests: `database_tests_job`
- LLM tests: Ollama connectivity tests in `llm/connectivity_test/`

## Dependencies

Primary Python packages:
- `dagster==1.12.2` - Orchestration framework
- `dbt-core>=1.10.15`, `dbt-postgres>=1.9.1` - Analytics transformations
- `psycopg2-binary` - PostgreSQL adapter
- `pymongo` - MongoDB driver
- `redis` - Redis client
- `boto3` - AWS/DynamoDB client
- `ollama` - Ollama LLM client
- `yfinance` - Yahoo Finance data
- `pandas`, `requests` - Data processing
