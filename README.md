# UK Police Stop & Search ETL

Small Dockerised Python app that pulls UK Police stop & search data and saves it locally (SQLite by default). Good for quick analysis or feeding another tool.

## Quick Start

### Using Docker Compose (Recommended)

1. Demo run (runs once, then exits)
   ```bash
   docker-compose --profile demo up etl-demo
   ```

2. Backfill a force
   ```bash
   docker-compose run --rm etl backfill --force metropolitan
   ```

3. Run the scheduler
   ```bash
   docker-compose run --rm etl schedule
   ```

### Using Docker

1. Build
   ```bash
   docker build -t stopsearch-etl .
   ```
2. Help
   ```bash
   docker run --rm stopsearch-etl
   ```

3. Backfill with a volume for the DB
   ```bash
   docker run --rm -v stopsearch_data:/app/data stopsearch-etl backfill --force metropolitan
   ```

4. One-off fetch
   ```bash
   docker run --rm -v stopsearch_data:/app/data stopsearch-etl run-once
   ```

### Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

2. Run commands:
   ```bash
   python -m stopsearch_etl --help
   python -m stopsearch_etl backfill --force metropolitan
   python -m stopsearch_etl run-once
   ```

## Configuration

Set environment variables:

- FORCES — comma-separated list (default: metropolitan)
- DATABASE_URL — DB connection string (default: sqlite:///stopsearch.db)
- LOG_LEVEL — DEBUG|INFO|WARNING|ERROR|CRITICAL (default: INFO)

## Architecture

Built following clean architecture principles with dependency injection:

- Domain Model: StopSearchRecord dataclass with API parsing and validation
- Repository Pattern: Abstract interface PoliceApiClient with concrete HTTP implementation
- ETL Pipeline: Extract -> Transform -> Load with handling and retry
- Data Persistence: SQLite with constraints for idempotent operations
- Scheduling: APScheduler for daily automated runs
- Metrics: Structured logging with success/failure tracking
- Containerization: Docker with best practices and health monitoring

### Key Design Patterns

- Test-Driven Development (TDD)
- Arrange-Act-Assert (AAA)
- SOLID Principles: Clean interfaces, dependency injection, single responsibility
- Repository Patter: Clean abstraction over data access layer

## Data Source

Uses the UK Police Data API:
- Endpoint: `https://data.police.uk/api/stops-force`
- Fields: Search type, demographics, location, outcome, legislation
- Coverage: Historical data varies by force

## Architecture Decision:

- Database – SQLite
Simple, zero-config, great for one box. If you need scale or multi-writer, use Postgres.

- Scheduling – APScheduler
Runs daily jobs inside the app. If you want distributed jobs, look at Celery + Redis/RabbitMQ.

- Idempotency – DB constraints
SQLite UNIQUE on (datetime, lat, lon, type, legislation) to stop dupes. Works well as long as source data is clean.

- Retries – exponential backoff
HTTP calls back off on errors/rate limits. Keeps the API happy without hammering it.

- Scaling – threads
ThreadPoolExecutor to process months in parallel. Good for I/O work; switch to asyncio or multiprocessing if you need more.


### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test categories
pytest tests/test_domain.py -v                    # Domain model tests
pytest tests/test_etl_service.py -v              # ETL pipeline tests
pytest tests/test_idempotency.py -v              # Idempotency tests

# Run with coverage
pytest tests/ --cov=src --cov-report=html

# Run integration tests (requires Docker)
pytest tests/ -m "not slow" -v                   # Skip slow integration tests
pytest tests/ -m "slow" -v                       # Run only integration tests
```

### Code Quality

Pre-commit hooks ensure consistent code quality:

```bash
# Install pre-commit hooks
pre-commit install

# Run manually
pre-commit run --all-files

# Format code
black src/ tests/
ruff check src/ tests/ --fix
```

### Deployment

- Runs as a non-root user in a slim Python image
- Healthcheck + structured logs
- Retries and duplicate protection baked in
- Concurrency is configurable