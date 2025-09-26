# UK Police Stop & Search ETL

A containerised Python application for fetching UK Police stop & search data and storing it locally for analysis.

## Quick Start

### Using Docker Compose (Recommended)

1. **Run a demo:**
   ```bash
   docker-compose --profile demo up etl-demo
   ```

2. **Run backfill for specific forces:**
   ```bash
   docker-compose run --rm etl backfill --force metropolitan
   ```

3. **Start scheduled service:**
   ```bash
   docker-compose run --rm etl schedule
   ```

### Using Docker

1. **Build the image:**
   ```bash
   docker build -t stopsearch-etl .
   ```

2. **Run commands:**
   ```bash
   # Show help
   docker run --rm stopsearch-etl

   # Run backfill
   docker run --rm -v stopsearch_data:/app/data stopsearch-etl backfill --force metropolitan

   # Run once
   docker run --rm -v stopsearch_data:/app/data stopsearch-etl run-once
   ```

### Local Development

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

2. **Run commands:**
   ```bash
   python -m stopsearch_etl --help
   python -m stopsearch_etl backfill --force metropolitan
   python -m stopsearch_etl run-once
   ```

## Configuration

Set environment variables:

- `FORCES`: Comma-separated list of police forces (default: `metropolitan`)
- `DATABASE_URL`: Database connection string (default: `sqlite:///stopsearch.db`)
- `LOG_LEVEL`: Logging level (default: `INFO`)

## Architecture

- **Domain Model**: `StopSearchRecord` with validation and API parsing
- **Repository Pattern**: Abstract interface with SQLite implementation
- **ETL Pipeline**: Extract → Transform → Load with error handling
- **Scheduling**: APScheduler for daily runs
- **Metrics**: Structured logging and operation tracking
- **CLI**: Argparse-based interface for operations

## Data Source

Uses the UK Police Data API:
- **Endpoint**: `https://data.police.uk/api/stops-force`
- **Fields**: Search type, demographics, location, outcome, legislation
- **Coverage**: Historical data varies by force

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Local Development with Override

1. Copy the override example:
   ```bash
   cp docker-compose.override.yml.example docker-compose.override.yml
   ```

2. Customize settings in the override file

3. Run with your settings:
   ```bash
   docker-compose up etl
   ```

## Deployment

The application is designed for production deployment with:

- **Security**: Non-root container user, minimal image
- **Monitoring**: Health checks, structured logging
- **Resilience**: Retry policies, idempotent operations
- **Scalability**: Configurable concurrency limits

## License

This project is for educational and research purposes.