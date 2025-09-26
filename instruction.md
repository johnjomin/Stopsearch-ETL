# How to Run Your Project with Docker (Beginner's Guide)

This guide will walk you through running your Stopsearch-ETL project using Docker and executing tests. No prior Docker experience required!

## What is Docker?

Docker is like a virtual container that packages your application with everything it needs to run. Think of it as a shipping container for software - it works the same way on any computer.

## Prerequisites

1. **Install Docker Desktop**
   - Windows: Download from [docker.com](https://www.docker.com/products/docker-desktop)
   - Make sure Docker Desktop is running (you'll see a whale icon in your system tray)

2. **Open Terminal/Command Prompt**
   - Windows: Press `Win + R`, type `cmd`, press Enter
   - Navigate to your project folder:
     ```bash
     cd "C:\Users\Jomin\Desktop\Programming Projects\Stopsearch-ETL"
     ```

## Running Your Project with Docker

### Method 1: Using Docker Compose (Easiest!)

Docker Compose lets you run your application with one simple command.

#### Quick Demo Run
```bash
docker-compose --profile demo up etl-demo
```
**What this does:** Runs your ETL once to fetch some sample data and then stops.

#### Run for Specific Police Forces
```bash
docker-compose run --rm etl backfill --force metropolitan
```
**What this does:** Fetches historical data for the Metropolitan Police force.

#### Start the Scheduled Service
```bash
docker-compose run --rm etl schedule
```
**What this does:** Starts the application to run automatically every day.

### Method 2: Using Docker Commands Directly

If you want more control, you can use Docker commands directly:

#### 1. Build Your Application
```bash
docker build -t stopsearch-etl .
```
**What this does:** Creates a Docker image (like a blueprint) of your application.

#### 2. Run Commands
```bash
# Show help menu
docker run --rm stopsearch-etl

# Get data for Metropolitan Police
docker run --rm -v stopsearch_data:/app/data stopsearch-etl backfill --force metropolitan

# Run once and exit
docker run --rm -v stopsearch_data:/app/data stopsearch-etl run-once
```

## Understanding the Commands

- `docker-compose`: Tool that manages multiple containers easily
- `docker run`: Runs a single container
- `--rm`: Automatically removes the container when it stops (keeps things clean)
- `-v stopsearch_data:/app/data`: Creates a storage volume so your data persists
- `backfill --force metropolitan`: Your app's command to fetch Metropolitan Police data

## Running Tests

### Option 1: Run Tests Inside Docker Container

1. **Build the test environment:**
   ```bash
   docker build -t stopsearch-etl .
   ```

2. **Run all tests:**
   ```bash
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/ -v"
   ```

3. **Run specific test categories:**
   ```bash
   # Test the core domain logic
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/test_stop_search_record.py -v"

   # Test the ETL pipeline
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/test_etl_service.py -v"

   # Test data integrity
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/test_idempotency.py -v"

   # Test error handling
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/test_error_edge_cases.py -v"
   ```

4. **Run tests with coverage report:**
   ```bash
   docker run --rm stopsearch-etl sh -c "python -m pytest tests/ --cov=src --cov-report=term-missing"
   ```

### Option 2: Run Tests with Docker Compose

```bash
# Run all tests
docker-compose run --rm etl sh -c "python -m pytest tests/ -v"

# Run tests with coverage
docker-compose run --rm etl sh -c "python -m pytest tests/ --cov=src --cov-report=html"
```

### Option 3: Run Tests Locally (Alternative)

If you prefer to run tests on your local machine:

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   pip install -e .
   ```

2. **Run tests:**
   ```bash
   # All tests
   pytest tests/ -v

   # Specific tests
   pytest tests/test_stop_search_record.py -v

   # With coverage
   pytest tests/ --cov=src --cov-report=html
   ```

## Understanding Test Output

- ✅ **PASSED**: Test worked correctly
- ❌ **FAILED**: Something is broken (check the error message)
- **Coverage**: Shows what percentage of your code is tested

## What Each Test Does

Your project has comprehensive tests that check:

- **Domain Logic** (`test_stop_search_record.py`): Tests data validation and parsing
- **ETL Pipeline** (`test_etl_service.py`): Tests data extraction, transformation, and loading
- **Database** (`test_sqlite_repository.py`): Tests data storage and retrieval
- **Error Handling** (`test_error_edge_cases.py`): Tests how the app handles problems
- **Integration** (`test_docker_compose.py`): Tests that Docker setup works

## Troubleshooting

### Common Issues:

1. **"Docker command not found"**
   - Make sure Docker Desktop is installed and running

2. **"Permission denied"**
   - On Windows, run Command Prompt as Administrator

3. **"Port already in use"**
   - Stop any running containers: `docker-compose down`

4. **"No space left on device"**
   - Clean up old containers: `docker system prune`

### Checking What's Running:
```bash
# See running containers
docker ps

# See all containers (running and stopped)
docker ps -a

# Stop all containers
docker-compose down
```

## Configuration Options

You can customize the application by setting these environment variables:

- `FORCES`: Which police forces to fetch data for (default: `metropolitan`)
- `DATABASE_URL`: Where to store the data (default: `sqlite:///stopsearch.db`)
- `LOG_LEVEL`: How much logging to show (default: `INFO`)

Example:
```bash
docker-compose run --rm -e FORCES=metropolitan,avon-and-somerset etl backfill
```

## Where Your Data is Stored

- **In Docker**: Data is stored in a Docker volume named `etl_data`
- **File location inside container**: `/app/data/stopsearch.db`
- **To access your data**: The SQLite database file persists between runs

## Next Steps

1. Start with the demo: `docker-compose --profile demo up etl-demo`
2. Run the tests to make sure everything works: `docker run --rm stopsearch-etl sh -c "python -m pytest tests/ -v"`
3. Try fetching data for your local police force
4. Set up the scheduler for daily runs

## Getting Help

- Run `docker run --rm stopsearch-etl --help` to see all available commands
- Check the main README.md for more detailed technical information
- Look at the test files in the `tests/` folder to understand what the application does