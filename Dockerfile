# small base image
# TODO: pin a patch version for repeatable builds (e.g. python:3.13.1-slim)
FROM python:3.13-slim

# Install system dependencies and create non-root user
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && adduser --disabled-password --gecos '' --uid 1000 etluser
# TODO: if add system deps later, group them here to keep layer count low

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt
# TODO: consider hashing/locking deps

# Copy application code
COPY src/ ./src/
COPY setup.py .
COPY pyproject.toml .

# Install the application
RUN pip install -e .

# Create healthcheck script
RUN echo '#!/bin/bash\necho "Health check: ETL application is running"' > /app/healthcheck.sh \
    && chmod +x /app/healthcheck.sh

# Change ownership to non-root user
RUN chown -R etluser:etluser /app

# Switch to non-root user
USER etluser

# Add healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /app/healthcheck.sh

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Default command
ENTRYPOINT ["python", "-m", "stopsearch_etl"]
CMD ["--help"]