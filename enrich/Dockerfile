# Use Python 3.9 slim image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install system dependencies if needed (e.g., git for cloning repos)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY enrich/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY enrich/github_enrichment.py .
COPY github_api_client.py .
COPY token_pool.py .
# Copy config if it exists locally, though we prefer env vars or GCS for config
# COPY crawler_config.yml . 

# Set environment variables to ensure output is not buffered
ENV PYTHONUNBUFFERED=1

# Command to run the script
# Arguments can be overridden by Cloud Run Job args
CMD ["python", "github_enrichment.py"]
