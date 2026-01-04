FROM python:3.9-slim

# Install Java (required for PySpark) and netcat (for wait scripts if needed)
RUN apt-get update && \
    apt-get install -y default-jre-headless procps netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code directories
# We copy them to specific locations to match the imports in tests
COPY scripts /app/scripts
COPY Docker/prediction-service/SparkML /app/SparkML
COPY tests /app/tests
COPY pytest.ini /app/pytest.ini

# Set PYTHONPATH so tests can import modules from scripts and SparkML
ENV PYTHONPATH=/app:/app/scripts:/app/SparkML

# Default command runs all tests
# Use -m "not integration" by default to avoid failing if services aren't ready
# Override this in Helm to run integration tests
CMD ["pytest", "-v"]
