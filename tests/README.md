# Project Tests

This directory contains unit and integration tests for the BigData project components.

## Prerequisites

Ensure you have the test dependencies installed:

```bash
pip install -r ../requirements.txt
```

You will also need Java installed (for PySpark tests).

## Running Tests

To run all tests (including integration tests if services are reachable):

```bash
pytest
```

To run only unit tests (no external services required):

```bash
pytest -m "not integration"
```

To run only integration tests (requires running services):

```bash
pytest -m integration
```

## Test Coverage

### ML Pipeline
- **`test_train_consumption_model.py`**: Tests the Spark ML training pipeline (data splitting, model training, and evaluation) using a local Spark session.
- **`test_aggregate_and_predict.py`**: Tests the online aggregation service that converts raw weather streams into 10-minute buckets and generates predictions.

### Data Ingestion
- **`test_fetch_realtime_weather.py`**: Tests the real-time weather fetcher (DMI API integration).
- **`test_fetch_realtime_energy.py`**: Tests the real-time energy fetcher (Energinet API integration).
- **`test_historical_fetchers.py`**: Tests the batch processing logic for historical weather and energy data ingestion.

### Infrastructure (Integration)
- **`test_infrastructure.py`**: Verifies connectivity to core services:
  - **Kafka**: Checks broker availability and topic existence.
  - **Schema Registry**: Checks service availability and registered subjects.
  - **TimescaleDB**: Checks database connection and `measurements` hypertable.
  - **HDFS**: Checks file system accessibility (requires `hdfs` CLI).
  - **Hive**: Checks metastore connectivity via Spark.

**Note**: Integration tests assume services are running on `localhost` ports (via port-forwarding) or that the tests are running inside the cluster. Configure environment variables (e.g., `KAFKA_BOOTSTRAP_SERVERS`, `DB_HOST`) to point to the correct locations if they differ from defaults.
