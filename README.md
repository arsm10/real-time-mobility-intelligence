# Real-Time Mobility Intelligence Platform

A Python-based real-time public transport data platform.

## Objectives

- Ingest vehicle position events.
- Process streaming transport data.
- Track current vehicle positions.
- Analyse historical delays and route performance.
- Detect service disruptions.
- Expose results through REST APIs and a dashboard.

## Technology Stack

- Python 3.12
- Apache Kafka
- PostgreSQL + PostGIS
- ClickHouse
- FastAPI
- Streamlit
- Docker Compose
- Pytest
- Ruff
- GitHub Actions

## Project Status

Completed milestones:

- M0: Project foundation and tooling.
- M1: Vehicle event contract and simulator.
- M2: Kafka producer.
- M3: Kafka consumer.
- M4: Event deduplication, out-of-order detection and dead-letter handling.
- M5: PostgreSQL + PostGIS operational storage.
- M6: ClickHouse historical analytics.
- M7: FastAPI analytics API and Docker deployment.

The API currently exposes historical analytics from ClickHouse.
Kafka-to-ClickHouse automated ingestion is not yet implemented.

## Local Development

### Prerequisites

- Python 3.12
- Docker Desktop
- Git

### Setup

Create a virtual environment:

    py -3.12 -m venv .venv

Activate it:

    .\.venv\Scripts\Activate.ps1

Install the project:

    python -m pip install --upgrade pip
    pip install -e ".[dev]"

### Start Kafka

    docker compose up -d

Check the container:

    docker compose ps

View logs:

    docker compose logs -f kafka

### Run tests

    pytest

### Run code quality checks

    ruff check .
    ruff format --check .

## Analytics API

The FastAPI service exposes the following endpoints:

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | API health check |
| GET | `/ready` | ClickHouse readiness check |
| GET | `/api/v1/analytics/routes/average-speed` | Average speed by route |
| GET | `/api/v1/analytics/events/hourly` | Hourly vehicle event counts |
| GET | `/api/v1/analytics/vehicles/{vehicle_id}/events` | Paginated vehicle events |

### Start the API

Configure the required environment variables in your `.env` file, including `CLICKHOUSE_PASSWORD`.

Build and start the API with its ClickHouse dependency:

    docker compose up -d --build api

### API Documentation

Open Swagger UI:

    http://localhost:8000/docs

OpenAPI specification:

    http://localhost:8000/openapi.json

## License

MIT
