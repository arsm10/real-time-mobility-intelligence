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

Currently in initial foundation development.

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

## License

MIT