# System Architecture

## 1. Project Overview

Real-Time Mobility Intelligence is a real-time public transport
data ingestion, processing and analytics platform.

## 2. Architecture

The planned system contains:

1. Data sources
   - Synthetic vehicle simulator
   - GTFS static data
   - GTFS-Realtime feeds (optional)

2. Ingestion
   - Python ingestion adapters
   - Event normalization
   - Schema validation

3. Streaming
   - Apache Kafka
   - Producer and consumer applications
   - Event processing

4. Storage
   - PostgreSQL with PostGIS
   - ClickHouse

5. Application
   - FastAPI
   - Streamlit dashboard

## 3. Initial Development Environment

Apache Kafka runs locally using Docker Compose.

Kafka operates in KRaft mode with one broker and controller.

This configuration is for development and testing,
not a production high-availability deployment.

## 4. Development Principles

- Python is the primary application language.
- Events use UTC timestamps.
- Every event has a unique event_id.
- vehicle_id is the Kafka message key.
- Event processing must account for duplicates and late arrivals.
- Storage operations should be idempotent.
- All important functionality must have automated tests.

## 5. Cost Constraint

The project must be runnable locally using free software
and synthetic transport data.

No paid API or cloud service is required.