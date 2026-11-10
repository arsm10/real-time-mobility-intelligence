# Vehicle Event Contract

## 1. Purpose

This document defines the standard event structure used by the
Real-Time Mobility Intelligence Platform.

One event represents one vehicle position observation and its
operational state at a specific point in time.

## 2. Event Type

`vehicle.position.updated`

## 3. Schema Version

Initial version: `1.0`

## 4. Event Fields

| Field | Type | Required | Description |
|---|---|---|---|
| event_id | UUID | Yes | Globally unique event identifier |
| schema_version | String | Yes | Event contract version |
| event_type | String | Yes | Event type identifier |
| vehicle_id | String | Yes | Vehicle identifier |
| route_id | String | Yes | Route identifier |
| trip_id | String | No | Journey identifier |
| event_time | UTC timestamp | Yes | Observation time |
| produced_at | UTC timestamp | Yes | Event generation time |
| latitude | Float | Yes | GPS latitude |
| longitude | Float | Yes | GPS longitude |
| speed_kmh | Float | Yes | Speed in kilometres per hour |
| heading_deg | Float | Yes | Direction in degrees |
| status | String | Yes | Vehicle operational status |
| source | String | Yes | Event source |

## 5. Validation Rules

- event_id must be a valid UUID.
- schema_version must match a supported contract version, initially 1.0.
- event_type must match the supported event type: vehicle.position.updated.
- vehicle_id, route_id and source must not be empty or whitespace-only.
- trip_id is optional, but must not be empty or whitespace-only when provided.
- event_time and produced_at must be timezone-aware timestamps and normalized to UTC.
- latitude must be between -90 and 90.
- longitude must be between -180 and 180.
- speed_kmh must be a finite number greater than or equal to 0.
- heading_deg must be a finite number greater than or equal to 0 and less than 360.
- status must be one of:
  - in_service
  - out_of_service
  - stopped

## 6. Event Processing Rules

- event_id is used for deduplication.
- event_time determines the chronological order of observations.
- produced_at helps measure event generation delay.
- ingestion time is added by the processing system.
- Older events must not overwrite newer vehicle state.
- Kafka messages use vehicle_id as their partition key.

## 7. Initial Event Source

The initial event source is a locally running synthetic vehicle simulator.

Future adapters may support free public transport data feeds.

## 8. Compatibility and Versioning

The initial contract version is 1.0.

Changes to the contract must be reviewed for compatibility
with existing producers, consumers and storage components.
