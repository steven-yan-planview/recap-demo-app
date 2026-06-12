# DataPlatform Ingestion Service

Core data ingestion and access service for the fictional Data Platform team.

## Overview

Handles inbound data from producer systems, validates and normalizes records, and writes to Snowflake. Exposes a REST API consumed by downstream analytics tools.

## Components

- **API layer** — FastAPI routes, auth middleware, rate limiting
- **Ingestion pipeline** — Schema validation, transformation, dead-letter queue
- **Storage** — Snowflake connection pool, retry logic, RLS enforcement
- **Auth** — OAuth 2.0 PKCE flow, JWT validation middleware

## Setup

```bash
pip install -e ".[dev]"
cp .env.example .env
uvicorn src.main:app --reload
```

## Testing

```bash
pytest tests/ -v
```
