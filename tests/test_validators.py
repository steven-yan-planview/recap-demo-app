import pytest
from datetime import datetime, timezone
from src.ingestion.validators import EventRecord, MetricRecord, get_validator


def test_event_record_defaults_id():
    r = EventRecord(event_type="click", occurred_at="2026-06-01T00:00:00Z", payload={})
    assert r.event_id != ""


def test_event_record_coerces_naive_timestamp():
    r = EventRecord(event_type="click", occurred_at="2026-06-01T12:00:00", payload={})
    assert r.occurred_at.tzinfo == timezone.utc


def test_metric_record_valid():
    r = MetricRecord(
        metric_name="cpu_usage",
        value=0.72,
        dimensions={"host": "prod-1"},
        timestamp="2026-06-01T00:00:00Z",
    )
    assert r.value == 0.72


def test_get_validator_known():
    assert get_validator("events") is EventRecord


def test_get_validator_unknown():
    assert get_validator("nope") is None
