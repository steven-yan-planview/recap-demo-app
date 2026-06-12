from __future__ import annotations
import uuid
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, field_validator


class EventRecord(BaseModel):
    event_id: str = ""
    event_type: str
    occurred_at: datetime
    payload: dict[str, Any]

    @field_validator("event_id", mode="before")
    @classmethod
    def default_id(cls, v: str) -> str:
        return v or str(uuid.uuid4())

    @field_validator("occurred_at", mode="before")
    @classmethod
    def coerce_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            dt = datetime.fromisoformat(v)
        elif isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(v, tz=timezone.utc)
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


class MetricRecord(BaseModel):
    metric_name: str
    value: float
    dimensions: dict[str, str] = {}
    timestamp: datetime

    @field_validator("timestamp", mode="before")
    @classmethod
    def coerce_utc(cls, v: Any) -> datetime:
        if isinstance(v, str):
            dt = datetime.fromisoformat(v)
        else:
            dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt


_REGISTRY: dict[str, type[BaseModel]] = {
    "events": EventRecord,
    "metrics": MetricRecord,
}


def get_validator(schema_name: str) -> type[BaseModel] | None:
    return _REGISTRY.get(schema_name)
