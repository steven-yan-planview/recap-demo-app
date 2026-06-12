from __future__ import annotations
import logging
import uuid
from dataclasses import dataclass

from src.storage.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


@dataclass
class IngestResult:
    record_id: str


class IngestPipeline:
    def __init__(self):
        self._client = SnowflakeClient()

    async def process(self, schema_name: str, record: dict) -> IngestResult:
        record_id = record.get("event_id") or str(uuid.uuid4())
        logger.info("Processing record %s for schema %s", record_id, schema_name)
        await self._client.write(schema_name, record)
        return IngestResult(record_id=record_id)
