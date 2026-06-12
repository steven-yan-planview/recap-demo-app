from __future__ import annotations
import json
import logging

from src.storage.connection_pool import pool

logger = logging.getLogger(__name__)

_TABLE_MAP = {
    "events": "RAW_EVENTS",
    "metrics": "RAW_METRICS",
}

_INSERT_SQL = """
INSERT INTO {table} (record_json, _tenant_id, _ingested_at)
SELECT PARSE_JSON(%s), %s, CURRENT_TIMESTAMP()
"""


class SnowflakeClient:
    async def write(self, schema_name: str, record: dict) -> None:
        table = _TABLE_MAP.get(schema_name)
        if table is None:
            raise ValueError(f"No table mapping for schema '{schema_name}'")

        tenant_id = record.get("_tenant_id", "")
        sql = _INSERT_SQL.format(table=table)

        async with pool.acquire() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (json.dumps(record, default=str), tenant_id))
            logger.debug("Wrote record to %s for tenant %s", table, tenant_id)
