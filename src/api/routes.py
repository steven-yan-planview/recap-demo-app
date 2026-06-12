import logging
from fastapi import APIRouter, Request, HTTPException, Depends

from src.ingestion.pipeline import IngestPipeline
from src.ingestion.validators import get_validator
from src.api.rate_limiter import enforce_rate_limit

logger = logging.getLogger(__name__)
router = APIRouter()
pipeline = IngestPipeline()


@router.post("/{schema_name}", status_code=202, dependencies=[Depends(enforce_rate_limit)])
async def ingest(schema_name: str, request: Request):
    validator = get_validator(schema_name)
    if validator is None:
        raise HTTPException(status_code=404, detail=f"Unknown schema: {schema_name}")

    body = await request.json()
    record = validator.validate(body)
    record["_tenant_id"] = request.state.tenant

    result = await pipeline.process(schema_name, record)
    return {"status": "accepted", "record_id": result.record_id}


@router.get("/health")
async def health():
    return {"status": "ok"}
