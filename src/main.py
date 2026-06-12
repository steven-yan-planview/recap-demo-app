import logging
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager

from src.storage.connection_pool import pool
from src.auth.middleware import AuthMiddleware
from src.api.routes import router
from src.config import settings


logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up — initializing connection pool")
    await pool.initialize()
    yield
    logger.info("Shutting down — draining connection pool")
    await pool.close()


app = FastAPI(
    title="DataPlatform Ingestion Service",
    version="0.4.0",
    lifespan=lifespan,
)

app.add_middleware(AuthMiddleware)
app.include_router(router, prefix="/ingest")


def run():
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=False)
