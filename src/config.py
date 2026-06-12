from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str = "DATA_PLATFORM"
    snowflake_schema: str = "INGESTION"
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_pool_min: int = 2
    snowflake_pool_max: int = 10

    jwks_uri: str
    jwt_audience: str = "data-platform"

    dlq_queue_url: str = ""

    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()
