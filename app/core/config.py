"""Application settings, loaded from environment variables / .env via pydantic-settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    PROJECT_NAME: str = "FastAPI Repository Pattern"
    API_V1_PREFIX: str = "/api/v1"
    DEBUG: bool = False

    # MySQL (async driver). Example: mysql+aiomysql://user:pass@localhost:3306/app_db
    DATABASE_URL: str = "mysql+aiomysql://app_user:app_password@localhost:3306/app_db"
    DB_ECHO: bool = False
    DB_POOL_SIZE: int = Field(default=10, ge=1)
    DB_MAX_OVERFLOW: int = Field(default=20, ge=0)
    # MySQL closes idle connections after `wait_timeout` (8h default). Recycle well before that.
    DB_POOL_RECYCLE: int = 1800

    # Dev convenience only. In production, manage the schema with Alembic migrations.
    AUTO_CREATE_TABLES: bool = False

    # Hard ceiling for page size on every list endpoint.
    MAX_PAGE_SIZE: int = Field(default=100, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
