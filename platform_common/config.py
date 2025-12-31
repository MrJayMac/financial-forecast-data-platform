from __future__ import annotations

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="allow")

    # Postgres
    POSTGRES_DSN: str = Field(
        default="postgresql+psycopg2://ffdp:ffdp@postgres:5432/ffdp",
        description="SQLAlchemy DSN for Postgres",
    )

    # S3 / MinIO
    S3_ENDPOINT: str = Field(default="http://minio:9000")
    S3_REGION: str = Field(default="us-east-1")
    S3_ACCESS_KEY: str = Field(default="minioadmin")
    S3_SECRET_KEY: str = Field(default="minioadmin")
    S3_BUCKET: str = Field(default="datalake")
    S3_SECURE: bool = Field(default=False)
    AWS_S3_FORCE_PATH_STYLE: bool = Field(default=True)

    # App
    UVICORN_HOST: str = Field(default="0.0.0.0")
    UVICORN_PORT: int = Field(default=8000)
    LOG_LEVEL: str = Field(default="INFO")

    # Data quality
    LATE_ARRIVAL_DAYS: int = Field(default=3)


class QualityResult(BaseModel):
    is_valid: bool
    issues: list[str] = Field(default_factory=list)
    is_late: bool = False

settings: AppSettings = AppSettings()
