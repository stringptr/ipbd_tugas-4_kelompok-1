"""Prefect Global Configuration"""

import os
from pathlib import Path

from dotenv import load_dotenv

possible_env_paths = [
    Path(__file__).parent / ".env",
    Path("/prefect/app/.env"),
    Path("/prefect/.env"),
    Path(".env"),
]

env_file = None
for p in possible_env_paths:
    if p.exists():
        env_file = p
        break

if env_file:
    _ = load_dotenv(env_file)


class Settings:
    """Application settings loaded from environment variables."""

    WAREHOUSE_HOST: str = os.getenv("WAREHOUSE_POSTGRES_HOST", "warehouse")
    WAREHOUSE_PORT: str = os.getenv("WAREHOUSE_POSTGRES_PORT", "5432")
    WAREHOUSE_USER: str = os.getenv("WAREHOUSE_POSTGRES_USER", "postgres")
    WAREHOUSE_PASSWORD: str = os.getenv("WAREHOUSE_POSTGRES_PASSWORD", "postgres")
    WAREHOUSE_NAME: str = os.getenv("WAREHOUSE_POSTGRES_DB", "postgres")

    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "5"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    DB_POOL_PRE_PING: bool = os.getenv("DB_POOL_PRE_PING", "True").lower() == "true"

    PREFECT_BLOCK_NAME: str = os.getenv("PREFECT_BLOCK_NAME", "postgres-demo-block")
    DEFAULT_TABLE_NAME: str = os.getenv("DEFAULT_TABLE_NAME", "posts")
    DEFAULT_LIMIT: int = int(os.getenv("DEFAULT_LIMIT", "20"))

    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))
    API_MAX_RETRIES: int = int(os.getenv("API_MAX_RETRIES", "3"))
    API_RETRY_DELAYS: list[int] = [2, 5, 10]

    @classmethod
    def get_db_connection_string(cls, async_driver: bool = False) -> str:
        """Build database connection string from settings."""
        driver = "postgresql+asyncpg" if async_driver else "postgresql"
        return (
            f"{driver}://{cls.WAREHOUSE_USER}:{cls.WAREHOUSE_PASSWORD}"
            f"@{cls.WAREHOUSE_HOST}:{cls.WAREHOUSE_PORT}/{cls.WAREHOUSE_NAME}"
        )

    @classmethod
    def validate_db_settings(cls) -> bool:
        """Validate all required database settings are present."""
        required = [
            "WAREHOUSE_USER",
            "WAREHOUSE_PASSWORD",
            "WAREHOUSE_HOST",
            "WAREHOUSE_PORT",
            "WAREHOUSE_NAME",
        ]
        missing = [var for var in required if not getattr(cls, var)]

        if missing:
            print(f"Missing environment variables: {missing}")
            return False
        return True

    @classmethod
    def to_dict(cls) -> dict:
        """Convert settings to dictionary for debugging."""
        return {
            "database": {
                "host": cls.WAREHOUSE_HOST,
                "port": cls.WAREHOUSE_PORT,
                "name": cls.WAREHOUSE_NAME,
                "user": cls.WAREHOUSE_USER,
                "pool_size": cls.DB_POOL_SIZE,
            },
            "prefect": {
                "block_name": cls.PREFECT_BLOCK_NAME,
                "default_table": cls.DEFAULT_TABLE_NAME,
            },
        }


settings = Settings()
