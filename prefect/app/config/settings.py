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

    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "database")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "5432")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "postgres")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "postgres")
    DATABASE_NAME: str = os.getenv("DATABASE_DB", "postgres")

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
            f"{driver}://{cls.DATABASE_USER}:{cls.DATABASE_PASSWORD}"
            f"@{cls.DATABASE_HOST}:{cls.DATABASE_PORT}/{cls.DATABASE_NAME}"
        )

    @classmethod
    def validate_db_settings(cls) -> bool:
        """Validate all required database settings are present."""
        required = [
            "DATABASE_USER",
            "DATABASE_PASSWORD",
            "DATABASE_HOST",
            "DATABASE_PORT",
            "DATABASE_NAME",
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
                "host": cls.DATABASE_HOST,
                "port": cls.DATABASE_PORT,
                "name": cls.DATABASE_NAME,
                "user": cls.DATABASE_USER,
                "pool_size": cls.DB_POOL_SIZE,
            },
            "prefect": {
                "block_name": cls.PREFECT_BLOCK_NAME,
                "default_table": cls.DEFAULT_TABLE_NAME,
            },
        }


settings = Settings()
