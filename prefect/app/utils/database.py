"""Database configuration and connection management."""

from contextlib import contextmanager

from collections.abc import Generator

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session

from config.settings import settings


class DatabaseManager:
    """Manages database connections and provides context managers."""

    def __init__(self, connection_string: str | None = None):
        """Initialize database manager with optional custom connection string."""
        self.connection_string = (
            connection_string or settings.get_db_connection_string()
        )
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Lazy-load the database engine with connection pooling."""
        if self._engine is None:
            self._engine = create_engine(
                self.connection_string,
                pool_size=settings.DB_POOL_SIZE,
                max_overflow=settings.DB_MAX_OVERFLOW,
                pool_pre_ping=settings.DB_POOL_PRE_PING,
                echo=False,
            )
        return self._engine

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = self.engine.connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager for SQLAlchemy ORM sessions."""
        SessionLocal = sessionmaker(bind=self.engine)
        session = SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def dispose(self):
        """Dispose of the connection pool."""
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def get_table_count(self, schema_name: str | None, table_name: str) -> int:
        """Get the number of rows in a table."""
        with self.get_connection() as connection:
            result = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {f'{schema_name}.' if schema_name else ''}{table_name}"
                )
            )
            return result.scalar()


db_manager = DatabaseManager()
