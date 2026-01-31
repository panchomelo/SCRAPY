"""
Async database connection management using SQLAlchemy 2.0 with aiosqlite.

Provides session factory and database initialization utilities.
Designed for SQLite with easy migration path to PostgreSQL.

aiosqlite is used as the async driver for SQLite, but accessed through
SQLAlchemy's async API for ORM benefits, type safety, and future
migration path to PostgreSQL if needed.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from src.core.config import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Global engine instance (initialized on first use)
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_engine() -> AsyncEngine:
    """
    Get or create the async database engine.

    Uses StaticPool for SQLite to handle async properly.
    The engine is created once and reused across the application.

    SQLite-specific optimizations:
    - check_same_thread=False: Required for async (aiosqlite handles thread safety)
    - StaticPool: Single connection for SQLite (no connection pooling needed)
    - WAL mode: Better concurrency for read-heavy workloads (set via event)

    Returns:
        AsyncEngine: SQLAlchemy async engine instance
    """
    global _engine

    if _engine is None:
        settings = get_settings()

        # SQLite-specific configuration for aiosqlite driver
        connect_args: dict[str, bool | int] = {}
        poolclass = None
        pool_pre_ping = True  # Verify connections before use

        if settings.database_url.startswith("sqlite"):
            # Required for async: aiosqlite handles thread safety internally
            connect_args = {
                "check_same_thread": False,
                "timeout": 30,  # Connection timeout in seconds
            }
            # StaticPool ensures single connection for SQLite
            # (SQLite doesn't benefit from connection pooling)
            poolclass = StaticPool
            pool_pre_ping = False  # Not needed with StaticPool

        _engine = create_async_engine(
            settings.database_url,
            echo=settings.debug,  # Log SQL statements in debug mode
            connect_args=connect_args,
            poolclass=poolclass,
            pool_pre_ping=pool_pre_ping,
        )

        # SQLite optimizations via event listener (aiosqlite best practices)
        if settings.database_url.startswith("sqlite"):

            @event.listens_for(_engine.sync_engine, "connect")
            def _set_sqlite_pragma(dbapi_conn, connection_record) -> None:  # noqa: ANN001
                """Configure SQLite for better async performance."""
                cursor = dbapi_conn.cursor()
                # WAL mode: Better concurrency for read-heavy workloads
                cursor.execute("PRAGMA journal_mode=WAL")
                # Foreign keys: Enforce referential integrity
                cursor.execute("PRAGMA foreign_keys=ON")
                # Synchronous NORMAL: Good balance of safety and speed
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.close()

        logger.info(
            "Database engine created",
            url=settings.database_url.split("///")[0] + "///***",  # Hide path
        )

    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Get or create the async session factory.

    Returns:
        async_sessionmaker: Factory for creating async sessions
    """
    global _session_factory

    if _session_factory is None:
        engine = get_engine()
        _session_factory = async_sessionmaker(
            bind=engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )

    return _session_factory


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Async context manager for database sessions.

    Handles session lifecycle: creation, commit on success,
    rollback on error, and cleanup.

    Yields:
        AsyncSession: Database session for executing queries

    Example:
        >>> async with get_session() as session:
        ...     job = await repository.create_job(session, data)
        ...     # Auto-commits on success, rollbacks on exception
    """
    factory = get_session_factory()
    session = factory()

    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


async def init_db() -> None:
    """
    Initialize the database by creating all tables.

    Call this once at application startup (in FastAPI lifespan).
    Uses SQLAlchemy's create_all which is idempotent.

    Example:
        >>> @asynccontextmanager
        ... async def lifespan(app: FastAPI):
        ...     await init_db()
        ...     yield
    """
    from src.database.models import Base

    engine = get_engine()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    logger.info("Database initialized", tables=list(Base.metadata.tables.keys()))


async def close_db() -> None:
    """
    Close database connections gracefully.

    Call this on application shutdown to release resources.
    """
    global _engine, _session_factory

    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
        logger.info("Database connections closed")
