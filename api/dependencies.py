"""
FastAPI dependencies for authentication and resource injection.

Provides:
- API Key authentication via X-API-Key header
- Database session injection
- Engine and service injection
"""

import secrets
from collections.abc import AsyncGenerator
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import Settings, get_settings
from src.core.engine import ScrapyEngine
from src.database.connection import get_session_factory
from src.services.callback_service import CallbackService
from src.utils.logging import get_logger

logger = get_logger(__name__)


async def verify_api_key(
    x_api_key: Annotated[str | None, Header()] = None,
    settings: Settings = Depends(get_settings),
) -> str:
    """
    Verify the API key from request header.

    Uses constant-time comparison to prevent timing attacks.

    Args:
        x_api_key: API key from X-API-Key header
        settings: Application settings

    Returns:
        Validated API key

    Raises:
        HTTPException: 401 if key is missing or invalid
    """
    if x_api_key is None:
        logger.warning("Missing API key in request")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Constant-time comparison to prevent timing attacks
    if not secrets.compare_digest(x_api_key, settings.api_key):
        logger.warning("Invalid API key attempt")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return x_api_key


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency for database session.

    Creates a new session for each request and handles cleanup.

    Yields:
        AsyncSession: Database session
    """
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def get_engine(request: Request) -> ScrapyEngine:
    """
    Dependency for ScrapyEngine.

    Gets the engine instance from application state.

    Args:
        request: FastAPI request object

    Returns:
        ScrapyEngine instance
    """
    return request.app.state.engine


async def get_callback_service(request: Request) -> CallbackService:
    """
    Dependency for CallbackService.

    Gets the callback service from application state.

    Args:
        request: FastAPI request object

    Returns:
        CallbackService instance
    """
    return request.app.state.callback_service


# Type aliases for cleaner route signatures
ApiKeyDep = Annotated[str, Depends(verify_api_key)]
DbSessionDep = Annotated[AsyncSession, Depends(get_db_session)]
EngineDep = Annotated[ScrapyEngine, Depends(get_engine)]
CallbackServiceDep = Annotated[CallbackService, Depends(get_callback_service)]
