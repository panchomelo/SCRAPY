"""
FastAPI application factory and lifespan management.

Creates the main FastAPI application with:
- Async lifespan for DB and engine initialization/cleanup
- CORS middleware configuration
- API router mounting
- Health check endpoint
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.core.config import get_settings
from src.core.engine import ScrapyEngine, close_engine
from src.database.connection import close_db, init_db
from src.models.jobs import HealthResponse
from src.services.callback_service import CallbackService, close_callback_service
from src.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager.

    Handles startup and shutdown events:
    - Startup: Initialize DB, create tables, start engine
    - Shutdown: Close engine, cleanup connections

    Args:
        app: FastAPI application instance

    Yields:
        None
    """
    settings = get_settings()

    # === STARTUP ===
    logger.info("Starting application", app_name=settings.app_name)

    # Configure logging
    configure_logging()

    # Initialize database
    await init_db()
    logger.info("Database initialized")

    # Initialize engine
    app.state.engine = ScrapyEngine()
    await app.state.engine.initialize()
    logger.info("ScrapyEngine initialized")

    # Initialize callback service
    app.state.callback_service = CallbackService()
    logger.info("CallbackService initialized")

    yield

    # === SHUTDOWN ===
    logger.info("Shutting down application")

    # Close callback service
    await close_callback_service()

    # Close engine
    await close_engine()
    if hasattr(app.state, "engine"):
        await app.state.engine.shutdown()

    # Close database
    await close_db()

    logger.info("Application shutdown complete")


def create_app() -> FastAPI:
    """
    Application factory.

    Creates and configures the FastAPI application with all
    middleware, routes, and settings.

    Returns:
        Configured FastAPI application
    """
    settings = get_settings()

    app = FastAPI(
        title=settings.app_name,
        description="Modular scraping engine for RAG pipeline integration",
        version="1.0.0",
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        openapi_url="/openapi.json" if settings.debug else None,
        lifespan=lifespan,
    )

    # Configure CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include routers
    from api.routes.jobs import router as jobs_router

    app.include_router(jobs_router, prefix="/api/v1")

    # Health check endpoint (no auth required)
    @app.get("/health", tags=["Health"], response_model=HealthResponse)
    async def health_check() -> HealthResponse:
        """
        Health check endpoint.

        Returns basic application status. Used by load balancers
        and monitoring systems.
        """
        return HealthResponse(
            status="healthy",
            version="1.0.0",
            database="connected",
        )

    # Root endpoint
    @app.get("/", tags=["Root"])
    async def root() -> dict:
        """Root endpoint with API information."""
        return {
            "app": settings.app_name,
            "version": "1.0.0",
            "docs": "/docs" if settings.debug else "Disabled in production",
            "health": "/health",
            "api": "/api/v1",
        }

    logger.info("FastAPI application created")

    return app


# Application instance for uvicorn
app = create_app()
