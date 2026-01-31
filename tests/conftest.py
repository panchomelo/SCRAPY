"""
Pytest configuration and fixtures.

Provides shared fixtures for testing the Scrapy Engine.

Modern pytest-asyncio configuration (v0.23+):

Configuration in pyproject.toml:
    asyncio_mode = "auto"
        - Auto-detects async test functions
        - No need for @pytest.mark.asyncio decorator
        - Takes ownership of async fixtures

    asyncio_default_fixture_loop_scope = "session"
        - Async fixtures share session-scoped event loop by default
        - Can override per-fixture with loop_scope parameter

    asyncio_default_test_loop_scope = "function"
        - Each test gets isolated event loop by default
        - Can override with @pytest.mark.asyncio(loop_scope="module")

Fixture scoping patterns:
    @pytest_asyncio.fixture(loop_scope="session", scope="function")
        - loop_scope: which event loop to run in (session = shared)
        - scope: how long to cache fixture value (function = fresh each test)
"""

from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.core.config import Settings
from src.core.engine import ScrapyEngine
from src.database.models import Base

# ============================================================
# Settings Fixtures
# ============================================================


@pytest.fixture
def test_settings() -> Settings:
    """Create test settings with in-memory database."""
    return Settings(
        app_name="Scrapy Engine Test",
        debug=True,
        log_level="DEBUG",
        api_key="test-api-key-for-testing-purposes-1234567890",
        database_url="sqlite+aiosqlite:///:memory:",
        playwright_headless=True,
        playwright_timeout=10000,
    )


# ============================================================
# Database Fixtures
# ============================================================


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def test_engine(test_settings: Settings) -> AsyncGenerator[Any, None]:
    """
    Create a test database engine with in-memory SQLite.

    Uses session-scoped event loop but function-scoped caching
    for test isolation.
    """
    engine = create_async_engine(
        test_settings.database_url,
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine

    # Cleanup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await engine.dispose()


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def test_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session with automatic rollback."""
    async_session = sessionmaker(
        test_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session() as session:
        yield session
        await session.rollback()


# ============================================================
# Engine Fixtures
# ============================================================


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def scrapy_engine() -> AsyncGenerator[ScrapyEngine, None]:
    """
    Create a ScrapyEngine for testing.

    Ensures proper initialization and cleanup of Playwright resources.
    Uses session-scoped loop for efficient Playwright reuse.
    """
    engine = ScrapyEngine()
    await engine.initialize()
    try:
        yield engine
    finally:
        await engine.shutdown()


# ============================================================
# Mock Data Fixtures
# ============================================================


@pytest.fixture
def sample_web_content() -> dict[str, Any]:
    """Sample web extraction result."""
    return {
        "source": "web",
        "source_url": "https://example.com",
        "content": "This is sample extracted content from a web page.",
        "content_type": "text",
        "metadata": {
            "title": "Example Page",
            "description": "A sample page for testing",
            "language": "en",
        },
    }


@pytest.fixture
def sample_pdf_content() -> dict[str, Any]:
    """Sample PDF extraction result."""
    return {
        "source": "pdf",
        "source_url": "test.pdf",
        "content": "This is sample extracted content from a PDF document.",
        "content_type": "mixed",
        "metadata": {
            "title": "Test Document",
            "page_count": 5,
        },
        "tables": [],
        "images": [],
    }


@pytest.fixture
def valid_api_key(test_settings: Settings) -> str:
    """Return the valid test API key."""
    return test_settings.api_key


@pytest.fixture
def invalid_api_key() -> str:
    """Return an invalid API key."""
    return "invalid-key-that-should-fail"


# ============================================================
# HTTP Client Fixtures (for API testing)
# ============================================================


@pytest_asyncio.fixture(loop_scope="session", scope="function")
async def test_client():
    """
    Create a test client for the FastAPI app.

    Note: Requires httpx to be installed.
    """
    from httpx import ASGITransport, AsyncClient

    from api.app import create_app

    app = create_app()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        yield client


# ============================================================
# Helper Functions
# ============================================================


def assert_extracted_content(content: dict[str, Any]) -> None:
    """Assert that content has required fields."""
    assert "source" in content
    assert "content" in content
    assert "metadata" in content
    assert len(content["content"]) > 0
