"""
Pytest configuration and fixtures.

Provides shared fixtures for testing the Scrapy Engine.
"""

import asyncio
from collections.abc import AsyncGenerator, Generator
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
# Event Loop Configuration
# ============================================================


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop, None, None]:
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


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


@pytest_asyncio.fixture
async def test_engine(test_settings: Settings) -> AsyncGenerator[Any, None]:
    """Create a test database engine with in-memory SQLite."""
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


@pytest_asyncio.fixture
async def test_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Create a test database session."""
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


@pytest_asyncio.fixture
async def scrapy_engine() -> AsyncGenerator[ScrapyEngine, None]:
    """Create a ScrapyEngine for testing."""
    engine = ScrapyEngine()
    await engine.initialize()
    yield engine
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


@pytest_asyncio.fixture
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
