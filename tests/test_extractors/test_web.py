"""
Tests for WebExtractor.

Tests web page extraction functionality using Playwright.
"""

import pytest

from src.database.models import ExtractionSource
from src.extractors.web import WebExtractor
from src.models.schemas import ContentType, ExtractedContent
from src.utils.exceptions import ExtractionError, WebExtractionError


class TestWebExtractor:
    """Tests for WebExtractor class."""

    @pytest.fixture
    def extractor(self) -> WebExtractor:
        """Create a WebExtractor instance for testing."""
        return WebExtractor()

    @pytest.fixture
    def extractor_with_config(self) -> WebExtractor:
        """Create a WebExtractor with custom config."""
        return WebExtractor(
            config={
                "wait_for_selector": "body",
                "timeout": 5000,
            }
        )

    # ============================================================
    # Initialization Tests
    # ============================================================

    @pytest.mark.unit
    def test_extractor_creation(self, extractor: WebExtractor) -> None:
        """Test that extractor is created correctly."""
        assert extractor is not None
        assert extractor.config is not None

    @pytest.mark.unit
    def test_extractor_with_custom_config(self, extractor_with_config: WebExtractor) -> None:
        """Test extractor with custom configuration."""
        assert extractor_with_config.config.get("wait_for_selector") == "body"
        assert extractor_with_config.config.get("timeout") == 5000

    # ============================================================
    # URL Validation Tests
    # ============================================================

    @pytest.mark.unit
    async def test_validate_valid_http_url(self, extractor: WebExtractor) -> None:
        """Test validation accepts http URLs."""
        await extractor.validate_source("http://example.com")

    @pytest.mark.unit
    async def test_validate_valid_https_url(self, extractor: WebExtractor) -> None:
        """Test validation accepts https URLs."""
        await extractor.validate_source("https://example.com")

    @pytest.mark.unit
    async def test_validate_invalid_url_scheme(self, extractor: WebExtractor) -> None:
        """Test validation rejects non-http/https URLs."""
        with pytest.raises(WebExtractionError, match=r"(?i)http"):
            await extractor.validate_source("ftp://example.com")

    @pytest.mark.unit
    async def test_validate_invalid_url_format(self, extractor: WebExtractor) -> None:
        """Test validation rejects invalid URL formats."""
        with pytest.raises(WebExtractionError):
            await extractor.validate_source("not-a-valid-url")

    # ============================================================
    # Extraction Tests (Integration)
    # ============================================================

    @pytest.mark.integration
    async def test_extract_example_com(self, extractor: WebExtractor) -> None:
        """Test extraction from example.com (real network call)."""
        try:
            result = await extractor.extract("https://example.com")

            assert isinstance(result, ExtractedContent)
            assert result.source == ExtractionSource.WEB
            assert result.source_url == "https://example.com"
            assert "Example Domain" in result.metadata.title
            assert len(result.content) > 0
            assert result.content_type == ContentType.TEXT

        finally:
            await extractor.close()

    @pytest.mark.integration
    async def test_extract_returns_metadata(self, extractor: WebExtractor) -> None:
        """Test that extraction returns proper metadata."""
        try:
            result = await extractor.extract("https://example.com")

            assert result.metadata is not None
            assert result.metadata.title is not None
            assert result.extracted_at is not None

        finally:
            await extractor.close()

    @pytest.mark.integration
    async def test_extract_cleans_html(self, extractor: WebExtractor) -> None:
        """Test that extraction removes HTML tags."""
        try:
            result = await extractor.extract("https://example.com")

            # Content should not contain HTML tags
            assert "<html" not in result.content.lower()
            assert "<body" not in result.content.lower()
            assert "<script" not in result.content.lower()

        finally:
            await extractor.close()

    # ============================================================
    # Error Handling Tests
    # ============================================================

    @pytest.mark.integration
    async def test_extract_nonexistent_domain(self, extractor: WebExtractor) -> None:
        """Test extraction fails gracefully for non-existent domains."""
        try:
            with pytest.raises(WebExtractionError):
                await extractor.extract("https://this-domain-definitely-does-not-exist-12345.com")
        finally:
            await extractor.close()

    @pytest.mark.unit
    async def test_extract_empty_url(self, extractor: WebExtractor) -> None:
        """Test extraction fails for empty URL."""
        with pytest.raises(
            (ExtractionError, WebExtractionError, ValueError), match=r"(?i)empty|invalid|url"
        ):
            await extractor.extract("")

    # ============================================================
    # Resource Cleanup Tests
    # ============================================================

    @pytest.mark.unit
    async def test_extractor_close(self, extractor: WebExtractor) -> None:
        """Test that extractor closes without error."""
        await extractor.close()
        # Should not raise any exceptions

    @pytest.mark.unit
    async def test_extractor_double_close(self, extractor: WebExtractor) -> None:
        """Test that double close doesn't cause errors."""
        await extractor.close()
        await extractor.close()
        # Should not raise any exceptions


class TestWebExtractorConfig:
    """Tests for WebExtractor configuration options."""

    @pytest.mark.unit
    def test_default_config(self) -> None:
        """Test default configuration values."""
        extractor = WebExtractor()
        # Default config should be an empty dict or have sensible defaults
        assert extractor.config is not None

    @pytest.mark.unit
    def test_custom_timeout(self) -> None:
        """Test custom timeout configuration."""
        extractor = WebExtractor(config={"timeout": 60000})
        assert extractor.config.get("timeout") == 60000

    @pytest.mark.unit
    def test_custom_wait_for_selector(self) -> None:
        """Test custom wait_for_selector configuration."""
        extractor = WebExtractor(config={"wait_for_selector": "#main-content"})
        assert extractor.config.get("wait_for_selector") == "#main-content"

    @pytest.mark.unit
    def test_custom_user_agent(self) -> None:
        """Test custom user agent configuration."""
        extractor = WebExtractor(config={"user_agent": "CustomBot/1.0"})
        assert extractor.config.get("user_agent") == "CustomBot/1.0"
