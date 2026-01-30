"""
Scrapy Engine - Central orchestrator for extraction operations.

Manages extractor lifecycle, routes requests to appropriate extractors,
and provides a unified interface for all extraction operations.
"""

from typing import Any

from src.core.config import get_settings
from src.database.models import ExtractionSource
from src.extractors.base import BaseExtractor
from src.extractors.documents import ExcelExtractor, PDFExtractor
from src.extractors.web import WebExtractor
from src.models.schemas import ExtractedContent
from src.services.apify_service import SocialMediaExtractor
from src.utils.exceptions import ExtractionError, ScrapyError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class ScrapyEngine:
    """
    Central orchestrator for the Scrapy extraction system.

    Manages extractor instances, routes extraction requests to the
    appropriate extractor based on source type, and handles lifecycle.

    Usage:
        async with ScrapyEngine() as engine:
            result = await engine.extract(
                source=ExtractionSource.WEB,
                target="https://example.com"
            )
    """

    def __init__(self) -> None:
        """Initialize the engine with lazy-loaded extractors."""
        self._settings = get_settings()
        self._extractors: dict[ExtractionSource, BaseExtractor] = {}
        self._initialized = False

        logger.info("ScrapyEngine created")

    async def __aenter__(self) -> "ScrapyEngine":
        """Async context manager entry."""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.shutdown()

    async def initialize(self) -> None:
        """
        Initialize all extractors.

        Extractors are created lazily but this method can be called
        to pre-initialize them if needed.
        """
        if self._initialized:
            return

        logger.info("Initializing ScrapyEngine")
        self._initialized = True

    async def shutdown(self) -> None:
        """
        Shutdown all extractors and release resources.

        Should be called when the engine is no longer needed.
        """
        logger.info("Shutting down ScrapyEngine")

        for source, extractor in self._extractors.items():
            try:
                await extractor.close()
                logger.debug("Extractor closed", source=source.value)
            except Exception as e:
                logger.warning(
                    "Error closing extractor",
                    source=source.value,
                    error=str(e),
                )

        self._extractors.clear()
        self._initialized = False

        logger.info("ScrapyEngine shutdown complete")

    def _get_extractor(
        self,
        source: ExtractionSource,
        config: dict[str, Any] | None = None,
    ) -> BaseExtractor:
        """
        Get or create an extractor for the given source type.

        Args:
            source: Type of extraction source
            config: Optional configuration for the extractor

        Returns:
            Appropriate extractor instance

        Raises:
            ValidationError: If source type is not supported
        """
        # Return cached extractor if available and no custom config
        if source in self._extractors and config is None:
            return self._extractors[source]

        # Create new extractor based on source type
        extractor: BaseExtractor

        match source:
            case ExtractionSource.WEB:
                extractor = WebExtractor(config=config)
            case ExtractionSource.PDF:
                extractor = PDFExtractor(config=config)
            case ExtractionSource.EXCEL:
                extractor = ExcelExtractor(config=config)
            case ExtractionSource.SOCIAL:
                extractor = SocialMediaExtractor(config=config)
            case _:
                raise ScrapyError(
                    f"Unsupported extraction source: {source}",
                    details={"source": str(source)},
                )

        # Cache extractor if no custom config (default extractors)
        if config is None:
            self._extractors[source] = extractor

        return extractor

    async def extract(
        self,
        *,
        source: ExtractionSource,
        target: str,
        config: dict[str, Any] | None = None,
        file_bytes: bytes | None = None,
    ) -> ExtractedContent:
        """
        Extract content from the specified source.

        Routes the request to the appropriate extractor based on source type.

        Args:
            source: Type of extraction (web, pdf, excel, social)
            target: URL, file path, or identifier to extract from
            config: Optional extractor-specific configuration
            file_bytes: Optional raw bytes for document extraction

        Returns:
            ExtractedContent: Unified extraction result

        Raises:
            ExtractionError: If extraction fails
            ValidationError: If source type is not supported
        """
        logger.info(
            "Engine extraction request",
            source=source.value,
            target=target,
            has_config=config is not None,
            has_bytes=file_bytes is not None,
        )

        try:
            extractor = self._get_extractor(source, config)

            # Use bytes extraction for documents if provided
            if file_bytes is not None and source in (
                ExtractionSource.PDF,
                ExtractionSource.EXCEL,
            ):
                result = await extractor.extract_bytes(file_bytes, filename=target)
            else:
                result = await extractor.extract(target)

            logger.info(
                "Engine extraction completed",
                source=source.value,
                target=target,
                content_length=len(result.content),
            )

            return result

        except ExtractionError:
            # Re-raise extraction errors as-is
            raise
        except Exception as e:
            logger.error(
                "Unexpected extraction error",
                source=source.value,
                target=target,
                error=str(e),
            )
            raise ExtractionError(
                f"Extraction failed for {source.value}: {e}",
                source=source.value,
                url=target,
            ) from e

    async def extract_web(
        self,
        url: str,
        config: dict[str, Any] | None = None,
    ) -> ExtractedContent:
        """
        Convenience method for web extraction.

        Args:
            url: Web URL to extract from
            config: Optional WebExtractionConfig parameters

        Returns:
            ExtractedContent from the web page
        """
        return await self.extract(
            source=ExtractionSource.WEB,
            target=url,
            config=config,
        )

    async def extract_pdf(
        self,
        path_or_url: str,
        config: dict[str, Any] | None = None,
        file_bytes: bytes | None = None,
    ) -> ExtractedContent:
        """
        Convenience method for PDF extraction.

        Args:
            path_or_url: File path or URL to PDF
            config: Optional PDFExtractionConfig parameters
            file_bytes: Optional raw PDF bytes

        Returns:
            ExtractedContent from the PDF
        """
        return await self.extract(
            source=ExtractionSource.PDF,
            target=path_or_url,
            config=config,
            file_bytes=file_bytes,
        )

    async def extract_excel(
        self,
        path_or_url: str,
        config: dict[str, Any] | None = None,
        file_bytes: bytes | None = None,
    ) -> ExtractedContent:
        """
        Convenience method for Excel extraction.

        Args:
            path_or_url: File path or URL to Excel file
            config: Optional ExcelExtractionConfig parameters
            file_bytes: Optional raw Excel bytes

        Returns:
            ExtractedContent from the Excel file
        """
        return await self.extract(
            source=ExtractionSource.EXCEL,
            target=path_or_url,
            config=config,
            file_bytes=file_bytes,
        )

    async def extract_social(
        self,
        profile_or_post_url: str,
        config: dict[str, Any] | None = None,
    ) -> ExtractedContent:
        """
        Convenience method for social media extraction.

        Args:
            profile_or_post_url: Social media URL to extract
            config: Optional configuration (actor_id, etc.)

        Returns:
            ExtractedContent from social media
        """
        return await self.extract(
            source=ExtractionSource.SOCIAL,
            target=profile_or_post_url,
            config=config,
        )

    @property
    def is_initialized(self) -> bool:
        """Check if the engine has been initialized."""
        return self._initialized

    @property
    def active_extractors(self) -> list[str]:
        """List of currently active extractor types."""
        return [source.value for source in self._extractors.keys()]


# Global engine instance for convenience
_engine: ScrapyEngine | None = None


async def get_engine() -> ScrapyEngine:
    """
    Get or create the global ScrapyEngine instance.

    For FastAPI, prefer using dependency injection instead.

    Returns:
        Initialized ScrapyEngine instance
    """
    global _engine

    if _engine is None:
        _engine = ScrapyEngine()
        await _engine.initialize()

    return _engine


async def close_engine() -> None:
    """Shutdown and cleanup the global engine instance."""
    global _engine

    if _engine is not None:
        await _engine.shutdown()
        _engine = None
