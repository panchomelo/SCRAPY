"""
Base extractor abstract class with retry logic.

Provides common interface and Tenacity-based retry decorators
for all extractor implementations.
"""

from abc import ABC, abstractmethod
from typing import Any

from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.core.config import get_settings
from src.models.schemas import ExtractedContent
from src.utils.exceptions import ExtractionError
from src.utils.logging import get_logger

logger = get_logger(__name__)


def create_retry_decorator(
    max_attempts: int = 3,
    min_wait: float = 1,
    max_wait: float = 10,
    retry_exceptions: tuple[type[Exception], ...] = (ExtractionError,),
):
    """
    Create a Tenacity retry decorator with configurable parameters.

    Args:
        max_attempts: Maximum number of retry attempts
        min_wait: Minimum wait time between retries (seconds)
        max_wait: Maximum wait time between retries (seconds)
        retry_exceptions: Exception types that trigger a retry

    Returns:
        Configured retry decorator
    """
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=min_wait, max=max_wait),
        retry=retry_if_exception_type(retry_exceptions),
        before_sleep=before_sleep_log(logger, log_level=20),  # INFO level
        reraise=True,
    )


# Default retry decorator for extraction operations
extraction_retry = create_retry_decorator(
    max_attempts=3,
    min_wait=1,
    max_wait=10,
    retry_exceptions=(ExtractionError, TimeoutError, ConnectionError),
)


class BaseExtractor(ABC):
    """
    Abstract base class for all extractors.

    Defines the common interface and provides shared utilities.
    All extractors must implement the `extract` method.

    Attributes:
        config: Extraction configuration dictionary
        debug: Whether debug mode is enabled
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """
        Initialize the extractor.

        Args:
            config: Optional extraction configuration
        """
        self.config = config or {}
        settings = get_settings()
        self.debug = settings.debug
        self._logger = get_logger(self.__class__.__name__)

    @abstractmethod
    async def extract(self, source: str, **kwargs: Any) -> ExtractedContent:
        """
        Extract content from the given source.

        Args:
            source: URL or file path to extract from
            **kwargs: Additional extraction parameters

        Returns:
            ExtractedContent: Extracted and validated content

        Raises:
            ExtractionError: If extraction fails
        """
        pass

    async def validate_source(self, source: str) -> None:
        """
        Validate the source before extraction.

        Override in subclasses for source-specific validation.

        Args:
            source: Source to validate

        Raises:
            ExtractionError: If source is invalid
        """
        if not source:
            raise ExtractionError("Source cannot be empty")

    def _log_start(self, source: str) -> None:
        """Log extraction start."""
        self._logger.info(
            "Starting extraction",
            source=source[:100] + "..." if len(source) > 100 else source,
            extractor=self.__class__.__name__,
        )

    def _log_success(self, source: str, content_length: int) -> None:
        """Log successful extraction."""
        self._logger.info(
            "Extraction completed",
            source=source[:50] + "..." if len(source) > 50 else source,
            content_length=content_length,
        )

    def _log_error(self, source: str, error: Exception) -> None:
        """Log extraction error."""
        self._logger.error(
            "Extraction failed",
            source=source[:50] + "..." if len(source) > 50 else source,
            error=str(error),
            error_type=type(error).__name__,
        )
