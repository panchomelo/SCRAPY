"""
Custom exceptions for the Scrapy engine.

Provides granular exception types for different failure scenarios,
enabling precise error handling and meaningful error messages.
"""

from typing import Any


class ScrapyError(Exception):
    """
    Base exception for all Scrapy engine errors.

    All custom exceptions inherit from this class, allowing
    catch-all handling when needed.

    Attributes:
        message: Human-readable error description
        details: Additional context for debugging
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        self.message = message
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for JSON serialization."""
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "details": self.details,
        }


# ===================
# Extraction Errors
# ===================


class ExtractionError(ScrapyError):
    """
    Base exception for extraction-related failures.

    Raised when content extraction fails regardless of source type.
    """

    pass


class WebExtractionError(ExtractionError):
    """
    Raised when web page extraction fails.

    Common causes:
    - Page not found (404)
    - Timeout during navigation
    - JavaScript rendering failure
    - Anti-bot protection triggered
    """

    def __init__(
        self,
        message: str,
        url: str | None = None,
        status_code: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if url:
            details["url"] = url
        if status_code:
            details["status_code"] = status_code
        super().__init__(message, details)


class DocumentExtractionError(ExtractionError):
    """
    Raised when document extraction fails (PDF, Excel).

    Common causes:
    - Corrupted file
    - Password-protected document
    - Unsupported format
    - Empty document
    """

    def __init__(
        self,
        message: str,
        file_path: str | None = None,
        file_type: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if file_path:
            details["file_path"] = file_path
        if file_type:
            details["file_type"] = file_type
        super().__init__(message, details)


class SocialMediaExtractionError(ExtractionError):
    """
    Raised when social media extraction via Apify fails.

    Common causes:
    - Invalid Apify token
    - Rate limiting
    - Profile not found
    - API changes
    """

    def __init__(
        self,
        message: str,
        platform: str | None = None,
        actor_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if platform:
            details["platform"] = platform
        if actor_id:
            details["actor_id"] = actor_id
        super().__init__(message, details)


# ===================
# Service Errors
# ===================


class CallbackError(ScrapyError):
    """
    Raised when callback to n8n webhook fails.

    This is critical as it means n8n won't receive the extraction results.
    Includes retry information for debugging.
    """

    def __init__(
        self,
        message: str,
        callback_url: str | None = None,
        status_code: int | None = None,
        attempt: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if callback_url:
            details["callback_url"] = callback_url
        if status_code:
            details["status_code"] = status_code
        if attempt:
            details["attempt"] = attempt
        super().__init__(message, details)


class ApifyServiceError(ScrapyError):
    """
    Raised when Apify API calls fail.

    Separate from SocialMediaExtractionError to distinguish
    between API issues and extraction logic issues.
    """

    def __init__(
        self,
        message: str,
        actor_id: str | None = None,
        run_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if actor_id:
            details["actor_id"] = actor_id
        if run_id:
            details["run_id"] = run_id
        super().__init__(message, details)


# ===================
# Configuration Errors
# ===================


class ConfigurationError(ScrapyError):
    """
    Raised when application configuration is invalid.

    Typically caught at startup to fail fast.
    """

    pass


# ===================
# Database Errors
# ===================


class DatabaseError(ScrapyError):
    """
    Raised when database operations fail.

    Wraps SQLAlchemy exceptions with additional context.
    """

    def __init__(
        self,
        message: str,
        operation: str | None = None,
        table: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if operation:
            details["operation"] = operation
        if table:
            details["table"] = table
        super().__init__(message, details)


class JobNotFoundError(DatabaseError):
    """Raised when a job ID is not found in the database."""

    def __init__(self, job_id: str) -> None:
        super().__init__(
            message=f"Job not found: {job_id}",
            operation="select",
            table="extraction_jobs",
            details={"job_id": job_id},
        )


# ===================
# File Errors
# ===================


class FileError(ScrapyError):
    """
    Raised when file operations fail.

    Covers download, temp file creation, and cleanup issues.
    """

    def __init__(
        self,
        message: str,
        file_path: str | None = None,
        operation: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        details = details or {}
        if file_path:
            details["file_path"] = file_path
        if operation:
            details["operation"] = operation
        super().__init__(message, details)
