"""
Structured logging configuration using structlog.

Provides JSON-formatted logs for production (parseable by n8n and monitoring tools)
and human-readable colored output for development.

Features:
    - Context variables for async-safe request scoping
    - CallsiteParameterAdder for filename, line number, function name
    - EventRenamer for JSON-standard "msg" key
    - Configurable console rendering with exception formatting
"""

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import structlog
from structlog.processors import (
    CallsiteParameter,
    CallsiteParameterAdder,
    EventRenamer,
)
from structlog.typing import BindableLogger, Processor

from src.core.config import get_settings


def _add_app_context(
    logger: Any,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add application context to all log entries."""
    settings = get_settings()
    event_dict["app"] = settings.app_name
    event_dict["environment"] = "development" if settings.debug else "production"
    return event_dict


def _configure_stdlib_logging(log_level: str) -> None:
    """Configure standard library logging to work with structlog."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level),
    )

    # Silence noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def configure_logging() -> None:
    """
    Configure structlog for the application.

    In debug mode: Human-readable colored output with rich tracebacks
    In production: JSON output for machine parsing with structured tracebacks

    Call this once at application startup.
    """
    settings = get_settings()

    _configure_stdlib_logging(settings.log_level)

    # Shared processors for both modes
    shared_processors: list[Processor] = [
        # Merge context variables first (async-safe)
        structlog.contextvars.merge_contextvars,
        # Add log level early for filtering
        structlog.processors.add_log_level,
        # Add callsite information (filename, line number, function)
        CallsiteParameterAdder(
            [
                CallsiteParameter.FILENAME,
                CallsiteParameter.LINENO,
                CallsiteParameter.FUNC_NAME,
            ]
        ),
        # ISO timestamp for consistency
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # Application context (app name, environment)
        _add_app_context,
        # Render stack info if present
        structlog.processors.StackInfoRenderer(),
        # Ensure all strings are unicode
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.debug:
        # Development: colored, human-readable output
        processors: list[Processor] = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(
                colors=True,
                # Use plain traceback for cleaner development output
                exception_formatter=structlog.dev.plain_traceback,
                # Sort keys for consistent output
                sort_keys=True,
            ),
        ]
    else:
        # Production: JSON output for n8n and monitoring
        processors = [
            *shared_processors,
            # Rename "event" to "msg" (JSON logging standard)
            EventRenamer(to="msg"),
            # Format exception info as string for JSON
            structlog.processors.format_exc_info,
            # Render as JSON
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, settings.log_level)),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None, **initial_context: Any) -> BindableLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically module name)
        **initial_context: Initial context to bind to all log entries

    Returns:
        BindableLogger: Structured logger with optional initial context

    Example:
        >>> logger = get_logger("extractors.web", job_id="abc123")
        >>> logger.info("Starting extraction", url="https://example.com")
        # Output (JSON in production):
        # {"msg": "Starting extraction", "url": "https://example.com",
        #  "job_id": "abc123", "filename": "web.py", "lineno": 42, ...}
    """
    logger = structlog.get_logger(name)

    if initial_context:
        logger = logger.bind(**initial_context)

    return logger


def bind_context(**context: Any) -> None:
    """
    Bind context variables that will be included in all subsequent log entries.

    Uses structlog's contextvars for async-safe, request-scoped context.
    Useful for job_id, user_id, request_id, etc.

    Args:
        **context: Key-value pairs to add to logging context

    Example:
        >>> bind_context(job_id="abc123", source="web")
        >>> logger.info("Processing")  # Will include job_id and source
    """
    structlog.contextvars.bind_contextvars(**context)


def unbind_context(*keys: str) -> None:
    """
    Remove specific keys from the logging context.

    Args:
        *keys: Keys to remove from context

    Example:
        >>> unbind_context("sensitive_data")
    """
    structlog.contextvars.unbind_contextvars(*keys)


def clear_context() -> None:
    """
    Clear all bound context variables.

    Call this at the end of request/job processing to avoid context leaking.
    """
    structlog.contextvars.clear_contextvars()


@contextmanager
def bound_context(**context: Any) -> Iterator[None]:
    """
    Temporarily bind context variables within a context manager.

    Context is automatically restored when exiting the block.
    Useful for adding temporary context without manual cleanup.

    Args:
        **context: Key-value pairs to temporarily add

    Yields:
        None

    Example:
        >>> with bound_context(operation="payment", user_id="u123"):
        ...     logger.info("Processing payment")  # Includes operation and user_id
        >>> logger.info("Done")  # Context restored, no operation/user_id
    """
    with structlog.contextvars.bound_contextvars(**context):
        yield


def get_current_context() -> dict[str, Any]:
    """
    Get a copy of the current logging context.

    Returns:
        dict: Current context variables

    Example:
        >>> bind_context(job_id="abc123")
        >>> ctx = get_current_context()
        >>> print(ctx)  # {'job_id': 'abc123'}
    """
    return structlog.contextvars.get_contextvars()
