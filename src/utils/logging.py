"""
Structured logging configuration using structlog.

Provides JSON-formatted logs for production (parseable by n8n and monitoring tools)
and human-readable colored output for development.
"""

import logging
import sys
from typing import Any

import structlog
from structlog.types import Processor

from src.core.config import get_settings


def _add_app_context(
    logger: logging.Logger,
    method_name: str,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    """Add application context to all log entries."""
    settings = get_settings()
    event_dict["app"] = settings.app_name
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

    In debug mode: Human-readable colored output
    In production: JSON output for machine parsing

    Call this once at application startup.
    """
    settings = get_settings()

    _configure_stdlib_logging(settings.log_level)

    # Shared processors for both modes
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _add_app_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.debug:
        # Development: colored, human-readable output
        processors: list[Processor] = [
            *shared_processors,
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.plain_traceback,
            ),
        ]
    else:
        # Production: JSON output for n8n and monitoring
        processors = [
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, settings.log_level)),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None, **initial_context: Any) -> structlog.stdlib.BoundLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically module name)
        **initial_context: Initial context to bind to all log entries

    Returns:
        BoundLogger: Structured logger with optional initial context

    Example:
        >>> logger = get_logger("extractors.web", job_id="abc123")
        >>> logger.info("Starting extraction", url="https://example.com")
        # Output (JSON):
        # {"event": "Starting extraction", "url": "https://example.com",
        #  "job_id": "abc123", "logger": "extractors.web", "level": "info", ...}
    """
    logger = structlog.get_logger(name)

    if initial_context:
        logger = logger.bind(**initial_context)

    return logger


def bind_context(**context: Any) -> None:
    """
    Bind context variables that will be included in all subsequent log entries.

    Useful for request-scoped context like job_id, user_id, etc.

    Args:
        **context: Key-value pairs to add to logging context

    Example:
        >>> bind_context(job_id="abc123", source="web")
        >>> logger.info("Processing")  # Will include job_id and source
    """
    structlog.contextvars.bind_contextvars(**context)


def clear_context() -> None:
    """
    Clear all bound context variables.

    Call this at the end of request/job processing to avoid context leaking.
    """
    structlog.contextvars.clear_contextvars()
