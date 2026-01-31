"""
Callback service for sending extraction results via HTTP webhook.

Uses httpx for async HTTP calls with Tenacity retry logic
to ensure reliable delivery to the RAG pipeline.
"""

from collections.abc import Mapping
from typing import Any

import httpx
from tenacity import (
    before_log,
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from src.core.config import get_settings
from src.models.jobs import CallbackPayload
from src.utils.exceptions import CallbackError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class CallbackService:
    """
    Service for sending extraction results to callback URLs.

    Handles HTTP POST requests to webhook endpoints with automatic
    retry logic for transient failures.

    Usage:
        service = CallbackService()
        await service.send_callback(payload, callback_url)
    """

    def __init__(self) -> None:
        """Initialize the callback service."""
        self._settings = get_settings()
        self._client: httpx.AsyncClient | None = None

    async def _log_request(self, request: httpx.Request) -> None:
        """Event hook to log outgoing requests."""
        logger.debug(
            "HTTP request",
            method=request.method,
            url=str(request.url),
        )

    async def _log_response(self, response: httpx.Response) -> None:
        """Event hook to log incoming responses."""
        logger.debug(
            "HTTP response",
            status_code=response.status_code,
            url=str(response.url),
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client with modern httpx configuration."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                # Modern httpx: HTTP/2 for better performance
                http2=True,
                # Follow redirects automatically (common for webhooks)
                follow_redirects=True,
                # Default headers for all requests
                headers={
                    "User-Agent": "Scrapy-Engine/1.0",
                    "Accept": "application/json",
                },
                # Granular timeout control (per httpx docs)
                timeout=httpx.Timeout(
                    connect=10.0,  # Time to establish connection
                    read=float(self._settings.callback_timeout),  # Time to read response
                    write=10.0,  # Time to send request data
                    pool=5.0,  # Time to acquire connection from pool
                ),
                # Connection pooling limits
                limits=httpx.Limits(
                    max_connections=100,  # Total connections allowed
                    max_keepalive_connections=20,  # Connections kept alive for reuse
                ),
                # Event hooks for request/response logging
                event_hooks={
                    "request": [self._log_request],
                    "response": [self._log_response],
                },
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @retry(
        # Combined stop: max 3 attempts OR 30 seconds total
        stop=(stop_after_attempt(3) | stop_after_delay(30)),
        # Random exponential backoff (jitter) to prevent thundering herd
        wait=wait_random_exponential(multiplier=1, min=1, max=10),
        # Retry on HTTP errors and callback failures
        retry=retry_if_exception_type((httpx.HTTPError, CallbackError)),
        # Log before sleeping between retries (INFO level)
        before_sleep=before_sleep_log(logger, log_level=20),
        # Log before each attempt (DEBUG level)
        before=before_log(logger, log_level=10),
        # Reraise original exception (not RetryError)
        reraise=True,
    )
    async def send_callback(
        self,
        payload: CallbackPayload,
        callback_url: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> bool:
        """
        Send extraction result to the callback URL.

        Args:
            payload: The callback payload with job results
            callback_url: Webhook URL to POST to
            headers: Optional additional headers (merged with defaults)

        Returns:
            True if callback was successful

        Raises:
            CallbackError: If callback fails after all retries
        """
        logger.info(
            "Sending callback",
            job_id=payload.job_id,
            url=callback_url,
            status=payload.status.value,
        )

        try:
            client = await self._get_client()

            # Merge with any custom headers (Content-Type set per-request)
            request_headers: dict[str, Any] = {"Content-Type": "application/json"}
            if headers:
                request_headers.update(headers)

            # Send POST request (client has default User-Agent/Accept)
            response = await client.post(
                callback_url,
                json=payload.model_dump(mode="json"),
                headers=request_headers,
            )

            # Check response status
            if response.status_code >= 400:
                logger.warning(
                    "Callback received error response",
                    job_id=payload.job_id,
                    url=callback_url,
                    status_code=response.status_code,
                    response_body=response.text[:500],
                )
                raise CallbackError(
                    f"Callback failed with status {response.status_code}",
                    url=callback_url,
                    status_code=response.status_code,
                )

            logger.info(
                "Callback successful",
                job_id=payload.job_id,
                url=callback_url,
                status_code=response.status_code,
            )
            return True

        # Granular timeout handling (per httpx docs)
        except httpx.ConnectTimeout as e:
            logger.error(
                "Callback connection timeout",
                job_id=payload.job_id,
                url=callback_url,
                error=str(e),
            )
            raise CallbackError(
                f"Connection timed out: {e}",
                url=callback_url,
            ) from e

        except httpx.ReadTimeout as e:
            logger.error(
                "Callback read timeout",
                job_id=payload.job_id,
                url=callback_url,
                error=str(e),
            )
            raise CallbackError(
                f"Read timed out: {e}",
                url=callback_url,
            ) from e

        except httpx.PoolTimeout as e:
            logger.error(
                "Callback pool timeout (connection pool exhausted)",
                job_id=payload.job_id,
                url=callback_url,
                error=str(e),
            )
            raise CallbackError(
                f"Connection pool exhausted: {e}",
                url=callback_url,
            ) from e

        except httpx.TimeoutException as e:
            # Catch-all for other timeout types (WriteTimeout, etc.)
            logger.error(
                "Callback timeout",
                job_id=payload.job_id,
                url=callback_url,
                error=str(e),
            )
            raise CallbackError(
                f"Callback timed out: {e}",
                url=callback_url,
            ) from e

        except httpx.HTTPError as e:
            logger.error(
                "Callback HTTP error",
                job_id=payload.job_id,
                url=callback_url,
                error=str(e),
            )
            raise CallbackError(
                f"Callback HTTP error: {e}",
                url=callback_url,
            ) from e

    async def send_success(
        self,
        job_id: str,
        callback_url: str,
        content: dict[str, Any],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> bool:
        """
        Send a success callback with extracted content.

        Args:
            job_id: The extraction job ID
            callback_url: Webhook URL to POST to
            content: The extracted content as dict
            headers: Optional additional headers

        Returns:
            True if callback was successful
        """
        from src.database.models import JobStatus

        payload = CallbackPayload(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            content=content,
            error=None,
        )
        return await self.send_callback(payload, callback_url, headers=headers)

    async def send_failure(
        self,
        job_id: str,
        callback_url: str,
        error_message: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> bool:
        """
        Send a failure callback with error information.

        Args:
            job_id: The extraction job ID
            callback_url: Webhook URL to POST to
            error_message: Description of the error
            headers: Optional additional headers

        Returns:
            True if callback was successful
        """
        from src.database.models import JobStatus

        payload = CallbackPayload(
            job_id=job_id,
            status=JobStatus.FAILED,
            content=None,
            error=error_message,
        )
        return await self.send_callback(payload, callback_url, headers=headers)


# Global callback service instance
_callback_service: CallbackService | None = None


def get_callback_service() -> CallbackService:
    """
    Get or create the global CallbackService instance.

    Returns:
        CallbackService instance
    """
    global _callback_service

    if _callback_service is None:
        _callback_service = CallbackService()

    return _callback_service


async def close_callback_service() -> None:
    """Close and cleanup the global callback service."""
    global _callback_service

    if _callback_service is not None:
        await _callback_service.close()
        _callback_service = None
