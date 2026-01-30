"""
Callback service for sending extraction results via HTTP webhook.

Uses httpx for async HTTP calls with Tenacity retry logic
to ensure reliable delivery to the RAG pipeline.
"""

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
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

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=30.0,
                    write=10.0,
                    pool=10.0,
                ),
                limits=httpx.Limits(
                    max_connections=100,
                    max_keepalive_connections=20,
                ),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.HTTPError, CallbackError)),
        before_sleep=before_sleep_log(logger, log_level=20),
        reraise=True,
    )
    async def send_callback(
        self,
        payload: CallbackPayload,
        callback_url: str,
        *,
        headers: dict[str, str] | None = None,
    ) -> bool:
        """
        Send extraction result to the callback URL.

        Args:
            payload: The callback payload with job results
            callback_url: Webhook URL to POST to
            headers: Optional additional headers

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

            # Prepare headers
            request_headers = {
                "Content-Type": "application/json",
                "User-Agent": "Scrapy-Engine/1.0",
            }
            if headers:
                request_headers.update(headers)

            # Send POST request
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

        except httpx.TimeoutException as e:
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
        content: dict,
        *,
        headers: dict[str, str] | None = None,
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
        headers: dict[str, str] | None = None,
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
