"""
Job service for orchestrating extraction workflows.

Coordinates the complete extraction lifecycle:
1. Create job in database
2. Execute extraction
3. Update job status
4. Send callback to RAG pipeline
"""

from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from src.core.engine import ScrapyEngine
from src.database.models import ExtractionSource, JobStatus
from src.database.repository import JobRepository
from src.models.jobs import JobRequest, JobResponse
from src.models.schemas import ExtractedContent
from src.services.callback_service import CallbackService
from src.utils.exceptions import CallbackError, ExtractionError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class JobService:
    """
    Service for managing extraction job workflows.

    Orchestrates the complete extraction process from job creation
    through callback delivery, handling all state transitions.

    Usage:
        service = JobService(engine, callback_service)
        job = await service.create_and_execute(session, request)
    """

    def __init__(
        self,
        engine: ScrapyEngine,
        callback_service: CallbackService,
    ) -> None:
        """
        Initialize the job service.

        Args:
            engine: ScrapyEngine for extraction operations
            callback_service: Service for sending callbacks
        """
        self._engine = engine
        self._callback_service = callback_service

    async def create_job(
        self,
        session: AsyncSession,
        request: JobRequest,
    ) -> JobResponse:
        """
        Create a new extraction job.

        Args:
            session: Database session
            request: Job creation request

        Returns:
            JobResponse with job details
        """
        # Determine source type from request
        source = self._determine_source(request)

        # Create job in database
        job = await JobRepository.create(
            session,
            source=source,
            callback_url=str(request.callback_url),
            source_url=request.url,
        )

        await session.commit()

        logger.info(
            "Job created",
            job_id=job.id,
            source=source.value,
            url=request.url,
        )

        return JobResponse(
            job_id=job.id,
            status=job.status,
            message="Job created successfully",
        )

    def _determine_source(self, request: JobRequest) -> ExtractionSource:
        """
        Determine the extraction source type from request.

        Args:
            request: Job request with URL and optional source hint

        Returns:
            Appropriate ExtractionSource
        """
        # If source is explicitly provided, use it
        if request.source:
            return request.source

        # Infer from URL
        url_lower = request.url.lower()

        # Check for social media platforms
        social_domains = [
            "twitter.com",
            "x.com",
            "facebook.com",
            "instagram.com",
            "linkedin.com",
            "tiktok.com",
            "youtube.com",
            "reddit.com",
        ]
        if any(domain in url_lower for domain in social_domains):
            return ExtractionSource.SOCIAL

        # Check for document extensions
        if url_lower.endswith(".pdf"):
            return ExtractionSource.PDF

        if url_lower.endswith((".xlsx", ".xls", ".csv")):
            return ExtractionSource.EXCEL

        # Default to web
        return ExtractionSource.WEB

    async def execute_extraction(
        self,
        session: AsyncSession,
        job_id: str,
        request: JobRequest,
    ) -> ExtractedContent | None:
        """
        Execute the extraction for a job.

        Updates job status to PROCESSING, performs extraction,
        and updates status based on result.

        Args:
            session: Database session
            job_id: Job ID to process
            request: Original job request

        Returns:
            ExtractedContent if successful, None if failed
        """
        # Update status to PROCESSING
        await JobRepository.update_status(
            session,
            job_id,
            JobStatus.PROCESSING,
        )
        await session.commit()

        logger.info("Starting extraction", job_id=job_id, url=request.url)

        try:
            # Determine source and extract
            source = self._determine_source(request)

            # Build config from request
            config = self._build_config(request)

            # Execute extraction
            result = await self._engine.extract(
                source=source,
                target=request.url,
                config=config,
            )

            # Update job with result
            await JobRepository.set_result(
                session,
                job_id,
                result.model_dump(mode="json"),
            )
            await session.commit()

            logger.info(
                "Extraction completed",
                job_id=job_id,
                content_length=len(result.content),
            )

            return result

        except ExtractionError as e:
            logger.error(
                "Extraction failed",
                job_id=job_id,
                error=str(e),
            )
            await JobRepository.set_error(session, job_id, str(e))
            await session.commit()
            return None

        except Exception as e:
            logger.error(
                "Unexpected extraction error",
                job_id=job_id,
                error=str(e),
            )
            await JobRepository.set_error(
                session,
                job_id,
                f"Unexpected error: {e}",
            )
            await session.commit()
            return None

    def _build_config(self, request: JobRequest) -> dict[str, Any] | None:
        """
        Build extractor config from job request.

        Args:
            request: Job request

        Returns:
            Config dict or None if no custom config
        """
        if request.config is None:
            return None

        # Convert Pydantic model to dict if needed
        if hasattr(request.config, "model_dump"):
            return request.config.model_dump(exclude_none=True)

        return dict(request.config)

    async def send_callback(
        self,
        job_id: str,
        callback_url: str,
        result: ExtractedContent | None,
        error: str | None = None,
    ) -> bool:
        """
        Send callback to the RAG pipeline.

        Args:
            job_id: Job ID
            callback_url: Webhook URL
            result: Extraction result (if successful)
            error: Error message (if failed)

        Returns:
            True if callback was sent successfully
        """
        try:
            if result is not None:
                # Success callback
                return await self._callback_service.send_success(
                    job_id=job_id,
                    callback_url=callback_url,
                    content=result.model_dump(mode="json"),
                )
            else:
                # Failure callback
                return await self._callback_service.send_failure(
                    job_id=job_id,
                    callback_url=callback_url,
                    error_message=error or "Unknown error",
                )

        except CallbackError as e:
            logger.error(
                "Callback delivery failed",
                job_id=job_id,
                url=callback_url,
                error=str(e),
            )
            return False

    async def create_and_execute(
        self,
        session: AsyncSession,
        request: JobRequest,
    ) -> tuple[JobResponse, ExtractedContent | None]:
        """
        Create a job and execute extraction synchronously.

        This is the main entry point for synchronous extraction
        (useful for CLI and testing).

        Args:
            session: Database session
            request: Job creation request

        Returns:
            Tuple of (JobResponse, ExtractedContent or None)
        """
        # Create job
        job_response = await self.create_job(session, request)

        # Execute extraction
        result = await self.execute_extraction(
            session,
            job_response.job_id,
            request,
        )

        # Update response with final status
        job = await JobRepository.get_by_id(session, job_response.job_id)
        job_response.status = job.status

        return job_response, result


async def execute_extraction_job(
    session: AsyncSession,
    engine: ScrapyEngine,
    callback_service: CallbackService,
    job_id: str,
    request: JobRequest,
) -> None:
    """
    Execute extraction job as a background task.

    This is the main entry point for FastAPI BackgroundTasks.
    Handles the complete workflow:
    1. Execute extraction
    2. Send callback
    3. Update final status

    Args:
        session: Database session
        engine: ScrapyEngine instance
        callback_service: Callback service instance
        job_id: Job ID to process
        request: Original job request
    """
    service = JobService(engine, callback_service)

    logger.info(
        "Background job started",
        job_id=job_id,
        url=request.url,
    )

    try:
        # Execute extraction
        result = await service.execute_extraction(session, job_id, request)

        # Send callback
        callback_sent = await service.send_callback(
            job_id=job_id,
            callback_url=str(request.callback_url),
            result=result,
            error=None if result else "Extraction failed",
        )

        if not callback_sent:
            logger.warning(
                "Callback delivery failed, job marked as completed but callback not delivered",
                job_id=job_id,
            )

        logger.info(
            "Background job completed",
            job_id=job_id,
            success=result is not None,
            callback_sent=callback_sent,
        )

    except Exception as e:
        logger.error(
            "Background job failed unexpectedly",
            job_id=job_id,
            error=str(e),
        )

        # Try to send failure callback
        try:
            await service.send_callback(
                job_id=job_id,
                callback_url=str(request.callback_url),
                result=None,
                error=str(e),
            )
        except Exception as callback_error:
            logger.error(
                "Failed to send failure callback",
                job_id=job_id,
                error=str(callback_error),
            )
