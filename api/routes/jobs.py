"""
Job management API routes.

Provides endpoints for:
- POST /scrape - Create and queue extraction job
- GET /jobs/{job_id} - Get job status
- GET /jobs - List jobs with filtering
- GET /stats - Get job statistics
"""

from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, status

from api.dependencies import ApiKeyDep, CallbackServiceDep, DbSessionDep, EngineDep
from src.database.models import JobStatus
from src.database.repository import JobRepository
from src.models.jobs import (
    JobListResponse,
    JobRequest,
    JobResponse,
    JobStatsResponse,
    JobStatusResponse,
)
from src.services.job_service import JobService, execute_extraction_job
from src.utils.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Jobs"])


@router.post(
    "/scrape",
    response_model=JobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create extraction job",
    description="Queue a new extraction job. Results will be sent to the callback URL.",
)
async def create_extraction_job(
    request: JobRequest,
    background_tasks: BackgroundTasks,
    session: DbSessionDep,
    engine: EngineDep,
    callback_service: CallbackServiceDep,
    _api_key: ApiKeyDep,
) -> JobResponse:
    """
    Create and queue a new extraction job.

    The job is created immediately and processing happens in the background.
    Results are sent to the callback_url when extraction completes.

    Args:
        request: Job creation request with source, URL/content, and callback
        background_tasks: FastAPI background task manager
        session: Database session
        engine: ScrapyEngine instance
        callback_service: Callback service instance
        _api_key: Validated API key (dependency)

    Returns:
        JobResponse with job_id and pending status
    """
    logger.info(
        "Creating extraction job",
        source=request.source.value,
        url=request.url,
        has_file=request.file_content is not None,
    )

    # Create job service
    job_service = JobService(engine, callback_service)

    # Create job in database
    job_response = await job_service.create_job(session, request)

    # Queue background extraction task
    background_tasks.add_task(
        execute_extraction_job,
        session=session,
        engine=engine,
        callback_service=callback_service,
        job_id=job_response.job_id,
        request=request,
    )

    logger.info("Job queued", job_id=job_response.job_id)

    return job_response


@router.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    summary="Get job status",
    description="Retrieve the current status and result of an extraction job.",
)
async def get_job_status(
    job_id: str,
    session: DbSessionDep,
    _api_key: ApiKeyDep,
) -> JobStatusResponse:
    """
    Get the status of an extraction job.

    Args:
        job_id: UUID of the job to query
        session: Database session
        _api_key: Validated API key

    Returns:
        JobStatusResponse with current status and result if completed

    Raises:
        HTTPException: 404 if job not found
    """
    job = await JobRepository.get_by_id_or_none(session, job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    return JobStatusResponse(
        job_id=job.id,
        status=job.status,
        source=job.source,
        source_url=job.source_url,
        callback_url=job.callback_url,
        created_at=job.created_at,
        completed_at=job.completed_at,
        result=job.result,
        error=job.error,
    )


@router.get(
    "/jobs",
    response_model=JobListResponse,
    summary="List jobs",
    description="List extraction jobs with optional status filtering.",
)
async def list_jobs(
    session: DbSessionDep,
    _api_key: ApiKeyDep,
    status_filter: Annotated[
        JobStatus | None,
        Query(alias="status", description="Filter by job status"),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="Maximum number of jobs to return"),
    ] = 20,
    offset: Annotated[
        int,
        Query(ge=0, description="Number of jobs to skip"),
    ] = 0,
) -> JobListResponse:
    """
    List extraction jobs with pagination and filtering.

    Args:
        session: Database session
        _api_key: Validated API key
        status_filter: Optional status to filter by
        limit: Maximum jobs to return (1-100)
        offset: Number of jobs to skip

    Returns:
        JobListResponse with list of jobs and count
    """
    if status_filter:
        jobs = await JobRepository.list_by_status(
            session,
            status_filter,
            limit=limit,
            offset=offset,
        )
    else:
        jobs = await JobRepository.list_recent(
            session,
            limit=limit,
            offset=offset,
        )

    # Convert to response models
    job_responses = [
        JobStatusResponse(
            job_id=job.id,
            status=job.status,
            source=job.source,
            source_url=job.source_url,
            callback_url=job.callback_url,
            created_at=job.created_at,
            completed_at=job.completed_at,
            result=None,  # Don't include full result in list
            error=job.error,
        )
        for job in jobs
    ]

    return JobListResponse(
        jobs=job_responses,
        total=len(job_responses),
        limit=limit,
        offset=offset,
    )


@router.get(
    "/stats",
    response_model=JobStatsResponse,
    summary="Get job statistics",
    description="Get aggregate statistics about extraction jobs.",
)
async def get_job_stats(
    session: DbSessionDep,
    _api_key: ApiKeyDep,
) -> JobStatsResponse:
    """
    Get aggregate job statistics.

    Args:
        session: Database session
        _api_key: Validated API key

    Returns:
        JobStatsResponse with counts by status
    """
    stats = await JobRepository.get_stats(session)

    return JobStatsResponse(
        total=stats.get("total", 0),
        pending=stats.get("pending", 0),
        processing=stats.get("processing", 0),
        completed=stats.get("completed", 0),
        failed=stats.get("failed", 0),
    )


@router.delete(
    "/jobs/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel/delete job",
    description="Cancel a pending job or delete a completed job.",
)
async def delete_job(
    job_id: str,
    session: DbSessionDep,
    _api_key: ApiKeyDep,
) -> None:
    """
    Cancel or delete an extraction job.

    Pending jobs are cancelled. Completed/failed jobs are deleted.
    Processing jobs cannot be cancelled.

    Args:
        job_id: UUID of the job
        session: Database session
        _api_key: Validated API key

    Raises:
        HTTPException: 404 if not found, 409 if processing
    """
    job = await JobRepository.get_by_id_or_none(session, job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job {job_id} not found",
        )

    if job.status == JobStatus.PROCESSING:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot cancel a job that is currently processing",
        )

    await JobRepository.delete(session, job_id)
    await session.commit()

    logger.info("Job deleted", job_id=job_id)
