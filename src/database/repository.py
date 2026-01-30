"""
Repository pattern for ExtractionJob CRUD operations.

Provides a clean interface for database operations,
abstracting SQLAlchemy details from the service layer.
"""

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database.models import ExtractionJob, ExtractionSource, JobStatus
from src.utils.exceptions import DatabaseError, JobNotFoundError
from src.utils.logging import get_logger

logger = get_logger(__name__)


class JobRepository:
    """
    Repository for ExtractionJob database operations.

    All methods require an AsyncSession to be passed in,
    allowing the caller to control transaction boundaries.
    """

    @staticmethod
    async def create(
        session: AsyncSession,
        *,
        source: ExtractionSource,
        callback_url: str,
        source_url: str | None = None,
    ) -> ExtractionJob:
        """
        Create a new extraction job.

        Args:
            session: Database session
            source: Extraction source type
            callback_url: Webhook URL for results
            source_url: URL or path being extracted

        Returns:
            ExtractionJob: Created job with PENDING status

        Raises:
            DatabaseError: If creation fails
        """
        try:
            job = ExtractionJob(
                id=str(uuid4()),
                status=JobStatus.PENDING,
                source=source,
                source_url=source_url,
                callback_url=callback_url,
            )

            session.add(job)
            await session.flush()  # Get the ID without committing

            logger.info(
                "Job created",
                job_id=job.id,
                source=source,
                source_url=source_url,
            )

            return job

        except Exception as e:
            logger.error("Failed to create job", error=str(e))
            raise DatabaseError(
                f"Failed to create extraction job: {e}",
                operation="insert",
                table="extraction_jobs",
            ) from e

    @staticmethod
    async def get_by_id(
        session: AsyncSession,
        job_id: str,
    ) -> ExtractionJob:
        """
        Get a job by ID.

        Args:
            session: Database session
            job_id: Job UUID

        Returns:
            ExtractionJob: Found job

        Raises:
            JobNotFoundError: If job doesn't exist
        """
        result = await session.execute(select(ExtractionJob).where(ExtractionJob.id == job_id))
        job = result.scalar_one_or_none()

        if job is None:
            raise JobNotFoundError(job_id)

        return job

    @staticmethod
    async def get_by_id_or_none(
        session: AsyncSession,
        job_id: str,
    ) -> ExtractionJob | None:
        """
        Get a job by ID, returning None if not found.

        Args:
            session: Database session
            job_id: Job UUID

        Returns:
            ExtractionJob or None
        """
        result = await session.execute(select(ExtractionJob).where(ExtractionJob.id == job_id))
        return result.scalar_one_or_none()

    @staticmethod
    async def update_status(
        session: AsyncSession,
        job_id: str,
        status: JobStatus,
    ) -> ExtractionJob:
        """
        Update job status.

        Args:
            session: Database session
            job_id: Job UUID
            status: New status

        Returns:
            ExtractionJob: Updated job

        Raises:
            JobNotFoundError: If job doesn't exist
        """
        job = await JobRepository.get_by_id(session, job_id)
        job.status = status

        # Set completed_at for terminal states
        if status in (JobStatus.COMPLETED, JobStatus.FAILED):
            job.completed_at = datetime.now(UTC)

        await session.flush()

        logger.info("Job status updated", job_id=job_id, status=status)

        return job

    @staticmethod
    async def set_result(
        session: AsyncSession,
        job_id: str,
        result: str,
    ) -> ExtractionJob:
        """
        Set job result and mark as completed.

        Args:
            session: Database session
            job_id: Job UUID
            result: JSON-serialized result

        Returns:
            ExtractionJob: Updated job
        """
        job = await JobRepository.get_by_id(session, job_id)
        job.status = JobStatus.COMPLETED
        job.result = result
        job.completed_at = datetime.now(UTC)

        await session.flush()

        logger.info("Job completed", job_id=job_id)

        return job

    @staticmethod
    async def set_error(
        session: AsyncSession,
        job_id: str,
        error: str,
    ) -> ExtractionJob:
        """
        Set job error and mark as failed.

        Args:
            session: Database session
            job_id: Job UUID
            error: Error message

        Returns:
            ExtractionJob: Updated job
        """
        job = await JobRepository.get_by_id(session, job_id)
        job.status = JobStatus.FAILED
        job.error = error
        job.completed_at = datetime.now(UTC)

        await session.flush()

        logger.warning("Job failed", job_id=job_id, error=error)

        return job

    @staticmethod
    async def list_by_status(
        session: AsyncSession,
        status: JobStatus | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExtractionJob]:
        """
        List jobs, optionally filtered by status.

        Args:
            session: Database session
            status: Filter by status (None for all)
            limit: Maximum jobs to return
            offset: Number of jobs to skip

        Returns:
            List of ExtractionJob ordered by created_at desc
        """
        query = select(ExtractionJob).order_by(ExtractionJob.created_at.desc())

        if status is not None:
            query = query.where(ExtractionJob.status == status)

        query = query.limit(limit).offset(offset)

        result = await session.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def count_by_status(
        session: AsyncSession,
    ) -> dict[str, int]:
        """
        Get job counts grouped by status.

        Args:
            session: Database session

        Returns:
            Dict mapping status to count
        """
        result = await session.execute(
            select(
                ExtractionJob.status,
                func.count(ExtractionJob.id).label("count"),
            ).group_by(ExtractionJob.status)
        )

        counts = {status.value: 0 for status in JobStatus}
        for row in result:
            counts[row.status] = row.count

        return counts

    @staticmethod
    async def get_stats(
        session: AsyncSession,
    ) -> dict:
        """
        Get overall job statistics.

        Args:
            session: Database session

        Returns:
            Dict with total, counts by status, and success rate
        """
        counts = await JobRepository.count_by_status(session)
        total = sum(counts.values())

        completed = counts.get(JobStatus.COMPLETED.value, 0)
        failed = counts.get(JobStatus.FAILED.value, 0)
        finished = completed + failed

        success_rate = (completed / finished * 100) if finished > 0 else 0.0

        return {
            "total": total,
            "by_status": counts,
            "success_rate": round(success_rate, 2),
            "pending": counts.get(JobStatus.PENDING.value, 0),
            "processing": counts.get("extracting", 0),
            "completed": completed,
            "failed": failed,
        }

    @staticmethod
    async def list_recent(
        session: AsyncSession,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ExtractionJob]:
        """
        List recent jobs ordered by creation date.

        Args:
            session: Database session
            limit: Maximum jobs to return
            offset: Number of jobs to skip

        Returns:
            List of ExtractionJob ordered by created_at desc
        """
        query = (
            select(ExtractionJob)
            .order_by(ExtractionJob.created_at.desc())
            .limit(limit)
            .offset(offset)
        )

        result = await session.execute(query)
        return list(result.scalars().all())

    @staticmethod
    async def delete(
        session: AsyncSession,
        job_id: str,
    ) -> bool:
        """
        Delete a job by ID.

        Args:
            session: Database session
            job_id: Job UUID

        Returns:
            True if deleted, False if not found
        """
        job = await JobRepository.get_by_id_or_none(session, job_id)

        if job is None:
            return False

        await session.delete(job)
        await session.flush()

        logger.info("Job deleted", job_id=job_id)

        return True
