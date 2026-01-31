"""
Pydantic schemas for job management and API contracts.

Defines request/response models for the webhook API
and callback payloads for the RAG service.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class JobStatus(str, Enum):
    """
    Status enum for extraction jobs.

    Lifecycle: PENDING → EXTRACTING → COMPLETED/FAILED
    """

    PENDING = "pending"
    EXTRACTING = "extracting"
    COMPLETED = "completed"
    FAILED = "failed"


class ExtractionSource(str, Enum):
    """Source type for extraction jobs."""

    WEB = "web"
    PDF = "pdf"
    EXCEL = "excel"
    SOCIAL = "social"


class JobRequest(BaseModel):
    """
    Request schema for creating an extraction job.

    Sent by the RAG service to initiate extraction.

    Example:
        {
            "callback_url": "https://rag-api.example.com/webhooks/scraper",
            "source": "web",
            "url": "https://example.com/article",
            "config": {"wait_for_selector": ".content"}
        }
    """

    model_config = ConfigDict(extra="forbid", validate_default=True)

    callback_url: HttpUrl = Field(
        description="Webhook URL to receive extraction results",
    )
    source: ExtractionSource = Field(
        description="Type of extraction to perform",
    )
    url: str | None = Field(
        default=None,
        description="URL to extract (for web/social sources)",
    )
    file_content: str | None = Field(
        default=None,
        description="Base64-encoded file content (for pdf/excel)",
    )
    file_name: str | None = Field(
        default=None,
        description="Original filename (for pdf/excel)",
    )
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific extraction configuration",
    )

    @model_validator(mode="after")
    def validate_source_requirements(self) -> Self:
        """Validate that appropriate fields are set for source type."""
        if self.source in (ExtractionSource.WEB, ExtractionSource.SOCIAL):
            if not self.url:
                raise ValueError(f"url is required for {self.source.value} extraction")
        elif self.source in (ExtractionSource.PDF, ExtractionSource.EXCEL):
            if not self.file_content and not self.url:
                raise ValueError(
                    f"file_content or url is required for {self.source.value} extraction"
                )
        return self


class JobResponse(BaseModel):
    """
    Response schema after creating an extraction job.

    Returned immediately with 202 Accepted status.

    Example:
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "pending",
            "message": "Job queued successfully. Results will be sent to callback URL."
        }
    """

    model_config = ConfigDict(extra="ignore")

    job_id: str = Field(
        description="Unique job identifier (UUID)",
    )
    status: JobStatus = Field(
        default=JobStatus.PENDING,
        description="Current job status",
    )
    message: str = Field(
        default="Job queued successfully. Results will be sent to callback URL.",
        description="Human-readable status message",
    )


class JobStatusResponse(BaseModel):
    """
    Response schema for job status queries.

    Used by GET /api/v1/jobs/{job_id} endpoint.

    Example:
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "completed",
            "source": "web",
            "source_url": "https://example.com/article",
            "created_at": "2026-01-30T12:00:00Z",
            "completed_at": "2026-01-30T12:00:05Z",
            "result": {...},
            "error": null
        }
    """

    job_id: str = Field(
        description="Unique job identifier",
    )
    status: JobStatus = Field(
        description="Current job status",
    )
    source: ExtractionSource = Field(
        description="Extraction source type",
    )
    source_url: str | None = Field(
        default=None,
        description="URL or file path being extracted",
    )
    callback_url: str = Field(
        description="Webhook URL for results",
    )
    created_at: datetime = Field(
        description="Job creation timestamp",
    )
    updated_at: datetime | None = Field(
        default=None,
        description="Last update timestamp",
    )
    completed_at: datetime | None = Field(
        default=None,
        description="Completion timestamp (if finished)",
    )
    result: dict[str, Any] | None = Field(
        default=None,
        description="Extraction result (if completed)",
    )
    error: str | None = Field(
        default=None,
        description="Error message (if failed)",
    )


class CallbackPayload(BaseModel):
    """
    Payload sent to the callback URL when job completes.

    This is what the RAG service receives via webhook.

    Example (success):
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "completed",
            "result": {
                "source": "web",
                "source_url": "https://example.com",
                "content": "Extracted text...",
                "metadata": {...}
            },
            "error": null,
            "completed_at": "2026-01-30T12:00:05Z"
        }

    Example (failure):
        {
            "job_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "failed",
            "result": null,
            "error": "Timeout while loading page",
            "completed_at": "2026-01-30T12:00:35Z"
        }
    """

    model_config = ConfigDict(extra="ignore")

    job_id: str = Field(
        description="Unique job identifier",
    )
    status: JobStatus = Field(
        description="Final job status (completed or failed)",
    )
    result: dict[str, Any] | None = Field(
        default=None,
        description="Extraction result (ExtractedContent as dict)",
    )
    error: str | None = Field(
        default=None,
        description="Error message if job failed",
    )
    completed_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Completion timestamp",
    )

    @classmethod
    def success(
        cls,
        job_id: str,
        result: dict[str, Any],
    ) -> Self:
        """Create a success callback payload."""
        return cls(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            result=result,
            error=None,
        )

    @classmethod
    def failure(
        cls,
        job_id: str,
        error: str,
    ) -> Self:
        """Create a failure callback payload."""
        return cls(
            job_id=job_id,
            status=JobStatus.FAILED,
            result=None,
            error=error,
        )


class JobListResponse(BaseModel):
    """
    Response schema for listing jobs.

    Used by GET /api/v1/jobs endpoint.
    """

    jobs: list[JobStatusResponse] = Field(
        description="List of jobs",
    )
    total: int = Field(
        description="Total number of jobs matching filters",
    )
    limit: int = Field(
        description="Maximum jobs returned",
    )
    offset: int = Field(
        description="Number of jobs skipped",
    )


class JobStatsResponse(BaseModel):
    """
    Response schema for job statistics.

    Used by GET /api/v1/stats endpoint.
    """

    total: int = Field(
        description="Total number of jobs",
    )
    pending: int = Field(
        default=0,
        description="Number of pending jobs",
    )
    processing: int = Field(
        default=0,
        description="Number of processing jobs",
    )
    completed: int = Field(
        default=0,
        description="Number of completed jobs",
    )
    failed: int = Field(
        default=0,
        description="Number of failed jobs",
    )


class HealthResponse(BaseModel):
    """Response schema for health check endpoint."""

    status: str = Field(
        default="healthy",
        description="Service status",
    )
    version: str = Field(
        description="API version",
    )
    database: str = Field(
        default="connected",
        description="Database connection status",
    )
