"""
SQLAlchemy ORM models for database tables.

Defines the ExtractionJob model for tracking scraping jobs
with full lifecycle status and results storage.
"""

from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import (
    DateTime,
    Enum,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class JobStatus(str, PyEnum):
    """
    Status enum for extraction jobs.

    Lifecycle: PENDING → EXTRACTING → COMPLETED/FAILED
    """

    PENDING = "pending"
    EXTRACTING = "extracting"
    COMPLETED = "completed"
    FAILED = "failed"


class ExtractionSource(str, PyEnum):
    """
    Source type for extraction jobs.

    Determines which extractor to use.
    """

    WEB = "web"
    PDF = "pdf"
    EXCEL = "excel"
    SOCIAL = "social"


class ExtractionJob(Base):
    """
    ORM model for extraction jobs.

    Stores job metadata, status, and results for full traceability.
    Designed for querying by status and time range.

    Attributes:
        id: Unique job identifier (UUID string)
        status: Current job status
        source: Type of extraction (web, pdf, excel, social)
        source_url: URL or file path being extracted
        callback_url: Webhook URL for result delivery
        result: JSON-serialized extraction result (on success)
        error: Error message (on failure)
        created_at: Job creation timestamp
        updated_at: Last modification timestamp
        completed_at: Job completion timestamp (success or failure)
    """

    __tablename__ = "extraction_jobs"

    # Primary key - UUID as string for simplicity
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        comment="Unique job identifier (UUID)",
    )

    # Job configuration
    status: Mapped[str] = mapped_column(
        Enum(JobStatus, native_enum=False, length=20),
        nullable=False,
        default=JobStatus.PENDING,
        comment="Current job status",
    )
    source: Mapped[str] = mapped_column(
        Enum(ExtractionSource, native_enum=False, length=20),
        nullable=False,
        comment="Extraction source type",
    )
    source_url: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="URL or file path being extracted",
    )
    callback_url: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Webhook URL for result delivery",
    )

    # Results
    result: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="JSON-serialized extraction result",
    )
    error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="Error message if job failed",
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        comment="Job creation timestamp",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
        comment="Last modification timestamp",
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        comment="Job completion timestamp",
    )

    # Indexes for common queries
    __table_args__ = (
        Index("ix_extraction_jobs_status", "status"),
        Index("ix_extraction_jobs_created_at", "created_at"),
        Index("ix_extraction_jobs_status_created", "status", "created_at"),
    )

    def __repr__(self) -> str:
        return f"ExtractionJob(id={self.id!r}, status={self.status!r}, source={self.source!r})"
