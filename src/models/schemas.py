"""
Pydantic schemas for extracted content.

Defines the unified output format for all extractors,
optimized for RAG pipeline consumption.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ExtractionSource(str, Enum):
    """Source type for extraction."""

    WEB = "web"
    PDF = "pdf"
    EXCEL = "excel"
    SOCIAL = "social"


class ContentType(str, Enum):
    """Type of extracted content."""

    TEXT = "text"
    TABLE = "table"
    IMAGE = "image"
    MIXED = "mixed"


class Metadata(BaseModel):
    """
    Metadata associated with extracted content.

    Provides context about the extraction for RAG indexing.
    """

    model_config = ConfigDict(extra="ignore", validate_default=True)

    title: str | None = Field(
        default=None,
        description="Document or page title",
    )
    author: str | None = Field(
        default=None,
        description="Content author if available",
    )
    description: str | None = Field(
        default=None,
        description="Brief description or summary",
    )
    language: str | None = Field(
        default=None,
        description="Detected content language (ISO 639-1)",
    )
    page_count: int | None = Field(
        default=None,
        ge=1,
        description="Number of pages (for documents)",
    )
    word_count: int | None = Field(
        default=None,
        ge=0,
        description="Approximate word count",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Extracted or inferred tags/keywords",
    )
    custom: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional custom metadata",
    )


class TableData(BaseModel):
    """
    Structured table data extracted from documents.

    Used for PDF tables and Excel sheets.
    """

    name: str | None = Field(
        default=None,
        description="Table or sheet name",
    )
    headers: list[str] = Field(
        default_factory=list,
        description="Column headers",
    )
    rows: list[list[Any]] = Field(
        default_factory=list,
        description="Table rows as list of lists",
    )
    page_number: int | None = Field(
        default=None,
        ge=1,
        description="Page number where table was found (PDFs)",
    )

    @property
    def row_count(self) -> int:
        """Number of data rows (excluding headers)."""
        return len(self.rows)

    @property
    def column_count(self) -> int:
        """Number of columns."""
        return len(self.headers) if self.headers else (len(self.rows[0]) if self.rows else 0)


class ImageData(BaseModel):
    """
    Image data extracted from documents.

    Images are stored as base64 for portability.
    """

    filename: str = Field(
        description="Generated filename for the image",
    )
    mime_type: str = Field(
        description="Image MIME type (e.g., image/png)",
    )
    base64_data: str = Field(
        description="Base64-encoded image data",
    )
    width: int | None = Field(
        default=None,
        ge=1,
        description="Image width in pixels",
    )
    height: int | None = Field(
        default=None,
        ge=1,
        description="Image height in pixels",
    )
    page_number: int | None = Field(
        default=None,
        ge=1,
        description="Page number where image was found",
    )
    caption: str | None = Field(
        default=None,
        description="Image caption if available",
    )


class ExtractedContent(BaseModel):
    """
    Unified output schema for all extractors.

    This is the main schema that gets sent to the RAG pipeline.
    Designed to be comprehensive yet flexible for different source types.

    Attributes:
        source: Type of extraction (web, pdf, excel, social)
        source_url: Original URL or file path
        content: Main text content (cleaned, ready for embedding)
        content_type: Type of content (text, table, mixed)
        metadata: Associated metadata for indexing
        tables: Extracted tables (if any)
        images: Extracted images (if any)
        raw_html: Original HTML (web only, for debugging)
        extracted_at: Timestamp of extraction
    """

    # Source information
    source: ExtractionSource = Field(
        description="Type of extraction performed",
    )
    source_url: str = Field(
        description="Original URL or file path",
    )

    # Main content
    content: str = Field(
        description="Main text content, cleaned and ready for embedding",
    )
    content_type: ContentType = Field(
        default=ContentType.TEXT,
        description="Type of content extracted",
    )

    # Metadata
    metadata: Metadata = Field(
        default_factory=Metadata,
        description="Content metadata for RAG indexing",
    )

    # Structured data
    tables: list[TableData] = Field(
        default_factory=list,
        description="Extracted tables (PDFs, Excel)",
    )
    images: list[ImageData] = Field(
        default_factory=list,
        description="Extracted images (PDFs)",
    )

    # Debug/raw data
    raw_html: str | None = Field(
        default=None,
        description="Original HTML content (web only)",
    )

    # Timestamp
    extracted_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp of extraction",
    )

    @property
    def has_tables(self) -> bool:
        """Check if content includes tables."""
        return len(self.tables) > 0

    @property
    def has_images(self) -> bool:
        """Check if content includes images."""
        return len(self.images) > 0

    def to_rag_document(self) -> dict[str, Any]:
        """
        Convert to a simplified format for RAG ingestion.

        Returns a dict with just the essential fields for embedding.
        """
        return {
            "content": self.content,
            "metadata": {
                "source": self.source.value,
                "source_url": self.source_url,
                "title": self.metadata.title,
                "extracted_at": self.extracted_at.isoformat(),
                "content_type": self.content_type.value,
                "has_tables": self.has_tables,
                "has_images": self.has_images,
                **self.metadata.custom,
            },
        }


class WebExtractionConfig(BaseModel):
    """Configuration for web extraction."""

    wait_for_selector: str | None = Field(
        default=None,
        description="CSS selector to wait for before extraction",
    )
    wait_timeout: int = Field(
        default=30000,
        ge=1000,
        le=120000,
        description="Timeout for wait_for_selector in ms",
    )
    remove_selectors: list[str] = Field(
        default_factory=lambda: [
            "script",
            "style",
            "nav",
            "header",
            "footer",
            "aside",
            "iframe",
            ".ad",
            ".advertisement",
            "[role='banner']",
            "[role='navigation']",
        ],
        description="CSS selectors to remove before extraction",
    )
    extract_links: bool = Field(
        default=False,
        description="Whether to extract and include links",
    )
    screenshot: bool = Field(
        default=False,
        description="Whether to capture a screenshot",
    )


class PDFExtractionConfig(BaseModel):
    """Configuration for PDF extraction."""

    extract_tables: bool = Field(
        default=True,
        description="Whether to extract tables",
    )
    extract_images: bool = Field(
        default=False,
        description="Whether to extract images",
    )
    page_range: tuple[int, int] | None = Field(
        default=None,
        description="Page range to extract (1-indexed, inclusive)",
    )
    table_settings: dict[str, Any] = Field(
        default_factory=dict,
        description="pdfplumber table extraction settings",
    )


class ExcelExtractionConfig(BaseModel):
    """Configuration for Excel extraction."""

    sheet_names: list[str] | None = Field(
        default=None,
        description="Specific sheets to extract (None = all)",
    )
    header_row: int = Field(
        default=0,
        ge=0,
        description="Row index containing headers (0-indexed)",
    )
    skip_empty_rows: bool = Field(
        default=True,
        description="Whether to skip empty rows",
    )
    max_rows: int | None = Field(
        default=None,
        ge=1,
        description="Maximum rows to extract per sheet",
    )
