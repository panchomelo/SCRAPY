"""
Document extractors for PDF and Excel files.

Handles structured content extraction from documents
with support for tables and images.

Uses pandas 2.x with openpyxl for Excel extraction,
pdfplumber for PDF extraction with table detection.
"""

import base64
from pathlib import Path
from typing import Any

import pandas as pd
import pdfplumber
from pdfplumber.page import Page as PDFPage

from src.extractors.base import BaseExtractor, extraction_retry
from src.models.schemas import (
    ContentType,
    ExcelExtractionConfig,
    ExtractedContent,
    ExtractionSource,
    ImageData,
    Metadata,
    PDFExtractionConfig,
    TableData,
)
from src.utils.exceptions import DocumentExtractionError
from src.utils.files import temp_file
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Default settings for pdfplumber table extraction
# Based on pdfplumber documentation best practices
DEFAULT_TABLE_SETTINGS: dict[str, Any] = {
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
    "snap_tolerance": 3,
    "snap_x_tolerance": 3,
    "snap_y_tolerance": 3,
    "join_tolerance": 3,
    "join_x_tolerance": 3,
    "join_y_tolerance": 3,
    "edge_min_length": 3,
    "min_words_vertical": 3,
    "min_words_horizontal": 1,
    "intersection_tolerance": 3,
    "intersection_x_tolerance": 3,
    "intersection_y_tolerance": 3,
    "text_tolerance": 3,
    "text_x_tolerance": 3,
    "text_y_tolerance": 3,
}


class PDFExtractor(BaseExtractor):
    """
    Extractor for PDF documents using pdfplumber.

    Features:
    - Text extraction with layout preservation
    - Table extraction with structure detection
    - Image extraction (optional)
    - Page range selection
    - Metadata extraction

    Example:
        >>> extractor = PDFExtractor({"extract_tables": True})
        >>> content = await extractor.extract("/path/to/document.pdf")
        >>> # Or with bytes
        >>> content = await extractor.extract_bytes(pdf_bytes, "document.pdf")
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self._config = PDFExtractionConfig(**self.config)

    async def validate_source(self, source: str) -> None:
        """Validate PDF file path."""
        await super().validate_source(source)

        path = Path(source)
        if not path.exists():
            raise DocumentExtractionError(
                f"PDF file not found: {source}",
                file_path=source,
                file_type="pdf",
            )
        if path.suffix.lower() != ".pdf":
            raise DocumentExtractionError(
                f"Invalid file type: {path.suffix}. Expected .pdf",
                file_path=source,
                file_type="pdf",
            )

    @extraction_retry
    async def extract(self, source: str, **kwargs: Any) -> ExtractedContent:
        """
        Extract content from a PDF file.

        Args:
            source: Path to PDF file
            **kwargs: Override config options

        Returns:
            ExtractedContent: Extracted PDF content
        """
        await self.validate_source(source)
        self._log_start(source)

        try:
            with pdfplumber.open(source) as pdf:
                return self._process_pdf(pdf, source)
        except Exception as e:
            self._log_error(source, e)
            raise DocumentExtractionError(
                f"Failed to extract PDF content: {e}",
                file_path=source,
                file_type="pdf",
            ) from e

    async def extract_bytes(
        self,
        content: bytes,
        filename: str = "document.pdf",
    ) -> ExtractedContent:
        """
        Extract content from PDF bytes.

        Args:
            content: PDF file content as bytes
            filename: Original filename for metadata

        Returns:
            ExtractedContent: Extracted PDF content
        """
        self._log_start(filename)

        try:
            with temp_file(suffix=".pdf", content=content) as tmp_path:
                with pdfplumber.open(tmp_path) as pdf:
                    return self._process_pdf(pdf, filename)
        except Exception as e:
            self._log_error(filename, e)
            raise DocumentExtractionError(
                f"Failed to extract PDF content: {e}",
                file_path=filename,
                file_type="pdf",
            ) from e

    def _process_pdf(
        self,
        pdf: pdfplumber.PDF,
        source: str,
    ) -> ExtractedContent:
        """Process PDF and extract content."""
        # Determine page range
        start_page = 0
        end_page = len(pdf.pages)

        if self._config.page_range:
            start_page = max(0, self._config.page_range[0] - 1)  # Convert to 0-indexed
            end_page = min(len(pdf.pages), self._config.page_range[1])

        pages_to_process = pdf.pages[start_page:end_page]

        # Extract content
        text_parts: list[str] = []
        tables: list[TableData] = []
        images: list[ImageData] = []

        for page_num, page in enumerate(pages_to_process, start=start_page + 1):
            # Extract text with layout preservation for better structure
            page_text = page.extract_text(layout=True) or ""
            if page_text.strip():
                text_parts.append(f"--- Page {page_num} ---\n{page_text}")

            # Extract tables
            if self._config.extract_tables:
                page_tables = self._extract_tables(page, page_num)
                tables.extend(page_tables)

            # Extract images
            if self._config.extract_images:
                page_images = self._extract_images(page, page_num)
                images.extend(page_images)

        # Combine text
        content = "\n\n".join(text_parts)

        # Determine content type
        content_type = ContentType.TEXT
        if tables and not content.strip():
            content_type = ContentType.TABLE
        elif tables:
            content_type = ContentType.MIXED

        # Extract metadata
        metadata = self._extract_metadata(pdf, source, len(pages_to_process))

        # Count words
        metadata.word_count = len(content.split())

        result = ExtractedContent(
            source=ExtractionSource.PDF,
            source_url=source,
            content=content,
            content_type=content_type,
            metadata=metadata,
            tables=tables,
            images=images,
        )

        self._log_success(source, len(content))
        return result

    def _extract_tables(self, page: PDFPage, page_num: int) -> list[TableData]:
        """
        Extract tables from a PDF page.

        Uses pdfplumber's find_tables() for better table metadata,
        then extracts data with configurable settings.
        """
        tables: list[TableData] = []

        # Merge user settings with defaults
        table_settings = {**DEFAULT_TABLE_SETTINGS, **(self._config.table_settings or {})}

        # Use find_tables() to get table objects with metadata
        found_tables = page.find_tables(table_settings=table_settings)

        for idx, table_obj in enumerate(found_tables):
            # Extract table data with tolerance settings
            table_data = table_obj.extract()

            if not table_data or len(table_data) < 2:  # Need at least header + 1 row
                continue

            # First row as headers
            headers = [str(cell).strip() if cell else "" for cell in table_data[0]]

            # Rest as rows, cleaning None values
            rows: list[list[Any]] = []
            for row in table_data[1:]:
                cleaned_row = [cell if cell is not None else "" for cell in row]
                rows.append(cleaned_row)

            # Get table bbox for metadata
            bbox = table_obj.bbox

            tables.append(
                TableData(
                    name=f"Table {idx + 1} (Page {page_num})",
                    headers=headers,
                    rows=rows,
                    page_number=page_num,
                )
            )

            logger.debug(
                "Extracted table",
                page=page_num,
                table_idx=idx + 1,
                rows=len(rows),
                cols=len(headers),
                bbox=bbox,
            )

        return tables

    def _extract_images(self, page: PDFPage, page_num: int) -> list[ImageData]:
        """Extract images from a PDF page."""
        images: list[ImageData] = []

        try:
            page_images = page.images

            for idx, img in enumerate(page_images):
                # Get image data
                if "stream" in img:
                    img_data = img["stream"].get_data()

                    # Encode as base64
                    base64_data = base64.b64encode(img_data).decode("utf-8")

                    # Determine MIME type (simplified)
                    mime_type = "image/png"  # Default

                    images.append(
                        ImageData(
                            filename=f"page{page_num}_image{idx + 1}.png",
                            mime_type=mime_type,
                            base64_data=base64_data,
                            width=int(img.get("width", 0)) or None,
                            height=int(img.get("height", 0)) or None,
                            page_number=page_num,
                        )
                    )
        except Exception as e:
            logger.warning(
                "Failed to extract images from page",
                page=page_num,
                error=str(e),
            )

        return images

    def _extract_metadata(
        self,
        pdf: pdfplumber.PDF,
        source: str,
        page_count: int,
    ) -> Metadata:
        """Extract metadata from PDF."""
        info = pdf.metadata or {}

        return Metadata(
            title=info.get("Title") or Path(source).stem,
            author=info.get("Author"),
            page_count=page_count,
            custom={
                "creator": info.get("Creator"),
                "producer": info.get("Producer"),
                "creation_date": str(info.get("CreationDate", "")),
            },
        )


class ExcelExtractor(BaseExtractor):
    """
    Extractor for Excel files using pandas + openpyxl.

    Uses openpyxl as the Excel engine for .xlsx/.xlsm files.
    openpyxl features utilized:
    - data_only mode: Read calculated values instead of formulas
    - Workbook properties: Extract author, title, creation date metadata

    Features:
    - Multi-sheet extraction
    - Header detection
    - Data type preservation
    - Empty row/column filtering
    - Workbook metadata extraction

    Example:
        >>> extractor = ExcelExtractor({"sheet_names": ["Sales", "Inventory"]})
        >>> content = await extractor.extract("/path/to/workbook.xlsx")
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self._config = ExcelExtractionConfig(**self.config)

    async def validate_source(self, source: str) -> None:
        """Validate Excel file path."""
        await super().validate_source(source)

        path = Path(source)
        if not path.exists():
            raise DocumentExtractionError(
                f"Excel file not found: {source}",
                file_path=source,
                file_type="excel",
            )
        if path.suffix.lower() not in (".xlsx", ".xls", ".xlsm"):
            raise DocumentExtractionError(
                f"Invalid file type: {path.suffix}. Expected .xlsx, .xls, or .xlsm",
                file_path=source,
                file_type="excel",
            )

    @extraction_retry
    async def extract(self, source: str, **kwargs: Any) -> ExtractedContent:
        """
        Extract content from an Excel file.

        Args:
            source: Path to Excel file
            **kwargs: Override config options

        Returns:
            ExtractedContent: Extracted Excel content
        """
        await self.validate_source(source)
        self._log_start(source)

        try:
            return self._process_excel(source)
        except Exception as e:
            self._log_error(source, e)
            raise DocumentExtractionError(
                f"Failed to extract Excel content: {e}",
                file_path=source,
                file_type="excel",
            ) from e

    async def extract_bytes(
        self,
        content: bytes,
        filename: str = "workbook.xlsx",
    ) -> ExtractedContent:
        """
        Extract content from Excel bytes.

        Args:
            content: Excel file content as bytes
            filename: Original filename for metadata

        Returns:
            ExtractedContent: Extracted Excel content
        """
        self._log_start(filename)

        try:
            # Determine suffix from filename
            suffix = Path(filename).suffix or ".xlsx"

            with temp_file(suffix=suffix, content=content) as tmp_path:
                return self._process_excel(str(tmp_path), original_name=filename)
        except Exception as e:
            self._log_error(filename, e)
            raise DocumentExtractionError(
                f"Failed to extract Excel content: {e}",
                file_path=filename,
                file_type="excel",
            ) from e

    def _process_excel(
        self,
        source: str,
        original_name: str | None = None,
    ) -> ExtractedContent:
        """
        Process Excel file and extract content.

        Uses pandas 2.x best practices:
        - Context manager for ExcelFile
        - dtype_backend for better null handling
        - Explicit na_values for data cleanliness
        """
        tables: list[TableData] = []
        text_parts: list[str] = []

        # Use context manager for proper resource cleanup (pandas docs best practice)
        with pd.ExcelFile(source, engine="openpyxl") as excel_file:
            # Determine which sheets to process
            sheet_names = self._config.sheet_names or excel_file.sheet_names

            for sheet_name in sheet_names:
                if sheet_name not in excel_file.sheet_names:
                    logger.warning(f"Sheet not found: {sheet_name}")
                    continue

                # Read sheet with modern pandas options
                df = pd.read_excel(
                    excel_file,
                    sheet_name=sheet_name,
                    header=self._config.header_row,
                    nrows=self._config.max_rows,
                    # pandas 2.x: Better null handling with nullable dtypes
                    dtype_backend="numpy_nullable",
                    # Treat empty strings and common NA indicators as NA
                    na_values=["", "NA", "N/A", "null", "NULL", "None"],
                )

                # Skip empty sheets
                if df.empty:
                    continue

                # Clean data: remove rows that are entirely NA
                if self._config.skip_empty_rows:
                    df = df.dropna(how="all")

                # Convert to TableData using pandas modern methods
                headers = [str(col) for col in df.columns.tolist()]

                # Use to_dict for cleaner conversion, then extract values
                # This handles nullable dtypes better than .values.tolist()
                records = df.to_dict(orient="records")
                rows = [
                    [self._clean_cell_value(row.get(col)) for col in df.columns] for row in records
                ]

                tables.append(
                    TableData(
                        name=sheet_name,
                        headers=headers,
                        rows=rows,
                    )
                )

                # Add text summary
                text_parts.append(
                    f"Sheet: {sheet_name}\nColumns: {', '.join(headers)}\nRows: {len(rows)}"
                )

        # Combine text summaries
        content = "\n\n".join(text_parts)

        # Extract workbook metadata using openpyxl directly (per openpyxl docs)
        # This provides richer metadata than pandas alone
        workbook_meta = self._extract_workbook_metadata(source)

        # Metadata
        display_name = original_name or source
        metadata = Metadata(
            title=workbook_meta.get("title") or Path(display_name).stem,
            author=workbook_meta.get("creator"),
            custom={
                "sheet_count": len(tables),
                "sheets": [t.name for t in tables],
                **workbook_meta,
            },
        )

        result = ExtractedContent(
            source=ExtractionSource.EXCEL,
            source_url=display_name,
            content=content,
            content_type=ContentType.TABLE,
            metadata=metadata,
            tables=tables,
        )

        self._log_success(display_name, len(content))
        return result

    def _extract_workbook_metadata(self, source: str) -> dict[str, Any]:
        """
        Extract workbook metadata using openpyxl.

        Uses openpyxl's read_only mode for efficiency (per openpyxl docs).
        Extracts document properties like author, title, creation date.
        """
        try:
            # read_only=True: Memory efficient, just need properties
            # data_only=True would give calculated values but we don't need cells here
            wb = load_workbook(source, read_only=True, data_only=False)
            props = wb.properties

            metadata: dict[str, Any] = {}

            if props:
                if props.creator:
                    metadata["creator"] = props.creator
                if props.title:
                    metadata["title"] = props.title
                if props.subject:
                    metadata["subject"] = props.subject
                if props.description:
                    metadata["description"] = props.description
                if props.created:
                    metadata["created"] = str(props.created)
                if props.modified:
                    metadata["modified"] = str(props.modified)
                if props.lastModifiedBy:
                    metadata["last_modified_by"] = props.lastModifiedBy
                if props.keywords:
                    metadata["keywords"] = props.keywords

            wb.close()
            return metadata

        except Exception as e:
            logger.debug(f"Could not extract workbook metadata: {e}")
            return {}

    def _clean_cell_value(self, value: Any) -> Any:
        """Clean cell value for JSON serialization."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float, str, bool)):
            return value
        return str(value)
