"""
File utilities for temporary file management and filename sanitization.

Provides safe handling of downloaded files with automatic cleanup
and cross-platform compatible filename generation.
"""

import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import unquote, urlparse

from slugify import slugify

from src.utils.exceptions import FileError
from src.utils.logging import get_logger

logger = get_logger(__name__)


def sanitize_filename(
    filename: str,
    max_length: int = 200,
    preserve_extension: bool = True,
) -> str:
    """
    Convert a filename to a safe, filesystem-compatible format.

    Handles problematic characters like :, &, spaces, accents, and emojis.

    Args:
        filename: Original filename (e.g., "Reporte Final: Ventas & Marketing 2026.pdf")
        max_length: Maximum length for the resulting filename
        preserve_extension: Whether to preserve the file extension

    Returns:
        Safe filename (e.g., "reporte-final-ventas-marketing-2026.pdf")

    Example:
        >>> sanitize_filename("Reporte Final: Ventas & Marketing 2026.pdf")
        'reporte-final-ventas-marketing-2026.pdf'
        >>> sanitize_filename("données_été_2026.xlsx")
        'donnees-ete-2026.xlsx'
    """
    if not filename:
        raise FileError("Filename cannot be empty", operation="sanitize")

    # Extract extension if needed
    extension = ""
    name = filename

    if preserve_extension and "." in filename:
        # Handle multiple dots (e.g., "file.backup.pdf")
        parts = filename.rsplit(".", 1)
        if len(parts) == 2 and len(parts[1]) <= 10:  # Reasonable extension length
            name, extension = parts
            extension = f".{extension.lower()}"

    # Slugify the name part
    safe_name = slugify(
        name,
        max_length=max_length - len(extension),
        lowercase=True,
        separator="-",
    )

    # Ensure we have a valid name
    if not safe_name:
        safe_name = "unnamed-file"

    return f"{safe_name}{extension}"


def extract_filename_from_url(url: str, default: str = "downloaded-file") -> str:
    """
    Extract and sanitize filename from a URL.

    Args:
        url: URL to extract filename from
        default: Default filename if extraction fails

    Returns:
        Sanitized filename

    Example:
        >>> extract_filename_from_url("https://example.com/docs/Report%202026.pdf")
        'report-2026.pdf'
    """
    try:
        parsed = urlparse(url)
        path = unquote(parsed.path)

        if path and "/" in path:
            filename = path.rsplit("/", 1)[-1]
            if filename and "." in filename:
                return sanitize_filename(filename)

        return sanitize_filename(default)
    except Exception:
        return sanitize_filename(default)


@contextmanager
def temp_file(
    suffix: str = "",
    prefix: str = "scrapy_",
    content: bytes | None = None,
) -> Generator[Path, None, None]:
    """
    Context manager for creating a temporary file with automatic cleanup.

    The file is automatically deleted when exiting the context,
    even if an exception occurs.

    Args:
        suffix: File suffix/extension (e.g., ".pdf")
        prefix: File prefix for identification
        content: Optional content to write to the file

    Yields:
        Path: Path to the temporary file

    Example:
        >>> with temp_file(suffix=".pdf", content=pdf_bytes) as path:
        ...     result = extract_pdf(path)
        ... # File is automatically deleted here

    Raises:
        FileError: If file creation or writing fails
    """
    tmp_path: Path | None = None

    try:
        # Create temp file (delete=False so we control deletion)
        with tempfile.NamedTemporaryFile(
            suffix=suffix,
            prefix=prefix,
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)

            if content:
                tmp.write(content)
                tmp.flush()

        logger.debug(
            "Created temporary file",
            path=str(tmp_path),
            size=len(content) if content else 0,
        )

        yield tmp_path

    except OSError as e:
        raise FileError(
            f"Failed to create temporary file: {e}",
            operation="create_temp",
            details={"suffix": suffix, "error": str(e)},
        ) from e

    finally:
        # Cleanup: delete the temp file
        if tmp_path and tmp_path.exists():
            try:
                tmp_path.unlink()
                logger.debug("Deleted temporary file", path=str(tmp_path))
            except OSError as e:
                logger.warning(
                    "Failed to delete temporary file",
                    path=str(tmp_path),
                    error=str(e),
                )


@contextmanager
def temp_directory(prefix: str = "scrapy_") -> Generator[Path, None, None]:
    """
    Context manager for creating a temporary directory with automatic cleanup.

    Useful when downloading multiple related files that need to be
    processed together.

    Args:
        prefix: Directory prefix for identification

    Yields:
        Path: Path to the temporary directory

    Example:
        >>> with temp_directory() as tmpdir:
        ...     pdf_path = tmpdir / "document.pdf"
        ...     pdf_path.write_bytes(pdf_bytes)
        ...     excel_path = tmpdir / "data.xlsx"
        ...     excel_path.write_bytes(excel_bytes)
        ...     # Process files...
        ... # Directory and all contents deleted here
    """
    tmpdir: Path | None = None

    try:
        tmpdir = Path(tempfile.mkdtemp(prefix=prefix))
        logger.debug("Created temporary directory", path=str(tmpdir))
        yield tmpdir

    finally:
        if tmpdir and tmpdir.exists():
            try:
                # Remove all contents and the directory
                for item in tmpdir.iterdir():
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        # Simple single-level cleanup
                        for subitem in item.iterdir():
                            subitem.unlink()
                        item.rmdir()
                tmpdir.rmdir()
                logger.debug("Deleted temporary directory", path=str(tmpdir))
            except OSError as e:
                logger.warning(
                    "Failed to fully cleanup temporary directory",
                    path=str(tmpdir),
                    error=str(e),
                )


def ensure_suffix(filename: str, suffix: str) -> str:
    """
    Ensure a filename has the specified suffix/extension.

    Args:
        filename: Original filename
        suffix: Required suffix (e.g., ".pdf")

    Returns:
        Filename with the correct suffix

    Example:
        >>> ensure_suffix("document", ".pdf")
        'document.pdf'
        >>> ensure_suffix("document.pdf", ".pdf")
        'document.pdf'
    """
    if not suffix.startswith("."):
        suffix = f".{suffix}"

    if not filename.lower().endswith(suffix.lower()):
        return f"{filename}{suffix}"

    return filename


def get_file_extension(filename: str) -> str | None:
    """
    Extract the file extension from a filename.

    Args:
        filename: Filename to extract extension from

    Returns:
        Extension with dot (e.g., ".pdf") or None if no extension

    Example:
        >>> get_file_extension("document.pdf")
        '.pdf'
        >>> get_file_extension("no-extension")
        None
    """
    if "." in filename:
        ext = filename.rsplit(".", 1)[-1].lower()
        if ext and len(ext) <= 10:  # Reasonable extension
            return f".{ext}"
    return None
