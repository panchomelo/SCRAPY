"""
Web content extractor using Playwright and BeautifulSoup.

Handles JavaScript-rendered pages with smart content cleaning
optimized for RAG consumption.
"""

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import Browser, Page, async_playwright
from playwright.async_api import TimeoutError as PlaywrightTimeout

from src.core.config import get_settings
from src.extractors.base import BaseExtractor, extraction_retry
from src.models.schemas import (
    ContentType,
    ExtractedContent,
    ExtractionSource,
    Metadata,
    WebExtractionConfig,
)
from src.utils.exceptions import WebExtractionError
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Default selectors to remove (ads, navigation, etc.)
DEFAULT_REMOVE_SELECTORS = [
    "script",
    "style",
    "noscript",
    "iframe",
    "nav",
    "header",
    "footer",
    "aside",
    ".ad",
    ".ads",
    ".advertisement",
    ".banner",
    "[role='banner']",
    "[role='navigation']",
    "[role='complementary']",
    ".sidebar",
    ".menu",
    ".nav",
    ".navigation",
    ".cookie",
    ".popup",
    ".modal",
    ".social-share",
    ".share-buttons",
    ".comments",
    "#comments",
]


class WebExtractor(BaseExtractor):
    """
    Extractor for web pages using Playwright + BeautifulSoup.

    Uses Playwright to handle JavaScript-rendered content and
    BeautifulSoup for HTML parsing and content cleaning.

    Features:
    - JavaScript rendering with configurable wait conditions
    - Smart content cleaning (removes ads, nav, scripts)
    - Metadata extraction (title, description, language)
    - Link extraction (optional)
    - Screenshot capture (optional, for debugging)

    Example:
        >>> extractor = WebExtractor({"wait_for_selector": ".content"})
        >>> content = await extractor.extract("https://example.com/article")
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self._browser: Browser | None = None

        # Parse config into typed object
        self._config = WebExtractionConfig(**self.config)

    async def _get_browser(self) -> Browser:
        """Get or create browser instance."""
        if self._browser is None or not self._browser.is_connected():
            settings = get_settings()
            playwright = await async_playwright().start()
            self._browser = await playwright.chromium.launch(
                headless=settings.playwright_headless_resolved,
            )
        return self._browser

    async def close(self) -> None:
        """Close browser and release resources."""
        if self._browser and self._browser.is_connected():
            await self._browser.close()
            self._browser = None

    async def validate_source(self, source: str) -> None:
        """Validate URL format."""
        await super().validate_source(source)

        parsed = urlparse(source)
        if parsed.scheme not in ("http", "https"):
            raise WebExtractionError(
                f"Invalid URL scheme: {parsed.scheme}. Must be http or https.",
                url=source,
            )
        if not parsed.netloc:
            raise WebExtractionError(
                "Invalid URL: missing domain",
                url=source,
            )

    @extraction_retry
    async def extract(self, source: str, **kwargs: Any) -> ExtractedContent:
        """
        Extract content from a web page.

        Args:
            source: URL to extract from
            **kwargs: Override config options

        Returns:
            ExtractedContent: Extracted web content

        Raises:
            WebExtractionError: If extraction fails
        """
        await self.validate_source(source)
        self._log_start(source)

        settings = get_settings()
        browser = await self._get_browser()
        page: Page | None = None

        try:
            page = await browser.new_page()

            # Navigate to page
            response = await page.goto(
                source,
                wait_until="domcontentloaded",
                timeout=settings.playwright_timeout,
            )

            if response and response.status >= 400:
                raise WebExtractionError(
                    f"HTTP error: {response.status}",
                    url=source,
                    status_code=response.status,
                )

            # Wait for specific selector if configured
            if self._config.wait_for_selector:
                try:
                    await page.wait_for_selector(
                        self._config.wait_for_selector,
                        timeout=self._config.wait_timeout,
                    )
                except PlaywrightTimeout:
                    logger.warning(
                        "Timeout waiting for selector",
                        selector=self._config.wait_for_selector,
                        url=source,
                    )

            # Get HTML content
            html = await page.content()

            # Parse and clean with BeautifulSoup
            soup = BeautifulSoup(html, "lxml")

            # Extract metadata before cleaning
            metadata = self._extract_metadata(soup, source)

            # Store raw HTML if in debug mode
            raw_html = html if self.debug else None

            # Clean the HTML
            self._remove_unwanted_elements(soup)

            # Extract main content
            content = self._extract_text_content(soup)

            # Extract links if requested
            if self._config.extract_links:
                links = self._extract_links(soup, source)
                metadata.custom["links"] = links

            result = ExtractedContent(
                source=ExtractionSource.WEB,
                source_url=source,
                content=content,
                content_type=ContentType.TEXT,
                metadata=metadata,
                raw_html=raw_html,
            )

            self._log_success(source, len(content))
            return result

        except PlaywrightTimeout as e:
            self._log_error(source, e)
            raise WebExtractionError(
                f"Timeout loading page: {source}",
                url=source,
            ) from e
        except WebExtractionError:
            raise
        except Exception as e:
            self._log_error(source, e)
            raise WebExtractionError(
                f"Failed to extract web content: {e}",
                url=source,
                details={"error_type": type(e).__name__},
            ) from e
        finally:
            if page:
                await page.close()

    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Metadata:
        """Extract metadata from HTML."""
        # Title
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        # Description from meta tags
        description = None
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            description = meta_desc["content"].strip()

        # OpenGraph fallbacks
        if not title:
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                title = og_title["content"].strip()

        if not description:
            og_desc = soup.find("meta", property="og:description")
            if og_desc and og_desc.get("content"):
                description = og_desc["content"].strip()

        # Language
        language = None
        html_tag = soup.find("html")
        if html_tag and html_tag.get("lang"):
            language = html_tag["lang"][:2].lower()

        # Author
        author = None
        meta_author = soup.find("meta", attrs={"name": "author"})
        if meta_author and meta_author.get("content"):
            author = meta_author["content"].strip()

        # Keywords as tags
        tags = []
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        if meta_keywords and meta_keywords.get("content"):
            tags = [k.strip() for k in meta_keywords["content"].split(",") if k.strip()]

        return Metadata(
            title=title,
            description=description,
            language=language,
            author=author,
            tags=tags[:10],  # Limit tags
            custom={"url": url},
        )

    def _remove_unwanted_elements(self, soup: BeautifulSoup) -> None:
        """Remove ads, navigation, scripts, and other unwanted elements."""
        selectors = self._config.remove_selectors or DEFAULT_REMOVE_SELECTORS

        for selector in selectors:
            for element in soup.select(selector):
                element.decompose()

    def _extract_text_content(self, soup: BeautifulSoup) -> str:
        """
        Extract clean text content from HTML.

        Preserves paragraph structure while removing excess whitespace.
        """
        # Try to find main content area
        main_content = (
            soup.find("main")
            or soup.find("article")
            or soup.find("div", class_=re.compile(r"content|main|body", re.I))
            or soup.find("body")
        )

        if not main_content:
            main_content = soup

        # Get text with proper spacing
        text = main_content.get_text(separator="\n", strip=True)

        # Clean up excessive whitespace while preserving paragraphs
        lines = []
        for line in text.split("\n"):
            line = line.strip()
            if line:
                # Collapse multiple spaces within line
                line = re.sub(r"\s+", " ", line)
                lines.append(line)

        # Join with double newline for paragraph separation
        content = "\n\n".join(lines)

        # Final cleanup
        content = re.sub(r"\n{3,}", "\n\n", content)

        return content.strip()

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[dict[str, str]]:
        """Extract all links with their text."""
        links = []
        seen_urls = set()

        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            # Make absolute URL
            absolute_url = urljoin(base_url, href)

            # Skip duplicates
            if absolute_url in seen_urls:
                continue
            seen_urls.add(absolute_url)

            text = a.get_text(strip=True)
            if text:
                links.append({"url": absolute_url, "text": text[:100]})

        return links[:50]  # Limit to 50 links
