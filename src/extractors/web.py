"""
Web content extractor using Playwright and BeautifulSoup.

Handles JavaScript-rendered pages with smart content cleaning
optimized for RAG consumption.

Uses lxml as BeautifulSoup parser for performance (faster than html.parser).
"""

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from lxml_html_clean import Cleaner  # Standalone package since lxml 5.2.0
from playwright.async_api import Browser, BrowserContext, Page, Playwright, async_playwright
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

# lxml Cleaner for robust HTML sanitization (per lxml docs best practices)
# This removes scripts, styles, and dangerous elements at the lxml level
# before BeautifulSoup further cleans the content
_html_cleaner = Cleaner(
    scripts=True,  # Remove <script> tags
    javascript=True,  # Remove javascript: links
    comments=True,  # Remove HTML comments
    style=True,  # Remove <style> tags
    inline_style=False,  # Keep inline styles (may contain layout info)
    links=False,  # Keep <link> (may be needed for metadata)
    meta=False,  # Keep <meta> (needed for metadata extraction)
    page_structure=False,  # Keep <html>, <head>, <body>
    processing_instructions=True,  # Remove <?...?>
    remove_unknown_tags=False,  # Keep custom elements
    safe_attrs_only=False,  # Keep all attributes
    forms=False,  # Keep forms (may contain content)
    annoying_tags=False,  # Keep <blink>, <marquee> (rare, but might have text)
    kill_tags=["noscript", "iframe"],  # Completely remove these
)


class WebExtractor(BaseExtractor):
    """
    Extractor for web pages using Playwright + BeautifulSoup.

    Uses Playwright to handle JavaScript-rendered content,
    lxml.html.clean for sanitization, and BeautifulSoup for
    HTML parsing and content cleaning.

    Features:
    - JavaScript rendering with configurable wait conditions
    - Smart content cleaning (removes ads, nav, scripts via lxml + BS4)
    - Metadata extraction (title, description, language)
    - Link extraction (optional)
    - Screenshot capture (optional, for debugging)

    Example:
        >>> extractor = WebExtractor({"wait_for_selector": ".content"})
        >>> content = await extractor.extract("https://example.com/article")
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

        # Parse config into typed object
        self._config = WebExtractionConfig(**self.config)

    async def _ensure_browser(self) -> BrowserContext:
        """
        Ensure browser and context are initialized.

        Returns a browser context for page isolation.
        """
        settings = get_settings()

        if self._playwright is None:
            self._playwright = await async_playwright().start()

        if self._browser is None or not self._browser.is_connected():
            self._browser = await self._playwright.chromium.launch(
                headless=settings.playwright_headless_resolved,
            )

        if self._context is None:
            self._context = await self._browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
            # Set default timeout for all operations in this context
            self._context.set_default_timeout(settings.playwright_timeout)

        return self._context

    async def close(self) -> None:
        """
        Close browser context, browser, and Playwright.

        Releases all resources in the correct order.
        """
        if self._context:
            await self._context.close()
            self._context = None

        if self._browser and self._browser.is_connected():
            await self._browser.close()
            self._browser = None

        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

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
        context = await self._ensure_browser()
        page: Page | None = None

        try:
            page = await context.new_page()

            # Set navigation timeout specifically for goto
            page.set_default_navigation_timeout(settings.playwright_timeout)

            # Navigate to page
            response = await page.goto(
                source,
                wait_until="domcontentloaded",
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

            # Pre-clean with lxml Cleaner (removes scripts, comments, etc.)
            # This is more robust than manual decompose() for security-critical elements
            try:
                from lxml import html as lxml_html

                doc = lxml_html.document_fromstring(html)
                _html_cleaner(doc)
                html = lxml_html.tostring(doc, encoding="unicode")
            except Exception:
                # If lxml cleaning fails, continue with raw HTML
                # BeautifulSoup will still do its own cleaning
                pass

            # Parse with BeautifulSoup using lxml parser (fastest)
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

            # Capture screenshot if requested (useful for debugging)
            screenshot_data = None
            if self._config.screenshot:
                screenshot_bytes = await page.screenshot(full_page=True)
                import base64

                screenshot_data = base64.b64encode(screenshot_bytes).decode("utf-8")
                metadata.custom["screenshot"] = screenshot_data

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
        """
        Extract metadata from HTML.

        Uses CSS selectors for cleaner, more maintainable code.
        Falls back through multiple sources (meta tags, OpenGraph, etc.)
        """
        # Title: try multiple sources
        title = None
        if soup.title and soup.title.string:
            title = soup.title.string.strip()

        # Fallback to OpenGraph title
        if not title:
            og_title = soup.select_one('meta[property="og:title"]')
            if og_title and og_title.get("content"):
                title = og_title["content"].strip()

        # Description: meta description or OpenGraph
        description = None
        meta_desc = soup.select_one('meta[name="description"]')
        if meta_desc and meta_desc.get("content"):
            description = meta_desc["content"].strip()

        if not description:
            og_desc = soup.select_one('meta[property="og:description"]')
            if og_desc and og_desc.get("content"):
                description = og_desc["content"].strip()

        # Language from html tag
        language = None
        html_tag = soup.select_one("html[lang]")
        if html_tag:
            language = html_tag["lang"][:2].lower()

        # Author
        author = None
        meta_author = soup.select_one('meta[name="author"]')
        if meta_author and meta_author.get("content"):
            author = meta_author["content"].strip()

        # Keywords as tags
        tags: list[str] = []
        meta_keywords = soup.select_one('meta[name="keywords"]')
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
        Uses CSS selectors for finding main content area.
        """
        # Try to find main content area using CSS selectors
        # Priority: <main> > <article> > div with content class > <body>
        main_content = (
            soup.select_one("main")
            or soup.select_one("article")
            or soup.select_one("div[class*='content']")
            or soup.select_one("div[class*='main']")
            or soup.select_one("div[class*='body']")
            or soup.select_one("body")
            or soup
        )

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
        """
        Extract all links with their text.

        Uses CSS selector for consistency with rest of codebase.
        """
        links: list[dict[str, str]] = []
        seen_urls: set[str] = set()

        # Use CSS selector for links with href attribute
        for a in soup.select("a[href]"):
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
