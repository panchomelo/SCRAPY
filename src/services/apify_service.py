"""
Apify service for social media scraping.

Provides an async client for Apify actors to extract content
from platforms like Instagram, Twitter, LinkedIn, etc.

Features:
    - ApifyClientAsync for native async/await support
    - ApifyApiError for granular error handling
    - Configurable retry behavior with exponential backoff
    - Automatic pagination with iterate_items()
"""

from typing import Any

from apify_client import ApifyClientAsync
from apify_client.errors import ApifyApiError

from src.core.config import get_settings
from src.extractors.base import BaseExtractor, extraction_retry
from src.models.schemas import (
    ContentType,
    ExtractedContent,
    ExtractionSource,
    Metadata,
)
from src.utils.exceptions import ApifyServiceError, SocialMediaExtractionError
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Common Apify actors for social media
APIFY_ACTORS = {
    "instagram_profile": "apify/instagram-profile-scraper",
    "instagram_post": "apify/instagram-post-scraper",
    "instagram_hashtag": "apify/instagram-hashtag-scraper",
    "twitter_profile": "quacker/twitter-scraper",
    "twitter_search": "quacker/twitter-search",
    "linkedin_profile": "anchor/linkedin-profile-scraper",
    "linkedin_company": "anchor/linkedin-company-scraper",
    "facebook_page": "apify/facebook-pages-scraper",
    "youtube_channel": "streamers/youtube-channel-scraper",
    "youtube_video": "bernardo/youtube-scraper",
    "tiktok_profile": "clockworks/tiktok-scraper",
}


# Default retry configuration
DEFAULT_MAX_RETRIES = 8
DEFAULT_MIN_DELAY_MS = 500
DEFAULT_TIMEOUT_SECS = 360


class ApifyService:
    """
    Async service for interacting with Apify API.

    Uses ApifyClientAsync for native async/await support with automatic
    retry handling and granular error management via ApifyApiError.

    Attributes:
        max_retries: Maximum retry attempts for failed requests.
        min_delay_ms: Minimum delay between retries in milliseconds.
        timeout_secs: Default request timeout in seconds.

    Example:
        >>> service = ApifyService()
        >>> results = await service.run_actor(
        ...     "instagram_profile",
        ...     {"usernames": ["nasa"]}
        ... )
    """

    def __init__(
        self,
        max_retries: int = DEFAULT_MAX_RETRIES,
        min_delay_ms: int = DEFAULT_MIN_DELAY_MS,
        timeout_secs: int = DEFAULT_TIMEOUT_SECS,
    ) -> None:
        settings = get_settings()

        if not settings.apify_api_token:
            raise ApifyServiceError(
                "APIFY_API_TOKEN not configured. Set it in .env to use social media extraction."
            )

        self._client = ApifyClientAsync(
            token=settings.apify_api_token,
            max_retries=max_retries,
            min_delay_between_retries_millis=min_delay_ms,
            timeout_secs=timeout_secs,
        )
        self._logger = get_logger(self.__class__.__name__)
        self._timeout_secs = timeout_secs

    async def run_actor(
        self,
        actor_name: str,
        input_data: dict[str, Any],
        timeout_secs: int | None = None,
        memory_mbytes: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Run an Apify actor asynchronously and return results.

        Uses ApifyClientAsync.actor().call() for native async execution
        with automatic retry handling on network errors, rate limits (429),
        and server errors (5xx).

        Args:
            actor_name: Actor name (key from APIFY_ACTORS or full actor ID)
            input_data: Input data for the actor
            timeout_secs: Maximum execution time (defaults to client timeout)
            memory_mbytes: Memory allocation for the actor run (optional)

        Returns:
            List of result items from the actor's default dataset

        Raises:
            ApifyServiceError: If actor run fails or returns no data
        """
        # Resolve actor ID
        actor_id = APIFY_ACTORS.get(actor_name, actor_name)
        effective_timeout = timeout_secs or self._timeout_secs

        self._logger.info(
            "Starting Apify actor",
            actor=actor_id,
            input_keys=list(input_data.keys()),
            timeout_secs=effective_timeout,
        )

        try:
            # Run the actor with async call()
            actor_client = self._client.actor(actor_id)
            run = await actor_client.call(
                run_input=input_data,
                timeout_secs=effective_timeout,
                memory_mbytes=memory_mbytes,
            )

            if not run:
                raise ApifyServiceError(
                    "Actor run returned no result",
                    actor_id=actor_id,
                )

            # Log run status
            run_status = run.get("status", "UNKNOWN")
            self._logger.debug(
                "Actor run completed",
                actor=actor_id,
                run_id=run.get("id"),
                status=run_status,
            )

            # Get results from default dataset
            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                raise ApifyServiceError(
                    "No dataset ID in actor run result",
                    actor_id=actor_id,
                    run_id=run.get("id"),
                )

            # Use async iterate_items() for automatic pagination
            dataset_client = self._client.dataset(dataset_id)
            items: list[dict[str, Any]] = []
            async for item in dataset_client.iterate_items():
                items.append(item)

            self._logger.info(
                "Actor completed",
                actor=actor_id,
                items_count=len(items),
                run_id=run.get("id"),
            )

            return items

        except ApifyApiError as e:
            # Granular handling of Apify-specific errors
            self._logger.error(
                "Apify API error",
                actor=actor_id,
                status_code=e.status_code,
                error_type=e.type,
                message=str(e),
            )
            if e.status_code == 404:
                raise ApifyServiceError(
                    f"Actor not found: {actor_id}",
                    actor_id=actor_id,
                ) from e
            elif e.status_code == 429:
                raise ApifyServiceError(
                    "Apify rate limit exceeded",
                    actor_id=actor_id,
                ) from e
            else:
                raise ApifyServiceError(
                    f"Apify API error ({e.status_code}): {e}",
                    actor_id=actor_id,
                ) from e
        except ApifyServiceError:
            raise
        except Exception as e:
            self._logger.error(
                "Apify actor failed",
                actor=actor_id,
                error=str(e),
            )
            raise ApifyServiceError(
                f"Failed to run Apify actor: {e}",
                actor_id=actor_id,
            ) from e

    def get_available_actors(self) -> dict[str, str]:
        """Get dictionary of available actor names and IDs."""
        return APIFY_ACTORS.copy()

    async def get_actor_info(self, actor_name: str) -> dict[str, Any] | None:
        """
        Get information about an actor.

        Args:
            actor_name: Actor name (key from APIFY_ACTORS or full actor ID)

        Returns:
            Actor info dict or None if not found
        """
        actor_id = APIFY_ACTORS.get(actor_name, actor_name)
        try:
            return await self._client.actor(actor_id).get()
        except ApifyApiError as e:
            if e.status_code == 404:
                return None
            raise

    async def close(self) -> None:
        """
        Close the async client connection.

        Should be called when done using the service to properly
        release resources.
        """
        # ApifyClientAsync handles cleanup internally
        # This method is provided for explicit resource management
        pass


class SocialMediaExtractor(BaseExtractor):
    """
    Extractor for social media content using Apify.

    Wraps ApifyService to provide consistent ExtractedContent output.

    Example:
        >>> extractor = SocialMediaExtractor({
        ...     "actor": "instagram_profile",
        ...     "actor_input": {"usernames": ["nasa"]}
        ... })
        >>> content = await extractor.extract("https://instagram.com/nasa")
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        super().__init__(config)

        settings = get_settings()
        if not settings.apify_api_token:
            self._service = None
            self._logger.warning("Apify token not configured. Social media extraction disabled.")
        else:
            self._service = ApifyService()

    async def validate_source(self, source: str) -> None:
        """Validate that Apify is configured."""
        await super().validate_source(source)

        if not self._service:
            raise SocialMediaExtractionError(
                "Apify is not configured. Set APIFY_API_TOKEN in .env",
                platform="apify",
            )

    @extraction_retry
    async def extract(self, source: str, **kwargs: Any) -> ExtractedContent:
        """
        Extract social media content.

        Args:
            source: URL to extract (used for metadata)
            **kwargs: Must include 'actor' and 'actor_input'

        Returns:
            ExtractedContent: Extracted social media content
        """
        await self.validate_source(source)
        self._log_start(source)

        actor = kwargs.get("actor") or self.config.get("actor")
        actor_input = kwargs.get("actor_input") or self.config.get("actor_input", {})

        if not actor:
            raise SocialMediaExtractionError(
                "No actor specified for social media extraction",
                platform="apify",
            )

        try:
            # Run the actor
            items = await self._service.run_actor(actor, actor_input)

            if not items:
                raise SocialMediaExtractionError(
                    "No results returned from Apify actor",
                    platform="apify",
                    actor_id=actor,
                )

            # Convert results to content
            content = self._format_results(items)

            # Build metadata
            metadata = Metadata(
                title=f"Social Media: {actor}",
                custom={
                    "actor": actor,
                    "items_count": len(items),
                    "source_url": source,
                },
            )

            result = ExtractedContent(
                source=ExtractionSource.SOCIAL,
                source_url=source,
                content=content,
                content_type=ContentType.TEXT,
                metadata=metadata,
            )

            self._log_success(source, len(content))
            return result

        except (ApifyServiceError, SocialMediaExtractionError):
            raise
        except Exception as e:
            self._log_error(source, e)
            raise SocialMediaExtractionError(
                f"Failed to extract social media content: {e}",
                platform="apify",
            ) from e

    def _format_results(self, items: list[dict[str, Any]]) -> str:
        """Format Apify results as readable text."""
        parts = []

        for idx, item in enumerate(items, 1):
            # Try to extract common fields
            text = item.get("text") or item.get("caption") or item.get("description") or ""
            author = item.get("ownerUsername") or item.get("author") or item.get("username") or ""
            date = item.get("timestamp") or item.get("date") or item.get("createdAt") or ""

            entry = f"--- Item {idx} ---\n"
            if author:
                entry += f"Author: {author}\n"
            if date:
                entry += f"Date: {date}\n"
            if text:
                entry += f"Content: {text}\n"

            # Add any other interesting fields
            for key in ["likes", "comments", "shares", "views", "followers"]:
                if key in item and item[key] is not None:
                    entry += f"{key.capitalize()}: {item[key]}\n"

            parts.append(entry)

        return "\n".join(parts)
