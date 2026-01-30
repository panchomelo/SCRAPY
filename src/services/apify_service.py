"""
Apify service for social media scraping.

Provides a client for Apify actors to extract content
from platforms like Instagram, Twitter, LinkedIn, etc.
"""

from typing import Any

from apify_client import ApifyClient

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


class ApifyService:
    """
    Service for interacting with Apify API.

    Handles actor runs and result retrieval for social media scraping.

    Example:
        >>> service = ApifyService()
        >>> results = await service.run_actor(
        ...     "instagram_profile",
        ...     {"usernames": ["nasa"]}
        ... )
    """

    def __init__(self) -> None:
        settings = get_settings()

        if not settings.apify_api_token:
            raise ApifyServiceError(
                "APIFY_API_TOKEN not configured. Set it in .env to use social media extraction."
            )

        self._client = ApifyClient(settings.apify_api_token)
        self._logger = get_logger(self.__class__.__name__)

    async def run_actor(
        self,
        actor_name: str,
        input_data: dict[str, Any],
        timeout_secs: int = 300,
    ) -> list[dict[str, Any]]:
        """
        Run an Apify actor and return results.

        Args:
            actor_name: Actor name (key from APIFY_ACTORS or full actor ID)
            input_data: Input data for the actor
            timeout_secs: Maximum execution time in seconds

        Returns:
            List of result items from the actor

        Raises:
            ApifyServiceError: If actor run fails
        """
        # Resolve actor ID
        actor_id = APIFY_ACTORS.get(actor_name, actor_name)

        self._logger.info(
            "Starting Apify actor",
            actor=actor_id,
            input_keys=list(input_data.keys()),
        )

        try:
            # Run the actor
            run = self._client.actor(actor_id).call(
                run_input=input_data,
                timeout_secs=timeout_secs,
            )

            if not run:
                raise ApifyServiceError(
                    "Actor run returned no result",
                    actor_id=actor_id,
                )

            # Get results from default dataset
            dataset_id = run.get("defaultDatasetId")
            if not dataset_id:
                raise ApifyServiceError(
                    "No dataset ID in actor run result",
                    actor_id=actor_id,
                    run_id=run.get("id"),
                )

            items = list(self._client.dataset(dataset_id).iterate_items())

            self._logger.info(
                "Actor completed",
                actor=actor_id,
                items_count=len(items),
            )

            return items

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
