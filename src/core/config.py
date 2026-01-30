"""
Application configuration using pydantic-settings.

Centralizes all environment variables and application settings.
Validates configuration at startup to fail fast on misconfiguration.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via .env file or environment variables.
    Environment variables take precedence over .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ===================
    # Application
    # ===================
    app_name: str = Field(
        default="Scrapy Engine", description="Application name for logging and identification"
    )
    debug: bool = Field(
        default=False, description="Enable debug mode (verbose logging, headful browser)"
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )

    # ===================
    # API Security
    # ===================
    api_key: str = Field(
        ...,  # Required
        min_length=32,
        description="API Key for webhook authentication (min 32 chars)",
    )

    # ===================
    # Database
    # ===================
    database_url: str = Field(
        default="sqlite+aiosqlite:///./scrapy.db", description="SQLAlchemy async database URL"
    )

    # ===================
    # Callback Configuration
    # ===================
    callback_timeout: int = Field(
        default=30, ge=5, le=120, description="Timeout in seconds for callback HTTP requests"
    )
    callback_max_retries: int = Field(
        default=3, ge=1, le=10, description="Maximum retry attempts for failed callbacks"
    )

    # ===================
    # Playwright Configuration
    # ===================
    playwright_headless: bool = Field(
        default=True, description="Run browser in headless mode (False for debugging)"
    )
    playwright_timeout: int = Field(
        default=30000,
        ge=5000,
        le=120000,
        description="Default timeout for Playwright operations in milliseconds",
    )

    # ===================
    # Apify Configuration
    # ===================
    apify_api_token: str | None = Field(
        default=None, description="Apify API token for social media scraping (optional)"
    )

    # ===================
    # Rate Limiting (optional)
    # ===================
    rate_limit_requests: int = Field(
        default=10, ge=1, description="Maximum requests per rate limit period"
    )
    rate_limit_period: int = Field(default=60, ge=1, description="Rate limit period in seconds")

    @field_validator("log_level", mode="before")
    @classmethod
    def uppercase_log_level(cls, v: str) -> str:
        """Ensure log level is uppercase."""
        if isinstance(v, str):
            return v.upper()
        return v

    @property
    def playwright_headless_resolved(self) -> bool:
        """
        Resolve headless mode: debug=True forces headful mode.

        This allows --debug flag to automatically show the browser.
        """
        if self.debug:
            return False
        return self.playwright_headless


@lru_cache
def get_settings() -> Settings:
    """
    Get cached application settings.

    Settings are loaded once and cached for performance.
    The cache is cleared on application restart.

    Returns:
        Settings: Validated application settings

    Raises:
        ValidationError: If required settings are missing or invalid
    """
    return Settings()
