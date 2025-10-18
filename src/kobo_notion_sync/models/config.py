"""Configuration model for kobo-notion-sync."""

import re
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class NotionConfig(BaseModel):
    """Notion integration settings."""

    database_id: str = Field(description="Notion database ID (UUID format)")
    workspace_name: str = Field(min_length=1, description="Workspace name for display")
    has_description_property: bool = Field(
        default=False, description="Whether Description property exists in database"
    )
    has_time_spent_property: bool = Field(
        default=False, description="Whether Time Spent property exists in database"
    )

    @field_validator("database_id")
    @classmethod
    def validate_database_id(cls, v: str) -> str:
        """Validate Notion database ID format (UUID with hyphens)."""
        # UUID format: 8-4-4-4-12 hex characters with hyphens
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        if not re.match(pattern, v, re.IGNORECASE):
            raise ValueError(
                "database_id must be a valid Notion UUID (format: 8-4-4-4-12 hex)"
            )
        return v


class KoboConfig(BaseModel):
    """Kobo device and cloud settings."""

    device_mount_path: Path = Field(description="Kobo device mount point")
    cloud_enabled: bool = Field(default=False, description="Enable Kobo cloud sync")
    cloud_email: Optional[str] = Field(
        default=None, description="Kobo cloud account email"
    )

    @field_validator("device_mount_path")
    @classmethod
    def validate_device_path(cls, v: Path) -> Path:
        """Ensure device_mount_path is absolute."""
        if not v.is_absolute():
            raise ValueError("device_mount_path must be an absolute path")
        return v

    @field_validator("cloud_email")
    @classmethod
    def validate_cloud_email(cls, v: Optional[str], info) -> Optional[str]:
        """Validate email format if cloud_enabled is true."""
        if info.data.get("cloud_enabled") and not v:
            raise ValueError("cloud_email required when cloud_enabled is true")
        if v:
            # Basic email validation
            email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            if not re.match(email_pattern, v):
                raise ValueError("cloud_email must be a valid email address")
        return v


class SyncConfig(BaseModel):
    """Sync scheduling configuration."""

    scheduled_enabled: bool = Field(
        default=False, description="Enable scheduled automatic sync"
    )
    scheduled_time: str = Field(
        default="09:00", description="Daily sync time in HH:MM format"
    )

    @field_validator("scheduled_time")
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        """Validate time format (HH:MM with valid hours 00-23, minutes 00-59)."""
        pattern = r"^([01]\d|2[0-3]):[0-5]\d$"
        if not re.match(pattern, v):
            raise ValueError(
                "scheduled_time must be in HH:MM format (24-hour) with valid hours (00-23) and minutes (00-59)"
            )
        return v


class LoggingConfig(BaseModel):
    """Logging and storage configuration."""

    level: str = Field(default="INFO", description="Log level")
    cache_dir: Optional[Path] = Field(default=None, description="Cache directory path")
    log_dir: Optional[Path] = Field(default=None, description="Log directory path")

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of DEBUG, INFO, WARNING, ERROR."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR"}
        if v not in valid_levels:
            raise ValueError(f"level must be one of: {', '.join(valid_levels)}")
        return v

    @field_validator("cache_dir", "log_dir")
    @classmethod
    def validate_directory_path(cls, v: Optional[Path]) -> Optional[Path]:
        """Ensure directory paths are absolute if provided."""
        if v is not None and not v.is_absolute():
            raise ValueError("Directory paths must be absolute")
        return v


class Configuration(BaseModel):
    """Complete configuration for kobo-notion-sync."""

    notion: NotionConfig
    kobo: KoboConfig
    sync: SyncConfig = Field(default_factory=SyncConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @property
    def cache_directory(self) -> Path:
        """Get cache directory, using default if not specified."""
        if self.logging.cache_dir:
            return self.logging.cache_dir
        return Path.home() / ".kobo-notion-sync" / "cache"

    @property
    def log_directory(self) -> Path:
        """Get log directory, using default if not specified."""
        if self.logging.log_dir:
            return self.logging.log_dir
        return Path.home() / ".kobo-notion-sync" / "logs"
