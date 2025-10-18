"""Sync session model for tracking sync operations."""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class SyncMode(str, Enum):
    """Sync operation mode."""

    FULL = "full"  # Full sync with USB connection (metadata + highlights)
    METADATA_ONLY = "metadata_only"  # Metadata sync via Kobo cloud (no highlights)


class SyncStatus(str, Enum):
    """Sync operation result status."""

    SUCCESS = "success"  # All operations completed, no errors
    PARTIAL = "partial"  # Some operations succeeded, some failed
    FAILED = "failed"  # Critical failure, no highlights synced


class SyncSession(BaseModel):
    """
    Tracks a single sync operation execution.

    See data-model.md "Sync Result" entity for full definition.
    """

    sync_mode: SyncMode = Field(description="Type of sync operation")
    start_time: datetime = Field(description="Sync start timestamp")
    end_time: Optional[datetime] = Field(default=None, description="Sync completion timestamp")
    books_processed: int = Field(default=0, description="Total books examined")
    books_created: int = Field(default=0, description="New books added to Notion")
    books_updated: int = Field(default=0, description="Existing books updated")
    highlights_synced: int = Field(default=0, description="New highlights added")
    highlights_skipped: int = Field(default=0, description="Duplicates skipped (from cache)")
    cache_hits: int = Field(default=0, description="Highlights found in cache (deduplication)")
    cache_misses: int = Field(default=0, description="Highlights not in cache (new)")
    errors: List[str] = Field(default_factory=list, description="Error messages if any")

    @property
    def status(self) -> SyncStatus:
        """
        Determine sync status based on results.

        - SUCCESS: No errors occurred
        - PARTIAL: Some errors but at least some highlights synced
        - FAILED: Errors occurred and zero highlights synced
        """
        if not self.errors:
            return SyncStatus.SUCCESS
        elif self.highlights_synced > 0:
            return SyncStatus.PARTIAL
        else:
            return SyncStatus.FAILED

    @property
    def duration_seconds(self) -> float:
        """
        Calculate sync duration in seconds.

        Returns 0 if sync hasn't completed yet.
        """
        if not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    @property
    def deduplication_rate(self) -> float:
        """
        Calculate deduplication rate as percentage.

        Returns percentage of highlights that were cached (duplicates).
        Returns 0.0 if no highlights were processed.
        """
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return (self.cache_hits / total) * 100.0

    def summary_message(self) -> str:
        """
        Generate human-readable summary message.

        Used for notifications and CLI output.
        """
        if self.status == SyncStatus.SUCCESS:
            dedup_msg = ""
            if self.cache_hits > 0:
                dedup_msg = f", {self.cache_hits} duplicates skipped"
            return (
                f"Synced {self.cache_misses} new highlights (from {self.books_processed} books) "
                f"in {self.duration_seconds:.1f}s{dedup_msg}"
            )
        elif self.status == SyncStatus.PARTIAL:
            dedup_msg = ""
            if self.cache_hits > 0:
                dedup_msg = f", {self.cache_hits} duplicates skipped"
            return (
                f"Partial sync: {self.highlights_synced} highlights synced, "
                f"{len(self.errors)} errors occurred{dedup_msg}"
            )
        else:
            error_msg = self.errors[0] if self.errors else "Unknown error"
            return f"Sync failed: {error_msg}"

    def add_error(self, error: str) -> None:
        """Add error message to the session."""
        self.errors.append(error)

    def complete(self) -> None:
        """Mark sync session as complete with current timestamp."""
        self.end_time = datetime.now()

    def __str__(self) -> str:
        """Human-readable representation."""
        return f"SyncSession({self.sync_mode.value}, {self.status.value}): {self.summary_message()}"
