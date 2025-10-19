"""Book model representing a book from Kobo library."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator

from kobo_notion_sync.lib.time_formatter import format_time_spent


class Book(BaseModel):
    """
    Represents a book in the user's Kobo library with synchronization state.

    See data-model.md for full entity definition and validation rules.
    """

    kobo_content_id: str = Field(min_length=1, description="Kobo ContentID (filename only)")
    title: str = Field(min_length=1, description="Book title")
    author: str = Field(min_length=1, description="Author name")
    isbn: Optional[str] = Field(default=None, description="ISBN-10 or ISBN-13")
    publisher: Optional[str] = Field(default=None, description="Publisher name")
    description: Optional[str] = Field(default=None, description="Book summary (HTML)")
    time_spent_reading: Optional[int] = Field(
        default=None, ge=0, description="Reading time in minutes"
    )
    read_status: int = Field(ge=0, le=2, description="Kobo ReadStatus: 0=Not Started, 1=Reading, 2=Finished")
    percent_read: float = Field(
        ge=0.0, le=100.0, description="Reading progress percentage"
    )
    date_last_read: Optional[datetime] = Field(
        default=None, description="Last time book was opened"
    )
    date_started: Optional[datetime] = Field(
        default=None, description="Date when book was first opened (for reading duration)"
    )
    date_finished: Optional[datetime] = Field(
        default=None, description="Date when book was marked as finished (read_status=2)"
    )
    content_type: int = Field(description="ContentType (must be 6 for EPUB ebooks)")
    cover_image_url: Optional[str] = Field(
        default=None, description="Retrieved cover image URL"
    )
    notion_page_id: Optional[str] = Field(
        default=None, description="Notion database entry ID"
    )
    last_sync_time: Optional[datetime] = Field(
        default=None, description="Last successful sync timestamp"
    )

    @field_validator("isbn")
    @classmethod
    def validate_isbn(cls, v: Optional[str]) -> Optional[str]:
        """Validate ISBN is 10 or 13 characters if present."""
        if v and not (len(v) == 10 or len(v) == 13):
            raise ValueError("ISBN must be 10 or 13 characters")
        return v

    @field_validator("content_type")
    @classmethod
    def validate_content_type(cls, v: int) -> int:
        """Validate ContentType equals 6 (EPUB ebooks only)."""
        if v != 6:
            raise ValueError(
                "content_type must equal 6 (EPUB ebooks). "
                f"Got {v} (audiobooks=9, articles=899 are excluded)"
            )
        return v

    @property
    def progress_code(self) -> str:
        """
        Derive Notion "Status" from Kobo read state.

        Status Determination:
        - "New": percent_read == 0% (purchased but not opened)
        - "Reading": 0% < percent_read < 100% AND read_status != 2
        - "Finished": read_status == 2 (authoritative indicator, regardless of progress %)

        This is the authoritative mapping per data-model.md "Book Status Lifecycle".
        Note: read_status=2 is the source of truth for finished books (Kobo's official status).
        """
        if self.percent_read == 0.0:
            return "New"
        elif self.read_status == 2:
            return "Finished"
        else:
            return "Reading"

    @property
    def time_spent_formatted(self) -> Optional[str]:
        """
        Get formatted time spent reading.

        Returns human-readable format like "2hr 30min", "45min", or None.
        """
        return format_time_spent(self.time_spent_reading)

    @property
    def is_synced(self) -> bool:
        """True if book exists in Notion (has notion_page_id)."""
        return self.notion_page_id is not None

    @property
    def needs_metadata_update(self) -> bool:
        """
        True if book metadata has changed since last sync.

        Currently always returns True if synced (conservative approach).
        Future optimization: Track field-level changes.
        """
        return self.is_synced

    def __str__(self) -> str:
        """Human-readable representation."""
        return f'"{self.title}" by {self.author} ({self.progress_code})'

    def __repr__(self) -> str:
        """Developer representation."""
        return (
            f"Book(title={self.title!r}, author={self.author!r}, "
            f"progress_code={self.progress_code!r}, "
            f"kobo_content_id={self.kobo_content_id!r})"
        )
