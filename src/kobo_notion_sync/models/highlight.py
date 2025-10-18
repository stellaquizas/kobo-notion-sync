"""Highlight model representing a highlighted passage from a book."""

import hashlib
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class Highlight(BaseModel):
    """
    Represents a single highlighted passage from a book with location and timestamp.

    See data-model.md for full entity definition and validation rules.
    """

    book_id: str = Field(description="Foreign key to Book.kobo_content_id")
    text: str = Field(min_length=1, description="Highlighted text content")
    chapter_progress: Optional[float] = Field(
        default=None, ge=0.0, le=100.0, description="Position within chapter (0-100%)"
    )
    date_created: datetime = Field(description="When highlight was created")
    annotation: Optional[str] = Field(
        default=None, description="User's note/comment on highlight"
    )
    notion_block_id: Optional[str] = Field(
        default=None, description="Notion block ID after sync"
    )

    @field_validator("text")
    @classmethod
    def validate_text_not_empty(cls, v: str) -> str:
        """Ensure text is not empty after stripping whitespace."""
        if not v.strip():
            raise ValueError("text must not be empty after stripping whitespace")
        return v

    @property
    def highlight_id(self) -> str:
        """
        Generate unique hash for deduplication.

        Hash: SHA-256(book_id + text + chapter_progress)

        This ensures:
        - Same text in different books creates different highlights
        - Same text at different locations creates different highlights
        - Duplicate highlights are detected reliably
        """
        content = f"{self.book_id}:{self.text}:{self.chapter_progress}"
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    @property
    def is_synced(self) -> bool:
        """True if highlight has been synced to Notion (has notion_block_id)."""
        return self.notion_block_id is not None

    @property
    def location_display(self) -> str:
        """
        Human-readable location string for display in Notion.

        Returns chapter progress as percentage if available, otherwise "Unknown location".
        Note: Kobo does not provide page numbers (per clarification Q8).
        """
        if self.chapter_progress is not None:
            return f"Chapter position: {self.chapter_progress:.1f}%"
        return "Unknown location"

    def __str__(self) -> str:
        """Human-readable representation."""
        preview = self.text[:50] + "..." if len(self.text) > 50 else self.text
        return f'Highlight: "{preview}" at {self.location_display}'

    def __repr__(self) -> str:
        """Developer representation."""
        return (
            f"Highlight(book_id={self.book_id!r}, "
            f"text={self.text[:30]!r}..., "
            f"highlight_id={self.highlight_id[:16]}...)"
        )
