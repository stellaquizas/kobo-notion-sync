"""Cache model for highlight deduplication."""

from datetime import datetime
from typing import Dict

from pydantic import BaseModel, Field, field_validator


class CacheEntry(BaseModel):
    """Single entry in the highlight cache."""

    book_id: str = Field(description="Kobo content ID")
    notion_page_id: str = Field(description="Notion page ID where highlight was synced")
    highlight_hash: str = Field(
        min_length=64, max_length=64, description="SHA-256 hash of highlight"
    )
    sync_timestamp: datetime = Field(description="When highlight was synced")

    @field_validator("highlight_hash")
    @classmethod
    def validate_hash_format(cls, v: str) -> str:
        """Validate hash is 64-character hexadecimal (SHA-256)."""
        if not all(c in "0123456789abcdef" for c in v.lower()):
            raise ValueError("highlight_hash must be hexadecimal")
        return v


class Cache(BaseModel):
    """
    Cache for highlight deduplication.

    Stored as JSON file at ~/.kobo-notion-sync/cache/highlights.json

    See data-model.md "Cache Entry" and contracts/cache-schema.json for full spec.
    """

    version: str = Field(default="1.0", description="Cache format version")
    last_updated: datetime = Field(description="Last cache write timestamp")
    entries: Dict[str, CacheEntry] = Field(
        default_factory=dict, description="Map of highlight_hash -> metadata"
    )

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate cache version matches expected format."""
        if v != "1.0":
            raise ValueError(f"Unsupported cache version: {v}. Expected 1.0")
        return v

    def has_highlight(self, highlight_hash: str) -> bool:
        """Check if highlight exists in cache."""
        return highlight_hash in self.entries

    def add_highlight(
        self, highlight_hash: str, book_id: str, notion_page_id: str
    ) -> None:
        """
        Add highlight to cache.

        Args:
            highlight_hash: SHA-256 hash of highlight
            book_id: Kobo content ID
            notion_page_id: Notion page ID where highlight was synced
        """
        self.entries[highlight_hash] = CacheEntry(
            book_id=book_id,
            notion_page_id=notion_page_id,
            highlight_hash=highlight_hash,
            sync_timestamp=datetime.now(),
        )
        self.last_updated = datetime.now()

    def remove_highlight(self, highlight_hash: str) -> bool:
        """
        Remove highlight from cache.

        Returns True if highlight was removed, False if not found.
        """
        if highlight_hash in self.entries:
            del self.entries[highlight_hash]
            self.last_updated = datetime.now()
            return True
        return False

    def get_highlights_for_book(self, book_id: str) -> list[str]:
        """Get list of highlight hashes for a specific book."""
        return [
            hash_
            for hash_, entry in self.entries.items()
            if entry.book_id == book_id
        ]

    def clear(self) -> None:
        """Clear all entries from cache."""
        self.entries.clear()
        self.last_updated = datetime.now()

    @property
    def size(self) -> int:
        """Get number of cached highlights."""
        return len(self.entries)

    def __str__(self) -> str:
        """Human-readable representation."""
        return f"Cache(version={self.version}, size={self.size}, last_updated={self.last_updated})"
