"""Cache manager for highlight deduplication using SQLite backend."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any

import structlog

from kobo_notion_sync.models.cache import Cache, CacheEntry
from kobo_notion_sync.models.highlight import Highlight

logger = structlog.get_logger(__name__)


class CacheCorruptionError(Exception):
    """Raised when cache database is corrupted."""

    pass


class CacheManager:
    """
    Manages highlight cache using SQLite backend at ~/.kobo-notion-sync/cache/.

    Provides:
    - Highlight deduplication via hash lookup (FR-030, FR-031, FR-032)
    - Cache corruption detection and auto-rebuild (FR-034A)
    - Atomic cache updates at session end (FR-033)
    """

    CACHE_DIR = Path.home() / ".kobo-notion-sync" / "cache"
    CACHE_DB = CACHE_DIR / "highlights.db"

    def __init__(self):
        """Initialize cache manager."""
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("cache_manager_initialized", cache_dir=str(self.CACHE_DIR))

    def has_highlight(self, highlight_hash: str) -> bool:
        """Check if highlight exists in cache (FR-031).

        Args:
            highlight_hash: SHA-256 hash of highlight (from Highlight.highlight_id)

        Returns:
            True if highlight has been synced before, False otherwise
        """
        try:
            if not self.CACHE_DB.exists():
                return False

            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT 1 FROM highlights WHERE highlight_hash = ? LIMIT 1",
                (highlight_hash,),
            )
            result = cursor.fetchone()
            conn.close()

            is_cached = result is not None
            logger.debug(
                "cache_lookup",
                highlight_hash=highlight_hash[:16],
                found=is_cached,
            )

            return is_cached

        except sqlite3.Error as e:
            logger.error("cache_lookup_failed", error=str(e))
            raise CacheCorruptionError(f"Cache database corrupted: {e}")

    def add_highlight(
        self,
        highlight_hash: str,
        book_id: str,
        notion_page_id: str,
    ) -> None:
        """Add highlight to cache (called during atomic update at session end).

        Args:
            highlight_hash: SHA-256 hash of highlight
            book_id: Kobo content ID
            notion_page_id: Notion page ID where highlight was synced
        """
        try:
            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            # Initialize schema if needed
            self._ensure_schema(cursor)

            # Insert or replace entry
            cursor.execute(
                """
                INSERT OR REPLACE INTO highlights
                (highlight_hash, book_id, notion_page_id, sync_timestamp)
                VALUES (?, ?, ?, ?)
                """,
                (highlight_hash, book_id, notion_page_id, datetime.now().isoformat()),
            )

            conn.commit()
            conn.close()

            logger.debug(
                "cache_add_highlight",
                highlight_hash=highlight_hash[:16],
                book_id=book_id,
            )

        except sqlite3.Error as e:
            logger.error("cache_add_failed", error=str(e))
            raise CacheCorruptionError(f"Failed to write to cache: {e}")

    def get_cached_highlights_for_book(self, book_id: str) -> List[str]:
        """Get list of cached highlight hashes for a book.

        Args:
            book_id: Kobo content ID

        Returns:
            List of cached highlight hashes for this book
        """
        try:
            if not self.CACHE_DB.exists():
                return []

            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT highlight_hash FROM highlights WHERE book_id = ?",
                (book_id,),
            )
            hashes = [row[0] for row in cursor.fetchall()]
            conn.close()

            logger.debug(
                "cache_get_book_highlights",
                book_id=book_id,
                count=len(hashes),
            )

            return hashes

        except sqlite3.Error as e:
            logger.error("cache_query_failed", error=str(e))
            raise CacheCorruptionError(f"Cache database corrupted: {e}")

    def validate_cache(self) -> Dict[str, Any]:
        """Validate cache database integrity and return status.

        Returns:
            Dict with validation status:
            - valid: bool - Whether cache is valid
            - error: str - Error message if invalid
            - entry_count: int - Number of entries in cache
            - size_bytes: int - Cache database size

        Raises:
            CacheCorruptionError: If cache is corrupted beyond repair
        """
        try:
            if not self.CACHE_DB.exists():
                logger.info("cache_validation", status="not_found")
                return {
                    "valid": True,
                    "error": None,
                    "entry_count": 0,
                    "size_bytes": 0,
                }

            # Try to open and query cache
            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            # Check for required table
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='highlights'"
            )
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                logger.warning("cache_validation", status="missing_table")
                conn.close()
                return {
                    "valid": False,
                    "error": "Cache table not found",
                    "entry_count": 0,
                    "size_bytes": self.CACHE_DB.stat().st_size,
                }

            # Count entries
            cursor.execute("SELECT COUNT(*) FROM highlights")
            entry_count = cursor.fetchone()[0]

            conn.close()

            size_bytes = self.CACHE_DB.stat().st_size

            logger.info(
                "cache_validation",
                status="valid",
                entry_count=entry_count,
                size_bytes=size_bytes,
            )

            return {
                "valid": True,
                "error": None,
                "entry_count": entry_count,
                "size_bytes": size_bytes,
            }

        except sqlite3.DatabaseError as e:
            logger.error("cache_validation_failed", error=str(e))
            return {
                "valid": False,
                "error": f"Database corrupted: {e}",
                "entry_count": 0,
                "size_bytes": self.CACHE_DB.stat().st_size if self.CACHE_DB.exists() else 0,
            }

    def rebuild_cache(self) -> None:
        """Rebuild cache database from scratch.

        Deletes existing cache and recreates with empty schema (per FR-034A).
        """
        try:
            logger.warning("cache_rebuild_starting")

            # Delete existing cache
            if self.CACHE_DB.exists():
                self.CACHE_DB.unlink()

            # Recreate with fresh schema
            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            conn.commit()
            conn.close()

            logger.info("cache_rebuild_complete")

        except Exception as e:
            logger.exception("cache_rebuild_failed", error=str(e))
            raise CacheCorruptionError(f"Failed to rebuild cache: {e}")

    def clear_book_highlights(self, book_id: str) -> None:
        """Clear all cached highlights for a specific book.

        Used when rebuilding cache for a specific book.

        Args:
            book_id: Kobo content ID
        """
        try:
            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            self._ensure_schema(cursor)

            cursor.execute("DELETE FROM highlights WHERE book_id = ?", (book_id,))
            conn.commit()
            conn.close()

            logger.debug("cache_cleared_book", book_id=book_id)

        except sqlite3.Error as e:
            logger.error("cache_clear_failed", error=str(e))
            raise CacheCorruptionError(f"Failed to clear cache: {e}")

    def _ensure_schema(self, cursor: sqlite3.Cursor) -> None:
        """Ensure cache database schema exists.

        Creates highlights table if it doesn't exist.

        Args:
            cursor: SQLite cursor
        """
        try:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS highlights (
                    highlight_hash TEXT PRIMARY KEY,
                    book_id TEXT NOT NULL,
                    notion_page_id TEXT NOT NULL,
                    sync_timestamp TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_book_id ON highlights(book_id)"
            )
        except sqlite3.Error as e:
            logger.error("schema_creation_failed", error=str(e))
            raise CacheCorruptionError(f"Failed to create cache schema: {e}")

    def atomic_update_highlights(self, highlights: List[Dict[str, str]]) -> None:
        """Atomically update cache with list of new highlights at session end (FR-033).

        This is a transaction that either commits all updates or none.

        Args:
            highlights: List of dicts with keys:
                - highlight_hash: SHA-256 hash
                - book_id: Kobo content ID
                - notion_page_id: Notion page ID
        """
        try:
            conn = sqlite3.connect(self.CACHE_DB)
            cursor = conn.cursor()

            self._ensure_schema(cursor)

            # Begin transaction
            cursor.execute("BEGIN TRANSACTION")

            timestamp = datetime.now().isoformat()

            for hl in highlights:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO highlights
                    (highlight_hash, book_id, notion_page_id, sync_timestamp)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        hl["highlight_hash"],
                        hl["book_id"],
                        hl["notion_page_id"],
                        timestamp,
                    ),
                )

            # Commit transaction
            conn.commit()
            conn.close()

            logger.info(
                "cache_atomic_update_complete",
                highlights_count=len(highlights),
            )

        except sqlite3.Error as e:
            logger.error("cache_atomic_update_failed", error=str(e))
            # Rollback will happen automatically when connection closes
            raise CacheCorruptionError(f"Failed to update cache atomically: {e}")
