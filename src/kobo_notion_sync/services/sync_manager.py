"""Sync orchestrator coordinating all sync operations."""

from datetime import datetime
from typing import Dict, List, Optional, Any

import structlog

from kobo_notion_sync.lib.config_loader import ConfigLoader
from kobo_notion_sync.models.book import Book
from kobo_notion_sync.models.config import Configuration
from kobo_notion_sync.models.sync_session import SyncSession, SyncMode
from kobo_notion_sync.services.cache_manager import CacheManager, CacheCorruptionError
from kobo_notion_sync.services.kobo_extractor import KoboExtractor, KoboDeviceError
from kobo_notion_sync.services.notion_client import NotionClient, NotionValidationError
from kobo_notion_sync.services.cover_image import CoverImageService

logger = structlog.get_logger(__name__)


class SyncError(Exception):
    """Raised when sync operation fails critically."""

    pass


class SyncManager:
    """
    Orchestrates sync operations coordinating kobo_extractor, cache_manager, and notion_client.

    Implements:
    - Kobo device connection verification (T045, FR-021)
    - Book metadata extraction from Kobo (T046-T047B, FR-016-FR-022)
    - Highlight extraction from Kobo (T049-T050, FR-018, FR-023)
    - Cache-based deduplication (T052-T053, FR-031-FR-034A)
    - Notion book creation/update (T055-T056, FR-027-FR-028)
    - Highlight page generation (T057-T057F, FR-028B)
    - Kobo-as-source-of-truth logic (T060, FR-028A)
    - Cache atomic updates (T064, FR-033)
    - Status transitions (T061, FR-024)
    - Structured logging (T066, FR-050-FR-051)
    """

    def __init__(
        self,
        kobo_extractor: KoboExtractor,
        notion_client: NotionClient,
        config: Configuration,
    ):
        """Initialize sync manager with services.

        Args:
            kobo_extractor: Kobo device and database service
            notion_client: Notion API wrapper
            config: Configuration with database_id and optional property flags
        """
        self.kobo_extractor = kobo_extractor
        self.notion_client = notion_client
        self.config = config
        self.cache_manager = CacheManager()
        self.cover_image_service = CoverImageService()

        logger.info("sync_manager_initialized")

    def sync_full(
        self,
        full_mode: bool = False,
        dry_run: bool = False,
    ) -> SyncSession:
        """Execute full manual sync from Kobo to Notion.

        Performs:
        1. Device verification (FR-021)
        2. Book extraction (FR-016, FR-017, FR-022)
        3. Highlight extraction (FR-018, FR-023)
        4. Cache deduplication (FR-029-FR-034)
        5. Notion book creation/update (FR-027-FR-028)
        6. Highlight page generation (FR-028B)
        7. Atomic cache update (FR-033)

        Args:
            full_mode: If True, bypass cache and re-sync all highlights (FR-024, T068)
            dry_run: If True, preview changes without syncing (T043)

        Returns:
            SyncSession with operation results and statistics

        Raises:
            SyncError: If critical error occurs (device not found, Notion unavailable)
        """
        session = SyncSession(
            sync_mode=SyncMode.FULL,
            start_time=datetime.now(),
        )

        logger.info("sync_full_started", full_mode=full_mode, dry_run=dry_run)

        try:
            # Step 1: Verify Kobo device connection (T045, FR-021)
            logger.info("sync_step_device_verification")
            try:
                device_mount = self.kobo_extractor.detect_device()
                device_info = self.kobo_extractor.get_device_info()
                logger.info(
                    "kobo_device_verified",
                    device_model=device_info.get("model"),
                    mount_path=device_mount,
                )
            except KoboDeviceError as e:
                logger.error("kobo_device_verification_failed", error=str(e))
                session.add_error(f"Kobo device not found: {e}")
                session.complete()
                return session

            # Step 2: Validate cache (FR-034A, T053)
            logger.info("sync_step_cache_validation")
            try:
                cache_status = self.cache_manager.validate_cache()
                if not cache_status["valid"]:
                    logger.warning(
                        "cache_invalid_rebuilding",
                        error=cache_status.get("error"),
                    )
                    self.cache_manager.rebuild_cache()
            except CacheCorruptionError as e:
                logger.warning("cache_corruption_detected", error=str(e))
                self.cache_manager.rebuild_cache()

            # Step 3: Extract books from Kobo (T046-T047B, FR-016-FR-022)
            logger.info("sync_step_extracting_books")
            try:
                config_dict = {
                    "extract_description": self.config.notion.has_description_property,
                    "extract_time_spent": self.config.notion.has_time_spent_property,
                }
                books = self.kobo_extractor.extract_books(config=config_dict)
                session.books_processed = len(books)
                logger.info("books_extracted", count=len(books))
            except KoboDeviceError as e:
                logger.error("books_extraction_failed", error=str(e))
                session.add_error(f"Failed to extract books: {e}")
                session.complete()
                return session

            # Step 4: Process each book (extract highlights, deduplicate, sync to Notion)
            logger.info("sync_step_processing_books")
            highlights_to_cache: List[Dict[str, str]] = []

            for book in books:
                # Check device connection before processing each book (T063, FR-044)
                if not self._check_device_connected():
                    logger.error("sync_device_disconnected_during_processing")
                    session.add_error("Kobo device disconnected during sync - halting")
                    session.complete()
                    return session

                try:
                    logger.info(
                        "sync_processing_book",
                        title=book.title,
                        kobo_id=book.kobo_content_id,
                        progress=book.progress_code,
                    )

                    # Extract highlights for this book (T049-T050, FR-018, FR-023)
                    try:
                        highlights = self.kobo_extractor.extract_highlights(
                            book.kobo_content_id
                        )
                        logger.info(
                            "highlights_extracted",
                            book_id=book.kobo_content_id,
                            count=len(highlights),
                        )
                    except KoboDeviceError as e:
                        logger.warning(
                            "highlights_extraction_failed",
                            book_id=book.kobo_content_id,
                            error=str(e),
                        )
                        highlights = []

                    # Filter out cached highlights (T052, FR-031, FR-032)
                    new_highlights = []
                    if not full_mode:
                        for hl in highlights:
                            if not self.cache_manager.has_highlight(hl.highlight_id):
                                new_highlights.append(hl)
                                session.cache_misses += 1
                            else:
                                session.highlights_skipped += 1
                                session.cache_hits += 1
                    else:
                        new_highlights = highlights
                        session.cache_misses += len(highlights)
                        logger.info("sync_full_mode_bypassing_cache")

                    logger.info(
                        "highlights_after_deduplication",
                        book_id=book.kobo_content_id,
                        new_count=len(new_highlights),
                        skipped_count=session.highlights_skipped,
                    )

                    # Skip Notion API calls for books with no new highlights (T073, FR-032)
                    if not new_highlights:
                        logger.debug(
                            "skipping_notion_api_no_new_highlights",
                            book_id=book.kobo_content_id,
                        )
                        continue

                    # Sync new highlights to Notion
                    session.highlights_synced += len(new_highlights)

                    # Track for atomic cache update at end (T064, FR-033)
                    for hl in new_highlights:
                        highlights_to_cache.append(
                            {
                                "highlight_hash": hl.highlight_id,
                                "book_id": book.kobo_content_id,
                                "notion_page_id": book.notion_page_id
                                or "",  # Set after Notion sync
                            }
                        )

                except Exception as e:
                    logger.error(
                        "book_processing_failed",
                        title=book.title,
                        error=str(e),
                    )
                    session.add_error(f"Failed to process book '{book.title}': {e}")

            # Step 5: Show results in dry-run mode
            if dry_run:
                logger.info(
                    "sync_dry_run_complete",
                    books_processed=session.books_processed,
                    highlights_synced=session.highlights_synced,
                    highlights_skipped=session.highlights_skipped,
                )
            else:
                # Step 6: Sync to Notion (T055-T057F, T096, FR-027-FR-028B)
                logger.info("sync_step_syncing_to_notion")
                
                for book in books:
                    try:
                        # Get highlights for this book (already extracted in Step 4)
                        highlights = self.kobo_extractor.extract_highlights(
                            book.kobo_content_id
                        )
                        
                        # Filter for new highlights if not in full mode
                        if not full_mode:
                            new_highlights = [
                                h for h in highlights
                                if not self.cache_manager.has_highlight(h.highlight_id)
                            ]
                        else:
                            new_highlights = highlights
                        
                        # Skip if no new highlights to sync
                        if not new_highlights:
                            continue
                        
                        # Sync book to Notion with cover image integration
                        page_id = self._sync_book_to_notion(book, new_highlights)
                        
                        if page_id:
                            # Track highlights for cache update
                            for h in new_highlights:
                                highlights_to_cache.append({
                                    "highlight_hash": h.highlight_id,
                                    "book_id": book.kobo_content_id,
                                    "notion_page_id": page_id,
                                })
                    
                    except Exception as e:
                        logger.error(
                            "notion_sync_failed",
                            title=book.title,
                            error=str(e),
                        )
                        session.add_error(f"Failed to sync book '{book.title}': {e}")

                # Atomic cache update at session end (T064, FR-033)
                if highlights_to_cache and not dry_run:
                    try:
                        self.cache_manager.atomic_update_highlights(highlights_to_cache)
                        logger.info(
                            "cache_updated_atomically",
                            highlights_count=len(highlights_to_cache),
                        )
                    except CacheCorruptionError as e:
                        logger.error("cache_update_failed", error=str(e))
                        session.add_error(f"Failed to update cache: {e}")

            session.complete()
            logger.info(
                "sync_full_complete",
                status=session.status.value,
                books_processed=session.books_processed,
                highlights_synced=session.highlights_synced,
                highlights_skipped=session.highlights_skipped,
                cache_hits=session.cache_hits,
                cache_misses=session.cache_misses,
                deduplication_rate_percent=f"{session.deduplication_rate:.1f}%",
                duration_seconds=session.duration_seconds,
            )

            return session

        except Exception as e:
            logger.exception("sync_full_unexpected_error", error=str(e))
            session.add_error(f"Unexpected sync error: {e}")
            session.complete()
            return session

    def _check_device_connected(self) -> bool:
        """Check if Kobo device is still connected (T063, FR-044).

        Returns:
            True if device is connected and accessible, False otherwise
        """
        try:
            if not self.kobo_extractor.mount_path:
                return False

            # Try to access device
            device_db = self.kobo_extractor.mount_path / self.kobo_extractor.KOBO_DB_PATH
            if not device_db.exists():
                logger.warning("kobo_device_disconnected")
                return False

            return True

        except Exception as e:
            logger.warning("device_connection_check_failed", error=str(e))
            return False
    
    def _sync_book_to_notion(
        self,
        book: Book,
        highlights: List[Any],
    ) -> Optional[str]:
        """Sync a book and its highlights to Notion (T096, T097, FR-017A, SC-028).
        
        Creates or updates the book page in Notion, sets cover image if available,
        and updates page content with highlights.
        
        Args:
            book: Book object to sync
            highlights: List of Highlight objects for this book
        
        Returns:
            Notion page ID if successful, None otherwise
        """
        page_id = None
        cover_success = False
        
        try:
            # Check if book already exists in Notion
            existing_page = self.notion_client.get_book_by_kobo_id(
                database_id=self.config.notion.database_id,
                kobo_content_id=book.kobo_content_id,
            )
            
            if existing_page:
                # Update existing book (T056, T076A, FR-027)
                page_id = existing_page.get("id")
                
                logger.info(
                    "updating_existing_book",
                    page_id=page_id,
                    title=book.title,
                )
                
                self.notion_client.update_book_page(
                    page_id=page_id,
                    progress_code=book.progress_code,
                    description=book.description if self.config.notion.has_description_property else None,
                    time_spent=book.time_spent_reading if self.config.notion.has_time_spent_property else None,
                )
                
                # Update status to Completed if applicable (T061, FR-024)
                if book.progress_code == "Completed":
                    self.notion_client.update_book_status_to_completed(
                        page_id=page_id,
                        completion_date=book.date_last_read,
                    )
            
            else:
                # Create new book page (T055, FR-028)
                logger.info(
                    "creating_new_book",
                    title=book.title,
                    author=book.author,
                )
                
                # Map progress_code to status ("Completed" -> "Finished")
                status_map = {"New": "New", "Reading": "Reading", "Completed": "Finished"}
                status = status_map.get(book.progress_code, "New")
                
                page_id = self.notion_client.create_book_page(
                    database_id=self.config.notion.database_id,
                    title=book.title,
                    author=book.author,
                    status=status,
                    progress_percent=book.percent_read,
                    page_type="Kobo",
                    isbn=book.isbn,
                    publisher=book.publisher,
                    kobo_content_id=book.kobo_content_id,
                    description=book.description if self.config.notion.has_description_property else None,
                    time_spent=book.time_spent_reading if self.config.notion.has_time_spent_property else None,
                )
            
            # Try to set cover image (T096, T097, FR-017A, SC-028)
            # This is non-blocking - if it fails, we continue with the sync
            if page_id and book.isbn:
                try:
                    logger.info(
                        "retrieving_cover_image",
                        isbn=book.isbn,
                        title=book.title,
                    )
                    
                    cover_url = self.cover_image_service.get_cover_url(
                        isbn=book.isbn,
                        title=book.title,
                        author=book.author,
                    )
                    
                    if cover_url:
                        self.notion_client.set_cover_image(
                            page_id=page_id,
                            image_url=cover_url,
                        )
                        cover_success = True
                        logger.info(
                            "cover_image_set_success",
                            page_id=page_id,
                            url=cover_url,
                        )
                    else:
                        logger.info(
                            "cover_image_not_found",
                            isbn=book.isbn,
                            title=book.title,
                        )
                
                except Exception as e:
                    # Cover image failures should not block sync (SC-028)
                    logger.warning(
                        "cover_image_failed_continuing",
                        page_id=page_id,
                        error=str(e),
                    )
            
            # Update page content with highlights (T057-T057F, FR-028B)
            if page_id and highlights:
                self.notion_client.create_highlight_blocks(
                    page_id=page_id,
                    highlights=[
                        {
                            "text": h.text,
                            "chapter_progress": h.chapter_progress,
                            "date_created": h.date_created,
                            "annotation": h.annotation,
                        }
                        for h in highlights
                    ],
                )
            
            # Update sync metadata (Last Sync Time and Highlights Count)
            if page_id:
                from datetime import datetime
                self.notion_client.update_sync_metadata(
                    page_id=page_id,
                    highlights_count=len(highlights),
                    sync_time=datetime.now(),
                )
            
            # Log cover image result (T097, FR-051)
            logger.info(
                "book_sync_complete",
                page_id=page_id,
                title=book.title,
                highlights_count=len(highlights),
                cover_image_success=cover_success,
            )
            
            return page_id
        
        except NotionValidationError as e:
            logger.error(
                "book_sync_failed",
                title=book.title,
                error=str(e),
            )
            return None
        
        except Exception as e:
            logger.error(
                "book_sync_unexpected_error",
                title=book.title,
                error=str(e),
            )
            return None
