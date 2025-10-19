"""Sync orchestrator coordinating all sync operations."""

from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
import time

import structlog

from kobo_notion_sync.lib.config_loader import ConfigLoader
from kobo_notion_sync.models.book import Book
from kobo_notion_sync.models.config import Configuration
from kobo_notion_sync.models.sync_session import SyncSession, SyncMode
from kobo_notion_sync.services.kobo_extractor import KoboExtractor, KoboDeviceError
from kobo_notion_sync.services.notion_client import NotionClient, NotionValidationError
from kobo_notion_sync.services.cover_image import CoverImageService

logger = structlog.get_logger(__name__)


def _short_uuid(uuid_str: Optional[str]) -> str:
    """Return first 8 characters of UUID for readable logging.
    
    Args:
        uuid_str: Full UUID string or None
    
    Returns:
        First 8 characters of UUID, or 'unknown' if None
    """
    if not uuid_str:
        return "unknown"
    return str(uuid_str)[:8]


class SyncError(Exception):
    """Raised when sync operation fails critically."""

    pass


class SyncManager:
    """
    Orchestrates sync operations coordinating kobo_extractor and notion_client.

    Implements:
    - Kobo device connection verification
    - Book metadata extraction from Kobo
    - Highlight extraction from Kobo
    - Last read date based updates (smart sync based on reading activity)
    - Notion book creation/update
    - Highlight page generation with reading period info
    - Kobo-as-source-of-truth logic 
    - Status transitions
    - Structured logging
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
        self.cover_image_service = CoverImageService()

        logger.info("sync_manager_initialized")

    def sync_full(
        self,
        full_mode: bool = False,
        dry_run: bool = False,
    ) -> SyncSession:
        """Execute full manual sync from Kobo to Notion.

        Performs:
        1. Device verification
        2. Book extraction
        3. Highlight extraction
        4. Notion book creation/update
        5. Highlight page generation/update
        6. Sync metadata update

        Args:
            full_mode: If True, force re-sync all books (ignored, not used)
            dry_run: If True, preview changes without syncin

        Returns:
            SyncSession with operation results and statistics

        Raises:
            SyncError: If critical error occurs (device not found, Notion unavailable)
        """
        session = SyncSession(
            sync_mode=SyncMode.FULL,
            start_time=datetime.now(timezone.utc).astimezone(),
        )

        logger.info("sync_full_started", full_mode=full_mode, dry_run=dry_run)

        try:
            # Step 1: Verify Kobo device connection
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

            # Step 1.5: Delete all Kobo books if full_mode (before starting sync)
            if full_mode:
                logger.info("sync_full_mode_deleting_existing_kobo_books")
                try:
                    deleted_count = self.notion_client.delete_all_kobo_books(
                        self.config.notion.database_id
                    )
                    logger.info(
                        "kobo_books_deleted_for_full_sync",
                        deleted_count=deleted_count,
                    )
                    session.add_error(f"[Full sync] Deleted {deleted_count} existing Kobo books")
                except NotionValidationError as e:
                    logger.error("delete_kobo_books_failed", error=str(e))
                    session.add_error(f"Failed to delete existing Kobo books: {e}")
                    session.complete()
                    return session

            # Step 2: Extract books from Kobo
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

            # Step 2.5: Get fast batch mapping of existing Notion books
            # (before processing any books to compare efficiently)
            logger.info("sync_step_batch_fetch_notion_books")
            try:
                notion_books_mapping = self.notion_client.get_kobo_books_mapping(
                    self.config.notion.database_id
                )
                logger.info(
                    "notion_books_mapping_fetched",
                    book_count=len(notion_books_mapping),
                )
            except Exception as e:
                logger.warning(
                    "notion_books_mapping_fetch_failed_continuing",
                    error=str(e),
                )
                notion_books_mapping = {}

            # Step 2.7: Identify books with changed last_read_date in Notion
            # (smart sync: ONLY source of truth is last_read_date comparison)
            logger.info("sync_step_identify_changed_books")
            books_to_delete_ids = []
            books_to_process = set()  # Track which books need processing
            books_created_list = []  # Track new books being created
            books_updated_list = []  # Track existing books being updated
            
            for book in books:
                book_id_short = _short_uuid(book.kobo_content_id)
                
                if book.kobo_content_id not in notion_books_mapping:
                    # Book not in Notion yet - will create
                    books_to_process.add(book.kobo_content_id)
                    books_created_list.append(book.title)
                    logger.info(
                        "book_update_decision",
                        decision="WILL_UPDATE",
                        reason="new_book_not_in_notion",
                        kobo_id=book_id_short,
                        title=book.title,
                    )
                    continue
                
                notion_book_data = notion_books_mapping[book.kobo_content_id]
                notion_last_read_str = notion_book_data.get("last_read_date")
                kobo_last_read = book.date_last_read.date() if book.date_last_read else None
                
                # Parse notion date string (format: "2025-10-19")
                notion_last_read = None
                if notion_last_read_str:
                    try:
                        notion_last_read = datetime.strptime(notion_last_read_str, "%Y-%m-%d").date()
                    except:
                        pass
                
                # SINGLE SOURCE OF TRUTH: last_read_date comparison
                # If dates differ -> mark for deletion and re-insertion
                # If dates same -> skip book entirely (no extraction, no cover image, nothing)
                if kobo_last_read != notion_last_read:
                    page_id = notion_book_data.get("page_id")
                    books_to_delete_ids.append(page_id)
                    books_to_process.add(book.kobo_content_id)
                    books_updated_list.append(book.title)
                    logger.info(
                        "book_update_decision",
                        decision="WILL_UPDATE",
                        reason="last_read_date_changed",
                        kobo_id=book_id_short,
                        title=book.title,
                        notion_last_read=str(notion_last_read),
                        kobo_last_read=str(kobo_last_read),
                    )
                else:
                    # Last read date unchanged - skip this book entirely
                    logger.info(
                        "book_update_decision",
                        decision="WILL_SKIP",
                        reason="last_read_date_unchanged",
                        kobo_id=book_id_short,
                        title=book.title,
                        last_read_date=str(kobo_last_read),
                    )

            # Step 2.9: Delete outdated books (batch operation)
            if books_to_delete_ids:
                logger.info("sync_step_delete_outdated_books", count=len(books_to_delete_ids))
                try:
                    deleted_count = self.notion_client.delete_pages_batch(books_to_delete_ids)
                    logger.info("outdated_books_deleted", count=deleted_count)
                except Exception as e:
                    logger.warning(
                        "delete_outdated_books_failed_continuing",
                        error=str(e),
                    )

            # Update session with tracking info
            session.books_created = len(books_created_list)
            session.books_updated = len(books_updated_list)
            session.books_processed = len(books_to_process)  # Only books actually processed
            session.books_skipped = len(books) - len(books_to_process)  # Books skipped due to smart sync
            session.updated_book_names = books_created_list + books_updated_list  # All books being created/updated
            
            logger.info(
                "smart_sync_summary",
                total_books=len(books),
                books_to_process=len(books_to_process),
                books_skipped=session.books_skipped,
                books_created=session.books_created,
                books_updated=session.books_updated,
            )

            # Step 3: Process each book (extract highlights, sync to Notion)
            logger.info("sync_step_processing_books", books_to_process_count=len(books_to_process))

            for book in books:
                # Skip books that don't need processing (smart sync)
                if book.kobo_content_id not in books_to_process:
                    logger.debug("book_skipped_no_changes", kobo_id=_short_uuid(book.kobo_content_id), title=book.title)
                    continue
                
                # Check device connection before processing each book
                if not self._check_device_connected():
                    logger.error("sync_device_disconnected_during_processing")
                    session.add_error("Kobo device disconnected during sync - halting")
                    session.complete()
                    return session

                try:
                    logger.info(
                        "sync_processing_book",
                        kobo_id=_short_uuid(book.kobo_content_id),
                        title=book.title,
                        progress=book.progress_code,
                    )

                    # Extract highlights for this book
                    try:
                        highlights = self.kobo_extractor.extract_highlights(
                            book.kobo_content_id
                        )
                        logger.info(
                            "highlights_extracted",
                            kobo_id=_short_uuid(book.kobo_content_id),
                            count=len(highlights),
                        )
                    except KoboDeviceError as e:
                        logger.warning(
                            "highlights_extraction_failed",
                            kobo_id=_short_uuid(book.kobo_content_id),
                            error=str(e),
                        )
                        highlights = []

                    # Store for later sync
                    session.highlights_synced += len(highlights)

                    logger.info(
                        "highlights_to_sync",
                        kobo_id=_short_uuid(book.kobo_content_id),
                        count=len(highlights),
                    )

                except Exception as e:
                    logger.error(
                        "book_processing_failed",
                        kobo_id=_short_uuid(book.kobo_content_id),
                        title=book.title,
                        error=str(e),
                    )
                    session.add_error(f"Failed to process book '{book.title}': {e}")

            # Step 4: Sync all books to Notion (batch/parallel friendly)
            if not dry_run:
                logger.info("sync_step_syncing_to_notion", book_count=len(books_to_process))
                
                for book in books:
                    # Skip books that don't need processing (smart sync)
                    if book.kobo_content_id not in books_to_process:
                        continue
                    
                    try:
                        # Get highlights for this book
                        highlights = self.kobo_extractor.extract_highlights(
                            book.kobo_content_id
                        )
                        
                        # Sync book to Notion with cover image integration
                        page_id = self._sync_book_to_notion(book, highlights)
                    
                    except Exception as e:
                        logger.error(
                            "notion_sync_failed",
                            kobo_id=_short_uuid(book.kobo_content_id),
                            title=book.title,
                            error=str(e),
                        )
                        session.add_error(f"Failed to sync book '{book.title}': {e}")

            session.complete()
            logger.info(
                "sync_full_complete",
                status=session.status.value,
                books_processed=session.books_processed,
                highlights_synced=session.highlights_synced,
                duration_seconds=session.duration_seconds,
                books_deleted_for_re_sync=len(books_to_delete_ids),
            )
            
            # Update database title with total book count
            try:
                total_books = len(books)
                self.notion_client.update_database_title(
                    database_id=self.config.notion.database_id,
                    total_books=total_books,
                )
            except Exception as e:
                logger.warning(
                    "database_title_update_failed_continuing",
                    error=str(e),
                )

            return session

        except Exception as e:
            logger.exception("sync_full_unexpected_error", error=str(e))
            session.add_error(f"Unexpected sync error: {e}")
            session.complete()
            return session

    def _check_device_connected(self) -> bool:
        """Check if Kobo device is still connected.

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
        """Sync a book and its highlights to Notion.
        
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
        book_id_short = _short_uuid(book.kobo_content_id)
        
        try:
            # Check if book already exists in Notion
            existing_page = self.notion_client.get_book_by_kobo_id(
                database_id=self.config.notion.database_id,
                kobo_content_id=book.kobo_content_id,
            )
            
            if existing_page:
                # Book exists - get its previous last_read_date for reading period calculation
                # Note: This book was already filtered in Step 2.7 to have changed last_read_date
                notion_last_read = self.notion_client.get_book_last_read_date(existing_page)
                page_id = existing_page.get("id")
                
                logger.info(
                    "updating_existing_book_last_read_changed",
                    kobo_id=book_id_short,
                    page_id=page_id,
                    title=book.title,
                    notion_last_read=notion_last_read.date() if notion_last_read else None,
                    kobo_last_read=book.date_last_read.date() if book.date_last_read else None,
                )
                
                # Update book metadata
                self.notion_client.update_book_page(
                    page_id=page_id,
                    progress_code=book.progress_code,
                    percent_read=book.percent_read / 100.0,
                    description=book.description if self.config.notion.has_description_property else None,
                    time_spent=book.time_spent_formatted if self.config.notion.has_time_spent_property else None,
                    last_read_date=book.date_last_read,
                )
                
                # Update status to Completed if applicable
                if book.progress_code == "Finished":
                    self.notion_client.update_book_status_to_completed(
                        page_id=page_id,
                        completion_date=book.date_finished,
                    )
            
            else:
                # Create new book page
                logger.info(
                    "creating_new_book",
                    kobo_id=book_id_short,
                    title=book.title,
                    author=book.author,
                )
                
                # Map progress_code to status ("Finished" -> "Finished")
                status_map = {"New": "New", "Reading": "Reading", "Finished": "Finished"}
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
                    time_spent=book.time_spent_formatted if self.config.notion.has_time_spent_property else None,
                    finished_date=book.date_finished if book.read_status == 2 else None,
                    last_read_date=book.date_last_read,
                )
                
                # New books don't have a previous reading period
                notion_last_read = None
            
            # Try to set cover image
            # This is non-blocking - if it fails, we continue with the sync
            if page_id and book.isbn:
                try:
                    logger.info(
                        "retrieving_cover_image",
                        kobo_id=book_id_short,
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
                            kobo_id=book_id_short,
                            page_id=page_id,
                            url=cover_url,
                        )
                    else:
                        logger.info(
                            "cover_image_not_found",
                            kobo_id=book_id_short,
                            isbn=book.isbn,
                            title=book.title,
                        )
                
                except Exception as e:
                    # Cover image failures should not block sync
                    logger.warning(
                        "cover_image_failed_continuing",
                        kobo_id=book_id_short,
                        page_id=page_id,
                        error=str(e),
                    )
            
            # Update page content with highlights
            if page_id and highlights:
                # Check if this is an update to existing book (book was re-read)
                is_book_update = existing_page is not None
                
                highlights_data = [
                    {
                        "text": h.text,
                        "chapter_progress": h.chapter_progress,
                        "date_created": h.date_created,
                        "annotation": h.annotation,
                    }
                    for h in highlights
                ]
                
                if is_book_update:
                    # Book was re-read - update highlights with reading period info
                    # Delete old highlights and recreate with new reading period
                    logger.info(
                        "updating_highlights_for_reread_book",
                        kobo_id=book_id_short,
                        page_id=page_id,
                        title=book.title,
                        last_read_date=book.date_last_read.isoformat() if book.date_last_read else None,
                    )
                    
                    self.notion_client.update_highlight_blocks(
                        page_id=page_id,
                        highlights=highlights_data,
                        start_read_date=notion_last_read,  # Previous last_read_date is now the start
                        last_read_date=book.date_last_read,  # Current last_read_date is now the end
                    )
                else:
                    # New book - create highlights with initial reading period
                    self.notion_client.create_highlight_blocks(
                        page_id=page_id,
                        highlights=highlights_data,
                        start_read_date=book.date_started,
                        last_read_date=book.date_last_read,
                    )
            
            # Update sync metadata (Last Sync Time and Highlights Count)
            if page_id:
                self.notion_client.update_sync_metadata(
                    page_id=page_id,
                    highlights_count=len(highlights),
                    sync_time=datetime.now(timezone.utc).astimezone(),
                )
            
            # Log cover image result
            logger.info(
                "book_sync_complete",
                kobo_id=book_id_short,
                page_id=page_id,
                title=book.title,
                highlights_count=len(highlights),
                cover_image_success=cover_success,
            )
            
            return page_id
        
        except NotionValidationError as e:
            logger.error(
                "book_sync_failed",
                kobo_id=book_id_short,
                title=book.title,
                error=str(e),
            )
            return None
        
        except Exception as e:
            logger.error(
                "book_sync_unexpected_error",
                kobo_id=book_id_short,
                title=book.title,
                error=str(e),
            )
            return None
