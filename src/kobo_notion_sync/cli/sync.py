"""Sync command for manual full sync from Kobo to Notion."""

import sys
from datetime import datetime
from pathlib import Path

import click
import structlog

from kobo_notion_sync.lib.config_loader import ConfigLoader, ConfigurationError
from kobo_notion_sync.lib.keychain import KeychainWrapper
from kobo_notion_sync.lib.lock_manager import LockManager, SyncInProgressError
from kobo_notion_sync.lib.logger import setup_console_logging
from kobo_notion_sync.lib.notifications import NotificationService
from kobo_notion_sync.services.kobo_extractor import KoboExtractor
from kobo_notion_sync.services.notion_client import NotionClient, NotionValidationError
from kobo_notion_sync.services.sync_manager import SyncManager, SyncError

logger = structlog.get_logger(__name__)


@click.command()
@click.option("--full", is_flag=True, help="Force full sync (bypass cache)")
@click.option("--dry-run", is_flag=True, help="Preview changes without syncing")
@click.option("--no-notification", is_flag=True, help="Disable desktop notifications")
def sync(full: bool, dry_run: bool, no_notification: bool) -> None:
    """
    Sync highlights from Kobo device to Notion database.

    This command performs a manual full sync by:
    1. Checking for Kobo device connected via USB
    2. Reading highlights and metadata from device
    3. Comparing with cache to find new highlights
    4. Creating/updating Notion pages with highlights and metadata
    5. Showing sync summary with statistics

    Exit codes:
    - 0: Sync successful
    - 1: Lock held by another sync or other error
    - 2: Configuration error
    """
    try:
        # Set up logging
        setup_console_logging()
        
        # Get config directory
        config_dir = Path.home() / ".kobo-notion-sync"
        
        # Step 1: Pre-flight lock check (FR-038A)
        logger.info("sync_command_started", full=full, dry_run=dry_run)
        try:
            lock_manager = LockManager(config_dir)
            lock_manager.acquire()
            logger.info("sync_lock_acquired")
        except SyncInProgressError as e:
            click.echo(
                f"Error: {e}",
                err=True,
            )
            logger.error("sync_lock_failed", error=str(e))
            sys.exit(1)
        
        try:
            # Step 2: Load configuration
            try:
                config_loader = ConfigLoader()
                config = config_loader.load()
                logger.info("config_loaded")
            except ConfigurationError as e:
                click.echo(
                    f"Configuration error: {e}\n\n"
                    "Please run: kobo-notion setup",
                    err=True,
                )
                logger.error("config_load_failed", error=str(e))
                sys.exit(2)
            
            # Step 3: Retrieve Notion token from keychain
            keychain = KeychainWrapper()
            try:
                notion_token = keychain.get_notion_token()
                if not notion_token:
                    click.echo(
                        "Notion token not found in keychain.\n"
                        "Please run: kobo-notion setup",
                        err=True,
                    )
                    logger.error("notion_token_not_found")
                    sys.exit(2)
            except Exception as e:
                click.echo(
                    f"Failed to retrieve Notion token: {e}",
                    err=True,
                )
                logger.error("notion_token_retrieval_failed", error=str(e))
                sys.exit(2)
            
            # Step 4: Initialize services and run sync (T043-T070)
            try:
                kobo_extractor = KoboExtractor()
                notion_client = NotionClient(token=notion_token)
                sync_manager = SyncManager(
                    kobo_extractor=kobo_extractor,
                    notion_client=notion_client,
                    config=config,
                )
                
                logger.info("services_initialized")
                
                # Run sync
                sync_session = sync_manager.sync_full(
                    full_mode=full,
                    dry_run=dry_run,
                )
                
                # Display results
                click.echo(f"\n{'='*50}")
                click.echo("Sync Summary")
                click.echo(f"{'='*50}")
                click.echo(f"Mode: {'Full' if full else 'Incremental'}")
                click.echo(f"Dry Run: {'Yes' if dry_run else 'No'}")
                click.echo(f"Books processed: {sync_session.books_processed}")
                click.echo(f"Highlights synced: {sync_session.highlights_synced}")
                click.echo(f"Highlights skipped (cached): {sync_session.highlights_skipped}")
                if sync_session.cache_hits > 0 or sync_session.cache_misses > 0:
                    click.echo(f"Cache hits (duplicates): {sync_session.cache_hits}")
                    click.echo(f"Cache misses (new): {sync_session.cache_misses}")
                    click.echo(f"Deduplication rate: {sync_session.deduplication_rate:.1f}%")
                
                if sync_session.errors:
                    click.echo(f"\nErrors encountered ({len(sync_session.errors)}):")
                    for error in sync_session.errors:
                        click.echo(f"  - {error}")
                    click.echo("\nSync completed with errors")
                    exit_code = 1
                else:
                    click.echo(f"\nStatus: ✓ Completed in {sync_session.duration_seconds:.1f}s")
                    exit_code = 0
                
                # Step 5: Send desktop notification (T067, FR-042, FR-016)
                if not no_notification:
                    try:
                        notif_service = NotificationService()
                        
                        # Build notification message
                        if dry_run:
                            mode_str = "Dry run"
                        elif full:
                            mode_str = "Full sync"
                        else:
                            mode_str = "Incremental sync"
                        
                        # Build deduplication message (T072)
                        dedup_msg = ""
                        if sync_session.cache_hits > 0 or sync_session.cache_misses > 0:
                            dedup_msg = f"\n({sync_session.cache_misses} new, {sync_session.cache_hits} skipped)"
                        
                        if sync_session.errors:
                            title = "Sync completed with errors"
                            message = (
                                f"{mode_str}: {sync_session.highlights_synced} highlights "
                                f"from {sync_session.books_processed} books in {sync_session.duration_seconds:.1f}s"
                                f"{dedup_msg}\n"
                                f"⚠️  {len(sync_session.errors)} error(s) occurred"
                            )
                            notif_service.show_error(title=title, message=message)
                        else:
                            title = "Sync completed successfully"
                            message = (
                                f"{mode_str}: {sync_session.highlights_synced} highlights "
                                f"from {sync_session.books_processed} books in {sync_session.duration_seconds:.1f}s"
                                f"{dedup_msg}"
                            )
                            notif_service.show_success(title=title, message=message)
                        
                        logger.info("sync_notification_sent", title=title)
                    except Exception as e:
                        logger.warning("notification_failed", error=str(e))
                
                sys.exit(exit_code)
            
            except NotionValidationError as e:
                click.echo(f"Notion API error: {e}", err=True)
                logger.error("notion_validation_failed", error=str(e))
                sys.exit(1)
            
            except SyncError as e:
                click.echo(f"Sync error: {e}", err=True)
                logger.error("sync_failed", error=str(e))
                sys.exit(1)
            
            except Exception as e:
                click.echo(f"Service initialization failed: {e}", err=True)
                logger.exception("service_initialization_failed", error=str(e))
                sys.exit(1)
        
        finally:
            # Always release lock
            lock_manager.release()
            logger.info("sync_lock_released")
    
    except Exception as e:
        click.echo(
            f"Unexpected error: {e}",
            err=True,
        )
        logger.exception("sync_unexpected_error", error=str(e))
        sys.exit(1)
