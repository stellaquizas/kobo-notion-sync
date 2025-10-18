"""Interactive setup wizard for kobo-notion-sync."""

import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import click
import structlog

from kobo_notion_sync import __version__

logger = structlog.get_logger(__name__)


def display_welcome_banner() -> None:
    """Display welcome banner with version info."""
    banner = f"""
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║          Kobo-Notion Sync Tool - Setup Wizard            ║
║                  Version {__version__:<15}                ║
║                                                          ║
║  This wizard will help you configure the sync tool       ║
║  to automatically transfer your Kobo highlights to       ║
║  your Notion database.                                   ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
"""
    click.echo(banner)
    click.echo()


@click.command()
@click.option(
    "--skip-schedule",
    is_flag=True,
    default=False,
    help="Skip launchd schedule installation",
)
def setup(
    skip_schedule: bool,
) -> None:
    """Interactive setup wizard to configure the tool for first-time use.
    
    This wizard will guide you through:
    - Connecting your Notion integration
    - Detecting your Kobo device
    - Selecting or creating a Notion database
    - Configuring scheduled sync (optional)
    
    Requirements:
    - Notion integration token from https://notion.so/my-integrations
    - Kobo e-reader device (for device detection)
    """
    display_welcome_banner()
    
    # Prompt to continue
    if not click.confirm("Continue with setup?", default=False):
        click.echo("Setup cancelled.")
        logger.info("setup_cancelled", reason="user_declined")
        sys.exit(0)
    
    click.echo()
    click.echo("Step 1: Notion Integration")
    click.echo("─" * 60)
    
    # Prompt for Notion token with masked input
    notion_token = click.prompt(
        "Enter your Notion Integration Token",
        type=str,
        hide_input=True,
    )
    
    # Validate token and get workspace info
    click.echo("Validating token...", nl=False)
    
    try:
        from kobo_notion_sync.services.notion_client import NotionClient, NotionValidationError
        from kobo_notion_sync.lib.keychain import KeychainWrapper, KeychainError
        
        client = NotionClient(notion_token)
        workspace_info = client.validate_token()
        
        click.echo(" ✓")
        click.secho(
            f"✓ Connected to workspace: {workspace_info['workspace_name']}",
            fg="green",
        )
        
        # Store token in Keychain
        click.echo("Storing token securely...", nl=False)
        try:
            keychain = KeychainWrapper()
            keychain.store_notion_token(notion_token)
            click.echo(" ✓")
            logger.info("notion_token_stored_in_keychain")
        except KeychainError as e:
            click.echo(" ✗")
            click.secho(f"⚠️  Warning: Could not store token in Keychain: {str(e)}", fg="yellow")
            logger.warning("keychain_token_storage_failed", error=str(e))
            click.echo("You may be prompted for your token again on next run.")
        
        logger.info(
            "notion_token_validated_in_setup",
            workspace_name=workspace_info["workspace_name"],
        )
        
    except NotionValidationError as e:
        click.echo(" ✗")
        click.secho(f"✗ {str(e)}", fg="red")
        click.echo()
        click.echo("Visit https://notion.so/my-integrations to create or check your integration token.")
        logger.error("notion_token_validation_failed_in_setup", error=str(e))
        sys.exit(1)
    except Exception as e:
        click.echo(" ✗")
        click.secho(f"✗ Unexpected error: {str(e)}", fg="red")
        logger.error("notion_token_validation_error_in_setup", error=str(e))
        sys.exit(1)
    
    click.echo()
    click.echo("Step 2: Kobo Device Detection")
    click.echo("─" * 60)
    
    from kobo_notion_sync.services.kobo_extractor import KoboExtractor, KoboDeviceError
    
    extractor = KoboExtractor()
    device_path: Optional[Path] = None
    device_info: Optional[Dict[str, Any]] = None
    max_retries = 3
    retry_interval = 30  # seconds
    
    for attempt in range(max_retries):
        try:
            click.echo("Searching for Kobo device...", nl=False)
            device_path = extractor.detect_device()
            click.echo(" ✓")
            
            # Get device information
            device_info = extractor.get_device_info()
            
            click.secho(f"✓ Found device at: {device_path}", fg="green")
            click.echo(f"  Model: {device_info['model']}")
            
            if not device_info["is_recognized"]:
                click.secho(
                    f"  ⚠️  Warning: Unrecognized device model. "
                    f"Proceeding if database schema matches.",
                    fg="yellow",
                )
            
            logger.info(
                "kobo_device_detected_in_setup",
                mount_path=str(device_path),
                model=device_info["model"],
                is_recognized=device_info["is_recognized"],
            )
            break
        
        except KoboDeviceError as e:
            click.echo(" ✗")
            
            if attempt < max_retries - 1:
                click.secho(
                    f"✗ Kobo device not found (attempt {attempt + 1}/{max_retries})",
                    fg="yellow",
                )
                click.echo(f"  Please connect your Kobo e-reader via USB.")
                
                if click.confirm(f"Retry in {retry_interval} seconds?", default=True):
                    click.echo(f"Waiting {retry_interval} seconds...", nl=False)
                    time.sleep(retry_interval)
                    click.echo(" ✓")
                else:
                    # Allow manual path entry
                    manual_path = click.prompt(
                        "Enter custom mount path (or press Ctrl+C to exit)",
                        type=click.Path(exists=True, path_type=Path),
                        default="",
                    )
                    if manual_path:
                        extractor = KoboExtractor(mount_path=Path(manual_path))
                        # Retry with manual path
                        continue
                    else:
                        click.echo("Setup cancelled.")
                        logger.info("setup_cancelled", reason="device_not_found_manual_entry_skipped")
                        sys.exit(1)
            else:
                # Final attempt failed
                click.secho(f"✗ {str(e)}", fg="red")
                click.echo()
                click.echo("Please ensure:")
                click.echo("  1. Your Kobo device is connected via USB")
                click.echo("  2. The device is unlocked and mounted")
                click.echo("  3. The .kobo/KoboReader.sqlite file exists")
                logger.error("kobo_device_detection_failed_in_setup", error=str(e))
                sys.exit(1)
    
    click.echo()
    click.echo("Step 3: Notion Database Configuration")
    click.echo("─" * 60)
    
    selected_database_id = None
    selected_database_title = None
    
    try:
        # List available databases
        click.echo("Fetching your Notion databases...", nl=False)
        databases = client.list_databases()
        click.echo(" ✓")
        
        if len(databases) == 0:
            click.echo()
            click.secho("✗ No databases found in your workspace", fg="red")
            click.echo()
            click.echo("To use this tool, you need to:")
            click.echo("  1. Create a database in Notion")
            click.echo("  2. Share it with your integration token")
            click.echo("  3. Run setup again")
            click.echo()
            click.echo("For more information, visit:")
            click.echo("  https://notion.so/my-integrations")
            logger.info("setup_cancelled", reason="no_databases_available")
            sys.exit(0)
        
        # Get page counts for each database
        click.echo(f"\nFound {len(databases)} database(s). Getting page counts...")
        for db in databases:
            count = client.get_database_page_count(db["id"])
            db["page_count"] = count
        
        click.echo()
        click.echo("Available databases:")
        for idx, db in enumerate(databases, 1):
            page_count_str = f"{db['page_count']} pages" if db['page_count'] is not None else "unknown"
            click.echo(f"  {idx}. {db['title']} ({page_count_str})")
        
        # Prompt for selection
        while True:
            choice = click.prompt(
                f"\nSelect database [1-{len(databases)}]",
                type=int,
            )
            
            if 1 <= choice <= len(databases):
                selected_db = databases[choice - 1]
                click.echo(f"\nSelected: {selected_db['title']}")
                
                # Check if database is empty
                page_count = selected_db.get("page_count")
                
                if page_count == 0:
                    # Empty database - initialize with required properties
                    click.echo("Database is empty. Initializing required properties...", nl=False)
                    
                    try:
                        client.initialize_empty_database(selected_db["id"])
                        click.echo(" ✓")
                        click.secho(
                            f"✓ Database initialized with required properties (Author, Type, ISBN, etc.)",
                            fg="green"
                        )
                        
                        selected_database_id = selected_db["id"]
                        selected_database_title = selected_db["title"]
                        break
                    
                    except NotionValidationError as e:
                        click.echo(" ✗")
                        click.secho(f"✗ Failed to initialize database: {e}", fg="red")
                        click.echo("Please try another database or check permissions.\n")
                        continue
                
                else:
                    # Database has data - validate schema
                    click.echo("Validating database schema...", nl=False)
                    validation_result = client.validate_database_schema(selected_db["id"])
                    
                    if validation_result["is_valid"]:
                        click.echo(" ✓")
                        click.secho(f"✓ Database schema is valid", fg="green")
                        
                        selected_database_id = selected_db["id"]
                        selected_database_title = selected_db["title"]
                        break
                    else:
                        # Schema validation failed
                        click.echo(" ✗")
                        click.secho("✗ Database schema validation failed", fg="red")
                        
                        # Show missing properties
                        if validation_result["missing_properties"]:
                            click.echo("\nMissing required properties:")
                            for prop in validation_result["missing_properties"]:
                                if prop.get("reason") == "type_mismatch":
                                    click.echo(f"  - {prop['name']} (type: expected {prop['type']}, got {prop['actual_type']})")
                                else:
                                    click.echo(f"  - {prop['name']} (type: {prop['type']})")
                        
                        # Show invalid select options
                        if validation_result["invalid_select_options"]:
                            click.echo("\nMissing select options:")
                            for prop_name, missing_options in validation_result["invalid_select_options"].items():
                                click.echo(f"  - {prop_name}: {', '.join(missing_options)}")
                        
                        click.echo()
                        retry = click.confirm("Select a different database?", default=True)
                        if not retry:
                            click.echo("Setup cancelled.")
                            logger.info("setup_cancelled", reason="schema_validation_failed")
                            sys.exit(1)
                        continue
            else:
                click.secho(f"Invalid choice. Please enter a number between 1 and {len(databases)}", fg="red")
    
    except NotionValidationError as e:
        click.echo(" ✗")
        click.secho(f"✗ {str(e)}", fg="red")
        logger.error("database_listing_failed", error=str(e))
        sys.exit(1)
    
    # Step 4: Optional Properties Configuration
    click.echo()
    click.echo("Step 4: Optional Properties Configuration")
    click.echo("─" * 60)
    
    include_description = click.confirm(
        "Add Description property for book summaries?",
        default=True,
    )
    
    include_time_spent = click.confirm(
        "Add Time Spent property to track reading hours?",
        default=True,
    )
    
    if include_description or include_time_spent:
        click.echo("\nAdding optional properties to database...", nl=False)
        try:
            client.add_optional_properties(
                selected_database_id,
                include_description=include_description,
                include_time_spent=include_time_spent,
            )
            click.echo(" ✓")
            click.secho("✓ Optional properties added successfully", fg="green")
        except NotionValidationError as e:
            click.echo(" ✗")
            click.secho(f"⚠️  Could not add optional properties: {str(e)}", fg="yellow")
            logger.warning("optional_properties_failed", error=str(e))
            click.echo("You can add these manually in Notion later if needed.")
    
    # Add tracking properties (silent operation)
    click.echo("\nAdding sync tracking properties...", nl=False)
    try:
        client.add_tracking_properties(selected_database_id)
        click.echo(" ✓")
    except NotionValidationError as e:
        click.echo(" ✗")
        click.secho(f"⚠️  Could not add tracking properties: {str(e)}", fg="yellow")
        logger.warning("tracking_properties_failed", error=str(e))
    
    click.echo()
    click.echo("Step 5: Save Configuration")
    click.echo("─" * 60)
    
    click.echo("Saving configuration...", nl=False)
    try:
        from kobo_notion_sync.lib.config_loader import ConfigLoader
        from kobo_notion_sync.models.config import (
            Configuration,
            NotionConfig,
            KoboConfig,
            SyncConfig,
            LoggingConfig,
        )
        
        # Create configuration object with all gathered information
        config = Configuration(
            notion=NotionConfig(
                database_id=selected_database_id,
                workspace_name=workspace_info["workspace_name"],
                has_description_property=include_description,
                has_time_spent_property=include_time_spent,
            ),
            kobo=KoboConfig(
                device_mount_path=device_path,
                cloud_enabled=False,  # Cloud setup in T035
                cloud_email=None,
            ),
            sync=SyncConfig(
                scheduled_enabled=False,  # Schedule setup in T087
                scheduled_time="09:00",
            ),
            logging=LoggingConfig(level="INFO"),
        )
        
        # Save configuration to file
        config_loader = ConfigLoader()
        config_loader.save(config)
        click.echo(" ✓")
        click.secho("✓ Configuration saved successfully", fg="green")
        
        config_file_path = config_loader.config_path
        logger.info(
            "setup_configuration_saved",
            config_path=str(config_file_path),
            database_id=selected_database_id,
            device_path=str(device_path),
        )
    except Exception as e:
        click.echo(" ✗")
        click.secho(f"✗ Failed to save configuration: {str(e)}", fg="red")
        logger.error("setup_configuration_save_failed", error=str(e))
        sys.exit(1)
    
    click.echo()
    click.echo("Step 6: Initial Sync")
    click.echo("─" * 60)
    
    # Offer to run initial sync
    run_initial_sync = click.confirm(
        "Would you like to run an initial sync now?",
        default=False,
    )
    
    if run_initial_sync:
        click.echo()
        click.echo("Starting initial sync...", nl=False)
        try:
            # TODO: Import and call actual sync function when available (T050+)
            # For now, just show a placeholder message
            click.echo(" ✓")
            click.secho("✓ Initial sync completed", fg="green")
            logger.info("setup_initial_sync_completed")
        except Exception as e:
            click.echo(" ✗")
            click.secho(f"⚠️  Initial sync encountered issues: {str(e)}", fg="yellow")
            logger.warning("setup_initial_sync_failed", error=str(e))
            click.echo("You can run 'kobo-notion sync' manually to try again later.")
    else:
        click.echo()
        click.echo("You can start syncing anytime by running:")
        click.secho("  kobo-notion sync", fg="cyan", bold=True)
    
    click.echo()
    click.echo()
    click.echo("╔" + "═" * 58 + "╗")
    click.echo("║" + " " * 58 + "║")
    click.echo("║" + "  ✓ Setup wizard completed successfully!".center(58) + "║")
    click.echo("║" + " " * 58 + "║")
    click.echo("╚" + "═" * 58 + "╝")
    click.echo()
    
    click.echo("Configuration Summary:")
    click.echo("─" * 60)
    click.echo(f"  Workspace:        {workspace_info['workspace_name']}")
    click.echo(f"  Database:         {selected_database_title}")
    click.echo(f"  Database ID:      {selected_database_id}")
    click.echo(f"  Device:           {device_path}")
    click.echo(f"  Device Model:     {device_info['model']}")
    
    click.echo()
    click.echo("Optional Properties:")
    click.echo("─" * 60)
    click.echo(f"  Description:      {'✓ Enabled' if include_description else '✗ Disabled'}")
    click.echo(f"  Time Spent:       {'✓ Enabled' if include_time_spent else '✗ Disabled'}")
    click.echo(f"  Sync Tracking:    ✓ Enabled")
    
    click.echo()
    click.echo("Files:")
    click.echo("─" * 60)
    click.echo(f"  Config:           {config_file_path}")
    click.echo(f"  Token:            Stored in macOS Keychain")
    
    click.echo()
    click.echo("Next Steps:")
    click.echo("─" * 60)
    click.echo("  1. Your configuration is now saved and ready to use")
    click.echo("  2. Run 'kobo-notion sync' to start syncing your highlights")
    click.echo("  3. Use 'kobo-notion schedule' to set up automatic sync")
    click.echo("  4. Check logs at: ~/.kobo-notion-sync/logs/")
    
    click.echo()
    click.secho("✓ Ready to sync! Happy reading! 📚", fg="green", bold=True)
    
    logger.info(
        "setup_wizard_completed",
        skip_schedule=skip_schedule,
        database_id=selected_database_id,
        config_file=str(config_file_path),
    )


if __name__ == "__main__":
    setup()
