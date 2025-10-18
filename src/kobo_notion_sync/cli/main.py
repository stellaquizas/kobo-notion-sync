"""Main CLI application for kobo-notion-sync."""

import click

from kobo_notion_sync import __version__
from kobo_notion_sync.cli.setup import setup as setup_command


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """Kobo-Notion Sync Tool - Sync Kobo e-reader highlights to Notion."""
    pass


# Register setup command
cli.add_command(setup_command, name="setup")


@cli.command()
@click.option("--full", is_flag=True, help="Force full sync (bypass cache)")
@click.option("--dry-run", is_flag=True, help="Preview changes without syncing")
@click.option("--no-notification", is_flag=True, help="Disable desktop notifications")
def sync(full: bool, dry_run: bool, no_notification: bool) -> None:
    """Sync highlights from Kobo to Notion."""
    click.echo("Sync command coming soon...")


if __name__ == "__main__":
    cli()
