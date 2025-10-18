"""Main CLI application for kobo-notion-sync."""

import click

from kobo_notion_sync import __version__
from kobo_notion_sync.cli.setup import setup as setup_command
from kobo_notion_sync.cli.sync import sync as sync_command


@click.group()
@click.version_option(version=__version__)
def cli() -> None:
    """Kobo-Notion Sync Tool - Sync Kobo e-reader highlights to Notion."""
    pass


# Register setup command
cli.add_command(setup_command, name="setup")

# Register sync command
cli.add_command(sync_command, name="sync")


if __name__ == "__main__":
    cli()

