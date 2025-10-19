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


@cli.command()
def help_command() -> None:
    """Show detailed help about all available commands."""
    banner = f"""
╔══════════════════════════════════════════════════════════╗
║                                                          ║
║          Kobo-Notion Sync Tool - Help                    ║
║                  Version {__version__:<15}                 ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝

📋 AVAILABLE COMMANDS
"""
    click.echo(banner)
    
    commands_info = [
        {
            "name": "setup",
            "description": "Interactive setup wizard for first-time configuration",
            "usage": "kobo-notion setup",
            "details": [
                "Configure Notion integration token",
                "Detect Kobo device",
                "Select or create Notion database",
                "Configure optional properties",
                "Run initial sync",
            ]
        },
        {
            "name": "sync",
            "description": "Manual sync from Kobo device to Notion database",
            "usage": "kobo-notion sync [OPTIONS]",
            "details": [
                "--full        Delete all existing books and re-import fresh from Kobo",
                "--dry-run     Preview changes without actually syncing",
                "--no-notification  Disable desktop notifications",
            ]
        },
        {
            "name": "help",
            "description": "Show this help message",
            "usage": "kobo-notion help",
            "details": []
        }
    ]
    
    for cmd in commands_info:
        click.secho(f"\n{cmd['name'].upper()}", fg="cyan", bold=True)
        click.echo("─" * 60)
        click.echo(f"Description: {cmd['description']}")
        click.echo(f"Usage:       {cmd['usage']}")
        
        if cmd['details']:
            click.echo("Options:")
            for detail in cmd['details']:
                click.echo(f"  {detail}")
    
    click.echo("\n")
    click.echo("╔" + "═" * 58 + "╗")
    click.echo("║" + "QUICK START GUIDE".center(58) + "║")
    click.echo("╚" + "═" * 58 + "╝")
    click.echo()
    click.echo("1️⃣  First time setup:")
    click.secho("   kobo-notion setup", fg="cyan", bold=True)
    click.echo()
    click.echo("2️⃣  Run sync after setup:")
    click.secho("   kobo-notion sync", fg="cyan", bold=True)
    click.echo()
    click.echo("3️⃣  Preview changes before syncing:")
    click.secho("   kobo-notion sync --dry-run", fg="cyan", bold=True)
    click.echo()
    click.echo("4️⃣  Re-import all books (full sync):")
    click.secho("   kobo-notion sync --full", fg="cyan", bold=True)
    click.echo()
    
    click.echo("\n📚 REQUIREMENTS")
    click.echo("─" * 60)
    click.echo("• Kobo e-reader device connected via USB")
    click.echo("• Notion integration token (from https://notion.so/my-integrations)")
    click.echo("• Python 3.11 or higher")
    click.echo()
    
    click.echo("\n📖 DOCUMENTATION")
    click.echo("─" * 60)
    click.echo("Repository: https://github.com/stellaquizas/kobo-notion-sync")
    click.echo("Issues:     https://github.com/stellaquizas/kobo-notion-sync/issues")
    click.echo("Logs:       ~/.kobo-notion-sync/logs/")
    click.echo()
    
    click.echo("\nFor more info: kobo-notion --help")
    click.echo()


# Register setup command
cli.add_command(setup_command, name="setup")

# Register sync command
cli.add_command(sync_command, name="sync")

# Register help command
cli.add_command(help_command, name="help")


if __name__ == "__main__":
    cli()

