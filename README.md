# Kobo-Notion Sync

Automatically sync your Kobo e-reader highlights and annotations to Notion.

## Features

- 📖 Extract highlights from Kobo e-reader via USB
- 📝 Sync highlights to Notion database
- 🔄 Deduplication to prevent duplicates
- 🎨 Automatic book cover retrieval
- 🔔 Optional sync reminders (macOS)
- 🔒 Secure credential storage in macOS Keychain

## Requirements

- macOS 10.15+ (Catalina or later)
- Python 3.11+
- Kobo e-reader (Libra 2, Clara 2E, or compatible 2020+ model)
- Notion account with workspace admin permissions

## Installation

### Using Poetry (Development)

```bash
# Clone the repository
git clone https://github.com/stellaquizas/kobo-notion-sync.git
cd kobo-notion-sync

# Install dependencies
poetry install

# Verify installation
poetry run kobo-notion --version
```

### Using pip (User Installation)

```bash
pip install kobo-notion-sync
```

## Quick Start

### 1. Set up Notion Integration

1. Go to https://notion.so/my-integrations
2. Click "+ New integration"
3. Give it a name (e.g., "Kobo Sync")
4. Copy the "Internal Integration Token"
5. Share your Notion database with the integration

### 2. Run Setup Wizard

```bash
poetry run kobo-notion setup
```

Follow the prompts to:

- Enter your Notion integration token
- Connect your Kobo device
- Select or create a Notion database
- Configure optional features

### 3. Sync Highlights

```bash
# Connect your Kobo via USB, then run:
poetry run kobo-notion sync
```

## Usage

### Commands

```bash
# Interactive setup wizard
poetry run kobo-notion setup

# Sync highlights (manual)
poetry run kobo-notion sync

# Force full sync (bypass cache)
poetry run kobo-notion sync --full

# Preview changes without syncing
poetry run kobo-notion sync --dry-run

# Disable notifications
poetry run kobo-notion sync --no-notification

# Show version
poetry run kobo-notion --version
```

## Configuration

Configuration is stored in `~/.kobo-notion-sync/config.toml`.

```toml
[notion]
database_id = "your-database-id"
workspace_name = "Your Workspace"

[kobo]
device_mount_path = "/Volumes/KOBOeReader"

[logging]
level = "INFO"
```

## Notion Database Schema

Your Notion database should have these properties:

- **Name** (Title) - Book title
- **Category** (Select) - Book category
- **Date Done #1** (Date) - Completion date
- **Image** (Files & Media) - Book cover
- **Progress Code** (Select) - Must have: "New", "Reading", "Completed"
- **Type** (Select) - Must have: "Kobo"

The setup wizard can help create these properties automatically.

## Troubleshooting

### Kobo device not detected

Ensure your Kobo is connected via USB and mounted at `/Volumes/KOBOeReader`.

```bash
# Check if Kobo is mounted
ls /Volumes/ | grep -i kobo
```

### Notion token invalid

Run `poetry run kobo-notion setup` again to re-enter your token.

Verify you're using an **Internal Integration Token** (starts with `secret_`), not an API key.

### Check logs

View logs at `~/.kobo-notion-sync/logs/sync.log`:

```bash
tail -f ~/.kobo-notion-sync/logs/sync.log
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- Built with [Click](https://click.palletsprojects.com/)
- Uses [Notion SDK](https://github.com/ramnes/notion-sdk-py)
- Inspired by the Kobo reading community
