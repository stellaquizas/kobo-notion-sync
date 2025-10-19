# Kobo-Notion Sync

Automatically sync your Kobo e-reader highlights and annotations to Notion.

## Features

- 📖 Extract highlights from Kobo e-reader via USB
- 📝 Sync highlights to Notion database
- 🧠 **Smart Sync** - Intelligently detects reading activity and only updates books you've re-read
- 🔄 Deduplication to prevent duplicates
- 📊 Reading period tracking - Automatically records when you read each book and for how long
- 🎨 Automatic book cover retrieval
- 🔒 Secure credential storage in macOS Keychain
- 💬 Desktop notifications for sync status

## Requirements

- macOS 10.15+ (Catalina or later)
- Python 3.11+
- Kobo e-reader (Libra 2, Clara 2E, or compatible 2020+ model)
- Notion account with workspace admin permissions

## Installation

### For Users

```bash
# Install directly from GitHub using uv
uv tool install git+https://github.com/stellaquizas/kobo-notion-sync.git

# Verify installation
kobo-notion --version
```

### For Developers

```bash
# Clone the repository
git clone https://github.com/stellaquizas/kobo-notion-sync.git
cd kobo-notion-sync

# Install dependencies
poetry install

# Verify installation
poetry run kobo-notion --version
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
kobo-notion setup
```

Follow the prompts to:

- Enter your Notion integration token
- Connect your Kobo device
- Select or create a Notion database
- Configure optional features

### 3. Sync Highlights and Metadata

```bash
# Connect your Kobo via USB, then run:
kobo-notion sync
```

## Usage

### Commands

```bash
# Interactive setup wizard
kobo-notion setup

# Sync highlights and metadata (default smart sync)
kobo-notion sync

# Re-sync all highlights and metadata (ignore smart sync logic)
kobo-notion sync --full

# Preview changes without syncing
kobo-notion sync --dry-run

# Disable notifications
kobo-notion sync --no-notification

# Show version
kobo-notion --version
```

## Smart Sync Feature

The Smart Sync system intelligently tracks reading activity to optimize sync performance and keep your Notion database up-to-date:

### How It Works

- **New Books**: Automatically creates Notion entries for books not yet in your database
- **Unchanged Books**: Skips books you haven't read since the last sync (based on last read date comparison)
- **Re-read Books**: When you open a book and continue reading, the tool detects this and updates all fields and highlights

### Reading Period Tracking

The system tracks your complete reading timeline for each book:

- **First Read Date** - The day you first opened the book on your Kobo (LastTimeStartedReading)
- **Last Read Date** - The most recent day you opened the book (DateLastRead)
- **Reading Period** - Calculated duration between first and last read dates

**For new books**, highlights display:
`Started: YYYY-MM-DD to Last: YYYY-MM-DD (X days)`

**When you re-read a book**, highlights are updated with the new reading session dates:
`YYYY-MM-DD to YYYY-MM-DD (X days)` (previous session to current session)

This gives you a complete history of your reading timeline and how long you spent on each book.

### Sync Status Notifications

After each sync, you'll see a summary showing:

- Number of new books added
- Number of books re-read and updated
- Number of unchanged books skipped

### Manual Full Sync

If you need to re-sync all highlights and metadata regardless of reading activity:

```bash
kobo-notion sync --full
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

Your Notion database will be initialized with these properties:

- **Name** (Title) - Book title
- **Author** (Text) - Book author
- **Type** (Select) - Entry type ("Kobo" for synced books)
- **ISBN** (Text) - International Standard Book Number
- **Publisher** (Text) - Book publisher
- **Description** (Text) - Book summary
- **Status** (Select) - Reading status: "New", "Reading", "Finished"
- **Progress** (Number/Percent) - Reading progress (0-100%)
- **Time Spent** (Number) - Reading time in minutes
- **Kobo Content ID** (Text) - Internal tracking field
- **Last Sync Time** (Date) - Last sync timestamp
- **Highlights Count** (Number) - Number of synced highlights

**Cover Images:** Book covers are set as page covers (not a database property). To display covers in Gallery view, configure your view settings to use "Page cover" as the card preview source.

The setup wizard automatically creates these properties for empty databases.

## Manual Notion Configuration

After the initial sync, configure your Notion database for optimal viewing. These settings are **not available via the Notion API** and must be set manually in Notion:

### 1. Switch to Gallery View

- Open your Notion database
- Click the `+ Add a view` button
- Select **Gallery**
- This gives you a beautiful card-based view of your books

### 2. Order Columns (in Table View)

- In your table view, drag and drop column headers to reorder them
- Suggested order: **Name → Author → Status → Progress → Type → ISBN → Publisher**
- Pin frequently-used columns by right-clicking the header

### 3. Set Page Cover to Card Preview

In Gallery view:

- Click the **Settings** (gear icon) at the top
- Go to **Card preview** section
- Select **Page cover** to display book covers on cards
- Optional: Choose which properties appear on the card (e.g., Author, Status, Progress)

### 4. Adjust Property Visibility

- In your table view, right-click any column header
- Select **Hide** to hide properties you don't need (e.g., Kobo Content ID)
- Unhide important properties like Description and Reading Period
- Properties like **Time Spent** and **Description** are optional and can be toggled based on your needs

### Why These Are Manual

The Notion API does not yet support:

- ❌ Creating or switching view types (Gallery, Table, Calendar, etc.)
- ❌ Reordering database columns
- ❌ Setting card preview sources
- ❌ Managing property visibility per view

These features must be configured directly in the Notion interface for now.

## Troubleshooting

### Kobo device not detected

Ensure your Kobo is connected via USB and mounted at `/Volumes/KOBOeReader`.

```bash
# Check if Kobo is mounted
ls /Volumes/ | grep -i kobo
```

### Notion token invalid

Run `kobo-notion setup` again to re-enter your token.

Verify you're using an **Internal Integration Token**, not an API key.

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

📚 Happy reading!
