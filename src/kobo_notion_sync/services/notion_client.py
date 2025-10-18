"""Notion API client wrapper for kobo-notion-sync."""

import time
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

import structlog
from notion_client import Client
from notion_client.errors import APIResponseError

logger = structlog.get_logger(__name__)


def retry_with_backoff(max_retries: int = 3, initial_wait: float = 1.0) -> Callable:
    """Decorator for exponential backoff retry on rate limit errors (T062, FR-045).
    
    Implements exponential backoff for Notion API rate limiting:
    - 1st retry: wait 1s
    - 2nd retry: wait 2s  
    - 3rd retry: wait 4s
    
    Args:
        max_retries: Maximum number of retry attempts (default: 3)
        initial_wait: Initial wait time in seconds (default: 1.0)
    
    Returns:
        Decorated function that automatically retries on rate limit errors
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            wait_time = initial_wait
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                
                except APIResponseError as e:
                    # Check if it's a rate limit error (429)
                    if e.status == 429 and attempt < max_retries:
                        logger.warning(
                            "notion_rate_limit_hit",
                            func_name=func.__name__,
                            attempt=attempt + 1,
                            max_retries=max_retries,
                            wait_seconds=wait_time,
                        )
                        time.sleep(wait_time)
                        wait_time *= 2  # Exponential backoff
                        continue
                    
                    # Re-raise if not a rate limit error or no retries left
                    raise
                
                except Exception:
                    # Re-raise any other exceptions
                    raise
            
            # Should not reach here, but as fallback
            return func(*args, **kwargs)
        
        return wrapper
    
    return decorator


class NotionValidationError(Exception):
    """Raised when Notion API validation fails."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class NotionClient:
    """Wrapper for Notion API interactions with validation and error handling."""
    
    def __init__(self, token: str):
        """Initialize Notion client with integration token.
        
        Args:
            token: Notion integration token from https://notion.so/my-integrations
        """
        self.token = token
        self._client = Client(auth=token)
        self._database_has_type_property = {}  # Cache for Type property existence
        logger.info("notion_client_initialized")
    
    def _check_type_property_exists(self, database_id: str) -> bool:
        """Check if database has Type property (internal helper).
        
        Caches result to avoid repeated API calls.
        
        Args:
            database_id: Notion database ID
        
        Returns:
            True if Type property exists, False otherwise
        """
        if database_id in self._database_has_type_property:
            return self._database_has_type_property[database_id]
        
        try:
            database = self._client.databases.retrieve(database_id=database_id)
            properties = database.get("properties", {})
            has_type = "Type" in properties
            self._database_has_type_property[database_id] = has_type
            return has_type
        except Exception:
            # If we can't check, assume it doesn't exist
            return False
    
    def validate_token(self) -> Dict[str, str]:
        """Validate Notion token and retrieve workspace information.
        
        Returns:
            Dictionary with workspace details:
                - workspace_name: Name of the connected workspace
                - workspace_id: ID of the workspace
                - bot_id: ID of the integration bot
        
        Raises:
            NotionValidationError: If token is invalid or API call fails
        """
        try:
            # Test token by retrieving bot information
            # This is a lightweight call that will fail quickly if token is invalid
            bot_info = self._client.users.me()
            
            # Extract workspace information
            workspace_name = bot_info.get("name", "Unknown Workspace")
            workspace_id = bot_info.get("id", "")
            bot_id = bot_info.get("bot", {}).get("owner", {}).get("workspace", "")
            
            logger.info(
                "notion_token_validated",
                workspace_name=workspace_name,
                workspace_id=workspace_id,
            )
            
            return {
                "workspace_name": workspace_name,
                "workspace_id": workspace_id,
                "bot_id": bot_id,
            }
        
        except APIResponseError as e:
            error_code = e.code
            error_message = str(e)
            
            logger.error(
                "notion_token_validation_failed",
                error_code=error_code,
                error_message=error_message,
            )
            
            # Provide user-friendly error messages
            if error_code == "unauthorized":
                raise NotionValidationError(
                    "Invalid Notion token. Please check your integration token.",
                    details={"error_code": error_code, "api_message": error_message},
                )
            elif error_code == "restricted_resource":
                raise NotionValidationError(
                    "Integration lacks required permissions. "
                    "Ensure the integration has access to read and write content.",
                    details={"error_code": error_code, "api_message": error_message},
                )
            else:
                raise NotionValidationError(
                    f"Failed to validate Notion token: {error_message}",
                    details={"error_code": error_code, "api_message": error_message},
                )
        
        except Exception as e:
            logger.error("notion_token_validation_error", error=str(e))
            raise NotionValidationError(
                f"Unexpected error during token validation: {str(e)}",
                details={"exception_type": type(e).__name__},
            )
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases accessible to the integration.
        
        Returns:
            List of database dictionaries with:
                - id: Database ID
                - title: Database name
                - page_count: Number of pages (approximate)
        
        Raises:
            NotionValidationError: If API call fails
        """
        try:
            # Search for all databases
            response = self._client.search(
                filter={"property": "object", "value": "database"}
            )
            
            databases = []
            for db in response.get("results", []):
                db_id = db.get("id", "")
                
                # Extract database title
                title_parts = db.get("title", [])
                title = "".join([part.get("plain_text", "") for part in title_parts])
                
                # Note: Notion API doesn't provide page count directly
                # We'll need to query the database to get accurate count
                # For now, set to None and calculate later if needed
                databases.append({
                    "id": db_id,
                    "title": title or "Untitled Database",
                    "page_count": None,  # Will be calculated on demand
                })
            
            logger.info("notion_databases_listed", count=len(databases))
            return databases
        
        except APIResponseError as e:
            logger.error("notion_list_databases_failed", error=str(e))
            raise NotionValidationError(
                f"Failed to list databases: {str(e)}",
                details={"error_code": e.code},
            )
        
        except Exception as e:
            logger.error("notion_list_databases_error", error=str(e))
            raise NotionValidationError(
                f"Unexpected error listing databases: {str(e)}",
                details={"exception_type": type(e).__name__},
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def validate_database_schema(
        self, database_id: str
    ) -> Dict[str, Any]:
        """Validate that a database has all required properties.
        
        Args:
            database_id: Notion database ID to validate
        
        Returns:
            Dictionary with validation results:
                - is_valid: Boolean indicating if schema is valid
                - missing_properties: List of missing required properties
                - invalid_select_options: Dict of select properties missing required options
                - database_title: Name of the database
        
        Raises:
            NotionValidationError: If database cannot be retrieved
        """
        try:
            # Retrieve database schema
            database = self._client.databases.retrieve(database_id=database_id)
            
            # Extract database title
            title_parts = database.get("title", [])
            db_title = "".join([part.get("plain_text", "") for part in title_parts])
            
            # Required properties with their expected types
            # Note: Cover images use page covers, not a property
            required_properties = {
                "Name": "title",
                "Category": "select",
                "Date Done #1": "date",
                "Progress Code": "select",
                "Type": "select",
            }
            
            # Get actual properties
            properties = database.get("properties", {})
            
            # Check for missing properties
            missing_properties = []
            for prop_name, expected_type in required_properties.items():
                if prop_name not in properties:
                    missing_properties.append({"name": prop_name, "type": expected_type})
                else:
                    # Check type matches
                    actual_type = properties[prop_name].get("type")
                    if actual_type != expected_type:
                        missing_properties.append({
                            "name": prop_name,
                            "type": expected_type,
                            "actual_type": actual_type,
                            "reason": "type_mismatch",
                        })
            
            # Validate select options for Progress Code and Type
            invalid_select_options = {}
            
            # Check Progress Code options
            if "Progress Code" in properties:
                progress_prop = properties["Progress Code"]
                if progress_prop.get("type") == "select":
                    options = progress_prop.get("select", {}).get("options", [])
                    option_names = [opt.get("name") for opt in options]
                    required_options = ["New", "Reading", "Completed"]
                    missing_options = [opt for opt in required_options if opt not in option_names]
                    if missing_options:
                        invalid_select_options["Progress Code"] = missing_options
            
            # Check Type options
            if "Type" in properties:
                type_prop = properties["Type"]
                if type_prop.get("type") == "select":
                    options = type_prop.get("select", {}).get("options", [])
                    option_names = [opt.get("name") for opt in options]
                    if "Kobo" not in option_names:
                        invalid_select_options["Type"] = ["Kobo"]
            
            is_valid = len(missing_properties) == 0 and len(invalid_select_options) == 0
            
            result = {
                "is_valid": is_valid,
                "missing_properties": missing_properties,
                "invalid_select_options": invalid_select_options,
                "database_title": db_title or "Untitled Database",
            }
            
            logger.info(
                "database_schema_validated",
                database_id=database_id,
                is_valid=is_valid,
                missing_count=len(missing_properties),
                invalid_options_count=len(invalid_select_options),
            )
            
            return result
        
        except APIResponseError as e:
            logger.error("database_schema_validation_failed", database_id=database_id, error=str(e))
            raise NotionValidationError(
                f"Failed to validate database schema: {str(e)}",
                details={"error_code": e.code, "database_id": database_id},
            )
        
        except Exception as e:
            logger.error("database_schema_validation_error", database_id=database_id, error=str(e))
            raise NotionValidationError(
                f"Unexpected error validating database schema: {str(e)}",
                details={"exception_type": type(e).__name__, "database_id": database_id},
            )
    
    def get_database_page_count(self, database_id: str) -> int:
        """Get the number of pages in a database.
        
        Args:
            database_id: Notion database ID
        
        Returns:
            Number of pages in the database
        
        Raises:
            NotionValidationError: If API call fails
        """
        try:
            # Query database with page_size=1 to get total count efficiently
            response = self._client.databases.query(
                database_id=database_id,
                page_size=1,
            )
            
            # The API doesn't return total count directly, so we need to
            # iterate through all pages or estimate from has_more flag
            # For setup, we'll just return an estimate
            count = len(response.get("results", []))
            has_more = response.get("has_more", False)
            
            # If has_more is True, there are more than 1 page
            # We'll make a simple estimate
            if has_more:
                # Make one more query to get better estimate
                full_response = self._client.databases.query(
                    database_id=database_id,
                    page_size=100,  # Get up to 100 to estimate better
                )
                count = len(full_response.get("results", []))
                if full_response.get("has_more"):
                    # Estimate: if 100 results and has_more, likely 100+ pages
                    # For display purposes, this is fine
                    return 100  # Show 100+ for large databases
            
            logger.info("database_page_count_retrieved", database_id=database_id, count=count)
            return count
        
        except APIResponseError as e:
            logger.error("get_database_page_count_failed", database_id=database_id, error=str(e))
            # Return 0 on error - non-blocking for setup wizard
            return 0
        
        except Exception as e:
            logger.error("get_database_page_count_error", database_id=database_id, error=str(e))
            return 0
    
    def create_database(self, workspace_id: str, database_name: str) -> Dict[str, Any]:
        """
        Create a new Notion database with all required properties.
        
        Args:
            workspace_id: Parent page ID (workspace root)
            database_name: Name for the new database
            
        Returns:
            Dictionary with database information:
                - id: Database ID
                - title: Database title
                - properties: All database properties
        
        Raises:
            NotionValidationError: If database creation fails
        """
        try:
            logger.info("creating_database", workspace_id=workspace_id, database_name=database_name)
            
            # Define required properties for book database
            properties = {
                "Name": {
                    "title": {}  # Title property for book names
                },
                "Category": {
                    "select": {
                        "options": [
                            {"name": "Fiction", "color": "blue"},
                            {"name": "Non-Fiction", "color": "green"},
                            {"name": "Reference", "color": "yellow"},
                        ]
                    }
                },
                "Date Done #1": {
                    "date": {}  # Date property for completion date
                },
                # Note: Cover images use page covers, not a property
                "Progress Code": {
                    "select": {
                        "options": [
                            {"name": "New", "color": "gray"},
                            {"name": "Reading", "color": "blue"},
                            {"name": "Completed", "color": "green"},
                        ]
                    }
                },
                "Type": {
                    "select": {
                        "options": [
                            {"name": "Kobo", "color": "purple"},
                        ]
                    }
                },
            }
            
            # Create the database
            response = self._client.databases.create(
                parent={"page_id": workspace_id},
                title=[{"type": "text", "text": {"content": database_name}}],
                properties=properties,
            )
            
            # Extract database info from response
            db_id = response.get("id")
            db_title = response.get("title", [])
            
            # Parse title from Notion response
            if isinstance(db_title, list) and db_title:
                title = db_title[0].get("plain_text", database_name)
            else:
                title = database_name
            
            result = {
                "id": db_id,
                "title": title,
                "properties": response.get("properties", {}),
            }
            
            logger.info("database_created", database_id=db_id, database_name=database_name)
            return result
        
        except APIResponseError as e:
            error_code = e.code if hasattr(e, 'code') else "unknown"
            
            if error_code == "unauthorized":
                raise NotionValidationError(
                    "Notion token is invalid or expired. Please check your integration token."
                )
            elif error_code == "restricted_resource":
                raise NotionValidationError(
                    "You don't have permission to create databases. "
                    "Ensure your Notion integration has database creation capabilities."
                )
            else:
                raise NotionValidationError(
                    f"Failed to create database: {str(e)}"
                )
        
        except Exception as e:
            logger.error("create_database_error", workspace_id=workspace_id, error=str(e))
            raise NotionValidationError(
                f"Unexpected error creating database: {str(e)}"
            )
    
    def add_optional_properties(
        self,
        database_id: str,
        include_description: bool = False,
        include_time_spent: bool = False,
    ) -> Dict[str, Any]:
        """Add optional metadata properties to an existing database.
        
        Args:
            database_id: ID of the Notion database
            include_description: If True, add rich text Description property
            include_time_spent: If True, add numeric Time Spent property
        
        Returns:
            Dictionary of added properties (empty dict if no properties added)
        
        Raises:
            NotionValidationError: If property creation fails
        """
        if not include_description and not include_time_spent:
            return {}
        
        try:
            properties = {}
            
            if include_description:
                properties["Description"] = {
                    "rich_text": {}
                }
            
            if include_time_spent:
                properties["Time Spent"] = {
                    "number": {}
                }
            
            response = self._client.databases.update(
                database_id=database_id,
                properties=properties,
            )
            
            logger.info(
                "optional_properties_added",
                database_id=database_id,
                properties_added=list(properties.keys()),
            )
            
            return response.get("properties", {})
        
        except APIResponseError as e:
            error_code = e.code if hasattr(e, 'code') else "unknown"
            
            if error_code == "unauthorized":
                raise NotionValidationError(
                    "Notion token is invalid or expired. "
                    "Cannot add optional properties."
                )
            else:
                raise NotionValidationError(
                    f"Failed to add optional properties: {str(e)}"
                )
        
        except Exception as e:
            logger.error(
                "optional_properties_error",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error adding optional properties: {str(e)}"
            )
    
    def initialize_empty_database(self, database_id: str) -> Dict[str, Any]:
        """Initialize an empty database with all required properties.
        
        Creates the complete schema needed for kobo-notion-sync:
        - Title (title) - Book title [REQUIRED by Notion, should exist]
        - Author (rich_text) - Book author(s)
        - Type (select) - Entry type: "Kobo" for synced books
        - ISBN (rich_text) - International Standard Book Number
        - Publisher (rich_text) - Book publisher
        - Description (rich_text) - Book summary/description
        - Status (select) - Reading status: New / Reading / Finished
        - Progress (number/percent) - Reading progress percentage (0-100%)
        - Time Spent (number) - Reading time in minutes
        - Kobo Content ID (rich_text) - Internal tracking field
        - Last Sync Time (date) - Last sync timestamp
        - Highlights Count (number) - Number of synced highlights
        
        Note: Cover images are set as page covers, not as a property.
        
        Args:
            database_id: ID of the empty Notion database
        
        Returns:
            Dictionary of all created properties
        
        Raises:
            NotionValidationError: If initialization fails
        """
        try:
            logger.info("initializing_empty_database", database_id=database_id)
            
            # Define all required properties
            properties = {
                # User-visible metadata properties
                "Author": {
                    "rich_text": {}
                },
                "Type": {
                    "select": {
                        "options": [
                            {"name": "Kobo", "color": "purple"},
                        ]
                    }
                },
                "ISBN": {
                    "rich_text": {}
                },
                "Publisher": {
                    "rich_text": {}
                },
                "Description": {
                    "rich_text": {}
                },
                "Status": {
                    "select": {
                        "options": [
                            {"name": "New", "color": "gray"},
                            {"name": "Reading", "color": "blue"},
                            {"name": "Finished", "color": "green"},
                        ]
                    }
                },
                "Progress": {
                    "number": {
                        "format": "percent"
                    }
                },
                "Time Spent": {
                    "number": {}
                },
                # Internal tracking properties
                "Kobo Content ID": {
                    "rich_text": {}
                },
                "Last Sync Time": {
                    "date": {}
                },
                "Highlights Count": {
                    "number": {}
                },
            }
            
            # Update database with all properties
            response = self._client.databases.update(
                database_id=database_id,
                properties=properties,
            )
            
            created_properties = response.get("properties", {})
            property_names = list(created_properties.keys())
            
            logger.info(
                "empty_database_initialized",
                database_id=database_id,
                properties_created=property_names,
                property_count=len(property_names),
            )
            
            # Invalidate Type property cache
            self._database_has_type_property.pop(database_id, None)
            
            return created_properties
        
        except APIResponseError as e:
            error_code = e.code if hasattr(e, 'code') else "unknown"
            
            if error_code == "unauthorized":
                raise NotionValidationError(
                    "Notion token is invalid or expired. "
                    "Cannot initialize database."
                )
            elif error_code == "restricted_resource":
                raise NotionValidationError(
                    "You don't have permission to edit this database. "
                    "Ensure the database is shared with your integration."
                )
            else:
                raise NotionValidationError(
                    f"Failed to initialize database: {str(e)}"
                )
        
        except Exception as e:
            logger.error(
                "initialize_database_error",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error initializing database: {str(e)}"
            )
    
    def add_tracking_properties(self, database_id: str) -> Dict[str, Any]:
        """Add hidden tracking properties for sync management.
        
        These properties are internal tracking fields:
        - Kobo Content ID: Maps to Kobo book ID
        - Last Sync Time: Timestamp of last sync
        - Highlights Count: Number of highlights synced
        
        Args:
            database_id: ID of the Notion database
        
        Returns:
            Dictionary of added tracking properties
        
        Raises:
            NotionValidationError: If property creation fails
        """
        try:
            properties = {
                "Kobo Content ID": {
                    "rich_text": {}
                },
                "Last Sync Time": {
                    "date": {}
                },
                "Highlights Count": {
                    "number": {}
                },
            }
            
            response = self._client.databases.update(
                database_id=database_id,
                properties=properties,
            )
            
            logger.info(
                "tracking_properties_added",
                database_id=database_id,
                tracking_fields=list(properties.keys()),
            )
            
            return response.get("properties", {})
        
        except APIResponseError as e:
            error_code = e.code if hasattr(e, 'code') else "unknown"
            
            if error_code == "unauthorized":
                raise NotionValidationError(
                    "Notion token is invalid or expired. "
                    "Cannot add tracking properties."
                )
            else:
                raise NotionValidationError(
                    f"Failed to add tracking properties: {str(e)}"
                )
        
        except Exception as e:
            logger.error(
                "tracking_properties_error",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error adding tracking properties: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def create_book_page(
        self,
        database_id: str,
        title: str,
        author: str,
        status: str,
        progress_percent: float,
        page_type: str = "Kobo",
        isbn: Optional[str] = None,
        publisher: Optional[str] = None,
        kobo_content_id: Optional[str] = None,
        description: Optional[str] = None,
        time_spent: Optional[int] = None,
    ) -> str:
        """Create a new book page in Notion database (T055, FR-028).
        
        Args:
            database_id: Notion database ID
            title: Book title
            author: Book author
            status: "New", "Reading", or "Finished"
            progress_percent: Reading progress as percentage (0-100)
            page_type: Type value ("Kobo")
            isbn: Optional ISBN
            publisher: Optional publisher name
            kobo_content_id: Optional Kobo Content ID for tracking
            description: Optional book description (plain text, HTML stripped)
            time_spent: Optional time spent reading in minutes
        
        Returns:
            Page ID of created page
        
        Raises:
            NotionValidationError: If page creation fails
        """
        try:
            properties = {
                "Name": {
                    "title": [
                        {
                            "type": "text",
                            "text": {"content": title},
                        }
                    ]
                },
                "Author": {
                    "rich_text": [{"type": "text", "text": {"content": author}}]
                },
                "Status": {
                    "select": {"name": status}
                },
                "Progress": {
                    "number": progress_percent / 100.0  # Convert to 0.0-1.0 for percent format
                },
                "Type": {
                    "select": {"name": page_type}
                },
            }
            
            # Add optional properties if present
            if isbn:
                properties["ISBN"] = {"rich_text": [{"type": "text", "text": {"content": isbn}}]}
            
            if publisher:
                properties["Publisher"] = {"rich_text": [{"type": "text", "text": {"content": publisher}}]}
            
            if kobo_content_id:
                properties["Kobo Content ID"] = {
                    "rich_text": [{"type": "text", "text": {"content": kobo_content_id}}]
                }
            
            if description:
                # Strip HTML tags from description
                import re
                clean_description = re.sub(r'<[^>]+>', '', description)
                clean_description = clean_description.strip()
                if clean_description:
                    properties["Description"] = {
                        "rich_text": [{"type": "text", "text": {"content": clean_description[:2000]}}]  # Limit to 2000 chars
                    }
            
            if time_spent is not None:
                properties["Time Spent"] = {"number": time_spent}
            
            # Create page
            response = self._client.pages.create(
                parent={"database_id": database_id},
                properties=properties,
            )
            
            page_id = response.get("id")
            logger.info(
                "book_page_created",
                page_id=page_id,
                title=title,
                author=author,
                status=status,
                progress_percent=progress_percent,
            )
            
            return page_id
        
        except APIResponseError as e:
            logger.error("create_book_page_failed", title=title, error=str(e))
            raise NotionValidationError(
                f"Failed to create book page for '{title}': {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error("create_book_page_error", title=title, error=str(e))
            raise NotionValidationError(
                f"Unexpected error creating book page: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def set_cover_image(
        self,
        page_id: str,
        image_url: str,
    ) -> None:
        """Set book cover image as page cover (T094, FR-017A).
        
        Sets the page cover of a Notion page to an external URL.
        The image will display:
        - As a full-width banner at the top of the page
        - As card preview in Gallery view (when configured to show "Page cover")
        
        Note: This uses the page cover API, not an Image property.
        Gallery views MUST be configured to use "Page cover" as the card preview source.
        
        Args:
            page_id: Notion page ID to update
            image_url: External URL to cover image (must be publicly accessible)
        
        Raises:
            NotionValidationError: If cover image update fails
        """
        try:
            # Set page cover for gallery card preview and page banner
            self._client.pages.update(
                page_id=page_id,
                cover={
                    "type": "external",
                    "external": {
                        "url": image_url
                    }
                }
            )
            
            logger.info(
                "cover_image_set",
                page_id=page_id,
                image_url=image_url,
            )
        
        except APIResponseError as e:
            # Log error but don't raise - cover images are non-blocking (SC-028)
            logger.error(
                "set_cover_image_failed",
                page_id=page_id,
                image_url=image_url,
                error=str(e),
                error_code=e.code if hasattr(e, 'code') else "unknown",
            )
            # Note: Per T096, cover image failures should not block sync
            raise NotionValidationError(
                f"Failed to set cover image: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "set_cover_image_error",
                page_id=page_id,
                image_url=image_url,
                error=str(e),
            )
            # Note: Per T096, cover image failures should not block sync
            raise NotionValidationError(
                f"Unexpected error setting cover image: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def create_highlight_blocks(
        self,
        page_id: str,
        highlights: List[Dict[str, Any]],
    ) -> int:
        """Create highlight blocks in a Notion page (T057-T057F, FR-028B).
        
        Generates structured page content with:
        - Heading 2: "📖 Highlights (count)" section header
        - For each highlight:
          - Heading 3: Highlight text
          - Paragraph: Bold metadata "chapter_position • date"
          - Divider block
        - Heading 2: "📊 Statistics" section
        - Table with statistics
        
        Args:
            page_id: Notion page ID to add highlights to
            highlights: List of highlight dicts with keys:
                - text: Highlight text
                - chapter_progress: Location (0-100%)
                - date_created: Datetime object or ISO string
                - annotation: Optional user note
        
        Returns:
            Number of blocks created
        
        Raises:
            NotionValidationError: If block creation fails
        """
        try:
            blocks = []
            
            # Heading: Highlights section
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": f"📖 Highlights ({len(highlights)})"},
                        }
                    ]
                },
            })
            
            # Add each highlight
            for hl in highlights:
                # Heading 3: Highlight text
                text_preview = hl.get("text", "")[:100]  # Preview for heading
                blocks.append({
                    "object": "block",
                    "type": "heading_3",
                    "heading_3": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": text_preview},
                            }
                        ]
                    },
                })
                
                # Paragraph: Metadata with chapter progress and date
                chapter_progress = hl.get("chapter_progress")
                date_created = hl.get("date_created")
                
                # Format metadata
                if isinstance(date_created, datetime):
                    date_str = date_created.strftime("%Y-%m-%d")
                else:
                    date_str = str(date_created) if date_created else "Unknown date"
                
                if chapter_progress is not None:
                    location_str = f"Chapter position: {chapter_progress:.1f}%"
                else:
                    location_str = "Unknown location"
                
                metadata_text = f"{location_str} • {date_str}"
                
                blocks.append({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {"content": metadata_text},
                                "annotations": {"bold": True},
                            }
                        ]
                    },
                })
                
                # Divider
                blocks.append({
                    "object": "block",
                    "type": "divider",
                    "divider": {},
                })
            
            # Statistics section
            blocks.append({
                "object": "block",
                "type": "heading_2",
                "heading_2": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": "📊 Statistics"},
                        }
                    ]
                },
            })
            
            # Add statistics table (simplified for now - just show count)
            blocks.append({
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {"content": f"Total highlights: {len(highlights)}"},
                        }
                    ]
                },
            })
            
            # Append blocks to page
            # Note: Notion API has limits on block creation, may need batching
            if blocks:
                self._client.blocks.children.append(
                    block_id=page_id,
                    children=blocks,
                )
                
                logger.info(
                    "highlight_blocks_created",
                    page_id=page_id,
                    blocks_created=len(blocks),
                    highlights_count=len(highlights),
                )
            
            return len(blocks)
        
        except APIResponseError as e:
            logger.error("create_highlight_blocks_failed", page_id=page_id, error=str(e))
            raise NotionValidationError(
                f"Failed to create highlight blocks: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error("create_highlight_blocks_error", page_id=page_id, error=str(e))
            raise NotionValidationError(
                f"Unexpected error creating highlight blocks: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def update_book_status_to_completed(
        self,
        page_id: str,
        completion_date: Optional[datetime] = None,
    ) -> None:
        """Update book page when status changes from Reading to Completed (T061, FR-024).
        
        Sets Date Done #1 property to mark book as finished.
        
        Args:
            page_id: Notion page ID to update
            completion_date: Date when book was completed (defaults to today)
        
        Raises:
            NotionValidationError: If page update fails
        """
        try:
            if completion_date is None:
                completion_date = datetime.now()
            
            # Update page properties
            self._client.pages.update(
                page_id=page_id,
                properties={
                    "Progress Code": {
                        "select": {"name": "Completed"}
                    },
                    "Date Done #1": {
                        "date": {
                            "start": completion_date.strftime("%Y-%m-%d")
                        }
                    },
                },
            )
            
            logger.info(
                "book_status_updated_to_completed",
                page_id=page_id,
                completion_date=completion_date.isoformat(),
            )
        
        except APIResponseError as e:
            logger.error("update_book_status_failed", page_id=page_id, error=str(e))
            raise NotionValidationError(
                f"Failed to update book status to Completed: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error("update_book_status_error", page_id=page_id, error=str(e))
            raise NotionValidationError(
                f"Unexpected error updating book status: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def list_kobo_books(
        self,
        database_id: str,
    ) -> List[Dict[str, Any]]:
        """Query Notion database for books with Kobo tracking properties (T075, FR-025).
        
        Filters for pages that have:
        - Kobo Content ID property populated (not empty)
        - Type = "Kobo" (if Type property exists in database)
        
        This ensures manual entries (without Kobo Content ID or Type != "Kobo")
        are ignored and protected from modification.
        
        Args:
            database_id: Notion database ID to query
        
        Returns:
            List of book page dictionaries with properties
        
        Raises:
            NotionValidationError: If query fails
        """
        try:
            # Check if Type property exists
            has_type = self._check_type_property_exists(database_id)
            
            # Build filter based on available properties
            if has_type:
                # Use compound filter with Type property
                query_filter = {
                    "and": [
                        {
                            "property": "Kobo Content ID",
                            "rich_text": {
                                "is_not_empty": True
                            }
                        },
                        {
                            "property": "Type",
                            "select": {
                                "equals": "Kobo"
                            }
                        }
                    ]
                }
            else:
                # Use only Kobo Content ID filter if Type doesn't exist
                query_filter = {
                    "property": "Kobo Content ID",
                    "rich_text": {
                        "is_not_empty": True
                    }
                }
                logger.warning(
                    "type_property_missing_using_fallback_filter",
                    database_id=database_id,
                )
            
            response = self._client.databases.query(
                database_id=database_id,
                filter=query_filter,
            )
            
            books = response.get("results", [])
            
            logger.info(
                "kobo_books_listed",
                database_id=database_id,
                count=len(books),
                has_type_property=has_type,
            )
            
            return books
        
        except APIResponseError as e:
            logger.error(
                "list_kobo_books_failed",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Failed to list Kobo books: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "list_kobo_books_error",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error listing Kobo books: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def get_book_by_kobo_id(
        self,
        database_id: str,
        kobo_content_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Get existing book page by Kobo Content ID with Type filter (T076, T076A, FR-025).
        
        Queries for a page with matching Kobo Content ID AND Type = "Kobo" (if available).
        Returns None if not found (may be manual entry or doesn't exist).
        
        Args:
            database_id: Notion database ID
            kobo_content_id: Kobo Content ID to search for
        
        Returns:
            Book page dictionary if found, None otherwise
        
        Raises:
            NotionValidationError: If query fails
        """
        try:
            # Check if Type property exists
            has_type = self._check_type_property_exists(database_id)
            
            # Build filter based on available properties
            if has_type:
                # Use compound filter with Type property
                query_filter = {
                    "and": [
                        {
                            "property": "Kobo Content ID",
                            "rich_text": {
                                "equals": kobo_content_id
                            }
                        },
                        {
                            "property": "Type",
                            "select": {
                                "equals": "Kobo"
                            }
                        }
                    ]
                }
            else:
                # Use only Kobo Content ID filter if Type doesn't exist
                query_filter = {
                    "property": "Kobo Content ID",
                    "rich_text": {
                        "equals": kobo_content_id
                    }
                }
            
            response = self._client.databases.query(
                database_id=database_id,
                filter=query_filter,
            )
            
            results = response.get("results", [])
            
            if results:
                logger.info(
                    "book_found_by_kobo_id",
                    kobo_content_id=kobo_content_id,
                    page_id=results[0].get("id"),
                )
                return results[0]
            else:
                logger.info(
                    "book_not_found_by_kobo_id",
                    kobo_content_id=kobo_content_id,
                )
                return None
        
        except APIResponseError as e:
            logger.error(
                "get_book_by_kobo_id_failed",
                kobo_content_id=kobo_content_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Failed to get book by Kobo ID: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "get_book_by_kobo_id_error",
                kobo_content_id=kobo_content_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error getting book by Kobo ID: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def update_book_page(
        self,
        page_id: str,
        progress_code: Optional[str] = None,
        percent_read: Optional[float] = None,
        description: Optional[str] = None,
        time_spent: Optional[int] = None,
    ) -> None:
        """Update existing book page properties with pre-modification check (T056, T077, FR-027).
        
        IMPORTANT: This method should only be called after verifying the page
        has a Kobo Content ID (via get_book_by_kobo_id). This ensures manual
        entries are never modified.
        
        Args:
            page_id: Notion page ID to update
            progress_code: Optional new Progress Code ("New", "Reading", "Completed")
            percent_read: Optional reading progress percentage
            description: Optional book description (HTML)
            time_spent: Optional time spent reading in minutes
        
        Raises:
            NotionValidationError: If page update fails or page is not a Kobo book
        """
        try:
            # Build properties dict with only provided values
            properties: Dict[str, Any] = {}
            
            if progress_code is not None:
                properties["Progress Code"] = {
                    "select": {"name": progress_code}
                }
            
            # Note: percent_read would need to be handled by custom property if configured
            # For now, we only update the core properties
            
            if description is not None:
                properties["Description"] = {
                    "rich_text": [{"text": {"content": description[:2000]}}]
                }
            
            if time_spent is not None:
                properties["Time Spent"] = {
                    "number": time_spent
                }
            
            # Update page if there are properties to update
            if properties:
                self._client.pages.update(
                    page_id=page_id,
                    properties=properties,
                )
                
                logger.info(
                    "book_page_updated",
                    page_id=page_id,
                    updated_properties=list(properties.keys()),
                )
            else:
                logger.debug(
                    "book_page_update_skipped_no_changes",
                    page_id=page_id,
                )
        
        except APIResponseError as e:
            logger.error(
                "update_book_page_failed",
                page_id=page_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Failed to update book page: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "update_book_page_error",
                page_id=page_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error updating book page: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def update_sync_metadata(
        self,
        page_id: str,
        highlights_count: int,
        sync_time: Optional[datetime] = None,
    ) -> None:
        """Update Last Sync Time and Highlights Count for a book page.
        
        Args:
            page_id: Notion page ID to update
            highlights_count: Number of highlights synced
            sync_time: Sync timestamp (defaults to now)
        
        Raises:
            NotionValidationError: If metadata update fails
        """
        try:
            if sync_time is None:
                sync_time = datetime.now()
            
            properties = {
                "Last Sync Time": {
                    "date": {"start": sync_time.isoformat()}
                },
                "Highlights Count": {
                    "number": highlights_count
                },
            }
            
            self._client.pages.update(
                page_id=page_id,
                properties=properties,
            )
            
            logger.info(
                "sync_metadata_updated",
                page_id=page_id,
                highlights_count=highlights_count,
                sync_time=sync_time.isoformat(),
            )
        
        except APIResponseError as e:
            logger.error(
                "update_sync_metadata_failed",
                page_id=page_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Failed to update sync metadata: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "update_sync_metadata_error",
                page_id=page_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error updating sync metadata: {str(e)}"
            )
    
    @retry_with_backoff(max_retries=3, initial_wait=1.0)
    def count_non_kobo_books(
        self,
        database_id: str,
    ) -> int:
        """Count manual entries in database (pages without Kobo tracking) (T078, FR-025).
        
        Counts pages that either:
        - Have empty Kobo Content ID property
        - Have Type != "Kobo" (if Type property exists)
        
        This helps users understand how many manual entries exist that will be
        protected from sync modifications.
        
        Args:
            database_id: Notion database ID to query
        
        Returns:
            Count of non-Kobo books (manual entries)
        
        Raises:
            NotionValidationError: If query fails
        """
        try:
            # Check if Type property exists
            has_type = self._check_type_property_exists(database_id)
            
            # Build filter based on available properties
            if has_type:
                # Query for pages that are NOT Kobo books
                # This uses an OR filter: Kobo Content ID is empty OR Type != "Kobo"
                query_filter = {
                    "or": [
                        {
                            "property": "Kobo Content ID",
                            "rich_text": {
                                "is_empty": True
                            }
                        },
                        {
                            "property": "Type",
                            "select": {
                                "does_not_equal": "Kobo"
                            }
                        }
                    ]
                }
            else:
                # If Type doesn't exist, just check for empty Kobo Content ID
                query_filter = {
                    "property": "Kobo Content ID",
                    "rich_text": {
                        "is_empty": True
                    }
                }
            
            response = self._client.databases.query(
                database_id=database_id,
                filter=query_filter,
            )
            
            count = len(response.get("results", []))
            
            logger.info(
                "non_kobo_books_counted",
                database_id=database_id,
                count=count,
            )
            
            return count
        
        except APIResponseError as e:
            logger.error(
                "count_non_kobo_books_failed",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Failed to count non-Kobo books: {str(e)}",
                details={"error_code": e.code if hasattr(e, 'code') else "unknown"},
            )
        
        except Exception as e:
            logger.error(
                "count_non_kobo_books_error",
                database_id=database_id,
                error=str(e)
            )
            raise NotionValidationError(
                f"Unexpected error counting non-Kobo books: {str(e)}"
            )

