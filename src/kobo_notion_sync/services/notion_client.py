"""Notion API client wrapper for kobo-notion-sync."""

from typing import Any, Dict, List, Optional

import structlog
from notion_client import Client
from notion_client.errors import APIResponseError

logger = structlog.get_logger(__name__)


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
        logger.info("notion_client_initialized")
    
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
            required_properties = {
                "Name": "title",
                "Category": "select",
                "Date Done #1": "date",
                "Image": "files",
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
                "Image": {
                    "files": {}  # Files property for book covers
                },
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
