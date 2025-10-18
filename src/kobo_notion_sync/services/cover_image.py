"""Cover image service for retrieving book cover URLs from external APIs."""

from typing import Optional
import structlog
import httpx

logger = structlog.get_logger(__name__)


class CoverImageError(Exception):
    """Raised when cover image retrieval fails."""
    pass


class CoverImageService:
    """
    Service for retrieving book cover image URLs from external APIs.
    
    Implements:
    - Open Library API cover lookup (T091, T092)
    - Google Books API cover lookup as fallback (T091, T092)
    - Cover image URL validation (T093)
    - Retry logic for transient failures (T093)
    
    Note: Returns URLs only - no downloads or file uploads needed.
    External URLs work directly in Notion's Image property.
    """
    
    OPEN_LIBRARY_URL = "https://covers.openlibrary.org/b/isbn/{isbn}-L.jpg"
    GOOGLE_BOOKS_API = "https://www.googleapis.com/books/v1/volumes"
    
    # Maximum retry attempts for URL validation
    MAX_VALIDATION_RETRIES = 3
    
    # Timeout for HTTP requests (in seconds)
    REQUEST_TIMEOUT = 5.0
    
    def __init__(self):
        """Initialize cover image service."""
        logger.info("cover_image_service_initialized")
    
    def get_cover_url(
        self,
        isbn: Optional[str] = None,
        title: Optional[str] = None,
        author: Optional[str] = None,
    ) -> Optional[str]:
        """Get cover image URL for a book (T092, FR-017A).
        
        Attempts to retrieve cover URL in this order:
        1. Open Library by ISBN (if ISBN provided)
        2. Google Books by ISBN (if ISBN provided)
        3. Google Books by title + author (if no ISBN or ISBN lookup failed)
        
        Args:
            isbn: Book ISBN (10 or 13 digits)
            title: Book title (used as fallback)
            author: Book author (used with title as fallback)
        
        Returns:
            Validated cover image URL, or None if not found
        """
        logger.info(
            "cover_url_lookup_started",
            isbn=isbn,
            title=title,
            author=author,
        )
        
        # Try Open Library first if ISBN is provided
        if isbn:
            url = self._try_open_library(isbn)
            if url:
                return url
            
            # Try Google Books by ISBN as fallback
            url = self._try_google_books_by_isbn(isbn)
            if url:
                return url
        
        # Try Google Books by title + author as last resort
        if title and author:
            url = self._try_google_books_by_title_author(title, author)
            if url:
                return url
        elif title:
            url = self._try_google_books_by_title_author(title, "")
            if url:
                return url
        
        logger.warning(
            "cover_url_not_found",
            isbn=isbn,
            title=title,
            author=author,
        )
        
        return None
    
    def _try_open_library(self, isbn: str) -> Optional[str]:
        """Try to get cover URL from Open Library (T092).
        
        Args:
            isbn: Book ISBN
        
        Returns:
            Validated URL or None
        """
        try:
            # Clean ISBN (remove hyphens, spaces)
            clean_isbn = isbn.replace("-", "").replace(" ", "")
            
            # Build Open Library URL
            url = self.OPEN_LIBRARY_URL.format(isbn=clean_isbn)
            
            # Validate URL with retries
            if self._validate_url(url):
                logger.info(
                    "cover_url_found_open_library",
                    isbn=isbn,
                    url=url,
                )
                return url
            else:
                logger.debug(
                    "cover_url_validation_failed_open_library",
                    isbn=isbn,
                    url=url,
                )
                return None
        
        except Exception as e:
            logger.error(
                "open_library_lookup_error",
                isbn=isbn,
                error=str(e),
            )
            return None
    
    def _try_google_books_by_isbn(self, isbn: str) -> Optional[str]:
        """Try to get cover URL from Google Books using ISBN (T092).
        
        Args:
            isbn: Book ISBN
        
        Returns:
            Validated URL or None
        """
        try:
            # Clean ISBN
            clean_isbn = isbn.replace("-", "").replace(" ", "")
            
            # Query Google Books API by ISBN
            with httpx.Client(timeout=self.REQUEST_TIMEOUT) as client:
                response = client.get(
                    self.GOOGLE_BOOKS_API,
                    params={"q": f"isbn:{clean_isbn}"},
                )
                
                if response.status_code != 200:
                    logger.debug(
                        "google_books_api_failed",
                        isbn=isbn,
                        status_code=response.status_code,
                    )
                    return None
                
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    logger.debug("google_books_no_results", isbn=isbn)
                    return None
                
                # Extract thumbnail URL from first result
                volume_info = items[0].get("volumeInfo", {})
                image_links = volume_info.get("imageLinks", {})
                thumbnail = image_links.get("thumbnail") or image_links.get("smallThumbnail")
                
                if thumbnail:
                    # Convert to HTTPS if needed
                    thumbnail = thumbnail.replace("http://", "https://")
                    
                    # Validate URL
                    if self._validate_url(thumbnail):
                        logger.info(
                            "cover_url_found_google_books_isbn",
                            isbn=isbn,
                            url=thumbnail,
                        )
                        return thumbnail
                
                return None
        
        except Exception as e:
            logger.error(
                "google_books_isbn_lookup_error",
                isbn=isbn,
                error=str(e),
            )
            return None
    
    def _try_google_books_by_title_author(
        self,
        title: str,
        author: str,
    ) -> Optional[str]:
        """Try to get cover URL from Google Books using title and author (T092).
        
        Args:
            title: Book title
            author: Book author
        
        Returns:
            Validated URL or None
        """
        try:
            # Build search query
            query_parts = [f"intitle:{title}"]
            if author:
                query_parts.append(f"inauthor:{author}")
            
            query = "+".join(query_parts)
            
            # Query Google Books API
            with httpx.Client(timeout=self.REQUEST_TIMEOUT) as client:
                response = client.get(
                    self.GOOGLE_BOOKS_API,
                    params={"q": query},
                )
                
                if response.status_code != 200:
                    logger.debug(
                        "google_books_api_failed",
                        title=title,
                        author=author,
                        status_code=response.status_code,
                    )
                    return None
                
                data = response.json()
                items = data.get("items", [])
                
                if not items:
                    logger.debug(
                        "google_books_no_results",
                        title=title,
                        author=author,
                    )
                    return None
                
                # Extract thumbnail URL from first result
                volume_info = items[0].get("volumeInfo", {})
                image_links = volume_info.get("imageLinks", {})
                thumbnail = image_links.get("thumbnail") or image_links.get("smallThumbnail")
                
                if thumbnail:
                    # Convert to HTTPS if needed
                    thumbnail = thumbnail.replace("http://", "https://")
                    
                    # Validate URL
                    if self._validate_url(thumbnail):
                        logger.info(
                            "cover_url_found_google_books_title",
                            title=title,
                            author=author,
                            url=thumbnail,
                        )
                        return thumbnail
                
                return None
        
        except Exception as e:
            logger.error(
                "google_books_title_lookup_error",
                title=title,
                author=author,
                error=str(e),
            )
            return None
    
    def _validate_url(self, url: str) -> bool:
        """Validate that URL is accessible and returns an image (T093, FR-017A).
        
        Uses HTTP HEAD request to verify:
        - URL returns 200 status
        - Content-Type is an image (image/jpeg, image/png, etc.)
        
        Implements retry logic with up to MAX_VALIDATION_RETRIES attempts.
        
        Args:
            url: URL to validate
        
        Returns:
            True if URL is valid and accessible, False otherwise
        """
        for attempt in range(self.MAX_VALIDATION_RETRIES):
            try:
                with httpx.Client(timeout=self.REQUEST_TIMEOUT) as client:
                    response = client.head(url, follow_redirects=True)
                    
                    if response.status_code == 200:
                        content_type = response.headers.get("content-type", "")
                        
                        if content_type.startswith("image/"):
                            logger.debug(
                                "cover_url_validated",
                                url=url,
                                content_type=content_type,
                            )
                            return True
                        else:
                            logger.debug(
                                "cover_url_invalid_content_type",
                                url=url,
                                content_type=content_type,
                            )
                            return False
                    else:
                        logger.debug(
                            "cover_url_invalid_status",
                            url=url,
                            status_code=response.status_code,
                            attempt=attempt + 1,
                        )
                
                # If validation failed and we have retries left, continue to next attempt
                if attempt < self.MAX_VALIDATION_RETRIES - 1:
                    continue
                else:
                    return False
            
            except Exception as e:
                logger.debug(
                    "cover_url_validation_error",
                    url=url,
                    attempt=attempt + 1,
                    error=str(e),
                )
                
                # If this was the last attempt, return False
                if attempt >= self.MAX_VALIDATION_RETRIES - 1:
                    return False
                
                # Otherwise continue to next retry
                continue
        
        return False
