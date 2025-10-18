"""Kobo device detection and data extraction."""

import os
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any

import structlog

logger = structlog.get_logger(__name__)


class KoboDeviceError(Exception):
    """Raised when Kobo device operations fail."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


class KoboExtractor:
    """Extract book and highlight data from Kobo e-reader device."""
    
    # Known Kobo mount point patterns
    KOBO_MOUNT_NAMES = ["KOBOeReader", "Kobo eReader", "KOBO"]
    
    # Relative path to Kobo database from mount point
    KOBO_DB_PATH = ".kobo/KoboReader.sqlite"
    
    # Known Kobo device models (all officially supported models)
    # Source: https://help.kobo.com/hc/en-us/articles/360019127133-Supported-eReader-models
    KNOWN_DEVICE_MODELS = [
        # Current/Recent models (2023-2024)
        "Kobo Libra Colour",
        "Kobo Clara Colour",
        "Kobo Clara BW",
        "Kobo Elipsa 2E",
        "Kobo Clara 2E",
        "Kobo Sage",
        "Kobo Libra 2",
        # Slightly older models (2021-2022)
        "Kobo Elipsa",
        "Kobo Nia",
        "Kobo Libra H2O",
        "Kobo Forma",
        # Older models (2019-2020)
        "Kobo Clara HD",
        "Kobo Aura H2O Edition 2",
        "Kobo Aura ONE",
        "Kobo Aura Edition 2",
        "Kobo Touch 2.0",
        "Kobo Glo HD",
        # Legacy models (2015-2018)
        "Kobo Aura H2O",
        "Kobo Aura",
        "Kobo Aura HD",
        "Kobo Glo",
        "Kobo Touch",
        # Generic fallback for unknown/legacy devices
        "Kobo eReader",
    ]
    
    def __init__(self, mount_path: Optional[Path] = None):
        """Initialize Kobo extractor.
        
        Args:
            mount_path: Optional custom mount path. If not provided, will auto-detect.
        """
        self.mount_path = mount_path
        self._device_info: Optional[Dict[str, Any]] = None
        logger.info("kobo_extractor_initialized", mount_path=str(mount_path) if mount_path else None)
    
    def detect_device(self) -> Path:
        """Detect Kobo device by scanning /Volumes/ for known mount names.
        
        Returns:
            Path to Kobo device mount point
        
        Raises:
            KoboDeviceError: If device not found or not accessible
        """
        if self.mount_path:
            # Use provided mount path
            if self._verify_mount_path(self.mount_path):
                logger.info("kobo_device_detected", mount_path=str(self.mount_path))
                return self.mount_path
            else:
                raise KoboDeviceError(
                    f"Provided mount path is not a valid Kobo device: {self.mount_path}",
                    details={"mount_path": str(self.mount_path)},
                )
        
        # Auto-detect device in /Volumes/
        volumes_path = Path("/Volumes")
        if not volumes_path.exists():
            raise KoboDeviceError(
                "/Volumes directory not found. This tool requires macOS.",
                details={"volumes_path": str(volumes_path)},
            )
        
        # Scan for Kobo devices
        for mount_name in self.KOBO_MOUNT_NAMES:
            candidate_path = volumes_path / mount_name
            if candidate_path.exists() and self._verify_mount_path(candidate_path):
                self.mount_path = candidate_path
                logger.info("kobo_device_auto_detected", mount_path=str(self.mount_path))
                return self.mount_path
        
        # Device not found
        logger.warning("kobo_device_not_found", scanned_names=self.KOBO_MOUNT_NAMES)
        raise KoboDeviceError(
            "Kobo device not found. Please connect your Kobo e-reader via USB.",
            details={
                "scanned_paths": [str(volumes_path / name) for name in self.KOBO_MOUNT_NAMES],
            },
        )
    
    def _verify_mount_path(self, mount_path: Path) -> bool:
        """Verify that a mount path contains a valid Kobo device.
        
        Args:
            mount_path: Path to verify
        
        Returns:
            True if path contains a valid Kobo device, False otherwise
        """
        # Check if path exists and is accessible
        if not mount_path.exists() or not os.access(mount_path, os.R_OK):
            return False
        
        # Check for Kobo database
        db_path = mount_path / self.KOBO_DB_PATH
        if not db_path.exists():
            logger.debug(
                "kobo_database_not_found",
                mount_path=str(mount_path),
                expected_db_path=str(db_path),
            )
            return False
        
        # Verify database is accessible
        if not self._verify_database(db_path):
            return False
        
        return True
    
    def _verify_database(self, db_path: Path) -> bool:
        """Verify that the Kobo SQLite database is valid and accessible.
        
        Args:
            db_path: Path to KoboReader.sqlite
        
        Returns:
            True if database is valid, False otherwise
        """
        try:
            # Try to open and query the database
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            
            # Check for required tables
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name IN ('content', 'Bookmark')"
            )
            tables = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            if "content" not in tables or "Bookmark" not in tables:
                logger.warning(
                    "kobo_database_missing_tables",
                    db_path=str(db_path),
                    found_tables=tables,
                )
                return False
            
            logger.debug("kobo_database_verified", db_path=str(db_path))
            return True
        
        except sqlite3.Error as e:
            logger.warning(
                "kobo_database_verification_failed",
                db_path=str(db_path),
                error=str(e),
            )
            return False
        
        except Exception as e:
            logger.error(
                "kobo_database_verification_error",
                db_path=str(db_path),
                error=str(e),
            )
            return False
    
    def get_device_info(self) -> Dict[str, Any]:
        """Get device information including model name.
        
        Returns:
            Dictionary with device information:
                - model: Device model name (e.g., "Kobo Libra 2")
                - mount_path: Device mount path
                - database_path: Path to KoboReader.sqlite
                - is_recognized: Whether device model is in known list
        
        Raises:
            KoboDeviceError: If device not detected or database not found
        """
        if self._device_info:
            return self._device_info
        
        if not self.mount_path:
            self.detect_device()
        
        if not self.mount_path:
            raise KoboDeviceError("Device not detected. Call detect_device() first.")
        
        db_path = self.mount_path / self.KOBO_DB_PATH
        
        # Try to get device model from database
        model = self._get_device_model_from_db(db_path)
        
        is_recognized = model in self.KNOWN_DEVICE_MODELS
        
        if not is_recognized and model:
            logger.warning(
                "unrecognized_kobo_device_model",
                model=model,
                known_models=self.KNOWN_DEVICE_MODELS,
            )
        
        self._device_info = {
            "model": model or "Unknown Kobo Device",
            "mount_path": str(self.mount_path),
            "database_path": str(db_path),
            "is_recognized": is_recognized,
        }
        
        logger.info("device_info_retrieved", device_info=self._device_info)
        return self._device_info
    
    def _get_device_model_from_db(self, db_path: Path) -> Optional[str]:
        """Extract device model name from Kobo device.
        
        Tries multiple methods to find device model:
        1. Check .kobo/version file (primary method)
        2. Try to query database (fallback)
        
        Args:
            db_path: Path to KoboReader.sqlite
        
        Returns:
            Device model name if found, None otherwise
        """
        # Method 1: Try to read from .kobo/version file
        try:
            version_file = db_path.parent / "version"
            if version_file.exists():
                with open(version_file, 'r') as f:
                    content = f.read().strip()
                    # First part of version file is device serial/code
                    # e.g., "N418190060008,4.1.15,4.38.23429,..."
                    if content:
                        device_code = content.split(',')[0]
                        logger.debug("device_model_from_version_file", device_code=device_code)
                        # Try to map device code to friendly name
                        model = self._map_device_code_to_model(device_code)
                        if model:
                            return model
        except Exception as e:
            logger.debug("device_model_from_version_file_failed", error=str(e))
        
        # Method 2: Fallback to database query (in case Kobo stores it there)
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            
            # Try different possible column names
            cursor.execute(
                "SELECT value FROM dbversion WHERE key = 'DeviceModel' LIMIT 1"
            )
            result = cursor.fetchone()
            
            conn.close()
            
            if result:
                model = result[0]
                logger.debug("device_model_from_db", model=model)
                return model
        
        except sqlite3.Error as e:
            logger.debug("device_model_from_db_failed", error=str(e))
        except Exception as e:
            logger.error("device_model_extraction_error", error=str(e))
        
        logger.debug("device_model_not_found")
        return None
    
    def _map_device_code_to_model(self, device_code: str) -> Optional[str]:
        """Map Kobo device code to friendly model name.
        
        Device codes sourced from official Kobo support:
        https://help.kobo.com/hc/en-us/articles/360019127133-Supported-eReader-models
        
        Args:
            device_code: Device code from version file (e.g., "N418190060008")
        
        Returns:
            Friendly model name if known, "Kobo eReader" for unknown devices
        """
        # Official Kobo device code mappings from support page
        device_map = {
            # Current/Recent models (2023-2024)
            "N428": "Kobo Libra Colour",
            "N367": "Kobo Clara Colour",
            "N365": "Kobo Clara BW",
            "N605": "Kobo Elipsa 2E",
            "N506": "Kobo Clara 2E",
            "N778": "Kobo Sage",
            "N418": "Kobo Libra 2",
            
            # Slightly older models (2021-2022)
            "N604": "Kobo Elipsa",
            "N306": "Kobo Nia",
            "N873": "Kobo Libra H2O",
            "N782": "Kobo Forma",
            
            # Older models (2019-2020)
            "N249": "Kobo Clara HD",
            "N867": "Kobo Aura H2O Edition 2",
            "N709": "Kobo Aura ONE",
            "N236": "Kobo Aura Edition 2",
            "N587": "Kobo Touch 2.0",
            "N437": "Kobo Glo HD",
            
            # Legacy models (2015-2018)
            "N250": "Kobo Aura H2O",
            "N514": "Kobo Aura",
            "N204": "Kobo Aura HD",
            "N204B": "Kobo Aura HD",
            "N613": "Kobo Glo",
            "N905": "Kobo Touch",
            "N905B": "Kobo Touch",
            "N905C": "Kobo Touch",
        }
        
        # Try exact match first
        if device_code in device_map:
            return device_map[device_code]
        
        # Try to match prefix (for variant codes like N418190060008 starts with N418)
        for code_prefix in device_map.keys():
            if device_code.startswith(code_prefix):
                return device_map[code_prefix]
        
        # Unknown device - return generic Kobo eReader
        # This gracefully handles any new or unknown Kobo devices
        logger.debug("unknown_device_code_using_generic", device_code=device_code)
        return "Kobo eReader"
