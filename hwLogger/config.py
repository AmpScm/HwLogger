"""
Configuration management for hwLogger.

Handles reading/writing configuration to SQLite database,
including secure storage of API tokens.
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration for HomeWizard P1 Logger."""

    # Paths
    db_file: Path
    
    # HomeWizard P1 device
    p1_host: str = "127.0.0.1"  # Default to localhost for testing (override with HW_LOGGER_P1_HOST)
    p1_api_version: str = "v2"
    p1_verify_ssl: bool = False  # Disable SSL verification (self-signed certs common)
    
    # REST API server
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    
    # Logging
    log_interval_seconds: int = 5  # WebSocket provides updates, we log periodically
    
    # Price fetching
    kwhprice_url: str = "https://kwhprice.eu/prices/NL"
    lookback_days: int = 2
    
    # Timeouts
    http_timeout_seconds: int = 10
    ws_timeout_seconds: int = 10
    
    @classmethod
    def from_env(cls) -> "Config":
        """Create config from environment variables."""
        db_file = Path(os.getenv("HW_LOGGER_DB", "~/.hw_logger.db")).expanduser()
        
        return cls(
            db_file=db_file,
            p1_host=os.getenv("HW_LOGGER_P1_HOST", "127.0.0.1"),
            p1_verify_ssl=os.getenv("HW_LOGGER_P1_VERIFY_SSL", "false").lower() == "true",
            api_host=os.getenv("HW_LOGGER_API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("HW_LOGGER_API_PORT", "8080")),
            log_interval_seconds=int(os.getenv("HW_LOGGER_LOG_INTERVAL", "5")),
        )
    
    def get_p1_ws_url(self) -> str:
        """Get WebSocket URL for P1 device (uses wss:// with default HTTPS port)."""
        return f"wss://{self.p1_host}/api/ws"
    
    def get_p1_api_url(self, endpoint: str) -> str:
        """Get HTTP API URL for P1 device."""
        return f"http://{self.p1_host}/api/{self.p1_api_version}/{endpoint}"
