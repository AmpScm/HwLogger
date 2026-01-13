"""
HTTP API client for HomeWizard P1 device (APIv2).

Provides methods to fetch data from the P1 device via HTTP API
with proper bearer token authentication.
"""

import aiohttp
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class P1HTTPClient:
    """HTTP client for HomeWizard P1 APIv2."""

    def __init__(
        self,
        api_url: str,
        token: str,
        timeout_seconds: int = 10,
        verify_ssl: bool = False,
    ):
        """
        Initialize HTTP client.

        Args:
            api_url: Base API URL (e.g., http://192.168.1.100/api/v2)
            token: Bearer token for authentication
            timeout_seconds: Request timeout
            verify_ssl: Whether to verify SSL certificates
        """
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl

    def _get_headers(self) -> Dict[str, str]:
        """Get headers with bearer token."""
        return {
            "Authorization": f"Bearer {self.token}",
        }

    async def get_data(self) -> Optional[Dict[str, Any]]:
        """
        Fetch current meter data from /data endpoint.

        Returns:
            Dictionary with meter data or None on error
        """
        url = f"{self.api_url}/data"

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)
            ) as session:
                async with session.get(
                    url,
                    headers=self._get_headers(),
                    ssl=self.verify_ssl,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logger.debug(f"Fetched meter data from {url}")
                    return data
        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch meter data: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching meter data: {e}")
            return None

    async def get_status(self) -> Optional[Dict[str, Any]]:
        """
        Fetch device status from /status endpoint.

        Returns:
            Dictionary with device status or None on error
        """
        url = f"{self.api_url}/status"

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout_seconds)
            ) as session:
                async with session.get(
                    url,
                    headers=self._get_headers(),
                    ssl=self.verify_ssl,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                    logger.debug(f"Fetched device status from {url}")
                    return data
        except aiohttp.ClientError as e:
            logger.error(f"Failed to fetch device status: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching device status: {e}")
            return None

    async def health_check(self) -> bool:
        """
        Check if P1 device is accessible and authenticated.

        Returns:
            True if device is accessible, False otherwise
        """
        try:
            status = await self.get_status()
            return status is not None
        except Exception:
            return False
