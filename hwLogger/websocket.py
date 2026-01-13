"""
WebSocket listener for HomeWizard P1 device (APIv2).

Connects to the device's WebSocket endpoint and receives
real-time meter data updates using aiohttp's async WebSocket client.
"""

import asyncio
import json
import logging
import ssl
from typing import Callable, Optional, Dict, Any

import aiohttp


logger = logging.getLogger(__name__)


class P1WebSocketListener:
    """
    Listens to HomeWizard P1 WebSocket for real-time meter data.

    Implements HomeWizard v2 API WebSocket protocol:
    1. Connect to wss://<ip>/api/ws
    2. Wait for authorization_requested message
    3. Send authorization message with token (within 40 seconds)
    4. Subscribe to topics

    Usage:
        listener = P1WebSocketListener(
            ws_url="wss://192.168.1.100/api/ws",
            token="YOUR_TOKEN_HERE",
            verify_ssl=False  # For self-signed certificates
        )
        await listener.start(on_data_callback)
    """

    def __init__(
        self,
        ws_url: str,
        token: str,
        timeout_seconds: int = 10,
        verify_ssl: bool = False,
    ):
        """
        Initialize WebSocket listener.

        Args:
            ws_url: WebSocket URL (e.g., wss://192.168.1.100/api/ws).
            token: Authorization token for authentication.
            timeout_seconds: Connection timeout.
            verify_ssl: Whether to verify SSL certificates (set to False for self-signed).
        """
        self.ws_url = ws_url
        self.token = token
        self.timeout_seconds = timeout_seconds
        self.verify_ssl = verify_ssl
        self.running = False
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(
        self,
        on_data_callback: Callable[[Dict[str, Any]], Any],
        on_error_callback: Optional[Callable[[Exception], Any]] = None,
    ):
        """
        Start listening to WebSocket.

        Args:
            on_data_callback: Async function called when data is received.
            on_error_callback: Optional async function called on errors.
        """
        self.running = True
        retry_delay = 1

        while self.running:
            try:
                await self._connect_and_listen(on_data_callback, on_error_callback)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                if on_error_callback:
                    try:
                        await on_error_callback(e)
                    except Exception as cb_err:
                        logger.error(f"Error in error callback: {cb_err}")

                if self.running:
                    logger.info(f"Reconnecting in {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    retry_delay = min(
                        retry_delay * 2, 60
                    )  # Exponential backoff, max 60s

    async def _connect_and_listen(
        self,
        on_data_callback: Callable[[Dict[str, Any]], Any],
        on_error_callback: Optional[Callable[[Exception], Any]] = None,
    ):
        """Connect and listen for messages, following HomeWizard v2 protocol."""
        # Create SSL context for self-signed certificates
        ssl_context = None
        if self.ws_url.startswith("wss://"):
            ssl_context = ssl.create_default_context()
            if not self.verify_ssl:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

        # Create session with custom SSL context
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds + 5)

        async with aiohttp.ClientSession(
            connector=connector, timeout=timeout
        ) as session:
            self._session = session
            try:
                async with session.ws_connect(self.ws_url, ssl=ssl_context) as ws:
                    self._ws = ws
                    logger.info(f"Connected to {self.ws_url}")

                    # Wait for authorization_requested message (device initiates auth)
                    try:
                        auth_request = await asyncio.wait_for(
                            ws.receive_str(),
                            timeout=5,  # Device should respond quickly
                        )
                        auth_msg = json.loads(auth_request)
                        logger.debug(f"Received auth request: {auth_msg}")

                        if auth_msg.get("type") != "authorization_requested":
                            logger.warning(
                                f"Expected authorization_requested, got: {auth_msg.get('type')}"
                            )

                    except asyncio.TimeoutError:
                        logger.error(
                            "Timeout waiting for authorization_requested message"
                        )
                        raise
                    except Exception as e:
                        logger.error(f"Error during auth handshake: {e}")
                        raise

                    # Send authorization with token (MUST be within 40 seconds)
                    try:
                        auth_response = json.dumps(
                            {"type": "authorization", "data": self.token}
                        )
                        await ws.send_str(auth_response)
                        logger.debug("Sent authorization token")
                    except Exception as e:
                        logger.error(f"Failed to send authorization: {e}")
                        raise

                    # Wait for authorized confirmation
                    try:
                        auth_confirm = await asyncio.wait_for(
                            ws.receive_str(),
                            timeout=5,
                        )
                        confirm_msg = json.loads(auth_confirm)
                        logger.debug(f"Received: {confirm_msg}")

                        if confirm_msg.get("type") == "authorized":
                            logger.info("Authorization successful")
                        elif confirm_msg.get("type") == "error":
                            raise Exception(
                                f"Authorization failed: {confirm_msg.get('data')}"
                            )

                    except asyncio.TimeoutError:
                        logger.error("Timeout waiting for authorization confirmation")
                        raise
                    except Exception as e:
                        logger.error(f"Authorization failed: {e}")
                        raise

                    # Subscribe to all data topics
                    try:
                        subscribe_msg = json.dumps({"type": "subscribe", "data": "*"})
                        await ws.send_str(subscribe_msg)
                        logger.debug("Sent subscribe request for all topics")
                    except Exception as e:
                        logger.error(f"Failed to send subscribe message: {e}")
                        raise

                    retry_delay = 1

                    while self.running:
                        try:
                            message = await asyncio.wait_for(
                                ws.receive_str(),
                                timeout=self.timeout_seconds + 5,  # Small buffer
                            )

                            try:
                                data = json.loads(message)
                                msg_type = data.get("type", "unknown")

                                # Debug: Log message type with brief data summary (not important for normal operation)
                                if "data" in data:
                                    if isinstance(data["data"], dict):
                                        # For dict data, show key fields
                                        keys = ", ".join(list(data["data"].keys())[:3])
                                        logger.debug(
                                            f"Received [{msg_type}]: {keys}..."
                                        )
                                    else:
                                        logger.debug(
                                            f"Received [{msg_type}]: {str(data['data'])[:80]}"
                                        )
                                else:
                                    logger.debug(f"Received [{msg_type}]")

                                # Log full measurement data for detailed inspection
                                if msg_type == "measurement":
                                    logger.debug(
                                        f"Full measurement: {json.dumps(data.get('data', {}), indent=2)}"
                                    )

                                logger.debug(f"Full message: {data}")

                                # Call the data callback
                                await on_data_callback(data)
                                retry_delay = 1  # Reset on successful receipt

                            except json.JSONDecodeError as e:
                                logger.warning(f"Invalid JSON received: {e}")

                        except asyncio.TimeoutError:
                            logger.warning("WebSocket receive timeout, reconnecting...")
                            break
            finally:
                self._ws = None
                self._session = None

    async def stop(self):
        """Stop the listener."""
        self.running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                logger.error(f"Error closing WebSocket: {e}")
        if self._session:
            try:
                await self._session.close()
            except Exception as e:
                logger.error(f"Error closing session: {e}")

    async def send_message(self, data: Dict[str, Any]):
        """Send a message to the WebSocket (if connected)."""
        if self._ws:
            try:
                await self._ws.send_str(json.dumps(data))
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
