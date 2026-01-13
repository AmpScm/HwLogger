"""
Main daemon for hwLogger.

Orchestrates WebSocket listener, REST API server, and data processing.
"""

import asyncio
import logging
import signal
import argparse
import aiohttp
import aiosqlite
from typing import Optional
from datetime import datetime, timedelta

from .config import Config
from .db import Database
from .websocket import P1WebSocketListener
from .http_client import P1HTTPClient
from .api import RestAPI
from .weighted_avg import WeightedAverageCalculator


logger = logging.getLogger(__name__)


class HWLoggerDaemon:
    """Main daemon that orchestrates the logger."""

    def __init__(self, config: Config):
        """
        Initialize daemon.
        
        Args:
            config: Configuration instance.
        """
        self.config = config
        self.db = Database(config.db_file)
        self.ws_listener: Optional[P1WebSocketListener] = None
        self.api: Optional[RestAPI] = None
        self.api_runner = None
        self.running = False
        self._ws_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Store recent readings for weighted average calculation
        self._recent_readings = []
        self._last_log_time: Optional[datetime] = None
        self._last_cleanup_time: Optional[datetime] = None
        self._last_30s_time: Optional[datetime] = None  # Track 30-second window
        self._backfill_done = False  # Track if backfill has been performed
        self._gap_info: Optional[dict] = None  # Track gap for energy interpolation

    async def initialize(self, host: Optional[str] = None, token: Optional[str] = None):
        """Initialize database and load configuration.
        
        Args:
            host: Optional P1 device host to override config/database
            token: Optional API token to override database
        """
        await self.db.init()
        logger.info(f"Database initialized at {self.config.db_file}")

        # Handle host argument
        if host:
            self.config.p1_host = host
            await self.db.set_config("p1_host", host)
            logger.info(f"P1 host set from command line: {host}")
        else:
            # Load P1 host from database
            p1_host = await self.db.get_config("p1_host")
            if p1_host:
                self.config.p1_host = p1_host
                logger.info(f"P1 host loaded from database: {p1_host}")
            else:
                logger.debug(f"Using P1 host from config: {self.config.p1_host}")

        # Handle token argument
        if token:
            await self.db.set_config("p1_token", token)
            logger.info("P1 token set from command line")
        else:
            # Load token from database
            db_token = await self.db.get_config("p1_token")
            if db_token:
                logger.info("P1 token loaded from database")
            elif self.config.p1_host != "127.0.0.1":
                # Host is available but no token - trigger registration
                logger.info(f"No token found for {self.config.p1_host}. Attempting to create new user...")
                await self._register_new_user()
            else:
                logger.warning("No P1 token found and no host configured")

    async def _register_new_user(self):
        """
        Register new user by triggering button click on P1 device.
        
        Creates 'local/hwlogger' user after waiting for button click.
        Stores the returned token in the database.
        """
        try:
            logger.info(f"Waiting for button click on {self.config.p1_host}...")
            logger.info("Please press the button on your HomeWizard P1 meter to authorize access")
            
            http_client = P1HTTPClient(
                api_url=f"http://{self.config.p1_host}/api/v2",
                token="",  # No token yet
                timeout_seconds=30,
            )
            
            # Call the API endpoint to create user
            # POST /api/v2/users with {"name": "local/hwlogger"}
            import aiohttp
            async with aiohttp.ClientSession() as session:
                url = f"http://{self.config.p1_host}/api/v2/users"
                payload = {"name": "local/hwlogger"}
                
                try:
                    async with session.post(
                        url,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=60),
                        ssl=False
                    ) as resp:
                        if resp.status == 201:
                            data = await resp.json()
                            token = data.get("token")
                            if token:
                                await self.db.set_config("p1_token", token)
                                logger.info(f"User 'local/hwlogger' created successfully!")
                                logger.info(f"Token saved to database")
                                return
                            else:
                                logger.error("No token in response")
                        elif resp.status == 409:
                            # User already exists, try to authenticate
                            logger.info("User 'local/hwlogger' already exists")
                            # In real scenario, we'd need to request a new token
                            # For now, log the error
                            logger.warning("Existing user found, but cannot retrieve token without button click")
                        else:
                            text = await resp.text()
                            logger.error(f"Failed to create user (HTTP {resp.status}): {text}")
                except asyncio.TimeoutError:
                    logger.error("Timeout waiting for button click or no response from device")
                except aiohttp.ClientError as e:
                    logger.error(f"HTTP error during registration: {e}")
                    
        except Exception as e:
            logger.error(f"Error during user registration: {e}", exc_info=True)

    async def _backfill_missing_blocks(self):
        """Fill gaps in 5-minute data for small outages (reboots, ~30 min max)."""
        try:
            last_row = await self.db.get_latest_meter_data_row()
            if not last_row:
                logger.debug("No previous data to backfill from")
                return
            
            # last_row structure: timestamp(0), tariff(1), import_kwh(2),
            # import_t1_kwh(3), import_t2_kwh(4), export_kwh(5),
            # w(6), l1_w(7), l2_w(8), l3_w(9), l1_v(10), l2_v(11), l3_v(12), any_power_fail_count(13), ...
            last_ts = datetime.fromisoformat(last_row[0])
            last_energy = last_row[2]  # import_kwh
            
            now = datetime.utcnow()
            gap = (now - last_ts).total_seconds()
            
            # Only backfill for gaps up to 30 minutes
            if gap < 60 or gap > 1800:
                if gap > 1800:
                    logger.info(f"Gap too large ({gap}s), skipping backfill")
                return
            
            # Store gap info for later interpolation when first reading arrives
            self._gap_info = {
                "last_ts": last_ts,
                "last_import_kwh": last_row[2],
                "last_import_t1_kwh": last_row[3],
                "last_import_t2_kwh": last_row[4],
                "last_export_kwh": last_row[5],
                "block_count": 0,
            }
            
            # Generate synthetic blocks for each missing 5-min boundary
            block_ts = last_ts + timedelta(minutes=5)
            block_ts = block_ts.replace(second=0, microsecond=0)
            block_count = 0
            
            while block_ts <= now:
                # Check if this block already exists
                async with aiosqlite.connect(str(self.db.db_path)) as db:
                    async with db.execute(
                        "SELECT 1 FROM meter_data WHERE timestamp = ?",
                        (block_ts.isoformat(),)
                    ) as cursor:
                        exists = await cursor.fetchone()
                
                if not exists:
                    # Create synthetic block with reading_count=0 marker
                    # Copy energy values from last reading (they don't change during outage)
                    # Set w=0 (no power during gap), readings=0 to mark as synthetic
                    synthetic_data = {
                        "timestamp": block_ts.isoformat(),
                        "tariff": last_row[1],
                        "import_kwh": last_row[2],  # Use last known value (energy doesn't reset)
                        "import_t1_kwh": last_row[3],
                        "import_t2_kwh": last_row[4],
                        "export_kwh": last_row[5],
                        "w": 0.0,  # No power during gap
                        "l1_w": 0.0,
                        "l2_w": 0.0,
                        "l3_w": 0.0,
                        "l1_v": None,  # Unknown during outage
                        "l2_v": None,
                        "l3_v": None,
                    }
                    synthetic_agg = {
                        "w_avg": 0.0,
                        "w_max": 0.0,
                        "l1_w_avg": 0.0,
                        "l2_w_avg": 0.0,
                        "l3_w_avg": 0.0,
                        "l1_w_max": 0.0,
                        "l2_w_max": 0.0,
                        "l3_w_max": 0.0,
                        "reading_count": 0,  # Mark as synthetic
                    }
                    await self.db.insert_meter_data(synthetic_data, synthetic_agg)
                    if self._gap_info:
                        self._gap_info["block_count"] += 1
                    block_count += 1
                
                block_ts += timedelta(minutes=5)
            
            if block_count > 0:
                logger.info(f"Backfilled {block_count} blocks ({int(gap)}s gap)")
        
        except Exception as e:
            logger.error(f"Error during backfill: {e}", exc_info=True)

    async def start(self, host: Optional[str] = None, token: Optional[str] = None):
        """Start the daemon.
        
        Args:
            host: Optional P1 device host to override config/database
            token: Optional API token to override database
        """
        await self.initialize(host=host, token=token)

        self.running = True
        logger.info("Starting hwLogger daemon...")

        # Setup signal handlers (Unix-only, Windows doesn't support add_signal_handler)
        try:
            loop = asyncio.get_event_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
        except NotImplementedError:
            # Windows doesn't support signal handlers in asyncio
            logger.debug("Signal handlers not available on this platform")

        # Start REST API
        self.api = RestAPI(self.db, self.config)
        self.api_runner = await self.api.start()
        logger.info("REST API started")

        # Load P1 host and token from database
        p1_host = await self.db.get_config("p1_host")
        if p1_host:
            self.config.p1_host = p1_host
            logger.info(f"P1 host loaded from database: {p1_host}")
        else:
            logger.warning("No P1 host in database, using configured host")

        token = await self.db.get_config("p1_token")
        if not token:
            logger.error("Cannot start: P1 token not configured in database")
            await self.stop()
            return

        ws_url = self.config.get_p1_ws_url()
        logger.info(f"Connecting to WebSocket: {ws_url}")
        
        self.ws_listener = P1WebSocketListener(
            ws_url=ws_url,
            token=token,
            timeout_seconds=self.config.ws_timeout_seconds,
            verify_ssl=self.config.p1_verify_ssl,
        )

        # Start WebSocket listener as background task (don't await it directly)
        logger.info("Starting WebSocket listener...")
        self._ws_task = asyncio.create_task(
            self.ws_listener.start(
                on_data_callback=self._handle_ws_data,
                on_error_callback=self._handle_ws_error,
            )
        )
        
        # Start cleanup task (runs every hour)
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())
        
        logger.info("Daemon started successfully")
    
    async def _periodic_cleanup(self):
        """Periodically clean up old 30-second and per-minute data."""
        while self.running:
            try:
                now = datetime.utcnow()
                if self._last_cleanup_time is None or (now - self._last_cleanup_time).total_seconds() >= 3600:
                    await self.db.cleanup_old_30s_data()
                    self._last_cleanup_time = now
                    logger.info("Cleaned up old 30-second data (>24h)")
                
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in cleanup task: {e}", exc_info=True)
                await asyncio.sleep(60)

    async def _handle_ws_data(self, data: dict):
        """Handle incoming WebSocket data."""
        try:
            msg_type = data.get("type")
            
            # Only process measurement data
            if msg_type != "measurement":
                logger.debug(f"Ignoring message type: {msg_type}")
                return
            
            measurement = data.get("data", {})
            if not measurement:
                return
            
            # Backfill on first reading, then interpolate energy backwards into synthetic blocks
            if not self._backfill_done:
                await self._backfill_missing_blocks()
                self._backfill_done = True
                
                # Interpolate energy consumption across the gap
                if self._gap_info and self._gap_info["block_count"] > 0:
                    current_import = measurement.get("energy_import_kwh")
                    last_import = self._gap_info["last_import_kwh"]
                    
                    if current_import is not None and last_import is not None:
                        energy_delta = current_import - last_import
                        block_count = self._gap_info["block_count"]
                        energy_per_block = energy_delta / (block_count + 1)  # +1 for gap period
                        
                        # Update synthetic blocks with interpolated energy values
                        block_ts = self._gap_info["last_ts"] + timedelta(minutes=5)
                        block_ts = block_ts.replace(second=0, microsecond=0)
                        
                        for i in range(block_count):
                            interpolated_import = last_import + (energy_per_block * (i + 1))
                            interpolated_t1 = self._gap_info["last_import_t1_kwh"] + (
                                (measurement.get("energy_import_t1_kwh", self._gap_info["last_import_t1_kwh"]) - 
                                 self._gap_info["last_import_t1_kwh"]) * (i + 1) / (block_count + 1)
                            )
                            interpolated_t2 = self._gap_info["last_import_t2_kwh"] + (
                                (measurement.get("energy_import_t2_kwh", self._gap_info["last_import_t2_kwh"]) - 
                                 self._gap_info["last_import_t2_kwh"]) * (i + 1) / (block_count + 1)
                            )
                            interpolated_export = self._gap_info["last_export_kwh"] + (
                                (measurement.get("energy_export_kwh", self._gap_info["last_export_kwh"]) - 
                                 self._gap_info["last_export_kwh"]) * (i + 1) / (block_count + 1)
                            )
                            
                            async with aiosqlite.connect(str(self.db.db_path)) as db:
                                await db.execute(
                                    """UPDATE meter_data SET import_kwh=?, import_t1_kwh=?, 
                                       import_t2_kwh=?, export_kwh=? WHERE timestamp=?""",
                                    (interpolated_import, interpolated_t1, interpolated_t2, 
                                     interpolated_export, block_ts.isoformat())
                                )
                                await db.commit()
                            
                            block_ts += timedelta(minutes=5)
                        
                        logger.info(f"Interpolated energy across {block_count} synthetic blocks "
                                  f"(delta: {energy_delta:.3f} kWh)")
                    
                    self._gap_info = None  # Clear gap info
            
            # Store latest reading with full precision timestamp (microseconds preserved)
            meter_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "tariff": measurement.get("tariff"),
                "import_kwh": measurement.get("energy_import_kwh"),
                "import_t1_kwh": measurement.get("energy_import_t1_kwh"),
                "import_t2_kwh": measurement.get("energy_import_t2_kwh"),
                "export_kwh": measurement.get("energy_export_kwh"),
                "w": measurement.get("power_w"),
                "l1_w": measurement.get("power_l1_w"),
                "l2_w": measurement.get("power_l2_w"),
                "l3_w": measurement.get("power_l3_w"),
                "l1_v": measurement.get("voltage_l1_v"),
                "l2_v": measurement.get("voltage_l2_v"),
                "l3_v": measurement.get("voltage_l3_v"),
            }
            
            # Accumulate readings for 30-second and 5-minute windows
            self._recent_readings.append(meter_data)
            
            now = datetime.utcnow()
            
            # Check 30-second boundary
            current_30s = (now.second // 30) * 30
            current_30s_time = now.replace(second=current_30s, microsecond=0)
            
            if self._last_30s_time is None:
                self._last_30s_time = current_30s_time
            elif now >= current_30s_time and self._last_30s_time < current_30s_time:
                # Crossed 30-second boundary - only use readings from this window
                window_start = self._last_30s_time.isoformat()
                window_end = current_30s_time.isoformat()
                # Filter: timestamp >= window_start AND timestamp < window_end (exclude next boundary)
                window_readings = [r for r in self._recent_readings 
                                  if r["timestamp"] >= window_start and r["timestamp"] < window_end]
                
                if len(window_readings) > 0:
                    aggregates = WeightedAverageCalculator.calculate_weighted_averages(
                        window_readings,
                        interval_seconds=30,
                    )
                else:
                    aggregates = None
                
                block_data = window_readings[-1].copy() if window_readings else self._recent_readings[-1].copy()
                # Use window END time for the timestamp (when data was collected)
                block_data["timestamp"] = current_30s_time.isoformat()
                await self.db.insert_meter_data_30s(block_data, aggregates)
                logger.debug(f"30s block {current_30s_time.isoformat()}: "
                            f"readings={len(window_readings)}")
                self._last_30s_time = current_30s_time
            
            # Check 5-minute boundary
            current_boundary = (now.minute // 5) * 5
            current_boundary_time = now.replace(minute=current_boundary, second=0, microsecond=0)
            
            if self._last_log_time is None:
                self._last_log_time = current_boundary_time
            elif now >= current_boundary_time and self._last_log_time < current_boundary_time:
                # Crossed 5-minute boundary
                if len(self._recent_readings) > 1:
                    aggregates = WeightedAverageCalculator.calculate_weighted_averages(
                        self._recent_readings,
                        interval_seconds=300,
                    )
                else:
                    aggregates = None
                
                block_data = self._recent_readings[-1].copy()
                # Use window END time for the timestamp (when data was collected)
                block_data["timestamp"] = current_boundary_time.isoformat()
                await self.db.insert_meter_data(block_data, aggregates)
                power_avg = aggregates.get("w_avg", "N/A") if aggregates else "N/A"
                power_max = aggregates.get("w_max", "N/A") if aggregates else "N/A"
                logger.info(f"5-min block {current_boundary_time.isoformat()}: "
                           f"avg={power_avg}W, max={power_max}W, readings={len(self._recent_readings)}")
                
                # Reset for next window
                self._recent_readings = []
                self._last_log_time = current_boundary_time
                self._last_30s_time = current_30s_time

        except Exception as e:
            logger.error(f"Error handling WebSocket data: {e}", exc_info=True)

    async def _handle_ws_error(self, error: Exception):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")

    async def stop(self):
        """Stop the daemon."""
        logger.info("Stopping hwLogger daemon...")
        self.running = False

        # Cancel background WebSocket task
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        # Cancel cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self.ws_listener:
            await self.ws_listener.stop()

        if self.api_runner:
            await self.api.stop(self.api_runner)

        logger.info("Daemon stopped")

    async def run(self, host: Optional[str] = None, token: Optional[str] = None):
        """Run the daemon until interrupted.
        
        Args:
            host: Optional P1 device host to override config/database
            token: Optional API token to override database
        """
        await self.start(host=host, token=token)
        # Keep running until stopped
        while self.running:
            await asyncio.sleep(1)


async def main():
    """Entry point for the daemon."""
    parser = argparse.ArgumentParser(description="HomeWizard P1 Logger daemon")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (debug) logging"
    )
    parser.add_argument(
        "--host",
        type=str,
        help="P1 device host IP address (overrides database and env config)"
    )
    parser.add_argument(
        "--token",
        type=str,
        help="P1 device API token (overrides database value)"
    )
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if args.verbose:
        logger.debug("Verbose logging enabled")

    try:
        config = Config.from_env()
        daemon = HWLoggerDaemon(config)
        await daemon.initialize(host=args.host, token=args.token)
        await daemon.run()
    except Exception as e:
        logger.error(f"Daemon fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
