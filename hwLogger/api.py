"""
Minimalistic REST API for hwLogger.

Provides endpoints for querying meter data, prices, and system health.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from aiohttp import web
import json

logger = logging.getLogger(__name__)


class RestAPI:
    """Minimalistic REST API for hwLogger."""

    def __init__(self, db, config):
        """
        Initialize REST API.

        Args:
            db: Database instance.
            config: Config instance.
        """
        self.db = db
        self.config = config
        self.app = web.Application()
        self._setup_routes()

    def _setup_routes(self):
        """Setup API routes."""
        self.app.router.add_get("/health", self.handle_health)
        self.app.router.add_get("/api/v1/latest", self.handle_latest_data)
        self.app.router.add_get("/api/v1/data", self.handle_data_range)
        self.app.router.add_get("/api/v1/prices", self.handle_prices)

    async def handle_health(self, request: web.Request) -> web.Response:
        """GET /health - System health check."""
        return web.json_response(
            {
                "status": "ok",
                "timestamp": datetime.utcnow().isoformat(),
            }
        )

    async def handle_latest_data(self, request: web.Request) -> web.Response:
        """GET /api/v1/latest - Get latest meter data."""
        try:
            data = await self.db.get_latest_meter_data()
            if not data:
                return web.json_response(
                    {"error": "No data available"},
                    status=404,
                )
            return web.json_response(data)
        except Exception as e:
            logger.error(f"Error retrieving latest data: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500,
            )

    async def handle_data_range(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/data?start=<timestamp>&end=<timestamp>
        Get meter data within timestamp range.

        Query params:
            start: ISO 8601 timestamp (default: 24 hours ago)
            end: ISO 8601 timestamp (default: now)
        """
        try:
            # Parse query parameters
            start_str = request.query.get("start")
            end_str = request.query.get("end")

            if not start_str:
                start = datetime.utcnow() - timedelta(days=1)
                start_str = start.replace(microsecond=0).isoformat()

            if not end_str:
                end = datetime.utcnow()
                end_str = end.replace(microsecond=0).isoformat()
            # Retrieve data
            data = await self.db.get_meter_data_range(start_str, end_str)

            return web.json_response(
                {
                    "start": start_str,
                    "end": end_str,
                    "count": len(data),
                    "data": data,
                }
            )
        except Exception as e:
            logger.error(f"Error retrieving data range: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500,
            )

    async def handle_prices(self, request: web.Request) -> web.Response:
        """
        GET /api/v1/prices?date=<YYYY-MM-DD>
        Get electricity prices for a specific date.
        """
        try:
            date_str = request.query.get("date")
            if not date_str:
                date_str = datetime.utcnow().date().isoformat()

            # Validate date format
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                return web.json_response(
                    {"error": "Invalid date format, use YYYY-MM-DD"},
                    status=400,
                )

            prices = await self.db.get_prices_for_date(date_str)

            return web.json_response(
                {
                    "date": date_str,
                    "count": len(prices),
                    "prices": [{"hour": h, "price_eur_per_kwh": p} for h, p in prices],
                }
            )
        except Exception as e:
            logger.error(f"Error retrieving prices: {e}")
            return web.json_response(
                {"error": str(e)},
                status=500,
            )

    async def start(self):
        """Start the REST API server."""
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(
            runner,
            self.config.api_host,
            self.config.api_port,
        )
        await site.start()
        logger.info(
            f"REST API listening on {self.config.api_host}:{self.config.api_port}"
        )
        return runner

    async def stop(self, runner):
        """Stop the REST API server."""
        await runner.cleanup()
