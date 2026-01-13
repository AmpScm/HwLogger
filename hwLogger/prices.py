"""
Electricity price fetcher for hwLogger.

Fetches day-ahead prices from kwhprice.eu and stores them in the database.
Can be integrated into the daemon or run as a separate scheduled task.
"""

import asyncio
import aiohttp
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Tuple

from hwLogger.db import Database
from hwLogger.config import Config


class PriceFetcher:
    """Fetch electricity prices from kwhprice.eu."""

    def __init__(self, db: Database, kwhprice_url: str = "https://kwhprice.eu/prices/NL"):
        """
        Initialize price fetcher.
        
        Args:
            db: Database instance.
            kwhprice_url: Base URL for kwhprice.eu API (NL prices).
        """
        self.db = db
        self.kwhprice_url = kwhprice_url

    async def fetch_prices_for_date(
        self,
        session: aiohttp.ClientSession,
        target_date: date,
    ) -> List[Tuple[str, float]]:
        """
        Fetch NL day-ahead prices for one date.
        
        Prices from API are in €/MWh -> converted to €/kWh.
        
        Args:
            session: aiohttp session.
            target_date: Date to fetch prices for (YYYY-MM-DD).
        
        Returns:
            List of (hour_timestamp, price_eur_per_kwh) tuples.
            Empty list if data not available (future dates) or error.
        """
        url = f"{self.kwhprice_url}/{target_date.isoformat()}"
        
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 404:
                    # Expected: data not yet published (future dates)
                    return []
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as e:
            print(f"Error fetching prices for {target_date}: {e}")
            return []
        except Exception as e:
            print(f"Unexpected error fetching prices: {e}")
            return []

        # Extract prices for the date
        day_prices = data.get("NL", {}).get(target_date.isoformat(), [])
        
        results = []
        for item in day_prices:
            try:
                hour = item["hour"]
                price_mwh = float(item["price"])
                hour_ts = f"{target_date.isoformat()}T{hour}:00"
                price_kwh = price_mwh / 1000.0  # Convert MWh to kWh
                results.append((hour_ts, price_kwh))
            except (KeyError, ValueError) as e:
                print(f"Error parsing price data for {target_date}: {e}")
                continue

        return results

    async def update_prices(
        self,
        lookback_days: int = 2,
        lookahead_days: int = 2,
    ):
        """
        Fetch and store prices for today, future days, and recent past days.
        
        Only fetches dates that don't already have prices in the database.
        
        Args:
            lookback_days: How many days back to attempt fetching (usually already have these).
            lookahead_days: How many future days to attempt fetching.
        """
        today = date.today()
        
        # Dates to check: today, lookahead_days forward, lookback_days backward
        dates_to_check = [today + timedelta(days=i) for i in range(-lookback_days, lookahead_days + 1)]

        async with aiohttp.ClientSession() as session:
            for target_date in dates_to_check:
                # Skip dates we already have
                has_prices = await self.db.has_prices_for_date(target_date.isoformat())
                if has_prices:
                    print(f"✓ Already have prices for {target_date}")
                    continue

                # Fetch and store
                print(f"Fetching prices for {target_date}...")
                prices = await self.fetch_prices_for_date(session, target_date)
                
                if prices:
                    await self.db.insert_prices(prices)
                    print(f"✓ Stored {len(prices)} prices for {target_date}")
                else:
                    print(f"  No prices available for {target_date} (may be future date)")


async def run_price_update():
    """Standalone script to fetch and update prices."""
    config = Config.from_env()
    db = Database(config.db_file)
    await db.init()
    
    fetcher = PriceFetcher(db, config.kwhprice_url)
    await fetcher.update_prices(
        lookback_days=config.lookback_days,
        lookahead_days=2,
    )
    
    print("\nPrice update complete!")


if __name__ == "__main__":
    asyncio.run(run_price_update())
