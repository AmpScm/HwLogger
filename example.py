"""
Example and testing script for hwLogger.

Shows how to:
1. Initialize the daemon
2. Set up the API token
3. Check the REST API
"""

import asyncio
import logging
from pathlib import Path

from hwLogger.config import Config
from hwLogger.db import Database
from hwLogger.daemon import HWLoggerDaemon

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


async def setup_example():
    """
    Example: Setup with token and basic checks.
    """
    print("=" * 60)
    print("hwLogger Setup Example")
    print("=" * 60)
    
    # Create test database
    config = Config(
        db_file=Path("test_hw_logger.db"),
        p1_host="127.0.0.1",  # Placeholder, will be stored in DB
        api_port=8080,
    )
    
    print(f"\nConfig:")
    print(f"  DB File: {config.db_file}")
    print(f"  API: http://{config.api_host}:{config.api_port}")
    
    # Initialize database
    db = Database(config.db_file)
    await db.init()
    print(f"\n✓ Database initialized")
    
    # Store P1 device host
    p1_host = input("Enter your P1 device IP address (e.g., 192.168.1.100): ").strip() or "127.0.0.1"
    await db.set_config("p1_host", p1_host)
    print(f"✓ P1 host stored in database: {p1_host}")
    
    # Store token
    token = input("Enter your P1 bearer token: ").strip()
    if token:
        await db.set_config("p1_token", token)
        print(f"✓ Token stored in database")
    else:
        print(f"⚠️  Skipped token storage (required to run daemon)")
    
    # Display WebSocket URL
    config.p1_host = p1_host  # Update config to show correct URL
    print(f"\n✓ P1 WebSocket: {config.get_p1_ws_url()}")
    
    # Verify both are stored
    if token:
        stored_token = await db.get_config("p1_token")
        stored_host = await db.get_config("p1_host")
        if stored_token == token and stored_host == p1_host:
            print(f"✓ Configuration verified in database")
    
    print("\n" + "=" * 60)
    print("Setup complete!")
    print(f"\nDatabase configuration:")
    print(f"  P1 Host: {p1_host}")
    print(f"  P1 Token: {'*' * len(token[:10])}... (hidden)")
    print(f"\nYou can now run the daemon without any configuration:")
    print(f"  python -m hwLogger.daemon")
    print(f"  (or: python run_daemon.py)")
    print(f"\nThe REST API will be available at:")
    print(f"  http://localhost:8080/health")
    print("=" * 60)


async def run_daemon_example():
    """
    Example: Run the daemon (requires P1 device on network).
    """
    config = Config.from_env()
    daemon = HWLoggerDaemon(config)
    
    try:
        await daemon.start()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("\nShutdown...")
        await daemon.stop()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        print("Starting daemon (requires P1 device)...")
        asyncio.run(run_daemon_example())
    else:
        asyncio.run(setup_example())
