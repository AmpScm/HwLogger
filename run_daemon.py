#!/usr/bin/env python3
"""
Main entry point for hwLogger daemon.

Usage:
    python -m hwLogger.daemon
"""

import asyncio
import sys
from hwLogger.daemon import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested")
        sys.exit(0)
    except Exception as e:
        import traceback

        print(f"Fatal error: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
        sys.exit(1)
