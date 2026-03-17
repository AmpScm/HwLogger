"""
Database management for hwLogger.

Handles SQLite database initialization, schema management,
and data operations.

TIMESTAMP SEMANTICS (STRICT):
=============================

Block Membership Rule:
- All timestamps represent EXACT meter times (no approximation)
- A reading belongs to block T if: last_block_time < reading_time <= T
- Equivalently: T - interval < reading_time <= T

Example (30-second blocks):
- Block ending at 14:05:30:
  * Includes readings: 14:05:00 < time <= 14:05:30
  * Previous block (14:05:00) ended exactly at 14:05:00
  * First reading at 14:05:00.001 belongs to the 14:05:30 block
  * Last reading at 14:05:30.000 belongs to the 14:05:30 block

METER READINGS:
- Received approximately every 1 second from the meter
- If measurement includes "timestamp", use that exact value (meter provided)
- If no timestamp, use daemon UTC time as fallback

ENERGY FIELDS:
- import_kwh, export_kwh, etc. are absolute meter counter values
- They represent the meter's reading AT that block's end timestamp
- Not accumulated within blocks - they're the meter's current total
- Can calculate consumption as: current_block - previous_block
"""

import aiosqlite
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any


# =====================
# SQL Schemas
# =====================

CREATE_METER_TABLE = """
CREATE TABLE IF NOT EXISTS meter_data (
    timestamp TEXT PRIMARY KEY NOT NULL,
    tariff INTEGER NOT NULL,
    import_kwh REAL NOT NULL,
    import_t1_kwh REAL NOT NULL,
    import_t2_kwh REAL NOT NULL,
    export_kwh REAL NOT NULL,
    w REAL NOT NULL,
    l1_w REAL,
    l2_w REAL,
    l3_w REAL,
    l1_v REAL,
    l2_v REAL,
    l3_v REAL,
    -- 5-minute aggregates (avg then max)
    w_avg REAL,
    l1_w_avg REAL,
    l2_w_avg REAL,
    l3_w_avg REAL,
    w_max REAL,
    l1_w_max REAL,
    l2_w_max REAL,
    l3_w_max REAL,
    reading_count INTEGER NOT NULL
);
-- TIMESTAMP SEMANTICS (5-minute blocks):
-- - timestamp: ISO 8601, represents END of 5-minute measurement period
-- - Block membership: T - 300 < reading_time <= T (last_block < reading_time <= current_block)
-- - Data stored indefinitely (primary historical record)
"""

CREATE_METER_30S_TABLE = """
CREATE TABLE IF NOT EXISTS meter_data_30s (
    timestamp TEXT PRIMARY KEY NOT NULL,
    tariff INTEGER NOT NULL,
    import_kwh REAL NOT NULL,
    import_t1_kwh REAL NOT NULL,
    import_t2_kwh REAL NOT NULL,
    export_kwh REAL NOT NULL,
    w REAL NOT NULL,
    l1_w REAL,
    l2_w REAL,
    l3_w REAL,
    l1_v REAL,
    l2_v REAL,
    l3_v REAL,
    -- 30-second aggregates (avg then max)
    w_avg REAL,
    l1_w_avg REAL,
    l2_w_avg REAL,
    l3_w_avg REAL,
    w_max REAL,
    l1_w_max REAL,
    l2_w_max REAL,
    l3_w_max REAL,
    reading_count INTEGER NOT NULL
);
-- TIMESTAMP SEMANTICS (30-second blocks):
-- - timestamp: ISO 8601, represents END of 30-second measurement period
-- - Block membership: T - 30 < reading_time <= T (last_block < reading_time <= current_block)
-- - Data rotated after 24 hours (rolling window storage)
"""

CREATE_PRICE_TABLE = """
CREATE TABLE IF NOT EXISTS hour_price (
    hour TEXT PRIMARY KEY,
    price_eur_per_kwh REAL
);
"""

CREATE_CONFIG_TABLE = """
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
);
"""

# =====================
# Insert/Update Statements
# =====================

INSERT_METER = """
INSERT OR REPLACE INTO meter_data VALUES (
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
);
"""

INSERT_METER_30S = """
INSERT OR REPLACE INTO meter_data_30s VALUES (
    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
);
"""

INSERT_PRICE = """
INSERT OR REPLACE INTO hour_price (hour, price_eur_per_kwh)
VALUES (?, ?);
"""

UPDATE_CONFIG = """
INSERT OR REPLACE INTO config (key, value, updated_at)
VALUES (?, ?, ?);
"""


# =====================
# Database class
# =====================


class Database:
    """Async SQLite database handler for hwLogger."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    async def init(self):
        """Initialize database with schema."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(CREATE_METER_TABLE)
            await db.execute(CREATE_METER_30S_TABLE)
            await db.execute(CREATE_PRICE_TABLE)
            await db.execute(CREATE_CONFIG_TABLE)
            await db.commit()

    async def close(self):
        """Close database connection."""
        # aiosqlite handles this internally
        pass

    # =====================
    # Configuration (token storage)
    # =====================

    async def set_config(self, key: str, value: str):
        """Store configuration value (e.g., API token)."""
        now = datetime.utcnow().isoformat()
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(UPDATE_CONFIG, (key, value, now))
            await db.commit()

    async def get_config(self, key: str) -> Optional[str]:
        """Retrieve configuration value."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT value FROM config WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    # =====================
    # Meter data
    # =====================

    async def insert_meter_data(
        self, data: Dict[str, Any], aggregates: Optional[Dict[str, float]] = None
    ):
        """
        Insert meter data.

        Args:
            data: Current meter reading
            aggregates: 5-minute aggregates (weighted avg, max, count)
        """
        ts = data.get("timestamp") or datetime.now().replace(microsecond=0).isoformat()

        values = (
            ts,
            data.get("tariff"),
            data.get("import_kwh"),
            data.get("import_t1_kwh"),
            data.get("import_t2_kwh"),
            data.get("export_kwh"),
            data.get("w"),
            data.get("l1_w"),
            data.get("l2_w"),
            data.get("l3_w"),
            data.get("l1_v"),
            data.get("l2_v"),
            data.get("l3_v"),
            aggregates.get("w_avg") if aggregates else None,
            aggregates.get("l1_w_avg") if aggregates else None,
            aggregates.get("l2_w_avg") if aggregates else None,
            aggregates.get("l3_w_avg") if aggregates else None,
            aggregates.get("w_max") if aggregates else None,
            aggregates.get("l1_w_max") if aggregates else None,
            aggregates.get("l2_w_max") if aggregates else None,
            aggregates.get("l3_w_max") if aggregates else None,
            aggregates.get("reading_count") if aggregates else 0,
        )

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(INSERT_METER, values)
            await db.commit()

    async def insert_meter_data_30s(
        self, data: Dict[str, Any], aggregates: Optional[Dict[str, float]] = None
    ):
        """Insert 30-second aggregate snapshot for last 24h."""
        ts = data.get("timestamp") or datetime.now().replace(microsecond=0).isoformat()

        values = (
            ts,
            data.get("tariff"),
            data.get("import_kwh"),
            data.get("import_t1_kwh"),
            data.get("import_t2_kwh"),
            data.get("export_kwh"),
            data.get("w"),
            data.get("l1_w"),
            data.get("l2_w"),
            data.get("l3_w"),
            data.get("l1_v"),
            data.get("l2_v"),
            data.get("l3_v"),
            aggregates.get("w_avg") if aggregates else None,
            aggregates.get("l1_w_avg") if aggregates else None,
            aggregates.get("l2_w_avg") if aggregates else None,
            aggregates.get("l3_w_avg") if aggregates else None,
            aggregates.get("w_max") if aggregates else None,
            aggregates.get("l1_w_max") if aggregates else None,
            aggregates.get("l2_w_max") if aggregates else None,
            aggregates.get("l3_w_max") if aggregates else None,
            aggregates.get("reading_count") if aggregates else 0,
        )

        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(INSERT_METER_30S, values)
            await db.commit()

    async def cleanup_old_30s_data(self):
        """Clean up 30-second data older than 24 hours."""
        cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.execute(
                "DELETE FROM meter_data_30s WHERE timestamp < ?", (cutoff,)
            )
            await db.commit()

    async def get_latest_meter_data_row(self) -> Optional[Tuple]:
        """Get the most recent meter data as raw row."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT * FROM meter_data ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                return await cursor.fetchone()

    async def get_meter_data_range(
        self, start_ts: str, end_ts: str
    ) -> List[Dict[str, Any]]:
        """Get meter data within timestamp range."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT * FROM meter_data WHERE timestamp >= ? AND timestamp <= ? "
                "ORDER BY timestamp ASC",
                (start_ts, end_ts),
            ) as cursor:
                rows = await cursor.fetchall()

                columns = [
                    "timestamp",
                    "tariff",
                    "import_kwh",
                    "import_t1_kwh",
                    "import_t2_kwh",
                    "export_kwh",
                    "w",
                    "l1_w",
                    "l2_w",
                    "l3_w",
                    "l1_v",
                    "l2_v",
                    "l3_v",
                    "w_avg",
                    "l1_w_avg",
                    "l2_w_avg",
                    "l3_w_avg",
                    "w_max",
                    "l1_w_max",
                    "l2_w_max",
                    "l3_w_max",
                    "reading_count",
                ]
                return [dict(zip(columns, row)) for row in rows]

    # =====================
    # Prices
    # =====================

    async def insert_prices(self, prices: List[Tuple[str, float]]):
        """Insert multiple price records."""
        if not prices:
            return
        async with aiosqlite.connect(str(self.db_path)) as db:
            await db.executemany(INSERT_PRICE, prices)
            await db.commit()

    async def get_prices_for_date(self, date_str: str) -> List[Tuple[str, float]]:
        """Get prices for a specific date (YYYY-MM-DD)."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT hour, price_eur_per_kwh FROM hour_price "
                "WHERE hour LIKE ? ORDER BY hour ASC",
                (f"{date_str}%",),
            ) as cursor:
                return await cursor.fetchall()

    async def has_prices_for_date(self, date_str: str) -> bool:
        """Check if prices exist for a date."""
        async with aiosqlite.connect(str(self.db_path)) as db:
            async with db.execute(
                "SELECT 1 FROM hour_price WHERE hour LIKE ? LIMIT 1", (f"{date_str}%",)
            ) as cursor:
                return (await cursor.fetchone()) is not None
