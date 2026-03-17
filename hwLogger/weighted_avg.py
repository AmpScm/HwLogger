"""
Weighted average calculator for energy consumption data.

Calculates weighted averages over 5-minute and 30-second time blocks based on
the time each reading was active before the next reading.

TIMESTAMP AND BLOCK SEMANTICS (STRICT):
========================================

EXACT TIMESLOT CONCEPT:
- Each block has a single authoritative END timestamp
- Readings are collected over the block duration and attributed to that block
- Block membership: last_block_time < reading_time <= current_block_time
- This ensures NO GAPS or OVERLAPS between consecutive blocks

TIMING EXAMPLE (30-second block):
- Block ends at 14:05:30
  * Includes readings: 14:05:00 < ts <= 14:05:30
  * Previous block (14:05:00) ended exactly at 14:05:00 (not included)
  * Reading at 14:05:00.001 belongs to this block
  * Reading at 14:05:30.000 belongs to this block
  * Reading at 14:05:30.001 belongs to the NEXT block

METER READINGS:
- Received approximately every 1 second from the meter
- Each reading has an exact timestamp (meter provided if available)
- Readings are in timestamp order (oldest first)

WEIGHTED AVERAGE CALCULATION:
- Each reading is weighted by how long it remained "active" until the next reading
- The last reading is weighted from its timestamp until the block END time
- Duration = next_reading_time - current_reading_time
- Last reading duration = block_end_time - last_reading_time
- This ensures accurate representation of power consumption patterns

EXAMPLE:
  Block 14:05:30 (30-second, spanning 14:05:00 < ts <= 14:05:30):
  - Reading at 14:05:01: 1000W (active for 10s until 14:05:11)
  - Reading at 14:05:11: 1100W (active for 10s until 14:05:21)
  - Reading at 14:05:21: 1050W (active for 9s until 14:05:30 block end)
  
  Total duration: 10 + 10 + 9 = 29s (should be 30s total, so last reading gets adjusted)
  Last reading weight: 30 - (10 + 10) = 10s
  Weighted avg = (1000*10 + 1100*10 + 1050*10) / 30 = 1050W
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta


class WeightedAverageCalculator:
    """
    Calculates weighted averages for power and current readings.

    Each reading is weighted by how long it was "active" until the
    next reading. For example, if readings occur at:
    - T0: 1000W (active for 5 minutes until T5)
    - T5: 1100W (active for 5 minutes until T10)

    The weighted average for the T0-T10 block is:
    (1000 * 5 + 1100 * 5) / 10 = 1050W
    
    STRICT METER TIMESTAMP USAGE:
    - Readings are extracted from measurement data with their meter timestamps
    - Block boundaries are determined strictly from meter time
    - No daemon system time is used in weighted average calculation
    - This ensures consistency even if daemon time is slightly off
    """

    # Power fields to average
    POWER_FIELDS = [
        "w",
        "l1_w",
        "l2_w",
        "l3_w",
    ]

    # Current fields to average (deprecated - no longer used)
    CURRENT_FIELDS = []

    @staticmethod
    def calculate_weighted_averages(
        readings: List[Dict],
        interval_seconds: int = 300,
    ) -> Optional[Dict[str, float]]:
        """
        Calculate weighted averages for readings within a time block.

        Takes meter readings with EXACT timestamps and calculates the weighted average
        of power consumption where each reading is weighted by its duration
        until the next reading (or block end for the last reading).

        STRICT TIMESTAMP SEMANTICS:
        - All timestamps in readings are exact meter times (from meter if available)
        - Readings are in timestamp order (oldest first)
        - Each reading's weight = time until next reading
        - Last reading weight = interval_seconds - sum(previous durations)
        - No rounding - all calculations use exact timestamps

        Block membership check (for reference):
        - A reading belongs to block T if: T - interval < reading_time <= T
        - Block spans exactly [last_block_time, current_block_time]

        Args:
            readings: List of meter data dicts with timestamps, sorted ascending by time.
                      Each dict must have a "timestamp" key (ISO 8601 format).
            interval_seconds: The target block duration (default 5 minutes = 300s).

        Returns:
            Dict with keys like 'w_avg', 'w_max', 'l1_w_avg', etc.
            Returns None if insufficient data.
        """
        if len(readings) < 2:
            return None

        # Parse timestamps
        try:
            times = [
                (
                    datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00"))
                    if isinstance(r["timestamp"], str)
                    else r["timestamp"]
                )
                for r in readings
            ]
        except (ValueError, AttributeError):
            return None

        # Calculate duration of each reading (time until next reading or end of block)
        durations = []
        for i in range(len(times) - 1):
            delta = (times[i + 1] - times[i]).total_seconds()
            durations.append(delta)
        # Last reading duration: from its timestamp until the end of the block
        # This represents the time this measurement was "active" before block completion
        durations.append(interval_seconds - sum(durations))

        total_duration = sum(durations)
        if total_duration <= 0:
            return None

        aggregates = {"reading_count": len(readings)}

        # Calculate power averages and maximums
        for field in WeightedAverageCalculator.POWER_FIELDS:
            values = [r.get(field) for r in readings]
            if all(v is not None for v in values):
                weighted_sum = sum(v * d for v, d in zip(values, durations))
                avg = weighted_sum / total_duration
                aggregates[f"{field}_avg"] = round(avg, 1)  # 1 decimal for avg watts
                aggregates[f"{field}_max"] = round(max(values), 2)

        return aggregates if aggregates else None

    @staticmethod
    def get_block_boundaries(
        reference_time: Optional[datetime] = None,
        interval_seconds: int = 300,
    ) -> Tuple[datetime, datetime]:
        """
        Get the start and end timestamps of the current 5-minute block.

        Args:
            reference_time: Time to calculate block for (default: now).
            interval_seconds: Block duration in seconds.

        Returns:
            Tuple of (block_start, block_end).
        """
        if reference_time is None:
            reference_time = datetime.utcnow()

        # Find start of current block (round down to nearest interval)
        seconds_into_block = (
            reference_time.second
            + (reference_time.minute % (interval_seconds // 60)) * 60
        )
        seconds_into_block = (
            reference_time.hour * 3600
            + reference_time.minute * 60
            + reference_time.second
        ) % interval_seconds

        block_start = reference_time - timedelta(seconds=seconds_into_block)
        block_start = block_start.replace(microsecond=0)

        block_end = block_start + timedelta(seconds=interval_seconds)

        return block_start, block_end
