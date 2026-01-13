"""
Weighted average calculator for energy consumption data.

Calculates weighted averages over 5-minute time blocks based on
the time each reading was active before the next reading.
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

        Args:
            readings: List of meter data dicts with timestamps, sorted ascending by time.
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
        # Last reading duration: assume it lasted until the end of the block
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
