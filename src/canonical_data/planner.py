"""Deterministic finite Polymarket 1h x 7 partition planner."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from canonical_data.models import Asset


def release_bucket(day: date) -> str:
    half = "a" if day.day <= 15 else "b"
    return f"{day:%Y-%m}-{half}"


def build_backfill_plan(start: date, end: date) -> list[dict[str, Any]]:
    if end < start:
        raise ValueError("backfill end precedes start")
    result: list[dict[str, Any]] = []
    current = start
    while current <= end:
        day = current.isoformat()
        release_group = f"polymarket-1h-seven-v1-{release_bucket(current)}"
        for asset in Asset:
            result.append(
                {
                    "partition_id": f"{asset.value}/1h/{day}",
                    "asset": asset.value,
                    "timeframe": "1h",
                    "day": day,
                    "release_group": release_group,
                }
            )
        current += timedelta(days=1)
    return result
