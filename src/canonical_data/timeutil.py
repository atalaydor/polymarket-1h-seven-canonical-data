"""Strict timestamp normalization to UTC nanoseconds."""

from __future__ import annotations

from datetime import UTC, datetime

from canonical_data.errors import SourceError


def epoch_to_ns(value: int | str, precision: str) -> int:
    number = int(value)
    multipliers = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
    try:
        return number * multipliers[precision]
    except KeyError as exc:
        raise SourceError(f"unsupported timestamp precision: {precision}") from exc


def iso_to_ns(value: str) -> int:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        raise SourceError("timestamp must include timezone")
    return int(parsed.astimezone(UTC).timestamp() * 1_000_000_000)


def ns_to_iso(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    base = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanoseconds:09d}Z"
