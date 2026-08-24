"""Pinned deterministic Arrow/Parquet serialization."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Sequence
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from canonical_data.errors import ConflictError
from canonical_data.models import (
    BookEvent,
    EventType,
    Exclusion,
    Level,
    Market,
    Sample200ms,
    UnderlyingObservation,
)

PRICE = pa.decimal128(9, 4)
SIZE = pa.decimal128(18, 6)
VALUE = pa.decimal128(24, 8)
LEVELS = pa.list_(
    pa.field(
        "element",
        pa.struct([pa.field("price", PRICE, False), pa.field("size", SIZE, False)]),
    )
)

MARKET_SCHEMA = pa.schema(
    [
        ("schema_version", pa.string()),
        ("asset", pa.string()),
        ("venue", pa.string()),
        ("timeframe", pa.string()),
        ("event_id", pa.string()),
        ("market_id", pa.string()),
        ("condition_id", pa.string()),
        ("token_up", pa.string()),
        ("token_down", pa.string()),
        ("market_start_ns", pa.int64()),
        ("market_end_ns", pa.int64()),
        ("rules_text_sha256", pa.string()),
        ("resolution_source_url", pa.string()),
        ("official_outcome", pa.string()),
        ("official_resolution_ts_ns", pa.int64()),
        ("quality_tier", pa.string()),
        ("evidence_sha256", pa.string()),
        ("exclusion_reason", pa.string()),
    ],
    metadata={b"contract": b"public-interchange/1.0.0"},
)
EVENT_SCHEMA = pa.schema(
    [
        ("condition_id", pa.string()),
        ("token_id", pa.string()),
        ("source_ts_ns", pa.int64()),
        ("receive_ts_ns", pa.int64()),
        ("source_object", pa.string()),
        ("source_row", pa.int64()),
        ("sequence", pa.int64()),
        ("event_type", pa.string()),
        ("source_id", pa.string()),
        ("bids", LEVELS),
        ("asks", LEVELS),
        ("side", pa.string()),
        ("price", PRICE),
        ("size", SIZE),
        ("best_bid", PRICE),
        ("best_ask", PRICE),
        ("old_tick_size", PRICE),
        ("new_tick_size", PRICE),
        ("fee_rate_bps", pa.int32()),
        ("transaction_hash", pa.string()),
    ],
    metadata={b"contract": b"public-interchange/1.0.0"},
)
SAMPLE_SCHEMA = pa.schema(
    [
        ("condition_id", pa.string()),
        ("token_id", pa.string()),
        ("grid_ts_ns", pa.int64()),
        ("asof_source_ts_ns", pa.int64()),
        ("asof_receive_ts_ns", pa.int64()),
        ("bids", LEVELS),
        ("asks", LEVELS),
        ("stale_ms", pa.int64()),
        ("source_event_sequence", pa.int64()),
        ("quality_tier", pa.string()),
        ("native_interval_ms", pa.int32()),
    ],
    metadata={b"contract": b"public-interchange/1.0.0"},
)
UNDERLYING_SCHEMA = pa.schema(
    [
        ("asset", pa.string()),
        ("venue", pa.string()),
        ("instrument_type", pa.string()),
        ("symbol", pa.string()),
        ("observation_kind", pa.string()),
        ("source_ts_ns", pa.int64()),
        ("receive_ts_ns", pa.int64()),
        ("value", VALUE),
        ("source_path", pa.string()),
        ("source_sha256", pa.string()),
        ("is_settlement", pa.bool_()),
    ],
    metadata={b"contract": b"public-interchange/1.0.0"},
)
EXCLUSION_SCHEMA = pa.schema(
    [
        ("market_id", pa.string()),
        ("reason_code", pa.string()),
        ("detail", pa.string()),
        ("evidence_json", pa.string()),
    ],
    metadata={b"contract": b"public-interchange/1.0.0"},
)


def _level_rows(levels: Sequence[Level] | None) -> list[dict[str, Decimal]] | None:
    if levels is None:
        return None
    return [{"price": level.price, "size": level.size} for level in levels]


def market_row(market: Market) -> dict[str, Any]:
    return {
        **asdict(market),
        "asset": market.asset.value,
        "official_outcome": market.official_outcome.value,
        "quality_tier": market.quality_tier.value,
        "exclusion_reason": market.exclusion_reason.value if market.exclusion_reason else None,
    }


def event_row(event: BookEvent) -> dict[str, Any]:
    return {
        **asdict(event),
        "event_type": event.event_type.value,
        "bids": _level_rows(event.bids),
        "asks": _level_rows(event.asks),
    }


def event_from_row(row: dict[str, Any]) -> BookEvent:
    def levels(value: list[dict[str, Decimal]] | None) -> tuple[Level, ...] | None:
        if value is None:
            return None
        return tuple(Level(item["price"], item["size"]) for item in value)

    return BookEvent(
        condition_id=str(row["condition_id"]),
        token_id=str(row["token_id"]),
        source_ts_ns=int(row["source_ts_ns"]),
        receive_ts_ns=(
            int(row["receive_ts_ns"]) if row["receive_ts_ns"] is not None else None
        ),
        source_object=str(row["source_object"]),
        source_row=int(row["source_row"]),
        sequence=int(row["sequence"]),
        event_type=EventType(str(row["event_type"])),
        source_id=str(row["source_id"]),
        bids=levels(row["bids"]),
        asks=levels(row["asks"]),
        side=str(row["side"]) if row["side"] is not None else None,
        price=row["price"],
        size=row["size"],
        best_bid=row["best_bid"],
        best_ask=row["best_ask"],
        old_tick_size=row["old_tick_size"],
        new_tick_size=row["new_tick_size"],
        fee_rate_bps=(int(row["fee_rate_bps"]) if row["fee_rate_bps"] is not None else None),
        transaction_hash=(
            str(row["transaction_hash"]) if row["transaction_hash"] is not None else None
        ),
    )


def sample_row(sample: Sample200ms) -> dict[str, Any]:
    return {
        **asdict(sample),
        "quality_tier": sample.quality_tier.value,
        "bids": _level_rows(sample.bids),
        "asks": _level_rows(sample.asks),
    }


def underlying_row(item: UnderlyingObservation) -> dict[str, Any]:
    return {**asdict(item), "asset": item.asset.value}


def exclusion_row(item: Exclusion) -> dict[str, Any]:
    return {
        "market_id": item.market_id,
        "reason_code": item.reason_code.value,
        "detail": item.detail,
        "evidence_json": json.dumps(item.evidence, sort_keys=True, separators=(",", ":")),
    }


def write_table_atomic(path: Path, schema: pa.Schema, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        temporary,
        compression="zstd",
        compression_level=9,
        use_dictionary=False,
        write_statistics=True,
        data_page_version="2.0",
        version="2.6",
        row_group_size=65_536,
    )
    with temporary.open("rb+") as handle:
        os.fsync(handle.fileno())
    if path.exists() and path.read_bytes() != temporary.read_bytes():
        temporary.unlink()
        raise ConflictError(f"refusing to replace conflicting output: {path.name}")
    os.replace(temporary, path)


class StreamingTableWriter:
    """Write already-sorted batches with the pinned deterministic Parquet settings."""

    def __init__(self, path: Path, schema: pa.Schema):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.schema = schema
        self.temporary = path.with_suffix(path.suffix + ".partial")
        self.temporary.unlink(missing_ok=True)
        self.writer = pq.ParquetWriter(
            self.temporary,
            schema,
            compression="zstd",
            compression_level=9,
            use_dictionary=False,
            write_statistics=True,
            data_page_version="2.0",
            version="2.6",
        )
        self.rows = 0

    def append(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self.writer.write_table(
            pa.Table.from_pylist(rows, schema=self.schema), row_group_size=65_536
        )
        self.rows += len(rows)

    def finish(self) -> int:
        self.writer.close()
        with self.temporary.open("rb+") as handle:
            os.fsync(handle.fileno())
        if self.path.exists() and self.path.read_bytes() != self.temporary.read_bytes():
            self.temporary.unlink()
            raise ConflictError(f"refusing to replace conflicting output: {self.path.name}")
        os.replace(self.temporary, self.path)
        return self.rows

    def abort(self) -> None:
        self.writer.close()
        self.temporary.unlink(missing_ok=True)


def write_partition_tables(
    directory: Path,
    markets: Iterable[Market],
    events: Iterable[BookEvent],
    samples: Iterable[Sample200ms],
    underlying: Iterable[UnderlyingObservation],
    exclusions: Iterable[Exclusion],
) -> dict[str, int]:
    collections: list[tuple[str, pa.Schema, list[dict[str, Any]]]] = [
        (
            "markets.parquet",
            MARKET_SCHEMA,
            sorted((market_row(item) for item in markets), key=lambda row: row["condition_id"]),
        ),
        (
            "book-events.parquet",
            EVENT_SCHEMA,
            sorted(
                (event_row(item) for item in events),
                key=lambda row: (
                    row["condition_id"],
                    row["token_id"],
                    row["receive_ts_ns"],
                    row["sequence"],
                ),
            ),
        ),
        (
            "book-200ms.parquet",
            SAMPLE_SCHEMA,
            sorted(
                (sample_row(item) for item in samples),
                key=lambda row: (row["condition_id"], row["token_id"], row["grid_ts_ns"]),
            ),
        ),
        (
            "underlying.parquet",
            UNDERLYING_SCHEMA,
            sorted(
                (underlying_row(item) for item in underlying),
                key=lambda row: (row["asset"], row["source_ts_ns"], row["observation_kind"]),
            ),
        ),
        (
            "exclusions.parquet",
            EXCLUSION_SCHEMA,
            sorted(
                (exclusion_row(item) for item in exclusions),
                key=lambda row: (row["market_id"], row["reason_code"]),
            ),
        ),
    ]
    counts: dict[str, int] = {}
    for name, schema, rows in collections:
        write_table_atomic(directory / name, schema, rows)
        counts[name] = len(rows)
    return counts


def verify_parquet(path: Path, expected_schema: pa.Schema, expected_rows: int) -> None:
    metadata = pq.read_metadata(path)
    if metadata.num_rows != expected_rows:
        raise ConflictError(f"row count mismatch: {path.name}")
    actual = pq.read_schema(path)
    if not actual.equals(expected_schema, check_metadata=True):
        raise ConflictError(f"schema mismatch: {path.name}")
