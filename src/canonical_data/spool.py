"""Bounded temporary PMXT event spool keyed by condition."""

from __future__ import annotations

import pickle
import sqlite3
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path

from canonical_data.errors import ConflictError
from canonical_data.models import BookEvent, EventType, Level
from canonical_data.pmxt import order_and_deduplicate

SPOOL_SCHEMA_VERSION = 2


def _encode_levels(levels: tuple[Level, ...] | None) -> tuple[tuple[str, str], ...] | None:
    if levels is None:
        return None
    return tuple((str(level.price), str(level.size)) for level in levels)


def _encode_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _encode_event(event: BookEvent) -> bytes:
    """Encode only values not already normalized into the spool primary key."""
    return pickle.dumps(
        (
            event.token_id,
            event.source_ts_ns,
            event.receive_ts_ns,
            event.sequence,
            event.event_type.value,
            event.source_id,
            _encode_levels(event.bids),
            _encode_levels(event.asks),
            event.side,
            _encode_decimal(event.price),
            _encode_decimal(event.size),
            _encode_decimal(event.best_bid),
            _encode_decimal(event.best_ask),
            _encode_decimal(event.old_tick_size),
            _encode_decimal(event.new_tick_size),
            event.fee_rate_bps,
            event.transaction_hash,
        ),
        protocol=5,
    )


def _decode_levels(
    levels: tuple[tuple[str, str], ...] | None,
) -> tuple[Level, ...] | None:
    if levels is None:
        return None
    return tuple(Level(Decimal(price), Decimal(size)) for price, size in levels)


def _decode_decimal(value: str | None) -> Decimal | None:
    return Decimal(value) if value is not None else None


def _decode_event(
    condition_id: str, source_object: str, source_row: int, payload: bytes
) -> BookEvent:
    try:
        values = pickle.loads(payload)
        if not isinstance(values, tuple) or len(values) != 17:
            raise ValueError("unexpected compact event payload")
        return BookEvent(
            condition_id=condition_id,
            token_id=str(values[0]),
            source_ts_ns=int(values[1]),
            receive_ts_ns=int(values[2]) if values[2] is not None else None,
            source_object=source_object,
            source_row=source_row,
            sequence=int(values[3]),
            event_type=EventType(str(values[4])),
            source_id=str(values[5]),
            bids=_decode_levels(values[6]),
            asks=_decode_levels(values[7]),
            side=str(values[8]) if values[8] is not None else None,
            price=_decode_decimal(values[9]),
            size=_decode_decimal(values[10]),
            best_bid=_decode_decimal(values[11]),
            best_ask=_decode_decimal(values[12]),
            old_tick_size=_decode_decimal(values[13]),
            new_tick_size=_decode_decimal(values[14]),
            fee_rate_bps=int(values[15]) if values[15] is not None else None,
            transaction_hash=str(values[16]) if values[16] is not None else None,
        )
    except (
        ArithmeticError,
        AttributeError,
        EOFError,
        ImportError,
        IndexError,
        TypeError,
        ValueError,
        pickle.PickleError,
    ) as exc:
        raise ConflictError("temporary event spool contains an invalid record") from exc


class EventSpool:
    def __init__(self, path: Path, create_index: bool = True):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.connection = sqlite3.connect(path)
        existing_columns = {
            str(row[1]) for row in self.connection.execute("PRAGMA table_info(events)")
        }
        if existing_columns and existing_columns != {
            "condition_key",
            "source_key",
            "source_row",
            "payload",
        }:
            self.connection.close()
            raise ConflictError("temporary event spool uses an incompatible schema")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS conditions ("
            "condition_key INTEGER PRIMARY KEY, condition_id TEXT NOT NULL UNIQUE)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS sources ("
            "source_key INTEGER PRIMARY KEY, source_object TEXT NOT NULL UNIQUE)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "condition_key INTEGER NOT NULL, source_key INTEGER NOT NULL, "
            "source_row INTEGER NOT NULL, payload BLOB NOT NULL, "
            "PRIMARY KEY(condition_key,source_key,source_row)) WITHOUT ROWID"
        )
        self.connection.execute(f"PRAGMA user_version={SPOOL_SCHEMA_VERSION}")
        self.connection.commit()
        self._condition_keys: dict[str, int] = {}
        self._source_keys: dict[str, int] = {}
        if create_index:
            self.ensure_index()

    def ensure_index(self) -> None:
        # Schema v2 is physically keyed by condition; no duplicate late index is needed.
        return None

    def drop_index(self) -> None:
        return None

    def _key(self, table: str, value_column: str, value: str) -> int:
        cache = self._condition_keys if table == "conditions" else self._source_keys
        cached = cache.get(value)
        if cached is not None:
            return cached
        self.connection.execute(
            f"INSERT OR IGNORE INTO {table}({value_column}) VALUES (?)", (value,)
        )
        row = self.connection.execute(
            f"SELECT {table[:-1]}_key FROM {table} WHERE {value_column}=?", (value,)
        ).fetchone()
        assert row is not None
        key = int(row[0])
        cache[value] = key
        return key

    def append(self, events: Iterable[BookEvent]) -> int:
        count = 0

        def rows() -> Iterable[tuple[int, int, int, sqlite3.Binary]]:
            nonlocal count
            for event in events:
                count += 1
                yield (
                    self._key("conditions", "condition_id", event.condition_id),
                    self._key("sources", "source_object", event.source_object),
                    event.source_row,
                    sqlite3.Binary(_encode_event(event)),
                )

        with self.connection:
            self.connection.executemany(
                "INSERT INTO events(condition_key,source_key,source_row,payload) "
                "VALUES (?,?,?,?)",
                rows(),
            )
        return count

    def discard_uncommitted_sources(self, completed_sources: set[str]) -> int:
        sources = {
            str(row[0])
            for row in self.connection.execute(
                "SELECT DISTINCT sources.source_object FROM events "
                "JOIN sources USING(source_key)"
            )
        }
        uncommitted = sources - completed_sources
        removed = 0
        with self.connection:
            for source in sorted(uncommitted):
                cursor = self.connection.execute(
                    "DELETE FROM events WHERE source_key=("
                    "SELECT source_key FROM sources WHERE source_object=?)",
                    (source,),
                )
                removed += cursor.rowcount
        return removed

    def load(self, condition_id: str) -> list[BookEvent]:
        rows = self.connection.execute(
            "SELECT sources.source_object,events.source_row,events.payload FROM events "
            "JOIN conditions USING(condition_key) JOIN sources USING(source_key) "
            "WHERE conditions.condition_id=?",
            (condition_id,),
        ).fetchall()
        events = []
        for source_object, source_row, payload in rows:
            events.append(
                _decode_event(condition_id, str(source_object), int(source_row), payload)
            )
        return order_and_deduplicate(events)

    def count(self) -> int:
        value = self.connection.execute("SELECT COUNT(*) FROM events").fetchone()
        assert value is not None
        return int(value[0])

    def count_condition(self, condition_id: str) -> int:
        value = self.connection.execute(
            "SELECT COUNT(*) FROM events JOIN conditions USING(condition_key) "
            "WHERE conditions.condition_id=?",
            (condition_id,),
        ).fetchone()
        assert value is not None
        return int(value[0])

    def counts_by_condition(self) -> dict[str, int]:
        rows = self.connection.execute(
            "SELECT conditions.condition_id,COUNT(*) FROM events "
            "JOIN conditions USING(condition_key) GROUP BY events.condition_key"
        ).fetchall()
        return {str(condition_id): int(count) for condition_id, count in rows}

    def storage_bytes(self) -> int:
        page_count = int(self.connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(self.connection.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> EventSpool:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
