"""Typed versioned interchange records."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any

SCHEMA_VERSION = "1.0.0"
NS_PER_MS = 1_000_000
NS_PER_SECOND = 1_000_000_000


class Asset(StrEnum):
    BTC = "BTC"
    ETH = "ETH"
    SOL = "SOL"
    XRP = "XRP"
    DOGE = "DOGE"
    BNB = "BNB"
    HYPE = "HYPE"


class QualityTier(StrEnum):
    TIER_A = "TIER_A"
    TIER_B = "TIER_B"
    EXCLUDED = "EXCLUDED"


class EventType(StrEnum):
    BOOK = "BOOK"
    PRICE_CHANGE = "PRICE_CHANGE"
    LAST_TRADE_PRICE = "LAST_TRADE_PRICE"
    TICK_SIZE_CHANGE = "TICK_SIZE_CHANGE"


class Outcome(StrEnum):
    UP = "UP"
    DOWN = "DOWN"
    SPLIT = "SPLIT"
    INVALID = "INVALID"
    UNRESOLVED = "UNRESOLVED"


class ExclusionReason(StrEnum):
    IDENTITY_CONFLICT = "IDENTITY_CONFLICT"
    RULES_UNVERIFIED = "RULES_UNVERIFIED"
    OUTCOME_UNVERIFIED = "OUTCOME_UNVERIFIED"
    NO_INITIAL_SNAPSHOT = "NO_INITIAL_SNAPSHOT"
    EVENT_CONFLICT = "EVENT_CONFLICT"
    SOURCE_GAP = "SOURCE_GAP"
    UNSUPPORTED_TIMEFRAME = "UNSUPPORTED_TIMEFRAME"
    OUT_OF_SCOPE_DATE = "OUT_OF_SCOPE_DATE"
    LICENSE_UNRESOLVED = "LICENSE_UNRESOLVED"
    UNRESOLVED_MARKET = "UNRESOLVED_MARKET"


@dataclass(frozen=True, order=True)
class Level:
    price: Decimal
    size: Decimal

    def __post_init__(self) -> None:
        if self.price < 0 or self.price > 1 or self.size < 0:
            raise ValueError("invalid prediction-market level")


@dataclass(frozen=True)
class Provenance:
    source_id: str
    source_url: str
    retrieved_at_ns: int
    byte_length: int
    sha256: str
    license_id: str
    source_precision: str
    etag: str | None = None
    upstream_checksum: str | None = None
    transformations: tuple[str, ...] = ()


@dataclass(frozen=True)
class Market:
    asset: Asset
    event_id: str
    market_id: str
    condition_id: str
    token_up: str
    token_down: str
    market_start_ns: int
    market_end_ns: int
    rules_text_sha256: str
    resolution_source_url: str
    official_outcome: Outcome
    official_resolution_ts_ns: int | None
    quality_tier: QualityTier
    evidence_sha256: str
    exclusion_reason: ExclusionReason | None = None
    venue: str = "polymarket"
    timeframe: str = "1h"
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True)
class BookEvent:
    condition_id: str
    token_id: str
    source_ts_ns: int
    receive_ts_ns: int | None
    source_object: str
    source_row: int
    sequence: int
    event_type: EventType
    source_id: str = "pmxt_v2"
    bids: tuple[Level, ...] | None = None
    asks: tuple[Level, ...] | None = None
    side: str | None = None
    price: Decimal | None = None
    size: Decimal | None = None
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    old_tick_size: Decimal | None = None
    new_tick_size: Decimal | None = None
    fee_rate_bps: int | None = None
    transaction_hash: str | None = None

    @property
    def order_key(self) -> tuple[int, int, str, int]:
        effective_receive = (
            self.receive_ts_ns if self.receive_ts_ns is not None else self.source_ts_ns
        )
        return (effective_receive, self.source_ts_ns, self.source_object, self.source_row)


@dataclass(frozen=True)
class BookState:
    condition_id: str
    token_id: str
    source_ts_ns: int
    receive_ts_ns: int | None
    sequence: int
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    tick_size: Decimal | None
    native_interval_ms: int = 0
    quality_tier: QualityTier = QualityTier.TIER_A


@dataclass(frozen=True)
class Sample200ms:
    condition_id: str
    token_id: str
    grid_ts_ns: int
    asof_source_ts_ns: int
    asof_receive_ts_ns: int | None
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    stale_ms: int
    source_event_sequence: int
    quality_tier: QualityTier
    native_interval_ms: int


@dataclass(frozen=True)
class UnderlyingObservation:
    asset: Asset
    instrument_type: str
    symbol: str
    observation_kind: str
    source_ts_ns: int
    receive_ts_ns: int | None
    value: Decimal
    source_path: str
    source_sha256: str
    is_settlement: bool = False
    venue: str = "binance"


@dataclass(frozen=True)
class Exclusion:
    market_id: str
    reason_code: ExclusionReason
    detail: str
    evidence: dict[str, Any] = field(default_factory=dict)
