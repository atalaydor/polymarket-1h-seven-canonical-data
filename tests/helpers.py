from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from canonical_data.models import Asset, Market, Outcome, Provenance, QualityTier

if os.name == "nt":
    def _writable_mkdtemp(
        suffix: str | None = None,
        prefix: str | None = None,
        dir: str | None = None,
    ) -> str:
        root = Path(dir or tempfile.gettempdir())
        for _ in range(100):
            candidate = root / f"{prefix or 'tmp'}{uuid.uuid4().hex}{suffix or ''}"
            try:
                os.mkdir(candidate, 0o777)
            except FileExistsError:
                continue
            return str(candidate)
        raise FileExistsError("could not allocate a writable Windows test directory")

    # Python 3.14's secure Windows mkdir mode can be unwritable in sandboxed CI shells.
    tempfile.mkdtemp = _writable_mkdtemp  # type: ignore[assignment]
from canonical_data.discovery import hourly_slug

START_S = 1_776_106_800
START_NS = START_S * 1_000_000_000
CONDITION = "0x" + "a" * 64


def gamma_payload(asset: Asset = Asset.DOGE, outcome_prices: list[str] | None = None) -> bytes:
    symbol = asset.value
    display = "Hyperliquid" if asset is Asset.HYPE else asset.value
    source = (
        "https://www.binance.com/en/futures/HYPEUSDT"
        if asset is Asset.HYPE
        else f"https://www.binance.com/en/trade/{symbol}_USDT"
    )
    event = {
        "id": "event-1",
        "markets": [
            {
                "id": "market-1",
                "slug": hourly_slug(asset, START_S),
                "conditionId": CONDITION,
                "outcomes": json.dumps(["Up", "Down"]),
                "clobTokenIds": json.dumps(["1", "2"]),
                "closed": True,
                "outcomePrices": json.dumps(outcome_prices or ["1", "0"]),
                "endDate": datetime.fromtimestamp(START_S + 3_600, UTC).isoformat(),
                "resolutionSource": source,
                "description": (
                    f'This market resolves to "Up" if the close price is greater than or equal '
                    f"to the open price for the {symbol}/USDT 1 hour candle. Otherwise, this "
                    f'market resolves to "Down". The resolution source is Binance for '
                    f"{display}: {source}"
                ),
            }
        ],
    }
    return json.dumps([event], sort_keys=True).encode()


def market(asset: Asset = Asset.DOGE, tier: QualityTier = QualityTier.TIER_A) -> Market:
    payload = gamma_payload(asset)
    return Market(
        asset=asset,
        event_id="event-1",
        market_id="market-1",
        condition_id=CONDITION,
        token_up="1",
        token_down="2",
        market_start_ns=START_NS,
        market_end_ns=START_NS + 3_600_000_000_000,
        rules_text_sha256="b" * 64,
        resolution_source_url=(
            "https://www.binance.com/en/futures/HYPEUSDT"
            if asset is Asset.HYPE
            else f"https://www.binance.com/en/trade/{asset.value}_USDT"
        ),
        official_outcome=Outcome.UP,
        official_resolution_ts_ns=START_NS + 301_000_000_000,
        quality_tier=tier,
        evidence_sha256=hashlib.sha256(payload).hexdigest(),
    )


def provenance(source_id: str = "pmxt_v2") -> Provenance:
    return Provenance(
        source_id=source_id,
        source_url="https://example.test/source",
        retrieved_at_ns=START_NS,
        byte_length=10,
        sha256="c" * 64,
        license_id="CC-BY-4.0",
        source_precision="ms",
        transformations=("decode",),
    )


def pmxt_rows(update: bool = True) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for token in ("1", "2"):
        rows.append(
            {
                "timestamp_received": START_S * 1000,
                "timestamp": START_S * 1000,
                "market": CONDITION,
                "event_type": "book",
                "asset_id": token,
                "bids": '[["0.40","10.000000"]]',
                "asks": '[["0.60","12.000000"]]',
            }
        )
    if update:
        rows.extend(
            [
                {
                    "timestamp_received": START_S * 1000 + 200,
                    "timestamp": START_S * 1000 + 190,
                    "market": CONDITION,
                    "event_type": "price_change",
                    "asset_id": "1",
                    "side": "BUY",
                    "price": "0.45",
                    "size": "5.000000",
                    "best_bid": "0.45",
                    "best_ask": "0.60",
                },
                {
                    "timestamp_received": START_S * 1000 + 300,
                    "timestamp": START_S * 1000 + 290,
                    "market": CONDITION,
                    "event_type": "tick_size_change",
                    "asset_id": "1",
                    "old_tick_size": "0.01",
                    "new_tick_size": "0.001",
                },
                {
                    "timestamp_received": START_S * 1000 + 400,
                    "timestamp": START_S * 1000 + 390,
                    "market": CONDITION,
                    "event_type": "last_trade_price",
                    "asset_id": "1",
                    "side": "BUY",
                    "price": "0.46",
                    "size": "1.000000",
                    "fee_rate_bps": 100,
                    "transaction_hash": "0x123",
                },
            ]
        )
    return rows


def d(value: str) -> Decimal:
    return Decimal(value)
