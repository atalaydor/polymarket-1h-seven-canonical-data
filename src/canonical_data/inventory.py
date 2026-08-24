"""Deterministic bounded source inventory."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from canonical_data.errors import SourceError
from canonical_data.models import Asset

PMXT_OBJECT_COVERAGE_START = datetime(2026, 4, 13, 19, tzinfo=UTC)
PMXT_OBJECT_COVERAGE_CUTOFF = datetime(2026, 8, 10, 1, tzinfo=UTC)
PMXT_VALIDATION_COVERAGE_START = datetime(2026, 4, 18, 20, tzinfo=UTC)
PMXT_MISSING_OBJECT_URLS = frozenset(
    f"https://r2v2.pmxt.dev/polymarket_orderbook_2026-06-11T{hour}.parquet"
    for hour in ("04", "05", "06")
)


@dataclass(frozen=True)
class SourceObject:
    source_id: str
    url: str
    expected_sha256: str | None = None
    expected_bytes: int | None = None


def expected_1h_market_starts(day: date, coverage_start: datetime, cutoff: datetime) -> list[int]:
    if coverage_start.tzinfo is None or cutoff.tzinfo is None:
        raise SourceError("coverage bounds must be timezone-aware")
    if cutoff <= coverage_start:
        raise SourceError("release cutoff must follow coverage start")
    midnight = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    coverage_start_s = int(coverage_start.timestamp())
    cutoff_s = int(cutoff.timestamp())
    return [
        midnight + offset
        for offset in range(0, 86_400, 3_600)
        if coverage_start_s <= midnight + offset < cutoff_s
    ]


def pmxt_hourly_objects(start_ns: int, end_ns: int) -> list[SourceObject]:
    if end_ns <= start_ns:
        raise SourceError("inventory range must be positive")
    catalog_start_ns = int(PMXT_OBJECT_COVERAGE_START.timestamp()) * 1_000_000_000
    catalog_cutoff_ns = int(PMXT_OBJECT_COVERAGE_CUTOFF.timestamp()) * 1_000_000_000
    if start_ns < catalog_start_ns or end_ns > catalog_cutoff_ns:
        raise SourceError("PMXT inventory range exceeds the frozen authoritative catalog")
    start = datetime.fromtimestamp(start_ns // 1_000_000_000, UTC).replace(
        minute=0, second=0, microsecond=0
    )
    end = datetime.fromtimestamp((end_ns - 1) // 1_000_000_000, UTC).replace(
        minute=0, second=0, microsecond=0
    )
    current = start
    result: list[SourceObject] = []
    while current <= end:
        stamp = current.strftime("%Y-%m-%dT%H")
        result.append(
            SourceObject("pmxt_v2", f"https://r2v2.pmxt.dev/polymarket_orderbook_{stamp}.parquet")
        )
        current += timedelta(hours=1)
    return result


def binance_daily_objects(asset: Asset, day: str, kinds: tuple[str, ...]) -> list[SourceObject]:
    symbol = f"{asset.value}USDT"
    market = "um" if asset is Asset.HYPE else "spot"
    root = f"https://data.binance.vision/data/{'futures/um' if market == 'um' else 'spot'}/daily"
    result: list[SourceObject] = []
    for kind in kinds:
        if kind in {"klines", "markPriceKlines", "indexPriceKlines"}:
            filename = f"{symbol}-1m-{day}.zip"
            url = f"{root}/{kind}/{symbol}/1m/{filename}"
        elif kind in {"trades", "aggTrades"}:
            filename = f"{symbol}-{kind}-{day}.zip"
            url = f"{root}/{kind}/{symbol}/{filename}"
        else:
            raise SourceError(f"unsupported Binance kind: {kind}")
        result.append(SourceObject("binance_public", url))
    return result
