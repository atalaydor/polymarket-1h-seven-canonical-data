"""Strict Binance public archive normalization as non-settlement evidence."""

from __future__ import annotations

import csv
import hashlib
import io
import zipfile
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from pathlib import Path

from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.models import Asset, UnderlyingObservation
from canonical_data.timeutil import epoch_to_ns

SYMBOLS = {
    Asset.BTC: ("BTCUSDT", "spot"),
    Asset.ETH: ("ETHUSDT", "spot"),
    Asset.SOL: ("SOLUSDT", "spot"),
    Asset.XRP: ("XRPUSDT", "spot"),
    Asset.DOGE: ("DOGEUSDT", "spot"),
    Asset.BNB: ("BNBUSDT", "spot"),
    Asset.HYPE: ("HYPEUSDT", "usds_m_perpetual"),
}


def verify_sha256(path: Path, expected: str) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != expected.lower():
        raise SourceError("Binance source checksum mismatch")
    return actual


def _rows_from_zip(path: Path, max_uncompressed_bytes: int) -> Iterable[list[str]]:
    with zipfile.ZipFile(path) as archive:
        members = [member for member in archive.infolist() if not member.is_dir()]
        if len(members) != 1:
            raise SourceError("Binance archive must contain exactly one file")
        member = members[0]
        if member.file_size > max_uncompressed_bytes:
            raise ResourceLimitError("Binance archive exceeds decompression cap")
        if Path(member.filename).name != member.filename:
            raise SourceError("unsafe Binance archive member")
        with archive.open(member) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            yield from csv.reader(text)


def _precision(asset: Asset, timestamp: int) -> str:
    if asset is Asset.HYPE:
        return "ms"
    return "us" if timestamp >= 1_735_689_600_000_000 else "ms"


def ingest_binance_zip(
    path: Path,
    expected_sha256: str,
    asset: Asset,
    kind: str,
    source_path: str,
    max_uncompressed_bytes: int = 250_000_000,
) -> list[UnderlyingObservation]:
    digest = verify_sha256(path, expected_sha256)
    symbol, instrument = SYMBOLS[asset]
    observations: list[UnderlyingObservation] = []
    supported = {"trades", "aggTrades", "klines", "markPriceKlines", "indexPriceKlines"}
    if kind not in supported:
        raise SourceError("unsupported Binance observation kind")
    for raw in _rows_from_zip(path, max_uncompressed_bytes):
        if not raw:
            continue
        if not raw[0].lstrip("-").isdigit():
            continue
        try:
            if kind == "trades":
                timestamp, value = int(raw[4]), Decimal(raw[1])
            elif kind == "aggTrades":
                timestamp, value = int(raw[5]), Decimal(raw[1])
            else:
                timestamp, value = int(raw[0]), Decimal(raw[4])
        except (IndexError, ValueError, InvalidOperation) as exc:
            raise SourceError("malformed Binance CSV row") from exc
        if not value.is_finite() or value <= 0:
            raise SourceError("invalid Binance value")
        observations.append(
            UnderlyingObservation(
                asset=asset,
                instrument_type=instrument,
                symbol=symbol,
                observation_kind=kind,
                source_ts_ns=epoch_to_ns(timestamp, _precision(asset, timestamp)),
                receive_ts_ns=None,
                value=value,
                source_path=source_path,
                source_sha256=digest,
                is_settlement=False,
            )
        )
    if not observations:
        raise SourceError("Binance archive contains no observations")
    return sorted(observations, key=lambda item: item.source_ts_ns)
