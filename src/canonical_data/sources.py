"""Sequential production source loader joining discovery, ranges, and provenance."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient, discover, hourly_slug
from canonical_data.errors import SourceError, UnresolvedMarketError
from canonical_data.manifest import hash_file
from canonical_data.models import Asset, BookEvent, Exclusion, ExclusionReason, Market, Provenance
from canonical_data.pmxt import read_pmxt_parquet
from canonical_data.rangeio import open_http_range


@dataclass(frozen=True)
class OfficialDiscovery:
    markets: tuple[Market, ...]
    provenance: tuple[Provenance, ...]
    exclusions: tuple[Exclusion, ...] = ()


@dataclass(frozen=True)
class PMXTLoad:
    events: tuple[BookEvent, ...]
    provenance: tuple[Provenance, ...]


class ProductionSourceLoader:
    def __init__(
        self, gamma: GammaClient, retrieved_at_ns: int, official_cache_dir: Path | None = None
    ):
        self.gamma = gamma
        self.retrieved_at_ns = retrieved_at_ns
        self.official_cache_dir = official_cache_dir

    def discover(
        self,
        asset: Asset,
        market_starts_s: list[int],
        allow_missing: bool = False,
        allow_unresolved: bool = False,
    ) -> OfficialDiscovery:
        markets: list[Market] = []
        provenance: list[Provenance] = []
        exclusions: list[Exclusion] = []
        for start in sorted(set(market_starts_s)):
            slug = hourly_slug(asset, start)
            cached = self.official_cache_dir / f"{slug}.json" if self.official_cache_dir else None
            if cached is not None and cached.exists():
                payload = cached.read_bytes()
                url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
            else:
                payload, url = self.gamma.fetch_slug_payload(asset, start)
                if cached is not None:
                    cached.parent.mkdir(parents=True, exist_ok=True)
                    temporary = cached.with_suffix(".partial")
                    with temporary.open("wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(temporary, cached)
            unresolved = False
            try:
                found = discover([payload])
            except UnresolvedMarketError as exc:
                if not allow_unresolved:
                    raise
                found = []
                unresolved = True
                exclusions.append(
                    Exclusion(
                        exc.market_id,
                        ExclusionReason.UNRESOLVED_MARKET,
                        "official Gamma market has no final outcome",
                        {
                            "condition_id": exc.condition_id,
                            "payload_sha256": hashlib.sha256(payload).hexdigest(),
                            "slug": exc.slug,
                        },
                    )
                )
            if len(found) == 1 and found[0].asset is asset:
                if found[0].market_start_ns != start * 1_000_000_000:
                    raise SourceError("Gamma start does not match inventory")
                markets.append(found[0])
            elif unresolved:
                pass
            elif allow_missing and not found:
                exclusions.append(
                    Exclusion(
                        slug,
                        ExclusionReason.SOURCE_GAP,
                        "official Gamma slug returned no market",
                        {"payload_sha256": hashlib.sha256(payload).hexdigest()},
                    )
                )
            else:
                raise SourceError("Gamma identity does not match inventory")
            provenance.append(
                Provenance(
                    source_id="polymarket_gamma_clob",
                    source_url=url,
                    retrieved_at_ns=self.retrieved_at_ns,
                    byte_length=len(payload),
                    sha256=hashlib.sha256(payload).hexdigest(),
                    license_id="ungranted_for_bulk_redistribution",
                    source_precision="official_api",
                    transformations=("minimum_identity_rules_resolution_binding",),
                )
            )
        return OfficialDiscovery(tuple(markets), tuple(provenance), tuple(exclusions))

    def load_pmxt(
        self,
        urls: list[str],
        markets: tuple[Market, ...],
        max_transfer_per_object: int = 800_000_000,
        max_scanned_rows_per_object: int = 25_000_000,
        max_filtered_rows_per_object: int = 500_000,
    ) -> PMXTLoad:
        conditions = {market.condition_id for market in markets}
        tokens = {token for market in markets for token in (market.token_up, market.token_down)}
        receive_bounds = {
            market.condition_id: (market.market_start_ns - 3_600_000_000_000, market.market_end_ns)
            for market in markets
        }
        events: list[BookEvent] = []
        provenance: list[Provenance] = []
        for url in sorted(set(urls)):
            reader, identity, raw = open_http_range(url, max_transfer_per_object)
            try:
                object_events = read_pmxt_parquet(
                    reader,
                    conditions,
                    tokens,
                    url,
                    max_scanned_rows_per_object,
                    max_filtered_rows_per_object,
                    receive_bounds,
                )
            finally:
                reader.close()
            ledger = [item.__dict__ for item in raw.ledger]
            ledger_payload = canonical_json_bytes(
                {"etag": identity.etag, "object_bytes": identity.byte_length, "ranges": ledger}
            )
            events.extend(object_events)
            provenance.append(
                Provenance(
                    source_id="pmxt_v2",
                    source_url=url,
                    retrieved_at_ns=self.retrieved_at_ns,
                    byte_length=raw.transferred,
                    sha256=hashlib.sha256(ledger_payload).hexdigest(),
                    license_id="CC-BY-4.0",
                    source_precision="ms",
                    etag=identity.etag,
                    transformations=(
                        "conditional_range_read",
                        "condition_token_filter",
                        "market_receive_window_filter",
                    ),
                )
            )
        return PMXTLoad(tuple(events), tuple(provenance))

    def load_downloaded_pmxt(
        self,
        path: Path,
        source_url: str,
        markets: tuple[Market, ...],
        etag: str | None,
        max_filtered_rows: int = 500_000,
        verified_identity: tuple[int, str] | None = None,
        event_batch_consumer: Callable[[list[BookEvent]], None] | None = None,
        source_row_partition_by_condition: Mapping[str, str] | None = None,
        token_ids_by_source_row_partition: Mapping[str, set[str]] | None = None,
        max_filtered_rows_per_source_row_partition: int | None = None,
    ) -> PMXTLoad:
        """Filter one bounded whole-object fallback without retaining the raw archive."""
        conditions = {market.condition_id for market in markets}
        tokens = {token for market in markets for token in (market.token_up, market.token_down)}
        receive_bounds = {
            market.condition_id: (market.market_start_ns - 3_600_000_000_000, market.market_end_ns)
            for market in markets
        }
        length, digest = verified_identity if verified_identity is not None else hash_file(path)
        events = read_pmxt_parquet(
            path,
            conditions,
            tokens,
            source_url,
            max_scanned_rows=2_000_000_000,
            max_output_rows=max_filtered_rows,
            receive_bounds_by_condition=receive_bounds,
            event_batch_consumer=event_batch_consumer,
            source_row_partition_by_condition=source_row_partition_by_condition,
            token_ids_by_source_row_partition=token_ids_by_source_row_partition,
            max_output_rows_per_source_row_partition=(
                max_filtered_rows_per_source_row_partition
            ),
        )
        provenance = Provenance(
            source_id="pmxt_v2",
            source_url=source_url,
            retrieved_at_ns=self.retrieved_at_ns,
            byte_length=length,
            sha256=digest,
            license_id="CC-BY-4.0",
            source_precision="ms",
            etag=etag,
            transformations=(
                "bounded_whole_object_fallback",
                "condition_token_filter",
                "market_receive_window_filter",
            ),
        )
        return PMXTLoad(tuple(events), (provenance,))
