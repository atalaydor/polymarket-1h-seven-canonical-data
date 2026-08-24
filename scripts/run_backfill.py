"""Sequential, restart-safe Polymarket 1h x 7 executor for Linux Actions."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar, cast

import pyarrow as pa
import pyarrow.parquet as pq

from canonical_data.acquire import BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient
from canonical_data.errors import ResourceLimitError, SourceError, SourceIdentityError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import (
    PMXT_MISSING_OBJECT_URLS,
    SourceObject,
    expected_1h_market_starts,
    pmxt_hourly_objects,
)
from canonical_data.manifest import hash_file
from canonical_data.models import (
    Asset,
    BookEvent,
    Exclusion,
    ExclusionReason,
    Market,
    Outcome,
    Provenance,
    QualityTier,
)
from canonical_data.parquetio import EVENT_SCHEMA, event_from_row, event_row
from canonical_data.pipeline import (
    PartitionInputs,
    Pipeline,
    PipelineLimits,
    StagedMarket,
    stage_market,
)
from canonical_data.planner import release_bucket
from canonical_data.pmxt import order_and_deduplicate
from canonical_data.release import GitHubReleaseBackend, Publisher
from canonical_data.sources import OfficialDiscovery, ProductionSourceLoader
from canonical_data.spool import EventSpool
from canonical_data.state import StateStore

REPOSITORY = "atalaydor/polymarket-1h-seven-canonical-data"
DATASET_RELEASE_PREFIX = "polymarket-1h-seven-v1"
RETRY_DELAYS = (2, 8, 32)
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
# The certified 15m reference measured 810,276 causal rows for one condition in
# one hourly object. Before the 1h canary supplies native measurements, permit
# five times that observed density. A 1h market's one-hour warm-up plus one-hour
# live window intersects two objects; each object intersects at most two 1h
# markets for one asset (one live and the following market's warm-up).
PMXT_MEASURED_ROWS_PER_MARKET_OBJECT = 810_276
PMXT_ROWS_PER_MARKET_OBJECT_WITH_MARGIN = (
    PMXT_MEASURED_ROWS_PER_MARKET_OBJECT * 5
)
PMXT_OBJECTS_PER_MARKET = 2
PMXT_ROWS_PER_MARKET_WITH_MARGIN = (
    PMXT_ROWS_PER_MARKET_OBJECT_WITH_MARGIN * PMXT_OBJECTS_PER_MARKET
)
PMXT_MARKETS_PER_ASSET_OBJECT = 2
PMXT_FILTERED_ROWS_PER_ASSET_OBJECT = (
    PMXT_ROWS_PER_MARKET_OBJECT_WITH_MARGIN * PMXT_MARKETS_PER_ASSET_OBJECT
)
PMXT_FILTERED_ROWS_PER_ASSET_DAY = PMXT_ROWS_PER_MARKET_WITH_MARGIN * 24
MAX_SOURCE_OBJECT_BYTES = 800_000_000
# With the compact condition-keyed spool there is no late index copy. This reserve
# covers two simultaneous maximum source objects plus two maximum transformed
# partitions; it is a runaway breaker, not a workload throttle.
MINIMUM_FREE_DISK_BYTES = 4_000_000_000
T = TypeVar("T")

# These immutable hourly objects span every Polymarket condition; their identities
# and observed absence are timeframe-neutral. The canary records fresh access.
PMXT_HTTP_404_GAPS = {
    url: {
        "accessed_at": "2026-08-15",
        "http_status": 404,
    }
    for url in PMXT_MISSING_OBJECT_URLS
}


def _peak_rss_kib() -> int:
    try:
        resource_module = cast(Any, importlib.import_module("resource"))
    except ModuleNotFoundError:
        return 0
    return int(resource_module.getrusage(resource_module.RUSAGE_SELF).ru_maxrss)


def enforce_shared_pmxt_asset_caps(
    events: tuple[BookEvent, ...],
    markets_by_asset: Mapping[Asset, tuple[Market, ...]],
    day_market_counts: dict[str, int] | None = None,
    day_asset_counts: dict[Asset, int] | None = None,
) -> dict[Asset, int]:
    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    counts = {asset: 0 for asset in markets_by_asset}
    market_counts: dict[str, int] = {}
    for event in events:
        asset = owner.get(event.condition_id)
        if asset is None:
            raise SourceError("shared PMXT event is outside the bound market inventory")
        counts[asset] += 1
        market_counts[event.condition_id] = market_counts.get(event.condition_id, 0) + 1
    for asset, count in counts.items():
        if count > PMXT_FILTERED_ROWS_PER_ASSET_OBJECT:
            raise ResourceLimitError(
                f"PMXT filtered output for {asset.value} exceeds per-object asset cap "
                f"({count} > {PMXT_FILTERED_ROWS_PER_ASSET_OBJECT})"
            )
    for condition_id, count in market_counts.items():
        if count > PMXT_ROWS_PER_MARKET_OBJECT_WITH_MARGIN:
            raise ResourceLimitError(
                "PMXT filtered output exceeds measured per-market object capacity bound "
                f"(condition={condition_id}, rows={count}, "
                f"bound={PMXT_ROWS_PER_MARKET_OBJECT_WITH_MARGIN})"
            )
    if day_market_counts is not None:
        for condition_id, count in market_counts.items():
            projected = day_market_counts.get(condition_id, 0) + count
            if projected > PMXT_ROWS_PER_MARKET_WITH_MARGIN:
                raise ResourceLimitError(
                    "PMXT filtered output exceeds measured per-market daily capacity bound "
                    f"(condition={condition_id}, rows={projected}, "
                    f"bound={PMXT_ROWS_PER_MARKET_WITH_MARGIN})"
                )
    if day_asset_counts is not None:
        for asset, count in counts.items():
            projected = day_asset_counts.get(asset, 0) + count
            if projected > PMXT_FILTERED_ROWS_PER_ASSET_DAY:
                raise ResourceLimitError(
                    f"PMXT filtered output for {asset.value} exceeds per-day asset cap "
                    f"({projected} > {PMXT_FILTERED_ROWS_PER_ASSET_DAY})"
                )
    if day_market_counts is not None:
        for condition_id, count in market_counts.items():
            day_market_counts[condition_id] = day_market_counts.get(condition_id, 0) + count
    if day_asset_counts is not None:
        for asset, count in counts.items():
            day_asset_counts[asset] = day_asset_counts.get(asset, 0) + count
    return counts


def _pmxt_source_window_ns(source: SourceObject) -> tuple[int, int]:
    stamp = source.url.rsplit("_", 1)[-1].removesuffix(".parquet")
    try:
        start = datetime.strptime(stamp, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    except ValueError as exc:
        raise SourceError("PMXT source URL lacks an authoritative hourly identity") from exc
    start_ns = int(start.timestamp()) * 1_000_000_000
    return start_ns, start_ns + 3_600_000_000_000


def _markets_relevant_to_source(
    markets: tuple[Market, ...], source: SourceObject
) -> tuple[Market, ...]:
    source_start_ns, source_end_ns = _pmxt_source_window_ns(source)
    relevant = tuple(
        market
        for market in markets
        if market.market_end_ns > source_start_ns
        and market.market_start_ns - 3_600_000_000_000 < source_end_ns
    )
    if len(relevant) > PMXT_MARKETS_PER_ASSET_OBJECT:
        raise ResourceLimitError("PMXT object intersects too many 1h market inventories")
    return relevant


def _restore_shared_pmxt_counts(
    spool: EventSpool, markets_by_asset: Mapping[Asset, tuple[Market, ...]]
) -> tuple[dict[str, int], dict[Asset, int]]:
    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    market_counts = spool.counts_by_condition()
    asset_counts = {asset: 0 for asset in markets_by_asset}
    for condition_id, count in market_counts.items():
        asset = owner.get(condition_id)
        if asset is None:
            raise SourceError("shared PMXT spool contains an event outside market inventory")
        if count > PMXT_ROWS_PER_MARKET_WITH_MARGIN:
            raise ResourceLimitError("resumed PMXT market exceeds daily capacity bound")
        asset_counts[asset] += count
    if any(count > PMXT_FILTERED_ROWS_PER_ASSET_DAY for count in asset_counts.values()):
        raise ResourceLimitError("resumed PMXT asset exceeds daily capacity bound")
    return market_counts, asset_counts


def _atomic_json(path: Path, value: Any) -> None:
    payload = canonical_json_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".partial")
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _tool_commit() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
    ).strip()


def _fetch_gamma(url: str, max_bytes: int) -> bytes:
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            request = urllib.request.Request(
                url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                length = response.headers.get("Content-Length")
                if length is not None and int(length) > max_bytes:
                    raise SourceError("Gamma payload exceeds configured bound")
                payload = cast(bytes, response.read(max_bytes + 1))
            if len(payload) > max_bytes:
                raise SourceError("Gamma payload exceeds configured bound")
            return payload
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _acquire_with_retry(
    source: SourceObject,
    raw_dir: Path,
    max_object_bytes: int = MAX_SOURCE_OBJECT_BYTES,
    expected_identity: tuple[int, str] | None = None,
) -> Any:
    stable_identity = expected_identity or _probe_source_identity(source)
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            return BoundedAcquirer(
                raw_dir, max_object_bytes, MINIMUM_FREE_DISK_BYTES
            ).acquire(
                source,
                expected_identity=stable_identity,
                validator=_validate_pmxt_download,
            )
        except (ResourceLimitError, SourceIdentityError):
            raise
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last = exc
        except (SourceError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _probe_source_identity(source: SourceObject) -> tuple[int, str]:
    request = urllib.request.Request(
        source.url,
        method="HEAD",
        headers={"Accept-Encoding": "identity", "User-Agent": USER_AGENT},
    )
    last: Exception | None = None
    for attempt, delay in enumerate((0, *RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                length = int(response.headers.get("Content-Length", "0"))
                etag = response.headers.get("ETag", "")
                if length <= 0 or not etag:
                    raise SourceIdentityError("source HEAD lacks stable length and ETag identity")
                return length, etag
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last = exc
        if attempt == len(RETRY_DELAYS):
            break
    assert last is not None
    raise last


def _validate_pmxt_download(path: Path) -> None:
    try:
        pq.read_metadata(path)
    except (pa.ArrowException, OSError) as exc:
        raise SourceError("downloaded PMXT object is not valid Parquet") from exc


def _provenance_from_json(value: dict[str, Any]) -> Provenance:
    return Provenance(
        source_id=str(value["source_id"]),
        source_url=str(value["source_url"]),
        retrieved_at_ns=int(value["retrieved_at_ns"]),
        byte_length=int(value["byte_length"]),
        sha256=str(value["sha256"]),
        license_id=str(value["license_id"]),
        source_precision=str(value["source_precision"]),
        etag=cast(str | None, value.get("etag")),
        upstream_checksum=cast(str | None, value.get("upstream_checksum")),
        transformations=tuple(str(item) for item in value["transformations"]),
    )


def _market_starts(
    day: date,
    coverage_start: datetime,
    cutoff: datetime,
    starts: tuple[int, ...] | None,
) -> list[int]:
    if starts is None:
        return expected_1h_market_starts(day, coverage_start, cutoff)
    midnight = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp())
    end = midnight + 86_400
    if any(start < midnight or start >= end or start % 3_600 for start in starts):
        raise SourceError("explicit market starts must be aligned 1h timestamps in one UTC day")
    return sorted(set(starts))


def _validate_expected_market_identities(
    discoveries: dict[Asset, OfficialDiscovery],
    expected: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None,
) -> None:
    if expected is None:
        return
    if set(expected) != set(discoveries):
        raise SourceError("expected market identities do not cover the execution assets")
    for asset, discovery in discoveries.items():
        actual = {
            (market.condition_id, frozenset((market.token_up, market.token_down)))
            for market in discovery.markets
        }
        if actual != expected[asset]:
            raise SourceError("child discovery does not match source-qualified canary identity")


def _validate_expected_source_identity(
    source: SourceObject,
    byte_length: int,
    etag: str | None,
    expected: dict[str, tuple[int, str]] | None,
) -> None:
    if expected is None:
        return
    if expected.get(source.url) != (byte_length, etag):
        raise SourceError("acquired PMXT object does not match source-qualified identity")


def _verify_shared_disk_margin(
    shared: Path,
    spool: EventSpool,
    completed_sources: int,
    source_bytes: int,
) -> int:
    usage = shutil.disk_usage(shared)
    spool_bytes = spool.storage_bytes()
    if usage.free < MINIMUM_FREE_DISK_BYTES:
        raise ResourceLimitError(
            "shared PMXT spool exhausted disk safety margin "
            f"(free_bytes={usage.free}, required_free_bytes={MINIMUM_FREE_DISK_BYTES}, "
            f"spool_bytes={spool_bytes}, completed_sources={completed_sources}, "
            f"source_bytes={source_bytes}, disk_total_bytes={usage.total})"
        )
    return usage.free


def _tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _write_pending_events(path: Path, events: list[BookEvent]) -> None:
    """Write a lossless, fast-compressed temporary fragment with atomic visibility."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    pq.write_table(
        pa.Table.from_pylist([event_row(item) for item in events], schema=EVENT_SCHEMA),
        temporary,
        compression="zstd",
        compression_level=1,
        use_dictionary=False,
        write_statistics=True,
        data_page_version="2.0",
        version="2.6",
        row_group_size=65_536,
    )
    with temporary.open("rb+") as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class PendingMarketFragments:
    """Compressed source fragments retained only until a market is causally complete."""

    def __init__(self, root: Path):
        self.root = root
        self._paths: dict[str, list[Path]] = {}

    def append(self, condition_id: str, source_index: int, events: list[BookEvent]) -> None:
        if not events:
            return
        path = self.root / condition_id / f"source-{source_index:02d}.parquet"
        _write_pending_events(path, events)
        self._paths.setdefault(condition_id, []).append(path)

    def load(self, condition_id: str) -> list[BookEvent]:
        events: list[BookEvent] = []
        for path in self._paths.get(condition_id, []):
            events.extend(event_from_row(row) for row in pq.read_table(path).to_pylist())
        return events

    def discard(self, condition_id: str) -> None:
        paths = self._paths.pop(condition_id, [])
        for path in paths:
            path.unlink(missing_ok=True)
        condition_dir = self.root / condition_id
        if condition_dir.exists():
            condition_dir.rmdir()

    def active_conditions(self) -> int:
        return len(self._paths)

    def storage_bytes(self) -> int:
        return _tree_bytes(self.root)


def _pending_fragment_consumer(
    fragments: PendingMarketFragments, shared: Path, free_samples: list[int]
) -> Callable[[list[BookEvent]], None]:
    batch_number = 0

    def consume(events: list[BookEvent]) -> None:
        nonlocal batch_number
        groups: dict[str, list[BookEvent]] = {}
        for event in events:
            groups.setdefault(event.condition_id, []).append(event)
        for condition_id, group in groups.items():
            fragments.append(condition_id, batch_number, group)
            batch_number += 1
        free_bytes = shutil.disk_usage(shared).free
        free_samples.append(free_bytes)
        if free_bytes < MINIMUM_FREE_DISK_BYTES:
            raise ResourceLimitError("current PMXT source fragments exhausted disk safety margin")

    return consume


def _verify_staged_disk_margin(
    shared: Path,
    pending: PendingMarketFragments,
    stage_roots: Mapping[Asset, Path],
    completed_sources: int,
    completed_markets: int,
    source_bytes: int,
) -> tuple[int, int, int]:
    usage = shutil.disk_usage(shared)
    pending_bytes = pending.storage_bytes()
    staged_bytes = sum(_tree_bytes(path) for path in stage_roots.values())
    if usage.free < MINIMUM_FREE_DISK_BYTES:
        raise ResourceLimitError(
            "bounded PMXT staging exhausted disk safety margin "
            f"(free_bytes={usage.free}, required_free_bytes={MINIMUM_FREE_DISK_BYTES}, "
            f"pending_bytes={pending_bytes}, staged_bytes={staged_bytes}, "
            f"active_conditions={pending.active_conditions()}, "
            f"completed_sources={completed_sources}, completed_markets={completed_markets}, "
            f"source_bytes={source_bytes}, disk_total_bytes={usage.total})"
        )
    return usage.free, pending_bytes, staged_bytes


def prepare_shared_day(
    day: date,
    work_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...] = tuple(Asset),
    starts: tuple[int, ...] | None = None,
    expected_market_identities: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None = None,
    expected_source_identities: dict[str, tuple[int, str]] | None = None,
) -> tuple[
    Path,
    dict[Asset, OfficialDiscovery],
    dict[Asset, tuple[Provenance, ...]],
    int,
]:
    selected_starts = _market_starts(day, coverage_start, cutoff, starts)
    shared = work_root / f"shared-{day.isoformat()}"
    state_path = shared / "state.json"
    state: dict[str, Any] = (
        json.loads(state_path.read_bytes())
        if state_path.exists()
        else {"completed_urls": {}, "source_bytes": 0, "source_gaps": {}}
    )
    discoveries: dict[Asset, OfficialDiscovery] = {}
    loaders: dict[Asset, ProductionSourceLoader] = {}
    for asset in assets:
        loader = ProductionSourceLoader(
            GammaClient(fetch=_fetch_gamma),
            time.time_ns(),
            work_root / f"{asset.value}-{day}" / "official",
        )
        loaders[asset] = loader
        discoveries[asset] = loader.discover(
            asset, selected_starts, allow_missing=True, allow_unresolved=True
        )
    _validate_expected_market_identities(discoveries, expected_market_identities)
    spool_path = shared / "events.sqlite"
    if not any(discovery.markets for discovery in discoveries.values()):
        with EventSpool(spool_path):
            pass
        return spool_path, discoveries, {asset: () for asset in assets}, 0

    markets_by_asset = {asset: discoveries[asset].markets for asset in assets}
    combined_markets = tuple(market for asset in assets for market in markets_by_asset[asset])
    first_start_ns = min(market.market_start_ns for market in combined_markets)
    last_end_ns = max(market.market_end_ns for market in combined_markets)
    inventory_start_ns = max(first_start_ns - 3_600_000_000_000, 0)
    source_objects = pmxt_hourly_objects(inventory_start_ns, last_end_ns)
    if expected_source_identities is not None and {source.url for source in source_objects} != set(
        expected_source_identities
    ):
        raise SourceError("qualified PMXT objects do not match the execution source set")
    with EventSpool(spool_path, create_index=False) as spool:
        spool.discard_uncommitted_sources(set(state["completed_urls"]))
        day_market_counts, day_asset_counts = _restore_shared_pmxt_counts(
            spool, markets_by_asset
        )
        spool.drop_index()
        minimum_free_bytes = shutil.disk_usage(shared).free
        for source in source_objects:
            if source.url in state["completed_urls"]:
                continue
            if source.url in PMXT_HTTP_404_GAPS:
                state["source_gaps"][source.url] = PMXT_HTTP_404_GAPS[source.url]
                _atomic_json(state_path, state)
                continue
            qualified_identity = (
                expected_source_identities.get(source.url)
                if expected_source_identities is not None
                else None
            )
            if expected_source_identities is not None and qualified_identity is None:
                raise SourceIdentityError("qualified PMXT source identity is missing")
            acquired = _acquire_with_retry(
                source,
                shared / "raw",
                expected_identity=qualified_identity,
            )
            _validate_expected_source_identity(
                source,
                acquired.byte_length,
                acquired.etag,
                expected_source_identities,
            )
            object_provenance: dict[str, dict[str, Any]] = {}
            for asset in assets:
                relevant_markets = _markets_relevant_to_source(markets_by_asset[asset], source)
                if not relevant_markets:
                    continue
                try:
                    loaded = loaders[asset].load_downloaded_pmxt(
                        acquired.path,
                        source.url,
                        relevant_markets,
                        acquired.etag,
                        max_filtered_rows=PMXT_FILTERED_ROWS_PER_ASSET_OBJECT,
                        verified_identity=(acquired.byte_length, acquired.sha256),
                    )
                except ResourceLimitError as exc:
                    raise ResourceLimitError(
                        f"{asset.value} capacity failure while filtering {source.url}: {exc}"
                    ) from exc
                enforce_shared_pmxt_asset_caps(
                    loaded.events,
                    {asset: relevant_markets},
                    day_market_counts,
                    day_asset_counts,
                )
                spool.append(loaded.events)
                object_provenance[asset.value] = asdict(loaded.provenance[0])
            if not object_provenance:
                raise SourceError("PMXT source object has no relevant authoritative market")
            shared_provenance = next(iter(object_provenance.values()))
            state["completed_urls"][source.url] = {
                asset.value: object_provenance.get(asset.value, shared_provenance)
                for asset in assets
            }
            state["source_bytes"] = int(state["source_bytes"]) + acquired.byte_length
            _atomic_json(state_path, state)
            acquired.path.unlink(missing_ok=True)
            minimum_free_bytes = min(
                minimum_free_bytes,
                _verify_shared_disk_margin(
                    shared,
                    spool,
                    len(state["completed_urls"]),
                    int(state["source_bytes"]),
                ),
            )
        spool.ensure_index()
        usage = shutil.disk_usage(shared)
        print(
            json.dumps(
                {
                    "storage_metrics": {
                        "completed_sources": len(state["completed_urls"]),
                        "disk_free_bytes": usage.free,
                        "disk_total_bytes": usage.total,
                        "minimum_free_bytes": minimum_free_bytes,
                        "required_free_bytes": MINIMUM_FREE_DISK_BYTES,
                        "source_bytes": int(state["source_bytes"]),
                        "spool_bytes": spool.storage_bytes(),
                        "spool_events": spool.count(),
                        "spool_schema": 2,
                    }
                },
                sort_keys=True,
            ),
            flush=True,
        )

    gap_provenance = tuple(
        Provenance(
            source_id="pmxt_v2",
            source_url=url,
            retrieved_at_ns=time.time_ns(),
            byte_length=0,
            sha256=hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            license_id="CC-BY-4.0",
            source_precision="http_status",
            transformations=("authoritative_http_404_absence",),
        )
        for url, evidence in sorted(state["source_gaps"].items())
    )
    provenance = {
        asset: (
            *tuple(
                _provenance_from_json(item[asset.value])
                for _, item in sorted(state["completed_urls"].items())
            ),
            *gap_provenance,
        )
        for asset in assets
    }
    return spool_path, discoveries, provenance, int(state["source_bytes"])


def prepare_staged_day(
    day: date,
    work_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...] = tuple(Asset),
    starts: tuple[int, ...] | None = None,
    expected_market_identities: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None = None,
    expected_source_identities: dict[str, tuple[int, str]] | None = None,
    retrieved_at_ns: int | None = None,
) -> tuple[
    dict[Asset, OfficialDiscovery],
    dict[Asset, tuple[Provenance, ...]],
    dict[Asset, tuple[StagedMarket, ...]],
    dict[Asset, Path],
    int,
    dict[str, int],
]:
    """Acquire each source once and evict each market as soon as its causal window closes."""
    selected_starts = _market_starts(day, coverage_start, cutoff, starts)
    shared = work_root / f"shared-{day.isoformat()}"
    shared.mkdir(parents=True, exist_ok=True)
    discoveries: dict[Asset, OfficialDiscovery] = {}
    loaders: dict[Asset, ProductionSourceLoader] = {}
    acquisition_ns = time.time_ns() if retrieved_at_ns is None else retrieved_at_ns
    if acquisition_ns <= 0:
        raise SourceError("retrieval identity must be a positive nanosecond timestamp")
    for asset in assets:
        loader = ProductionSourceLoader(
            GammaClient(fetch=_fetch_gamma),
            acquisition_ns,
            work_root / f"{asset.value}-{day}" / "official",
        )
        loaders[asset] = loader
        discoveries[asset] = loader.discover(
            asset, selected_starts, allow_missing=True, allow_unresolved=True
        )
    _validate_expected_market_identities(discoveries, expected_market_identities)
    markets_by_asset = {asset: discoveries[asset].markets for asset in assets}
    combined_markets = tuple(market for asset in assets for market in markets_by_asset[asset])
    stage_roots = {
        asset: work_root / f"{asset.value}-{day}" / "staged-markets" for asset in assets
    }
    if not combined_markets:
        return (
            discoveries,
            {asset: () for asset in assets},
            {asset: () for asset in assets},
            stage_roots,
            0,
            {
                "peak_pending_bytes": 0,
                "peak_staged_bytes": 0,
                "peak_active_conditions": 0,
                "pmxt_scan_passes": 0,
            },
        )
    if len({market.condition_id for market in combined_markets}) != len(combined_markets):
        raise SourceError("official discoveries contain duplicate cross-asset conditions")

    owner = {
        market.condition_id: asset
        for asset, markets in markets_by_asset.items()
        for market in markets
    }
    market_by_condition = {market.condition_id: market for market in combined_markets}
    first_start_ns = min(market.market_start_ns for market in combined_markets)
    last_end_ns = max(market.market_end_ns for market in combined_markets)
    source_objects = pmxt_hourly_objects(max(first_start_ns - 3_600_000_000_000, 0), last_end_ns)
    if expected_source_identities is not None and {source.url for source in source_objects} != set(
        expected_source_identities
    ):
        raise SourceError("qualified PMXT objects do not match the execution source set")

    pending = PendingMarketFragments(shared / "pending")
    staged: dict[Asset, list[StagedMarket]] = {asset: [] for asset in assets}
    staged_conditions: set[str] = set()
    provenance_lists: dict[Asset, list[Provenance]] = {asset: [] for asset in assets}
    day_market_counts: dict[str, int] = {}
    day_asset_counts = {asset: 0 for asset in assets}
    source_gaps: dict[str, dict[str, Any]] = {}
    source_bytes = 0
    minimum_free_bytes = shutil.disk_usage(shared).free
    peak_pending_bytes = 0
    peak_current_bytes = 0
    peak_staged_bytes = 0
    peak_active_conditions = 0
    peak_rss_kib = _peak_rss_kib()
    scan_passes = 0
    completed_sources = 0
    minimum_available_memory_bytes = int(
        json.loads(Path("config/pipeline.json").read_bytes())["resource_limits"][
            "minimum_available_memory_bytes"
        ]
    )

    for source_index, source in enumerate(source_objects):
        source_began = time.perf_counter()
        relevant_by_asset = {
            asset: _markets_relevant_to_source(markets_by_asset[asset], source)
            for asset in assets
        }
        relevant_markets = tuple(
            market for asset in assets for market in relevant_by_asset[asset]
        )
        if not relevant_markets:
            raise SourceError("PMXT source object has no relevant authoritative market")
        current = PendingMarketFragments(shared / "current")
        source_free_samples: list[int] = []
        if source.url in PMXT_HTTP_404_GAPS:
            source_gaps[source.url] = PMXT_HTTP_404_GAPS[source.url]
        else:
            qualified_identity = (
                expected_source_identities.get(source.url)
                if expected_source_identities is not None
                else None
            )
            if expected_source_identities is not None and qualified_identity is None:
                raise SourceIdentityError("qualified PMXT source identity is missing")
            acquired = _acquire_with_retry(
                source,
                shared / "raw",
                expected_identity=qualified_identity,
            )
            _validate_expected_source_identity(
                source,
                acquired.byte_length,
                acquired.etag,
                expected_source_identities,
            )
            loaded = loaders[assets[0]].load_downloaded_pmxt(
                acquired.path,
                source.url,
                relevant_markets,
                acquired.etag,
                max_filtered_rows=PMXT_FILTERED_ROWS_PER_ASSET_OBJECT * len(assets),
                max_filtered_rows_per_source_row_partition=(
                    PMXT_FILTERED_ROWS_PER_ASSET_OBJECT
                ),
                verified_identity=(acquired.byte_length, acquired.sha256),
                event_batch_consumer=_pending_fragment_consumer(
                    current, shared, source_free_samples
                ),
                source_row_partition_by_condition={
                    market.condition_id: owner[market.condition_id].value
                    for market in relevant_markets
                },
                token_ids_by_source_row_partition={
                    asset.value: {
                        token
                        for market in relevant_by_asset[asset]
                        for token in (market.token_up, market.token_down)
                    }
                    for asset in assets
                    if relevant_by_asset[asset]
                },
            )
            scan_passes += 1
            source_free_samples.append(shutil.disk_usage(shared).free)
            peak_current_bytes = max(peak_current_bytes, current.storage_bytes())
            base_provenance = loaded.provenance[0]
            for asset in assets:
                provenance_lists[asset].append(
                    replace(base_provenance, retrieved_at_ns=loaders[asset].retrieved_at_ns)
                )
            source_bytes += acquired.byte_length
            completed_sources += 1
            acquired.path.unlink(missing_ok=True)

        _, source_end_ns = _pmxt_source_window_ns(source)
        object_asset_counts = {asset: 0 for asset in assets}
        for market in sorted(relevant_markets, key=lambda item: item.condition_id):
            if (
                Pipeline._available_memory_bytes()
                < minimum_available_memory_bytes
            ):
                raise ResourceLimitError("available memory is below staged-market headroom")
            current_events = order_and_deduplicate(current.load(market.condition_id))
            asset = owner[market.condition_id]
            object_asset_counts[asset] += len(current_events)
            if object_asset_counts[asset] > PMXT_FILTERED_ROWS_PER_ASSET_OBJECT:
                raise ResourceLimitError(
                    f"PMXT filtered output for {asset.value} exceeds per-object asset cap "
                    f"({object_asset_counts[asset]} > {PMXT_FILTERED_ROWS_PER_ASSET_OBJECT})"
                )
            enforce_shared_pmxt_asset_caps(
                tuple(current_events),
                {asset: (market,)},
                day_market_counts,
                day_asset_counts,
            )
            if market.market_end_ns <= source_end_ns:
                events = order_and_deduplicate(
                    [*pending.load(market.condition_id), *current_events]
                )
                item = stage_market(market, events, stage_roots[asset])
                staged[asset].append(item)
                staged_conditions.add(market.condition_id)
                staged_free_bytes = shutil.disk_usage(shared).free
                source_free_samples.append(staged_free_bytes)
                if staged_free_bytes < MINIMUM_FREE_DISK_BYTES:
                    raise ResourceLimitError(
                        "staged canonical fragments exhausted disk safety margin"
                    )
                current.discard(market.condition_id)
                pending.discard(market.condition_id)
            else:
                pending.append(market.condition_id, source_index, current_events)
                pending_free_bytes = shutil.disk_usage(shared).free
                source_free_samples.append(pending_free_bytes)
                if pending_free_bytes < MINIMUM_FREE_DISK_BYTES:
                    raise ResourceLimitError(
                        "pending PMXT fragments exhausted disk safety margin"
                    )
                current.discard(market.condition_id)
        if current.active_conditions():
            raise SourceError("combined PMXT scan escaped the active market lifecycle")
        if current.root.exists():
            current.root.rmdir()

        finalizable_conditions = {
            market.condition_id
            for market in combined_markets
            if market.market_end_ns <= source_end_ns
        }
        if not finalizable_conditions.issubset(staged_conditions):
            raise SourceError("causally complete PMXT markets were not finalized")

        free_bytes, pending_bytes, staged_bytes = _verify_staged_disk_margin(
            shared,
            pending,
            stage_roots,
            completed_sources,
            len(staged_conditions),
            source_bytes,
        )
        minimum_free_bytes = min(minimum_free_bytes, free_bytes)
        if source_free_samples:
            minimum_free_bytes = min(minimum_free_bytes, *source_free_samples)
        peak_pending_bytes = max(peak_pending_bytes, pending_bytes)
        peak_staged_bytes = max(peak_staged_bytes, staged_bytes)
        peak_active_conditions = max(peak_active_conditions, pending.active_conditions())
        peak_rss_kib = max(peak_rss_kib, _peak_rss_kib())
        print(
            json.dumps(
                {
                    "storage_progress": {
                        "active_conditions": pending.active_conditions(),
                        "completed_markets": len(staged_conditions),
                        "completed_sources": completed_sources,
                        "disk_free_bytes": free_bytes,
                        "pending_bytes": pending_bytes,
                        "pmxt_scan_passes": scan_passes,
                        "peak_rss_kib": peak_rss_kib,
                        "source_elapsed_seconds": time.perf_counter() - source_began,
                        "staged_bytes": staged_bytes,
                    }
                },
                sort_keys=True,
            ),
            flush=True,
        )

    if staged_conditions != set(market_by_condition):
        raise SourceError("bounded PMXT lifecycle did not finalize every market")
    if pending.active_conditions():
        raise SourceError("bounded PMXT lifecycle retained completed market fragments")
    gap_provenance = tuple(
        Provenance(
            source_id="pmxt_v2",
            source_url=url,
            retrieved_at_ns=acquisition_ns,
            byte_length=0,
            sha256=hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            license_id="CC-BY-4.0",
            source_precision="http_status",
            transformations=("authoritative_http_404_absence",),
        )
        for url, evidence in sorted(source_gaps.items())
    )
    provenance = {
        asset: (*tuple(provenance_lists[asset]), *gap_provenance) for asset in assets
    }
    metrics = {
        "minimum_free_bytes": minimum_free_bytes,
        "peak_active_conditions": peak_active_conditions,
        "peak_pending_bytes": peak_pending_bytes,
        "peak_current_bytes": peak_current_bytes,
        "peak_rss_kib": peak_rss_kib,
        "peak_staged_bytes": peak_staged_bytes,
        "pmxt_scan_passes": scan_passes,
        "source_objects": len(source_objects),
    }
    print(json.dumps({"storage_metrics": metrics}, sort_keys=True), flush=True)
    return (
        discoveries,
        provenance,
        {asset: tuple(staged[asset]) for asset in assets},
        stage_roots,
        source_bytes,
        metrics,
    )


def run_partition(
    asset: Asset,
    day: date,
    work_root: Path,
    ledger_path: Path,
    cutoff: datetime,
    discovery: OfficialDiscovery,
    shared_spool: Path,
    shared_provenance: tuple[Provenance, ...],
    shared_source_bytes: int = 0,
    release_prefix: str = DATASET_RELEASE_PREFIX,
    coverage_start_ns: int | None = None,
) -> dict[str, Any]:
    partition_id = f"{asset.value}/1h/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    cpu_began = time.process_time()
    work = work_root / f"{asset.value}-{day.isoformat()}"
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    release_cutoff = min(day_start + timedelta(days=1), cutoff)
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovery.markets,
        provenance=(*discovery.provenance, *shared_provenance),
        event_spool_path=shared_spool,
        preexisting_exclusions=discovery.exclusions,
    )
    pipeline_config = json.loads(Path("config/pipeline.json").read_bytes())
    pipeline = Pipeline(
        work / "output",
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits.from_config(pipeline_config),
    )
    built = pipeline.build(
        inputs,
        int(release_cutoff.timestamp()) * 1_000_000_000,
        coverage_start_ns=coverage_start_ns,
    )
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    published = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend(REPOSITORY)),
        release_tag,
        (),
    )
    with EventSpool(shared_spool) as spool:
        pmxt_events = sum(
            spool.count_condition(market.condition_id) for market in discovery.markets
        )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovery.markets),
        "pmxt_events": pmxt_events,
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(published),
        "source_bytes": shared_source_bytes,
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": _peak_rss_kib(),
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def run_staged_partition(
    asset: Asset,
    day: date,
    work_root: Path,
    ledger_path: Path,
    cutoff: datetime,
    discovery: OfficialDiscovery,
    staged_markets: tuple[StagedMarket, ...],
    stage_root: Path,
    shared_provenance: tuple[Provenance, ...],
    shared_source_bytes: int = 0,
    release_prefix: str = DATASET_RELEASE_PREFIX,
    coverage_start_ns: int | None = None,
) -> dict[str, Any]:
    partition_id = f"{asset.value}/1h/{day.isoformat()}"
    ledger = json.loads(ledger_path.read_bytes()) if ledger_path.exists() else {"partitions": {}}
    if partition_id in ledger["partitions"]:
        return cast(dict[str, Any], ledger["partitions"][partition_id])
    began = time.perf_counter()
    cpu_began = time.process_time()
    work = work_root / f"{asset.value}-{day.isoformat()}"
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    release_cutoff = min(day_start + timedelta(days=1), cutoff)
    if {item.condition_id for item in staged_markets} != {
        market.condition_id for market in discovery.markets
    }:
        raise SourceError("staged partition inventory does not match official discovery")
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovery.markets,
        provenance=(*discovery.provenance, *shared_provenance),
        preexisting_exclusions=discovery.exclusions,
        staged_markets=staged_markets,
    )
    pipeline_config = json.loads(Path("config/pipeline.json").read_bytes())
    pipeline = Pipeline(
        work / "output",
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits.from_config(pipeline_config),
    )
    built = pipeline.build(
        inputs,
        int(release_cutoff.timestamp()) * 1_000_000_000,
        coverage_start_ns=coverage_start_ns,
    )
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    published = pipeline.publish(
        built,
        Publisher(GitHubReleaseBackend(REPOSITORY)),
        release_tag,
        (stage_root,),
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovery.markets),
        "pmxt_events": sum(item.input_event_count for item in staged_markets),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "release_tag": release_tag,
        "remote_assets": len(published),
        "source_bytes": shared_source_bytes,
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": _peak_rss_kib(),
    }
    ledger["partitions"][partition_id] = result
    _atomic_json(ledger_path, ledger)
    shutil.rmtree(work)
    return result


def compute_staged_partition(
    asset: Asset,
    day: date,
    work_root: Path,
    output_root: Path,
    cutoff: datetime,
    discovery: OfficialDiscovery,
    staged_markets: tuple[StagedMarket, ...],
    stage_root: Path,
    shared_provenance: tuple[Provenance, ...],
    coverage_start_ns: int,
) -> dict[str, Any]:
    """Build and verify one canonical partition without mutating remote authority."""
    partition_id = f"{asset.value}/1h/{day.isoformat()}"
    began = time.perf_counter()
    cpu_began = time.process_time()
    work = work_root / f"{asset.value}-{day.isoformat()}"
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    release_cutoff = min(day_start + timedelta(days=1), cutoff)
    if {item.condition_id for item in staged_markets} != {
        market.condition_id for market in discovery.markets
    }:
        raise SourceError("staged partition inventory does not match official discovery")
    inputs = PartitionInputs(
        asset,
        day.isoformat(),
        discovery.markets,
        provenance=(*discovery.provenance, *shared_provenance),
        preexisting_exclusions=discovery.exclusions,
        staged_markets=staged_markets,
    )
    pipeline_config = json.loads(Path("config/pipeline.json").read_bytes())
    pipeline = Pipeline(
        output_root,
        StateStore(work / "state"),
        _tool_commit(),
        PipelineLimits.from_config(pipeline_config),
    )
    built = pipeline.build(
        inputs,
        int(release_cutoff.timestamp()) * 1_000_000_000,
        coverage_start_ns=coverage_start_ns,
    )
    result = {
        "partition_id": partition_id,
        "quality": built.tier.value,
        "markets": len(discovery.markets),
        "pmxt_events": sum(item.input_event_count for item in staged_markets),
        "canonical_bytes": sum(path.stat().st_size for path in built.directory.iterdir()),
        "manifest_sha256": built.manifest_digest,
        "wall_seconds": time.perf_counter() - began,
        "cpu_seconds": time.process_time() - cpu_began,
        "peak_rss_kib": _peak_rss_kib(),
    }
    shutil.rmtree(work, ignore_errors=True)
    shutil.rmtree(stage_root, ignore_errors=True)
    return result


SEGMENT_RECEIPT_SCHEMA = "1.0.0"


def day_segment_starts(
    day: date,
    coverage_start: datetime,
    cutoff: datetime,
    segment_index: int,
    segment_count: int,
) -> tuple[int, ...]:
    """Return one exact contiguous slice of the day's canonical market starts."""
    if segment_count < 1 or segment_index < 0 or segment_index >= segment_count:
        raise SourceError("day segment index is outside its bounded segment count")
    starts = expected_1h_market_starts(day, coverage_start, cutoff)
    lower = len(starts) * segment_index // segment_count
    upper = len(starts) * (segment_index + 1) // segment_count
    return tuple(starts[lower:upper])


def _market_from_json(value: dict[str, Any]) -> Market:
    return Market(
        asset=Asset(str(value["asset"])),
        event_id=str(value["event_id"]),
        market_id=str(value["market_id"]),
        condition_id=str(value["condition_id"]),
        token_up=str(value["token_up"]),
        token_down=str(value["token_down"]),
        market_start_ns=int(value["market_start_ns"]),
        market_end_ns=int(value["market_end_ns"]),
        rules_text_sha256=str(value["rules_text_sha256"]),
        resolution_source_url=str(value["resolution_source_url"]),
        official_outcome=Outcome(str(value["official_outcome"])),
        official_resolution_ts_ns=(
            None
            if value.get("official_resolution_ts_ns") is None
            else int(value["official_resolution_ts_ns"])
        ),
        quality_tier=QualityTier(str(value["quality_tier"])),
        evidence_sha256=str(value["evidence_sha256"]),
        exclusion_reason=(
            None
            if value.get("exclusion_reason") is None
            else ExclusionReason(str(value["exclusion_reason"]))
        ),
        venue=str(value["venue"]),
        timeframe=str(value["timeframe"]),
        schema_version=str(value["schema_version"]),
    )


def _provenance_from_segment_json(value: dict[str, Any]) -> Provenance:
    return Provenance(
        source_id=str(value["source_id"]),
        source_url=str(value["source_url"]),
        retrieved_at_ns=int(value["retrieved_at_ns"]),
        byte_length=int(value["byte_length"]),
        sha256=str(value["sha256"]),
        license_id=str(value["license_id"]),
        source_precision=str(value["source_precision"]),
        etag=None if value.get("etag") is None else str(value["etag"]),
        upstream_checksum=(
            None
            if value.get("upstream_checksum") is None
            else str(value["upstream_checksum"])
        ),
        transformations=tuple(str(item) for item in value["transformations"]),
    )


def _exclusion_from_json(value: dict[str, Any]) -> Exclusion:
    evidence = value.get("evidence")
    if not isinstance(evidence, dict):
        raise SourceError("day segment exclusion evidence is malformed")
    return Exclusion(
        market_id=str(value["market_id"]),
        reason_code=ExclusionReason(str(value["reason_code"])),
        detail=str(value["detail"]),
        evidence=evidence,
    )


def _segment_file_inventory(root: Path) -> list[dict[str, Any]]:
    result = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "receipt.json":
            continue
        byte_length, digest = hash_file(path)
        result.append(
            {
                "path": path.relative_to(root).as_posix(),
                "byte_length": byte_length,
                "sha256": digest,
            }
        )
    return result


def _copy_segment_staged_market(
    item: StagedMarket, asset: Asset, output_root: Path
) -> dict[str, Any]:
    event_relative = None
    sample_relative = None
    if item.event_path is not None:
        event_relative = f"fragments/{asset.value}/{item.event_path.name}"
        destination = output_root / event_relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(item.event_path, destination)
    if item.sample_path is not None:
        sample_relative = f"fragments/{asset.value}/{item.sample_path.name}"
        destination = output_root / sample_relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(item.sample_path, destination)
    return {
        "condition_id": item.condition_id,
        "accepted_market": None if item.accepted_market is None else asdict(item.accepted_market),
        "tier": None if item.tier is None else item.tier.value,
        "exclusion": None if item.exclusion is None else asdict(item.exclusion),
        "event_path": event_relative,
        "sample_path": sample_relative,
        "event_count": item.event_count,
        "input_event_count": item.input_event_count,
        "sample_count": item.sample_count,
        "sample_min_ns": item.sample_min_ns,
        "sample_max_ns": item.sample_max_ns,
    }


def compute_day_segment(
    day: date,
    work_root: Path,
    output_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...],
    segment_index: int,
    segment_count: int,
    retrieved_at_ns: int,
) -> dict[str, Any]:
    """Compute one durable, noncanonical causal fragment checkpoint."""
    if output_root.exists() and any(output_root.iterdir()):
        raise SourceError("segment output root is not empty")
    output_root.mkdir(parents=True, exist_ok=True)
    starts = day_segment_starts(
        day, coverage_start, cutoff, segment_index, segment_count
    )
    discoveries, provenance, staged, _, source_bytes, metrics = prepare_staged_day(
        day,
        work_root,
        coverage_start,
        cutoff,
        assets,
        starts,
        retrieved_at_ns=retrieved_at_ns,
    )
    asset_receipts: dict[str, Any] = {}
    for asset in assets:
        asset_receipts[asset.value] = {
            "markets": [asdict(item) for item in discoveries[asset].markets],
            "official_provenance": [asdict(item) for item in discoveries[asset].provenance],
            "preexisting_exclusions": [asdict(item) for item in discoveries[asset].exclusions],
            "shared_provenance": [asdict(item) for item in provenance[asset]],
            "staged_markets": [
                _copy_segment_staged_market(item, asset, output_root)
                for item in staged[asset]
            ],
        }
    receipt = {
        "schema_version": SEGMENT_RECEIPT_SCHEMA,
        "dataset_id": "polymarket-1h-seven-v1",
        "authority": "noncanonical authenticated causal fragment checkpoint",
        "day": day.isoformat(),
        "segment_index": segment_index,
        "segment_count": segment_count,
        "market_starts": list(starts),
        "assets": [asset.value for asset in assets],
        "retrieved_at_ns": retrieved_at_ns,
        "tool_commit": _tool_commit(),
        "source_bytes": source_bytes,
        "metrics": metrics,
        "asset_receipts": asset_receipts,
        "files": _segment_file_inventory(output_root),
    }
    _atomic_json(output_root / "receipt.json", receipt)
    return receipt


def _load_day_segment(root: Path) -> dict[str, Any]:
    receipt_path = root / "receipt.json"
    payload = receipt_path.read_bytes()
    receipt = json.loads(payload)
    if canonical_json_bytes(receipt) != payload:
        raise SourceError("day segment receipt is not canonical JSON")
    if not isinstance(receipt, dict) or set(receipt) != {
        "schema_version",
        "dataset_id",
        "authority",
        "day",
        "segment_index",
        "segment_count",
        "market_starts",
        "assets",
        "retrieved_at_ns",
        "tool_commit",
        "source_bytes",
        "metrics",
        "asset_receipts",
        "files",
    }:
        raise SourceError("day segment receipt has the wrong shape")
    files = receipt.get("files")
    if not isinstance(files, list) or any(
        not isinstance(item, dict) or set(item) != {"path", "byte_length", "sha256"}
        for item in files
    ):
        raise SourceError("day segment file receipt is malformed")
    actual_files = _segment_file_inventory(root)
    if actual_files != files:
        raise SourceError("day segment file receipt digest mismatch")
    return cast(dict[str, Any], receipt)


def _unique_records(values: list[T], key: Callable[[T], str], label: str) -> tuple[T, ...]:
    result: dict[str, T] = {}
    for value in values:
        identity = key(value)
        previous = result.get(identity)
        if previous is not None and previous != value:
            raise SourceError(f"day segments contain divergent {label}")
        result[identity] = value
    return tuple(result.values())


def assemble_day_segments(
    day: date,
    work_root: Path,
    output_root: Path,
    segments_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...],
    segment_count: int,
    retrieved_at_ns: int,
) -> list[dict[str, Any]]:
    """Authenticate and assemble all causal checkpoints into canonical day bytes."""
    roots = sorted(path.parent for path in segments_root.glob("*/receipt.json"))
    if len(roots) != segment_count:
        raise SourceError("day segment checkpoint set is incomplete")
    receipts = [_load_day_segment(root) for root in roots]
    by_index = {
        int(receipt.get("segment_index", -1)): (root, receipt)
        for root, receipt in zip(roots, receipts, strict=True)
    }
    if set(by_index) != set(range(segment_count)) or len(by_index) != len(receipts):
        raise SourceError("day segment indexes are incomplete or duplicated")
    expected_assets = [asset.value for asset in assets]
    expected_starts = expected_1h_market_starts(day, coverage_start, cutoff)
    observed_starts: list[int] = []
    for index in range(segment_count):
        _, receipt = by_index[index]
        if (
            receipt.get("schema_version") != SEGMENT_RECEIPT_SCHEMA
            or receipt.get("dataset_id") != "polymarket-1h-seven-v1"
            or receipt.get("authority")
            != "noncanonical authenticated causal fragment checkpoint"
            or receipt.get("day") != day.isoformat()
            or receipt.get("segment_count") != segment_count
            or receipt.get("assets") != expected_assets
            or receipt.get("retrieved_at_ns") != retrieved_at_ns
            or receipt.get("tool_commit") != _tool_commit()
            or receipt.get("market_starts")
            != list(day_segment_starts(day, coverage_start, cutoff, index, segment_count))
        ):
            raise SourceError("day segment authority mismatch")
        observed_starts.extend(int(item) for item in receipt["market_starts"])
    if observed_starts != expected_starts:
        raise SourceError("day segments do not exactly cover the canonical UTC day")

    discoveries: dict[Asset, OfficialDiscovery] = {}
    staged_by_asset: dict[Asset, tuple[StagedMarket, ...]] = {}
    shared_by_asset: dict[Asset, tuple[Provenance, ...]] = {}
    stage_roots: dict[Asset, Path] = {}
    for asset in assets:
        markets: list[Market] = []
        official: list[Provenance] = []
        exclusions: list[Exclusion] = []
        shared: list[Provenance] = []
        staged: list[StagedMarket] = []
        for index in range(segment_count):
            root, receipt = by_index[index]
            raw_asset = receipt.get("asset_receipts", {}).get(asset.value)
            if not isinstance(raw_asset, dict) or set(raw_asset) != {
                "markets",
                "official_provenance",
                "preexisting_exclusions",
                "shared_provenance",
                "staged_markets",
            }:
                raise SourceError("day segment omits an assigned asset")
            recorded_paths = {str(item["path"]) for item in receipt["files"]}
            markets.extend(_market_from_json(item) for item in raw_asset["markets"])
            official.extend(
                _provenance_from_segment_json(item)
                for item in raw_asset["official_provenance"]
            )
            exclusions.extend(
                _exclusion_from_json(item)
                for item in raw_asset["preexisting_exclusions"]
            )
            shared.extend(
                _provenance_from_segment_json(item)
                for item in raw_asset["shared_provenance"]
            )
            for item in raw_asset["staged_markets"]:
                if not isinstance(item, dict) or set(item) != {
                    "condition_id",
                    "accepted_market",
                    "tier",
                    "exclusion",
                    "event_path",
                    "sample_path",
                    "event_count",
                    "input_event_count",
                    "sample_count",
                    "sample_min_ns",
                    "sample_max_ns",
                }:
                    raise SourceError("staged market checkpoint has the wrong shape")
                event_relative = (
                    None if item["event_path"] is None else str(item["event_path"])
                )
                sample_relative = (
                    None if item["sample_path"] is None else str(item["sample_path"])
                )
                expected_prefix = f"fragments/{asset.value}/"
                if any(
                    path is not None
                    and (path not in recorded_paths or not path.startswith(expected_prefix))
                    for path in (event_relative, sample_relative)
                ):
                    raise SourceError("staged market path escapes its authenticated asset files")
                event_path = None if event_relative is None else root / event_relative
                sample_path = None if sample_relative is None else root / sample_relative
                staged.append(
                    StagedMarket(
                        condition_id=str(item["condition_id"]),
                        accepted_market=(
                            None
                            if item["accepted_market"] is None
                            else _market_from_json(item["accepted_market"])
                        ),
                        tier=(
                            None if item["tier"] is None else QualityTier(str(item["tier"]))
                        ),
                        exclusion=(
                            None
                            if item["exclusion"] is None
                            else _exclusion_from_json(item["exclusion"])
                        ),
                        event_path=event_path,
                        sample_path=sample_path,
                        event_count=int(item["event_count"]),
                        input_event_count=int(item["input_event_count"]),
                        sample_count=int(item["sample_count"]),
                        sample_min_ns=(
                            None if item["sample_min_ns"] is None else int(item["sample_min_ns"])
                        ),
                        sample_max_ns=(
                            None if item["sample_max_ns"] is None else int(item["sample_max_ns"])
                        ),
                    )
                )
        unique_markets = _unique_records(markets, lambda item: item.condition_id, "markets")
        unique_official = _unique_records(
            official, lambda item: f"{item.source_id}\0{item.source_url}", "official provenance"
        )
        unique_exclusions = _unique_records(
            exclusions, lambda item: item.market_id, "official exclusions"
        )
        unique_shared = _unique_records(
            shared, lambda item: f"{item.source_id}\0{item.source_url}", "PMXT provenance"
        )
        unique_staged = _unique_records(
            staged, lambda item: item.condition_id, "staged market fragments"
        )
        allowed_starts = set(expected_starts)
        if any(
            item.asset is not asset
            or item.market_start_ns // 1_000_000_000 not in allowed_starts
            for item in unique_markets
        ):
            raise SourceError("assembled market is outside its assigned asset/day identity")
        if any(
            item.retrieved_at_ns != retrieved_at_ns
            for item in (*unique_official, *unique_shared)
        ):
            raise SourceError("assembled provenance does not share its acquisition identity")
        if (
            len(unique_official) != len(expected_starts)
            or len(unique_markets) + len(unique_exclusions) != len(expected_starts)
        ):
            raise SourceError("assembled official dispositions do not cover every market start")
        if {item.condition_id for item in unique_markets} != {
            item.condition_id for item in unique_staged
        }:
            raise SourceError("assembled staged inventory does not match official markets")
        discoveries[asset] = OfficialDiscovery(
            tuple(sorted(unique_markets, key=lambda item: item.market_start_ns)),
            unique_official,
            unique_exclusions,
        )
        staged_by_asset[asset] = unique_staged
        shared_by_asset[asset] = unique_shared
        # The authenticated checkpoint roots are shared by every asset and must
        # remain readable until final assembly completes.  The partition helper
        # only uses this path for best-effort cleanup.
        stage_roots[asset] = work_root / "preserved-segment-inputs" / asset.value

    if output_root.exists() and any(output_root.iterdir()):
        raise SourceError("compute output root is not empty")
    output_root.mkdir(parents=True, exist_ok=True)
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    coverage_start_ns = int(max(day_start, coverage_start).timestamp()) * 1_000_000_000
    results = []
    for asset in assets:
        results.append(
            compute_staged_partition(
                asset,
                day,
                work_root,
                output_root,
                cutoff,
                discoveries[asset],
                staged_by_asset[asset],
                stage_roots[asset],
                shared_by_asset[asset],
                coverage_start_ns,
            )
        )
    return results


def compute_day(
    day: date,
    work_root: Path,
    output_root: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...],
) -> list[dict[str, Any]]:
    """Compute isolated canonical bytes while leaving publication to a short writer job."""
    if output_root.exists() and any(output_root.iterdir()):
        raise SourceError("compute output root is not empty")
    output_root.mkdir(parents=True, exist_ok=True)
    discoveries, provenance, staged, stage_roots, _, _ = prepare_staged_day(
        day,
        work_root,
        coverage_start,
        cutoff,
        assets,
    )
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    coverage_start_ns = int(max(day_start, coverage_start).timestamp()) * 1_000_000_000
    results = []
    for asset in assets:
        results.append(
            compute_staged_partition(
                asset,
                day,
                work_root,
                output_root,
                cutoff,
                discoveries[asset],
                staged[asset],
                stage_roots[asset],
                provenance[asset],
                coverage_start_ns,
            )
        )
    shutil.rmtree(work_root / f"shared-{day.isoformat()}", ignore_errors=True)
    return results


def run_day(
    day: date,
    work_root: Path,
    ledger: Path,
    coverage_start: datetime,
    cutoff: datetime,
    assets: tuple[Asset, ...],
    starts: tuple[int, ...] | None = None,
    release_prefix: str = DATASET_RELEASE_PREFIX,
    expected_market_identities: dict[Asset, frozenset[tuple[str, frozenset[str]]]] | None = None,
    expected_source_identities: dict[str, tuple[int, str]] | None = None,
) -> list[dict[str, Any]]:
    discoveries, provenance, staged, stage_roots, source_bytes, _ = prepare_staged_day(
        day,
        work_root,
        coverage_start,
        cutoff,
        assets,
        starts,
        expected_market_identities,
        expected_source_identities,
    )
    day_start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    partition_coverage_start = (
        datetime.fromtimestamp(min(starts), UTC) if starts else max(day_start, coverage_start)
    )
    coverage_start_ns = int(partition_coverage_start.timestamp()) * 1_000_000_000
    results = []
    for index, asset in enumerate(assets):
        results.append(
            run_staged_partition(
                asset,
                day,
                work_root,
                ledger,
                cutoff,
                discoveries[asset],
                staged[asset],
                stage_roots[asset],
                provenance[asset],
                source_bytes if index == 0 else 0,
                release_prefix,
                coverage_start_ns,
            )
        )
    shutil.rmtree(work_root / f"shared-{day.isoformat()}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work-root", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--coverage-start", type=datetime.fromisoformat, required=True)
    parser.add_argument("--cutoff", type=datetime.fromisoformat, required=True)
    parser.add_argument("--assets", default=",".join(asset.value for asset in Asset))
    parser.add_argument("--market-starts", default="")
    parser.add_argument("--release-prefix", default=DATASET_RELEASE_PREFIX)
    parser.add_argument("--expected-market-identities", type=Path)
    parser.add_argument("--expected-source-identities", type=Path)
    parser.add_argument("--compute-output-root", type=Path)
    parser.add_argument("--segment-output-root", type=Path)
    parser.add_argument("--segment-input-root", type=Path)
    parser.add_argument("--segment-index", type=int)
    parser.add_argument("--segment-count", type=int)
    parser.add_argument("--retrieved-at-ns", type=int)
    args = parser.parse_args()
    assets = tuple(Asset(value) for value in args.assets.split(","))
    if not assets or len(set(assets)) != len(assets):
        parser.error("--assets must contain a non-empty unique asset subset")
    if args.coverage_start.tzinfo is None or args.cutoff.tzinfo is None:
        parser.error("--coverage-start and --cutoff must be timezone-aware")
    starts = tuple(int(value) for value in args.market_starts.split(",") if value)
    expected_market_identities = None
    if args.expected_market_identities is not None:
        raw_expected = json.loads(args.expected_market_identities.read_bytes())
        expected_market_identities = {
            Asset(asset): frozenset(
                (
                    str(value["condition_id"]),
                    frozenset(str(token) for token in value["token_ids"]),
                )
                for value in values
            )
            for asset, values in raw_expected.items()
        }
    expected_source_identities = None
    if args.expected_source_identities is not None:
        raw_sources = json.loads(args.expected_source_identities.read_bytes())
        expected_source_identities = {
            str(url): (int(value["byte_length"]), str(value["etag"]))
            for url, value in raw_sources.items()
        }
    current = args.start
    while current <= args.end:
        if args.segment_output_root is not None:
            if (
                args.start != args.end
                or starts
                or expected_market_identities
                or expected_source_identities
                or args.compute_output_root is not None
                or args.segment_input_root is not None
                or args.segment_index is None
                or args.segment_count is None
                or args.retrieved_at_ns is None
            ):
                parser.error("segment execution requires one complete UTC day and exact bounds")
            receipt = compute_day_segment(
                current,
                args.work_root,
                args.segment_output_root,
                args.coverage_start,
                args.cutoff,
                assets,
                args.segment_index,
                args.segment_count,
                args.retrieved_at_ns,
            )
            print(
                json.dumps(
                    {
                        "day": current.isoformat(),
                        "segment_index": receipt["segment_index"],
                        "market_starts": len(receipt["market_starts"]),
                        "source_objects": receipt["metrics"]["source_objects"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            results = []
        elif args.segment_input_root is not None:
            if (
                args.start != args.end
                or starts
                or expected_market_identities
                or expected_source_identities
                or args.compute_output_root is None
                or args.segment_index is not None
                or args.segment_count is None
                or args.retrieved_at_ns is None
            ):
                parser.error("segment assembly requires one complete UTC day and exact bounds")
            results = assemble_day_segments(
                current,
                args.work_root,
                args.compute_output_root,
                args.segment_input_root,
                args.coverage_start,
                args.cutoff,
                assets,
                args.segment_count,
                args.retrieved_at_ns,
            )
        elif args.compute_output_root is not None:
            if (
                args.start != args.end
                or starts
                or expected_market_identities
                or expected_source_identities
                or args.segment_index is not None
                or args.segment_count is not None
                or args.retrieved_at_ns is not None
            ):
                parser.error("compute-only execution requires one complete UTC day")
            results = compute_day(
                current,
                args.work_root,
                args.compute_output_root,
                args.coverage_start,
                args.cutoff,
                assets,
            )
        else:
            results = run_day(
                current,
                args.work_root,
                args.ledger,
                args.coverage_start,
                args.cutoff,
                assets,
                starts or None,
                args.release_prefix,
                expected_market_identities,
                expected_source_identities,
            )
        for result in results:
            print(json.dumps(result, sort_keys=True), flush=True)
        current += timedelta(days=1)


if __name__ == "__main__":
    main()
