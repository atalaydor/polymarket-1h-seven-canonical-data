"""End-to-end deterministic daily partition orchestration."""

from __future__ import annotations

import hashlib
import shutil
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import MissingInitialSnapshotError, PipelineError, ResourceLimitError
from canonical_data.manifest import build_manifest, verify_manifest
from canonical_data.models import (
    Asset,
    BookEvent,
    Exclusion,
    ExclusionReason,
    Market,
    Provenance,
    QualityTier,
    Sample200ms,
    UnderlyingObservation,
)
from canonical_data.parquetio import (
    EVENT_SCHEMA,
    EXCLUSION_SCHEMA,
    MARKET_SCHEMA,
    SAMPLE_SCHEMA,
    UNDERLYING_SCHEMA,
    StreamingTableWriter,
    event_row,
    exclusion_row,
    market_row,
    sample_row,
    underlying_row,
    verify_parquet,
    write_partition_tables,
    write_table_atomic,
)
from canonical_data.pmxt import BookReconstructor, decode_rows
from canonical_data.quality import classify
from canonical_data.release import Publisher, ReleaseAsset
from canonical_data.resample import resample_200ms
from canonical_data.spool import EventSpool
from canonical_data.state import ORDER, Checkpoint, Phase, StateStore


@dataclass(frozen=True)
class PartitionInputs:
    asset: Asset
    day: str
    markets: tuple[Market, ...]
    pmxt_rows: tuple[dict[str, Any], ...] = ()
    pmxt_source_object: str = ""
    underlying: tuple[UnderlyingObservation, ...] = ()
    provenance: tuple[Provenance, ...] = ()
    temporary_raw_paths: tuple[Path, ...] = ()
    decoded_pmxt_events: tuple[BookEvent, ...] = ()
    event_spool_path: Path | None = None
    preexisting_exclusions: tuple[Exclusion, ...] = ()
    staged_markets: tuple[StagedMarket, ...] = ()


@dataclass(frozen=True)
class StagedMarket:
    condition_id: str
    accepted_market: Market | None
    tier: QualityTier | None
    exclusion: Exclusion | None
    event_path: Path | None
    sample_path: Path | None
    event_count: int
    input_event_count: int
    sample_count: int
    sample_min_ns: int | None
    sample_max_ns: int | None


def transform_market(
    market: Market, market_events: list[BookEvent]
) -> tuple[Market | None, QualityTier | None, Exclusion | None, list[Sample200ms]]:
    has_pmxt = bool(market_events)
    gaps: list[tuple[int, int]] = []
    try:
        if has_pmxt:
            states = BookReconstructor().reconstruct(market_events)
            native_samples, gaps = resample_200ms(market, states)
        else:
            native_samples = []
            gaps = [(market.market_start_ns, market.market_end_ns)]
    except MissingInitialSnapshotError as reconstruction_error:
        return (
            None,
            None,
            Exclusion(
                market.market_id,
                ExclusionReason.NO_INITIAL_SNAPSHOT,
                str(reconstruction_error),
                {
                    "condition_id": market.condition_id,
                    "market_evidence_sha256": market.evidence_sha256,
                },
            ),
            [],
        )
    except PipelineError as reconstruction_error:
        return (
            None,
            None,
            Exclusion(
                market.market_id,
                ExclusionReason.EVENT_CONFLICT,
                f"invalid PMXT market evidence: {reconstruction_error}",
                {
                    "condition_id": market.condition_id,
                    "market_evidence_sha256": market.evidence_sha256,
                },
            ),
            [],
        )
    tier, exclusion = classify(market, has_pmxt, gaps)
    if exclusion is not None:
        return None, None, exclusion, []
    accepted = Market(**{**market.__dict__, "quality_tier": tier})
    return accepted, tier, None, native_samples


def stage_market(
    market: Market,
    market_events: list[BookEvent],
    directory: Path,
) -> StagedMarket:
    """Losslessly transform one causally complete market into compressed fragments."""
    accepted, tier, exclusion, native_samples = transform_market(market, market_events)
    if accepted is None:
        return StagedMarket(
            market.condition_id,
            None,
            None,
            exclusion,
            None,
            None,
            0,
            len(market_events),
            0,
            None,
            None,
        )
    directory.mkdir(parents=True, exist_ok=True)
    event_path = directory / f"{market.condition_id}.events.parquet"
    sample_path = directory / f"{market.condition_id}.samples.parquet"
    write_table_atomic(event_path, EVENT_SCHEMA, [event_row(item) for item in market_events])
    write_table_atomic(sample_path, SAMPLE_SCHEMA, [sample_row(item) for item in native_samples])
    return StagedMarket(
        market.condition_id,
        accepted,
        tier,
        None,
        event_path,
        sample_path,
        len(market_events),
        len(market_events),
        len(native_samples),
        native_samples[0].grid_ts_ns if native_samples else None,
        native_samples[-1].grid_ts_ns if native_samples else None,
    )


@dataclass(frozen=True)
class BuiltPartition:
    partition_id: str
    directory: Path
    manifest_digest: str
    tier: QualityTier
    checkpoint: Checkpoint


@dataclass(frozen=True)
class PipelineLimits:
    max_markets: int = 300
    max_pmxt_rows: int = 194_466_240
    max_samples: int = 1_000_000
    max_underlying_rows: int = 1_000_000
    max_partition_bytes: int = 1_000_000_000
    minimum_available_memory_bytes: int = 0

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PipelineLimits:
        limits = config["resource_limits"]
        return cls(
            max_markets=int(limits["max_markets_per_partition"]),
            max_pmxt_rows=int(limits["max_pmxt_rows_per_partition"]),
            max_samples=int(limits["max_derived_samples_per_partition"]),
            max_underlying_rows=int(limits["max_underlying_rows_per_partition"]),
            max_partition_bytes=int(limits["max_transformed_partition_bytes"]),
            minimum_available_memory_bytes=int(limits["minimum_available_memory_bytes"]),
        )


class Pipeline:
    def __init__(
        self,
        output_root: Path,
        state_store: StateStore,
        tool_commit: str,
        limits: PipelineLimits | None = None,
    ):
        if len(tool_commit) != 40 or not all(char in "0123456789abcdef" for char in tool_commit):
            raise ValueError("tool_commit must be a full Git SHA")
        self.output_root = output_root
        self.state_store = state_store
        self.tool_commit = tool_commit
        self.limits = limits or PipelineLimits()

    def build(
        self,
        inputs: PartitionInputs,
        release_cutoff_ns: int,
        coverage_start_ns: int | None = None,
    ) -> BuiltPartition:
        self._enforce_input_limits(inputs)
        partition_id = f"{inputs.asset.value}/1h/{inputs.day}"
        identity_digest = self._identity_digest(inputs)
        existing = self.state_store.load(partition_id)
        directory = (
            self.output_root
            / f"asset={inputs.asset.value}"
            / "timeframe=1h"
            / f"date={inputs.day}"
        )
        if existing is not None and existing.phase in {Phase.VERIFIED, Phase.PUBLISHED}:
            if existing.identity_digest != identity_digest:
                raise PipelineError("existing partition identity conflicts")
            actual = verify_manifest(directory)
            if actual != existing.manifest_digest:
                raise PipelineError("existing partition manifest conflicts")
            return BuiltPartition(
                partition_id, directory, actual, self._manifest_tier(directory), existing
            )
        self._advance_if_needed(partition_id, Phase.INVENTORIED, identity_digest)
        events: list[BookEvent] = []
        samples = []
        exclusions: list[Exclusion] = list(inputs.preexisting_exclusions)
        accepted_markets: list[Market] = []
        partition_tiers: list[QualityTier] = []
        modes = sum(
            bool(item)
            for item in (
                inputs.decoded_pmxt_events,
                inputs.pmxt_rows,
                inputs.event_spool_path,
                inputs.staged_markets,
            )
        )
        if modes > 1:
            raise PipelineError("provide exactly one PMXT event input mode")
        pmxt_events = list(inputs.decoded_pmxt_events)
        if inputs.pmxt_rows:
            pmxt_events = decode_rows(inputs.pmxt_rows, inputs.pmxt_source_object)
        self._advance_if_needed(partition_id, Phase.ACQUIRED, identity_digest)
        event_writer = None
        sample_writer = None
        sample_min: int | None = None
        sample_max: int | None = None
        event_count = 0
        sample_count = 0
        spool = EventSpool(inputs.event_spool_path) if inputs.event_spool_path else None
        staged = {item.condition_id: item for item in inputs.staged_markets}
        if len(staged) != len(inputs.staged_markets):
            raise PipelineError("staged market conditions must be unique")
        streaming = spool is not None or bool(staged)
        if streaming:
            event_writer = StreamingTableWriter(directory / "book-events.parquet", EVENT_SCHEMA)
            sample_writer = StreamingTableWriter(directory / "book-200ms.parquet", SAMPLE_SCHEMA)
        ordered_markets = sorted(inputs.markets, key=lambda item: item.condition_id)
        for market in ordered_markets:
            if market.asset is not inputs.asset:
                raise PipelineError("market asset does not match partition")
            if inputs.staged_markets:
                item = staged.pop(market.condition_id, None)
                if item is None:
                    self._abort_spooled_build(spool, event_writer, sample_writer)
                    raise PipelineError("staged market inventory is incomplete")
                if item.exclusion is not None:
                    exclusions.append(item.exclusion)
                    continue
                if (
                    item.accepted_market is None
                    or item.tier is None
                    or item.event_path is None
                    or item.sample_path is None
                    or event_writer is None
                    or sample_writer is None
                ):
                    self._abort_spooled_build(spool, event_writer, sample_writer)
                    raise PipelineError("accepted staged market is incomplete")
                accepted_markets.append(item.accepted_market)
                partition_tiers.append(item.tier)
                event_count += item.event_count
                sample_count += item.sample_count
                if event_count > self.limits.max_pmxt_rows:
                    self._abort_spooled_build(spool, event_writer, sample_writer)
                    raise ResourceLimitError("native events exceed partition cap")
                if sample_count > self.limits.max_samples:
                    self._abort_spooled_build(spool, event_writer, sample_writer)
                    raise ResourceLimitError("derived samples exceed partition cap")
                if item.sample_min_ns is not None:
                    sample_min = (
                        item.sample_min_ns
                        if sample_min is None
                        else min(sample_min, item.sample_min_ns)
                    )
                if item.sample_max_ns is not None:
                    sample_max = (
                        item.sample_max_ns
                        if sample_max is None
                        else max(sample_max, item.sample_max_ns)
                    )
                event_writer.append(pq.read_table(item.event_path).to_pylist())
                sample_writer.append(pq.read_table(item.sample_path).to_pylist())
                continue
            market_events = (
                spool.load(market.condition_id)
                if spool
                else [event for event in pmxt_events if event.condition_id == market.condition_id]
            )
            accepted, tier, exclusion, native_samples = transform_market(market, market_events)
            if exclusion is not None:
                exclusions.append(exclusion)
                continue
            assert accepted is not None and tier is not None
            accepted_markets.append(accepted)
            partition_tiers.append(tier)
            sample_count += len(native_samples)
            if sample_count > self.limits.max_samples:
                self._abort_spooled_build(spool, event_writer, sample_writer)
                raise ResourceLimitError("derived samples exceed partition cap")
            if native_samples:
                sample_min = (
                    native_samples[0].grid_ts_ns
                    if sample_min is None
                    else min(sample_min, native_samples[0].grid_ts_ns)
                )
                sample_max = (
                    native_samples[-1].grid_ts_ns
                    if sample_max is None
                    else max(sample_max, native_samples[-1].grid_ts_ns)
                )
            native_events = market_events
            event_count += len(native_events)
            if event_count > self.limits.max_pmxt_rows:
                self._abort_spooled_build(spool, event_writer, sample_writer)
                raise ResourceLimitError("native events exceed partition cap")
            if event_writer is not None and sample_writer is not None:
                event_writer.append([event_row(item) for item in native_events])
                sample_writer.append([sample_row(item) for item in native_samples])
            else:
                events.extend(native_events)
                samples.extend(native_samples)
        tier = self._partition_tier(partition_tiers)
        if tier is QualityTier.EXCLUDED and not exclusions:
            self._abort_spooled_build(spool, event_writer, sample_writer)
            raise PipelineError("excluded partition lacks explicit exclusion evidence")
        if any(not exclusion.evidence for exclusion in exclusions):
            self._abort_spooled_build(spool, event_writer, sample_writer)
            raise PipelineError("exclusion lacks bound evidence")
        if staged:
            self._abort_spooled_build(spool, event_writer, sample_writer)
            raise PipelineError("staged market inventory contains unexpected conditions")
        if not streaming:
            counts = write_partition_tables(
                directory, accepted_markets, events, samples, inputs.underlying, exclusions
            )
        else:
            assert event_writer is not None and sample_writer is not None
            counts = {
                "book-events.parquet": event_writer.finish(),
                "book-200ms.parquet": sample_writer.finish(),
            }
            small = (
                (
                    "markets.parquet",
                    MARKET_SCHEMA,
                    sorted(
                        (market_row(item) for item in accepted_markets),
                        key=lambda row: row["condition_id"],
                    ),
                ),
                (
                    "underlying.parquet",
                    UNDERLYING_SCHEMA,
                    sorted(
                        (underlying_row(item) for item in inputs.underlying),
                        key=lambda row: (
                            row["asset"],
                            row["source_ts_ns"],
                            row["observation_kind"],
                        ),
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
            )
            for name, schema, rows in small:
                write_table_atomic(directory / name, schema, rows)
                counts[name] = len(rows)
            if spool is not None:
                spool.close()
        partition_bytes = sum(path.stat().st_size for path in directory.glob("*.parquet"))
        if partition_bytes > self.limits.max_partition_bytes:
            raise ResourceLimitError("transformed partition exceeds byte cap")
        self._advance_if_needed(partition_id, Phase.TRANSFORMED, identity_digest)
        schemas = {
            "markets.parquet": MARKET_SCHEMA,
            "book-events.parquet": EVENT_SCHEMA,
            "book-200ms.parquet": SAMPLE_SCHEMA,
            "underlying.parquet": UNDERLYING_SCHEMA,
            "exclusions.parquet": EXCLUSION_SCHEMA,
        }
        for name, schema in schemas.items():
            verify_parquet(directory / name, schema, counts[name])
        statistics = {
            "row_counts": counts,
            "market_count": len(accepted_markets),
            "exclusion_count": len(exclusions),
            "sample_min_ts_ns": sample_min
            if streaming
            else min((item.grid_ts_ns for item in samples), default=None),
            "sample_max_ts_ns": sample_max
            if streaming
            else max((item.grid_ts_ns for item in samples), default=None),
        }
        _, digest = build_manifest(
            directory,
            inputs.asset,
            inputs.day,
            tier,
            inputs.provenance,
            exclusions,
            self.tool_commit,
            statistics,
            coverage_start_ns
            if coverage_start_ns is not None
            else int(
                datetime.fromisoformat(inputs.day).replace(tzinfo=UTC).timestamp() * 1_000_000_000
            ),
            release_cutoff_ns,
        )
        verify_manifest(directory)
        checkpoint = Checkpoint(partition_id, Phase.VERIFIED, identity_digest, digest)
        self.state_store.advance(checkpoint)
        return BuiltPartition(partition_id, directory, digest, tier, checkpoint)

    def publish(
        self,
        built: BuiltPartition,
        publisher: Publisher,
        release_tag: str,
        raw_paths: Iterable[Path],
    ) -> list[ReleaseAsset]:
        assets = publisher.publish_partition(release_tag, built.partition_id, built.directory)
        checkpoint = Checkpoint(
            built.partition_id,
            Phase.PUBLISHED,
            built.checkpoint.identity_digest,
            built.manifest_digest,
            release_tag,
        )
        self.state_store.advance(checkpoint)
        for path in raw_paths:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        return assets

    @staticmethod
    def _partition_tier(tiers: list[QualityTier]) -> QualityTier:
        if not tiers:
            return QualityTier.EXCLUDED
        if any(tier is QualityTier.TIER_B for tier in tiers):
            return QualityTier.TIER_B
        return QualityTier.TIER_A

    @staticmethod
    def _abort_spooled_build(
        spool: EventSpool | None,
        event_writer: StreamingTableWriter | None,
        sample_writer: StreamingTableWriter | None,
    ) -> None:
        if event_writer is not None:
            event_writer.abort()
        if sample_writer is not None:
            sample_writer.abort()
        if spool is not None:
            spool.close()

    @staticmethod
    def _identity_digest(inputs: PartitionInputs) -> str:
        identity = {
            "partition": f"{inputs.asset.value}/1h/{inputs.day}",
            "markets": [(market.condition_id, market.evidence_sha256) for market in inputs.markets],
            "provenance": [
                (item.source_id, item.sha256, item.byte_length) for item in inputs.provenance
            ],
        }
        return hashlib.sha256(canonical_json_bytes(identity)).hexdigest()

    @staticmethod
    def _manifest_tier(directory: Path) -> QualityTier:
        import json

        return QualityTier(json.loads((directory / "manifest.json").read_bytes())["quality_tier"])

    def _enforce_input_limits(self, inputs: PartitionInputs) -> None:
        limits = self.limits
        checks = (
            (len(inputs.markets), limits.max_markets, "markets"),
            (len(inputs.pmxt_rows), limits.max_pmxt_rows, "PMXT rows"),
            (len(inputs.decoded_pmxt_events), limits.max_pmxt_rows, "decoded PMXT events"),
            (len(inputs.staged_markets), limits.max_markets, "staged markets"),
            (len(inputs.underlying), limits.max_underlying_rows, "underlying rows"),
        )
        for actual, maximum, name in checks:
            if actual > maximum:
                raise ResourceLimitError(f"{name} exceed partition cap")
        if (
            limits.minimum_available_memory_bytes
            and self._available_memory_bytes() < limits.minimum_available_memory_bytes
        ):
            raise ResourceLimitError("available memory is below configured headroom")

    @staticmethod
    def _available_memory_bytes() -> int:
        memory_info = Path("/proc/meminfo")
        if not memory_info.exists():
            raise ResourceLimitError("cannot determine available memory")
        for line in memory_info.read_text().splitlines():
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
        raise ResourceLimitError("cannot determine available memory")

    def _advance_if_needed(self, partition_id: str, phase: Phase, identity_digest: str) -> None:
        previous = self.state_store.load(partition_id)
        if previous is not None and previous.identity_digest != identity_digest:
            raise PipelineError("existing partition identity conflicts")
        if previous is not None and ORDER[previous.phase] >= ORDER[phase]:
            return
        self.state_store.advance(Checkpoint(partition_id, phase, identity_digest))
