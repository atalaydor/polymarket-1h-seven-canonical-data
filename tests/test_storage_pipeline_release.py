from __future__ import annotations

import json
import pickle
import sqlite3
import tempfile
import unittest
import urllib.error
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pyarrow.parquet as pq
from helpers import market, pmxt_rows, provenance

from canonical_data.errors import ConflictError, PipelineError, ResourceLimitError
from canonical_data.manifest import (
    build_manifest,
    build_notice,
    build_release_index,
    hash_file,
    verify_manifest,
)
from canonical_data.models import (
    Asset,
    Exclusion,
    ExclusionReason,
    QualityTier,
)
from canonical_data.parquetio import write_partition_tables
from canonical_data.pipeline import PartitionInputs, Pipeline, PipelineLimits, stage_market
from canonical_data.pmxt import BookReconstructor, decode_rows, order_and_deduplicate
from canonical_data.release import (
    DirectoryReleaseBackend,
    GitHubAPIError,
    GitHubReleaseBackend,
    Publisher,
    ReleaseAsset,
    download_and_verify_release,
)
from canonical_data.resample import resample_200ms
from canonical_data.spool import EventSpool
from canonical_data.state import Checkpoint, Phase, StateStore

COMMIT = "d" * 40


class ParquetManifestTests(unittest.TestCase):
    def test_release_index_and_notice_are_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            counts = write_partition_tables(root / "p", [market()], [], [], [], [])
            build_manifest(
                root / "p",
                Asset.DOGE,
                "2026-04-13",
                QualityTier.TIER_A,
                [provenance()],
                [],
                COMMIT,
                {"row_counts": counts},
                market().market_start_ns,
                market().market_end_ns,
            )
            index_digest = build_release_index(
                root / "index.json",
                "v1",
                market().market_end_ns,
                [root / "p" / "manifest.json"],
            )
            config = {
                "sources": [
                    {
                        "id": "pmxt_v2",
                        "class": "PRIMARY",
                        "url": "https://example.test",
                        "license": "CC-BY-4.0",
                        "role": "event_book",
                    },
                    {
                        "id": "excluded",
                        "class": "EXCLUDED",
                        "url": "",
                        "license": "none",
                        "role": "none",
                    },
                ]
            }
            notice_digest = build_notice(root / "NOTICE.json", config)
            self.assertEqual(len(index_digest), 64)
            self.assertEqual(len(notice_digest), 64)
            self.assertNotIn(str(root).encode(), (root / "index.json").read_bytes())
            notice = json.loads((root / "NOTICE.json").read_bytes())
            self.assertIsNone(notice["combined_dataset_license"])
            self.assertEqual([item["source_id"] for item in notice["sources"]], ["pmxt_v2"])

    def test_deterministic_parquet_and_manifest(self) -> None:
        events = decode_rows(pmxt_rows(), "fixture")
        samples, _ = resample_200ms(market(), BookReconstructor().reconstruct(events))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            digests = []
            for name in ("one", "two"):
                directory = root / name
                counts = write_partition_tables(directory, [market()], events, samples, [], [])
                _, digest = build_manifest(
                    directory,
                    Asset.DOGE,
                    "2026-04-13",
                    QualityTier.TIER_A,
                    [provenance()],
                    [],
                    COMMIT,
                    {"row_counts": counts},
                    market().market_start_ns,
                    market().market_end_ns,
                )
                self.assertEqual(verify_manifest(directory), digest)
                digests.append(digest)
            self.assertEqual(digests[0], digests[1])
            for filename in (
                "markets.parquet",
                "book-events.parquet",
                "book-200ms.parquet",
                "underlying.parquet",
                "exclusions.parquet",
            ):
                self.assertEqual(
                    (root / "one" / filename).read_bytes(), (root / "two" / filename).read_bytes()
                )

    def test_manifest_detects_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            counts = write_partition_tables(directory, [market()], [], [], [], [])
            build_manifest(
                directory,
                Asset.DOGE,
                "2026-04-13",
                QualityTier.TIER_A,
                [provenance()],
                [],
                COMMIT,
                {"row_counts": counts},
                market().market_start_ns,
                market().market_end_ns,
            )
            (directory / "markets.parquet").write_bytes(b"substituted")
            with self.assertRaises(ConflictError):
                verify_manifest(directory)

    def test_exclusions_are_canonical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            exclusion = Exclusion("m", ExclusionReason.SOURCE_GAP, "gap", {"z": 1, "a": 2})
            counts = write_partition_tables(Path(temp), [], [], [], [], [exclusion])
            self.assertEqual(counts["exclusions.parquet"], 1)


class StateTests(unittest.TestCase):
    def test_resume_idempotency_and_conflict_refusal(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            store = StateStore(Path(temp))
            first = Checkpoint("DOGE/1h/2026-04-13", Phase.INVENTORIED, "a")
            self.assertTrue(store.advance(first))
            self.assertFalse(store.advance(first))
            store.advance(Checkpoint(first.partition_id, Phase.VERIFIED, "a", "b"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.ACQUIRED, "a"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.PUBLISHED, "different", "b"))
            with self.assertRaises(ConflictError):
                store.advance(Checkpoint(first.partition_id, Phase.PUBLISHED, "a", "different"))


class PipelineTests(unittest.TestCase):
    def test_empty_partition_without_exclusion_evidence_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(PipelineError, "lacks explicit exclusion evidence"):
                Pipeline(root / "out", StateStore(root / "state"), COMMIT).build(
                    PartitionInputs(
                        Asset.DOGE,
                        "2026-04-13",
                        (),
                        provenance=(provenance(),),
                    ),
                    market().market_end_ns,
                )


    def test_compact_spool_is_lossless_and_avoids_duplicate_condition_index(self) -> None:
        decoded = decode_rows(pmxt_rows(), "https://example.test/hour.parquet")
        with tempfile.TemporaryDirectory() as temp:
            spool_path = Path(temp) / "events.sqlite"
            with EventSpool(spool_path) as spool:
                spool.append(decoded)
                self.assertEqual(spool.load(market().condition_id), decoded)
                indexes = {
                    str(row[1])
                    for row in spool.connection.execute("PRAGMA index_list(events)")
                }
                self.assertNotIn("events_condition", indexes)
                self.assertEqual(
                    {
                        str(row[1])
                        for row in spool.connection.execute("PRAGMA table_info(events)")
                    },
                    {"condition_key", "source_key", "source_row", "payload"},
                )

    def test_exact_legacy_disk_failure_shape_is_materially_reduced(self) -> None:
        base = decode_rows(pmxt_rows(), "https://example.test/hour.parquet")[1]
        events = [replace(base, source_row=index) for index in range(4_000)]
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            compact_path = root / "compact.sqlite"
            with EventSpool(compact_path) as compact:
                compact.append(events)
                compact_bytes = compact.storage_bytes()

            legacy_path = root / "legacy.sqlite"
            legacy = sqlite3.connect(legacy_path)
            with legacy:
                legacy.execute(
                    "CREATE TABLE events (condition_id TEXT NOT NULL, "
                    "source_object TEXT NOT NULL, payload BLOB NOT NULL)"
                )
                legacy.executemany(
                    "INSERT INTO events(condition_id,source_object,payload) VALUES (?,?,?)",
                    (
                        (
                            event.condition_id,
                            event.source_object,
                            sqlite3.Binary(pickle.dumps(event, protocol=5)),
                        )
                        for event in events
                    ),
                )
                legacy.execute("CREATE INDEX events_condition ON events(condition_id)")
            page_count = int(legacy.execute("PRAGMA page_count").fetchone()[0])
            page_size = int(legacy.execute("PRAGMA page_size").fetchone()[0])
            legacy.close()
            legacy_bytes = page_count * page_size

            self.assertLess(compact_bytes * 2, legacy_bytes)

    def test_all_market_source_gaps_build_an_explicit_excluded_partition(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            item = market()
            exclusion = Exclusion(
                item.market_id,
                ExclusionReason.SOURCE_GAP,
                "no usable event evidence",
                {"payload_sha256": "a" * 64},
            )
            built = Pipeline(root / "out", StateStore(root / "state"), COMMIT).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (),
                    provenance=(provenance(),),
                    preexisting_exclusions=(exclusion,),
                ),
                item.market_end_ns,
            )
            manifest = json.loads((built.directory / "manifest.json").read_bytes())
            self.assertEqual(built.tier, QualityTier.EXCLUDED)
            self.assertEqual(manifest["quality_tier"], "EXCLUDED")
            self.assertEqual(manifest["statistics"]["market_count"], 0)
            self.assertEqual(pq.read_metadata(built.directory / "exclusions.parquet").num_rows, 1)

    def test_disk_spool_pipeline_is_bounded_and_byte_deterministic(self) -> None:
        decoded = decode_rows(pmxt_rows(), "fixture")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            digests: list[str] = []
            files: list[bytes] = []
            for name in ("one", "two"):
                spool_path = root / f"{name}.sqlite"
                with EventSpool(spool_path) as spool:
                    self.assertEqual(spool.append(reversed(decoded)), len(decoded))
                    self.assertEqual(spool.count(), len(decoded))
                    self.assertEqual(spool.count_condition(market().condition_id), len(decoded))
                    spool.drop_index()
                    self.assertEqual(spool.load(market().condition_id), decoded)
                    spool.ensure_index()
                built = Pipeline(
                    root / name / "out", StateStore(root / name / "state"), COMMIT
                ).build(
                    PartitionInputs(
                        Asset.DOGE,
                        "2026-04-13",
                        (market(),),
                        event_spool_path=spool_path,
                        provenance=(provenance(),),
                        preexisting_exclusions=(
                            Exclusion(
                                "missing-slug",
                                ExclusionReason.SOURCE_GAP,
                                "official gap",
                                {"payload_sha256": "a" * 64},
                            ),
                        ),
                    ),
                    market().market_end_ns,
                )
                digests.append(built.manifest_digest)
                files.append((built.directory / "book-events.parquet").read_bytes())
                self.assertGreater(
                    pq.read_metadata(built.directory / "book-200ms.parquet").num_rows, 0
                )
                self.assertEqual(
                    pq.read_metadata(built.directory / "exclusions.parquet").num_rows, 1
                )
            self.assertEqual(digests[0], digests[1])
            self.assertEqual(files[0], files[1])

    def test_causally_staged_pipeline_is_byte_equivalent_to_full_spool(self) -> None:
        decoded = decode_rows(pmxt_rows(), "fixture")
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            spool_path = root / "events.sqlite"
            with EventSpool(spool_path) as spool:
                spool.append(decoded)
            full = Pipeline(root / "full", StateStore(root / "full-state"), COMMIT).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    provenance=(provenance(),),
                    event_spool_path=spool_path,
                ),
                market().market_end_ns,
            )
            staged_market = stage_market(
                market(), order_and_deduplicate(decoded), root / "staged-fragments"
            )
            bounded = Pipeline(
                root / "bounded", StateStore(root / "bounded-state"), COMMIT
            ).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    provenance=(provenance(),),
                    staged_markets=(staged_market,),
                ),
                market().market_end_ns,
            )
            self.assertEqual(full.manifest_digest, bounded.manifest_digest)
            self.assertEqual(
                {path.name: path.read_bytes() for path in full.directory.iterdir()},
                {path.name: path.read_bytes() for path in bounded.directory.iterdir()},
            )

    def test_causal_staging_preserves_source_gap_and_event_conflict_outputs(self) -> None:
        conflict = {**pmxt_rows()[2], "best_ask": "0.55"}
        cases = {
            "source-gap": [],
            "event-conflict": decode_rows([*pmxt_rows(False), conflict], "fixture"),
        }
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, events in cases.items():
                with self.subTest(name=name):
                    case_root = root / name
                    spool_path = case_root / "events.sqlite"
                    with EventSpool(spool_path) as spool:
                        spool.append(events)
                    full = Pipeline(
                        case_root / "full", StateStore(case_root / "full-state"), COMMIT
                    ).build(
                        PartitionInputs(
                            Asset.DOGE,
                            "2026-04-13",
                            (market(),),
                            provenance=(provenance(),),
                            event_spool_path=spool_path,
                        ),
                        market().market_end_ns,
                    )
                    staged_market = stage_market(
                        market(),
                        order_and_deduplicate(events),
                        case_root / "fragments",
                    )
                    bounded = Pipeline(
                        case_root / "bounded",
                        StateStore(case_root / "bounded-state"),
                        COMMIT,
                    ).build(
                        PartitionInputs(
                            Asset.DOGE,
                            "2026-04-13",
                            (market(),),
                            provenance=(provenance(),),
                            staged_markets=(staged_market,),
                        ),
                        market().market_end_ns,
                    )
                    self.assertEqual(full.manifest_digest, bounded.manifest_digest)
                    self.assertEqual(
                        {path.name: path.read_bytes() for path in full.directory.iterdir()},
                        {path.name: path.read_bytes() for path in bounded.directory.iterdir()},
                    )

    def test_disk_spool_enforces_native_event_cap_after_deduplication(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            spool_path = root / "events.sqlite"
            with EventSpool(spool_path) as spool:
                spool.append(decode_rows(pmxt_rows(), "fixture"))
            with self.assertRaises(ResourceLimitError):
                Pipeline(
                    root / "out",
                    StateStore(root / "state"),
                    COMMIT,
                    PipelineLimits(max_pmxt_rows=1),
                ).build(
                    PartitionInputs(
                        Asset.DOGE,
                        "2026-04-13",
                        (market(),),
                        event_spool_path=spool_path,
                        provenance=(provenance(),),
                    ),
                    market().market_end_ns,
                )

    def test_invalid_pmxt_is_evidence_backed_exclusion_without_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            bad_pmxt = (pmxt_rows()[2],)  # increment without any snapshot
            inputs = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                bad_pmxt,
                "bad-fixture",
                provenance=(provenance(),),
            )
            built = pipeline.build(inputs, market().market_end_ns)
            self.assertEqual(built.tier, QualityTier.EXCLUDED)
            exclusions = pq.read_table(built.directory / "exclusions.parquet").to_pylist()
            self.assertEqual(exclusions[0]["reason_code"], "NO_INITIAL_SNAPSHOT")
            self.assertIn("condition_id", exclusions[0]["evidence_json"])

    def test_missing_claimed_bbo_level_is_event_conflict_not_synthetic_depth(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            conflict = {
                **pmxt_rows()[2],
                "best_ask": "0.55",
            }
            built = Pipeline(root / "out", StateStore(root / "state"), COMMIT).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    tuple([*pmxt_rows(False), conflict]),
                    "fixture",
                    provenance=(provenance(),),
                ),
                market().market_end_ns,
            )
            self.assertEqual(built.tier, QualityTier.EXCLUDED)
            exclusions = pq.read_table(built.directory / "exclusions.parquet").to_pylist()
            self.assertEqual(exclusions[0]["reason_code"], "EVENT_CONFLICT")
            self.assertIn(
                "best ask disagrees: reconstructed=0.60, claimed=0.55",
                exclusions[0]["detail"],
            )
            self.assertIn("receive_ts_ns=", exclusions[0]["detail"])
            self.assertIn("source_row=", exclusions[0]["detail"])

    def test_pre_snapshot_increment_does_not_poison_later_full_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            prefix = {
                **pmxt_rows()[2],
                "timestamp": market().market_start_ns // 1_000_000 - 2_000,
                "timestamp_received": market().market_start_ns // 1_000_000 - 2_000,
            }
            snapshots = [
                {
                    **row,
                    "timestamp": row["timestamp"] - 1_000,
                    "timestamp_received": row["timestamp_received"] - 1_000,
                }
                for row in pmxt_rows(False)
            ]
            built = Pipeline(root / "out", StateStore(root / "state"), COMMIT).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    tuple([prefix, *snapshots, pmxt_rows()[2]]),
                    "fixture",
                    provenance=(provenance(),),
                ),
                market().market_end_ns,
            )
            self.assertEqual(built.tier, QualityTier.TIER_A)
            manifest = json.loads((built.directory / "manifest.json").read_bytes())
            self.assertEqual(manifest["statistics"]["exclusion_count"], 0)

    def test_reconstructable_pmxt_initial_gap_is_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            delayed = []
            for row in pmxt_rows(False):
                delayed.append(
                    {
                        **row,
                        "timestamp": row["timestamp"] + 1_000,
                        "timestamp_received": row["timestamp_received"] + 1_000,
                    }
                )
            built = Pipeline(root / "out", StateStore(root / "state"), COMMIT).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    tuple(delayed),
                    "fixture",
                    provenance=(provenance(),),
                ),
                market().market_end_ns,
            )
            self.assertEqual(built.tier, QualityTier.EXCLUDED)
            exclusions = pq.read_table(built.directory / "exclusions.parquet").to_pylist()
            self.assertEqual(exclusions[0]["reason_code"], "SOURCE_GAP")
            evidence = json.loads(exclusions[0]["evidence_json"])
            self.assertEqual(evidence["gap_count"], 2)
            self.assertEqual(
                evidence["gap_intervals_ns"],
                [
                    [market().market_start_ns, market().market_start_ns + 1_000_000_000],
                    [market().market_start_ns, market().market_start_ns + 1_000_000_000],
                ],
            )
            self.assertEqual(evidence["gap_total_ns"], 2_000_000_000)

    def test_partition_resource_caps_fail_before_work(self) -> None:
        self.assertEqual(PipelineLimits().max_pmxt_rows, 194_466_240)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            limits = PipelineLimits(max_markets=0)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT, limits)
            inputs = PartitionInputs(Asset.DOGE, "2026-04-13", (market(),))
            with self.assertRaises(ResourceLimitError):
                pipeline.build(inputs, market().market_end_ns)
            self.assertFalse((root / "state").exists())

    def test_memory_and_transformed_byte_caps_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            inputs = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance(),),
            )
            memory_limited = Pipeline(
                root / "memory",
                StateStore(root / "memory-state"),
                COMMIT,
                PipelineLimits(minimum_available_memory_bytes=10**30),
            )
            with self.assertRaises(ResourceLimitError):
                memory_limited.build(inputs, market().market_end_ns)
            byte_limited = Pipeline(
                root / "bytes",
                StateStore(root / "byte-state"),
                COMMIT,
                PipelineLimits(max_partition_bytes=1),
            )
            with self.assertRaises(ResourceLimitError):
                byte_limited.build(inputs, market().market_end_ns)

    def test_tiny_end_to_end_and_exact_noop(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            inputs = PartitionInputs(
                asset=Asset.DOGE,
                day="2026-04-13",
                markets=(market(),),
                pmxt_rows=tuple(pmxt_rows()),
                pmxt_source_object="fixture",
                provenance=(provenance(),),
            )
            built = pipeline.build(inputs, market().market_end_ns)
            again = pipeline.build(inputs, market().market_end_ns)
            self.assertEqual(built.manifest_digest, again.manifest_digest)
            manifest = json.loads((built.directory / "manifest.json").read_bytes())
            self.assertEqual(manifest["quality_tier"], "TIER_A")
            self.assertEqual(manifest["statistics"]["market_count"], 1)
            raw = root / "raw.source"
            raw.write_bytes(b"temporary raw")
            publisher = Publisher(DirectoryReleaseBackend(root / "remote"))
            assets = pipeline.publish(built, publisher, "pilot", [raw])
            self.assertFalse(raw.exists())
            self.assertEqual(len(assets), 6)
            checkpoint = StateStore(root / "state").load(built.partition_id)
            assert checkpoint is not None
            self.assertEqual(checkpoint.phase, Phase.PUBLISHED)

    def test_same_partition_different_identity_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            original = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance(),),
            )
            pipeline.build(original, market().market_end_ns)
            changed = PartitionInputs(
                Asset.DOGE,
                "2026-04-13",
                (market(),),
                tuple(pmxt_rows()),
                "fixture",
                provenance=(provenance("different"),),
            )
            with self.assertRaises(PipelineError):
                pipeline.build(changed, market().market_end_ns)

    def test_pipeline_resumes_after_acquired_or_transformed_checkpoint(self) -> None:
        for interrupted_phase in (Phase.ACQUIRED, Phase.TRANSFORMED):
            with self.subTest(phase=interrupted_phase), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                inputs = PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    tuple(pmxt_rows()),
                    "fixture",
                    provenance=(provenance(),),
                )
                pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
                identity = pipeline._identity_digest(inputs)
                StateStore(root / "state").advance(
                    Checkpoint("DOGE/1h/2026-04-13", interrupted_phase, identity)
                )
                built = pipeline.build(inputs, market().market_end_ns)
                self.assertEqual(built.checkpoint.phase, Phase.VERIFIED)
                self.assertEqual(verify_manifest(built.directory), built.manifest_digest)

    def test_no_publishable_market_is_canonical_excluded_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            pipeline = Pipeline(root / "out", StateStore(root / "state"), COMMIT)
            inputs = PartitionInputs(
                Asset.DOGE, "2026-04-13", (market(),), provenance=(provenance(),)
            )
            built = pipeline.build(inputs, market().market_end_ns)
            self.assertEqual(built.tier, QualityTier.EXCLUDED)
            self.assertEqual(verify_manifest(built.directory), built.manifest_digest)


class FailingOnceBackend(DirectoryReleaseBackend):
    def __init__(self, root: Path):
        super().__init__(root)
        self.fail = True

    def upload(self, release_id: str, name: str, path: Path):  # type: ignore[no-untyped-def]
        if self.fail:
            self.fail = False
            raise OSError("simulated interruption")
        return super().upload(release_id, name, path)


class ReleaseTests(unittest.TestCase):
    def test_ambiguous_release_create_reconciles_before_retry(self) -> None:
        class AmbiguousCreate(GitHubReleaseBackend):
            def __init__(self, created_remotely: bool) -> None:
                self.api = "https://api.example.test/repos/owner/repository"
                self.created_remotely = created_remotely
                self.post_calls = 0
                self.inventory_calls = 0

            def _request(
                self,
                method: str,
                url: str,
                payload: bytes | None = None,
                content_type: str = "application/json",
            ) -> Any:
                del url, payload, content_type
                if method == "GET":
                    self.inventory_calls += 1
                    if self.inventory_calls == 1 or not self.created_remotely:
                        return []
                    return [{"id": 42, "tag_name": "pilot", "draft": True}]
                self.post_calls += 1
                self.created_remotely = True
                raise GitHubAPIError("POST", 503)

        backend = AmbiguousCreate(created_remotely=False)
        with patch("canonical_data.release.time.sleep") as sleep:
            self.assertEqual(backend.ensure_draft("pilot"), "42")
        self.assertEqual(backend.post_calls, 1)
        self.assertEqual(backend.inventory_calls, 2)
        sleep.assert_not_called()

    def test_release_create_retries_only_after_empty_reconciliation(self) -> None:
        class RetryCreate(GitHubReleaseBackend):
            def __init__(self) -> None:
                self.api = "https://api.example.test/repos/owner/repository"
                self.created = False
                self.post_calls = 0

            def _request(
                self,
                method: str,
                url: str,
                payload: bytes | None = None,
                content_type: str = "application/json",
            ) -> Any:
                del url, payload, content_type
                if method == "GET":
                    return (
                        [{"id": 42, "tag_name": "pilot", "draft": True}]
                        if self.created
                        else []
                    )
                self.post_calls += 1
                if self.post_calls == 1:
                    raise GitHubAPIError("POST", 503)
                self.created = True
                return {"id": 42, "tag_name": "pilot", "draft": True}

        backend = RetryCreate()
        with patch("canonical_data.release.time.sleep") as sleep:
            self.assertEqual(backend.ensure_draft("pilot"), "42")
        self.assertEqual(backend.post_calls, 2)
        sleep.assert_called_once_with(2)

    def test_release_create_relists_after_backoff_before_retry(self) -> None:
        class DelayedVisibility(GitHubReleaseBackend):
            def __init__(self) -> None:
                self.api = "https://api.example.test/repos/owner/repository"
                self.inventory_calls = 0
                self.post_calls = 0

            def _request(
                self,
                method: str,
                url: str,
                payload: bytes | None = None,
                content_type: str = "application/json",
            ) -> Any:
                del url, payload, content_type
                if method == "GET":
                    self.inventory_calls += 1
                    if self.inventory_calls < 3:
                        return []
                    return [{"id": 42, "tag_name": "pilot", "draft": True}]
                self.post_calls += 1
                raise GitHubAPIError("POST", 503)

        backend = DelayedVisibility()
        with patch("canonical_data.release.time.sleep") as sleep:
            self.assertEqual(backend.ensure_draft("pilot"), "42")
        self.assertEqual(backend.post_calls, 1)
        self.assertEqual(backend.inventory_calls, 3)
        sleep.assert_called_once_with(2)

    def test_github_inventory_retries_tls_without_disabling_verification(self) -> None:
        class Response:
            def __enter__(self) -> Response:
                return self

            def __exit__(self, *args: object) -> None:
                return None

            def read(self) -> bytes:
                return b"[]"

        backend = GitHubReleaseBackend("owner/repository", "token")
        with (
            patch(
                "canonical_data.release.urllib.request.urlopen",
                side_effect=(urllib.error.URLError("certificate verify failed"), Response()),
            ) as urlopen,
            patch("canonical_data.release.time.sleep") as sleep,
        ):
            self.assertEqual(backend._request("GET", "https://api.example.test"), [])
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_exact_non_durable_asset_is_removed_before_idempotent_upload(self) -> None:
        class StarterBackend(DirectoryReleaseBackend):
            def __init__(self, root: Path) -> None:
                super().__init__(root)
                self.starter: ReleaseAsset | None = None
                self.deleted = False

            def list_assets(self, release_id: str) -> list[ReleaseAsset]:
                assets = super().list_assets(release_id)
                return ([self.starter] if self.starter is not None else []) + assets

            def delete_incomplete(self, release_id: str, asset: ReleaseAsset) -> None:
                del release_id
                self.assert_not_uploaded(asset)
                self.starter = None
                self.deleted = True

            @staticmethod
            def assert_not_uploaded(asset: ReleaseAsset) -> None:
                if asset.state == "uploaded":
                    raise AssertionError("durable asset deletion attempted")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = StarterBackend(root / "remote")
            backend.ensure_draft("pilot")
            path = fixture / "book.parquet"
            length, digest = hash_file(path)
            name = f"DOGE--1h--2026-04-13--{digest}--book.parquet"
            backend.starter = ReleaseAsset(
                name, length, digest, "https://example.test/starter", "starter", "1"
            )
            assets = Publisher(backend).publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            self.assertTrue(backend.deleted)
            self.assertTrue(all(asset.state == "uploaded" for asset in assets))

    def test_github_502_upload_reconciles_completed_content_addressed_asset(self) -> None:
        class Response:
            status = 502

            def read(self) -> bytes:
                return b""

        class Connection:
            def putrequest(self, method: str, endpoint: str) -> None:
                del method, endpoint

            def putheader(self, name: str, value: str) -> None:
                del name, value

            def endheaders(self) -> None:
                return None

            def send(self, chunk: bytes) -> None:
                del chunk

            def getresponse(self) -> Response:
                return Response()

            def close(self) -> None:
                return None

        class ReconcilingBackend(GitHubReleaseBackend):
            def __init__(self, expected: ReleaseAsset) -> None:
                super().__init__("owner/repository", "token")
                self.expected = expected

            def list_assets(self, release_id: str) -> list[ReleaseAsset]:
                del release_id
                return [self.expected]

        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.json"
            path.write_bytes(b"canonical")
            length, digest = hash_file(path)
            name = f"DOGE--1h--2026-04-13--{digest}--manifest.json"
            expected = ReleaseAsset(name, length, digest, "https://api.example.test/asset")
            backend = ReconcilingBackend(expected)
            with patch(
                "canonical_data.release.http.client.HTTPSConnection",
                return_value=Connection(),
            ):
                self.assertEqual(backend.upload("42", name, path), expected)

    def test_github_draft_lookup_enumerates_drafts_instead_of_tag_endpoint(self) -> None:
        class FakeGitHub(GitHubReleaseBackend):
            def __init__(self) -> None:
                self.api = "https://api.example.test/repos/owner/repository"
                self.calls: list[tuple[str, str]] = []

            def _request(
                self,
                method: str,
                url: str,
                payload: bytes | None = None,
                content_type: str = "application/json",
            ) -> Any:
                del payload, content_type
                self.calls.append((method, url))
                return [{"id": 42, "tag_name": "pilot", "draft": True}]

        backend = FakeGitHub()
        self.assertEqual(backend.ensure_draft("pilot"), "42")
        self.assertEqual(backend.calls[0][0], "GET")
        self.assertIn("/releases?", backend.calls[0][1])

    def test_same_logical_filename_in_different_partitions_is_not_a_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = root / "first"
            second = root / "second"
            first.mkdir()
            second.mkdir()
            (first / "manifest.json").write_bytes(b"one")
            (second / "manifest.json").write_bytes(b"two")
            publisher = Publisher(DirectoryReleaseBackend(root / "release"))
            publisher.publish_partition("draft", "DOGE/1h/2026-04-13", first)
            publisher.publish_partition("draft", "BNB/1h/2026-04-13", second)
            self.assertEqual(len(DirectoryReleaseBackend(root / "release").list_assets("draft")), 2)

    def _fixture(self, root: Path) -> Path:
        fixture = root / "partition"
        fixture.mkdir()
        (fixture / "manifest.json").write_text('{"tiny":true}\n')
        (fixture / "book.parquet").write_bytes(b"tiny fixture only")
        return fixture

    def test_draft_roundtrip_idempotency_and_download_verify(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = DirectoryReleaseBackend(root / "remote")
            publisher = Publisher(backend)
            first = publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            second = publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            self.assertEqual(first, second)
            downloaded = download_and_verify_release(backend, "pilot", root / "download")
            self.assertEqual(len(downloaded), 2)
            self.assertTrue((root / "remote" / "pilot" / ".draft").exists())

    def test_conflicting_remote_output_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            publisher = Publisher(DirectoryReleaseBackend(root / "remote"))
            publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            (fixture / "book.parquet").write_bytes(b"different")
            with self.assertRaises(ConflictError):
                publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)

    def test_interruption_is_restartable_and_raw_cleanup_waits_for_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = FailingOnceBackend(root / "remote")
            raw = root / "raw.source"
            raw.write_bytes(b"raw")
            publisher = Publisher(backend)
            with self.assertRaises(OSError):
                publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            self.assertTrue(raw.exists())
            publisher.publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)

    def test_download_detects_remote_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fixture = self._fixture(root)
            backend = DirectoryReleaseBackend(root / "remote")
            assets = Publisher(backend).publish_partition("pilot", "DOGE/1h/2026-04-13", fixture)
            Path(assets[0].download_url).write_bytes(b"tampered")
            with self.assertRaises(ConflictError):
                download_and_verify_release(backend, "pilot", root / "download")
