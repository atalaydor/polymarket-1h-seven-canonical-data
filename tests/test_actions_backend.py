from __future__ import annotations

import hashlib
import io
import json
import re
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
from collections.abc import Callable
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, replace
from datetime import UTC, date, datetime, timedelta
from email.message import Message
from pathlib import Path
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
from helpers import START_S, market, pmxt_rows, provenance

from canonical_data.acquire import AcquiredObject, BoundedAcquirer
from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import (
    ConflictError,
    ResourceLimitError,
    SourceError,
    SourceIdentityError,
    UnresolvedMarketError,
)
from canonical_data.inventory import SourceObject, pmxt_hourly_objects
from canonical_data.models import Asset
from canonical_data.pipeline import PartitionInputs, Pipeline, stage_market
from canonical_data.pmxt import decode_rows
from canonical_data.sources import OfficialDiscovery
from canonical_data.state import StateStore
from scripts.actions_backend import (
    CANARY_MAX_CANDIDATES,
    CANARY_MAX_CANDIDATES_TOTAL,
    CANARY_MAX_GAMMA_REQUESTS,
    CANARY_MAX_ROUNDS,
    CANARY_MAX_SOURCE_OBJECTS,
    Authority,
    CanaryQualification,
    QualifiedCandidate,
    RemoteAsset,
    _adaptive_round_authorities,
    _assets_by_day,
    _bundle_file_inventory,
    _bundle_partition_directory,
    _candidate_starts,
    _control_plane_digest,
    _execute_canary_round,
    _full_plan,
    _market_authority_projection,
    _pmxt_source_identity,
    _raise_child_failure,
    _request,
    _require_canary_receipt,
    _require_complete_staged_coverage,
    _validate_receipt_coverage,
    _validate_staged_partition,
    _validated_compute_assets,
    _verify_canary_dispositions,
    command_certify,
    command_execute_day,
    day_plan,
    inventory_anomalies,
    load_authority,
    minimum_canary_cover,
    qualify_canary_candidates,
    unfinished_plan,
    verified_partitions,
)
from scripts.run_backfill import (
    MINIMUM_FREE_DISK_BYTES,
    _acquire_with_retry,
    _atomic_json,
    _copy_segment_staged_market,
    _market_starts,
    _segment_file_inventory,
    _tool_commit,
    _validate_expected_market_identities,
    _validate_expected_source_identity,
    _validate_pmxt_download,
    _verify_shared_disk_margin,
    assemble_day_segments,
    compute_staged_partition,
    day_segment_starts,
    run_day,
    run_staged_partition,
)


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.offset = 0
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/octet-stream"
        self.headers["Content-Length"] = str(len(payload))

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.payload) - self.offset
        chunk = self.payload[self.offset : self.offset + size]
        self.offset += len(chunk)
        return chunk


class ActionsBackendTests(unittest.TestCase):
    @staticmethod
    def authority(start: date = date(2026, 4, 5), end: date = date(2026, 4, 6)) -> Authority:
        return Authority(
            datetime(start.year, start.month, start.day, tzinfo=UTC),
            datetime(end.year, end.month, end.day, tzinfo=UTC) + timedelta(days=1),
            tuple(Asset),
            datetime.fromtimestamp(START_S, UTC),
            datetime.fromtimestamp(START_S, UTC),
            15,
        )

    @staticmethod
    def assets(partition: str) -> list[RemoteAsset]:
        asset, _, day = partition.split("/")
        return [
            RemoteAsset(
                f"{asset}--1h--{day}--{'a' * 64}--{filename}",
                1,
                "https://example.test/asset",
                "a" * 64,
                filename,
            )
            for filename in (
                "book-200ms.parquet",
                "book-events.parquet",
                "exclusions.parquet",
                "manifest.json",
                "markets.parquet",
                "underlying.parquet",
            )
        ]

    def test_repository_authority_is_exact_1h_x7_and_finite(self) -> None:
        authority = load_authority()
        self.assertEqual(authority.assets, tuple(Asset))
        self.assertEqual(
            [asset.value for asset in authority.assets],
            ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"],
        )
        self.assertEqual(authority.start, datetime(2026, 4, 13, 20, tzinfo=UTC))
        self.assertEqual(authority.cutoff, datetime(2026, 8, 10, 1, tzinfo=UTC))
        self.assertEqual(authority.canary_search_start, datetime(2026, 8, 5, 23, tzinfo=UTC))
        candidates = _candidate_starts(authority)
        self.assertEqual(len(candidates), CANARY_MAX_CANDIDATES)
        self.assertEqual(len(_adaptive_round_authorities(authority)), CANARY_MAX_ROUNDS)
        all_candidates = [
            start
            for selected in _adaptive_round_authorities(authority)
            for start in _candidate_starts(selected)
        ]
        self.assertEqual(len(all_candidates), CANARY_MAX_CANDIDATES_TOTAL)
        self.assertEqual(len(all_candidates), len(set(all_candidates)))
        self.assertEqual(len(_full_plan(authority)), 840)

    def test_production_entrypoints_use_import_safe_module_execution(self) -> None:
        root = Path(__file__).resolve().parents[1]
        workflows = sorted((root / ".github/workflows").glob("*.y*ml"))
        sources = sorted((root / "scripts").glob("*.py"))
        for path in workflows:
            self.assertNotIn("python scripts/", path.read_text(), str(path))
        direct_script = re.compile(r'["\']scripts[/\\][^"\']+\.py["\']')
        for path in sources:
            self.assertNotRegex(path.read_text(), direct_script, str(path))
        workflow = (root / ".github/workflows/polymarket-1h-seven.yml").read_text()
        for command in ("canary", "plan", "checkpoint", "certify"):
            self.assertIn(f"python -m scripts.actions_backend {command}", workflow)
        self.assertIn("polymarket-1h-seven-accelerated-day.yml", workflow)
        self.assertIn("polymarket-1h-seven-v1-certified", workflow)
        self.assertIn("max-parallel: 4", workflow)
        self.assertIn("polymarket-1h-seven-canary-authority", workflow)
        self.assertIn("fetch-depth: 0", workflow)
        self.assertIn("queue: max", workflow)
        self.assertIn("needs:\n      - plan\n      - backfill-day", workflow)
        self.assertLess(
            workflow.index("git pull --ff-only origin main"),
            workflow.index(
                "python -m pip install --disable-pip-version-check --no-cache-dir .",
                workflow.index("git pull --ff-only origin main"),
            ),
        )
        self.assertEqual(MINIMUM_FREE_DISK_BYTES, 4_000_000_000)
        recovery = (
            root / ".github/workflows/polymarket-1h-seven-recover-day.yml"
        ).read_text()
        self.assertIn("inputs.release_group", recovery)
        self.assertIn("python -m scripts.actions_backend execute-day", recovery)
        self.assertIn('--day "${{ inputs.day }}"', recovery)
        self.assertIn('--expected-release-group "${{ inputs.release_group }}"', recovery)
        self.assertLess(
            recovery.index("git pull --ff-only origin main"),
            recovery.index("python -m pip install --disable-pip-version-check --no-cache-dir ."),
        )
        accelerated = (
            root / ".github/workflows/polymarket-1h-seven-accelerated-day.yml"
        ).read_text()
        batch = (
            root / ".github/workflows/polymarket-1h-seven-accelerated-batch.yml"
        ).read_text()
        self.assertIn("python -m scripts.actions_backend compute-day-segment", accelerated)
        self.assertIn("python -m scripts.actions_backend assemble-day-segments", accelerated)
        self.assertIn('--expected-assets "$ASSETS"', accelerated)
        self.assertNotIn("workflow_dispatch:", accelerated)
        self.assertIn("python -m scripts.actions_backend publish-staged-day", accelerated)
        self.assertIn("retention-days: 1", accelerated)
        self.assertEqual(accelerated.count("concurrency:"), 1)
        self.assertLess(accelerated.index("publish:"), accelerated.index("concurrency:"))
        self.assertIn("max-parallel: 4", batch)
        self.assertIn("validate-accelerated-matrix", batch)
        self.assertIn("contents: write", batch)
        for module in ("scripts.actions_backend", "scripts.run_backfill"):
            completed = subprocess.run(
                [sys.executable, "-m", module, "--help"],
                cwd=root,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
        bootstrap = subprocess.run(
            [
                sys.executable,
                "-c",
                "import pathlib, scripts, canonical_data; "
                "root=pathlib.Path.cwd().resolve(); "
                "assert pathlib.Path(canonical_data.__file__).resolve().is_relative_to(root/'src')",
            ],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(bootstrap.returncode, 0, bootstrap.stderr)

    def test_final_certification_reconciles_exact_remote_authority(self) -> None:
        authority = self.authority()
        partition_ids = [f"BTC/1h/2026-04-{index + 1:02d}" for index in range(840)]
        plan = [
            {
                "partition_id": partition,
                "release_group": "polymarket-1h-seven-v1-2026-04-a",
            }
            for partition in partition_ids
        ]
        inventory = {
            partition: [
                replace(asset, release_tag="polymarket-1h-seven-v1-2026-04-a")
                for asset in self.assets(partition)
            ]
            for partition in partition_ids
        }
        partitions = {
            partition: {
                "quality": "TIER_A" if index < 480 else "EXCLUDED",
                "manifest_sha256": "a" * 64,
                "release_tag": "polymarket-1h-seven-v1-2026-04-a",
            }
            for index, partition in enumerate(partition_ids)
        }
        ledger = {
            "planned": 840,
            "completed": 840,
            "unfinished": 0,
            "continuation_partition": None,
            "partitions": partitions,
        }
        empty_anomalies: dict[str, list[str]] = {
            "partial": [],
            "divergent": [],
            "duplicate": [],
            "unexpected_files": [],
            "out_of_plan": [],
        }
        with (
            patch.dict(
                "os.environ",
                {"GITHUB_TOKEN": "token", "RECOVERY_RUN_IDS": "1,2"},
                clear=False,
            ),
            patch("scripts.actions_backend.load_authority", return_value=authority),
            patch("scripts.actions_backend._require_canary_receipt"),
            patch("scripts.actions_backend.remote_inventory", return_value=inventory),
            patch("scripts.actions_backend.inventory_anomalies", return_value=empty_anomalies),
            patch("scripts.actions_backend._full_plan", return_value=plan),
            patch("scripts.actions_backend.verified_partitions", return_value=set(partition_ids)),
            patch("scripts.actions_backend.reconcile_ledger", return_value=ledger),
            patch("scripts.actions_backend._atomic_json") as atomic,
            redirect_stdout(io.StringIO()),
        ):
            command_certify()
        report = atomic.call_args_list[-1].args[1]
        self.assertEqual(
            (report["tier_a"], report["tier_b"], report["excluded"]),
            (480, 0, 360),
        )
        self.assertEqual(report["remote_assets"], 5040)
        self.assertEqual(report["recovery_run_ids"], [1, 2])
        self.assertEqual(report["anomalies"], empty_anomalies)

    def test_bounded_recovery_fails_before_inventory_on_wrong_release_group(self) -> None:
        authority = self.authority()
        with (
            patch("scripts.actions_backend.load_authority", return_value=authority),
            patch("scripts.actions_backend._require_canary_receipt"),
            patch(
                "scripts.actions_backend._full_plan",
                return_value=[
                    {
                        "day": "2026-04-05",
                        "release_group": "polymarket-1h-seven-v1-2026-04-a",
                    }
                ],
            ),
            patch("scripts.actions_backend.remote_inventory") as inventory,
            self.assertRaisesRegex(RuntimeError, "does not match the frozen plan"),
        ):
            command_execute_day("2026-04-05", "wrong-release")
        inventory.assert_not_called()

    def test_staged_bundle_authentication_rejects_substitution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            partition_id = "DOGE/1h/2026-04-13"
            directory = _bundle_partition_directory(root, partition_id)
            built = Pipeline(
                root / "partitions",
                StateStore(root / "state"),
                "a" * 40,
            ).build(
                PartitionInputs(
                    Asset.DOGE,
                    "2026-04-13",
                    (market(),),
                    provenance=(provenance(),),
                    decoded_pmxt_events=tuple(decode_rows(pmxt_rows(), "fixture")),
                ),
                market().market_end_ns,
            )
            self.assertEqual(directory, built.directory)
            files = _bundle_file_inventory(directory, root)
            self.assertEqual(
                _validate_staged_partition(root, partition_id, "a" * 40, files),
                directory,
            )
            (directory / "book-events.parquet").write_bytes(b"substituted")
            with self.assertRaisesRegex(ConflictError, "receipt digest mismatch"):
                _validate_staged_partition(root, partition_id, "a" * 40, files)

    def test_staged_publication_requires_every_currently_unfinished_partition(self) -> None:
        plan = {"BTC/1h/2026-04-13", "ETH/1h/2026-04-13"}
        with self.assertRaisesRegex(ConflictError, "omits unfinished partitions"):
            _require_complete_staged_coverage(
                ["BTC/1h/2026-04-13"], plan, set()
            )
        _require_complete_staged_coverage(
            ["BTC/1h/2026-04-13"], plan, {"ETH/1h/2026-04-13"}
        )

    def test_accelerated_assignment_is_exact_bounded_and_fail_closed(self) -> None:
        plan = [
            {"day": "2026-04-13", "asset": "BTC"},
            {"day": "2026-04-13", "asset": "ETH"},
        ]
        self.assertEqual(_assets_by_day(plan), {"2026-04-13": ["BTC", "ETH"]})
        self.assertEqual(
            _validated_compute_assets(plan, "BTC,ETH"), (Asset.BTC, Asset.ETH)
        )
        for assignment in ("", "BTC,BTC", "BTC,SOL"):
            with self.subTest(assignment=assignment), self.assertRaises(
                (RuntimeError, ValueError)
            ):
                _validated_compute_assets(plan, assignment)

    def test_accelerated_day_segments_are_exact_disjoint_and_seven_sources_each(self) -> None:
        day = date(2026, 7, 2)
        coverage_start = datetime(2026, 7, 2, tzinfo=UTC)
        cutoff = datetime(2026, 7, 3, tzinfo=UTC)
        segments = [
            day_segment_starts(day, coverage_start, cutoff, index, 4)
            for index in range(4)
        ]
        self.assertEqual([len(item) for item in segments], [6, 6, 6, 6])
        self.assertEqual(len({start for item in segments for start in item}), 24)
        self.assertTrue(segments[0][-1] < segments[1][0] < segments[2][0] < segments[3][0])
        for starts in segments:
            objects = pmxt_hourly_objects(
                (starts[0] - 3_600) * 1_000_000_000,
                (starts[-1] + 3_600) * 1_000_000_000,
            )
            self.assertEqual(len(objects), 7)

    def test_compute_only_partition_is_byte_equivalent_to_ordinary_staged_build(self) -> None:
        selected = market()
        day = datetime.fromtimestamp(START_S, UTC).date()
        events = decode_rows(pmxt_rows(), "fixture")
        cutoff = datetime.fromtimestamp(START_S + 3_600, UTC)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            compute_stage = root / "compute-stage"
            ordinary_stage = root / "ordinary-stage"
            computed_market = stage_market(selected, events, compute_stage)
            ordinary_market = stage_market(selected, events, ordinary_stage)
            discovery = OfficialDiscovery((selected,), (provenance("gamma"),))
            with (
                patch("scripts.run_backfill.shutil.rmtree"),
                patch(
                    "canonical_data.pipeline.Pipeline._available_memory_bytes",
                    return_value=16_000_000_000,
                ),
            ):
                computed = compute_staged_partition(
                    Asset.DOGE,
                    day,
                    root / "compute-work",
                    root / "compute-output",
                    cutoff,
                    discovery,
                    (computed_market,),
                    compute_stage,
                    (provenance(),),
                    START_S * 1_000_000_000,
                )
                with (
                    patch("scripts.run_backfill.Pipeline.publish", return_value=[]),
                    patch("scripts.run_backfill.GitHubReleaseBackend", return_value=Mock()),
                ):
                    ordinary = run_staged_partition(
                        Asset.DOGE,
                        day,
                        root / "ordinary-work",
                        root / "ledger.json",
                        cutoff,
                        discovery,
                        (ordinary_market,),
                        ordinary_stage,
                        (provenance(),),
                        coverage_start_ns=START_S * 1_000_000_000,
                    )
            self.assertEqual(computed["manifest_sha256"], ordinary["manifest_sha256"])
            computed_directory = (
                root
                / "compute-output"
                / "asset=DOGE"
                / "timeframe=1h"
                / f"date={day.isoformat()}"
            )
            ordinary_directory = (
                root
                / "ordinary-work"
                / f"DOGE-{day.isoformat()}"
                / "output"
                / "asset=DOGE"
                / "timeframe=1h"
                / f"date={day.isoformat()}"
            )
            self.assertEqual(
                {path.name: path.read_bytes() for path in computed_directory.iterdir()},
                {path.name: path.read_bytes() for path in ordinary_directory.iterdir()},
            )

    def test_three_segment_resume_is_byte_equivalent_and_authenticates_fragments(self) -> None:
        selected = market()
        day = datetime.fromtimestamp(START_S, UTC).date()
        coverage_start = datetime.fromtimestamp(START_S, UTC)
        cutoff = datetime.fromtimestamp(START_S + 3_600, UTC)
        retrieved_at_ns = provenance().retrieved_at_ns
        events = decode_rows(pmxt_rows(), "fixture")
        discovery = OfficialDiscovery((selected,), (provenance("gamma"),))
        shared = (provenance(),)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            direct_stage = root / "direct-stage"
            direct_item = stage_market(selected, events, direct_stage)
            with patch(
                "canonical_data.pipeline.Pipeline._available_memory_bytes",
                return_value=16_000_000_000,
            ):
                compute_staged_partition(
                    Asset.DOGE,
                    day,
                    root / "direct-work",
                    root / "direct-output",
                    cutoff,
                    discovery,
                    (direct_item,),
                    direct_stage,
                    shared,
                    START_S * 1_000_000_000,
                )

                segment_parent = root / "segments"
                for index in range(3):
                    segment_root = segment_parent / f"segment-{index}"
                    starts = day_segment_starts(day, coverage_start, cutoff, index, 3)
                    items = []
                    markets = []
                    official = []
                    shared_provenance = []
                    if starts:
                        source_stage = root / f"source-stage-{index}"
                        staged_item = stage_market(selected, events, source_stage)
                        items = [
                            _copy_segment_staged_market(
                                staged_item, Asset.DOGE, segment_root
                            )
                        ]
                        markets = [asdict(selected)]
                        official = [asdict(provenance("gamma"))]
                        shared_provenance = [asdict(provenance())]
                    segment_root.mkdir(parents=True, exist_ok=True)
                    receipt = {
                        "schema_version": "1.0.0",
                        "dataset_id": "polymarket-1h-seven-v1",
                        "authority": "noncanonical authenticated causal fragment checkpoint",
                        "day": day.isoformat(),
                        "segment_index": index,
                        "segment_count": 3,
                        "market_starts": list(starts),
                        "assets": ["DOGE"],
                        "retrieved_at_ns": retrieved_at_ns,
                        "tool_commit": _tool_commit(),
                        "source_bytes": 0,
                        "metrics": {},
                        "asset_receipts": {
                            "DOGE": {
                                "markets": markets,
                                "official_provenance": official,
                                "preexisting_exclusions": [],
                                "shared_provenance": shared_provenance,
                                "staged_markets": items,
                            }
                        },
                        "files": _segment_file_inventory(segment_root),
                    }
                    _atomic_json(segment_root / "receipt.json", receipt)

                assemble_day_segments(
                    day,
                    root / "assembled-work",
                    root / "assembled-output",
                    segment_parent,
                    coverage_start,
                    cutoff,
                    (Asset.DOGE,),
                    3,
                    retrieved_at_ns,
                )

            direct = next((root / "direct-output").rglob("manifest.json")).parent
            assembled = next((root / "assembled-output").rglob("manifest.json")).parent
            self.assertEqual(
                {path.name: path.read_bytes() for path in direct.iterdir()},
                {path.name: path.read_bytes() for path in assembled.iterdir()},
            )

            event_fragment = next(segment_parent.rglob("*.events.parquet"))
            event_fragment.write_bytes(b"substituted")
            with self.assertRaisesRegex(SourceError, "receipt digest mismatch"):
                assemble_day_segments(
                    day,
                    root / "tampered-work",
                    root / "tampered-output",
                    segment_parent,
                    coverage_start,
                    cutoff,
                    (Asset.DOGE,),
                    3,
                    retrieved_at_ns,
                )

    def test_shared_spool_breaker_reports_exact_capacity_figures(self) -> None:
        spool = Mock()
        spool.storage_bytes.return_value = 23_456
        usage = Mock(total=72_000_000_000, free=3_999_999_999)
        with (
            patch("scripts.run_backfill.shutil.disk_usage", return_value=usage),
            self.assertRaisesRegex(
                ResourceLimitError,
                "free_bytes=3999999999.*required_free_bytes=4000000000.*"
                "spool_bytes=23456.*completed_sources=26.*source_bytes=12428536165.*"
                "disk_total_bytes=72000000000",
            ),
        ):
            _verify_shared_disk_margin(Path("shared"), spool, 26, 12_428_536_165)

    def test_control_plane_digest_is_checkout_newline_independent(self) -> None:
        expected = hashlib.sha256(b"only.txt\0alpha\nbeta\n\0").hexdigest()
        with (
            patch("scripts.actions_backend.subprocess.check_output", return_value="only.txt\0"),
            patch.object(Path, "read_bytes", return_value=b"alpha\r\nbeta\r\n"),
        ):
            self.assertEqual(_control_plane_digest(), expected)

    def test_github_request_retries_tls_transport_failure(self) -> None:
        tls_failure = urllib.error.URLError("certificate verify failed")
        with (
            patch(
                "scripts.actions_backend.urllib.request.urlopen",
                side_effect=(tls_failure, FakeResponse(b"{}")),
            ) as urlopen,
            patch("scripts.actions_backend.time.sleep") as sleep,
        ):
            self.assertEqual(_request("https://api.github.test/releases"), b"{}")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_child_failure_surfaces_captured_diagnostics(self) -> None:
        completed = subprocess.CompletedProcess(
            ["executor"], 7, stdout="child-out\n", stderr="child-error\n"
        )
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            redirect_stdout(stdout),
            redirect_stderr(stderr),
            self.assertRaisesRegex(RuntimeError, "exit code 7"),
        ):
            _raise_child_failure(completed)
        self.assertEqual(stdout.getvalue(), "child-out\n")
        self.assertEqual(stderr.getvalue(), "child-error\n")

    def test_acquisition_does_not_retry_limits_or_unexplained_404(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        missing = urllib.error.HTTPError(source.url, 404, "Not Found", Message(), None)
        for failure in (
            ResourceLimitError("cap"),
            SourceIdentityError("changed"),
            missing,
        ):
            with (
                patch(
                    "scripts.run_backfill.BoundedAcquirer.acquire", side_effect=failure
                ) as acquire,
                patch("scripts.run_backfill.time.sleep") as sleep,
                tempfile.TemporaryDirectory() as temporary,
            ):
                with self.assertRaises(type(failure)):
                    _acquire_with_retry(
                        source, Path(temporary), expected_identity=(123, '"stable"')
                    )
            acquire.assert_called_once()
            sleep.assert_not_called()

    def test_corrupt_pmxt_is_reacquired_only_under_stable_identity(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        identity = (123, '"stable"')
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            corrupt = root / "corrupt.parquet"
            corrupt.write_bytes(b"not parquet")
            valid = root / "valid.parquet"
            pq.write_table(pa.table({"value": [1]}), valid)
            attempts = 0

            def acquire(
                selected: SourceObject,
                *,
                expected_identity: tuple[int, str],
                validator: Callable[[Path], None],
            ) -> AcquiredObject:
                nonlocal attempts
                attempts += 1
                path = corrupt if attempts == 1 else valid
                validator(path)
                payload = path.read_bytes()
                return AcquiredObject(
                    selected,
                    path,
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                    expected_identity[1],
                )

            with (
                patch(
                    "scripts.run_backfill.BoundedAcquirer.acquire",
                    side_effect=acquire,
                ) as acquire_mock,
                patch("scripts.run_backfill.time.sleep") as sleep,
            ):
                result = _acquire_with_retry(
                    source, root, expected_identity=identity
                )
            self.assertEqual(result.path, valid)
            self.assertEqual(acquire_mock.call_count, 2)
            self.assertTrue(
                all(
                    call.kwargs["expected_identity"] == identity
                    for call in acquire_mock.call_args_list
                )
            )
            self.assertTrue(
                all(
                    call.kwargs["validator"] is _validate_pmxt_download
                    for call in acquire_mock.call_args_list
                )
            )
            sleep.assert_called_once_with(2)

    def test_real_acquisition_rejects_incomplete_parquet_and_persists_identity(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "fixture.parquet"
            pq.write_table(pa.table({"value": [1]}), fixture)
            valid_payload = fixture.read_bytes()
            identity = (len(valid_payload), '"stable"')
            head = FakeResponse(b"")
            head.headers.replace_header("Content-Length", str(identity[0]))
            head.headers["ETag"] = identity[1]
            incomplete = FakeResponse(valid_payload[:-4])
            incomplete.headers.replace_header("Content-Length", str(identity[0]))
            incomplete.headers["ETag"] = identity[1]
            complete = FakeResponse(valid_payload)
            complete.headers["ETag"] = identity[1]
            work = root / "work"
            with (
                patch(
                    "canonical_data.acquire.urllib.request.urlopen",
                    side_effect=(head, incomplete, complete),
                ) as urlopen,
                patch("scripts.run_backfill.time.sleep") as sleep,
            ):
                acquired = _acquire_with_retry(source, work)
            self.assertEqual(acquired.etag, identity[1])
            self.assertEqual(acquired.byte_length, identity[0])
            self.assertEqual(urlopen.call_count, 3)
            sleep.assert_called_once_with(2)
            with patch(
                "canonical_data.acquire.urllib.request.urlopen"
            ) as unexpected_download:
                resumed = BoundedAcquirer(work, 1_000_000, 0).acquire(
                    source,
                    expected_identity=identity,
                    validator=_validate_pmxt_download,
                )
            unexpected_download.assert_not_called()
            self.assertEqual(resumed, acquired)

    def test_real_acquisition_does_not_retry_changed_get_identity(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        head = FakeResponse(b"")
        head.headers.replace_header("Content-Length", "123")
        head.headers["ETag"] = '"stable"'
        changed = FakeResponse(b"changed")
        changed.headers["ETag"] = '"substituted"'
        with (
            patch(
                "canonical_data.acquire.urllib.request.urlopen",
                side_effect=(head, changed),
            ) as urlopen,
            patch("scripts.run_backfill.time.sleep") as sleep,
            tempfile.TemporaryDirectory() as temporary,
            self.assertRaisesRegex(SourceIdentityError, "identity changed"),
        ):
            _acquire_with_retry(source, Path(temporary))
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_not_called()

    def test_explicit_market_starts_must_be_one_day_and_1h_aligned(self) -> None:
        day = datetime.fromtimestamp(START_S, UTC).date()
        cutoff = datetime.fromtimestamp(START_S + 3_600, UTC)
        coverage_start = datetime.fromtimestamp(START_S, UTC)
        self.assertEqual(_market_starts(day, coverage_start, cutoff, (START_S,)), [START_S])
        with self.assertRaisesRegex(SourceError, "aligned"):
            _market_starts(day, coverage_start, cutoff, (START_S + 1,))

    def test_remote_durable_partitions_are_zero_times_and_unfinished_once(self) -> None:
        authority = self.authority()
        durable = "BTC/1h/2026-04-05"
        inventory = {durable: self.assets(durable)}
        plan = unfinished_plan(inventory, authority)
        ids = [str(item["partition_id"]) for item in plan]
        self.assertNotIn(durable, ids)
        self.assertEqual(len(ids), 13)
        self.assertEqual(len(ids), len(set(ids)))
        days = day_plan(plan)
        self.assertEqual(
            days,
            [
                {
                    "day": "2026-04-05",
                    "release_group": "polymarket-1h-seven-v1-2026-04-a",
                },
                {
                    "day": "2026-04-06",
                    "release_group": "polymarket-1h-seven-v1-2026-04-a",
                },
            ],
        )

    def test_day_plan_round_robins_release_groups(self) -> None:
        plan = [
            {
                "partition_id": f"BTC/1h/{day}",
                "release_group": release_group,
            }
            for day, release_group in (
                ("2026-04-01", "release-a"),
                ("2026-04-02", "release-a"),
                ("2026-04-16", "release-b"),
                ("2026-04-17", "release-b"),
            )
        ]
        self.assertEqual(
            day_plan(plan),
            [
                {"day": "2026-04-01", "release_group": "release-a"},
                {"day": "2026-04-16", "release_group": "release-b"},
                {"day": "2026-04-02", "release_group": "release-a"},
                {"day": "2026-04-17", "release_group": "release-b"},
            ],
        )

    def test_partial_resumes_while_unsafe_remote_state_fails_closed(self) -> None:
        authority = self.authority()
        partition = "BTC/1h/2026-04-05"
        assets = self.assets(partition)
        self.assertEqual(verified_partitions({partition: assets}), {partition})
        partial_inventory = {partition: assets[:-1]}
        self.assertEqual(inventory_anomalies(partial_inventory, authority)["partial"], [partition])
        self.assertIn(
            partition,
            {str(item["partition_id"]) for item in unfinished_plan(partial_inventory, authority)},
        )
        divergent = [*assets, replace(assets[0], digest="b" * 64)]
        self.assertTrue(inventory_anomalies({partition: divergent}, authority)["divergent"])
        duplicate = [*assets, assets[0]]
        self.assertTrue(inventory_anomalies({partition: duplicate}, authority)["duplicate"])
        outside = "BTC/1h/2026-04-07"
        self.assertEqual(
            inventory_anomalies({outside: self.assets(outside)}, authority)["out_of_plan"],
            [outside],
        )

    def test_actions_discovery_is_gamma_first_bounded_and_reuses_source_probes(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        newest = START_S + 7_200
        oldest = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(newest, UTC),
            canary_search_end=datetime.fromtimestamp(oldest, UTC),
            canary_step_minutes=60,
        )
        events: list[str] = []

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                events.append(f"gamma:{start}:{asset.value}")
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", f"https://example.test/{asset.value}/{start}"

        def source_identity(source: SourceObject) -> tuple[int, str]:
            events.append(f"source:{source.url}")
            return 100, '"stable"'

        with patch("scripts.actions_backend.GammaClient", FakeGamma):
            result = qualify_canary_candidates(authority, source_identity)
        self.assertEqual([item.start for item in result.candidates], [newest])
        self.assertEqual(result.gamma_requests, 7)
        self.assertLessEqual(result.gamma_requests, CANARY_MAX_GAMMA_REQUESTS)
        self.assertEqual(result.source_requests, 2)
        self.assertLessEqual(result.source_requests, CANARY_MAX_SOURCE_OBJECTS)
        self.assertTrue(all(item.startswith("gamma:") for item in events[:7]))
        self.assertEqual(
            [item.removeprefix("source:") for item in events if item.startswith("source:")],
            [
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T20.parquet",
                "https://r2v2.pmxt.dev/polymarket_orderbook_2026-04-13T21.parquet",
            ],
        )

    def test_adaptive_discovery_queries_only_uncovered_assets(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(start, UTC),
            canary_search_end=datetime.fromtimestamp(start, UTC),
        )
        requested: list[Asset] = []

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, candidate: int) -> tuple[object, bytes, str]:
                requested.append(asset)
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{candidate}",
                    condition_id=f"0x{candidate + tuple(Asset).index(asset):064x}",
                    market_start_ns=candidate * 1_000_000_000,
                    market_end_ns=(candidate + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", f"https://example.test/{asset.value}/{candidate}"

        with patch("scripts.actions_backend.GammaClient", FakeGamma):
            result = qualify_canary_candidates(
                authority,
                Mock(return_value=(100, '"stable"')),
                assets=(Asset.ETH, Asset.HYPE),
            )
        self.assertEqual(requested, [Asset.ETH, Asset.HYPE])
        self.assertEqual(result.gamma_requests, 2)
        self.assertEqual([asset for asset, _ in result.candidates[0].markets], requested)

    def test_market_authority_projection_ignores_payload_noise_but_binds_semantics(self) -> None:
        selected = market(Asset.BTC)
        first_payload = {"market": selected.market_id, "updatedAt": "one", "volume": 10}
        second_payload = {"volume": 99, "updatedAt": "two", "market": selected.market_id}
        projection = _market_authority_projection(selected)
        digest = hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
        self.assertNotEqual(
            hashlib.sha256(canonical_json_bytes(first_payload)).hexdigest(),
            hashlib.sha256(canonical_json_bytes(second_payload)).hexdigest(),
        )
        self.assertEqual(digest, hashlib.sha256(canonical_json_bytes(projection)).hexdigest())
        changed_rules = _market_authority_projection(
            replace(selected, rules_text_sha256="f" * 64)
        )
        self.assertNotEqual(
            digest,
            hashlib.sha256(canonical_json_bytes(changed_rules)).hexdigest(),
        )

    def test_multi_window_execution_acquires_one_shared_source_bundle(self) -> None:
        day = datetime.fromtimestamp(START_S, UTC).date()
        starts = (START_S, START_S + 3_600)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            discoveries = {
                asset: OfficialDiscovery((market(asset),), ()) for asset in tuple(Asset)[:2]
            }
            provenance = {asset: () for asset in discoveries}
            staged = {asset: () for asset in discoveries}
            stage_roots = {asset: root / asset.value for asset in discoveries}
            with (
                patch(
                    "scripts.run_backfill.prepare_staged_day",
                    return_value=(
                        discoveries,
                        provenance,
                        staged,
                        stage_roots,
                        123,
                        {},
                    ),
                ) as prepare,
                patch("scripts.run_backfill.run_staged_partition", return_value={}) as partition,
                patch("scripts.run_backfill.shutil.rmtree"),
            ):
                run_day(
                    day,
                    root / "work",
                    root / "ledger.json",
                    datetime.fromtimestamp(START_S, UTC),
                    datetime.fromtimestamp(START_S + 1_800, UTC),
                    tuple(discoveries),
                    starts,
                )
        prepare.assert_called_once()
        self.assertEqual(prepare.call_args.args[5], starts)
        self.assertEqual([call.args[9] for call in partition.call_args_list], [123, 0])

    def test_staged_partition_rejects_an_empty_or_incomplete_market_inventory(self) -> None:
        day = datetime.fromtimestamp(START_S, UTC).date()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with self.assertRaisesRegex(SourceError, "does not match official discovery"):
                run_staged_partition(
                    Asset.DOGE,
                    day,
                    root,
                    root / "ledger.json",
                    datetime.fromtimestamp(START_S + 1_800, UTC),
                    OfficialDiscovery((market(),), ()),
                    (),
                    root / "stage",
                    (),
                )

    def test_actions_discovery_fails_closed_on_unresolved_gamma(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class UnresolvedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                raise UnresolvedMarketError(f"{asset.value}-{start}", "market", "condition")

        source_probe = Mock(return_value=(100, '"stable"'))
        with (
            patch("scripts.actions_backend.GammaClient", UnresolvedGamma),
            self.assertRaisesRegex(RuntimeError, "no authoritative 1h candidates"),
        ):
            qualify_canary_candidates(authority, source_probe)
        source_probe.assert_not_called()

    def test_actions_discovery_fails_closed_on_unexplained_source_absence(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class FakeGamma:
            calls = 0

            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                self.__class__.calls += 1
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        def missing(source: SourceObject) -> tuple[int, str]:
            raise RuntimeError(f"catalog-listed PMXT canary source is missing: {source.url}")

        with (
            patch("scripts.actions_backend.GammaClient", FakeGamma),
            self.assertRaisesRegex(RuntimeError, "catalog-listed PMXT canary source is missing"),
        ):
            qualify_canary_candidates(authority, missing)
        self.assertEqual(FakeGamma.calls, 7)

    def test_actions_discovery_rejects_mismatched_or_reused_asset_identity(self) -> None:
        authority = self.authority(date(2026, 4, 13), date(2026, 4, 13))
        canary_start = START_S + 3_600
        authority = replace(
            authority,
            canary_search_start=datetime.fromtimestamp(canary_start, UTC),
            canary_search_end=datetime.fromtimestamp(canary_start, UTC),
        )

        class MismatchedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(Asset.BTC),
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        class ReusedGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        for gamma, message in (
            (MismatchedGamma, "violates exact 1h identity"),
            (ReusedGamma, "reused an identity"),
        ):
            source_probe = Mock(return_value=(100, '"stable"'))
            with (
                self.subTest(gamma=gamma.__name__),
                patch("scripts.actions_backend.GammaClient", gamma),
                self.assertRaisesRegex(RuntimeError, message),
            ):
                qualify_canary_candidates(authority, source_probe)
            source_probe.assert_not_called()

        class CrossRoundGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_id="prior-market",
                    condition_id="0x" + "f" * 64,
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        source_probe = Mock(return_value=(100, '"stable"'))
        with (
            patch("scripts.actions_backend.GammaClient", CrossRoundGamma),
            self.assertRaisesRegex(RuntimeError, "reused an identity"),
        ):
            qualify_canary_candidates(
                authority,
                source_probe,
                assets=(Asset.BTC,),
                prior_market_ids=frozenset(("prior-market",)),
                prior_conditions=frozenset(("0x" + "f" * 64,)),
            )
        source_probe.assert_not_called()

    def test_canary_source_probe_captures_object_identity(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        response = FakeResponse(b"")
        response.headers.replace_header("Content-Length", "123")
        response.headers["ETag"] = '"stable"'
        with patch("scripts.actions_backend.urllib.request.urlopen", return_value=response):
            self.assertEqual(_pmxt_source_identity(source), (123, '"stable"'))
        changed = FakeResponse(b"")
        changed.headers.replace_header("Content-Length", "0")
        with (
            patch("scripts.actions_backend.urllib.request.urlopen", return_value=changed),
            self.assertRaisesRegex(RuntimeError, "lacks object identity"),
        ):
            _pmxt_source_identity(source)

    def test_child_acquisition_must_match_source_qualified_object(self) -> None:
        source = SourceObject("pmxt_v2", "https://example.test/hour.parquet")
        expected = {source.url: (123, '"stable"')}
        _validate_expected_source_identity(source, 123, '"stable"', expected)
        with self.assertRaisesRegex(SourceError, "source-qualified identity"):
            _validate_expected_source_identity(source, 124, '"changed"', expected)

    def test_child_discovery_must_match_source_qualified_identity(self) -> None:
        discovered = market(Asset.BTC)
        discoveries = {Asset.BTC: OfficialDiscovery((discovered,), ())}
        expected = {
            Asset.BTC: frozenset(
                (
                    (
                        discovered.condition_id,
                        frozenset((discovered.token_up, discovered.token_down)),
                    ),
                )
            )
        }
        _validate_expected_market_identities(discoveries, expected)
        expected[Asset.BTC] = frozenset(
            (("0x" + "b" * 64, frozenset((discovered.token_up, discovered.token_down))),)
        )
        with self.assertRaisesRegex(SourceError, "source-qualified canary identity"):
            _validate_expected_market_identities(discoveries, expected)

    def test_canary_candidate_outside_catalog_is_rejected_before_probe(self) -> None:
        authority = replace(
            self.authority(),
            canary_search_start=datetime(2026, 8, 14, 23, tzinfo=UTC),
            canary_search_end=datetime(2026, 8, 14, 23, tzinfo=UTC),
        )
        source_probe = Mock(return_value=True)

        class FakeGamma:
            def __init__(self, *args: object, **kwargs: object) -> None:
                pass

            def fetch_market(self, asset: Asset, start: int) -> tuple[object, bytes, str]:
                fixture = replace(
                    market(asset),
                    market_id=f"{asset.value}-{start}",
                    condition_id=f"0x{start + tuple(Asset).index(asset):064x}",
                    market_start_ns=start * 1_000_000_000,
                    market_end_ns=(start + 3_600) * 1_000_000_000,
                )
                return fixture, b"payload", "https://example.test/gamma"

        with (
            patch("scripts.actions_backend.GammaClient", FakeGamma),
            self.assertRaisesRegex(SourceError, "authoritative catalog"),
        ):
            qualify_canary_candidates(authority, source_probe)
        source_probe.assert_not_called()

    def test_canary_cover_prefers_one_common_window(self) -> None:
        assets = frozenset(Asset)
        self.assertEqual(
            minimum_canary_cover({3: assets, 2: frozenset((Asset.BTC,)), 1: assets}),
            (3,),
        )

    def test_canary_cover_uses_minimum_windows_and_ignores_excluded_assets(self) -> None:
        first = frozenset(tuple(Asset)[:4])
        second = frozenset(tuple(Asset)[4:])
        self.assertEqual(minimum_canary_cover({3: first, 2: second, 1: first}), (3, 2))
        with self.assertRaisesRegex(RuntimeError, "no usable evidence cover"):
            minimum_canary_cover({3: frozenset(tuple(Asset)[:-1])})

    def test_canary_cover_ignores_full_day_empty_starts_with_deterministic_tie(
        self,
    ) -> None:
        usable = {start: frozenset[Asset]() for start in range(10_000, 10_096)}
        usable[9_000] = frozenset(tuple(Asset)[:4])
        usable[8_000] = frozenset(tuple(Asset)[4:])
        usable[7_000] = frozenset(tuple(Asset)[4:])
        self.assertEqual(minimum_canary_cover(usable), (9_000, 8_000))

    def test_remote_exclusion_is_a_disposition_but_not_usable_coverage(self) -> None:
        accepted = replace(market(Asset.BTC), market_id="accepted")
        excluded = replace(
            market(Asset.BTC),
            market_id="excluded",
            market_start_ns=accepted.market_start_ns + 3_600_000_000_000,
            market_end_ns=accepted.market_end_ns + 3_600_000_000_000,
        )
        accepted_rows = [{**_market_authority_projection(accepted), "quality_tier": "TIER_A"}]
        excluded_rows = [
            {
                "market_id": excluded.market_id,
                "evidence_json": json.dumps({"condition_id": excluded.condition_id}),
            }
        ]
        self.assertEqual(
            _verify_canary_dispositions(
                accepted_rows,
                excluded_rows,
                {accepted.market_id: accepted, excluded.market_id: excluded},
            ),
            [accepted.market_start_ns // 1_000_000_000],
        )

    def test_new_1h_repository_imports_no_prior_canary_authority(self) -> None:
        self.assertFalse(Path("config/canary-prior-evidence.json").exists())
        self.assertFalse(Path("config/canary-receipt.json").exists())

    def test_receipt_recomputes_proof_bound_minimum_cover(self) -> None:
        authority = self.authority()
        first_assets = tuple(Asset)[:4]
        usable = {
            asset.value: [10_800] if asset in first_assets else [7_200]
            for asset in authority.assets
        }

        def binding(asset: Asset) -> dict[str, object]:
            start = usable[asset.value][0]
            selected = replace(
                market(asset),
                event_id=f"event-{asset.value}-{start}",
                market_id=f"{asset.value}-{start}",
                condition_id="0x" + f"{tuple(Asset).index(asset):064x}",
                token_up=str(tuple(Asset).index(asset) * 2 + 1),
                token_down=str(tuple(Asset).index(asset) * 2 + 2),
                market_start_ns=start * 1_000_000_000,
                market_end_ns=(start + 3_600) * 1_000_000_000,
            )
            projection = _market_authority_projection(selected)
            return {
                "authority_projection": projection,
                "authority_sha256": hashlib.sha256(
                    canonical_json_bytes(projection)
                ).hexdigest(),
            }

        receipt = {
            "release_tags": ["canary-proof"],
            "usable_market_starts_by_asset": usable,
            "remote_proofs": {
                asset.value: {
                    "accepted_market_starts": usable[asset.value],
                    "manifest_sha256": "a" * 64,
                    "quality": "TIER_A",
                    "release_tag": "canary-proof",
                    "tool_commit": "b" * 40,
                    "accepted_market_bindings": [binding(asset)],
                }
                for asset in authority.assets
            },
            "selected_market_starts": [10_800, 7_200],
            "asset_market_starts": {
                asset.value: usable[asset.value][0] for asset in authority.assets
            },
        }
        _validate_receipt_coverage(receipt, authority, [10_800, 7_200, 3_600])
        altered = json.loads(json.dumps(receipt))
        altered["remote_proofs"]["BTC"]["accepted_market_bindings"][0][
            "authority_projection"
        ][
            "condition_id"
        ] = "not-a-condition"
        with self.assertRaisesRegex(RuntimeError, "market identity binding"):
            _validate_receipt_coverage(altered, authority, [10_800, 7_200, 3_600])
        reused = json.loads(json.dumps(receipt))
        reused["remote_proofs"]["ETH"]["accepted_market_bindings"][0][
            "authority_projection"
        ][
            "condition_id"
        ] = reused["remote_proofs"]["BTC"]["accepted_market_bindings"][0][
            "authority_projection"
        ][
            "condition_id"
        ]
        with self.assertRaisesRegex(RuntimeError, "market identity binding"):
            _validate_receipt_coverage(reused, authority, [10_800, 7_200, 3_600])
        receipt["selected_market_starts"] = [10_800, 7_200, 3_600]
        with self.assertRaisesRegex(RuntimeError, "exact usable minimum cover"):
            _validate_receipt_coverage(receipt, authority, [10_800, 7_200, 3_600])

    def test_production_is_locked_until_fresh_1h_canary_receipt(self) -> None:
        authority = load_authority()
        receipt_path = Mock()
        receipt_path.exists.return_value = False
        with patch("scripts.actions_backend.CANARY_RECEIPT_PATH", receipt_path):
            with self.assertRaisesRegex(RuntimeError, "locked until the one canary"):
                _require_canary_receipt(authority)

    def test_canary_candidate_search_is_bounded(self) -> None:
        authority = replace(
            self.authority(),
            canary_search_start=datetime(2026, 4, 6, tzinfo=UTC),
            canary_search_end=datetime(2026, 4, 4, tzinfo=UTC),
            canary_step_minutes=60,
        )
        with self.assertRaisesRegex(RuntimeError, "finite cap"):
            _candidate_starts(authority)

    def test_adaptive_round_refuses_work_after_wall_deadline(self) -> None:
        start = 1_786_132_800
        fixture = replace(
            market(Asset.BTC),
            market_id=f"BTC-{start}",
            condition_id=f"0x{start:064x}",
            market_start_ns=start * 1_000_000_000,
            market_end_ns=(start + 3_600) * 1_000_000_000,
        )
        qualification = CanaryQualification(
            (
                QualifiedCandidate(
                    start,
                    ((Asset.BTC, fixture),),
                    ((Asset.BTC, b"gamma", "https://example.test/gamma"),),
                ),
            ),
            (("https://r2v2.pmxt.dev/hour.parquet", 1, '"etag"'),),
            1,
            1,
        )
        with (
            patch("scripts.actions_backend.subprocess.run") as run,
            self.assertRaisesRegex(RuntimeError, "five-hour execution bound"),
        ):
            _execute_canary_round(
                self.authority(),
                qualification,
                (Asset.BTC,),
                "123",
                1,
                10_000_000_000,
                time.monotonic() - 1,
            )
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
