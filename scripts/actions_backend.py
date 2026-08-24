"""Bounded Actions control plane for the Polymarket 1h x 7 dataset."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq

from canonical_data.audit import canonical_json_bytes
from canonical_data.discovery import GammaClient, bind_gamma_market, hourly_slug
from canonical_data.errors import ConflictError, IdentityError, UnresolvedMarketError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import (
    PMXT_MISSING_OBJECT_URLS,
    PMXT_OBJECT_COVERAGE_CUTOFF,
    PMXT_OBJECT_COVERAGE_START,
    PMXT_VALIDATION_COVERAGE_START,
    SourceObject,
    pmxt_hourly_objects,
)
from canonical_data.manifest import hash_file, verify_manifest
from canonical_data.models import Asset, Market
from canonical_data.planner import build_backfill_plan, release_bucket
from canonical_data.quality import classify
from canonical_data.release import GitHubReleaseBackend, Publisher
from scripts.run_backfill import (
    DATASET_RELEASE_PREFIX,
    REPOSITORY,
    _fetch_gamma,
    _tool_commit,
)

API = f"https://api.github.com/repos/{REPOSITORY}"
CANARY_RELEASE_PREFIX = "polymarket-1h-seven-canary-v1"
AUTHORITY_PATH = Path("config/production-plan.json")
CANARY_RECEIPT_PATH = Path("config/canary-receipt.json")
CANARY_PRIOR_EVIDENCE_PATH = Path("config/canary-prior-evidence.json")
LEDGER_PATH = Path("config/backfill-ledger.json")
CERTIFICATION_PATH = Path("docs/final-certification.json")
CANARY_MAX_CANDIDATES = 24
CANARY_MAX_GAMMA_REQUESTS = CANARY_MAX_CANDIDATES * len(tuple(Asset))
CANARY_MAX_SOURCE_OBJECTS = 25
CANARY_MAX_SOURCE_BYTES = 20_000_000_000
CANARY_MAX_ROUNDS = 4
CANARY_MAX_CANDIDATES_TOTAL = CANARY_MAX_CANDIDATES * CANARY_MAX_ROUNDS
CANARY_PRIOR_CANDIDATE_COUNTS: frozenset[int] = frozenset()
CANARY_PRIOR_GAMMA_REQUESTS = 0
CANARY_MAX_GAMMA_REQUESTS_TOTAL = (
    CANARY_MAX_CANDIDATES_TOTAL * len(tuple(Asset)) + CANARY_PRIOR_GAMMA_REQUESTS
)
CANARY_MAX_SOURCE_OBJECTS_TOTAL = CANARY_MAX_SOURCE_OBJECTS * CANARY_MAX_ROUNDS
CANARY_MAX_SOURCE_BYTES_TOTAL = CANARY_MAX_SOURCE_BYTES * CANARY_MAX_ROUNDS
CANARY_MAX_WALL_SECONDS = 18_000
EXPECTED_FILES = {
    "book-200ms.parquet",
    "book-events.parquet",
    "exclusions.parquet",
    "manifest.json",
    "markets.parquet",
    "underlying.parquet",
}
MARKET_AUTHORITY_FIELDS = {
    "asset",
    "venue",
    "timeframe",
    "event_id",
    "market_id",
    "condition_id",
    "token_up",
    "token_down",
    "market_start_ns",
    "market_end_ns",
    "rules_text_sha256",
    "resolution_source_url",
    "official_outcome",
    "official_resolution_ts_ns",
}
ASSET_PATTERN = re.compile(
    r"^(BTC|ETH|SOL|XRP|DOGE|BNB|HYPE)--1h--"
    r"(\d{4}-\d{2}-\d{2})--([0-9a-f]{64})--(.+)$"
)
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
TRANSFER_RETRY_DELAYS = (2, 8, 32)
DAY_SEGMENT_COUNT = 4


@dataclass(frozen=True)
class Authority:
    start: datetime
    cutoff: datetime
    assets: tuple[Asset, ...]
    canary_search_start: datetime
    canary_search_end: datetime
    canary_step_minutes: int
    canary_rounds: tuple[tuple[datetime, datetime, int], ...] = ()


@dataclass(frozen=True)
class QualifiedCandidate:
    start: int
    markets: tuple[tuple[Asset, Market], ...]
    payloads: tuple[tuple[Asset, bytes, str], ...]


@dataclass(frozen=True)
class CanaryQualification:
    candidates: tuple[QualifiedCandidate, ...]
    source_objects: tuple[tuple[str, int, str], ...]
    gamma_requests: int
    source_requests: int


@dataclass(frozen=True)
class RemoteAsset:
    name: str
    size: int
    url: str
    digest: str
    filename: str
    state: str = "uploaded"
    asset_id: str | None = None
    release_tag: str | None = None


def _request(url: str, accept: str = "application/vnd.github+json") -> bytes:
    token = os.environ.get("GITHUB_TOKEN", "")
    headers = {
        "Accept": accept,
        "User-Agent": "polymarket-1h-seven-actions/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return cast(bytes, response.read())
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            break
    assert last_error is not None
    raise last_error


def _json(url: str) -> Any:
    return json.loads(_request(url))


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise RuntimeError("authority timestamps must be UTC")
    return parsed.astimezone(UTC)


def _bounded_public_fetch(url: str, max_bytes: int = 25_000_000) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=120) as response:
        length = int(response.headers.get("Content-Length", "0"))
        if length > max_bytes:
            raise RuntimeError(f"public authority payload exceeds bound: {url}")
        payload = cast(bytes, response.read(max_bytes + 1))
    if len(payload) > max_bytes:
        raise RuntimeError(f"public authority payload exceeds bound: {url}")
    return payload


def audit_source_truth(authority: Authority) -> dict[str, Any]:
    """Reproduce the exact finite PMXT and official 1h market inventories."""
    key_pattern = re.compile(rb"polymarket_orderbook_(\d{4}-\d{2}-\d{2}T\d{2})\.parquet")
    page_pattern = re.compile(rb"Page <!-- -->(\d+)<!-- --> of <!-- -->(\d+)")
    catalog_keys: set[str] = set()
    total_pages: int | None = None
    page = 1
    while total_pages is None or page <= total_pages:
        if page > 100:
            raise RuntimeError("PMXT catalog exceeded bounded pagination")
        suffix = "" if page == 1 else f"?page={page}"
        url = f"https://archive.pmxt.dev/Polymarket/v2{suffix}"
        observed: set[str] = set()
        observed_page = 0
        observed_total = 0
        for attempt in range(5):
            payload = _bounded_public_fetch(url)
            marker = page_pattern.search(payload)
            if marker is not None:
                observed_page = int(marker.group(1))
                observed_total = int(marker.group(2))
            observed = {item.decode() for item in key_pattern.findall(payload)}
            if observed_page == page and observed_total > 0 and observed:
                break
            time.sleep(2**attempt)
        else:
            raise RuntimeError(f"PMXT catalog page {page} was incomplete after bounded retry")
        if total_pages is None:
            total_pages = observed_total
        if observed_total != total_pages:
            raise RuntimeError("PMXT catalog page count changed during audit")
        if catalog_keys & observed:
            raise RuntimeError(f"PMXT catalog page {page} overlaps an earlier page")
        catalog_keys.update(observed)
        page += 1
    expected_keys: set[str] = set()
    current = PMXT_OBJECT_COVERAGE_START
    missing_stamps = {
        url.removeprefix("https://r2v2.pmxt.dev/polymarket_orderbook_").removesuffix(
            ".parquet"
        )
        for url in PMXT_MISSING_OBJECT_URLS
    }
    while current < PMXT_OBJECT_COVERAGE_CUTOFF:
        stamp = current.strftime("%Y-%m-%dT%H")
        if stamp not in missing_stamps:
            expected_keys.add(stamp)
        current += timedelta(hours=1)
    if catalog_keys != expected_keys:
        missing = sorted(expected_keys - catalog_keys)[:10]
        unexpected = sorted(catalog_keys - expected_keys)[:10]
        raise RuntimeError(
            f"PMXT catalog conflicts with frozen authority: missing={missing} "
            f"unexpected={unexpected}"
        )
    catalog_digest = hashlib.sha256(
        canonical_json_bytes(sorted(catalog_keys))
    ).hexdigest()

    expected_starts = tuple(
        range(int(authority.start.timestamp()), int(authority.cutoff.timestamp()), 3_600)
    )
    gamma = GammaClient(fetch=_fetch_gamma)
    inventories: dict[str, dict[str, Any]] = {}
    seen_market_ids: set[str] = set()
    seen_conditions: set[str] = set()
    for asset in authority.assets:
        seed_payload, _ = gamma.fetch_slug_payload(asset, expected_starts[-1])
        seed = json.loads(seed_payload)
        if isinstance(seed, list):
            if len(seed) != 1:
                raise RuntimeError("Gamma seed lookup is not unique")
            seed = seed[0]
        series = seed.get("series") if isinstance(seed, dict) else None
        if not isinstance(series, list) or len(series) != 1 or not isinstance(series[0], dict):
            raise RuntimeError(f"Gamma hourly series identity is missing for {asset.value}")
        series_id = str(series[0].get("id", ""))
        if not series_id.isdigit():
            raise RuntimeError(f"Gamma hourly series id is invalid for {asset.value}")
        expected_slugs = {hourly_slug(asset, start): start for start in expected_starts}
        found: dict[int, Market] = {}
        excluded: dict[int, UnresolvedMarketError] = {}
        window_start = authority.start
        while window_start < authority.cutoff:
            window_end = min(window_start + timedelta(days=14), authority.cutoff)
            offset = 0
            while offset < 1_000:
                query = urllib.parse.urlencode(
                    {
                        "series_id": series_id,
                        "closed": "true",
                        "end_date_min": window_start.isoformat().replace("+00:00", "Z"),
                        "end_date_max": window_end.isoformat().replace("+00:00", "Z"),
                        "limit": 500,
                        "offset": offset,
                    }
                )
                payload = _fetch_gamma(
                    f"https://gamma-api.polymarket.com/events?{query}", 50_000_000
                )
                events = json.loads(payload)
                if not isinstance(events, list):
                    raise RuntimeError("Gamma series inventory is not a list")
                for event in events:
                    if not isinstance(event, dict):
                        raise RuntimeError("Gamma series inventory contains a non-object")
                    markets = event.get("markets")
                    raw = markets[0] if isinstance(markets, list) and len(markets) == 1 else None
                    slug = raw.get("slug") if isinstance(raw, dict) else None
                    if slug not in expected_slugs:
                        continue
                    market = bind_gamma_market(event, canonical_json_bytes(event))
                    start = expected_slugs[cast(str, slug)]
                    if market.market_start_ns != start * 1_000_000_000:
                        raise RuntimeError("Gamma series inventory has divergent time identity")
                    prior = found.get(start)
                    if prior is not None:
                        if prior != market:
                            raise RuntimeError("Gamma series inventory has divergent identity")
                        continue
                    if (
                        market.market_id in seen_market_ids
                        or market.condition_id in seen_conditions
                    ):
                        raise RuntimeError("Gamma series inventory reuses a market identity")
                    found[start] = market
                    seen_market_ids.add(market.market_id)
                    seen_conditions.add(market.condition_id)
                if not events:
                    break
                offset += len(events)
            else:
                raise RuntimeError("Gamma series inventory exceeded bounded window pagination")
            window_start = window_end
        # Gamma series tags are an efficient inventory index but are not themselves
        # the slug authority. Reconcile any index omissions against the exact,
        # deterministic official slug endpoint before declaring an unsupported hour.
        for start in sorted(set(expected_starts) - set(found)):
            try:
                market, _, _ = gamma.fetch_market(asset, start)
            except UnresolvedMarketError as exc:
                if exc.slug != hourly_slug(asset, start):
                    raise RuntimeError("Gamma unresolved slug has divergent identity") from exc
                if exc.market_id in seen_market_ids or exc.condition_id in seen_conditions:
                    raise RuntimeError("Gamma unresolved market reuses an identity") from exc
                excluded[start] = exc
                seen_market_ids.add(exc.market_id)
                seen_conditions.add(exc.condition_id)
                continue
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    continue
                raise
            if market.market_start_ns != start * 1_000_000_000:
                raise RuntimeError("Gamma slug reconciliation has divergent time identity")
            if market.market_id in seen_market_ids or market.condition_id in seen_conditions:
                raise RuntimeError("Gamma slug reconciliation reuses a market identity")
            found[start] = market
            seen_market_ids.add(market.market_id)
            seen_conditions.add(market.condition_id)
        dispositions = set(found) | set(excluded)
        if dispositions != set(expected_starts):
            missing_starts = sorted(set(expected_starts) - dispositions)[:10]
            raise RuntimeError(
                f"{asset.value} cannot support the exact intended 1h interval; "
                f"missing={missing_starts}"
            )
        inventories[asset.value] = {
            "series_id": series_id,
            "markets": len(dispositions),
            "tier_a_markets": len(found),
            "excluded_unresolved_markets": len(excluded),
            "first_start": expected_starts[0],
            "last_start": expected_starts[-1],
        }
    plan = _full_plan(authority)
    return {
        "pmxt_objects": len(catalog_keys),
        "pmxt_catalog_sha256": catalog_digest,
        "pmxt_first_object": min(catalog_keys),
        "pmxt_last_object": max(catalog_keys),
        "pmxt_missing_hours": sorted(missing_stamps),
        "causal_warmup_seconds": 3_600,
        "market_duration_seconds": 3_600,
        "market_starts_per_asset": len(expected_starts),
        "market_inventories": inventories,
        "finite_partitions": len(plan),
    }


def load_authority(path: Path = AUTHORITY_PATH) -> Authority:
    raw = json.loads(path.read_bytes())
    expected_assets = tuple(Asset)
    assets = tuple(Asset(item) for item in raw["partition"]["assets"])
    if raw.get("dataset_id") != "polymarket-1h-seven-v1":
        raise RuntimeError("production plan has the wrong dataset identity")
    if raw["partition"].get("timeframe") != "1h" or assets != expected_assets:
        raise RuntimeError("production plan is not exactly the frozen 1h x 7 scope")
    if raw["publication"].get("release_prefix") != DATASET_RELEASE_PREFIX:
        raise RuntimeError("production release namespace is not frozen")
    if raw["publication"].get("canary_release_prefix") != CANARY_RELEASE_PREFIX:
        raise RuntimeError("canary release namespace is not frozen")
    canary = raw["canary"]
    search = canary["search"]
    limits = canary["limits"]
    if limits != {
        "max_rounds": CANARY_MAX_ROUNDS,
        "max_candidates_per_round": CANARY_MAX_CANDIDATES,
        "max_candidates_total": CANARY_MAX_CANDIDATES_TOTAL,
        "max_gamma_requests_total": CANARY_MAX_GAMMA_REQUESTS_TOTAL,
        "max_source_objects_total": CANARY_MAX_SOURCE_OBJECTS_TOTAL,
        "max_source_transfer_bytes_total": CANARY_MAX_SOURCE_BYTES_TOTAL,
        "max_wall_seconds": CANARY_MAX_WALL_SECONDS,
    }:
        raise RuntimeError("canary discovery limits are not frozen")
    raw_rounds = search.get("rounds")
    if not isinstance(raw_rounds, list) or len(raw_rounds) != CANARY_MAX_ROUNDS:
        raise RuntimeError("adaptive canary rounds are not exactly bounded")
    rounds = tuple(
        (
            _parse_utc(item["start"]),
            _parse_utc(item["end"]),
            int(item["step_minutes"]),
        )
        for item in raw_rounds
    )
    authority = Authority(
        _parse_utc(raw["coverage_start"]),
        _parse_utc(raw["release_cutoff"]),
        assets,
        rounds[0][0],
        rounds[0][1],
        rounds[0][2],
        rounds,
    )
    if authority.cutoff <= authority.start:
        raise RuntimeError("release cutoff must follow release start")
    if (
        authority.start < PMXT_VALIDATION_COVERAGE_START
        or authority.cutoff > PMXT_OBJECT_COVERAGE_CUTOFF
    ):
        raise RuntimeError("production coverage exceeds authoritative PMXT validation coverage")
    all_candidates: list[int] = []
    for start, end, step in rounds:
        if (
            start < authority.start
            or start >= authority.cutoff
            or end < authority.start
            or end >= authority.cutoff
            or start < end
            or step not in {15, 30, 60}
        ):
            raise RuntimeError("adaptive canary round exceeds frozen coverage or cadence")
        selected = replace(
            authority,
            canary_search_start=start,
            canary_search_end=end,
            canary_step_minutes=step,
        )
        candidates = _candidate_starts(selected)
        if len(candidates) != CANARY_MAX_CANDIDATES:
            raise RuntimeError("canary round does not use its exact candidate budget")
        if len({datetime.fromtimestamp(item, UTC).date() for item in candidates}) != 1:
            raise RuntimeError("canary round must fit one UTC source-reuse day")
        source_bundle = pmxt_hourly_objects(
            (min(candidates) - 3_600) * 1_000_000_000,
            (max(candidates) + 3_600) * 1_000_000_000,
        )
        if len(source_bundle) != CANARY_MAX_SOURCE_OBJECTS:
            raise RuntimeError("canary round does not fit its source-object budget")
        all_candidates.extend(candidates)
    if len(all_candidates) != len(set(all_candidates)):
        raise RuntimeError("adaptive canary rounds reuse a candidate identity")
    return authority


def _load_prior_canary_evidence(authority: Authority) -> dict[Asset, dict[str, Any]]:
    raw = json.loads(CANARY_PRIOR_EVIDENCE_PATH.read_bytes())
    proofs = raw.get("proofs")
    if (
        raw.get("schema_version") != "2.0.0"
        or raw.get("status") != "ACTIVE_SEMANTIC_REVALIDATION_REQUIRED"
        or not isinstance(proofs, list)
        or not proofs
    ):
        raise RuntimeError("prior canary evidence is not active semantic authority")
    result: dict[Asset, dict[str, Any]] = {}
    for proof in proofs:
        if not isinstance(proof, dict):
            raise RuntimeError("prior canary evidence entry is malformed")
        asset = Asset(str(proof.get("asset", "")))
        starts = proof.get("qualified_market_starts")
        partition = str(proof.get("partition_id", ""))
        release_tag = str(proof.get("release_tag", ""))
        if (
            asset in result
            or asset not in authority.assets
            or not isinstance(starts, list)
            or len(starts) not in CANARY_PRIOR_CANDIDATE_COUNTS
            or len(starts) != len(set(starts))
            or any(not isinstance(start, int) or start % 3_600 for start in starts)
            or any(
                not int(authority.start.timestamp()) <= start < int(authority.cutoff.timestamp())
                for start in starts
            )
            or len({datetime.fromtimestamp(start, UTC).date() for start in starts}) != 1
            or partition
            != f"{asset.value}/1h/{datetime.fromtimestamp(starts[0], UTC).date().isoformat()}"
            or re.match(r"polymarket-1h-seven-canary-v[4567]-", release_tag) is None
            or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("manifest_sha256", "")))
            is None
            or re.fullmatch(r"[0-9a-f]{40}", str(proof.get("tool_commit", ""))) is None
        ):
            raise RuntimeError("prior canary evidence entry violates frozen identity bounds")
        result[asset] = proof
    if sum(len(item["qualified_market_starts"]) for item in result.values()) != (
        CANARY_PRIOR_GAMMA_REQUESTS
    ):
        raise RuntimeError("prior canary Gamma revalidation budget is inconsistent")
    return result


def _full_plan(authority: Authority) -> list[dict[str, Any]]:
    final_day = (authority.cutoff - timedelta(microseconds=1)).date()
    return build_backfill_plan(authority.start.date(), final_day)


def _validate_source_truth_receipt(value: object, authority: Authority) -> None:
    if not isinstance(value, dict):
        raise RuntimeError("canary source-truth receipt is missing")
    inventories = value.get("market_inventories")
    market_starts = (
        int(authority.cutoff.timestamp()) - int(authority.start.timestamp())
    ) // 3_600
    if (
        value.get("pmxt_objects") != 2_835
        or value.get("pmxt_first_object") != "2026-04-13T19"
        or value.get("pmxt_last_object") != "2026-08-10T00"
        or value.get("pmxt_missing_hours")
        != ["2026-06-11T04", "2026-06-11T05", "2026-06-11T06"]
        or value.get("causal_warmup_seconds") != 3_600
        or value.get("market_duration_seconds") != 3_600
        or value.get("market_starts_per_asset") != market_starts
        or value.get("finite_partitions") != len(_full_plan(authority))
        or not isinstance(inventories, dict)
        or set(inventories) != {asset.value for asset in authority.assets}
        or any(
            not isinstance(inventories[asset.value], dict)
            or inventories[asset.value].get("markets") != market_starts
            or not str(inventories[asset.value].get("series_id", "")).isdigit()
            for asset in authority.assets
        )
    ):
        raise RuntimeError("canary source-truth receipt conflicts with frozen authority")


def _control_plane_digest() -> str:
    tracked = subprocess.check_output(["git", "ls-files", "-z"], encoding="utf-8").split("\0")
    excluded = {
        "config/backfill-ledger.json",
        "config/canary-receipt.json",
        "docs/final-certification.json",
    }
    digest = hashlib.sha256()
    for name in sorted(item for item in tracked if item and item not in excluded):
        path = Path(name)
        digest.update(name.encode())
        digest.update(b"\0")
        # Git's text checkout converts LF to CRLF on Windows. Bind the canonical
        # tracked content, including local edits, independently of that conversion.
        digest.update(path.read_bytes().replace(b"\r\n", b"\n"))
        digest.update(b"\0")
    return digest.hexdigest()


def _validate_receipt_coverage(
    receipt: dict[str, Any], authority: Authority, qualified_starts: list[int]
) -> None:
    usable_raw = receipt.get("usable_market_starts_by_asset")
    proofs_raw = receipt.get("remote_proofs")
    proof_release_tags = {
        str(tag)
        for name in ("release_tags", "prior_release_tags")
        for tag in receipt.get(name, [])
    }
    selected = receipt.get("selected_market_starts")
    asset_selection = receipt.get("asset_market_starts")
    expected_assets = {asset.value for asset in authority.assets}
    if (
        not isinstance(usable_raw, dict)
        or not isinstance(proofs_raw, dict)
        or not isinstance(selected, list)
        or not isinstance(asset_selection, dict)
        or set(usable_raw) != expected_assets
        or set(proofs_raw) != expected_assets
        or set(asset_selection) != expected_assets
    ):
        raise RuntimeError("canary receipt has malformed authenticated coverage")
    usable_by_start = {start: set[Asset]() for start in qualified_starts}
    seen_market_ids: set[str] = set()
    seen_conditions: set[str] = set()
    for asset in authority.assets:
        starts = usable_raw[asset.value]
        proof = proofs_raw[asset.value]
        bindings = proof.get("accepted_market_bindings") if isinstance(proof, dict) else None
        if (
            not isinstance(starts, list)
            or not starts
            or len(starts) != len(set(starts))
            or not set(starts).issubset(set(qualified_starts))
            or not isinstance(proof, dict)
            or proof.get("accepted_market_starts") != starts
            or proof.get("quality") != "TIER_A"
            or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("manifest_sha256", ""))) is None
            or not isinstance(proof.get("release_tag"), str)
            or not proof["release_tag"]
            or proof["release_tag"] not in proof_release_tags
            or re.fullmatch(r"[0-9a-f]{40}", str(proof.get("tool_commit", ""))) is None
            or not isinstance(bindings, list)
            or len(bindings) != len(starts)
        ):
            raise RuntimeError("canary receipt usable evidence is not proof-bound")
        binding_by_start: dict[int, dict[str, Any]] = {}
        for binding in bindings:
            if not isinstance(binding, dict):
                raise RuntimeError("canary receipt has malformed market identity binding")
            projection = binding.get("authority_projection")
            if not isinstance(projection, dict):
                raise RuntimeError("canary receipt has malformed market identity binding")
            market_start = int(projection.get("market_start_ns", -1)) // 1_000_000_000
            market_id = str(projection.get("market_id", ""))
            condition_id = str(projection.get("condition_id", ""))
            token_up = str(projection.get("token_up", ""))
            token_down = str(projection.get("token_down", ""))
            if (
                market_start in binding_by_start
                or market_start not in starts
                or set(projection) != MARKET_AUTHORITY_FIELDS
                or not str(projection.get("event_id", ""))
                or not market_id
                or market_id in seen_market_ids
                or condition_id in seen_conditions
                or re.fullmatch(r"0x[0-9a-fA-F]{64}", condition_id) is None
                or re.fullmatch(r"[0-9]+", token_up) is None
                or re.fullmatch(r"[0-9]+", token_down) is None
                or token_up == token_down
                or projection.get("asset") != asset.value
                or projection.get("venue") != "polymarket"
                or projection.get("timeframe") != "1h"
                or int(projection.get("market_start_ns", -1)) % 3_600_000_000_000
                != 0
                or int(projection.get("market_end_ns", 0))
                - int(projection.get("market_start_ns", 0))
                != 3_600_000_000_000
                or re.fullmatch(r"[0-9a-f]{64}", str(projection.get("rules_text_sha256", "")))
                is None
                or projection.get("official_outcome") not in {"UP", "DOWN", "SPLIT"}
                or not (
                    projection.get("official_resolution_ts_ns") is None
                    or isinstance(projection.get("official_resolution_ts_ns"), int)
                )
                or not re.fullmatch(
                    r"https://www\.binance\.com/en/trade/"
                    rf"{asset.value}_USDT",
                    str(projection.get("resolution_source_url", "")),
                )
                or binding.get("authority_sha256")
                != hashlib.sha256(canonical_json_bytes(projection)).hexdigest()
            ):
                raise RuntimeError("canary receipt has malformed market identity binding")
            binding_by_start[market_start] = binding
            seen_market_ids.add(market_id)
            seen_conditions.add(condition_id)
        if set(binding_by_start) != set(starts):
            raise RuntimeError("canary receipt market bindings do not match usable starts")
        for start in starts:
            usable_by_start[int(start)].add(asset)
    computed = minimum_canary_cover(
        {start: frozenset(assets) for start, assets in usable_by_start.items()},
        authority.assets,
    )
    if (
        selected != list(computed)
        or len(selected) != len(set(selected))
        or any(
            asset_selection[asset.value] not in usable_raw[asset.value]
            or asset_selection[asset.value] not in selected
            for asset in authority.assets
        )
    ):
        raise RuntimeError("canary receipt does not contain the exact usable minimum cover")


def _require_canary_receipt(authority: Authority) -> dict[str, Any]:
    if not CANARY_RECEIPT_PATH.exists():
        raise RuntimeError("full planning is locked until the one canary receipt is committed")
    receipt = json.loads(CANARY_RECEIPT_PATH.read_bytes())
    qualified_starts = receipt.get("qualified_market_starts", [])
    selected_starts = receipt.get("selected_market_starts", [])
    asset_starts = receipt.get("asset_market_starts", {})
    if (
        not isinstance(qualified_starts, list)
        or not isinstance(selected_starts, list)
        or not isinstance(asset_starts, dict)
    ):
        raise RuntimeError("canary receipt has malformed coverage selection")
    new_allowed_starts = {
        candidate
        for round_authority in _adaptive_round_authorities(authority)
        for candidate in _candidate_starts(round_authority)
    }
    configured_prior: dict[Asset, dict[str, Any]] = {}
    prior_assets_raw = receipt.get("prior_evidence_assets", [])
    invalidated_prior_raw = receipt.get("invalidated_prior_evidence_assets", [])
    if not isinstance(prior_assets_raw, list) or not isinstance(invalidated_prior_raw, list):
        raise RuntimeError("canary receipt has malformed prior evidence disposition")
    prior_assets = {Asset(value) for value in prior_assets_raw}
    invalidated_prior = {Asset(value) for value in invalidated_prior_raw}
    configured_prior_assets = set(configured_prior)
    if not (prior_assets | invalidated_prior) <= configured_prior_assets:
        raise RuntimeError("canary receipt cites unknown prior evidence")
    prior_starts = {
        int(start)
        for asset in prior_assets
        for start in configured_prior[asset]["qualified_market_starts"]
    }
    allowed_starts = new_allowed_starts | prior_starts
    release_tags = receipt.get("release_tags", [])
    prior_release_tags = receipt.get("prior_release_tags", [])
    executed_rounds = int(receipt.get("executed_rounds", 0))
    if (
        receipt.get("status") != "PASSED"
        or receipt.get("dataset_id") != "polymarket-1h-seven-v1"
        or receipt.get("timeframe") != "1h"
        or receipt.get("assets") != [asset.value for asset in authority.assets]
        or receipt.get("unexplained_failures") != 0
        or receipt.get("authenticated_no_op_partitions") != len(authority.assets)
        or receipt.get("settlement_bindings") != len(authority.assets)
        or receipt.get("usable_market_bindings") != len(authority.assets)
        or receipt.get("legitimate_exclusion_contract_checks") != len(authority.assets)
        or receipt.get("canary_release_prefix") != CANARY_RELEASE_PREFIX
        or len(prior_assets_raw) != len(prior_assets)
        or len(invalidated_prior_raw) != len(invalidated_prior)
        or prior_assets & invalidated_prior
        or (prior_assets | invalidated_prior) != configured_prior_assets
        or not isinstance(prior_release_tags, list)
        or len(prior_release_tags) != len(set(prior_release_tags))
        or set(prior_release_tags)
        != {str(configured_prior[asset]["release_tag"]) for asset in prior_assets}
        or not isinstance(release_tags, list)
        or len(release_tags) != executed_rounds
        or len(release_tags) != len(set(release_tags))
        or any(not str(tag).startswith(f"{CANARY_RELEASE_PREFIX}-") for tag in release_tags)
        or receipt.get("new_candidate_limit") != CANARY_MAX_CANDIDATES_TOTAL
        or not 0 <= executed_rounds <= CANARY_MAX_ROUNDS
        or (
            executed_rounds == 0
            and (
                prior_assets != set(authority.assets)
                or invalidated_prior
                or release_tags
                or int(receipt.get("source_head_requests", 0)) != 0
                or int(receipt.get("shared_pmxt_objects", 0)) != 0
                or int(receipt.get("source_transfer_bytes", 0)) != 0
            )
        )
        or not executed_rounds
        <= int(receipt.get("qualified_new_candidates", 0))
        <= executed_rounds * CANARY_MAX_CANDIDATES
        or int(receipt.get("qualified_candidates_total", 0))
        != int(receipt.get("qualified_new_candidates", 0)) + len(prior_starts)
        or len(qualified_starts) != int(receipt.get("qualified_candidates_total", 0))
        or len(qualified_starts) != len(set(qualified_starts))
        or not set(qualified_starts).issubset(allowed_starts)
        or bool(new_allowed_starts & prior_starts)
        or not 1 <= len(selected_starts) <= len(authority.assets)
        or not set(selected_starts).issubset(set(qualified_starts))
        or receipt.get("common_window") != (len(selected_starts) == 1)
        or set(asset_starts) != {asset.value for asset in authority.assets}
        or not set(asset_starts.values()).issubset(set(selected_starts))
        or not CANARY_PRIOR_GAMMA_REQUESTS
        <= int(receipt.get("gamma_requests", 0))
        <= CANARY_MAX_GAMMA_REQUESTS_TOTAL
        or (
            executed_rounds == 0
            and int(receipt.get("gamma_requests", 0)) != CANARY_PRIOR_GAMMA_REQUESTS
        )
        or not 0
        <= int(receipt.get("source_head_requests", 0))
        <= CANARY_MAX_SOURCE_OBJECTS_TOTAL
        or receipt.get("shared_source_transfer_owners") != executed_rounds
        or not 0
        <= int(receipt.get("shared_pmxt_objects", 0))
        <= CANARY_MAX_SOURCE_OBJECTS_TOTAL
        or receipt.get("shared_pmxt_objects") != receipt.get("source_head_requests")
        or not 0
        <= int(receipt.get("source_transfer_bytes", 0))
        <= CANARY_MAX_SOURCE_BYTES_TOTAL
        or (
            executed_rounds > 0
            and (
                int(receipt.get("source_head_requests", 0)) < 1
                or int(receipt.get("shared_pmxt_objects", 0)) < 1
                or int(receipt.get("source_transfer_bytes", 0)) < 1
            )
        )
        or int(receipt.get("canonical_bytes", 0)) < 1
        or not 0
        <= int(receipt.get("prior_remote_verification_bytes", 0))
        <= int(receipt.get("canonical_bytes", 0))
        or receipt.get("isolated_from_production") is not True
        or float(receipt.get("timeout_margin_seconds", 0)) <= 3_600
        or int(receipt.get("peak_rss_kib", 0)) < 1
        or int(receipt.get("minimum_free_disk_bytes", 0)) < 8_000_000_000
        or receipt.get("control_plane_sha256") != _control_plane_digest()
    ):
        raise RuntimeError("canary receipt does not authorize the frozen full plan")
    _validate_source_truth_receipt(receipt.get("source_truth"), authority)
    _validate_receipt_coverage(receipt, authority, qualified_starts)
    proofs = cast(dict[str, dict[str, Any]], receipt["remote_proofs"])
    for asset in prior_assets:
        expected = configured_prior[asset]
        proof = proofs.get(asset.value, {})
        if (
            proof.get("manifest_sha256") != expected["manifest_sha256"]
            or proof.get("release_tag") != expected["release_tag"]
            or proof.get("tool_commit") != expected["tool_commit"]
        ):
            raise RuntimeError("canary receipt changed reusable remote authority")
    return cast(dict[str, Any], receipt)


def remote_inventory(
    release_prefix: str = DATASET_RELEASE_PREFIX,
    exact_tags: set[str] | None = None,
) -> dict[str, list[RemoteAsset]]:
    result: dict[str, list[RemoteAsset]] = {}
    releases = []
    for page in range(1, 11):
        batch = _json(f"{API}/releases?per_page=100&page={page}")
        releases.extend(batch)
        if len(batch) < 100:
            break
    else:
        raise RuntimeError("release inventory exceeded bounded pagination")
    for release in releases:
        release_tag = str(release.get("tag_name", ""))
        if exact_tags is not None:
            selected = release_tag in exact_tags
        else:
            selected = release_tag.startswith(f"{release_prefix}-")
        if not selected:
            continue
        release_id = int(release["id"])
        for page in range(1, 11):
            assets = _json(f"{API}/releases/{release_id}/assets?per_page=100&page={page}")
            for item in assets:
                match = ASSET_PATTERN.fullmatch(str(item["name"]))
                if match is None:
                    raise RuntimeError(
                        f"1h release contains a noncanonical asset name: {item['name']}"
                    )
                partition = f"{match[1]}/1h/{match[2]}"
                expected_tag = f"{release_prefix}-{release_bucket(date.fromisoformat(match[2]))}"
                if exact_tags is None and release_tag != expected_tag:
                    raise RuntimeError(f"partition is published in the wrong release: {partition}")
                result.setdefault(partition, []).append(
                    RemoteAsset(
                        str(item["name"]),
                        int(item["size"]),
                        str(item["url"]),
                        match[3],
                        match[4],
                        str(item.get("state", "")),
                        str(item["id"]),
                        release_tag,
                    )
                )
            if len(assets) < 100:
                break
        else:
            raise RuntimeError("release asset inventory exceeded bounded pagination")
    return result


def verified_partitions(inventory: dict[str, list[RemoteAsset]]) -> set[str]:
    verified: set[str] = set()
    for partition, assets in inventory.items():
        filenames = [asset.filename for asset in assets]
        if (
            len(assets) == len(EXPECTED_FILES)
            and set(filenames) == EXPECTED_FILES
            and all(asset.state == "uploaded" for asset in assets)
        ):
            verified.add(partition)
    return verified


def unfinished_plan(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> list[dict[str, Any]]:
    selected = authority or load_authority()
    complete = verified_partitions(inventory)
    return [item for item in _full_plan(selected) if item["partition_id"] not in complete]


def inventory_anomalies(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> dict[str, list[str]]:
    selected = authority or load_authority()
    plan_ids = {str(item["partition_id"]) for item in _full_plan(selected)}
    verified = verified_partitions(inventory)
    partial = sorted(
        partition
        for partition, assets in inventory.items()
        if partition in plan_ids and partition not in verified and assets
    )
    divergent = sorted(
        f"{partition}/{filename}"
        for partition, assets in inventory.items()
        for filename in {asset.filename for asset in assets}
        if len({asset.digest for asset in assets if asset.filename == filename}) > 1
    )
    duplicate = sorted(
        f"{partition}/{filename}"
        for partition, assets in inventory.items()
        for filename in {asset.filename for asset in assets}
        if sum(asset.filename == filename for asset in assets) > 1
    )
    unexpected_files = sorted(
        f"{partition}/{asset.filename}"
        for partition, assets in inventory.items()
        for asset in assets
        if asset.filename not in EXPECTED_FILES
    )
    return {
        "partial": partial,
        "divergent": divergent,
        "duplicate": duplicate,
        "unexpected_files": unexpected_files,
        "out_of_plan": sorted(set(inventory) - plan_ids),
    }


def _fatal_inventory_anomalies(anomalies: dict[str, list[str]]) -> bool:
    return any(values for name, values in anomalies.items() if name != "partial")


def day_plan(plan: list[dict[str, Any]]) -> list[dict[str, str]]:
    days_by_release: dict[str, set[str]] = {}
    for item in plan:
        day = str(item["partition_id"]).split("/")[2]
        release_group = str(item["release_group"])
        days_by_release.setdefault(release_group, set()).add(day)
    ordered = {group: sorted(days) for group, days in sorted(days_by_release.items())}
    return [
        {"day": days[index], "release_group": group}
        for index in range(max((len(days) for days in ordered.values()), default=0))
        for group, days in ordered.items()
        if index < len(days)
    ]


def _download_verify(asset: RemoteAsset, directory: Path) -> Path:
    target = directory / asset.filename
    target.write_bytes(_request(asset.url, "application/octet-stream"))
    payload = target.read_bytes()
    if len(payload) != asset.size or hashlib.sha256(payload).hexdigest() != asset.digest:
        raise RuntimeError(f"remote digest verification failed: {asset.name}")
    return target


def _market_authority_projection(market: Market) -> dict[str, Any]:
    return {
        "asset": market.asset.value,
        "venue": market.venue,
        "timeframe": market.timeframe,
        "event_id": market.event_id,
        "market_id": market.market_id,
        "condition_id": market.condition_id,
        "token_up": market.token_up,
        "token_down": market.token_down,
        "market_start_ns": market.market_start_ns,
        "market_end_ns": market.market_end_ns,
        "rules_text_sha256": market.rules_text_sha256,
        "resolution_source_url": market.resolution_source_url,
        "official_outcome": market.official_outcome.value,
        "official_resolution_ts_ns": market.official_resolution_ts_ns,
    }


def _row_authority_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {name: row[name] for name in sorted(MARKET_AUTHORITY_FIELDS)}


def _verify_canary_dispositions(
    market_rows: list[dict[str, Any]],
    exclusion_rows: list[dict[str, Any]],
    expected_candidates: dict[str, Market],
) -> list[int]:
    accepted_ids = [str(row["market_id"]) for row in market_rows]
    excluded_ids = [str(row["market_id"]) for row in exclusion_rows]
    if (
        len(accepted_ids) != len(set(accepted_ids))
        or len(excluded_ids) != len(set(excluded_ids))
        or set(accepted_ids) & set(excluded_ids)
        or set(accepted_ids) | set(excluded_ids) != set(expected_candidates)
    ):
        raise RuntimeError("remote canary candidate disposition is incomplete")
    for row in market_rows:
        expected = expected_candidates[str(row["market_id"])]
        if (
            _row_authority_projection(row) != _market_authority_projection(expected)
            or row["quality_tier"] != "TIER_A"
        ):
            raise RuntimeError("remote usable canary market changed authority or quality")
    for row in exclusion_rows:
        expected = expected_candidates[str(row["market_id"])]
        evidence = json.loads(str(row["evidence_json"]))
        if evidence.get("condition_id") != expected.condition_id:
            raise RuntimeError("remote canary exclusion lost its condition binding")
    return sorted(int(row["market_start_ns"]) // 1_000_000_000 for row in market_rows)


def verify_remote_partition(
    partition: str,
    inventory: dict[str, list[RemoteAsset]],
    expected_market: tuple[str, frozenset[str]] | None = None,
    expected_sources: frozenset[tuple[str, int, str]] | None = None,
    expected_candidates: dict[str, Market] | None = None,
    expected_gamma: frozenset[tuple[str, int, str]] | None = None,
) -> dict[str, Any]:
    expected_asset = partition.split("/", 1)[0]
    assets = inventory.get(partition, [])
    if partition not in verified_partitions(inventory):
        raise RuntimeError(f"partition is not durably complete: {partition}")
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        for asset in sorted(assets, key=lambda item: item.filename):
            _download_verify(asset, directory)
        payload = (directory / "manifest.json").read_bytes()
        manifest = json.loads(payload)
        if canonical_json_bytes(manifest) != payload:
            raise RuntimeError("remote manifest is not canonical JSON")
        if (
            manifest.get("dataset_id") != "polymarket-1h-seven-v1"
            or manifest.get("partition_id") != partition
            or manifest.get("timeframe") != "1h"
        ):
            raise RuntimeError("remote manifest identity mismatch")
        market_rows = pq.read_table(directory / "markets.parquet").to_pylist()
        exclusion_rows = pq.read_table(directory / "exclusions.parquet").to_pylist()
        if expected_market is not None:
            published = {
                (row["condition_id"], frozenset((row["token_up"], row["token_down"])))
                for row in market_rows
            }
            if published != {expected_market}:
                raise RuntimeError("remote canary market does not match source-qualified identity")
        if expected_candidates is not None:
            _verify_canary_dispositions(market_rows, exclusion_rows, expected_candidates)
        if expected_sources is not None:
            published_sources = frozenset(
                (item["source_url"], int(item["byte_length"]), str(item.get("etag", "")))
                for item in manifest["provenance"]
                if item["source_id"] == "pmxt_v2"
            )
            if published_sources != expected_sources:
                raise RuntimeError("remote canary PMXT provenance changed")
        if expected_gamma is not None:
            published_gamma = frozenset(
                (
                    str(item["source_url"]),
                    int(item["byte_length"]),
                    str(item["sha256"]),
                )
                for item in manifest["provenance"]
                if item["source_id"] == "polymarket_gamma_clob"
            )
            if published_gamma != expected_gamma:
                raise RuntimeError("remote canary Gamma provenance changed")
        for row in market_rows:
            if (
                row["asset"] != expected_asset
                or row["timeframe"] != "1h"
                or row["market_end_ns"] - row["market_start_ns"] != 3_600_000_000_000
                or row["official_outcome"] not in {"UP", "DOWN", "SPLIT"}
                or not re.fullmatch(
                    r"https://www\.binance\.com/en/trade/"
                    rf"{expected_asset}_USDT",
                    str(row["resolution_source_url"]),
                )
            ):
                raise RuntimeError("remote market lacks frozen 1h settlement semantics")
    return {
        "partition_id": partition,
        "result": "VERIFIED_NO_OP",
        "assets": len(assets),
        "bytes": sum(asset.size for asset in assets),
        "markets": len(market_rows),
        "quality": manifest["quality_tier"],
        "accepted_market_starts": sorted(
            int(row["market_start_ns"]) // 1_000_000_000 for row in market_rows
        ),
        "accepted_market_bindings": [
            {
                "authority_projection": _row_authority_projection(row),
                "authority_sha256": hashlib.sha256(
                    canonical_json_bytes(_row_authority_projection(row))
                ).hexdigest(),
            }
            for row in sorted(market_rows, key=lambda item: int(item["market_start_ns"]))
        ],
        "exclusions": len(exclusion_rows),
        "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        "tool_commit": str(manifest["tool_commit"]),
        "pmxt_urls": sorted(
            item["source_url"] for item in manifest["provenance"] if item["source_id"] == "pmxt_v2"
        ),
        "authenticated_redownload": bool(os.environ.get("GITHUB_TOKEN")),
    }


def _revalidate_prior_canary_evidence(
    authority: Authority, deadline: float
) -> dict[str, Any]:
    configured = _load_prior_canary_evidence(authority)
    tags = {str(item["release_tag"]) for item in configured.values()}
    inventory = remote_inventory(exact_tags=tags)
    gamma = GammaClient(fetch=_fetch_gamma)
    proofs: dict[Asset, dict[str, Any]] = {}
    markets: dict[Asset, tuple[Market, ...]] = {}
    invalidated: list[Asset] = []
    gamma_requests = 0
    verified_bytes = 0
    seen_market_ids: set[str] = set()
    seen_conditions: set[str] = set()
    for asset, expected in configured.items():
        if time.monotonic() >= deadline:
            raise RuntimeError("prior evidence revalidation exhausted the canary wall bound")
        partition = str(expected["partition_id"])
        proof = verify_remote_partition(partition, inventory)
        verified_bytes += int(proof["bytes"])
        starts = tuple(int(start) for start in expected["qualified_market_starts"])
        expected_urls = {
            source.url
            for source in pmxt_hourly_objects(
                (min(starts) - 3_600) * 1_000_000_000,
                (max(starts) + 3_600) * 1_000_000_000,
            )
        }
        if (
            proof["manifest_sha256"] != expected["manifest_sha256"]
            or proof["tool_commit"] != expected["tool_commit"]
            or proof["quality"] != "TIER_A"
            or not proof["accepted_market_starts"]
            or len(proof["accepted_market_starts"])
            != len(set(proof["accepted_market_starts"]))
            or len(proof["accepted_market_bindings"])
            != len(proof["accepted_market_starts"])
            or not set(proof["accepted_market_starts"]).issubset(set(starts))
            or set(proof["pmxt_urls"]) != expected_urls
            or proof["authenticated_redownload"] is not True
        ):
            raise RuntimeError("prior canary remote proof changed immutable authority")
        current: list[Market] = []
        current_market_ids: set[str] = set()
        current_conditions: set[str] = set()
        unresolved = False
        for start in starts:
            gamma_requests += 1
            try:
                market, _, _ = gamma.fetch_market(asset, start)
            except (IdentityError, UnresolvedMarketError):
                unresolved = True
                break
            if (
                market.asset is not asset
                or market.timeframe != "1h"
                or market.market_start_ns != start * 1_000_000_000
                or market.market_end_ns - market.market_start_ns != 3_600_000_000_000
                or market.market_id in seen_market_ids
                or market.market_id in current_market_ids
                or market.condition_id in seen_conditions
                or market.condition_id in current_conditions
            ):
                raise RuntimeError("prior Gamma authority violates exact unique 1h identity")
            current.append(market)
            current_market_ids.add(market.market_id)
            current_conditions.add(market.condition_id)
        if unresolved:
            invalidated.append(asset)
            continue
        current_by_start = {
            market.market_start_ns // 1_000_000_000: market for market in current
        }
        semantic_match = all(
            binding["authority_projection"]
            == _market_authority_projection(current_by_start[int(start)])
            for start, binding in (
                (
                    int(binding["authority_projection"]["market_start_ns"])
                    // 1_000_000_000,
                    binding,
                )
                for binding in proof["accepted_market_bindings"]
            )
        )
        if not semantic_match:
            invalidated.append(asset)
            continue
        for market in current:
            seen_market_ids.add(market.market_id)
            seen_conditions.add(market.condition_id)
        proofs[asset] = proof
        markets[asset] = tuple(current)
    if gamma_requests > CANARY_PRIOR_GAMMA_REQUESTS:
        raise RuntimeError("prior canary exceeded its Gamma revalidation budget")
    return {
        "proofs": proofs,
        "markets": markets,
        "invalidated": tuple(invalidated),
        "gamma_requests": gamma_requests,
        "verified_bytes": verified_bytes,
        "release_tags": {asset: str(configured[asset]["release_tag"]) for asset in proofs},
        "configured": configured,
    }


def _atomic_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(".partial")
    temporary.write_bytes(canonical_json_bytes(value))
    os.replace(temporary, path)


def reconcile_ledger(
    inventory: dict[str, list[RemoteAsset]], authority: Authority | None = None
) -> dict[str, Any]:
    selected = authority or load_authority()
    anomalies = inventory_anomalies(inventory, selected)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    complete = verified_partitions(inventory)
    partitions: dict[str, Any] = {}
    for partition in sorted(complete):
        manifest_asset = next(
            asset for asset in inventory[partition] if asset.filename == "manifest.json"
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = _download_verify(manifest_asset, Path(temporary))
            manifest = json.loads(path.read_bytes())
        partitions[partition] = {
            "manifest_sha256": manifest_asset.digest,
            "quality": manifest["quality_tier"],
            "release_tag": manifest_asset.release_tag,
        }
    plan = _full_plan(selected)
    unfinished = [
        str(item["partition_id"]) for item in plan if item["partition_id"] not in complete
    ]
    return {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-1h-seven-v1",
        "planned": len(plan),
        "completed": len(complete),
        "unfinished": len(unfinished),
        "continuation_partition": unfinished[0] if unfinished else None,
        "partitions": partitions,
        "durable_identity": "remote content-addressed assets plus embedded manifests",
    }


def _write_output(name: str, value: str) -> None:
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"{name}={value}\n")


def command_plan() -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    unfinished = unfinished_plan(inventory, authority)
    assets_by_day = _assets_by_day(unfinished)
    retrieved_at_ns = str(time.time_ns())
    days = [
        {
            **item,
            "assets": ",".join(assets_by_day[item["day"]]),
            "retrieved_at_ns": retrieved_at_ns,
        }
        for item in day_plan(unfinished)
    ]
    if len(days) > 256:
        raise RuntimeError("finite plan exceeds the single bounded Actions matrix")
    matrix = json.dumps({"include": days}, separators=(",", ":"))
    _write_output("matrix", matrix)
    print(
        json.dumps(
            {
                "planned_partitions": len(_full_plan(authority)),
                "durable_partitions": len(verified_partitions(inventory)),
                "unfinished_partitions": len(unfinished),
                "utc_days": len(days),
                "matrix": json.loads(matrix),
            },
            sort_keys=True,
        )
    )


def command_validate_accelerated_matrix() -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    requested = json.loads(os.environ.get("REQUESTED_MATRIX", ""))
    if not isinstance(requested, dict) or set(requested) != {"include"}:
        raise RuntimeError("accelerated matrix must contain only include")
    include = requested["include"]
    if not isinstance(include, list) or not include or len(include) > 64:
        raise RuntimeError("accelerated matrix must contain 1..64 bounded days")
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    unfinished_by_day = _assets_by_day(unfinished_plan(inventory, authority))
    validated = []
    retrieved_at_ns = time.time_ns()
    seen: set[str] = set()
    for item in include:
        if not isinstance(item, dict) or set(item) != {"day", "release_group"}:
            raise RuntimeError("accelerated matrix row has the wrong shape")
        day_text = str(item["day"])
        release_group = str(item["release_group"])
        if day_text in seen:
            raise RuntimeError("accelerated matrix contains a duplicate day")
        seen.add(day_text)
        _validated_day_plan(authority, day_text, release_group)
        assets = unfinished_by_day.get(day_text, [])
        if assets:
            validated.append(
                {
                    "day": day_text,
                    "release_group": release_group,
                    "assets": ",".join(assets),
                    "retrieved_at_ns": str(retrieved_at_ns),
                }
            )
    if not validated:
        raise RuntimeError("accelerated matrix has no remotely unfinished days")
    matrix = json.dumps({"include": validated}, separators=(",", ":"))
    _write_output("matrix", matrix)
    print(matrix)


def command_checkpoint() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        raise RuntimeError("checkpoint requires authenticated GitHub remote authority")
    authority = load_authority()
    _require_canary_receipt(authority)
    ledger = reconcile_ledger(remote_inventory(), authority)
    _atomic_json(LEDGER_PATH, ledger)
    print(
        json.dumps(
            {
                "completed": ledger["completed"],
                "unfinished": ledger["unfinished"],
                "continuation_partition": ledger["continuation_partition"],
            },
            sort_keys=True,
        )
    )


def command_certify() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        raise RuntimeError("certification requires authenticated GitHub remote authority")
    authority = load_authority()
    _require_canary_receipt(authority)
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    plan = _full_plan(authority)
    expected_partitions = len(plan)
    complete = verified_partitions(inventory)
    if expected_partitions <= 0 or len(complete) != expected_partitions or any(anomalies.values()):
        raise RuntimeError(
            "final certification requires the exact frozen plan and no anomalies"
        )
    ledger = reconcile_ledger(inventory, authority)
    partitions = cast(dict[str, dict[str, Any]], ledger.get("partitions", {}))
    if (
        ledger.get("completed") != expected_partitions
        or ledger.get("unfinished") != 0
        or ledger.get("continuation_partition") is not None
        or len(partitions) != expected_partitions
    ):
        raise RuntimeError("reconciled ledger is not complete")
    qualities = [str(item.get("quality", "")) for item in partitions.values()]
    if set(qualities) - {"TIER_A", "TIER_B", "EXCLUDED"}:
        raise RuntimeError("remote manifest contains an unsupported quality tier")
    tier_a = qualities.count("TIER_A")
    tier_b = qualities.count("TIER_B")
    excluded = qualities.count("EXCLUDED")
    if tier_a + tier_b + excluded != expected_partitions:
        raise RuntimeError("final quality totals do not reconcile")
    expected_release_tags = sorted({str(item["release_group"]) for item in plan})
    release_tags = sorted(
        {
            str(asset.release_tag)
            for assets in inventory.values()
            for asset in assets
            if asset.release_tag is not None
        }
    )
    if release_tags != expected_release_tags:
        raise RuntimeError("final release set does not match the frozen plan")
    remote_assets = sum(len(assets) for assets in inventory.values())
    if remote_assets != expected_partitions * len(EXPECTED_FILES):
        raise RuntimeError("final remote asset count does not reconcile")
    ledger.update(
        {
            "tier_a": tier_a,
            "tier_b": tier_b,
            "excluded": excluded,
            "remote_assets": remote_assets,
            "release_tags": release_tags,
            "certification_status": "CERTIFIED",
        }
    )
    _atomic_json(LEDGER_PATH, ledger)
    recovery_runs = [
        int(item)
        for item in os.environ.get("RECOVERY_RUN_IDS", "").split(",")
        if item.strip()
    ]
    report = {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-1h-seven-v1",
        "status": "CERTIFIED",
        "certified_at": datetime.now(UTC).isoformat(),
        "coverage_start": authority.start.isoformat().replace("+00:00", "Z"),
        "release_cutoff": authority.cutoff.isoformat().replace("+00:00", "Z"),
        "planned": expected_partitions,
        "durable": expected_partitions,
        "completion_percentage": 100,
        "tier_a": tier_a,
        "tier_b": tier_b,
        "excluded": excluded,
        "unfinished": 0,
        "remote_assets": remote_assets,
        "release_tags": release_tags,
        "anomalies": anomalies,
        "durable_identity": "remote content-addressed assets plus embedded manifests",
        "ledger_sha256": hashlib.sha256(canonical_json_bytes(ledger)).hexdigest(),
        "recovery_run_ids": recovery_runs,
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
    }
    _atomic_json(CERTIFICATION_PATH, report)
    print(json.dumps(report, sort_keys=True))


def _raise_child_failure(completed: subprocess.CompletedProcess[str]) -> None:
    if completed.returncode == 0:
        return
    if completed.stdout:
        sys.stdout.write(completed.stdout)
    if completed.stderr:
        sys.stderr.write(completed.stderr)
    raise RuntimeError(f"1h executor failed with exit code {completed.returncode}")


def command_execute_day(day_text: str, expected_release_group: str | None = None) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    day = date.fromisoformat(day_text)
    plan_for_day = [item for item in _full_plan(authority) if item["day"] == day_text]
    if not plan_for_day:
        raise RuntimeError("requested UTC day is outside the frozen plan")
    release_groups = {str(item["release_group"]) for item in plan_for_day}
    if expected_release_group is not None and release_groups != {expected_release_group}:
        raise RuntimeError("requested recovery release group does not match the frozen plan")
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    complete = verified_partitions(inventory)
    assets = tuple(
        Asset(str(item["asset"])) for item in plan_for_day if item["partition_id"] not in complete
    )
    if not assets:
        print(json.dumps({"day": day_text, "result": "AUTHENTICATED_NO_OP"}))
        return
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_backfill",
                "--work-root",
                str(root / "work"),
                "--ledger",
                str(root / "ledger.json"),
                "--start",
                day.isoformat(),
                "--end",
                day.isoformat(),
                "--coverage-start",
                authority.start.isoformat(),
                "--cutoff",
                authority.cutoff.isoformat(),
                "--assets",
                ",".join(asset.value for asset in assets),
            ],
            text=True,
            check=False,
        )
        _raise_child_failure(completed)
    refreshed = remote_inventory()
    for asset in assets:
        verify_remote_partition(f"{asset.value}/1h/{day_text}", refreshed)
    _atomic_json(LEDGER_PATH, reconcile_ledger(refreshed, authority))


def _validated_day_plan(
    authority: Authority, day_text: str, expected_release_group: str
) -> list[dict[str, Any]]:
    day = date.fromisoformat(day_text)
    plan_for_day = [item for item in _full_plan(authority) if item["day"] == day.isoformat()]
    if not plan_for_day:
        raise RuntimeError("requested UTC day is outside the frozen plan")
    release_groups = {str(item["release_group"]) for item in plan_for_day}
    if release_groups != {expected_release_group}:
        raise RuntimeError("requested recovery release group does not match the frozen plan")
    return plan_for_day


def _assets_by_day(plan: list[dict[str, Any]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for item in plan:
        result.setdefault(str(item["day"]), []).append(str(item["asset"]))
    return result


def _validated_compute_assets(
    plan_for_day: list[dict[str, Any]], expected_assets_text: str
) -> tuple[Asset, ...]:
    if not expected_assets_text:
        raise RuntimeError("staged compute asset assignment is empty or duplicated")
    requested_assets = expected_assets_text.split(",")
    if len(requested_assets) != len(set(requested_assets)):
        raise RuntimeError("staged compute asset assignment is empty or duplicated")
    assets = tuple(Asset(value) for value in requested_assets)
    planned_assets = {Asset(str(item["asset"])) for item in plan_for_day}
    if not set(assets).issubset(planned_assets):
        raise RuntimeError("staged compute asset assignment is outside the frozen day plan")
    return assets


def _bundle_partition_directory(root: Path, partition_id: str) -> Path:
    asset, timeframe, day_text = partition_id.split("/")
    return root / "partitions" / f"asset={asset}" / f"timeframe={timeframe}" / f"date={day_text}"


def _bundle_file_inventory(directory: Path, root: Path) -> list[dict[str, Any]]:
    filenames = {path.name for path in directory.iterdir() if path.is_file()}
    if filenames != EXPECTED_FILES:
        raise ConflictError("staged partition has a noncanonical file inventory")
    result = []
    for path in sorted(directory.iterdir()):
        byte_length, digest = hash_file(path)
        result.append(
            {
                "path": path.relative_to(root).as_posix(),
                "byte_length": byte_length,
                "sha256": digest,
            }
        )
    return result


def _validate_staged_partition(
    root: Path,
    partition_id: str,
    expected_tool_commit: str,
    expected_files: list[dict[str, Any]],
) -> Path:
    directory = _bundle_partition_directory(root, partition_id)
    if not directory.is_dir():
        raise ConflictError("staged partition directory is missing")
    actual_files = _bundle_file_inventory(directory, root)
    if actual_files != expected_files:
        raise ConflictError("staged partition receipt digest mismatch")
    manifest_digest = verify_manifest(directory)
    manifest = json.loads((directory / "manifest.json").read_bytes())
    asset, _, _ = partition_id.split("/")
    manifest_files = manifest.get("files")
    if not isinstance(manifest_files, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("path"), str)
        for item in manifest_files
    ):
        raise ConflictError("staged partition manifest file inventory is malformed")
    if (
        {str(item["path"]) for item in manifest_files} != EXPECTED_FILES - {"manifest.json"}
        or manifest.get("dataset_id") != "polymarket-1h-seven-v1"
        or manifest.get("partition_id") != partition_id
        or manifest.get("asset") != asset
        or manifest.get("venue") != "polymarket"
        or manifest.get("timeframe") != "1h"
        or manifest.get("tool_commit") != expected_tool_commit
        or manifest.get("quality_tier") not in {"TIER_A", "EXCLUDED"}
        or manifest_digest
        != next(item["sha256"] for item in actual_files if item["path"].endswith("manifest.json"))
    ):
        raise ConflictError("staged partition manifest authority mismatch")
    return directory


def _require_complete_staged_coverage(
    partition_ids: list[str], plan_ids: set[str], complete: set[str]
) -> None:
    missing = sorted((plan_ids - complete) - set(partition_ids))
    if missing:
        raise ConflictError(f"staged receipt omits unfinished partitions: {missing}")


def command_compute_day(
    day_text: str,
    expected_release_group: str,
    expected_assets_text: str,
    bundle_root: Path,
) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    plan_for_day = _validated_day_plan(authority, day_text, expected_release_group)
    assets = _validated_compute_assets(plan_for_day, expected_assets_text)
    if bundle_root.exists() and any(bundle_root.iterdir()):
        raise RuntimeError("staged bundle root is not empty")
    bundle_root.mkdir(parents=True, exist_ok=True)
    if assets:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "scripts.run_backfill",
                    "--work-root",
                    str(root / "work"),
                    "--ledger",
                    str(root / "ledger.json"),
                    "--start",
                    day_text,
                    "--end",
                    day_text,
                    "--coverage-start",
                    authority.start.isoformat(),
                    "--cutoff",
                    authority.cutoff.isoformat(),
                    "--assets",
                    ",".join(asset.value for asset in assets),
                    "--compute-output-root",
                    str(bundle_root / "partitions"),
                ],
                text=True,
                check=False,
            )
            _raise_child_failure(completed)
    _write_compute_bundle_receipt(
        day_text, expected_release_group, assets, bundle_root
    )


def _write_compute_bundle_receipt(
    day_text: str,
    expected_release_group: str,
    assets: tuple[Asset, ...],
    bundle_root: Path,
) -> None:
    tool_commit = _tool_commit()
    partitions = []
    for asset in assets:
        partition_id = f"{asset.value}/1h/{day_text}"
        directory = _bundle_partition_directory(bundle_root, partition_id)
        files = _bundle_file_inventory(directory, bundle_root)
        _validate_staged_partition(bundle_root, partition_id, tool_commit, files)
        partitions.append({"partition_id": partition_id, "files": files})
    receipt = {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-1h-seven-v1",
        "authority": "noncanonical immutable staged bytes pending authenticated publication",
        "day": day_text,
        "release_group": expected_release_group,
        "tool_commit": tool_commit,
        "partitions": partitions,
    }
    _atomic_json(bundle_root / "receipt.json", receipt)
    print(
        json.dumps(
            {
                "day": day_text,
                "result": "STAGED" if partitions else "AUTHENTICATED_NO_OP",
                "staged_partitions": len(partitions),
            },
            sort_keys=True,
        )
    )


def command_compute_day_segment(
    day_text: str,
    expected_release_group: str,
    expected_assets_text: str,
    bundle_root: Path,
    segment_index: int,
    segment_count: int,
    retrieved_at_ns: int,
) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    plan_for_day = _validated_day_plan(authority, day_text, expected_release_group)
    assets = _validated_compute_assets(plan_for_day, expected_assets_text)
    if segment_count != DAY_SEGMENT_COUNT:
        raise RuntimeError("compute segment count does not match the frozen architecture")
    if segment_index < 0 or segment_index >= segment_count or retrieved_at_ns <= 0:
        raise RuntimeError("compute segment bounds are invalid")
    if bundle_root.exists() and any(bundle_root.iterdir()):
        raise RuntimeError("segment bundle root is not empty")
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_backfill",
                "--work-root",
                str(root / "work"),
                "--ledger",
                str(root / "ledger.json"),
                "--start",
                day_text,
                "--end",
                day_text,
                "--coverage-start",
                authority.start.isoformat(),
                "--cutoff",
                authority.cutoff.isoformat(),
                "--assets",
                ",".join(asset.value for asset in assets),
                "--segment-output-root",
                str(bundle_root),
                "--segment-index",
                str(segment_index),
                "--segment-count",
                str(segment_count),
                "--retrieved-at-ns",
                str(retrieved_at_ns),
            ],
            text=True,
            check=False,
        )
        _raise_child_failure(completed)


def command_assemble_day_segments(
    day_text: str,
    expected_release_group: str,
    expected_assets_text: str,
    segments_root: Path,
    bundle_root: Path,
    segment_count: int,
    retrieved_at_ns: int,
) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    plan_for_day = _validated_day_plan(authority, day_text, expected_release_group)
    assets = _validated_compute_assets(plan_for_day, expected_assets_text)
    if segment_count != DAY_SEGMENT_COUNT or retrieved_at_ns <= 0:
        raise RuntimeError("segment assembly bounds do not match the frozen architecture")
    if bundle_root.exists() and any(bundle_root.iterdir()):
        raise RuntimeError("staged bundle root is not empty")
    bundle_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "scripts.run_backfill",
                "--work-root",
                str(root / "work"),
                "--ledger",
                str(root / "ledger.json"),
                "--start",
                day_text,
                "--end",
                day_text,
                "--coverage-start",
                authority.start.isoformat(),
                "--cutoff",
                authority.cutoff.isoformat(),
                "--assets",
                ",".join(asset.value for asset in assets),
                "--segment-input-root",
                str(segments_root),
                "--segment-count",
                str(segment_count),
                "--retrieved-at-ns",
                str(retrieved_at_ns),
                "--compute-output-root",
                str(bundle_root / "partitions"),
            ],
            text=True,
            check=False,
        )
        _raise_child_failure(completed)
    _write_compute_bundle_receipt(
        day_text, expected_release_group, assets, bundle_root
    )


def command_publish_staged_day(
    day_text: str, expected_release_group: str, bundle_root: Path
) -> None:
    authority = load_authority()
    _require_canary_receipt(authority)
    plan_for_day = _validated_day_plan(authority, day_text, expected_release_group)
    plan_ids = {str(item["partition_id"]) for item in plan_for_day}
    receipt_path = bundle_root / "receipt.json"
    payload = receipt_path.read_bytes()
    receipt = json.loads(payload)
    if canonical_json_bytes(receipt) != payload:
        raise ConflictError("staged receipt is not canonical JSON")
    tool_commit = _tool_commit()
    partitions_raw = receipt.get("partitions")
    if not isinstance(partitions_raw, list) or any(
        not isinstance(item, dict) or set(item) != {"partition_id", "files"}
        for item in partitions_raw
    ):
        raise ConflictError("staged receipt partitions have the wrong shape")
    partitions = cast(list[dict[str, Any]], partitions_raw)
    partition_ids = [str(item.get("partition_id")) for item in partitions]
    if (
        receipt.get("schema_version") != "1.0.0"
        or receipt.get("dataset_id") != "polymarket-1h-seven-v1"
        or receipt.get("authority")
        != "noncanonical immutable staged bytes pending authenticated publication"
        or receipt.get("day") != day_text
        or receipt.get("release_group") != expected_release_group
        or receipt.get("tool_commit") != tool_commit
        or len(partition_ids) != len(set(partition_ids))
        or not set(partition_ids).issubset(plan_ids)
    ):
        raise ConflictError("staged receipt authority mismatch")
    inventory = remote_inventory()
    anomalies = inventory_anomalies(inventory, authority)
    if _fatal_inventory_anomalies(anomalies):
        raise RuntimeError(f"remote inventory fails closed: {json.dumps(anomalies)}")
    complete = verified_partitions(inventory)
    _require_complete_staged_coverage(partition_ids, plan_ids, complete)
    directories = {
        partition_id: _validate_staged_partition(
            bundle_root,
            partition_id,
            tool_commit,
            cast(list[dict[str, Any]], item.get("files")),
        )
        for partition_id, item in zip(partition_ids, partitions, strict=True)
    }
    publisher = Publisher(GitHubReleaseBackend(REPOSITORY))
    published = []
    for partition_id in partition_ids:
        if partition_id in complete:
            continue
        publisher.publish_partition(
            expected_release_group,
            partition_id,
            directories[partition_id],
        )
        published.append(partition_id)
    refreshed = remote_inventory()
    refreshed_anomalies = inventory_anomalies(refreshed, authority)
    if _fatal_inventory_anomalies(refreshed_anomalies):
        raise RuntimeError(
            f"remote inventory fails closed: {json.dumps(refreshed_anomalies)}"
        )
    for partition_id in partition_ids:
        verify_remote_partition(partition_id, refreshed)
    print(
        json.dumps(
            {
                "day": day_text,
                "published_partitions": len(published),
                "authenticated_no_op_partitions": len(partition_ids) - len(published),
            },
            sort_keys=True,
        )
    )


def _candidate_starts(authority: Authority) -> list[int]:
    current = authority.canary_search_start
    result = []
    while current >= authority.canary_search_end:
        timestamp = int(current.timestamp())
        if timestamp % 3_600:
            raise RuntimeError("canary search boundaries must be 1h aligned")
        result.append(timestamp)
        current -= timedelta(minutes=authority.canary_step_minutes)
    if not result or len(result) > CANARY_MAX_CANDIDATES:
        raise RuntimeError("canary candidate search is empty or exceeds its finite cap")
    return result


def _pmxt_source_identity(source: SourceObject) -> tuple[int, str]:
    request = urllib.request.Request(source.url, method="HEAD", headers={"User-Agent": USER_AGENT})
    last_error: Exception | None = None
    for attempt, delay in enumerate((0, *TRANSFER_RETRY_DELAYS)):
        if delay:
            time.sleep(delay)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                status = int(response.status)
                if not 200 <= status < 300:
                    raise RuntimeError(f"PMXT canary HEAD returned HTTP {status}")
                length = int(response.headers.get("Content-Length", "0"))
                etag = response.headers.get("ETag", "")
                if length <= 0 or not etag:
                    raise RuntimeError("PMXT canary HEAD lacks object identity")
                return length, etag
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise RuntimeError(
                    f"catalog-listed PMXT canary source is missing: {source.url}"
                ) from exc
            if exc.code not in TRANSIENT_HTTP_STATUS:
                raise
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
        if attempt == len(TRANSFER_RETRY_DELAYS):
            break
    assert last_error is not None
    raise last_error


def qualify_canary_candidates(
    authority: Authority,
    source_identity: Callable[[SourceObject], tuple[int, str]] = _pmxt_source_identity,
    assets: tuple[Asset, ...] | None = None,
    prior_market_ids: frozenset[str] = frozenset(),
    prior_conditions: frozenset[str] = frozenset(),
) -> CanaryQualification:
    selected_assets = assets or authority.assets
    if not selected_assets or len(selected_assets) != len(set(selected_assets)):
        raise RuntimeError("canary qualification requires unique uncovered assets")
    if not set(selected_assets).issubset(set(authority.assets)):
        raise RuntimeError("canary qualification asset is outside frozen scope")
    gamma_limit = CANARY_MAX_CANDIDATES * len(selected_assets)
    gamma = GammaClient(fetch=_fetch_gamma)
    gamma_requests = 0
    source_requests = 0
    candidates: list[QualifiedCandidate] = []
    source_identities: dict[str, tuple[int, str]] = {}
    seen_market_ids = set(prior_market_ids)
    seen_conditions = set(prior_conditions)
    for start in _candidate_starts(authority):
        markets: list[tuple[Asset, Market]] = []
        payloads: list[tuple[Asset, bytes, str]] = []
        rejected = False
        for asset in selected_assets:
            gamma_requests += 1
            if gamma_requests > gamma_limit:
                raise RuntimeError("canary exceeded its Gamma request budget")
            try:
                market, payload, url = gamma.fetch_market(asset, start)
            except (IdentityError, UnresolvedMarketError):
                rejected = True
                break
            if (
                market.asset is not asset
                or market.timeframe != "1h"
                or market.market_start_ns != start * 1_000_000_000
                or market.market_end_ns - market.market_start_ns != 3_600_000_000_000
            ):
                raise RuntimeError("Gamma candidate violates exact 1h identity")
            markets.append((asset, market))
            payloads.append((asset, payload, url))
        if rejected:
            continue
        market_ids = {market.market_id for _, market in markets}
        conditions = {market.condition_id for _, market in markets}
        if (
            len(market_ids) != len(selected_assets)
            or len(conditions) != len(selected_assets)
            or market_ids & seen_market_ids
            or conditions & seen_conditions
        ):
            raise RuntimeError("Gamma reused an identity across canary assets or windows")
        seen_market_ids.update(market_ids)
        seen_conditions.update(conditions)
        source_objects = pmxt_hourly_objects(
            (start - 3_600) * 1_000_000_000,
            (start + 3_600) * 1_000_000_000,
        )
        if any(source.url in PMXT_MISSING_OBJECT_URLS for source in source_objects):
            continue
        for source in source_objects:
            if source.url in source_identities:
                continue
            source_requests += 1
            if source_requests > CANARY_MAX_SOURCE_OBJECTS:
                raise RuntimeError("canary exceeded its PMXT source-object budget")
            source_identities[source.url] = source_identity(source)
            if sum(length for length, _ in source_identities.values()) > CANARY_MAX_SOURCE_BYTES:
                raise RuntimeError("canary exceeded its PMXT source-transfer budget")
        candidates.append(QualifiedCandidate(start, tuple(markets), tuple(payloads)))
        break
    if not candidates:
        raise RuntimeError("bounded Actions discovery found no authoritative 1h candidates")
    starts = [candidate.start for candidate in candidates]
    if len({datetime.fromtimestamp(start, UTC).date() for start in starts}) != 1:
        raise RuntimeError("canary candidates must share one UTC source-reuse day")
    required_sources = pmxt_hourly_objects(
        (min(starts) - 3_600) * 1_000_000_000,
        (max(starts) + 3_600) * 1_000_000_000,
    )
    if any(source.url in PMXT_MISSING_OBJECT_URLS for source in required_sources):
        raise RuntimeError("canary source bundle crosses canonical PMXT absence")
    for source in required_sources:
        if source.url not in source_identities:
            source_requests += 1
            if source_requests > CANARY_MAX_SOURCE_OBJECTS:
                raise RuntimeError("canary exceeded its PMXT source-object budget")
            source_identities[source.url] = source_identity(source)
    if sum(length for length, _ in source_identities.values()) > CANARY_MAX_SOURCE_BYTES:
        raise RuntimeError("canary exceeded its PMXT source-transfer budget")
    return CanaryQualification(
        tuple(candidates),
        tuple((url, length, etag) for url, (length, etag) in sorted(source_identities.items())),
        gamma_requests,
        source_requests,
    )


def minimum_canary_cover(
    usable_by_start: dict[int, frozenset[Asset]],
    required_assets: tuple[Asset, ...] = tuple(Asset),
) -> tuple[int, ...]:
    if not required_assets or len(required_assets) != len(set(required_assets)):
        raise RuntimeError("canary cover requires a non-empty unique asset scope")
    bit_by_asset = {asset: 1 << index for index, asset in enumerate(required_assets)}
    required_mask = (1 << len(required_assets)) - 1
    best_by_mask: dict[int, tuple[int, ...]] = {0: ()}
    for start in sorted(usable_by_start, reverse=True):
        mask = sum(bit_by_asset.get(asset, 0) for asset in usable_by_start[start])
        if not mask:
            continue
        for covered, selected in tuple(best_by_mask.items()):
            combined = covered | mask
            candidate = (*selected, start)
            existing = best_by_mask.get(combined)
            if existing is None or len(candidate) < len(existing) or (
                len(candidate) == len(existing) and candidate > existing
            ):
                best_by_mask[combined] = candidate
    if required_mask in best_by_mask:
        return best_by_mask[required_mask]
    raise RuntimeError("bounded canary candidates provide no usable evidence cover")


def _adaptive_round_authorities(authority: Authority) -> tuple[Authority, ...]:
    return tuple(
        replace(
            authority,
            canary_search_start=start,
            canary_search_end=end,
            canary_step_minutes=step,
        )
        for start, end, step in authority.canary_rounds
    )


def _execute_canary_round(
    authority: Authority,
    qualification: CanaryQualification,
    assets: tuple[Asset, ...],
    run_id: str,
    round_index: int,
    disk_before: int,
    deadline: float,
) -> dict[str, Any]:
    starts = tuple(candidate.start for candidate in qualification.candidates)
    day = datetime.fromtimestamp(starts[0], UTC).date()
    coverage_start = datetime.fromtimestamp(min(starts), UTC)
    cutoff = datetime.fromtimestamp(max(starts) + 3_600, UTC)
    release_prefix = (
        f"{CANARY_RELEASE_PREFIX}-{run_id}-r{round_index}-{max(starts)}-{min(starts)}"
    )
    release_tag = f"{release_prefix}-{release_bucket(day)}"
    markets_by_asset: dict[Asset, dict[str, Market]] = {asset: {} for asset in assets}
    gamma_by_asset: dict[Asset, set[tuple[str, int, str]]] = {asset: set() for asset in assets}
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        work_root = root / "work"
        ledger_path = root / "ledger.json"
        expected_markets_path = root / "expected-markets.json"
        expected_sources_path = root / "expected-sources.json"
        for candidate in qualification.candidates:
            candidate_markets = dict(candidate.markets)
            for asset, payload, url in candidate.payloads:
                market = candidate_markets[asset]
                markets_by_asset[asset][market.market_id] = market
                gamma_by_asset[asset].add(
                    (url, len(payload), hashlib.sha256(payload).hexdigest())
                )
                slug = hourly_slug(asset, candidate.start)
                cache = work_root / f"{asset.value}-{day.isoformat()}" / "official"
                cache.mkdir(parents=True, exist_ok=True)
                (cache / f"{slug}.json").write_bytes(payload)
        _atomic_json(
            expected_markets_path,
            {
                asset.value: [
                    {
                        "condition_id": market.condition_id,
                        "token_ids": sorted((market.token_up, market.token_down)),
                    }
                    for market in sorted(
                        markets_by_asset[asset].values(), key=lambda item: item.market_start_ns
                    )
                ]
                for asset in assets
            },
        )
        _atomic_json(
            expected_sources_path,
            {
                url: {"byte_length": byte_length, "etag": etag}
                for url, byte_length, etag in qualification.source_objects
            },
        )
        minimum_free_disk = disk_before
        stop_sampling = threading.Event()

        def sample_disk() -> None:
            nonlocal minimum_free_disk
            while not stop_sampling.wait(1):
                minimum_free_disk = min(minimum_free_disk, shutil.disk_usage(root).free)

        sampler = threading.Thread(target=sample_disk, daemon=True)
        sampler.start()
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
            try:
                completed = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "scripts.run_backfill",
                        "--work-root",
                        str(work_root),
                        "--ledger",
                        str(ledger_path),
                        "--start",
                        day.isoformat(),
                        "--end",
                        day.isoformat(),
                        "--coverage-start",
                        coverage_start.isoformat(),
                        "--cutoff",
                        cutoff.isoformat(),
                        "--assets",
                        ",".join(asset.value for asset in assets),
                        "--market-starts",
                        ",".join(str(start) for start in starts),
                        "--release-prefix",
                        release_prefix,
                        "--expected-market-identities",
                        str(expected_markets_path),
                        "--expected-source-identities",
                        str(expected_sources_path),
                    ],
                    text=True,
                    capture_output=True,
                    check=False,
                    timeout=remaining,
                )
            except subprocess.TimeoutExpired as exc:
                if exc.stdout:
                    sys.stdout.write(str(exc.stdout))
                if exc.stderr:
                    sys.stderr.write(str(exc.stderr))
                raise RuntimeError(
                    "adaptive canary child exhausted its five-hour execution bound"
                ) from exc
        finally:
            stop_sampling.set()
            sampler.join()
        _raise_child_failure(completed)
        sys.stdout.write(completed.stdout)
        ledger = json.loads(ledger_path.read_bytes())
    if len(ledger["partitions"]) != len(assets):
        raise RuntimeError("adaptive round did not execute each uncovered asset")
    if any(
        item["markets"] != len(qualification.candidates)
        for item in ledger["partitions"].values()
    ):
        raise RuntimeError("adaptive round lost an authoritative candidate identity")
    inventory = remote_inventory(exact_tags={release_tag})
    expected_sources = frozenset(qualification.source_objects)
    proofs: dict[Asset, dict[str, Any]] = {}
    for asset in assets:
        if time.monotonic() >= deadline:
            raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
        partition = f"{asset.value}/1h/{day.isoformat()}"
        proof = verify_remote_partition(
            partition,
            inventory,
            expected_sources=expected_sources,
            expected_candidates=markets_by_asset[asset],
            expected_gamma=frozenset(gamma_by_asset[asset]),
        )
        if not proof["authenticated_redownload"]:
            raise RuntimeError("adaptive canary publication proof is incomplete")
        proofs[asset] = proof
        verify_remote_partition(
            partition,
            inventory,
            expected_sources=expected_sources,
            expected_candidates=markets_by_asset[asset],
            expected_gamma=frozenset(gamma_by_asset[asset]),
        )
    if time.monotonic() >= deadline:
        raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
    source_bytes = sum(int(item["source_bytes"]) for item in ledger["partitions"].values())
    source_owners = sum(
        int(item["source_bytes"]) > 0 for item in ledger["partitions"].values()
    )
    if source_owners != 1 or not 0 < source_bytes <= CANARY_MAX_SOURCE_BYTES:
        raise RuntimeError("adaptive round did not share one bounded PMXT transfer")
    return {
        "starts": starts,
        "release_tag": release_tag,
        "proofs": proofs,
        "markets_by_asset": markets_by_asset,
        "source_bytes": source_bytes,
        "source_owners": source_owners,
        "source_objects": len(qualification.source_objects),
        "gamma_requests": qualification.gamma_requests,
        "source_requests": qualification.source_requests,
        "minimum_free_disk": minimum_free_disk,
        "peak_rss_kib": max(
            int(item["peak_rss_kib"]) for item in ledger["partitions"].values()
        ),
        "canonical_bytes": sum(int(proof["bytes"]) for proof in proofs.values()),
    }


def command_canary() -> None:
    if not os.environ.get("GITHUB_TOKEN"):
        raise RuntimeError("canary requires authenticated GitHub remote authority")
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not run_id.isdigit():
        raise RuntimeError("canary requires its numeric GitHub Actions run identity")
    authority = load_authority()
    began = time.monotonic()
    deadline = began + CANARY_MAX_WALL_SECONDS
    disk_before = shutil.disk_usage(Path.cwd()).free
    source_truth = audit_source_truth(authority)
    production_before = remote_inventory()
    prior: dict[str, Any] = {
        "proofs": {},
        "markets": {},
        "release_tags": {},
        "gamma_requests": 0,
        "verified_bytes": 0,
        "configured": {},
        "invalidated": (),
    }
    proofs_by_asset = cast(dict[Asset, dict[str, Any]], prior["proofs"])
    reused_prior_assets = frozenset(proofs_by_asset)
    prior_markets = cast(dict[Asset, tuple[Market, ...]], prior["markets"])
    proof_tags = cast(dict[Asset, str], prior["release_tags"])
    uncovered = tuple(asset for asset in authority.assets if asset not in proofs_by_asset)
    usable_by_start: dict[int, set[Asset]] = {}
    for asset, proof in proofs_by_asset.items():
        for start in proof["accepted_market_starts"]:
            usable_by_start.setdefault(int(start), set()).add(asset)
    release_tags: list[str] = []
    prior_release_tags = sorted(set(proof_tags.values()))
    gamma_requests = int(prior["gamma_requests"])
    source_requests = 0
    source_objects = 0
    source_bytes = 0
    source_owners = 0
    prior_remote_verification_bytes = int(prior["verified_bytes"])
    canonical_bytes = prior_remote_verification_bytes
    minimum_free_disk = disk_before
    peak_rss_kib = 1
    checked_exclusion_assets = {
        asset for asset, proof in proofs_by_asset.items() if int(proof["exclusions"]) > 0
    }
    executed_rounds = 0
    configured_prior = cast(dict[Asset, dict[str, Any]], prior["configured"])
    qualified_starts = {
        int(start)
        for asset in proofs_by_asset
        for start in configured_prior[asset]["qualified_market_starts"]
    }
    qualified_new_starts: set[int] = set()
    seen_market_ids = {
        market.market_id for markets in prior_markets.values() for market in markets
    }
    seen_conditions = {
        market.condition_id for markets in prior_markets.values() for market in markets
    }
    fallback_starts = {
        candidate
        for round_authority in _adaptive_round_authorities(authority)
        for candidate in _candidate_starts(round_authority)
    }
    if qualified_starts & fallback_starts:
        raise RuntimeError("adaptive canary fallback overlaps reusable proof authority")

    for round_index, round_authority in enumerate(_adaptive_round_authorities(authority), 1):
        if not uncovered:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
        qualification = qualify_canary_candidates(
            round_authority,
            assets=uncovered,
            prior_market_ids=frozenset(seen_market_ids),
            prior_conditions=frozenset(seen_conditions),
        )
        if time.monotonic() >= deadline:
            raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
        for candidate in qualification.candidates:
            for _, market in candidate.markets:
                seen_market_ids.add(market.market_id)
                seen_conditions.add(market.condition_id)
        gamma_requests += qualification.gamma_requests
        source_requests += qualification.source_requests
        source_objects += len(qualification.source_objects)
        expected_bytes = sum(item[1] for item in qualification.source_objects)
        if (
            gamma_requests > CANARY_MAX_GAMMA_REQUESTS_TOTAL
            or source_requests > CANARY_MAX_SOURCE_OBJECTS_TOTAL
            or source_objects > CANARY_MAX_SOURCE_OBJECTS_TOTAL
            or source_bytes + expected_bytes > CANARY_MAX_SOURCE_BYTES_TOTAL
        ):
            raise RuntimeError("adaptive canary exceeded its frozen aggregate search bounds")
        result = _execute_canary_round(
            round_authority,
            qualification,
            uncovered,
            run_id,
            round_index,
            disk_before,
            deadline,
        )
        executed_rounds += 1
        release_tags.append(str(result["release_tag"]))
        source_bytes += int(result["source_bytes"])
        source_owners += int(result["source_owners"])
        canonical_bytes += int(result["canonical_bytes"])
        minimum_free_disk = min(minimum_free_disk, int(result["minimum_free_disk"]))
        peak_rss_kib = max(peak_rss_kib, int(result["peak_rss_kib"]))
        round_starts = {int(start) for start in result["starts"]}
        qualified_starts.update(round_starts)
        qualified_new_starts.update(round_starts)
        for start in round_starts:
            usable_by_start.setdefault(start, set())
        for asset in uncovered:
            proof = cast(dict[str, Any], result["proofs"][asset])
            if asset not in checked_exclusion_assets:
                sample_market = next(iter(result["markets_by_asset"][asset].values()))
                tier, exclusion = classify(
                    sample_market,
                    False,
                    [(sample_market.market_start_ns, sample_market.market_end_ns)],
                )
                if tier.value != "EXCLUDED" or exclusion is None or not exclusion.evidence:
                    raise RuntimeError(
                        "fail-closed exclusion contract lacks actual-market evidence"
                    )
                checked_exclusion_assets.add(asset)
            for start in proof["accepted_market_starts"]:
                usable_by_start[int(start)].add(asset)
            if proof["quality"] == "TIER_A" and proof["accepted_market_starts"]:
                proofs_by_asset[asset] = proof
                proof_tags[asset] = str(result["release_tag"])
        uncovered = tuple(asset for asset in uncovered if asset not in proofs_by_asset)

    if uncovered:
        names = ",".join(asset.value for asset in uncovered)
        raise RuntimeError(
            f"adaptive canary exhausted bounded search without usable proof: {names}"
        )
    if source_owners != executed_rounds:
        raise RuntimeError("each adaptive round must charge exactly one shared PMXT transfer")
    selected_starts = minimum_canary_cover(
        {start: frozenset(assets) for start, assets in usable_by_start.items()},
        authority.assets,
    )
    asset_market_starts = {
        asset.value: next(start for start in selected_starts if asset in usable_by_start[start])
        for asset in authority.assets
    }
    usable_market_starts_by_asset = {
        asset.value: sorted(
            start for start, usable_assets in usable_by_start.items() if asset in usable_assets
        )
        for asset in authority.assets
    }
    if remote_inventory() != production_before:
        raise RuntimeError("isolated canary publication changed production authority")
    if time.monotonic() >= deadline:
        raise RuntimeError("adaptive canary exhausted its five-hour execution bound")
    wall_seconds = time.monotonic() - began
    disk_after = shutil.disk_usage(Path.cwd()).free
    receipt: dict[str, Any] = {
        "schema_version": "1.0.0",
        "dataset_id": "polymarket-1h-seven-v1",
        "status": "PASSED",
        "timeframe": "1h",
        "assets": [asset.value for asset in authority.assets],
        "qualified_market_starts": sorted(qualified_starts),
        "selected_market_starts": list(selected_starts),
        "asset_market_starts": asset_market_starts,
        "usable_market_starts_by_asset": usable_market_starts_by_asset,
        "remote_proofs": {
            asset.value: {
                "accepted_market_starts": usable_market_starts_by_asset[asset.value],
                "manifest_sha256": proofs_by_asset[asset]["manifest_sha256"],
                "quality": proofs_by_asset[asset]["quality"],
                "accepted_market_bindings": proofs_by_asset[asset][
                    "accepted_market_bindings"
                ],
                "release_tag": proof_tags[asset],
                "tool_commit": proofs_by_asset[asset]["tool_commit"],
            }
            for asset in authority.assets
        },
        "common_window": len(selected_starts) == 1,
        "release_tags": release_tags,
        "prior_release_tags": prior_release_tags,
        "prior_evidence_assets": sorted(asset.value for asset in reused_prior_assets),
        "invalidated_prior_evidence_assets": sorted(
            asset.value for asset in cast(tuple[Asset, ...], prior["invalidated"])
        ),
        "executed_rounds": executed_rounds,
        "canary_release_prefix": CANARY_RELEASE_PREFIX,
        "isolated_from_production": True,
        "new_candidate_limit": CANARY_MAX_CANDIDATES_TOTAL,
        "qualified_new_candidates": len(qualified_new_starts),
        "qualified_candidates_total": len(qualified_starts),
        "gamma_requests": gamma_requests,
        "source_head_requests": source_requests,
        "settlement_bindings": len(authority.assets),
        "source_truth": source_truth,
        "usable_market_bindings": len(asset_market_starts),
        "shared_pmxt_objects": source_objects,
        "shared_source_transfer_owners": source_owners,
        "source_transfer_bytes": source_bytes,
        "canonical_bytes": canonical_bytes,
        "prior_remote_verification_bytes": prior_remote_verification_bytes,
        "authenticated_no_op_partitions": len(proofs_by_asset),
        "legitimate_exclusion_contract_checks": len(checked_exclusion_assets),
        "unexplained_failures": 0,
        "wall_seconds": wall_seconds,
        "timeout_margin_seconds": 21_600 - wall_seconds,
        "peak_rss_kib": peak_rss_kib,
        "disk_free_before_bytes": disk_before,
        "disk_free_after_bytes": disk_after,
        "minimum_free_disk_bytes": minimum_free_disk,
        "tool_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True, encoding="utf-8"
        ).strip(),
        "control_plane_sha256": _control_plane_digest(),
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    if receipt["timeout_margin_seconds"] <= 3_600 or minimum_free_disk < 8_000_000_000:
        raise RuntimeError("canary lacks six-hour timeout or disk safety margin")
    _atomic_json(CANARY_RECEIPT_PATH, receipt)
    print(json.dumps(receipt, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan")
    commands.add_parser("canary")
    commands.add_parser("reconcile")
    commands.add_parser("checkpoint")
    commands.add_parser("certify")
    commands.add_parser("validate-accelerated-matrix")
    day = commands.add_parser("execute-day")
    day.add_argument("--day", required=True)
    day.add_argument("--expected-release-group")
    compute = commands.add_parser("compute-day")
    compute.add_argument("--day", required=True)
    compute.add_argument("--expected-release-group", required=True)
    compute.add_argument("--expected-assets", required=True)
    compute.add_argument("--bundle-root", type=Path, required=True)
    segment = commands.add_parser("compute-day-segment")
    segment.add_argument("--day", required=True)
    segment.add_argument("--expected-release-group", required=True)
    segment.add_argument("--expected-assets", required=True)
    segment.add_argument("--bundle-root", type=Path, required=True)
    segment.add_argument("--segment-index", type=int, required=True)
    segment.add_argument("--segment-count", type=int, required=True)
    segment.add_argument("--retrieved-at-ns", type=int, required=True)
    assemble = commands.add_parser("assemble-day-segments")
    assemble.add_argument("--day", required=True)
    assemble.add_argument("--expected-release-group", required=True)
    assemble.add_argument("--expected-assets", required=True)
    assemble.add_argument("--segments-root", type=Path, required=True)
    assemble.add_argument("--bundle-root", type=Path, required=True)
    assemble.add_argument("--segment-count", type=int, required=True)
    assemble.add_argument("--retrieved-at-ns", type=int, required=True)
    publish = commands.add_parser("publish-staged-day")
    publish.add_argument("--day", required=True)
    publish.add_argument("--expected-release-group", required=True)
    publish.add_argument("--bundle-root", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "plan":
        command_plan()
    elif args.command == "canary":
        command_canary()
    elif args.command == "reconcile":
        authority = load_authority()
        _require_canary_receipt(authority)
        print(json.dumps(reconcile_ledger(remote_inventory(), authority), sort_keys=True))
    elif args.command == "checkpoint":
        command_checkpoint()
    elif args.command == "certify":
        command_certify()
    elif args.command == "validate-accelerated-matrix":
        command_validate_accelerated_matrix()
    elif args.command == "compute-day":
        command_compute_day(
            args.day,
            args.expected_release_group,
            args.expected_assets,
            args.bundle_root,
        )
    elif args.command == "compute-day-segment":
        command_compute_day_segment(
            args.day,
            args.expected_release_group,
            args.expected_assets,
            args.bundle_root,
            args.segment_index,
            args.segment_count,
            args.retrieved_at_ns,
        )
    elif args.command == "assemble-day-segments":
        command_assemble_day_segments(
            args.day,
            args.expected_release_group,
            args.expected_assets,
            args.segments_root,
            args.bundle_root,
            args.segment_count,
            args.retrieved_at_ns,
        )
    elif args.command == "publish-staged-day":
        command_publish_staged_day(args.day, args.expected_release_group, args.bundle_root)
    else:
        command_execute_day(args.day, args.expected_release_group)


if __name__ == "__main__":
    main()
