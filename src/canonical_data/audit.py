"""Small, deterministic contract checks and bounded HTTP probe helpers."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

from canonical_data.httpclient import USER_AGENT


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize JSON deterministically for manifests and digests."""
    serialized = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return (serialized + "\n").encode()


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def load_json(path: Path) -> Any:
    with path.open("rb") as handle:
        return json.load(handle)


def offline_audit(root: Path | None = None) -> dict[str, Any]:
    root = root or Path.cwd()
    sources = load_json(root / "config/sources.json")
    scope = load_json(root / "config/scope.json")
    pipeline = load_json(root / "config/pipeline.json")
    production = load_json(root / "config/production-plan.json")
    neutral_evidence = load_json(root / "docs/timeframe-neutral-evidence.json")
    schema = load_json(root / "schemas/partition-manifest.schema.json")
    ids = [source["id"] for source in sources["sources"]]
    errors: list[str] = []
    if len(ids) != len(set(ids)):
        errors.append("duplicate source id")
    if scope["timeframes"]["included"] != ["1h"]:
        errors.append("only 1h may be included")
    if scope["assets"] != ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"]:
        errors.append("asset scope changed")
    if schema.get("$schema") != "https://json-schema.org/draft/2020-12/schema":
        errors.append("manifest schema draft changed")
    if pipeline["execution"] != {
        "workers": 4,
        "sample_interval_ms": 200,
        "partition": "asset/1h/utc_date",
        "shared_source_unit": "six_hour_causal_segment",
    }:
        errors.append("pipeline execution boundary changed")
    limits = pipeline["resource_limits"]
    if limits["max_transformed_partition_bytes"] != 1_000_000_000:
        errors.append("transformed partition cap changed")
    # Two maximum source objects plus two maximum transformed partitions require
    # 3.6 GB; the rounded 4 GB floor is the fail-closed runaway reserve.
    if limits["minimum_free_disk_bytes"] < 4_000_000_000:
        errors.append("disk runaway reserve weakened")
    if limits["minimum_available_memory_bytes"] < 2_000_000_000:
        errors.append("memory headroom weakened")
    if production["dataset_id"] != "polymarket-1h-seven-v1":
        errors.append("production dataset identity changed")
    if (
        production.get("coverage_start") != "2026-04-18T20:00:00Z"
        or production.get("release_cutoff") != "2026-08-10T01:00:00Z"
    ):
        errors.append("production coverage boundary changed")
    catalog = neutral_evidence.get("refreshed_pmxt_catalog", {})
    validation = neutral_evidence.get("validation_coverage", {})
    pmxt_source: dict[str, Any] = next(
        (source for source in sources["sources"] if source["id"] == "pmxt_v2"), {}
    )
    missing_hours = [
        "2026-06-11T04:00:00Z",
        "2026-06-11T05:00:00Z",
        "2026-06-11T06:00:00Z",
    ]
    if (
        neutral_evidence.get("evidence_class")
        != "timeframe_neutral_raw_source_catalog_metadata"
        or catalog.get("object_start") != "2026-04-13T19:00:00Z"
        or catalog.get("object_end") != "2026-08-10T00:00:00Z"
        or catalog.get("objects") != 2835
        or catalog.get("expected_hour_keys") != 2838
        or catalog.get("missing_hour_keys") != missing_hours
        or validation.get("start") != production.get("coverage_start")
        or validation.get("cutoff") != production.get("release_cutoff")
        or validation.get("lookback_hours") != 1
        or pmxt_source.get("accessed_at") != "2026-08-24"
        or pmxt_source.get("catalog_url") != catalog.get("catalog_url")
        or pmxt_source.get("object_coverage_start") != catalog.get("object_start")
        or pmxt_source.get("object_coverage_end") != catalog.get("object_end")
        or pmxt_source.get("validation_coverage_start") != validation.get("start")
        or pmxt_source.get("validation_coverage_cutoff") != validation.get("cutoff")
    ):
        errors.append("PMXT coverage lacks timeframe-neutral evidence")
    if production["partition"] != {
        "unit": "asset/timeframe/utc_date",
        "assets": ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"],
        "timeframe": "1h",
    }:
        errors.append("production partition authority changed")
    if production["execution"].get("segment_count") != 4:
        errors.append("production segmentation changed")
    if production["execution"].get("maximum_parallel_compute_jobs") != 16:
        errors.append("production compute concurrency changed")
    if production["publication"]["release_prefix"] != "polymarket-1h-seven-v1":
        errors.append("production namespace changed")
    if (
        production["publication"].get("grouping") != "half_month"
        or production["publication"].get("maximum_assets_per_release") != 672
    ):
        errors.append("production release grouping is not bounded")
    return {
        "errors": errors,
        "source_count": len(ids),
        "scope_digest": sha256_bytes(canonical_json_bytes(scope)),
    }


def bounded_http_probe(url: str, byte_limit: int = 65536) -> dict[str, Any]:
    """Read at most byte_limit bytes; suitable for metadata/range feasibility only."""
    request = urllib.request.Request(
        url,
        headers={"Range": f"bytes=0-{byte_limit - 1}", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = response.read(byte_limit + 1)
        if len(payload) > byte_limit:
            raise ValueError("server exceeded bounded probe limit")
        return {
            "url": url,
            "status": response.status,
            "content_range": response.headers.get("Content-Range"),
            "accept_ranges": response.headers.get("Accept-Ranges"),
            "bytes_read": len(payload),
            "sha256": sha256_bytes(payload),
        }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--offline", action="store_true", help="run repository consistency only")
    parser.add_argument("--url", help="optional bounded range URL")
    parser.add_argument("--byte-limit", type=int, default=65536)
    args = parser.parse_args()
    result = offline_audit()
    if args.url and not args.offline:
        result["http"] = bounded_http_probe(args.url, args.byte_limit)
    print(canonical_json_bytes(result).decode(), end="")
    if result["errors"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
