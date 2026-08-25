"""Canonical manifests, release indexes, checksums and provenance."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import ConflictError
from canonical_data.models import SCHEMA_VERSION, Asset, Exclusion, Provenance, QualityTier


def hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1_048_576):
            total += len(chunk)
            digest.update(chunk)
    return total, digest.hexdigest()


def write_canonical_json(path: Path, value: Any) -> str:
    payload = canonical_json_bytes(value)
    digest = hashlib.sha256(payload).hexdigest()
    temporary = path.with_suffix(path.suffix + ".partial")
    path.parent.mkdir(parents=True, exist_ok=True)
    with temporary.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    if path.exists() and path.read_bytes() != payload:
        temporary.unlink()
        raise ConflictError(f"immutable JSON conflict: {path}")
    os.replace(temporary, path)
    return digest


def build_manifest(
    directory: Path,
    asset: Asset,
    day: str,
    tier: QualityTier,
    provenance: Iterable[Provenance],
    exclusions: Iterable[Exclusion],
    tool_commit: str,
    statistics: dict[str, Any],
    coverage_start_ns: int,
    release_cutoff_ns: int,
) -> tuple[dict[str, Any], str]:
    file_entries = []
    for path in sorted(directory.glob("*.parquet")):
        length, digest = hash_file(path)
        file_entries.append({"path": path.name, "byte_length": length, "sha256": digest})
    provenance_rows = [
        {
            "source_id": item.source_id,
            "source_url": item.source_url,
            "retrieved_at_ns": item.retrieved_at_ns,
            "byte_length": item.byte_length,
            "sha256": item.sha256,
            "license_id": item.license_id,
            "source_precision": item.source_precision,
            "etag": item.etag,
            "upstream_checksum": item.upstream_checksum,
            "transformations": list(item.transformations),
        }
        for item in sorted(provenance, key=lambda row: (row.source_id, row.source_url, row.sha256))
    ]
    exclusion_rows = [
        {
            "market_id": item.market_id,
            "reason_code": item.reason_code.value,
            "detail": item.detail,
            "evidence": item.evidence,
        }
        for item in sorted(exclusions, key=lambda row: (row.market_id, row.reason_code.value))
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "polymarket-1h-seven-v1",
        "partition_id": f"{asset.value}/1h/{day}",
        "asset": asset.value,
        "venue": "polymarket",
        "timeframe": "1h",
        "quality_tier": tier.value,
        "files": file_entries,
        "provenance": provenance_rows,
        "inputs": [
            {
                "source_id": item["source_id"],
                "byte_length": item["byte_length"],
                "sha256": item["sha256"],
            }
            for item in provenance_rows
        ],
        "tool_commit": tool_commit,
        "parameters": {
            "sample_interval_ms": 200,
            "ordering": "receive,source,object,row_desc_on_exact_timestamp_tie",
        },
        "statistics": statistics,
        "coverage_start_ns": coverage_start_ns,
        "release_cutoff_ns": release_cutoff_ns,
        "exclusions": exclusion_rows,
    }
    digest = write_canonical_json(directory / "manifest.json", manifest)
    return manifest, digest


def verify_manifest(directory: Path) -> str:
    import json

    path = directory / "manifest.json"
    payload = path.read_bytes()
    manifest = json.loads(payload)
    if canonical_json_bytes(manifest) != payload:
        raise ConflictError("manifest is not canonical JSON")
    for item in manifest["files"]:
        actual_length, actual_digest = hash_file(directory / item["path"])
        if actual_length != item["byte_length"] or actual_digest != item["sha256"]:
            raise ConflictError(f"manifest file verification failed: {item['path']}")
    return hashlib.sha256(payload).hexdigest()


def build_release_index(
    path: Path,
    release_version: str,
    release_cutoff_ns: int,
    partition_manifests: Iterable[Path],
) -> str:
    partitions = []
    for manifest_path in sorted(partition_manifests, key=lambda item: str(item)):
        payload = manifest_path.read_bytes()
        import json

        manifest = json.loads(payload)
        if canonical_json_bytes(manifest) != payload:
            raise ConflictError("partition manifest is not canonical")
        asset, timeframe, day = manifest["partition_id"].split("/")
        partitions.append(
            {
                "partition_id": manifest["partition_id"],
                "manifest_path": (f"asset={asset}/timeframe={timeframe}/date={day}/manifest.json"),
                "byte_length": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return write_canonical_json(
        path,
        {
            "schema_version": "1.0.0",
            "release_version": release_version,
            "release_cutoff_ns": release_cutoff_ns,
            "partitions": sorted(partitions, key=lambda item: item["partition_id"]),
        },
    )


def build_notice(path: Path, sources_config: dict[str, Any]) -> str:
    notices = []
    for source in sources_config["sources"]:
        if source["class"] == "EXCLUDED":
            continue
        notices.append(
            {
                "source_id": source["id"],
                "source_url": source["url"],
                "license_id": source["license"],
                "role": source["role"],
                "attribution_required": source["license"] == "CC-BY-4.0",
            }
        )
    return write_canonical_json(
        path,
        {
            "schema_version": "1.0.0",
            "combined_dataset_license": None,
            "sources": sorted(notices, key=lambda item: item["source_id"]),
        },
    )
