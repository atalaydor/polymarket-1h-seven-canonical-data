"""Atomic resumable partition checkpoints."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from canonical_data.audit import canonical_json_bytes
from canonical_data.errors import ConflictError


class Phase(StrEnum):
    INVENTORIED = "INVENTORIED"
    ACQUIRED = "ACQUIRED"
    TRANSFORMED = "TRANSFORMED"
    VERIFIED = "VERIFIED"
    PUBLISHED = "PUBLISHED"


ORDER = {phase: index for index, phase in enumerate(Phase)}


@dataclass(frozen=True)
class Checkpoint:
    partition_id: str
    phase: Phase
    identity_digest: str
    manifest_digest: str | None = None
    remote_release: str | None = None


class StateStore:
    def __init__(self, root: Path):
        self.root = root

    def _path(self, partition_id: str) -> Path:
        safe = partition_id.replace("/", "--")
        return self.root / f"{safe}.json"

    def load(self, partition_id: str) -> Checkpoint | None:
        path = self._path(partition_id)
        if not path.exists():
            return None
        raw = json.loads(path.read_bytes())
        return Checkpoint(
            partition_id=raw["partition_id"],
            phase=Phase(raw["phase"]),
            identity_digest=raw["identity_digest"],
            manifest_digest=raw.get("manifest_digest"),
            remote_release=raw.get("remote_release"),
        )

    def advance(self, checkpoint: Checkpoint) -> bool:
        previous = self.load(checkpoint.partition_id)
        if previous is not None:
            if previous.identity_digest != checkpoint.identity_digest:
                raise ConflictError("partition identity digest conflict")
            if (
                previous.manifest_digest
                and checkpoint.manifest_digest
                and previous.manifest_digest != checkpoint.manifest_digest
            ):
                raise ConflictError("partition manifest digest conflict")
            if ORDER[checkpoint.phase] < ORDER[previous.phase]:
                raise ConflictError("checkpoint phase regression")
            if checkpoint == previous:
                return False
        path = self._path(checkpoint.partition_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_json_bytes(
            {
                "partition_id": checkpoint.partition_id,
                "phase": checkpoint.phase.value,
                "identity_digest": checkpoint.identity_digest,
                "manifest_digest": checkpoint.manifest_digest,
                "remote_release": checkpoint.remote_release,
            }
        )
        temporary = path.with_suffix(".partial")
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        return True
