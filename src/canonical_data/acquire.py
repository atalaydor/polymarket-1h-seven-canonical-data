"""Bounded, verified and restart-safe source acquisition."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol

from canonical_data.errors import ResourceLimitError, SourceError, SourceIdentityError
from canonical_data.httpclient import USER_AGENT
from canonical_data.inventory import SourceObject


class Response(Protocol):
    headers: object

    def read(self, size: int = -1) -> bytes: ...

    def __enter__(self) -> Response: ...

    def __exit__(self, *args: object) -> None: ...


@dataclass(frozen=True)
class AcquiredObject:
    source: SourceObject
    path: Path
    byte_length: int
    sha256: str
    etag: str | None


class BoundedAcquirer:
    def __init__(
        self,
        work_dir: Path,
        max_object_bytes: int = 800_000_000,
        min_free_bytes: int = 8_000_000_000,
    ):
        self.work_dir = work_dir
        self.max_object_bytes = max_object_bytes
        self.min_free_bytes = min_free_bytes

    def _verify_headroom(self) -> None:
        self.work_dir.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(self.work_dir).free < self.min_free_bytes:
            raise ResourceLimitError("insufficient disk headroom")

    def acquire(
        self,
        source: SourceObject,
        expected_identity: tuple[int, str] | None = None,
        validator: Callable[[Path], None] | None = None,
    ) -> AcquiredObject:
        self._verify_headroom()
        name = hashlib.sha256(source.url.encode()).hexdigest() + ".source"
        final = self.work_dir / name
        partial = final.with_suffix(".partial")
        identity_path = final.with_suffix(".identity.json")
        if final.exists():
            try:
                return self._verify_existing(
                    source, final, identity_path, expected_identity, validator
                )
            except SourceError:
                final.unlink(missing_ok=True)
                identity_path.unlink(missing_ok=True)
        request = urllib.request.Request(
            source.url,
            headers={"Accept-Encoding": "identity", "User-Agent": USER_AGENT},
        )
        digest = hashlib.sha256()
        total = 0
        etag: str | None = None
        try:
            with (
                urllib.request.urlopen(request, timeout=60) as response,
                partial.open("wb") as output,
            ):
                headers = response.headers
                content_length = headers.get("Content-Length")
                etag = headers.get("ETag")
                try:
                    claimed_length = int(content_length) if content_length is not None else None
                except ValueError as exc:
                    raise SourceError("source content length is malformed") from exc
                if claimed_length is not None and claimed_length > self.max_object_bytes:
                    raise ResourceLimitError("source object exceeds acquisition cap")
                if expected_identity is not None and (claimed_length, etag) != expected_identity:
                    raise SourceIdentityError("source object identity changed during acquisition")
                while chunk := response.read(1_048_576):
                    total += len(chunk)
                    if total > self.max_object_bytes:
                        raise ResourceLimitError("stream exceeded acquisition cap")
                    output.write(chunk)
                    digest.update(chunk)
                output.flush()
                os.fsync(output.fileno())
            actual = digest.hexdigest()
            if claimed_length is not None and total != claimed_length:
                raise SourceError("source transfer length mismatch")
            self._verify_claims(source, total, actual)
            if validator is not None:
                validator(partial)
            os.replace(partial, final)
            metadata = {
                "byte_length": total,
                "etag": etag,
                "sha256": actual,
                "source_url": source.url,
            }
            identity_partial = identity_path.with_suffix(".partial")
            with identity_partial.open("w", encoding="utf-8", newline="\n") as output:
                json.dump(metadata, output, sort_keys=True, separators=(",", ":"))
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(identity_partial, identity_path)
            return AcquiredObject(source, final, total, actual, etag)
        except Exception:
            partial.unlink(missing_ok=True)
            raise

    def _verify_existing(
        self,
        source: SourceObject,
        path: Path,
        identity_path: Path,
        expected_identity: tuple[int, str] | None,
        validator: Callable[[Path], None] | None,
    ) -> AcquiredObject:
        try:
            metadata = json.loads(identity_path.read_bytes())
            stored_identity = (int(metadata["byte_length"]), metadata.get("etag"))
            stored_sha256 = str(metadata["sha256"])
            stored_url = str(metadata["source_url"])
        except (FileNotFoundError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SourceError("cached source identity is unavailable") from exc
        if stored_url != source.url:
            raise SourceIdentityError("cached source URL identity changed")
        if expected_identity is not None and stored_identity != expected_identity:
            raise SourceIdentityError("cached source object identity changed")
        digest = hashlib.sha256()
        total = 0
        with path.open("rb") as handle:
            while chunk := handle.read(1_048_576):
                total += len(chunk)
                digest.update(chunk)
        actual = digest.hexdigest()
        self._verify_claims(source, total, actual)
        if (total, actual) != (stored_identity[0], stored_sha256):
            raise SourceError("cached source content does not match its identity record")
        if validator is not None:
            validator(path)
        return AcquiredObject(source, path, total, actual, stored_identity[1])

    @staticmethod
    def _verify_claims(source: SourceObject, total: int, actual: str) -> None:
        if source.expected_bytes is not None and total != source.expected_bytes:
            raise SourceError("source byte length mismatch")
        if source.expected_sha256 is not None and actual != source.expected_sha256:
            raise SourceError("source checksum mismatch")


def copy_bounded(source: BinaryIO, target: BinaryIO, max_bytes: int) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    while chunk := source.read(min(1_048_576, max_bytes + 1 - total)):
        total += len(chunk)
        if total > max_bytes:
            raise ResourceLimitError("copy exceeded bound")
        target.write(chunk)
        digest.update(chunk)
    return total, digest.hexdigest()
