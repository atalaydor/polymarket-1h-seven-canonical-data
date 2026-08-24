"""Seekable bounded HTTP range reader with substitution protection and a range ledger."""

from __future__ import annotations

import io
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, cast

from canonical_data.audit import sha256_bytes
from canonical_data.errors import ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT


@dataclass(frozen=True)
class RangeEvidence:
    offset: int
    byte_length: int
    sha256: str


FetchRange = Callable[[int, int], bytes]


class BoundedRangeReader(io.RawIOBase):
    def __init__(self, size: int, fetch: FetchRange, max_transfer_bytes: int):
        super().__init__()
        self.size = size
        self.fetch = fetch
        self.max_transfer_bytes = max_transfer_bytes
        self.transferred = 0
        self.position = 0
        self.ledger: list[RangeEvidence] = []

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            offset += self.position
        elif whence == io.SEEK_END:
            offset += self.size
        elif whence != io.SEEK_SET:
            raise ValueError("invalid whence")
        if offset < 0:
            raise ValueError("negative seek")
        self.position = min(offset, self.size)
        return self.position

    def readinto(self, buffer: Any) -> int:
        target = memoryview(buffer)
        if self.position >= self.size:
            return 0
        length = min(len(target), self.size - self.position)
        if self.transferred + length > self.max_transfer_bytes:
            raise ResourceLimitError("HTTP range transfer cap exceeded")
        payload = self.fetch(self.position, length)
        if len(payload) != length:
            raise SourceError("range response length mismatch")
        target[:length] = payload
        self.ledger.append(RangeEvidence(self.position, length, sha256_bytes(payload)))
        self.position += length
        self.transferred += length
        return length


@dataclass(frozen=True)
class HTTPObjectIdentity:
    url: str
    byte_length: int
    etag: str


def open_http_range(
    url: str, max_transfer_bytes: int
) -> tuple[io.BufferedReader, HTTPObjectIdentity, BoundedRangeReader]:
    head = urllib.request.Request(
        url,
        method="HEAD",
        headers={"Accept-Encoding": "identity", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(head, timeout=30) as response:
            length_header = response.headers.get("Content-Length")
            etag = response.headers.get("ETag")
            accepts = response.headers.get("Accept-Ranges")
    except urllib.error.URLError as exc:
        raise SourceError("PMXT HEAD request failed") from exc
    if length_header is None or etag is None or accepts != "bytes":
        raise SourceError("source does not prove stable byte-range support")
    size = int(length_header)

    def fetch(offset: int, length: int) -> bytes:
        request = urllib.request.Request(
            url,
            headers={
                "Range": f"bytes={offset}-{offset + length - 1}",
                "If-Match": etag,
                "Accept-Encoding": "identity",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                if response.status != 206 or response.headers.get("ETag") != etag:
                    raise SourceError("range source changed or ignored the requested range")
                payload = cast(bytes, response.read(length + 1))
        except urllib.error.URLError as exc:
            raise SourceError("PMXT range request failed") from exc
        if len(payload) != length:
            raise SourceError("range payload length mismatch")
        return payload

    raw = BoundedRangeReader(size, fetch, max_transfer_bytes)
    return io.BufferedReader(raw, buffer_size=64 * 1024), HTTPObjectIdentity(url, size, etag), raw
