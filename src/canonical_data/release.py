"""Draft GitHub Release publication and complete re-verification."""

from __future__ import annotations

import http.client
import json
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from canonical_data.errors import ConflictError, ResourceLimitError, SourceError
from canonical_data.httpclient import USER_AGENT
from canonical_data.manifest import hash_file

MAX_RELEASE_ASSET_BYTES = 1_900_000_000
TRANSIENT_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
RETRY_DELAYS = (2, 8, 32)


class GitHubAPIError(SourceError):
    def __init__(self, method: str, code: int):
        super().__init__(f"GitHub API {method} failed: {code}")
        self.code = code


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    byte_length: int
    sha256: str
    download_url: str
    state: str = "uploaded"
    asset_id: str | None = None


class ReleaseBackend(Protocol):
    def ensure_draft(self, tag: str) -> str: ...

    def list_assets(self, release_id: str) -> list[ReleaseAsset]: ...

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset: ...

    def download(self, asset: ReleaseAsset, target: Path) -> None: ...

    def delete_incomplete(self, release_id: str, asset: ReleaseAsset) -> None: ...

    def finalize(self, release_id: str) -> None: ...


def content_addressed_name(partition_id: str, path: Path, digest: str) -> str:
    return f"{partition_id.replace('/', '--')}--{digest}--{path.name}"


class Publisher:
    def __init__(self, backend: ReleaseBackend):
        self.backend = backend

    def publish_partition(
        self, tag: str, partition_id: str, directory: Path, finalize: bool = False
    ) -> list[ReleaseAsset]:
        release_id = self.backend.ensure_draft(tag)
        existing = {asset.name: asset for asset in self.backend.list_assets(release_id)}
        accepted: list[ReleaseAsset] = []
        for path in sorted(directory.iterdir()):
            if not path.is_file():
                continue
            length, digest = hash_file(path)
            if length >= MAX_RELEASE_ASSET_BYTES:
                raise ResourceLimitError("release asset exceeds repository cap")
            name = content_addressed_name(partition_id, path, digest)
            logical_prefix = f"{partition_id.replace('/', '--')}--"
            logical_suffix = f"--{path.name}"
            conflicts = [
                item
                for item in existing.values()
                if item.name.startswith(logical_prefix)
                and item.name.endswith(logical_suffix)
                and item.name != name
            ]
            if conflicts:
                raise ConflictError(f"conflicting remote asset identity: {path.name}")
            asset = existing.get(name)
            if asset is not None and asset.state != "uploaded":
                self.backend.delete_incomplete(release_id, asset)
                existing.pop(name)
                asset = None
            if asset is None:
                asset = self.backend.upload(release_id, name, path)
            if asset.state != "uploaded":
                raise ConflictError(f"uploaded asset is not durable: {path.name}")
            if asset.byte_length != length or asset.sha256 != digest:
                raise ConflictError(f"uploaded metadata mismatch: {path.name}")
            self._download_verify(asset, length, digest, directory)
            accepted.append(asset)
        if finalize:
            self.backend.finalize(release_id)
        return accepted

    def _download_verify(
        self, asset: ReleaseAsset, length: int, digest: str, directory: Path
    ) -> None:
        target = directory / f".{asset.name}.verify"
        try:
            self.backend.download(asset, target)
            actual_length, actual_digest = hash_file(target)
            if (actual_length, actual_digest) != (length, digest):
                raise ConflictError(f"download re-verification failed: {asset.name}")
        finally:
            target.unlink(missing_ok=True)


class DirectoryReleaseBackend:
    """Filesystem backend for exhaustive publication semantics tests."""

    def __init__(self, root: Path):
        self.root = root

    def ensure_draft(self, tag: str) -> str:
        directory = self.root / tag
        directory.mkdir(parents=True, exist_ok=True)
        marker = directory / ".draft"
        marker.touch(exist_ok=True)
        return tag

    def list_assets(self, release_id: str) -> list[ReleaseAsset]:
        result = []
        for path in sorted((self.root / release_id).iterdir()):
            if path.name.startswith("."):
                continue
            length, _ = hash_file(path)
            result.append(ReleaseAsset(path.name, length, _digest_from_name(path.name), str(path)))
        return result

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset:
        target = self.root / release_id / name
        if target.exists():
            raise ConflictError("backend refuses overwrite")
        temporary = target.with_suffix(target.suffix + ".partial")
        shutil.copyfile(path, temporary)
        os.replace(temporary, target)
        length, digest = hash_file(target)
        return ReleaseAsset(name, length, digest, str(target))

    def download(self, asset: ReleaseAsset, target: Path) -> None:
        shutil.copyfile(asset.download_url, target)

    def delete_incomplete(self, release_id: str, asset: ReleaseAsset) -> None:
        del release_id
        if asset.state == "uploaded":
            raise ConflictError("refusing to delete a durable release asset")
        Path(asset.download_url).unlink(missing_ok=True)

    def finalize(self, release_id: str) -> None:
        marker = self.root / release_id / ".draft"
        if not marker.exists():
            raise ConflictError("release is not draft")
        marker.rename(self.root / release_id / ".published")


class GitHubReleaseBackend:
    """Production GitHub API backend using the existing authenticated environment."""

    def __init__(self, repository: str, token: str | None = None):
        self.repository = repository
        self.token = token or os.environ.get("GITHUB_TOKEN") or ""
        if not self.token:
            raise SourceError("GitHub authenticated environment is unavailable")
        self.api = f"https://api.github.com/repos/{repository}"

    def _request(
        self,
        method: str,
        url: str,
        payload: bytes | None = None,
        content_type: str = "application/json",
    ) -> Any:
        delays = (0, *RETRY_DELAYS) if method in {"GET", "PATCH", "DELETE"} else (0,)
        last_error: Exception | None = None
        for delay in delays:
            if delay:
                time.sleep(delay)
            request = urllib.request.Request(
                url,
                data=payload,
                method=method,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.github+json",
                    "Content-Type": content_type,
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": USER_AGENT,
                },
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    body = response.read()
                return json.loads(body) if body else None
            except urllib.error.HTTPError as exc:
                if exc.code not in TRANSIENT_HTTP_STATUS:
                    raise GitHubAPIError(method, exc.code) from exc
                last_error = exc
            except (urllib.error.URLError, TimeoutError) as exc:
                last_error = exc
        assert last_error is not None
        if isinstance(last_error, urllib.error.HTTPError):
            raise GitHubAPIError(method, last_error.code) from last_error
        raise SourceError(
            f"GitHub API {method} transport failed after bounded retries"
        ) from last_error

    def ensure_draft(self, tag: str) -> str:
        matches = self._releases_by_tag(tag)
        if matches:
            return self._single_draft_id(matches)
        payload = json.dumps(
            {"tag_name": tag, "name": tag, "draft": True, "prerelease": False}
        ).encode()
        last_error: SourceError | None = None
        for attempt, delay in enumerate((0, *RETRY_DELAYS)):
            if delay:
                time.sleep(delay)
                matches = self._releases_by_tag(tag)
                if matches:
                    return self._single_draft_id(matches)
            created: dict[str, Any] | None = None
            try:
                created = self._request("POST", f"{self.api}/releases", payload)
            except SourceError as exc:
                last_error = exc
            matches = self._releases_by_tag(tag)
            if matches:
                return self._single_draft_id(matches)
            if created is not None:
                for reconcile_delay in RETRY_DELAYS:
                    time.sleep(reconcile_delay)
                    matches = self._releases_by_tag(tag)
                    if matches:
                        return self._single_draft_id(matches)
                raise SourceError("created GitHub release is absent from reconciled inventory")
            if (
                isinstance(last_error, GitHubAPIError)
                and last_error.code not in TRANSIENT_HTTP_STATUS
            ):
                raise last_error
            if attempt == len(RETRY_DELAYS):
                break
        assert last_error is not None
        raise last_error

    def _releases_by_tag(self, tag: str) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        for page in range(1, 11):
            releases = self._request("GET", f"{self.api}/releases?per_page=100&page={page}")
            matches.extend(item for item in releases if item.get("tag_name") == tag)
            if len(releases) < 100:
                break
        else:
            raise ResourceLimitError("GitHub release inventory exceeds bounded lookup")
        return matches

    @staticmethod
    def _single_draft_id(matches: list[dict[str, Any]]) -> str:
        if len(matches) > 1:
            raise ConflictError("multiple GitHub releases share the staged tag")
        release = matches[0]
        if not release.get("draft"):
            raise ConflictError("existing release is not staged as draft")
        return str(release["id"])

    def list_assets(self, release_id: str) -> list[ReleaseAsset]:
        result: list[ReleaseAsset] = []
        for page in range(1, 11):
            raw = self._request(
                "GET", f"{self.api}/releases/{release_id}/assets?per_page=100&page={page}"
            )
            result.extend(
                ReleaseAsset(
                    item["name"],
                    int(item["size"]),
                    _digest_from_name(item["name"]),
                    item["url"],
                    str(item.get("state", "")),
                    str(item["id"]),
                )
                for item in raw
            )
            if len(raw) < 100:
                return result
        raise ResourceLimitError("GitHub release reached the 1,000 asset limit")

    def upload(self, release_id: str, name: str, path: Path) -> ReleaseAsset:
        query = urllib.parse.urlencode({"name": name})
        length = path.stat().st_size
        if length >= MAX_RELEASE_ASSET_BYTES:
            raise ResourceLimitError("release asset exceeds repository cap")
        endpoint = f"/repos/{self.repository}/releases/{release_id}/assets?{query}"
        last_error: Exception | None = None
        for delay in (0, *RETRY_DELAYS):
            if delay:
                time.sleep(delay)
            connection = http.client.HTTPSConnection("uploads.github.com", timeout=120)
            try:
                connection.putrequest("POST", endpoint)
                connection.putheader("Authorization", f"Bearer {self.token}")
                connection.putheader("Accept", "application/vnd.github+json")
                connection.putheader("Content-Type", "application/octet-stream")
                connection.putheader("Content-Length", str(length))
                connection.putheader("X-GitHub-Api-Version", "2022-11-28")
                connection.putheader("User-Agent", USER_AGENT)
                connection.endheaders()
                with path.open("rb") as handle:
                    while chunk := handle.read(1_048_576):
                        connection.send(chunk)
                response = connection.getresponse()
                body = response.read()
                if response.status in {200, 201}:
                    raw = json.loads(body)
                    asset = ReleaseAsset(
                        raw["name"],
                        int(raw["size"]),
                        _digest_from_name(raw["name"]),
                        raw["url"],
                        str(raw.get("state", "")),
                        str(raw["id"]),
                    )
                    if asset.state != "uploaded":
                        self.delete_incomplete(release_id, asset)
                        last_error = ConflictError("upload returned a non-durable asset")
                    else:
                        return asset
                elif response.status not in TRANSIENT_HTTP_STATUS | {422}:
                    raise GitHubAPIError("POST", response.status)
                else:
                    last_error = GitHubAPIError("POST", response.status)
            except (OSError, http.client.HTTPException, TimeoutError) as exc:
                last_error = exc
            finally:
                connection.close()
            reconciled = self._reconcile_upload(release_id, name, length)
            if reconciled is not None:
                return reconciled
        assert last_error is not None
        if isinstance(last_error, GitHubAPIError):
            raise last_error
        raise SourceError("GitHub upload transport failed after bounded retries") from last_error

    def _reconcile_upload(self, release_id: str, name: str, length: int) -> ReleaseAsset | None:
        matches = [asset for asset in self.list_assets(release_id) if asset.name == name]
        if len(matches) > 1:
            raise ConflictError("multiple remote assets share one content-addressed name")
        if not matches:
            return None
        asset = matches[0]
        if asset.state != "uploaded":
            self.delete_incomplete(release_id, asset)
            return None
        if asset.byte_length != length or asset.sha256 != _digest_from_name(name):
            raise ConflictError("reconciled upload metadata mismatch")
        return asset

    def download(self, asset: ReleaseAsset, target: Path) -> None:
        last_error: Exception | None = None
        for delay in (0, *RETRY_DELAYS):
            if delay:
                time.sleep(delay)
            request = urllib.request.Request(
                asset.download_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/octet-stream",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": USER_AGENT,
                },
            )
            try:
                with (
                    urllib.request.urlopen(request, timeout=120) as response,
                    target.open("wb") as handle,
                ):
                    shutil.copyfileobj(response, handle, length=1_048_576)
                return
            except urllib.error.HTTPError as exc:
                target.unlink(missing_ok=True)
                if exc.code not in TRANSIENT_HTTP_STATUS:
                    raise GitHubAPIError("GET", exc.code) from exc
                last_error = exc
            except (urllib.error.URLError, TimeoutError) as exc:
                target.unlink(missing_ok=True)
                last_error = exc
        assert last_error is not None
        raise SourceError("GitHub download failed after bounded retries") from last_error

    def delete_incomplete(self, release_id: str, asset: ReleaseAsset) -> None:
        if asset.state == "uploaded":
            raise ConflictError("refusing to delete a durable release asset")
        if asset.asset_id is None:
            raise ConflictError("incomplete release asset lacks an API identity")
        self._request("DELETE", f"{self.api}/releases/assets/{asset.asset_id}")

    def finalize(self, release_id: str) -> None:
        self._request(
            "PATCH", f"{self.api}/releases/{release_id}", json.dumps({"draft": False}).encode()
        )


def _digest_from_name(name: str) -> str:
    parts = name.rsplit("--", 2)
    if (
        len(parts) != 3
        or len(parts[1]) != 64
        or not all(char in "0123456789abcdef" for char in parts[1])
    ):
        raise ConflictError("remote asset is not content-addressed")
    return parts[1]


def download_and_verify_release(
    backend: ReleaseBackend, release_id: str, target: Path
) -> list[Path]:
    target.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for asset in backend.list_assets(release_id):
        if asset.state != "uploaded":
            raise ConflictError(f"release contains a non-durable asset: {asset.name}")
        path = target / asset.name
        backend.download(asset, path)
        length, digest = hash_file(path)
        if length != asset.byte_length or digest != asset.sha256:
            raise ConflictError(f"release asset substituted: {asset.name}")
        paths.append(path)
    return paths
