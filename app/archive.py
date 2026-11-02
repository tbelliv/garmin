"""Immutable raw archive.

Files are written once and never overwritten: each is written to a temporary
file, fsynced, then hard-linked into place (which fails if the target exists).
Paths stored in the ledger are relative to DATA_DIR so the archive can be
restored to a different mount point.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

FILE_MODE = 0o440
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class StoredFile:
    rel_path: str
    sha256: str
    size: int
    written: bool  # False when an identical file was already present


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_component(value: str) -> str:
    cleaned = _SAFE.sub("_", value).strip("._")
    return cleaned or "_"


def write_immutable(data_dir: Path, rel_path: str, data: bytes) -> StoredFile:
    """Write bytes at data_dir/rel_path unless the file already exists.

    If the file exists with identical content this is a no-op.  If it exists
    with different content a FileExistsError is raised: archived files are
    never replaced.
    """
    target = data_dir / rel_path
    digest = sha256_bytes(data)
    if target.exists():
        if sha256_file(target) == digest:
            return StoredFile(rel_path, digest, len(data), written=False)
        raise FileExistsError(f"refusing to overwrite archived file {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, FILE_MODE)
        os.link(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    dir_fd = os.open(target.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)
    return StoredFile(rel_path, digest, len(data), written=True)


def timestamp_slug(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%S%fZ")


def encode_json(payload: Any) -> bytes:
    """Serialize a decoded API response.

    The official client returns decoded JSON, so this re-encodes it.  Python's
    json round-trips strings, integers and floats exactly; key order is kept.
    """
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def api_response_path(endpoint: str, unit_key: str, year: str, slug: str) -> str:
    return (
        f"raw/api/{safe_component(endpoint)}/{safe_component(year)}/"
        f"{safe_component(unit_key)}/{slug}.json"
    )


def store_api_response(
    data_dir: Path,
    *,
    endpoint: str,
    unit_key: str,
    year: str,
    payload: Any,
    meta: dict[str, Any],
    previous_sha256: str | None,
) -> StoredFile:
    """Store a response and a metadata sidecar; skip if unchanged since last time."""
    body = encode_json(payload)
    digest = sha256_bytes(body)
    if previous_sha256 is not None and previous_sha256 == digest:
        return StoredFile("", digest, len(body), written=False)
    slug = timestamp_slug()
    rel = api_response_path(endpoint, unit_key, year, slug)
    stored = write_immutable(data_dir, rel, body)
    sidecar = dict(meta, sha256=digest, bytes=len(body), payload_file=Path(rel).name)
    write_immutable(
        data_dir,
        rel[: -len(".json")] + ".meta.json",
        json.dumps(sidecar, indent=2, sort_keys=True).encode("utf-8"),
    )
    return stored
