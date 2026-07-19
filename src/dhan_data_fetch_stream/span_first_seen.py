"""Credential-free NSE SPAN archive first-seen evidence collector."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from io import BytesIO
import os
from pathlib import Path
import time
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import Request, urlopen
import uuid
from zoneinfo import ZoneInfo
from zipfile import BadZipFile, ZipFile

import msvcrt

from .span_gold import _atomic_json, _read_json, _fsync_file


IST = ZoneInfo("Asia/Kolkata")
SLOTS = frozenset({"BOD", "ID1", "ID2", "ID3", "ID4", "EOD"})


@dataclass(frozen=True)
class FirstSeenResult:
    trading_date: str
    time_slot: str
    source_sha256: str
    first_seen_ts_ist: str
    source_url: str
    size_bytes: int
    manifest_path: str
    archive_path: str | None
    resumed: bool
    attempts: int


def poll_span_archive_first_seen(
    *,
    url: str,
    trading_date: str,
    time_slot: str,
    manifest_path: str | Path,
    archive_dir: str | Path | None = None,
    poll_seconds: float = 30.0,
    max_attempts: int = 20,
    timeout_seconds: float = 20.0,
    opener: Callable[..., Any] = urlopen,
) -> FirstSeenResult:
    """Poll a single exact archive URL until a valid ZIP SHA is first observed."""
    day = date.fromisoformat(trading_date)
    slot = time_slot.upper()
    if slot not in SLOTS:
        raise ValueError(f"invalid SPAN slot: {time_slot}")
    _validate_url(url)
    if not 1 <= max_attempts <= 10_000:
        raise ValueError("max_attempts must be in [1,10000]")
    if not 0 <= poll_seconds <= 3600:
        raise ValueError("poll_seconds must be in [0,3600]")
    if not 1 <= timeout_seconds <= 300:
        raise ValueError("timeout_seconds must be in [1,300]")

    target_manifest = Path(manifest_path).resolve()
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            payload = _download_valid_zip(url, timeout_seconds, opener)
            digest = sha256(payload).hexdigest()
            with _manifest_lock(target_manifest):
                records = _load_records(target_manifest)
                existing = next(
                    (
                        row
                        for row in records
                        if row["trading_date"] == day.isoformat()
                        and row["time_slot"] == slot
                        and row["source_sha256"] == digest
                    ),
                    None,
                )
                if existing is not None:
                    return _result(
                        existing, target_manifest, resumed=True, attempts=attempt
                    )
                observed = datetime.now(IST).isoformat()
                archive_path = _publish_archive(payload, digest, archive_dir)
                record = {
                    "trading_date": day.isoformat(),
                    "time_slot": slot,
                    "source_sha256": digest,
                    "first_seen_ts_ist": observed,
                    "source_url": url,
                    "size_bytes": len(payload),
                    "archive_path": str(archive_path) if archive_path else None,
                }
                records.append(record)
                records.sort(
                    key=lambda row: (
                        row["trading_date"],
                        row["time_slot"],
                        row["source_sha256"],
                    )
                )
                _atomic_json(target_manifest, records)
                return _result(record, target_manifest, resumed=False, attempts=attempt)
        except (OSError, TimeoutError, BadZipFile, ValueError) as exc:
            last_error = exc
            if attempt < max_attempts:
                time.sleep(poll_seconds)
    raise RuntimeError(
        f"no valid SPAN archive observed after {max_attempts} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    )


def _download_valid_zip(
    url: str, timeout_seconds: float, opener: Callable[..., Any]
) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; DhanSpanTimingEvidence/1.0)",
            "Accept": "application/zip,application/octet-stream,*/*",
        },
    )
    with opener(request, timeout=timeout_seconds) as response:
        status = int(getattr(response, "status", response.getcode()))
        if status != 200:
            raise OSError(f"HTTP {status}")
        payload = response.read()
    if len(payload) < 22 or not payload.startswith(b"PK"):
        raise BadZipFile("HTTP-200 response is not a ZIP archive")
    with ZipFile(BytesIO(payload)) as archive:
        if not archive.namelist():
            raise BadZipFile("archive has no members")
        corrupt = archive.testzip()
        if corrupt is not None:
            raise BadZipFile(f"archive member failed CRC: {corrupt}")
    return payload


def _load_records(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    payload = _read_json(path)
    if not isinstance(payload, list):
        raise ValueError("first-seen manifest must be a JSON array")
    required = {
        "trading_date",
        "time_slot",
        "source_sha256",
        "first_seen_ts_ist",
        "source_url",
        "size_bytes",
        "archive_path",
    }
    seen: set[tuple[str, str, str]] = set()
    for row in payload:
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError("first-seen manifest row schema mismatch")
        key = (row["trading_date"], row["time_slot"], row["source_sha256"])
        if key in seen:
            raise ValueError("duplicate first-seen manifest key")
        seen.add(key)
        parsed = datetime.fromisoformat(row["first_seen_ts_ist"])
        if parsed.tzinfo is None or parsed.date() < date.fromisoformat(
            row["trading_date"]
        ):
            raise ValueError("invalid first-seen timestamp")
    return payload


def _publish_archive(
    payload: bytes, digest: str, archive_dir: str | Path | None
) -> Path | None:
    if archive_dir is None:
        return None
    root = Path(archive_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{digest}.zip"
    if target.is_file():
        if sha256(target.read_bytes()).hexdigest() != digest:
            raise ValueError(f"existing archive hash mismatch: {target}")
        return target
    partial = target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_file(partial)
        os.replace(partial, target)
    finally:
        partial.unlink(missing_ok=True)
    return target


@contextmanager
def _manifest_lock(manifest: Path):
    lock_path = manifest.with_suffix(manifest.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def _validate_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in {
        "nseclearing.in",
        "www.nseclearing.in",
        "archives.nseindia.com",
    }:
        raise ValueError(
            "first-seen polling is restricted to official NSE/NSE Clearing HTTPS"
        )


def _result(
    record: dict[str, Any], manifest: Path, *, resumed: bool, attempts: int
) -> FirstSeenResult:
    return FirstSeenResult(
        trading_date=str(record["trading_date"]),
        time_slot=str(record["time_slot"]),
        source_sha256=str(record["source_sha256"]),
        first_seen_ts_ist=str(record["first_seen_ts_ist"]),
        source_url=str(record["source_url"]),
        size_bytes=int(record["size_bytes"]),
        manifest_path=str(manifest),
        archive_path=record.get("archive_path"),
        resumed=resumed,
        attempts=attempts,
    )
