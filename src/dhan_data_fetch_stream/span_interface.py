"""Strict, read-only contract for future point-in-time SPAN enrichment.

This module validates audited inputs and selects the latest eligible SPAN slot.
It intentionally does not enrich Dhan rows or claim that final gold is ready.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo


SPAN_INTERFACE_VERSION = "1.0"
SPAN_SLOT_LABELS = {
    "i1": "BOD",
    "i2": "ID1",
    "i3": "ID2",
    "i4": "ID3",
    "i5": "ID4",
    "s": "EOD",
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
IST = ZoneInfo("Asia/Kolkata")
_EFFECTIVE_TIME_SOURCES = {"official_metadata", "audited_file_timestamp", "unknown"}


@dataclass(frozen=True)
class SpanManifest:
    interface_version: str
    business_date: date
    slot_code: str
    slot_label: str
    effective_at: datetime | None
    effective_time_source: str
    source_path: str
    sha256: str
    row_count: int
    unique_key_count: int
    duplicate_key_count: int
    key_fields: tuple[str, ...]
    phase1_acceptance_status: str
    producer_evidence_sha256: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SpanManifest":
        required = {
            "interface_version",
            "business_date",
            "slot_code",
            "effective_at",
            "effective_time_source",
            "source_path",
            "sha256",
            "row_count",
            "unique_key_count",
            "duplicate_key_count",
            "key_fields",
            "phase1_acceptance_status",
            "producer_evidence_sha256",
        }
        missing = sorted(required.difference(value))
        if missing:
            raise ValueError(f"SPAN manifest missing fields: {', '.join(missing)}")
        slot_code = str(value["slot_code"]).strip().lower()
        if slot_code not in SPAN_SLOT_LABELS:
            raise ValueError(f"unknown SPAN slot_code: {slot_code}")
        effective_at = None if value["effective_at"] in (None, "") else _as_datetime(value["effective_at"])
        if effective_at is not None and (effective_at.tzinfo is None or effective_at.utcoffset() is None):
            raise ValueError("SPAN effective_at must be timezone-aware")
        effective_time_source = str(value["effective_time_source"]).strip().lower()
        if effective_time_source not in _EFFECTIVE_TIME_SOURCES:
            raise ValueError("invalid SPAN effective_time_source")
        if effective_at is None and effective_time_source != "unknown":
            raise ValueError("missing SPAN effective_at requires effective_time_source='unknown'")
        if effective_at is not None and effective_time_source == "unknown":
            raise ValueError("known SPAN effective_at requires a documented effective_time_source")
        business_date = _as_date(value["business_date"])
        digest = str(value["sha256"]).strip().lower()
        if not _SHA256.fullmatch(digest):
            raise ValueError("SPAN sha256 must be 64 lowercase hexadecimal characters")
        row_count = _non_negative_int(value["row_count"], "row_count")
        unique_count = _non_negative_int(value["unique_key_count"], "unique_key_count")
        duplicate_count = _non_negative_int(value["duplicate_key_count"], "duplicate_key_count")
        if unique_count + duplicate_count != row_count:
            raise ValueError("SPAN cardinality must satisfy unique_key_count + duplicate_key_count == row_count")
        if duplicate_count:
            raise ValueError("SPAN input has duplicate join keys")
        key_fields = tuple(str(item).strip() for item in value["key_fields"] if str(item).strip())
        if not key_fields:
            raise ValueError("SPAN key_fields must be non-empty")
        interface_version = str(value["interface_version"])
        if interface_version != SPAN_INTERFACE_VERSION:
            raise ValueError(
                f"unsupported SPAN interface_version {interface_version!r}; expected {SPAN_INTERFACE_VERSION!r}"
            )
        acceptance_status = str(value["phase1_acceptance_status"]).strip().lower()
        if acceptance_status != "accepted":
            raise ValueError("SPAN Phase 1 input is not accepted")
        producer_evidence_sha256 = str(value["producer_evidence_sha256"]).strip().lower()
        if not _SHA256.fullmatch(producer_evidence_sha256):
            raise ValueError("SPAN producer_evidence_sha256 must be 64 lowercase hexadecimal characters")
        return cls(
            interface_version=interface_version,
            business_date=business_date,
            slot_code=slot_code,
            slot_label=SPAN_SLOT_LABELS[slot_code],
            effective_at=effective_at,
            effective_time_source=effective_time_source,
            source_path=str(value["source_path"]),
            sha256=digest,
            row_count=row_count,
            unique_key_count=unique_count,
            duplicate_key_count=duplicate_count,
            key_fields=key_fields,
            phase1_acceptance_status=acceptance_status,
            producer_evidence_sha256=producer_evidence_sha256,
        )


@dataclass(frozen=True)
class SpanInputVerification:
    ok: bool
    status: str
    actual_sha256: str | None
    errors: tuple[str, ...]


@dataclass(frozen=True)
class SpanCardinality:
    status: str
    left_rows: int
    span_rows: int
    matched_left_rows: int
    unmatched_left_rows: int
    duplicate_span_keys: int


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def verify_span_input(manifest: SpanManifest, *, path: str | Path | None = None) -> SpanInputVerification:
    source = Path(manifest.source_path if path is None else path)
    errors: list[str] = []
    if not source.is_file():
        errors.append("source_file_missing")
        return SpanInputVerification(False, "invalid", None, tuple(errors))
    actual = sha256_file(source)
    if actual != manifest.sha256:
        errors.append("sha256_mismatch")
    return SpanInputVerification(not errors, "verified" if not errors else "invalid", actual, tuple(errors))


def select_effective_span_manifest(
    manifests: Iterable[SpanManifest],
    *,
    observation_ts: datetime,
    business_date: date | str,
) -> SpanManifest | None:
    if observation_ts.tzinfo is None or observation_ts.utcoffset() is None:
        raise ValueError("observation_ts must be timezone-aware")
    wanted_date = _as_date(business_date)
    observation_ist = observation_ts.astimezone(IST)
    eligible = [
        item
        for item in manifests
        if item.business_date == wanted_date
        and item.effective_at is not None
        and item.effective_at <= observation_ts
        and (item.slot_code != "s" or (observation_ist.hour, observation_ist.minute) >= (15, 30))
    ]
    if eligible:
        return max(eligible, key=lambda item: item.effective_at or datetime.min)
    # Unknown times are not guessed.  Only an accepted BOD file may serve as a
    # conservative session-wide fallback; unknown intraday/EOD slots remain ineligible.
    bod_fallback = [
        item
        for item in manifests
        if item.business_date == wanted_date
        and item.slot_code == "i1"
        and item.effective_at is None
        and item.effective_time_source == "unknown"
    ]
    if len(bod_fallback) > 1:
        raise ValueError("multiple unknown-time BOD SPAN manifests for one business date")
    return bod_fallback[0] if bod_fallback else None


def validate_join_cardinality(
    left_keys: Sequence[tuple[Any, ...]],
    span_keys: Sequence[tuple[Any, ...]],
) -> SpanCardinality:
    span_set = set(span_keys)
    duplicate_span_keys = len(span_keys) - len(span_set)
    matched = sum(key in span_set for key in left_keys)
    return SpanCardinality(
        status="ok" if duplicate_span_keys == 0 else "duplicate_span_keys",
        left_rows=len(left_keys),
        span_rows=len(span_keys),
        matched_left_rows=matched,
        unmatched_left_rows=len(left_keys) - matched,
        duplicate_span_keys=duplicate_span_keys,
    )


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _as_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _non_negative_int(value: Any, label: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"SPAN {label} must be non-negative")
    return parsed
