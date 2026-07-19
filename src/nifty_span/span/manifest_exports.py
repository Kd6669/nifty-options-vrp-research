"""Deterministic, atomic exports of append-only SPAN JSONL manifests.

The live manifests are journals.  Research deliverables need a rectangular
latest-state view without racing an append in progress or discarding evidence
that is not represented by a first-class column.  This module reads only the
last newline-terminated prefix, selects the latest event for each manifest
identity, and writes canonical JSON plus typed Parquet.  ``event_json`` is a
lossless canonical copy of the complete source event.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence
import uuid


ManifestKind = Literal["download", "extraction"]
EXPORT_SCHEMA_VERSION = "span-manifest-export-v1"


@dataclass(frozen=True)
class StableJsonlPrefix:
    observed_bytes: int
    prefix_bytes: int
    ignored_trailing_bytes: int
    prefix_sha256: str
    event_count: int
    events: tuple[tuple[int, Mapping[str, Any]], ...]


@dataclass(frozen=True)
class ManifestExportReport:
    manifest_kind: str
    source_manifest: str
    source_prefix_sha256: str
    source_prefix_bytes: int
    source_observed_bytes: int
    ignored_trailing_bytes: int
    source_event_count: int
    latest_row_count: int
    superseded_event_count: int
    json_path: str
    json_sha256: str
    parquet_path: str
    parquet_sha256: str
    metadata_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def export_latest_manifest(
    source_manifest: str | Path,
    output_root: str | Path,
    *,
    manifest_kind: ManifestKind,
    stem: str | None = None,
) -> ManifestExportReport:
    """Export a journal's stable latest-state view and publish metadata last.

    Each artifact is replaced atomically.  The metadata sidecar is the commit
    marker for the set and contains hashes of both data artifacts.
    """
    if manifest_kind not in {"download", "extraction"}:
        raise ValueError("manifest_kind must be 'download' or 'extraction'")
    source = Path(source_manifest).resolve()
    if not source.is_file():
        raise FileNotFoundError(source)
    target_root = Path(output_root).resolve()
    target_root.mkdir(parents=True, exist_ok=True)
    output_stem = stem or f"{manifest_kind}_manifest"
    if not output_stem or Path(output_stem).name != output_stem:
        raise ValueError("stem must be a non-empty filename stem")

    snapshot = read_stable_jsonl_prefix(source)
    selected = _latest_events(snapshot.events, manifest_kind)
    rows = tuple(
        _event_to_row(event, line_number, manifest_kind)
        for _, (line_number, event) in sorted(selected.items())
    )
    schema = manifest_export_schema(manifest_kind)
    source_metadata = {
        "event_count": snapshot.event_count,
        "ignored_trailing_bytes": snapshot.ignored_trailing_bytes,
        "latest_row_count": len(rows),
        "observed_bytes": snapshot.observed_bytes,
        "prefix_bytes": snapshot.prefix_bytes,
        "prefix_sha256": snapshot.prefix_sha256,
        "superseded_event_count": snapshot.event_count - len(rows),
    }
    json_payload = {
        "columns": [
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
            for field in schema
        ],
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "identity_fields": list(_identity_field_names(manifest_kind)),
        "manifest_kind": manifest_kind,
        "rows": [_jsonable(row) for row in rows],
        "source": source_metadata,
    }
    json_bytes = (
        json.dumps(json_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")

    json_path = target_root / f"{output_stem}.json"
    parquet_path = target_root / f"{output_stem}.parquet"
    metadata_path = target_root / f"{output_stem}.metadata.json"
    _atomic_write_bytes(json_path, json_bytes)
    _atomic_write_parquet(
        parquet_path,
        rows,
        schema,
        manifest_kind=manifest_kind,
        source_metadata=source_metadata,
    )
    json_digest = _sha256_file(json_path)
    parquet_digest = _sha256_file(parquet_path)
    metadata_payload = {
        "artifacts": {
            "json": {
                "name": json_path.name,
                "sha256": json_digest,
                "size_bytes": json_path.stat().st_size,
            },
            "parquet": {
                "name": parquet_path.name,
                "sha256": parquet_digest,
                "size_bytes": parquet_path.stat().st_size,
            },
        },
        "export_schema_version": EXPORT_SCHEMA_VERSION,
        "manifest_kind": manifest_kind,
        "source": source_metadata,
    }
    metadata_bytes = (
        json.dumps(metadata_payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    _atomic_write_bytes(metadata_path, metadata_bytes)
    return ManifestExportReport(
        manifest_kind=manifest_kind,
        source_manifest=str(source),
        source_prefix_sha256=snapshot.prefix_sha256,
        source_prefix_bytes=snapshot.prefix_bytes,
        source_observed_bytes=snapshot.observed_bytes,
        ignored_trailing_bytes=snapshot.ignored_trailing_bytes,
        source_event_count=snapshot.event_count,
        latest_row_count=len(rows),
        superseded_event_count=snapshot.event_count - len(rows),
        json_path=str(json_path),
        json_sha256=json_digest,
        parquet_path=str(parquet_path),
        parquet_sha256=parquet_digest,
        metadata_path=str(metadata_path),
    )


def read_stable_jsonl_prefix(path: str | Path) -> StableJsonlPrefix:
    """Read exactly one byte snapshot, excluding a possibly in-flight tail."""
    source = Path(path)
    raw = source.read_bytes()
    boundary = raw.rfind(b"\n") + 1
    prefix = raw[:boundary]
    events: list[tuple[int, Mapping[str, Any]]] = []
    for line_number, encoded in enumerate(prefix.splitlines(), start=1):
        if not encoded.strip():
            continue
        try:
            event = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid stable manifest JSON at {source}:{line_number}: {exc}") from exc
        if not isinstance(event, Mapping):
            raise ValueError(f"manifest event is not an object at {source}:{line_number}")
        events.append((line_number, event))
    return StableJsonlPrefix(
        observed_bytes=len(raw),
        prefix_bytes=len(prefix),
        ignored_trailing_bytes=len(raw) - len(prefix),
        prefix_sha256=sha256(prefix).hexdigest(),
        event_count=len(events),
        events=tuple(events),
    )


def manifest_export_schema(manifest_kind: ManifestKind) -> Any:
    """Return the versioned rectangular Arrow schema for an export kind."""
    import pyarrow as pa  # type: ignore[import-not-found]

    common = [
        pa.field("manifest_kind", pa.string(), nullable=False),
        pa.field("identity_key", pa.string(), nullable=False),
        pa.field("source_line_number", pa.int64(), nullable=False),
    ]
    if manifest_kind == "download":
        fields = [
            *common,
            pa.field("trading_date", pa.date32(), nullable=False),
            pa.field("slot", pa.string(), nullable=False),
            pa.field("suffix", pa.string()),
            pa.field("state", pa.string()),
            pa.field("terminal", pa.bool_()),
            pa.field("attempt", pa.int32()),
            pa.field("schema_version", pa.int32()),
            pa.field("event_id", pa.string()),
            pa.field("run_id", pa.string()),
            pa.field("observed_at_utc", pa.timestamp("us", tz="UTC")),
            pa.field("http_status", pa.int16()),
            pa.field("path", pa.string()),
            pa.field("sha256", pa.string()),
            pa.field("size_bytes", pa.int64()),
            pa.field("zip_crc_ok", pa.bool_()),
            pa.field("error", pa.string()),
            pa.field("members", pa.list_(pa.string())),
            pa.field("returned_suffixes", pa.list_(pa.string())),
            pa.field("response_json", pa.string()),
            pa.field("outer_member_json", pa.string()),
            pa.field("inner_spn_json", pa.string()),
            pa.field("event_json", pa.string(), nullable=False),
        ]
    else:
        fields = [
            *common,
            pa.field("date", pa.date32(), nullable=False),
            pa.field("slot", pa.string(), nullable=False),
            pa.field("event", pa.string()),
            pa.field("source_file", pa.string()),
            pa.field("source_sha256", pa.string(), nullable=False),
            pa.field("fragment_path", pa.string()),
            pa.field("fragment_sha256", pa.string()),
            pa.field("fragment_size_bytes", pa.int64()),
            pa.field("parser_version", pa.string()),
            pa.field("schema_version", pa.string()),
            pa.field("symbols_filter", pa.list_(pa.string())),
            pa.field("extraction_identity", pa.string()),
            pa.field("ingested_at_utc", pa.timestamp("us", tz="UTC")),
            pa.field("row_count", pa.int64()),
            pa.field("instrument_counts_json", pa.string()),
            pa.field("error_type", pa.string()),
            pa.field("error", pa.string()),
            pa.field("event_json", pa.string(), nullable=False),
        ]
    return pa.schema(fields)


def _latest_events(
    events: Sequence[tuple[int, Mapping[str, Any]]], manifest_kind: ManifestKind
) -> dict[tuple[str, ...], tuple[int, Mapping[str, Any]]]:
    latest: dict[tuple[str, ...], tuple[int, Mapping[str, Any]]] = {}
    for line_number, event in events:
        identity = _identity(event, manifest_kind, line_number)
        latest[identity] = (line_number, event)
    return latest


def _identity(
    event: Mapping[str, Any], manifest_kind: ManifestKind, line_number: int
) -> tuple[str, ...]:
    if manifest_kind == "download":
        values = (str(event.get("trading_date", "")), str(event.get("slot", "")))
    else:
        values = (
            str(event.get("date", event.get("trading_date", ""))),
            str(event.get("slot", "")),
            str(event.get("source_sha256", "")),
        )
    if any(not value for value in values):
        names = ", ".join(_identity_field_names(manifest_kind))
        raise ValueError(f"missing manifest identity field ({names}) at line {line_number}")
    return values


def _identity_field_names(manifest_kind: ManifestKind) -> tuple[str, ...]:
    if manifest_kind == "download":
        return ("trading_date", "slot")
    return ("date", "slot", "source_sha256")


def _event_to_row(
    event: Mapping[str, Any], line_number: int, manifest_kind: ManifestKind
) -> dict[str, Any]:
    identity = _identity(event, manifest_kind, line_number)
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if manifest_kind == "download":
        return {
            "manifest_kind": manifest_kind,
            "identity_key": "|".join(identity),
            "source_line_number": line_number,
            "trading_date": _date(event.get("trading_date"), "trading_date", line_number),
            "slot": identity[1],
            "suffix": _string(event.get("suffix")),
            "state": _string(event.get("state")),
            "terminal": _bool(event.get("terminal"), "terminal", line_number),
            "attempt": _int(event.get("attempt"), "attempt", line_number),
            "schema_version": _int(event.get("schema_version"), "schema_version", line_number),
            "event_id": _string(event.get("event_id")),
            "run_id": _string(event.get("run_id")),
            "observed_at_utc": _timestamp(event.get("observed_at_utc"), "observed_at_utc", line_number),
            "http_status": _int(event.get("http_status"), "http_status", line_number),
            "path": _string(event.get("path")),
            "sha256": _string(event.get("sha256")),
            "size_bytes": _int(event.get("size_bytes"), "size_bytes", line_number),
            "zip_crc_ok": _bool(event.get("zip_crc_ok"), "zip_crc_ok", line_number),
            "error": _string(event.get("error")),
            "members": _string_list(event.get("members"), "members", line_number),
            "returned_suffixes": _string_list(
                event.get("returned_suffixes"), "returned_suffixes", line_number
            ),
            "response_json": _canonical_optional(event.get("response")),
            "outer_member_json": _canonical_optional(event.get("outer_member")),
            "inner_spn_json": _canonical_optional(event.get("inner_spn")),
            "event_json": canonical,
        }
    return {
        "manifest_kind": manifest_kind,
        "identity_key": "|".join(identity),
        "source_line_number": line_number,
        "date": _date(event.get("date", event.get("trading_date")), "date", line_number),
        "slot": identity[1],
        "event": _string(event.get("event")),
        "source_file": _string(event.get("source_file")),
        "source_sha256": identity[2],
        "fragment_path": _string(event.get("fragment_path")),
        "fragment_sha256": _string(event.get("fragment_sha256")),
        "fragment_size_bytes": _int(
            event.get("fragment_size_bytes"), "fragment_size_bytes", line_number
        ),
        "parser_version": _string(event.get("parser_version")),
        "schema_version": _string(event.get("schema_version")),
        "symbols_filter": _string_list(event.get("symbols_filter"), "symbols_filter", line_number),
        "extraction_identity": _string(event.get("extraction_identity")),
        "ingested_at_utc": _timestamp(event.get("ingested_at_utc"), "ingested_at_utc", line_number),
        "row_count": _int(event.get("row_count"), "row_count", line_number),
        "instrument_counts_json": _canonical_optional(event.get("instrument_counts")),
        "error_type": _string(event.get("error_type")),
        "error": _string(event.get("error")),
        "event_json": canonical,
    }


def _date(value: Any, name: str, line_number: int) -> date:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {name} at manifest line {line_number}: {value!r}") from exc


def _timestamp(value: Any, name: str, line_number: int) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid {name} at manifest line {line_number}: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware at manifest line {line_number}")
    return parsed.astimezone(UTC)


def _int(value: Any, name: str, line_number: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"invalid integer {name} at manifest line {line_number}: {value!r}")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid integer {name} at manifest line {line_number}: {value!r}") from exc


def _bool(value: Any, name: str, line_number: int) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"invalid boolean {name} at manifest line {line_number}: {value!r}")
    return value


def _string(value: Any) -> str | None:
    return None if value is None else str(value)


def _string_list(value: Any, name: str, line_number: int) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError(f"invalid list {name} at manifest line {line_number}: {value!r}")
    return [str(item) for item in value]


def _canonical_optional(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _atomic_write_parquet(
    target: Path,
    rows: Sequence[Mapping[str, Any]],
    schema: Any,
    *,
    manifest_kind: ManifestKind,
    source_metadata: Mapping[str, Any],
) -> None:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    metadata = {
        b"export_schema_version": EXPORT_SCHEMA_VERSION.encode("ascii"),
        b"manifest_kind": manifest_kind.encode("ascii"),
        b"source_prefix_sha256": str(source_metadata["prefix_sha256"]).encode("ascii"),
        b"source_prefix_bytes": str(source_metadata["prefix_bytes"]).encode("ascii"),
        b"source_event_count": str(source_metadata["event_count"]).encode("ascii"),
        b"latest_row_count": str(source_metadata["latest_row_count"]).encode("ascii"),
    }
    table = pa.Table.from_pylist(list(rows), schema=schema.with_metadata(metadata))
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = _partial_path(target)
    try:
        pq.write_table(
            table,
            partial,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
            data_page_version="1.0",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
        _fsync_file(partial)
        os.replace(partial, target)
        _fsync_directory(target.parent)
    finally:
        partial.unlink(missing_ok=True)


def _atomic_write_bytes(target: Path, content: bytes) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = _partial_path(target)
    try:
        with partial.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, target)
        _fsync_directory(target.parent)
    finally:
        partial.unlink(missing_ok=True)


def _partial_path(target: Path) -> Path:
    return target.with_name(f".{target.name}.{uuid.uuid4().hex}.partial")


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as handle:
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
