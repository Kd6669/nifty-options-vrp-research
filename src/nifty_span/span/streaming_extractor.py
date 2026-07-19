"""Streaming, lineage-preserving extraction for downloaded NSE SPAN archives.

The downloader manifest is the authority for discovery.  This module deliberately
does not search the raw tree: every archive must be named in the manifest and must
validate against its recorded SHA-256 before any XML is read.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from concurrent.futures import ProcessPoolExecutor
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import uuid
from typing import Any, BinaryIO, Iterator, Mapping, Sequence
import xml.etree.ElementTree as ET
import xml.parsers.expat as expat
import zipfile
from zoneinfo import ZoneInfo

from .durable_jsonl import append_jsonl_record
from .manifest_exports import read_stable_jsonl_prefix


PARSER_VERSION = "span-stream-v2-expat"
SCHEMA_VERSION = "span-arrow-schema-v1"
SUCCESS_STATES = frozenset({"downloaded", "downloaded_existing"})
SLOT_ORDER = {"BOD": 0, "ID1": 1, "ID2": 2, "ID3": 3, "ID4": 4, "EOD": 5}
SUFFIX_TO_SLOT = {"i1": "BOD", "i2": "ID1", "i3": "ID2", "i4": "ID3", "i5": "ID4", "s": "EOD"}
BUSINESS_FIELDS = (
    "date", "time_slot", "symbol", "instrument", "expiry", "strike", "price", "delta",
    "implied_vol", "price_scan_range", "vol_scan_range", "cvf",
    *(f"s{i}" for i in range(1, 17)), "composite_delta",
)
LINEAGE_FIELDS = (
    "source_file", "source_sha256", "source_member", "parser_version", "ingested_at_utc",
    "slot_order", "span_file_created", "span_effective_ts_ist", "effective_time_source",
)
NATURAL_KEY = ("date", "time_slot", "symbol", "instrument", "expiry", "strike")
MAX_SPAN_MEMBER_BYTES = 512 * 1024 * 1024


class SpanExtractionError(RuntimeError):
    """Base class for deterministic extraction failures."""


class SpanManifestError(SpanExtractionError):
    """The downloader manifest is malformed or violates the raw layout contract."""


class SpanNaturalKeyConflictError(SpanExtractionError):
    """At least one natural key has disagreeing business values."""

    def __init__(self, message: str, *, quarantine_path: Path) -> None:
        super().__init__(message)
        self.quarantine_path = quarantine_path


@dataclass(frozen=True)
class ManifestArchive:
    trading_date: date
    slot: str
    suffix: str
    path: Path
    source_file: str
    sha256: str
    ingested_at_utc: datetime


@dataclass(frozen=True)
class SpanStreamingExtractionReport:
    manifest_archive_count: int
    created_fragment_count: int
    skipped_fragment_count: int
    failed_archive_count: int
    row_count: int
    fragments: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return self.failed_archive_count == 0


@dataclass(frozen=True)
class SpanMonthCompactionReport:
    year: int
    month: int
    fragment_count: int
    input_row_count: int
    output_row_count: int
    duplicate_row_count: int
    output_path: str
    changed: bool


@dataclass(frozen=True)
class _ArchiveExtractionResult:
    fragment: Path
    state: str
    row_count: int
    fragment_sha256: str | None
    event: dict[str, Any] | None


def span_arrow_schema() -> Any:
    """Return the stable 29-business-field plus nine-lineage-field schema."""
    import pyarrow as pa  # type: ignore[import-not-found]

    return pa.schema(
        [
            pa.field("date", pa.date32(), nullable=False),
            pa.field("time_slot", pa.string(), nullable=False),
            pa.field("symbol", pa.string(), nullable=False),
            pa.field("instrument", pa.string(), nullable=False),
            pa.field("expiry", pa.date32(), nullable=False),
            pa.field("strike", pa.float64(), nullable=False),
            pa.field("price", pa.float64(), nullable=False),
            pa.field("delta", pa.float64(), nullable=False),
            pa.field("implied_vol", pa.float64(), nullable=False),
            pa.field("price_scan_range", pa.float64(), nullable=False),
            pa.field("vol_scan_range", pa.float64(), nullable=False),
            pa.field("cvf", pa.float64(), nullable=False),
            *(pa.field(f"s{i}", pa.float64(), nullable=False) for i in range(1, 17)),
            pa.field("composite_delta", pa.float64(), nullable=False),
            pa.field("source_file", pa.string(), nullable=False),
            pa.field("source_sha256", pa.string(), nullable=False),
            pa.field("source_member", pa.string(), nullable=False),
            pa.field("parser_version", pa.string(), nullable=False),
            pa.field("ingested_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("slot_order", pa.int8(), nullable=False),
            pa.field("span_file_created", pa.string()),
            pa.field("span_effective_ts_ist", pa.timestamp("us", tz="Asia/Kolkata")),
            pa.field("effective_time_source", pa.string(), nullable=False),
        ]
    )


def load_manifest_archives(
    download_manifest: str | Path,
    raw_root: str | Path,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[ManifestArchive, ...]:
    """Load successful archives from JSONL and enforce ``raw/YYYY/MM/DD`` containment.

    The append-only log is reduced to its latest event per date/slot cell. A
    later failure or missing-slot event suppresses any stale earlier success.
    """
    manifest_path = Path(download_manifest)
    root = Path(raw_root).resolve()
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    latest: dict[tuple[date, str], tuple[dict[str, Any], int]] = {}
    first_success: dict[tuple[date, str, str, str], datetime] = {}
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SpanManifestError(f"invalid JSON at {manifest_path}:{line_number}: {exc}") from exc
            if not isinstance(event, dict):
                raise SpanManifestError(f"manifest event is not an object at {manifest_path}:{line_number}")
            day = _parse_date(_first(event, "date", "trading_date"))
            slot = str(_first(event, "slot", "time_slot") or "").upper().strip()
            suffix = str(_first(event, "suffix", "file_suffix") or "").lower().strip()
            if day is None and not slot and not suffix:
                continue
            if not slot and suffix:
                slot = SUFFIX_TO_SLOT.get(suffix, "")
            if not suffix and slot:
                suffix = next((key for key, value in SUFFIX_TO_SLOT.items() if value == slot), "")
            if day is None or slot not in SLOT_ORDER or not suffix:
                raise SpanManifestError(f"incomplete archive identity at {manifest_path}:{line_number}")
            if SUFFIX_TO_SLOT.get(suffix) != slot:
                raise SpanManifestError(
                    f"slot/suffix mismatch at {manifest_path}:{line_number}: slot={slot!r}, suffix={suffix!r}"
                )
            if start_date is not None and day < start_date:
                continue
            if end_date is not None and day > end_date:
                continue
            latest[(day, slot)] = (event, line_number)
            state = str(_first(event, "state", "status", "result") or "").lower()
            raw_path = _first(event, "path", "raw_path", "archive_path", "file_path")
            digest = str(_first(event, "sha256", "source_sha256") or "").lower().strip()
            timestamp = _event_timestamp(event)
            if state in SUCCESS_STATES and raw_path is not None and re.fullmatch(r"[0-9a-f]{64}", digest) and timestamp:
                first_success.setdefault((day, slot, str(raw_path), digest), timestamp)

    found: list[ManifestArchive] = []
    for (day, slot), (event, line_number) in latest.items():
        suffix = str(_first(event, "suffix", "file_suffix") or "").lower().strip()
        if not suffix:
            suffix = next(key for key, value in SUFFIX_TO_SLOT.items() if value == slot)
        state = str(_first(event, "state", "status", "result") or "").lower()
        if state not in SUCCESS_STATES:
            continue
        raw_path = _first(event, "path", "raw_path", "archive_path", "file_path")
        digest = str(_first(event, "sha256", "source_sha256") or "").lower().strip()
        if raw_path is None or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise SpanManifestError(f"latest successful event lacks path/SHA at {manifest_path}:{line_number}")
        timestamp = first_success.get((day, slot, str(raw_path), digest)) or _event_timestamp(event)
        if timestamp is None:
            raise SpanManifestError(f"archive event lacks a timezone-aware durable timestamp at {manifest_path}:{line_number}")
        path = Path(str(raw_path))
        if not path.is_absolute():
            path = root / path
        resolved = path.resolve()
        expected_dir = (root / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}").resolve()
        if resolved.parent != expected_dir or not _is_relative_to(resolved, root):
            raise SpanManifestError(
                f"archive path must be directly under raw/YYYY/MM/DD at {manifest_path}:{line_number}: {resolved}"
            )
        expected_name = f"nsccl.{day:%Y%m%d}.{suffix}.zip"
        if resolved.name != expected_name:
            raise SpanManifestError(
                f"archive filename mismatch at {manifest_path}:{line_number}: "
                f"expected={expected_name!r}, actual={resolved.name!r}"
            )
        logical_path = resolved.relative_to(root).as_posix()
        archive = ManifestArchive(day, slot, suffix, resolved, logical_path, digest, timestamp)
        found.append(archive)
    return tuple(sorted(found, key=lambda item: (item.trading_date, SLOT_ORDER[item.slot], str(item.path))))


def extract_manifest_archives(
    *,
    download_manifest: str | Path,
    raw_root: str | Path,
    fragment_root: str | Path,
    extraction_manifest: str | Path,
    symbols_filter: Sequence[str] = ("NIFTY",),
    batch_rows: int = 50_000,
    parser_version: str = PARSER_VERSION,
    start_date: date | None = None,
    end_date: date | None = None,
    max_workers: int = 4,
) -> SpanStreamingExtractionReport:
    """Materialize one immutable parquet fragment per manifest archive."""
    if batch_rows < 1:
        raise ValueError("batch_rows must be positive")
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    normalized_symbols = _normalize_symbols(symbols_filter)
    if not normalized_symbols:
        raise ValueError("symbols_filter must contain at least one symbol")
    if not str(parser_version).strip():
        raise ValueError("parser_version must be non-empty")
    extraction_identity = _extraction_identity(parser_version, normalized_symbols)
    archives = load_manifest_archives(
        download_manifest,
        raw_root,
        start_date=start_date,
        end_date=end_date,
    )
    fragments: list[str] = []
    created = skipped = failed = total_rows = 0
    manifest_out = Path(extraction_manifest)
    journaled_successes = _journaled_extraction_successes(manifest_out)
    worker_args = (
        Path(fragment_root), normalized_symbols, batch_rows, parser_version, extraction_identity
    )
    window_size = max_workers * 2
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        for offset in range(0, len(archives), window_size):
            window = archives[offset : offset + window_size]
            futures = [
                pool.submit(
                    _extract_one_archive,
                    archive,
                    *worker_args,
                    existing_success_journaled=(
                        _extraction_success_key(archive, extraction_identity)
                        in journaled_successes
                    ),
                )
                for archive in window
            ]
            # Consume in canonical archive order. Workers may finish out of order,
            # but the durable extraction manifest never does.
            for result in (future.result() for future in futures):
                fragments.append(str(result.fragment))
                if result.state == "created":
                    created += 1
                    total_rows += result.row_count
                elif result.state == "skipped":
                    skipped += 1
                else:
                    failed += 1
                if result.event is not None:
                    _append_jsonl(manifest_out, result.event)
                    if result.event["event"] in {
                        "fragment_created",
                        "fragment_already_valid",
                    }:
                        journaled_successes.add(
                            _extraction_success_key_from_event(result.event)
                        )
    return SpanStreamingExtractionReport(len(archives), created, skipped, failed, total_rows, tuple(fragments))


def _journaled_extraction_successes(path: Path) -> set[tuple[str, str, str, str]]:
    if not path.exists():
        return set()
    snapshot = read_stable_jsonl_prefix(path)
    if snapshot.ignored_trailing_bytes:
        raise SpanManifestError(
            f"extraction manifest has an unterminated tail: {path}"
        )
    latest: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
    for _line_number, event in snapshot.events:
        key = _extraction_success_key_from_event(event)
        if all(key):
            latest[key] = event
    return {
        key
        for key, event in latest.items()
        if event.get("event") in {"fragment_created", "fragment_already_valid"}
    }


def _extraction_success_key(
    archive: ManifestArchive, extraction_identity: str
) -> tuple[str, str, str, str]:
    return (
        archive.trading_date.isoformat(),
        archive.slot,
        archive.sha256,
        extraction_identity,
    )


def _extraction_success_key_from_event(
    event: Mapping[str, Any],
) -> tuple[str, str, str, str]:
    return (
        str(event.get("date", event.get("trading_date", ""))),
        str(event.get("slot", "")),
        str(event.get("source_sha256", "")),
        str(event.get("extraction_identity", "")),
    )


def _already_valid_event(
    *,
    archive: ManifestArchive,
    fragment: Path,
    fragment_root: Path,
    fragment_sha256: str,
    row_count: int,
    instrument_counts: Mapping[str, int],
    parser_version: str,
    symbols_filter: tuple[str, ...],
    extraction_identity: str,
) -> dict[str, Any]:
    return {
        "event": "fragment_already_valid",
        "date": archive.trading_date.isoformat(),
        "slot": archive.slot,
        "source_file": archive.source_file,
        "source_sha256": archive.sha256,
        "fragment_path": fragment.relative_to(fragment_root).as_posix(),
        "fragment_sha256": fragment_sha256,
        "fragment_size_bytes": fragment.stat().st_size,
        "parser_version": parser_version,
        "schema_version": SCHEMA_VERSION,
        "symbols_filter": list(symbols_filter),
        "extraction_identity": extraction_identity,
        "ingested_at_utc": archive.ingested_at_utc.isoformat(),
        "row_count": row_count,
        "instrument_counts": dict(instrument_counts),
    }


def _extract_one_archive(
    archive: ManifestArchive,
    fragment_root: Path,
    symbols_filter: tuple[str, ...],
    batch_rows: int,
    parser_version: str,
    extraction_identity: str,
    *,
    existing_success_journaled: bool = True,
) -> _ArchiveExtractionResult:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    fragment = _fragment_path(fragment_root, archive, extraction_identity)
    partial = fragment.with_name(f".{fragment.name}.{uuid.uuid4().hex}.partial")
    try:
        if fragment.exists():
            fragment_sha, row_count, instrument_counts = _validate_existing_fragment(
                fragment,
                archive=archive,
                parser_version=parser_version,
                symbols_filter=symbols_filter,
                extraction_identity=extraction_identity,
            )
            event = (
                None
                if existing_success_journaled
                else _already_valid_event(
                    archive=archive,
                    fragment=fragment,
                    fragment_root=fragment_root,
                    fragment_sha256=fragment_sha,
                    row_count=row_count,
                    instrument_counts=instrument_counts,
                    parser_version=parser_version,
                    symbols_filter=symbols_filter,
                    extraction_identity=extraction_identity,
                )
            )
            return _ArchiveExtractionResult(
                fragment, "skipped", row_count, fragment_sha, event
            )
        actual_sha = _sha256_file(archive.path)
        if actual_sha != archive.sha256:
            raise SpanExtractionError(
                f"SHA-256 mismatch for {archive.path}: manifest={archive.sha256}, actual={actual_sha}"
            )
        fragment.parent.mkdir(parents=True, exist_ok=True)
        partial.unlink(missing_ok=True)
        filter_json = json.dumps(symbols_filter, separators=(",", ":"))
        writer_schema = span_arrow_schema().with_metadata(
            {
                b"source_sha256": archive.sha256.encode("ascii"),
                b"parser_version": parser_version.encode("utf-8"),
                b"schema_version": SCHEMA_VERSION.encode("ascii"),
                b"symbols_filter": filter_json.encode("utf-8"),
                b"extraction_identity": extraction_identity.encode("ascii"),
                b"trading_date": archive.trading_date.isoformat().encode("ascii"),
                b"slot": archive.slot.encode("ascii"),
                b"source_file": archive.source_file.encode("utf-8"),
            }
        )
        writer = None
        batch: list[dict[str, Any]] = []
        rows_written = 0
        instrument_counts = {"FUT": 0, "CE": 0, "PE": 0}
        try:
            for row in iter_span_rows(
                archive.path,
                archive=archive,
                symbols_filter=symbols_filter,
                parser_version=parser_version,
            ):
                batch.append(row)
                instrument = str(row["instrument"])
                instrument_counts[instrument] = instrument_counts.get(instrument, 0) + 1
                if len(batch) >= batch_rows:
                    table = pa.Table.from_pylist(batch, schema=span_arrow_schema())
                    writer = writer or pq.ParquetWriter(partial, writer_schema, compression="zstd")
                    writer.write_table(table)
                    rows_written += len(batch)
                    batch.clear()
            if batch:
                table = pa.Table.from_pylist(batch, schema=span_arrow_schema())
                writer = writer or pq.ParquetWriter(partial, writer_schema, compression="zstd")
                writer.write_table(table)
                rows_written += len(batch)
                batch.clear()
            if rows_written == 0:
                raise SpanExtractionError(
                    f"coverage anomaly: no rows for symbols={symbols_filter!r} in {archive.source_file}"
                )
            assert writer is not None
            writer.add_key_value_metadata(
                {
                    "row_count": str(rows_written),
                    "instrument_counts": json.dumps(instrument_counts, sort_keys=True, separators=(",", ":")),
                }
            )
        finally:
            if writer is not None:
                writer.close()
        _fsync_file(partial)
        fragment_sha, _, validated_counts = _validate_fragment(
            partial,
            archive=archive,
            parser_version=parser_version,
            symbols_filter=symbols_filter,
            extraction_identity=extraction_identity,
            expected_rows=rows_written,
        )
        try:
            os.link(partial, fragment)
            partial.unlink()
            _fsync_directory(fragment.parent)
        except FileExistsError:
            partial.unlink(missing_ok=True)
            existing_sha, existing_rows, existing_counts = _validate_existing_fragment(
                fragment,
                archive=archive,
                parser_version=parser_version,
                symbols_filter=symbols_filter,
                extraction_identity=extraction_identity,
            )
            event = (
                None
                if existing_success_journaled
                else _already_valid_event(
                    archive=archive,
                    fragment=fragment,
                    fragment_root=fragment_root,
                    fragment_sha256=existing_sha,
                    row_count=existing_rows,
                    instrument_counts=existing_counts,
                    parser_version=parser_version,
                    symbols_filter=symbols_filter,
                    extraction_identity=extraction_identity,
                )
            )
            return _ArchiveExtractionResult(
                fragment, "skipped", existing_rows, existing_sha, event
            )
        event = {
            "event": "fragment_created",
            "date": archive.trading_date.isoformat(),
            "slot": archive.slot,
            "source_file": archive.source_file,
            "source_sha256": archive.sha256,
            "fragment_path": fragment.relative_to(fragment_root).as_posix(),
            "fragment_sha256": fragment_sha,
            "fragment_size_bytes": fragment.stat().st_size,
            "parser_version": parser_version,
            "schema_version": SCHEMA_VERSION,
            "symbols_filter": list(symbols_filter),
            "extraction_identity": extraction_identity,
            "ingested_at_utc": archive.ingested_at_utc.isoformat(),
            "row_count": rows_written,
            "instrument_counts": validated_counts,
        }
        return _ArchiveExtractionResult(fragment, "created", rows_written, fragment_sha, event)
    except Exception as exc:
        partial.unlink(missing_ok=True)
        event = {
            "event": "fragment_failed",
            "date": archive.trading_date.isoformat(),
            "slot": archive.slot,
            "source_file": archive.source_file,
            "source_sha256": archive.sha256,
            "fragment_path": fragment.relative_to(fragment_root).as_posix(),
            "parser_version": parser_version,
            "schema_version": SCHEMA_VERSION,
            "symbols_filter": list(symbols_filter),
            "extraction_identity": extraction_identity,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        return _ArchiveExtractionResult(fragment, "failed", 0, None, event)


def iter_span_rows(
    zip_path: str | Path,
    *,
    archive: ManifestArchive,
    symbols_filter: Sequence[str] = ("NIFTY",),
    parser_version: str = PARSER_VERSION,
) -> Iterator[dict[str, Any]]:
    """Yield rows while retaining at most one ``futPf``/``oopPf`` XML subtree."""
    allowed = {str(symbol).upper().strip() for symbol in symbols_filter}
    with zipfile.ZipFile(zip_path) as zipped:
        members = sorted(info for info in zipped.infolist() if info.filename.lower().endswith(".spn"))
        if not members:
            raise SpanExtractionError(f"archive contains no .spn member: {zip_path}")
        for info in members:
            _validate_member_name(info.filename)
            if info.file_size > MAX_SPAN_MEMBER_BYTES:
                raise SpanExtractionError(
                    f"SPAN member exceeds the 512 MiB extraction limit: {info.filename}"
                )
            with zipped.open(info) as handle:
                yield from _iter_member_rows(
                    handle,
                    archive=archive,
                    source_member=info.filename,
                    allowed_symbols=allowed,
                    parser_version=parser_version,
                )


def compact_span_month(
    *,
    fragment_root: str | Path,
    parquet_root: str | Path,
    quarantine_root: str | Path,
    year: int,
    month: int,
) -> SpanMonthCompactionReport:
    """Compact one month, deduplicating equality and failing closed on disagreement."""
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.compute as pc  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    if not 1 <= month <= 12:
        raise ValueError("month must be in 1..12")
    month_root = Path(fragment_root) / f"{year:04d}" / f"{month:02d}"
    fragments = sorted(month_root.glob("*/*.parquet")) if month_root.exists() else []
    tables: list[Any] = []
    for fragment in fragments:
        tables.append(pq.read_table(fragment, schema=span_arrow_schema()))
    table = pa.concat_tables(tables) if tables else pa.Table.from_pylist([], schema=span_arrow_schema())
    input_count = table.num_rows
    sort_names = list(NATURAL_KEY) + ["source_sha256", "source_member", "parser_version"]
    if table.num_rows:
        indices = pc.sort_indices(table, sort_keys=[(name, "ascending") for name in sort_names])
        table = table.take(indices)
    keep_indices: list[int] = []
    conflict_indices: list[int] = []
    duplicates = 0
    position = 0
    while position < table.num_rows:
        end = position + 1
        while end < table.num_rows and _arrow_key_equal(table, position, end):
            end += 1
        group = range(position, end)
        if all(_arrow_business_equal(table, position, candidate) for candidate in range(position + 1, end)):
            keep_indices.append(position)
            duplicates += end - position - 1
        else:
            conflict_indices.extend(group)
        position = end
    if conflict_indices:
        conflict_table = table.take(pa.array(conflict_indices, type=pa.int64()))
        quarantine_path = _write_quarantine_table(Path(quarantine_root), year, month, conflict_table)
        raise SpanNaturalKeyConflictError(
            f"{len(conflict_indices)} conflicting rows for {year:04d}-{month:02d}; compact output not written",
            quarantine_path=quarantine_path,
        )
    table = table.take(pa.array(keep_indices, type=pa.int64()))
    destination = Path(parquet_root) / f"{year:04d}_{month:02d}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        existing = pq.read_table(destination, schema=span_arrow_schema())
        if existing.equals(table):
            return SpanMonthCompactionReport(
                year, month, len(fragments), input_count, table.num_rows, duplicates, str(destination), False
            )
    partial = destination.with_name(destination.name + ".partial")
    pq.write_table(table, partial, compression="zstd", use_dictionary=["time_slot", "symbol", "instrument"])
    _fsync_file(partial)
    os.replace(partial, destination)
    _fsync_directory(destination.parent)
    return SpanMonthCompactionReport(
        year, month, len(fragments), input_count, table.num_rows, duplicates, str(destination), True
    )


def _iter_member_rows(
    handle: BinaryIO,
    *,
    archive: ManifestArchive,
    source_member: str,
    allowed_symbols: set[str],
    parser_version: str,
) -> Iterator[dict[str, Any]]:
    raw = handle.read()
    if len(raw) > MAX_SPAN_MEMBER_BYTES:
        raise SpanExtractionError(f"SPAN member exceeds the 512 MiB extraction limit: {source_member}")
    parser = expat.ParserCreate()

    def reject_entity(*_: Any) -> None:
        raise SpanExtractionError(f"XML entity declarations are not permitted in {source_member}")

    parser.EntityDeclHandler = reject_entity
    parser.ExternalEntityRefHandler = lambda *_: 0
    parser.Parse(raw, True)
    if not raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        block_pattern = re.compile(rb"<(futPf|oopPf)>(.*?)</\1>", re.DOTALL)
        blocks = tuple(block_pattern.finditer(raw))
        if blocks and len(blocks) == raw.count(b"<futPf>") + raw.count(b"<oopPf>"):
            try:
                rows = tuple(
                    _iter_standard_span_blocks(
                        raw,
                        blocks=blocks,
                        archive=archive,
                        source_member=source_member,
                        allowed_symbols=allowed_symbols,
                        parser_version=parser_version,
                    )
                )
            except (ET.ParseError, UnicodeDecodeError):
                pass
            else:
                yield from rows
                return
    yield from _iter_member_rows_expat(
        io.BytesIO(raw),
        archive=archive,
        source_member=source_member,
        allowed_symbols=allowed_symbols,
        parser_version=parser_version,
    )


def _iter_standard_span_blocks(
    raw: bytes,
    *,
    blocks: Sequence[re.Match[bytes]],
    archive: ManifestArchive,
    source_member: str,
    allowed_symbols: set[str],
    parser_version: str,
) -> Iterator[dict[str, Any]]:
    metadata: dict[str, str | None] = {"span_file_created": None, "span_effective_raw": None}
    root_match = re.search(rb"<([A-Za-z_][\w:.-]*)(?:\s[^>]*)?>", raw)
    if root_match is not None:
        opening = root_match.group(0)
        try:
            root = ET.fromstring(opening[:-1] + b"/>")
        except ET.ParseError:
            root = None
        if root is not None:
            for key in ("fileCreated", "created", "creationTime", "createdAt"):
                if root.attrib.get(key):
                    metadata["span_file_created"] = root.attrib[key].strip()
                    break
            for key in ("effectiveTimestamp", "effectiveDateTime", "effectiveTime"):
                if root.attrib.get(key):
                    metadata["span_effective_raw"] = root.attrib[key].strip()
                    break
    prefix = raw[: blocks[0].start()]
    for tag in ("fileCreated", "creationTime", "createdAt"):
        match = re.search(fr"<{tag}>\s*([^<]+?)\s*</{tag}>".encode(), prefix)
        if match and not metadata["span_file_created"]:
            metadata["span_file_created"] = match.group(1).decode("utf-8").strip()
            break
    for tag in ("effectiveTimestamp", "effectiveDateTime", "effectiveTime"):
        match = re.search(fr"<{tag}>\s*([^<]+?)\s*</{tag}>".encode(), prefix)
        if match and not metadata["span_effective_raw"]:
            metadata["span_effective_raw"] = match.group(1).decode("utf-8").strip()
            break
    effective, source = _effective_timestamp(metadata["span_effective_raw"])
    lineage = {
        "source_file": archive.source_file,
        "source_sha256": archive.sha256,
        "source_member": source_member,
        "parser_version": parser_version,
        "ingested_at_utc": archive.ingested_at_utc,
        "slot_order": SLOT_ORDER[archive.slot],
        "span_file_created": metadata["span_file_created"],
        "span_effective_ts_ist": effective,
        "effective_time_source": source,
    }
    pf_code_pattern = re.compile(rb"<pfCode>\s*([^<]+?)\s*</pfCode>")
    for match in blocks:
        block = match.group(0)
        pf_code = pf_code_pattern.search(block)
        if pf_code is None or pf_code.group(1).decode("utf-8").strip().upper() not in allowed_symbols:
            continue
        element = ET.fromstring(block)
        if match.group(1) == b"futPf":
            yield from _rows_from_fut_pf(element, archive, allowed_symbols, lineage)
        else:
            yield from _rows_from_oop_pf(element, archive, allowed_symbols, lineage)


def _iter_member_rows_expat(
    handle: BinaryIO,
    *,
    archive: ManifestArchive,
    source_member: str,
    allowed_symbols: set[str],
    parser_version: str,
) -> Iterator[dict[str, Any]]:
    metadata: dict[str, str | None] = {"span_file_created": None, "span_effective_raw": None}
    pending: list[dict[str, Any]] = []
    parser = expat.ParserCreate(namespace_separator="}")
    parser.buffer_text = True
    portfolio_tag: str | None = None
    portfolio_depth = 0
    portfolio_builder: ET.TreeBuilder | None = None
    portfolio_allowed: bool | None = None
    metadata_tag: str | None = None
    metadata_chunks: list[str] = []
    root_seen = False

    def local_name(name: str) -> str:
        return name.rsplit("}", 1)[-1]

    def start_element(name: str, attributes: dict[str, str]) -> None:
        nonlocal portfolio_tag, portfolio_depth, portfolio_builder, portfolio_allowed
        nonlocal metadata_tag, metadata_chunks, root_seen
        if portfolio_tag is not None and portfolio_builder is None:
            portfolio_depth += 1
            return
        tag = local_name(name)
        if not root_seen:
            root_seen = True
            normalized_attributes = {local_name(key): value for key, value in attributes.items()}
            for key in ("fileCreated", "created", "creationTime", "createdAt"):
                if normalized_attributes.get(key):
                    metadata["span_file_created"] = normalized_attributes[key].strip()
                    break
            for key in ("effectiveTimestamp", "effectiveDateTime", "effectiveTime"):
                if normalized_attributes.get(key):
                    metadata["span_effective_raw"] = normalized_attributes[key].strip()
                    break
        if portfolio_tag is None:
            if tag in {"futPf", "oopPf"}:
                portfolio_tag = tag
                portfolio_depth = 1
                portfolio_allowed = None
                portfolio_builder = ET.TreeBuilder()
                normalized_attributes = {local_name(key): value for key, value in attributes.items()}
                portfolio_builder.start(tag, normalized_attributes)
                parser.CharacterDataHandler = character_data
            elif tag in {"fileCreated", "creationTime", "createdAt", "effectiveTimestamp", "effectiveDateTime", "effectiveTime"}:
                metadata_tag = tag
                metadata_chunks = []
                parser.CharacterDataHandler = character_data
            return
        portfolio_depth += 1
        if portfolio_builder is not None:
            normalized_attributes = {local_name(key): value for key, value in attributes.items()}
            portfolio_builder.start(tag, normalized_attributes)

    def character_data(data: str) -> None:
        if portfolio_builder is not None:
            portfolio_builder.data(data)
        elif portfolio_tag is None and metadata_tag is not None:
            metadata_chunks.append(data)

    def end_element(name: str) -> None:
        nonlocal portfolio_tag, portfolio_depth, portfolio_builder, portfolio_allowed
        nonlocal metadata_tag, metadata_chunks
        if portfolio_tag is not None and portfolio_builder is None:
            portfolio_depth -= 1
            if portfolio_depth == 0:
                portfolio_tag = None
                portfolio_allowed = None
                parser.CharacterDataHandler = None
            return
        tag = local_name(name)
        if portfolio_tag is None:
            if metadata_tag == tag:
                text = "".join(metadata_chunks).strip()
                if text:
                    if tag in {"fileCreated", "creationTime", "createdAt"} and not metadata["span_file_created"]:
                        metadata["span_file_created"] = text
                    elif tag in {"effectiveTimestamp", "effectiveDateTime", "effectiveTime"} and not metadata["span_effective_raw"]:
                        metadata["span_effective_raw"] = text
                metadata_tag = None
                metadata_chunks = []
                parser.CharacterDataHandler = None
            return
        element = portfolio_builder.end(tag) if portfolio_builder is not None else None
        if tag == "pfCode" and portfolio_allowed is None and element is not None:
            portfolio_allowed = (element.text or "").strip().upper() in allowed_symbols
            if not portfolio_allowed:
                # pfCode precedes the large risk arrays in NSE SPAN portfolios.
                # Stop materializing this portfolio as soon as its symbol is known.
                portfolio_builder = None
                parser.CharacterDataHandler = None
        portfolio_depth -= 1
        if portfolio_depth != 0:
            return
        if portfolio_builder is not None and portfolio_allowed is not False:
            portfolio_element = portfolio_builder.close()
            effective, source = _effective_timestamp(metadata["span_effective_raw"])
            lineage = {
                "source_file": archive.source_file,
                "source_sha256": archive.sha256,
                "source_member": source_member,
                "parser_version": parser_version,
                "ingested_at_utc": archive.ingested_at_utc,
                "slot_order": SLOT_ORDER[archive.slot],
                "span_file_created": metadata["span_file_created"],
                "span_effective_ts_ist": effective,
                "effective_time_source": source,
            }
            row_iter = (
                _rows_from_fut_pf(portfolio_element, archive, allowed_symbols, lineage)
                if portfolio_tag == "futPf"
                else _rows_from_oop_pf(portfolio_element, archive, allowed_symbols, lineage)
            )
            pending.extend(row_iter)
        portfolio_tag = None
        portfolio_depth = 0
        portfolio_builder = None
        portfolio_allowed = None
        parser.CharacterDataHandler = None

    def reject_entity(*_: Any) -> None:
        raise SpanExtractionError(f"XML entity declarations are not permitted in {source_member}")

    parser.StartElementHandler = start_element
    parser.EndElementHandler = end_element
    parser.CharacterDataHandler = None
    parser.EntityDeclHandler = reject_entity
    parser.ExternalEntityRefHandler = lambda *_: 0
    while block := handle.read(1024 * 1024):
        parser.Parse(block, False)
        if pending:
            yield from pending
            pending.clear()
    parser.Parse(b"", True)
    if pending:
        yield from pending


def _rows_from_fut_pf(
    elem: ET.Element, archive: ManifestArchive, allowed: set[str], lineage: Mapping[str, Any]
) -> Iterator[dict[str, Any]]:
    symbol = _child_text(elem, "pfCode").upper()
    if symbol not in allowed:
        return
    pf_cvf = _float(_child_text(elem, "cvf"), 1.0)
    for fut in _children(elem, "fut"):
        expiry = _parse_date(_child_text(fut, "pe"))
        if expiry is None:
            continue
        scan = _child(fut, "scanRate")
        risk, composite = _risk(_child(fut, "ra"))
        yield _make_row(
            archive, symbol, "FUT", expiry, 0.0, _float(_child_text(fut, "p")),
            _float(_child_text(fut, "d")), 0.0,
            _float(_child_text(scan, "priceScan")) if scan is not None else 0.0,
            _float(_child_text(scan, "volScan")) if scan is not None else 0.0,
            _float(_child_text(fut, "cvf"), pf_cvf), risk, composite, lineage,
        )


def _rows_from_oop_pf(
    elem: ET.Element, archive: ManifestArchive, allowed: set[str], lineage: Mapping[str, Any]
) -> Iterator[dict[str, Any]]:
    symbol = _child_text(elem, "pfCode").upper()
    if symbol not in allowed:
        return
    pf_cvf = _float(_child_text(elem, "cvf"), 1.0)
    for series in _children(elem, "series"):
        expiry = _parse_date(_child_text(series, "pe"))
        if expiry is None:
            continue
        scan = _child(series, "scanRate")
        price_scan = _float(_child_text(scan, "priceScan")) if scan is not None else 0.0
        vol_scan = _float(_child_text(scan, "volScan")) if scan is not None else 0.0
        cvf = _float(_child_text(series, "cvf"), pf_cvf)
        for option in _children(series, "opt"):
            raw_type = _child_text(option, "o").upper()
            if not raw_type.startswith(("C", "P")):
                continue
            risk, composite = _risk(_child(option, "ra"))
            yield _make_row(
                archive, symbol, "CE" if raw_type.startswith("C") else "PE", expiry,
                _float(_child_text(option, "k")), _float(_child_text(option, "p")),
                _float(_child_text(option, "d")), _float(_child_text(option, "v")),
                price_scan, vol_scan, cvf, risk, composite, lineage,
            )


def _make_row(
    archive: ManifestArchive, symbol: str, instrument: str, expiry: date, strike: float,
    price: float, delta: float, implied_vol: float, price_scan: float, vol_scan: float,
    cvf: float, risk: tuple[float, ...], composite: float, lineage: Mapping[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "date": archive.trading_date, "time_slot": archive.slot, "symbol": symbol,
        "instrument": instrument, "expiry": expiry, "strike": strike, "price": price,
        "delta": delta, "implied_vol": implied_vol, "price_scan_range": price_scan,
        "vol_scan_range": vol_scan, "cvf": cvf, "composite_delta": composite,
    }
    row.update({f"s{i + 1}": value for i, value in enumerate(risk)})
    row.update(lineage)
    return row


def _risk(elem: ET.Element | None) -> tuple[tuple[float, ...], float]:
    if elem is None:
        return tuple(0.0 for _ in range(16)), 0.0
    values = [_float(child.text) for child in _children(elem, "a")]
    values.extend(0.0 for _ in range(16 - len(values)))
    return tuple(values[:16]), _float(_child_text(elem, "d"))


def _fragment_path(root: Path, archive: ManifestArchive, extraction_identity: str) -> Path:
    day = archive.trading_date
    filename = (
        f"{day.isoformat()}.{SLOT_ORDER[archive.slot]:02d}.{archive.slot}."
        f"{archive.sha256[:16]}.{extraction_identity[:16]}.parquet"
    )
    return root / f"{day.year:04d}" / f"{day.month:02d}" / f"{day.day:02d}" / filename


def _validate_existing_fragment(
    path: Path,
    *,
    archive: ManifestArchive,
    parser_version: str,
    symbols_filter: tuple[str, ...],
    extraction_identity: str,
) -> tuple[str, int, dict[str, int]]:
    return _validate_fragment(
        path,
        archive=archive,
        parser_version=parser_version,
        symbols_filter=symbols_filter,
        extraction_identity=extraction_identity,
        expected_rows=None,
    )


def _validate_fragment(
    path: Path,
    *,
    archive: ManifestArchive,
    parser_version: str,
    symbols_filter: tuple[str, ...],
    extraction_identity: str,
    expected_rows: int | None,
) -> tuple[str, int, dict[str, int]]:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    parquet_metadata = pq.read_metadata(path)
    file_metadata = parquet_metadata.metadata or {}
    expected_metadata = {
        b"source_sha256": archive.sha256,
        b"parser_version": parser_version,
        b"schema_version": SCHEMA_VERSION,
        b"symbols_filter": json.dumps(symbols_filter, separators=(",", ":")),
        b"extraction_identity": extraction_identity,
        b"trading_date": archive.trading_date.isoformat(),
        b"slot": archive.slot,
        b"source_file": archive.source_file,
    }
    for key, expected in expected_metadata.items():
        actual = file_metadata.get(key, b"").decode("utf-8", "replace")
        if actual != expected:
            raise SpanExtractionError(
                f"fragment metadata mismatch for {key.decode()}: expected={expected!r}, actual={actual!r}: {path}"
            )
    row_count = parquet_metadata.num_rows
    recorded_rows = file_metadata.get(b"row_count", b"").decode("ascii", "replace")
    if recorded_rows != str(row_count):
        raise SpanExtractionError(f"fragment row-count metadata mismatch: {path}")
    if row_count < 1:
        raise SpanExtractionError(f"fragment has zero rows: {path}")
    if expected_rows is not None and row_count != expected_rows:
        raise SpanExtractionError(f"fragment row-count mismatch: {path}")
    raw_counts = file_metadata.get(b"instrument_counts", b"").decode("utf-8", "replace")
    try:
        instrument_counts = {str(key): int(value) for key, value in json.loads(raw_counts).items()}
    except (AttributeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SpanExtractionError(f"fragment instrument-count metadata is invalid: {path}") from exc
    if sum(instrument_counts.values()) != row_count:
        raise SpanExtractionError(f"fragment instrument counts do not sum to row count: {path}")
    return _sha256_file(path), row_count, instrument_counts


def _write_quarantine_table(root: Path, year: int, month: int, table: Any) -> Path:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    digest = hashlib.sha256(
        json.dumps(
            [_jsonable(row) for row in table.to_pylist()],
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    destination = root / f"{year:04d}_{month:02d}" / f"conflicts.{digest}.parquet"
    if destination.exists():
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".partial")
    pq.write_table(table, partial, compression="zstd")
    _fsync_file(partial)
    os.replace(partial, destination)
    _fsync_directory(destination.parent)
    return destination


def _arrow_key_equal(table: Any, left: int, right: int) -> bool:
    return all(table[name][left].as_py() == table[name][right].as_py() for name in NATURAL_KEY)


def _arrow_business_equal(table: Any, left: int, right: int) -> bool:
    return all(table[name][left].as_py() == table[name][right].as_py() for name in BUSINESS_FIELDS)


def _effective_timestamp(raw: str | None) -> tuple[datetime | None, str]:
    if not raw:
        return None, "unknown"
    parsed = _parse_timestamp(raw)
    if parsed is None or parsed.tzinfo is None:
        return None, "unknown"
    return parsed.astimezone(ZoneInfo("Asia/Kolkata")), "span_effective_timestamp_explicit_offset"


def _parse_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_utc_timestamp(value: Any) -> datetime | None:
    parsed = _parse_timestamp(value)
    if parsed is None or parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _event_timestamp(event: Mapping[str, Any]) -> datetime | None:
    return _parse_utc_timestamp(
        _first(
            event,
            "observed_at_utc",
            "event_at_utc",
            "downloaded_at_utc",
            "timestamp",
            "created_at_utc",
            "recorded_at_utc",
        )
    )


def _normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()}))


def _extraction_identity(parser_version: str, symbols_filter: tuple[str, ...]) -> str:
    payload = {
        "parser_version": str(parser_version),
        "schema_version": SCHEMA_VERSION,
        "symbols_filter": symbols_filter,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _child(parent: ET.Element | None, name: str) -> ET.Element | None:
    if parent is None:
        return None
    return next((item for item in list(parent) if _local_name(item.tag) == name), None)


def _children(parent: ET.Element, name: str) -> list[ET.Element]:
    return [item for item in list(parent) if _local_name(item.tag) == name]


def _child_text(parent: ET.Element | None, name: str) -> str:
    item = _child(parent, name)
    return str(item.text or "").strip() if item is not None else ""


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else float(default)
    except (TypeError, ValueError):
        return float(default)


def _validate_member_name(name: str) -> None:
    pure = PurePosixPath(name.replace("\\", "/"))
    if pure.is_absolute() or ".." in pure.parts or not pure.name:
        raise SpanExtractionError(f"unsafe SPAN member path: {name!r}")


def _first(event: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if event.get(key) is not None:
            return event[key]
    archive = event.get("archive")
    if isinstance(archive, Mapping):
        for key in keys:
            if archive.get(key) is not None:
                return archive[key]
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _append_jsonl(path: Path, event: Mapping[str, Any]) -> None:
    append_jsonl_record(path, event)


def _fsync_file(path: Path) -> None:
    # Windows' CRT rejects fsync on a read-only descriptor; r+b is portable.
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


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value
