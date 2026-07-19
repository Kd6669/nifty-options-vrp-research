from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping
import json
import os
import zipfile

from .backfill_downloader import DOWNLOADED_STATES, MISSING_STATES, SLOT_SPECS
from .availability import (
    REPEATED_STATIC_BOUNDARY_EVENT,
    REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA,
)
from .streaming_extractor import NATURAL_KEY, span_arrow_schema


@dataclass(frozen=True)
class SpanAuditCell:
    trading_date: str
    slot: str
    suffix: str
    slot_order: int
    download_state: str
    terminal: bool
    observed_at_utc: str | None
    http_status: int | None
    raw_path: str | None
    source_sha256: str | None
    size_bytes: int | None
    raw_integrity_ok: bool | None
    extraction_state: str
    fragment_path: str | None
    fragment_exists: bool | None
    row_count: int | None
    accounted: bool
    issue: str | None
    availability_event_type: str | None = None
    availability_observed_at_utc: str | None = None
    calendar_classification: str | None = None
    classification_outcome: str | None = None
    source_boundary_proven: bool = False
    availability_source_ids: tuple[str, ...] = ()
    audit_disposition: str = "unresolved"


@dataclass(frozen=True)
class SpanAuditMonth:
    year: int
    month: int
    compacted_path: str
    exists: bool
    row_count: int
    natural_key_duplicates: int
    sha256: str | None
    instruments: tuple[str, ...]
    source_archive_count: int
    issue: str | None = None


@dataclass(frozen=True)
class SpanAuditSlotYear:
    """Latest-manifest state coverage for one calendar year and SPAN slot."""

    year: int
    slot: str
    suffix: str
    total_cells: int
    terminal_cells: int
    downloaded_valid_cells: int
    raw_missing_response_cells: int
    accepted_unavailable_cells: int
    unresolved_missing_cells: int
    manifest_missing_cells: int
    nonterminal_or_failed_cells: int
    extracted_valid_cells: int
    download_state_counts: dict[str, int]
    extraction_state_counts: dict[str, int]


@dataclass(frozen=True)
class SpanBackfillAuditReport:
    start_date: str
    end_date: str
    requested_dates: int
    expected_cells: int
    accounted_cells: int
    terminal_cells: int
    downloaded_cells: int
    unavailable_cells: int
    raw_missing_response_cells: int
    accepted_unavailable_cells: int
    unresolved_missing_cells: int
    source_boundary_cells: int
    resolved_or_blocked_cells: int
    unresolved_non_boundary_cells: int
    ambiguous_source_cells: int
    failed_or_incomplete_cells: int
    raw_integrity_failures: int
    downloaded_without_valid_extraction: int
    compacted_months: int
    compacted_rows: int
    duplicate_natural_keys: int
    unmanifested_raw_files: int
    unmanifested_fragments: int
    earliest_proven_download_date: str | None
    latest_proven_download_date: str | None
    download_manifest_path: str
    extraction_manifest_path: str
    availability_manifest_path: str | None
    download_manifest_sha256: str | None
    extraction_manifest_sha256: str | None
    availability_manifest_sha256: str | None
    matrix_complete: bool
    blocked_matrix_complete: bool
    raw_integrity_ok: bool
    extraction_complete: bool
    compacted_unique: bool
    outcome: str
    ok: bool
    matrix_parquet: str
    summary_json: str
    audit_markdown: str
    cells: tuple[SpanAuditCell, ...]
    months: tuple[SpanAuditMonth, ...]
    slot_year_counts: tuple[SpanAuditSlotYear, ...]

    def to_dict(self, *, include_cells: bool = True) -> dict[str, Any]:
        payload = asdict(self)
        if not include_cells:
            payload.pop("cells", None)
        return payload


def audit_span_backfill(
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    extraction_manifest: str | Path,
    fragment_root: str | Path,
    compacted_root: str | Path,
    report_root: str | Path,
    availability_manifest: str | Path | None = None,
) -> SpanBackfillAuditReport:
    """Reconcile every requested date/slot from request evidence through Parquet."""

    if start_date > end_date:
        raise ValueError(f"start date {start_date} must be <= end date {end_date}")
    raw = Path(raw_root).resolve()
    fragments = Path(fragment_root).resolve()
    compacted = Path(compacted_root).resolve()
    reports = Path(report_root).resolve()
    reports.mkdir(parents=True, exist_ok=True)

    download_manifest_path = Path(download_manifest).resolve()
    extraction_manifest_path = Path(extraction_manifest).resolve()
    availability_manifest_path = (
        None if availability_manifest is None else Path(availability_manifest).resolve()
    )
    download_latest, download_manifest_sha256 = _latest_download_events(
        download_manifest_path
    )
    if availability_manifest is None:
        availability_latest: Mapping[tuple[str, str], Mapping[str, Any]] = {}
        availability_manifest_sha256 = None
    else:
        from .availability import load_availability_events

        availability_latest = load_availability_events(availability_manifest_path)
        availability_manifest_sha256 = _optional_file_sha256(availability_manifest_path)
    extraction_latest, extraction_manifest_sha256 = _latest_extraction_events(
        extraction_manifest_path
    )
    cells: list[SpanAuditCell] = []
    expected_sources_by_month: dict[tuple[int, int], dict[str, tuple[str, str]]] = {}
    day = start_date
    while day <= end_date:
        for order, (slot, suffix) in enumerate(SLOT_SPECS):
            event = download_latest.get((day.isoformat(), slot))
            availability_event = availability_latest.get((day.isoformat(), slot))
            cell = _audit_cell(
                day=day,
                slot=slot,
                suffix=suffix,
                order=order,
                event=event,
                availability_event=availability_event,
                raw_root=raw,
                fragment_root=fragments,
                extraction_latest=extraction_latest,
            )
            cells.append(cell)
            if cell.download_state in DOWNLOADED_STATES and cell.source_sha256:
                expected_sources_by_month.setdefault((day.year, day.month), {})[
                    cell.source_sha256
                ] = (cell.trading_date, cell.slot)
        day += timedelta(days=1)

    months = tuple(
        _audit_month(
            compacted_root=compacted,
            year=year,
            month=month,
            expected_sources=sources,
        )
        for (year, month), sources in sorted(expected_sources_by_month.items())
    )
    slot_year_counts = _slot_year_counts(
        cells,
        download_latest=download_latest,
        availability_latest=availability_latest,
    )
    proven_download_dates = sorted(
        {
            cell.trading_date
            for cell in cells
            if cell.download_state in DOWNLOADED_STATES
            and cell.raw_integrity_ok is True
        }
    )
    expected_cells = ((end_date - start_date).days + 1) * len(SLOT_SPECS)
    accounted = sum(cell.accounted for cell in cells)
    terminal = sum(cell.terminal for cell in cells)
    downloaded = sum(cell.download_state in DOWNLOADED_STATES for cell in cells)
    unavailable = sum(cell.download_state in MISSING_STATES for cell in cells)
    ambiguous = sum(
        cell.download_state in MISSING_STATES
        and not _has_independent_absence_classification(
            download_latest[(cell.trading_date, cell.slot)],
            availability_latest.get((cell.trading_date, cell.slot)),
        )
        for cell in cells
        if (cell.trading_date, cell.slot) in download_latest
    )
    raw_failures = sum(cell.raw_integrity_ok is False for cell in cells)
    extraction_failures = sum(
        cell.download_state in DOWNLOADED_STATES
        and not (
            cell.extraction_state in {"fragment_created", "fragment_already_valid"}
            and cell.fragment_exists
        )
        for cell in cells
    )
    failed_or_incomplete = sum(
        not cell.terminal
        or cell.download_state not in DOWNLOADED_STATES | MISSING_STATES
        or (
            cell.download_state in MISSING_STATES
            and not _has_independent_absence_classification(
                download_latest[(cell.trading_date, cell.slot)],
                availability_latest.get((cell.trading_date, cell.slot)),
            )
        )
        for cell in cells
    )
    boundary_cells = sum(
        _is_current_source_boundary(
            download_latest.get((cell.trading_date, cell.slot)),
            availability_latest.get((cell.trading_date, cell.slot)),
        )
        for cell in cells
    )
    resolved_or_blocked = sum(
        cell.terminal
        or _is_current_source_boundary(
            download_latest.get((cell.trading_date, cell.slot)),
            availability_latest.get((cell.trading_date, cell.slot)),
        )
        for cell in cells
    )
    unresolved_non_boundary = sum(
        not _is_current_source_boundary(
            download_latest.get((cell.trading_date, cell.slot)),
            availability_latest.get((cell.trading_date, cell.slot)),
        )
        and (
            not cell.terminal
            or cell.download_state not in DOWNLOADED_STATES | MISSING_STATES
            or (
                cell.download_state in MISSING_STATES
                and not _has_independent_absence_classification(
                    download_latest[(cell.trading_date, cell.slot)],
                    availability_latest.get((cell.trading_date, cell.slot)),
                )
            )
        )
        for cell in cells
    )
    duplicate_keys = sum(month.natural_key_duplicates for month in months)
    month_failures = sum(
        not month.exists or month.issue is not None for month in months
    )
    unmanifested_raw = _count_unmanifested_raw(raw, cells, start_date, end_date)
    unmanifested_fragments = _count_unmanifested_fragments(
        fragments, cells, start_date, end_date
    )
    matrix_complete = accounted == expected_cells and terminal == expected_cells
    blocked_matrix_complete = (
        accounted == expected_cells
        and resolved_or_blocked == expected_cells
        and boundary_cells > 0
        and unresolved_non_boundary == 0
    )
    raw_ok = raw_failures == 0
    extraction_ok = extraction_failures == 0 and month_failures == 0
    unique = duplicate_keys == 0
    integrity_failure = (
        not raw_ok
        or not extraction_ok
        or not unique
        or unmanifested_raw > 0
        or unmanifested_fragments > 0
    )
    blocked_source = boundary_cells > 0
    if integrity_failure:
        outcome = "FAIL_INTEGRITY"
    elif blocked_source and blocked_matrix_complete:
        # Boundary cells may remain intentionally nonterminal (for example, an
        # exact official archive that is reproducibly corrupt).  Every other
        # cell must still be completely and independently accounted for.
        outcome = "BLOCKED_SOURCE"
    elif not matrix_complete or failed_or_incomplete > 0:
        outcome = "FAIL_INCOMPLETE"
    else:
        outcome = "PASS_READY"

    matrix_path = reports / "span_date_slot_matrix.parquet"
    summary_path = reports / "span_backfill_summary.json"
    markdown_path = reports / "SPAN_BACKFILL_AUDIT.md"
    _write_matrix(matrix_path, cells)
    provisional = SpanBackfillAuditReport(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        requested_dates=(end_date - start_date).days + 1,
        expected_cells=expected_cells,
        accounted_cells=accounted,
        terminal_cells=terminal,
        downloaded_cells=downloaded,
        unavailable_cells=unavailable,
        raw_missing_response_cells=unavailable,
        accepted_unavailable_cells=unavailable - ambiguous,
        unresolved_missing_cells=ambiguous,
        source_boundary_cells=boundary_cells,
        resolved_or_blocked_cells=resolved_or_blocked,
        unresolved_non_boundary_cells=unresolved_non_boundary,
        ambiguous_source_cells=ambiguous,
        failed_or_incomplete_cells=failed_or_incomplete,
        raw_integrity_failures=raw_failures,
        downloaded_without_valid_extraction=extraction_failures,
        compacted_months=sum(month.exists for month in months),
        compacted_rows=sum(month.row_count for month in months),
        duplicate_natural_keys=duplicate_keys,
        unmanifested_raw_files=unmanifested_raw,
        unmanifested_fragments=unmanifested_fragments,
        earliest_proven_download_date=(
            proven_download_dates[0] if proven_download_dates else None
        ),
        latest_proven_download_date=(
            proven_download_dates[-1] if proven_download_dates else None
        ),
        download_manifest_path=str(download_manifest_path),
        extraction_manifest_path=str(extraction_manifest_path),
        availability_manifest_path=(
            None
            if availability_manifest_path is None
            else str(availability_manifest_path)
        ),
        download_manifest_sha256=download_manifest_sha256,
        extraction_manifest_sha256=extraction_manifest_sha256,
        availability_manifest_sha256=availability_manifest_sha256,
        matrix_complete=matrix_complete,
        blocked_matrix_complete=blocked_matrix_complete,
        raw_integrity_ok=raw_ok,
        extraction_complete=extraction_ok,
        compacted_unique=unique,
        outcome=outcome,
        ok=outcome == "PASS_READY",
        matrix_parquet=str(matrix_path),
        summary_json=str(summary_path),
        audit_markdown=str(markdown_path),
        cells=tuple(cells),
        months=months,
        slot_year_counts=slot_year_counts,
    )
    _atomic_json(summary_path, provisional.to_dict(include_cells=False))
    _atomic_text(markdown_path, _markdown(provisional))
    return provisional


def _audit_cell(
    *,
    day: date,
    slot: str,
    suffix: str,
    order: int,
    event: Mapping[str, Any] | None,
    availability_event: Mapping[str, Any] | None,
    raw_root: Path,
    fragment_root: Path,
    extraction_latest: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> SpanAuditCell:
    if event is None:
        return SpanAuditCell(
            day.isoformat(),
            slot,
            suffix,
            order,
            "manifest_cell_missing",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            "not_applicable",
            None,
            None,
            None,
            False,
            "no durable download-manifest event",
            audit_disposition="manifest_missing",
        )
    state = str(event.get("state", ""))
    digest = str(event.get("sha256", "")) or None
    integrity: bool | None = None
    issue: str | None = None
    resolved: Path | None = None
    if state in DOWNLOADED_STATES:
        integrity, resolved, issue = _validate_raw_event(raw_root, day, suffix, event)

    extraction_state = "not_applicable"
    fragment_path: str | None = None
    fragment_exists: bool | None = None
    row_count: int | None = None
    if digest:
        extraction = extraction_latest.get((day.isoformat(), slot, digest))
        if extraction is not None:
            extraction_state = str(extraction.get("event", ""))
            fragment_path = str(extraction.get("fragment_path", "")) or None
            row_count = _optional_int(extraction.get("row_count"))
            if fragment_path:
                fragment_exists, fragment_issue = _validate_fragment_event(
                    fragment_root, extraction, digest, row_count
                )
                if fragment_issue:
                    issue = _join_issue(issue, fragment_issue)
            else:
                fragment_exists = False
        elif state in DOWNLOADED_STATES:
            extraction_state = "extraction_manifest_missing"
            fragment_exists = False

    if state in DOWNLOADED_STATES and extraction_state not in {
        "fragment_created",
        "fragment_already_valid",
    }:
        issue = _join_issue(
            issue, f"downloaded archive has extraction state {extraction_state}"
        )
    if state in DOWNLOADED_STATES and fragment_exists is False:
        issue = _join_issue(issue, "valid extraction fragment is absent")
    classification = availability_event or event
    source_ids = tuple(
        sorted(
            {
                str(source.get("source_id", ""))
                for source in classification.get("sources", [])
                if isinstance(source, Mapping) and source.get("source_id")
            }
        )
    )
    boundary = _is_current_source_boundary(event, availability_event)
    if boundary:
        disposition = "source_boundary"
    elif state in DOWNLOADED_STATES:
        disposition = (
            "downloaded_extracted"
            if integrity is True
            and extraction_state in {"fragment_created", "fragment_already_valid"}
            and fragment_exists is True
            else "downloaded_integrity_or_extraction_failure"
        )
    elif state in MISSING_STATES and _has_independent_absence_classification(
        event, availability_event
    ):
        disposition = "accepted_absence"
    elif state in MISSING_STATES:
        disposition = "unresolved_missing"
    elif bool(event.get("terminal")):
        disposition = "terminal_unrecognized_state"
    else:
        disposition = "nonterminal_unresolved"
    return SpanAuditCell(
        trading_date=day.isoformat(),
        slot=slot,
        suffix=suffix,
        slot_order=order,
        download_state=state,
        terminal=bool(event.get("terminal")),
        observed_at_utc=str(event.get("observed_at_utc", "")) or None,
        http_status=_optional_int(event.get("http_status")),
        raw_path=None if resolved is None else str(resolved),
        source_sha256=digest,
        size_bytes=_optional_int(event.get("size_bytes")),
        raw_integrity_ok=integrity,
        extraction_state=extraction_state,
        fragment_path=fragment_path,
        fragment_exists=fragment_exists,
        row_count=row_count,
        accounted=True,
        issue=issue,
        availability_event_type=str(classification.get("event", "")) or None,
        availability_observed_at_utc=(
            str(classification.get("observed_at_utc", "")) or None
        ),
        calendar_classification=(
            str(classification.get("calendar_classification", "")) or None
        ),
        classification_outcome=(
            str(classification.get("classification_outcome", "")) or None
        ),
        source_boundary_proven=boundary,
        availability_source_ids=source_ids,
        audit_disposition=disposition,
    )


def _validate_raw_event(
    root: Path, day: date, suffix: str, event: Mapping[str, Any]
) -> tuple[bool, Path | None, str | None]:
    relative = str(event.get("path", ""))
    digest = str(event.get("sha256", ""))
    if not relative or len(digest) != 64:
        return False, None, "download event lacks path or SHA-256"
    path = (root / relative).resolve()
    expected_parent = (root / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}").resolve()
    if not _is_relative_to(path, root) or path.parent != expected_parent:
        return False, path, "raw path escapes the expected date directory"
    if path.name.lower() != f"nsccl.{day:%Y%m%d}.{suffix}.zip":
        return False, path, "raw filename/date/slot mismatch"
    if not path.is_file():
        return False, path, "raw archive is missing"
    if _optional_int(event.get("size_bytes")) != path.stat().st_size:
        return False, path, "raw byte size differs from manifest"
    if _sha256_file(path) != digest:
        return False, path, "raw SHA-256 differs from manifest"
    try:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                return False, path, "raw ZIP CRC check failed"
            members = [item for item in archive.infolist() if not item.is_dir()]
            expected_member = f"nsccl.{day:%Y%m%d}.{'i0' + suffix[1] if suffix.startswith('i') else 's'}.spn"
            if len(members) != 1 or members[0].filename.lower() != expected_member:
                return False, path, "raw ZIP member mismatch"
    except (OSError, zipfile.BadZipFile) as exc:
        return False, path, f"raw ZIP validation failed: {type(exc).__name__}: {exc}"
    return True, path, None


def _validate_fragment_event(
    root: Path,
    event: Mapping[str, Any],
    source_sha256: str,
    expected_rows: int | None,
) -> tuple[bool, str | None]:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    raw_path = str(event.get("fragment_path", ""))
    expected_hash = str(event.get("fragment_sha256", ""))
    if not raw_path:
        return False, "extraction event lacks fragment path"
    path = _resolve_fragment_path(root, raw_path)
    if not _is_relative_to(path, root) or not path.is_file():
        return False, "fragment is missing or outside fragment root"
    if len(expected_hash) != 64:
        return False, "extraction event lacks fragment SHA-256"
    expected_size = _optional_int(event.get("fragment_size_bytes"))
    if expected_size is None or path.stat().st_size != expected_size:
        return False, "fragment byte size differs from extraction manifest"
    if _sha256_file(path) != expected_hash:
        return False, "fragment SHA-256 differs from extraction manifest"
    try:
        parquet = pq.ParquetFile(path)
        if (
            parquet.schema_arrow.remove_metadata()
            != span_arrow_schema().remove_metadata()
        ):
            return False, "fragment schema differs from versioned SPAN schema"
        if expected_rows is None or parquet.metadata.num_rows != expected_rows:
            return False, "fragment row count differs from extraction manifest"
        metadata = parquet.metadata.metadata or {}
        if metadata.get(b"source_sha256", b"").decode() != source_sha256:
            return False, "fragment file metadata has wrong source SHA-256"
        parser_version = str(event.get("parser_version", ""))
        if (
            not parser_version
            or metadata.get(b"parser_version", b"").decode() != parser_version
        ):
            return False, "fragment file metadata has wrong parser version"
        schema_version = str(event.get("schema_version", ""))
        if (
            not schema_version
            or metadata.get(b"schema_version", b"").decode() != schema_version
        ):
            return False, "fragment file metadata has wrong schema version"
        extraction_identity = str(event.get("extraction_identity", ""))
        if (
            not extraction_identity
            or metadata.get(b"extraction_identity", b"").decode() != extraction_identity
        ):
            return False, "fragment file metadata has wrong extraction identity"
        symbols = tuple(
            sorted(str(value).upper() for value in event.get("symbols_filter", ()))
        )
        if symbols != ("NIFTY",):
            return False, f"fragment symbols filter is {symbols!r}, expected NIFTY only"
        metadata_symbols = tuple(
            sorted(json.loads(metadata.get(b"symbols_filter", b"[]").decode("utf-8")))
        )
        if metadata_symbols != symbols:
            return False, "fragment file metadata has wrong symbols filter"
        event_counts = {
            str(key): int(value)
            for key, value in dict(event.get("instrument_counts", {})).items()
        }
        table = pq.read_table(path, columns=["instrument"])
        actual_counts: dict[str, int] = {}
        for value in table.column("instrument").to_pylist():
            actual_counts[str(value)] = actual_counts.get(str(value), 0) + 1
        if event_counts != actual_counts:
            return False, "fragment instrument counts differ from extraction manifest"
    except Exception as exc:  # noqa: BLE001 - corrupt Parquet must become an audit failure.
        return False, f"fragment validation failed: {type(exc).__name__}: {exc}"
    return True, None


def _resolve_fragment_path(root: Path, raw_path: str) -> Path:
    """Resolve current root-relative paths and legacy root-prefixed relative paths."""

    supplied = Path(raw_path)
    if supplied.is_absolute():
        return supplied.resolve()
    direct = (root / supplied).resolve()
    if direct.is_file():
        return direct
    matching_anchors = [
        index
        for index, part in enumerate(supplied.parts)
        if part.casefold() == root.name.casefold()
    ]
    if matching_anchors:
        legacy = (root / Path(*supplied.parts[matching_anchors[-1] + 1 :])).resolve()
        if legacy.is_file():
            return legacy
    return direct


def _audit_month(
    *,
    compacted_root: Path,
    year: int,
    month: int,
    expected_sources: Mapping[str, tuple[str, str]],
) -> SpanAuditMonth:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    path = compacted_root / f"{year:04d}_{month:02d}.parquet"
    if not path.is_file():
        return SpanAuditMonth(
            year,
            month,
            str(path),
            False,
            0,
            0,
            None,
            (),
            0,
            "compacted month is missing",
        )
    try:
        table = pq.read_table(path)
        expected_schema = span_arrow_schema()
        if table.schema.remove_metadata() != expected_schema.remove_metadata():
            raise ValueError("schema differs from versioned SPAN schema")
        duplicate_view = table.select(list(NATURAL_KEY)).sort_by(
            [(name, "ascending") for name in NATURAL_KEY]
        )
        duplicates = _count_adjacent_duplicates(duplicate_view)
        sources = {
            str(value) for value in table.column("source_sha256").to_pylist() if value
        }
        expected_source_set = set(expected_sources)
        missing_sources = expected_source_set - sources
        unexpected_sources = sources - expected_source_set
        issue = None
        if missing_sources:
            issue = f"{len(missing_sources)} extracted source archive(s) absent from compacted lineage"
        if unexpected_sources:
            issue = _join_issue(
                issue,
                f"{len(unexpected_sources)} unexpected source archive(s) in compacted lineage",
            )
        dates = table.column("date").to_pylist()
        if any(
            value is None or value.year != year or value.month != month
            for value in dates
        ):
            issue = _join_issue(
                issue, "compacted rows escape the requested month partition"
            )
        symbols = {str(value) for value in table.column("symbol").unique().to_pylist()}
        if symbols != {"NIFTY"}:
            issue = _join_issue(
                issue, f"compacted symbols are {sorted(symbols)!r}, expected NIFTY only"
            )
        slots = {str(value) for value in table.column("time_slot").unique().to_pylist()}
        allowed_slots = {slot for slot, _ in SLOT_SPECS}
        if not slots <= allowed_slots:
            issue = _join_issue(
                issue, f"invalid compacted slots: {sorted(slots - allowed_slots)!r}"
            )
        row_slots = table.column("time_slot").to_pylist()
        row_sources = table.column("source_sha256").to_pylist()
        if any(
            source not in expected_sources
            or expected_sources[source] != (row_date.isoformat(), row_slot)
            for source, row_date, row_slot in zip(
                row_sources, dates, row_slots, strict=True
            )
        ):
            issue = _join_issue(
                issue, "row date/slot does not match source-manifest cell identity"
            )
        instruments_set = {
            str(value) for value in table.column("instrument").unique().to_pylist()
        }
        if not instruments_set <= {"CE", "PE", "FUT"}:
            issue = _join_issue(
                issue,
                f"invalid instruments: {sorted(instruments_set - {'CE', 'PE', 'FUT'})!r}",
            )
        if any(table.column(f"s{index}").null_count for index in range(1, 17)):
            issue = _join_issue(issue, "one or more risk-array values are null")
        effective = table.column("span_effective_ts_ist").to_pylist()
        effective_source = table.column("effective_time_source").to_pylist()
        if any(
            (source == "unknown" and timestamp is not None)
            or (source != "unknown" and timestamp is None)
            for timestamp, source in zip(effective, effective_source, strict=True)
        ):
            issue = _join_issue(
                issue, "effective timestamp/source provenance is inconsistent"
            )
        instruments = tuple(sorted(instruments_set))
        return SpanAuditMonth(
            year,
            month,
            str(path),
            True,
            table.num_rows,
            duplicates,
            _sha256_file(path),
            instruments,
            len(sources),
            issue,
        )
    except Exception as exc:  # noqa: BLE001 - audit must preserve any unreadable/schema failure.
        return SpanAuditMonth(
            year,
            month,
            str(path),
            True,
            0,
            0,
            _sha256_file(path),
            (),
            0,
            f"{type(exc).__name__}: {exc}",
        )


def _count_adjacent_duplicates(table: Any) -> int:
    if table.num_rows < 2:
        return 0
    columns = [table.column(name) for name in NATURAL_KEY]
    duplicates = 0
    previous = tuple(column[0].as_py() for column in columns)
    for index in range(1, table.num_rows):
        current = tuple(column[index].as_py() for column in columns)
        if current == previous:
            duplicates += 1
        previous = current
    return duplicates


def _latest_download_events(
    path: Path,
) -> tuple[dict[tuple[str, str], Mapping[str, Any]], str | None]:
    latest: dict[tuple[str, str], Mapping[str, Any]] = {}
    events, digest = _jsonl_snapshot(path)
    for event in events:
        trading_date = str(event.get("trading_date", ""))
        slot = str(event.get("slot", ""))
        if trading_date and slot:
            latest[(trading_date, slot)] = event
    return latest, digest


def _latest_extraction_events(
    path: Path,
) -> tuple[dict[tuple[str, str, str], Mapping[str, Any]], str | None]:
    latest: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    events, manifest_digest = _jsonl_snapshot(path)
    for event in events:
        day = str(event.get("date", event.get("trading_date", "")))
        slot = str(event.get("slot", ""))
        source_digest = str(event.get("source_sha256", ""))
        if day and slot and source_digest:
            latest[(day, slot, source_digest)] = event
    return latest, manifest_digest


def _count_unmanifested_raw(
    root: Path, cells: Iterable[SpanAuditCell], start: date, end: date
) -> int:
    expected = {
        Path(cell.raw_path).resolve()
        for cell in cells
        if cell.download_state in DOWNLOADED_STATES and cell.raw_path
    }
    actual: set[Path] = set()
    day = start
    while day <= end:
        directory = root / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}"
        if directory.is_dir():
            actual.update(
                path.resolve() for path in directory.glob("*.zip") if path.is_file()
            )
        day += timedelta(days=1)
    return len(actual - expected)


def _count_unmanifested_fragments(
    root: Path, cells: Iterable[SpanAuditCell], start: date, end: date
) -> int:
    expected: set[Path] = set()
    for cell in cells:
        if not cell.fragment_path:
            continue
        expected.add(_resolve_fragment_path(root, cell.fragment_path))
    actual: set[Path] = set()
    day = start
    while day <= end:
        directory = root / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}"
        if directory.is_dir():
            actual.update(
                path.resolve() for path in directory.glob("*.parquet") if path.is_file()
            )
        day += timedelta(days=1)
    return len(actual - expected)


def _jsonl_snapshot(path: Path) -> tuple[list[Mapping[str, Any]], str | None]:
    if not path.is_file():
        return [], None
    digest = sha256()
    events: list[Mapping[str, Any]] = []
    with path.open("rb") as handle:
        for line_number, raw_line in enumerate(handle, 1):
            digest.update(raw_line)
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(
                    f"invalid UTF-8 at {path}:{line_number}: {exc}"
                ) from exc
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"invalid JSON at {path}:{line_number}: {exc}"
                ) from exc
            if not isinstance(event, dict):
                raise ValueError(
                    f"manifest event is not an object at {path}:{line_number}"
                )
            events.append(event)
    return events, digest.hexdigest()


def _write_matrix(path: Path, cells: list[SpanAuditCell]) -> None:
    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    table = pa.Table.from_pylist([asdict(cell) for cell in cells])
    partial = path.with_name(path.name + ".partial")
    pq.write_table(table, partial, compression="zstd")
    _fsync_path(partial)
    os.replace(partial, path)


def _slot_year_counts(
    cells: Iterable[SpanAuditCell],
    *,
    download_latest: Mapping[tuple[str, str], Mapping[str, Any]],
    availability_latest: Mapping[tuple[str, str], Mapping[str, Any]],
) -> tuple[SpanAuditSlotYear, ...]:
    grouped: dict[tuple[int, str, str], list[SpanAuditCell]] = {}
    for cell in cells:
        grouped.setdefault(
            (int(cell.trading_date[:4]), cell.slot, cell.suffix), []
        ).append(cell)

    order_by_slot = {slot: order for order, (slot, _suffix) in enumerate(SLOT_SPECS)}
    summaries: list[SpanAuditSlotYear] = []
    for (year, slot, suffix), group in sorted(
        grouped.items(), key=lambda item: (item[0][0], order_by_slot[item[0][1]])
    ):
        download_states = _state_counts(cell.download_state for cell in group)
        extraction_states = _state_counts(cell.extraction_state for cell in group)
        manifest_missing = sum(
            cell.download_state == "manifest_cell_missing" for cell in group
        )
        raw_missing = sum(cell.download_state in MISSING_STATES for cell in group)
        accepted_missing = sum(
            cell.download_state in MISSING_STATES
            and _has_independent_absence_classification(
                download_latest[(cell.trading_date, cell.slot)],
                availability_latest.get((cell.trading_date, cell.slot)),
            )
            for cell in group
        )
        unresolved_missing = raw_missing - accepted_missing
        summaries.append(
            SpanAuditSlotYear(
                year=year,
                slot=slot,
                suffix=suffix,
                total_cells=len(group),
                terminal_cells=sum(cell.terminal for cell in group),
                downloaded_valid_cells=sum(
                    cell.download_state in DOWNLOADED_STATES
                    and cell.raw_integrity_ok is True
                    for cell in group
                ),
                raw_missing_response_cells=raw_missing,
                accepted_unavailable_cells=accepted_missing,
                unresolved_missing_cells=unresolved_missing,
                manifest_missing_cells=manifest_missing,
                nonterminal_or_failed_cells=sum(
                    not cell.terminal
                    or cell.download_state not in DOWNLOADED_STATES | MISSING_STATES
                    or (
                        cell.download_state in DOWNLOADED_STATES
                        and cell.raw_integrity_ok is not True
                    )
                    or (
                        cell.download_state in MISSING_STATES
                        and not _has_independent_absence_classification(
                            download_latest[(cell.trading_date, cell.slot)],
                            availability_latest.get((cell.trading_date, cell.slot)),
                        )
                    )
                    for cell in group
                ),
                extracted_valid_cells=sum(
                    cell.extraction_state
                    in {"fragment_created", "fragment_already_valid"}
                    and cell.fragment_exists is True
                    for cell in group
                ),
                download_state_counts=download_states,
                extraction_state_counts=extraction_states,
            )
        )
    return tuple(summaries)


def _state_counts(states: Iterable[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for state in states:
        counts[state] = counts.get(state, 0) + 1
    return dict(sorted(counts.items()))


def _markdown(report: SpanBackfillAuditReport) -> str:
    status = "PASS" if report.ok else "FAIL"
    lines = [
        "# SPAN Backfill Audit",
        "",
        f"- Status: **{status}**",
        f"- Range: `{report.start_date}` through `{report.end_date}`",
        f"- Date/slot cells: `{report.accounted_cells:,}` / `{report.expected_cells:,}` accounted; "
        f"`{report.terminal_cells:,}` terminal",
        f"- Downloaded valid states: `{report.downloaded_cells:,}`",
        f"- Raw missing-response cells: `{report.raw_missing_response_cells:,}`",
        f"- Accepted unavailable cells: `{report.accepted_unavailable_cells:,}`",
        f"- Unresolved missing-response cells: `{report.unresolved_missing_cells:,}`",
        f"- Failed/incomplete cells: `{report.failed_or_incomplete_cells:,}`",
        f"- Raw integrity failures: `{report.raw_integrity_failures:,}`",
        f"- Downloaded without valid extraction: `{report.downloaded_without_valid_extraction:,}`",
        f"- Compacted months/rows: `{report.compacted_months:,}` / `{report.compacted_rows:,}`",
        f"- Duplicate compacted natural keys: `{report.duplicate_natural_keys:,}`",
        f"- Unmanifested raw files/fragments: `{report.unmanifested_raw_files:,}` / `{report.unmanifested_fragments:,}`",
        f"- Earliest/latest proven downloaded dates: `{report.earliest_proven_download_date or ''}` / "
        f"`{report.latest_proven_download_date or ''}`",
        "",
        "## Gates",
        "",
        "| Gate | Result |",
        "|---|---|",
        f"| Final outcome | {report.outcome} |",
        f"| Complete durable matrix | {'PASS' if report.matrix_complete else 'FAIL'} |",
        f"| Raw archives match manifest | {'PASS' if report.raw_integrity_ok else 'FAIL'} |",
        f"| Every downloaded archive extracted/compacted | {'PASS' if report.extraction_complete else 'FAIL'} |",
        f"| Compacted natural keys unique | {'PASS' if report.compacted_unique else 'FAIL'} |",
        "",
        "`not_returned_http_404` remains source-response evidence only; without independent calendar or source-boundary classification it prevents acceptance.",
        "Unknown SPAN effective times remain unknown and must not be used to introduce EOD lookahead.",
        "",
        "## Manifest fingerprints",
        "",
        "| Manifest | Path | SHA-256 |",
        "|---|---|---|",
        f"| Download | `{report.download_manifest_path}` | `{report.download_manifest_sha256 or ''}` |",
        f"| Extraction | `{report.extraction_manifest_path}` | `{report.extraction_manifest_sha256 or ''}` |",
        f"| Availability | `{report.availability_manifest_path or ''}` | "
        f"`{report.availability_manifest_sha256 or ''}` |",
        "",
        "## Slot/year latest-state coverage",
        "",
        "Counts use the latest manifest event for each date/slot cell. "
        "Downloaded-valid requires a matching file hash and valid ZIP; extracted-valid requires a verified fragment.",
        "",
        "| Year | Slot | Suffix | Cells | Terminal | Downloaded valid | Raw missing response | "
        "Accepted unavailable | Unresolved missing | Manifest missing | Nonterminal/failed | "
        "Extracted valid | Download states | Extraction states |",
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for coverage in report.slot_year_counts:
        lines.append(
            f"| {coverage.year} | {coverage.slot} | {coverage.suffix} | {coverage.total_cells:,} | "
            f"{coverage.terminal_cells:,} | {coverage.downloaded_valid_cells:,} | "
            f"{coverage.raw_missing_response_cells:,} | {coverage.accepted_unavailable_cells:,} | "
            f"{coverage.unresolved_missing_cells:,} | {coverage.manifest_missing_cells:,} | "
            f"{coverage.nonterminal_or_failed_cells:,} | "
            f"{coverage.extracted_valid_cells:,} | "
            f"`{json.dumps(coverage.download_state_counts, sort_keys=True)}` | "
            f"`{json.dumps(coverage.extraction_state_counts, sort_keys=True)}` |"
        )
    lines.extend(
        [
            "",
            "## Compacted months",
            "",
            "| Month | Rows | Sources | SHA-256 | Issue |",
            "|---|---:|---:|---|---|",
        ]
    )
    for month in report.months:
        lines.append(
            f"| {month.year:04d}-{month.month:02d} | {month.row_count:,} | {month.source_archive_count:,} | "
            f"`{month.sha256 or ''}` | {month.issue or ''} |"
        )
    return "\n".join(lines) + "\n"


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(
        path, json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n"
    )


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _fsync_path(path: Path) -> None:
    # Windows' CRT rejects fsync on a descriptor opened read-only.
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _optional_file_sha256(path: Path | None) -> str | None:
    return _sha256_file(path) if path is not None and path.is_file() else None


def _optional_int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except (TypeError, ValueError):
        return None


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _join_issue(existing: str | None, new: str) -> str:
    return new if existing is None else f"{existing}; {new}"


def _has_independent_absence_classification(
    event: Mapping[str, Any],
    availability_event: Mapping[str, Any] | None = None,
) -> bool:
    # Accepted absences and source boundaries must come from the separately
    # validated availability manifest.  A download event cannot independently
    # assert the evidence needed to dispose of itself.
    if availability_event is None:
        return False
    classification = str(availability_event.get("calendar_classification", "")).lower()
    return classification in {
        "official_non_trading_day",
        "official_holiday",
        "official_weekend",
    } or bool(availability_event.get("source_availability_boundary_proven"))


def _is_current_source_boundary(
    event: Mapping[str, Any] | None,
    availability_event: Mapping[str, Any] | None,
) -> bool:
    """Return true only for the latest still-blocked disposition of this cell."""

    if event is None:
        return False
    state = str(event.get("state", ""))
    if state in DOWNLOADED_STATES:
        return False
    if availability_event is None:
        return False
    basic_match = bool(
        availability_event.get("source_availability_boundary_proven")
        and availability_event.get("classification_outcome") == "source_boundary"
        and availability_event.get("download_state") == state
    )
    if not basic_match:
        return False
    if availability_event.get("event") == REPEATED_STATIC_BOUNDARY_EVENT:
        reports = availability_event.get("reports_api_evidence")
        observations = availability_event.get("static_archive_observations")
        return bool(
            availability_event.get("schema_version")
            == REPEATED_STATIC_BOUNDARY_EVENT_SCHEMA
            and isinstance(reports, Mapping)
            and reports.get("manifest_event_id") == event.get("event_id")
            and reports.get("manifest_run_id") == event.get("run_id")
            and isinstance(observations, list)
            and len(observations) == 3
        )
    if availability_event.get("event") != "official_source_corrupt_archive":
        return True
    reports = availability_event.get("reports_api_evidence")
    latest_rejected = event.get("rejected_inner")
    if not isinstance(reports, Mapping) or not isinstance(latest_rejected, Mapping):
        return False
    proven_rejected = reports.get("rejected_inner")
    if not isinstance(proven_rejected, Mapping):
        return False
    return bool(
        reports.get("manifest_event_id") == event.get("event_id")
        and proven_rejected.get("sha256") == latest_rejected.get("sha256")
        and proven_rejected.get("size_bytes") == latest_rejected.get("size_bytes")
    )
