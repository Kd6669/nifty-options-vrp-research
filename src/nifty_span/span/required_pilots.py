"""Deterministic acceptance audit for the four required Phase 1 SPAN pilots.

The auditor is intentionally read-only with respect to SPAN acquisition and
extraction artifacts.  It consumes the immutable monthly audit summary/matrix
and compacted Parquet, then writes a separate JSON/Markdown acceptance report.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Iterable, Mapping
import json
import os
import re

from .backfill_downloader import DOWNLOADED_STATES, MISSING_STATES, SLOT_SPECS
from .streaming_extractor import NATURAL_KEY


SCHEMA_VERSION = "span-required-pilots/v1"
INSTRUMENTS = ("CE", "PE", "FUT")
_EXTRACTION_SUCCESS = frozenset({"fragment_created", "fragment_already_valid"})
_WEEKDAYS = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class RequiredPilotSpec:
    pilot_id: str
    name: str
    year: int
    month: int
    purpose: str
    special_session_date: date | None = None
    list_observed_expiries: bool = False

    @property
    def month_key(self) -> str:
        return f"{self.year:04d}_{self.month:02d}"


REQUIRED_PILOTS = (
    RequiredPilotSpec(
        pilot_id="ordinary_early_2021_01",
        name="Ordinary early-history month",
        year=2021,
        month=1,
        purpose="Validate an available 2021 month using the immutable evidence contract.",
    ),
    RequiredPilotSpec(
        pilot_id="special_session_2024_03",
        name="Special-session month",
        year=2024,
        month=3,
        purpose="Prove that the Saturday 2024-03-02 source and NIFTY rows were not dropped.",
        special_session_date=date(2024, 3, 2),
    ),
    RequiredPilotSpec(
        pilot_id="expiry_regime_2025_09",
        name="Expiry-regime observation month",
        year=2025,
        month=9,
        purpose="List NIFTY option expiries observed in SPAN rows across the transition month.",
        list_observed_expiries=True,
    ),
    RequiredPilotSpec(
        pilot_id="ordinary_recent_2026_06",
        name="Ordinary recent month",
        year=2026,
        month=6,
        purpose="Validate a recent ordinary month using the same immutable evidence contract.",
    ),
)


@dataclass(frozen=True)
class RequiredPilotsAuditResult:
    payload: dict[str, Any]
    json_path: str
    markdown_path: str

    @property
    def overall_status(self) -> str:
        return str(self.payload["overall_status"])


def audit_required_span_pilots(
    run_root: str | Path,
    output_root: str | Path | None = None,
) -> RequiredPilotsAuditResult:
    """Audit all required pilots and atomically publish deterministic evidence."""

    root = Path(run_root).resolve()
    destination = (
        root / "reports" / "required_pilots"
        if output_root is None
        else Path(output_root).resolve()
    )
    payload = inspect_required_span_pilots(root)
    json_path = destination / "span_required_pilots.json"
    markdown_path = destination / "SPAN_REQUIRED_PILOTS.md"
    _atomic_text(json_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _atomic_text(markdown_path, _render_markdown(payload))
    return RequiredPilotsAuditResult(payload, str(json_path), str(markdown_path))


def inspect_required_span_pilots(run_root: str | Path) -> dict[str, Any]:
    """Recompute the canonical pilot payload without publishing any files."""

    root = Path(run_root).resolve()
    pilots = [_audit_pilot(root, spec) for spec in REQUIRED_PILOTS]
    statuses = {str(pilot["status"]) for pilot in pilots}
    if "FAIL" in statuses:
        overall = "FAIL"
    elif "WAITING" in statuses:
        overall = "WAITING"
    else:
        overall = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "run_root": str(root),
        "overall_status": overall,
        "pilot_count": len(pilots),
        "status_counts": {
            status: sum(pilot["status"] == status for pilot in pilots)
            for status in ("PASS", "WAITING", "FAIL")
        },
        "pilots": pilots,
        "evidence_contract": [
            "Only immutable monthly audit summary/matrix and compacted Parquet are inspected.",
            "Input artifact and source-lineage SHA-256 values are reported exactly as observed.",
            "BLOCKED_SOURCE is accepted only when every cell is accounted, every exceptional cell is a proven source boundary, and no non-boundary gap remains.",
            "No network requests, exchange-calendar assumptions, or publication times are introduced.",
        ],
    }


def _audit_pilot(root: Path, spec: RequiredPilotSpec) -> dict[str, Any]:
    monthly = root / "reports" / "monthly" / spec.month_key
    paths = {
        "audit_summary": monthly / "span_backfill_summary.json",
        "date_slot_matrix": monthly / "span_date_slot_matrix.parquet",
        "compacted_parquet": root / "compacted" / f"{spec.month_key}.parquet",
    }
    artifacts = {name: _artifact_evidence(root, path) for name, path in paths.items()}
    missing = [name for name, evidence in artifacts.items() if not evidence["exists"]]
    base: dict[str, Any] = {
        "pilot_id": spec.pilot_id,
        "name": spec.name,
        "month": f"{spec.year:04d}-{spec.month:02d}",
        "purpose": spec.purpose,
        "status": "WAITING",
        "reasons": [],
        "artifacts": artifacts,
        "monthly_audit": None,
        "matrix": None,
        "compacted": None,
        "special_session": None,
        "observed_option_expiries": [],
        "source_archive_sha256": [],
        "source_archive_set_sha256": None,
        "evidence_limitations": _evidence_limitations(spec),
    }
    if missing:
        base["reasons"] = [
            f"required artifact not published: {name}" for name in missing
        ]
        summary_path = paths["audit_summary"]
        if summary_path.is_file():
            try:
                summary = _load_json_object(summary_path)
                base["monthly_audit"] = _summary_projection(summary)
                if str(summary.get("outcome")) in {"PASS_READY", "BLOCKED_SOURCE"}:
                    base["status"] = "FAIL"
                    base["reasons"].append(
                        "monthly audit claims an accepted outcome while required pilot artifacts are absent"
                    )
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                base["status"] = "FAIL"
                base["reasons"].append(
                    f"audit summary is unreadable: {type(exc).__name__}: {exc}"
                )
        return base

    issues: list[str] = []
    waiting_reasons: list[str] = []
    try:
        summary = _load_json_object(paths["audit_summary"])
        summary_projection = _summary_projection(summary)
        base["monthly_audit"] = summary_projection
        outcome = str(summary.get("outcome", ""))
        if outcome == "FAIL_INCOMPLETE":
            waiting_reasons.append("monthly audit is not yet complete")
        elif outcome not in {"PASS_READY", "BLOCKED_SOURCE"}:
            issues.append(
                f"monthly audit outcome is {outcome or 'missing'}, expected PASS_READY or BLOCKED_SOURCE"
            )
        _validate_summary(summary, spec, issues)

        matrix_table = _read_matrix(paths["date_slot_matrix"])
        if outcome == "BLOCKED_SOURCE":
            _validate_blocked_source_matrix(matrix_table, summary, issues)
        matrix_result, expected_cells, matrix_sources = _inspect_matrix(
            matrix_table, spec, issues
        )
        base["matrix"] = matrix_result
        if (
            outcome == "PASS_READY"
            and matrix_result["terminal_cells"] != matrix_result["expected_row_count"]
        ):
            issues.append("PASS_READY matrix contains nonterminal cells")

        compacted_table = _read_compacted(paths["compacted_parquet"])
        compacted_result, actual_cells, compacted_sources = _inspect_compacted(
            compacted_table,
            spec,
            issues,
            require_instruments=outcome in {"PASS_READY", "BLOCKED_SOURCE"},
        )
        base["compacted"] = compacted_result
        _cross_check_evidence(
            summary=summary,
            matrix_expected_cells=expected_cells,
            actual_cells=actual_cells,
            matrix_sources=matrix_sources,
            compacted_sources=compacted_sources,
            compacted_rows=int(compacted_result["row_count"]),
            issues=issues,
        )
        _verify_artifacts_unchanged(root, paths, artifacts, issues)

        source_hashes = sorted(matrix_sources)
        base["source_archive_sha256"] = source_hashes
        base["source_archive_set_sha256"] = _hash_string_set(source_hashes)
        if spec.special_session_date is not None:
            special = _special_session_evidence(
                spec.special_session_date,
                matrix_table,
                compacted_table,
            )
            base["special_session"] = special
            if not special["source_exists"]:
                message = (
                    f"{special['date']} has no downloaded/extracted source; "
                    "Saturday retention is unproven"
                )
                if outcome == "FAIL_INCOMPLETE":
                    waiting_reasons.append(message)
                else:
                    issues.append(message)
            elif int(special["compacted_nifty_rows"]) == 0:
                issues.append(
                    f"{special['date']} has source evidence but zero compacted NIFTY rows"
                )
        if spec.list_observed_expiries:
            expiries = _observed_option_expiries(compacted_table)
            base["observed_option_expiries"] = expiries
            if not expiries:
                issues.append("no NIFTY CE/PE expiry values were observed")
    except Exception as exc:  # noqa: BLE001 - all corrupt evidence must become a pilot failure.
        issues.append(f"evidence inspection failed: {type(exc).__name__}: {exc}")

    if issues:
        base["status"] = "FAIL"
        base["reasons"] = _stable_unique(issues)
    elif waiting_reasons:
        base["status"] = "WAITING"
        base["reasons"] = _stable_unique(waiting_reasons)
    else:
        base["status"] = "PASS"
        base["reasons"] = ["all required immutable evidence checks passed"]
    return base


def _artifact_evidence(root: Path, path: Path) -> dict[str, Any]:
    exists = path.is_file()
    try:
        relative = path.resolve().relative_to(root).as_posix()
    except ValueError:
        relative = str(path.resolve())
    return {
        "path": relative,
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else None,
        "sha256": _sha256_file(path) if exists else None,
    }


def _verify_artifacts_unchanged(
    root: Path,
    paths: Mapping[str, Path],
    before: Mapping[str, Mapping[str, Any]],
    issues: list[str],
) -> None:
    """Reject a report if any supposedly immutable input changed mid-audit."""

    for name, path in paths.items():
        after = _artifact_evidence(root, path)
        if (
            before[name].get("exists") != after["exists"]
            or before[name].get("size_bytes") != after["size_bytes"]
            or before[name].get("sha256") != after["sha256"]
        ):
            issues.append(f"input artifact changed while being audited: {name}")


def _summary_projection(summary: Mapping[str, Any]) -> dict[str, Any]:
    fields = (
        "start_date",
        "end_date",
        "requested_dates",
        "expected_cells",
        "accounted_cells",
        "terminal_cells",
        "downloaded_cells",
        "unavailable_cells",
        "failed_or_incomplete_cells",
        "source_boundary_cells",
        "resolved_or_blocked_cells",
        "unresolved_non_boundary_cells",
        "ambiguous_source_cells",
        "unresolved_missing_cells",
        "raw_integrity_failures",
        "downloaded_without_valid_extraction",
        "compacted_rows",
        "duplicate_natural_keys",
        "matrix_complete",
        "blocked_matrix_complete",
        "raw_integrity_ok",
        "extraction_complete",
        "compacted_unique",
        "outcome",
        "ok",
    )
    return {field: summary.get(field) for field in fields}


def _validate_summary(
    summary: Mapping[str, Any], spec: RequiredPilotSpec, issues: list[str]
) -> None:
    start = date(spec.year, spec.month, 1)
    end = _month_end(spec.year, spec.month)
    expected_dates = (end - start).days + 1
    expected_cells = expected_dates * len(SLOT_SPECS)
    expected_values = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "requested_dates": expected_dates,
        "expected_cells": expected_cells,
    }
    for field, expected in expected_values.items():
        if summary.get(field) != expected:
            issues.append(
                f"monthly summary {field}={summary.get(field)!r}, expected {expected!r}"
            )
    if str(summary.get("outcome")) == "PASS_READY":
        pass_requirements = {
            "accounted_cells": expected_cells,
            "terminal_cells": expected_cells,
            "failed_or_incomplete_cells": 0,
            "raw_integrity_failures": 0,
            "downloaded_without_valid_extraction": 0,
            "duplicate_natural_keys": 0,
            "matrix_complete": True,
            "raw_integrity_ok": True,
            "extraction_complete": True,
            "compacted_unique": True,
            "ok": True,
        }
        for field, expected in pass_requirements.items():
            if summary.get(field) != expected:
                issues.append(
                    f"PASS_READY summary has {field}={summary.get(field)!r}, expected {expected!r}"
                )
    elif str(summary.get("outcome")) == "BLOCKED_SOURCE":
        blocked_requirements = {
            "accounted_cells": expected_cells,
            "resolved_or_blocked_cells": expected_cells,
            "unresolved_non_boundary_cells": 0,
            "ambiguous_source_cells": 0,
            "unresolved_missing_cells": 0,
            "raw_integrity_failures": 0,
            "downloaded_without_valid_extraction": 0,
            "duplicate_natural_keys": 0,
            "blocked_matrix_complete": True,
            "raw_integrity_ok": True,
            "extraction_complete": True,
            "compacted_unique": True,
            "ok": False,
        }
        for field, expected in blocked_requirements.items():
            if summary.get(field) != expected:
                issues.append(
                    f"BLOCKED_SOURCE summary has {field}={summary.get(field)!r}, expected {expected!r}"
                )
        boundary_cells = _nonnegative_int(summary.get("source_boundary_cells"))
        if boundary_cells is None or boundary_cells < 1:
            issues.append("BLOCKED_SOURCE summary has no proven source-boundary cells")


def _validate_blocked_source_matrix(
    table: Any,
    summary: Mapping[str, Any],
    issues: list[str],
) -> None:
    required = {
        "accounted",
        "audit_disposition",
        "availability_event_type",
        "classification_outcome",
        "source_boundary_proven",
    }
    missing = required - set(table.column_names)
    if missing:
        issues.append(f"BLOCKED_SOURCE matrix lacks proof columns {sorted(missing)!r}")
        return

    boundary_cells = 0
    for row in table.to_pylist():
        day = str(row.get("trading_date", ""))
        slot = str(row.get("slot", ""))
        state = str(row.get("download_state", ""))
        disposition = str(row.get("audit_disposition", ""))
        source_boundary = row.get("source_boundary_proven") is True
        classification = str(row.get("classification_outcome", ""))
        availability_event = str(row.get("availability_event_type", ""))
        if row.get("accounted") is not True:
            issues.append(f"BLOCKED_SOURCE matrix cell {day} {slot} is not accounted")
        if state in DOWNLOADED_STATES:
            if disposition != "downloaded_extracted":
                issues.append(
                    f"BLOCKED_SOURCE downloaded matrix cell {day} {slot} has disposition {disposition or 'missing'}"
                )
            if source_boundary or classification == "source_boundary":
                issues.append(
                    f"BLOCKED_SOURCE downloaded matrix cell {day} {slot} is incorrectly marked as a source boundary"
                )
            continue
        if disposition == "accepted_absence":
            if (
                state not in MISSING_STATES
                or source_boundary
                or classification != "accepted_absence"
                or availability_event != "availability_classification"
            ):
                issues.append(
                    f"BLOCKED_SOURCE accepted-absence cell {day} {slot} lacks consistent classification proof"
                )
            if row.get("terminal") is not True:
                issues.append(
                    f"BLOCKED_SOURCE accepted-absence cell {day} {slot} is nonterminal"
                )
            continue
        if disposition == "source_boundary":
            boundary_cells += 1
            if (
                not source_boundary
                or classification != "source_boundary"
                or availability_event
                not in {
                    "official_source_corrupt_archive",
                    "official_source_repeated_static_boundary",
                }
            ):
                issues.append(
                    f"BLOCKED_SOURCE matrix cell {day} {slot} lacks explicit source-boundary proof"
                )
            continue
        issues.append(
            f"BLOCKED_SOURCE matrix cell {day} {slot} has unresolved disposition {disposition or 'missing'}"
        )

    expected_boundaries = _nonnegative_int(summary.get("source_boundary_cells"))
    if expected_boundaries is None or boundary_cells != expected_boundaries:
        issues.append(
            "BLOCKED_SOURCE matrix source-boundary count "
            f"{boundary_cells} does not match summary {summary.get('source_boundary_cells')!r}"
        )


def _read_matrix(path: Path) -> Any:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    required = {
        "trading_date",
        "slot",
        "download_state",
        "terminal",
        "source_sha256",
        "raw_integrity_ok",
        "extraction_state",
        "fragment_exists",
        "row_count",
    }
    table = pq.read_table(path)
    missing = required - set(table.column_names)
    if missing:
        raise ValueError(f"date/slot matrix lacks columns {sorted(missing)!r}")
    return table


def _read_compacted(path: Path) -> Any:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    required = set(NATURAL_KEY) | {"source_sha256"}
    schema = pq.read_schema(path)
    missing = required - set(schema.names)
    if missing:
        raise ValueError(f"compacted Parquet lacks columns {sorted(missing)!r}")
    return pq.read_table(path, columns=[*NATURAL_KEY, "source_sha256"])


def _inspect_matrix(
    table: Any,
    spec: RequiredPilotSpec,
    issues: list[str],
) -> tuple[
    dict[str, Any],
    dict[tuple[str, str], int],
    dict[str, set[tuple[str, str]]],
]:
    start = date(spec.year, spec.month, 1)
    end = _month_end(spec.year, spec.month)
    expected_days = (end - start).days + 1
    slot_names = tuple(slot for slot, _suffix in SLOT_SPECS)
    rows = table.to_pylist()
    by_cell: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in rows:
        key = (str(row.get("trading_date", "")), str(row.get("slot", "")))
        if key in by_cell:
            issues.append(f"duplicate matrix cell {key[0]} {key[1]}")
        by_cell[key] = row
    expected_keys = {
        (day.isoformat(), slot)
        for day in _date_range(start, end)
        for slot in slot_names
    }
    actual_keys = set(by_cell)
    if actual_keys != expected_keys:
        issues.append(
            f"matrix cell identity mismatch: missing={len(expected_keys - actual_keys)}, "
            f"unexpected={len(actual_keys - expected_keys)}"
        )
    expected_rows: dict[tuple[str, str], int] = {}
    source_cells: dict[str, set[tuple[str, str]]] = {}
    slot_coverage: dict[str, dict[str, Any]] = {}
    for slot in slot_names:
        slot_rows = [
            row for (day, row_slot), row in by_cell.items() if row_slot == slot
        ]
        downloaded = [
            row for row in slot_rows if row.get("download_state") in DOWNLOADED_STATES
        ]
        extracted = [
            row
            for row in downloaded
            if row.get("raw_integrity_ok") is True
            and row.get("extraction_state") in _EXTRACTION_SUCCESS
            and row.get("fragment_exists") is True
        ]
        slot_sources: set[str] = set()
        for row in downloaded:
            digest = str(row.get("source_sha256") or "")
            if not _SHA256_RE.fullmatch(digest):
                issues.append(
                    f"downloaded matrix cell {row.get('trading_date')} {slot} lacks a valid source SHA-256"
                )
            else:
                source_cells.setdefault(digest, set()).add(
                    (str(row.get("trading_date")), slot)
                )
                slot_sources.add(digest)
            if row not in extracted:
                issues.append(
                    f"downloaded matrix cell {row.get('trading_date')} {slot} lacks valid extraction evidence"
                )
        for row in extracted:
            count = _nonnegative_int(row.get("row_count"))
            if count is None:
                issues.append(
                    f"extracted matrix cell {row.get('trading_date')} {slot} lacks a valid row count"
                )
            else:
                expected_rows[(str(row["trading_date"]), slot)] = count
        slot_coverage[slot] = {
            "matrix_cells": len(slot_rows),
            "terminal_cells": sum(row.get("terminal") is True for row in slot_rows),
            "downloaded_source_cells": len(downloaded),
            "extracted_valid_cells": len(extracted),
            "matrix_extracted_rows": sum(
                _nonnegative_int(row.get("row_count")) or 0 for row in extracted
            ),
            "source_archive_sha256": sorted(slot_sources),
        }
    nonterminal = sum(row.get("terminal") is not True for row in rows)
    for digest, cells in source_cells.items():
        if len(cells) != 1:
            issues.append(f"matrix source {digest} spans {len(cells)} date/slot cells")
    return (
        {
            "row_count": table.num_rows,
            "expected_row_count": expected_days * len(SLOT_SPECS),
            "calendar_dates": len({key[0] for key in actual_keys}),
            "terminal_cells": table.num_rows - nonterminal,
            "slot_coverage": slot_coverage,
        },
        expected_rows,
        source_cells,
    )


def _inspect_compacted(
    table: Any,
    spec: RequiredPilotSpec,
    issues: list[str],
    *,
    require_instruments: bool,
) -> tuple[
    dict[str, Any],
    dict[tuple[str, str], int],
    dict[str, set[tuple[str, str]]],
]:
    dates = table.column("date").to_pylist()
    slots = table.column("time_slot").to_pylist()
    symbols = table.column("symbol").to_pylist()
    instruments = table.column("instrument").to_pylist()
    sources_raw = table.column("source_sha256").to_pylist()
    actual_cells: dict[tuple[str, str], int] = {}
    source_cells: dict[str, set[tuple[str, str]]] = {}
    instrument_counts = {instrument: 0 for instrument in INSTRUMENTS}
    slot_counts = {slot: 0 for slot, _suffix in SLOT_SPECS}
    invalid_symbols: set[str] = set()
    invalid_instruments: set[str] = set()
    invalid_slots: set[str] = set()
    invalid_dates = 0
    invalid_hashes = 0
    for day, slot_raw, symbol_raw, instrument_raw, source_raw in zip(
        dates, slots, symbols, instruments, sources_raw, strict=True
    ):
        slot = str(slot_raw)
        symbol = str(symbol_raw)
        instrument = str(instrument_raw)
        source = str(source_raw)
        if day is None or day.year != spec.year or day.month != spec.month:
            invalid_dates += 1
            continue
        key = (day.isoformat(), slot)
        actual_cells[key] = actual_cells.get(key, 0) + 1
        if symbol != "NIFTY":
            invalid_symbols.add(symbol)
        if instrument not in instrument_counts:
            invalid_instruments.add(instrument)
        else:
            instrument_counts[instrument] += 1
        if slot not in slot_counts:
            invalid_slots.add(slot)
        else:
            slot_counts[slot] += 1
        if not _SHA256_RE.fullmatch(source):
            invalid_hashes += 1
        else:
            source_cells.setdefault(source, set()).add(key)
    if invalid_dates:
        issues.append(
            f"compacted Parquet has {invalid_dates} rows outside {spec.year:04d}-{spec.month:02d}"
        )
    if invalid_symbols:
        issues.append(
            f"compacted symbols include {sorted(invalid_symbols)!r}, expected NIFTY only"
        )
    if invalid_instruments:
        issues.append(
            f"compacted instruments include invalid values {sorted(invalid_instruments)!r}"
        )
    if invalid_slots:
        issues.append(
            f"compacted slots include invalid values {sorted(invalid_slots)!r}"
        )
    if invalid_hashes:
        issues.append(
            f"compacted Parquet has {invalid_hashes} rows without valid source SHA-256"
        )
    for instrument, count in instrument_counts.items():
        if require_instruments and count == 0:
            issues.append(f"compacted month has no NIFTY {instrument} rows")
    for source, cells in source_cells.items():
        if len(cells) != 1:
            issues.append(f"source {source} spans {len(cells)} date/slot cells")
    natural_key_nulls = {name: table.column(name).null_count for name in NATURAL_KEY}
    if any(natural_key_nulls.values()):
        issues.append(
            "compacted natural key contains nulls: "
            + ", ".join(
                f"{name}={count}" for name, count in natural_key_nulls.items() if count
            )
        )
    duplicates = _count_natural_key_duplicates(table)
    if duplicates:
        issues.append(f"compacted month has {duplicates} duplicate natural keys")
    return (
        {
            "row_count": table.num_rows,
            "natural_key": list(NATURAL_KEY),
            "natural_key_null_counts": natural_key_nulls,
            "natural_key_duplicates": duplicates,
            "natural_key_unique": duplicates == 0
            and not any(natural_key_nulls.values()),
            "instrument_presence": {
                instrument: {"present": count > 0, "row_count": count}
                for instrument, count in instrument_counts.items()
            },
            "slot_coverage": {
                slot: {
                    "present": count > 0,
                    "row_count": count,
                    "calendar_dates": len(
                        {day for day, cell_slot in actual_cells if cell_slot == slot}
                    ),
                }
                for slot, count in slot_counts.items()
            },
            "source_archive_count": len(source_cells),
        },
        actual_cells,
        source_cells,
    )


def _cross_check_evidence(
    *,
    summary: Mapping[str, Any],
    matrix_expected_cells: Mapping[tuple[str, str], int],
    actual_cells: Mapping[tuple[str, str], int],
    matrix_sources: Mapping[str, set[tuple[str, str]]],
    compacted_sources: Mapping[str, set[tuple[str, str]]],
    compacted_rows: int,
    issues: list[str],
) -> None:
    if matrix_expected_cells != actual_cells:
        missing = sorted(set(matrix_expected_cells) - set(actual_cells))
        unexpected = sorted(set(actual_cells) - set(matrix_expected_cells))
        differing = sorted(
            key
            for key in set(matrix_expected_cells) & set(actual_cells)
            if matrix_expected_cells[key] != actual_cells[key]
        )
        issues.append(
            "matrix/compacted date-slot row counts disagree: "
            f"missing={len(missing)}, unexpected={len(unexpected)}, differing={len(differing)}"
        )
    if matrix_sources != compacted_sources:
        matrix_hashes = set(matrix_sources)
        compacted_hashes = set(compacted_sources)
        mismatched = {
            digest
            for digest in matrix_hashes & compacted_hashes
            if matrix_sources[digest] != compacted_sources[digest]
        }
        issues.append(
            "matrix/compacted source SHA-256 lineage disagrees: "
            f"missing={len(matrix_hashes - compacted_hashes)}, "
            f"unexpected={len(compacted_hashes - matrix_hashes)}, "
            f"wrong_cell={len(mismatched)}"
        )
    summary_rows = _nonnegative_int(summary.get("compacted_rows"))
    if summary_rows != compacted_rows:
        issues.append(
            f"summary compacted_rows={summary.get('compacted_rows')!r}, actual={compacted_rows}"
        )


def _special_session_evidence(day: date, matrix: Any, compacted: Any) -> dict[str, Any]:
    day_text = day.isoformat()
    matrix_rows = [
        row for row in matrix.to_pylist() if str(row.get("trading_date")) == day_text
    ]
    source_rows = [
        row
        for row in matrix_rows
        if row.get("download_state") in DOWNLOADED_STATES
        and row.get("raw_integrity_ok") is True
        and row.get("extraction_state") in _EXTRACTION_SUCCESS
        and row.get("fragment_exists") is True
    ]
    compacted_dates = compacted.column("date").to_pylist()
    compacted_symbols = compacted.column("symbol").to_pylist()
    retained_rows = sum(
        value == day and symbol == "NIFTY"
        for value, symbol in zip(compacted_dates, compacted_symbols, strict=True)
    )
    return {
        "date": day_text,
        "weekday": _weekday(day),
        "matrix_cells": len(matrix_rows),
        "source_exists": bool(source_rows),
        "downloaded_extracted_slots": sorted(str(row["slot"]) for row in source_rows),
        "source_archive_sha256": sorted(
            str(row["source_sha256"]) for row in source_rows
        ),
        "matrix_extracted_nifty_rows": sum(
            _nonnegative_int(row.get("row_count")) or 0 for row in source_rows
        ),
        "compacted_nifty_rows": retained_rows,
        "retained": bool(source_rows) and retained_rows > 0,
    }


def _observed_option_expiries(table: Any) -> list[dict[str, Any]]:
    dates = table.column("date").to_pylist()
    slots = table.column("time_slot").to_pylist()
    instruments = table.column("instrument").to_pylist()
    expiries = table.column("expiry").to_pylist()
    grouped: dict[date, dict[str, Any]] = {}
    for observed, slot, instrument, expiry in zip(
        dates, slots, instruments, expiries, strict=True
    ):
        if instrument not in {"CE", "PE"} or expiry is None:
            continue
        item = grouped.setdefault(
            expiry,
            {
                "expiry_date": expiry.isoformat(),
                "weekday": _weekday(expiry),
                "ce_rows": 0,
                "pe_rows": 0,
                "observed_slots": set(),
                "observed_dates": set(),
            },
        )
        item["ce_rows" if instrument == "CE" else "pe_rows"] += 1
        item["observed_slots"].add(str(slot))
        item["observed_dates"].add(observed.isoformat())
    result: list[dict[str, Any]] = []
    for expiry in sorted(grouped):
        item = grouped[expiry]
        observed_dates = sorted(item.pop("observed_dates"))
        item["observed_slots"] = sorted(item["observed_slots"])
        item["first_observed_on"] = observed_dates[0]
        item["last_observed_on"] = observed_dates[-1]
        result.append(item)
    return result


def _count_natural_key_duplicates(table: Any) -> int:
    import pyarrow.compute as pc  # type: ignore[import-not-found]

    if table.num_rows < 2:
        return 0
    ordered = (
        table.select(list(NATURAL_KEY))
        .sort_by([(name, "ascending") for name in NATURAL_KEY])
        .combine_chunks()
    )
    matches = None
    for name in NATURAL_KEY:
        column = ordered.column(name)
        equal = pc.equal(column.slice(1), column.slice(0, ordered.num_rows - 1))
        matches = equal if matches is None else pc.and_(matches, equal)
    return int(pc.sum(matches).as_py() or 0)


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Required SPAN Phase 1 pilots",
        "",
        f"- Overall status: **{payload['overall_status']}**",
        f"- Evidence schema: `{payload['schema_version']}`",
        f"- Run root: `{payload['run_root']}`",
        "",
        "| Pilot | Month | Status | Rows | CE | PE | FUT | Unique key |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for pilot in payload["pilots"]:
        compacted = pilot.get("compacted") or {}
        presence = compacted.get("instrument_presence") or {}
        lines.append(
            f"| {pilot['name']} | {pilot['month']} | **{pilot['status']}** | "
            f"{compacted.get('row_count', 0):,} | "
            f"{_instrument_rows(presence, 'CE'):,} | {_instrument_rows(presence, 'PE'):,} | "
            f"{_instrument_rows(presence, 'FUT'):,} | "
            f"{compacted.get('natural_key_unique', 'n/a')} |"
        )
    for pilot in payload["pilots"]:
        lines.extend(["", f"## {pilot['name']} ({pilot['month']})", ""])
        lines.append(f"Status: **{pilot['status']}**")
        lines.extend(["", "Reasons:", ""])
        lines.extend(f"- {reason}" for reason in pilot["reasons"])
        lines.extend(["", "Input evidence:", ""])
        for name, artifact in pilot["artifacts"].items():
            digest = artifact["sha256"] or "not available"
            lines.append(f"- `{name}`: `{artifact['path']}` — SHA-256 `{digest}`")
        compacted = pilot.get("compacted")
        if compacted:
            lines.extend(["", "Slot coverage:", ""])
            lines.extend(
                [
                    "| Slot | Matrix sources | Matrix rows | Compacted rows | Dates |",
                    "|---|---:|---:|---:|---:|",
                ]
            )
            matrix_slots = pilot["matrix"]["slot_coverage"]
            compacted_slots = compacted["slot_coverage"]
            for slot, _suffix in SLOT_SPECS:
                lines.append(
                    f"| {slot} | {matrix_slots[slot]['downloaded_source_cells']:,} | "
                    f"{matrix_slots[slot]['matrix_extracted_rows']:,} | "
                    f"{compacted_slots[slot]['row_count']:,} | "
                    f"{compacted_slots[slot]['calendar_dates']:,} |"
                )
        special = pilot.get("special_session")
        if special:
            lines.extend(
                [
                    "",
                    "Special-session evidence:",
                    "",
                    f"- Date/weekday: `{special['date']}` / `{special['weekday']}`",
                    f"- Downloaded/extracted slots: `{', '.join(special['downloaded_extracted_slots'])}`",
                    f"- Matrix/compacted NIFTY rows: `{special['matrix_extracted_nifty_rows']:,}` / "
                    f"`{special['compacted_nifty_rows']:,}`",
                    f"- Retained: `{special['retained']}`",
                ]
            )
        expiries = pilot.get("observed_option_expiries") or []
        if expiries:
            lines.extend(
                [
                    "",
                    "Observed NIFTY option expiry values:",
                    "",
                    "| Expiry | Weekday | CE rows | PE rows | First observed | Last observed |",
                    "|---|---|---:|---:|---|---|",
                ]
            )
            for expiry in expiries:
                lines.append(
                    f"| {expiry['expiry_date']} | {expiry['weekday']} | {expiry['ce_rows']:,} | "
                    f"{expiry['pe_rows']:,} | {expiry['first_observed_on']} | "
                    f"{expiry['last_observed_on']} |"
                )
        lines.extend(["", "Evidence limitations:", ""])
        lines.extend(f"- {limitation}" for limitation in pilot["evidence_limitations"])
    lines.extend(["", "## Evidence contract", ""])
    lines.extend(f"- {item}" for item in payload["evidence_contract"])
    return "\n".join(lines) + "\n"


def _evidence_limitations(spec: RequiredPilotSpec) -> list[str]:
    limitations = [
        "The auditor verifies frozen local evidence; it does not re-download or independently authenticate NSE availability.",
        "Source SHA-256 values prove byte identity in recorded lineage, not official publication or effective time.",
        "Slot labels do not prove exact intraday availability, so no publication-time mechanics are inferred.",
    ]
    if spec.special_session_date:
        limitations.append(
            "Saturday retention is proved only by matching downloaded/extracted source evidence to compacted NIFTY rows on 2024-03-02."
        )
    if spec.list_observed_expiries:
        limitations.append(
            "Expiry dates and weekdays are observed CE/PE values only; they do not assert an official expiry rule, transition mechanism, or lot size."
        )
    if spec.pilot_id == "ordinary_recent_2026_06":
        limitations.append(
            "The ordinary-month pilot establishes internal corpus integrity, not external completeness beyond the audited manifests."
        )
    return limitations


def _month_end(year: int, month: int) -> date:
    next_month = date(year + (month == 12), 1 if month == 12 else month + 1, 1)
    return next_month - timedelta(days=1)


def _date_range(start: date, end: date) -> Iterable[date]:
    for ordinal in range(start.toordinal(), end.toordinal() + 1):
        yield date.fromordinal(ordinal)


def _weekday(value: date) -> str:
    return _WEEKDAYS[value.weekday()]


def _load_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("JSON root must be an object")
    return value


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed if parsed >= 0 else None


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _hash_string_set(values: Iterable[str]) -> str:
    return sha256(
        "".join(f"{value}\n" for value in sorted(values)).encode("ascii")
    ).hexdigest()


def _stable_unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _instrument_rows(presence: Mapping[str, Any], instrument: str) -> int:
    value = presence.get(instrument)
    return int(value.get("row_count", 0)) if isinstance(value, Mapping) else 0


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)
