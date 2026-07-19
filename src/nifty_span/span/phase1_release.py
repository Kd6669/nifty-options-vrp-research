"""Publish the owner-accepted Phase 1 SPAN release and Dhan handoff.

The technical audit remains ``BLOCKED_SOURCE``.  This module adds a separate,
fail-closed owner release decision and never mutates acquisition journals,
archives, fragments, or compacted Parquet inputs.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from typing import Any, Iterable, Mapping, Sequence

from .backfill_downloader import DOWNLOADED_STATES, SLOT_SPECS
from .streaming_extractor import NATURAL_KEY


RELEASE_SCHEMA_VERSION = "span-phase1-release/v1"
GAP_SCHEMA_VERSION = "span-source-gap-manifest/v1"
HANDOFF_SCHEMA_VERSION = "dhan-span-handoff/v1"
EXPECTED_MONTHS = 67
EXPECTED_ROWS = 24_870_123
EXPECTED_CELLS = 12_132
EXPECTED_DOWNLOADED = 8_139
EXPECTED_ACCEPTED_UNAVAILABLE = 3_941
EXPECTED_SOURCE_BOUNDARIES = 93
EXPECTED_CORRUPT = 52
EXPECTED_GAPS = EXPECTED_ACCEPTED_UNAVAILABLE + EXPECTED_CORRUPT
EXPECTED_START = "2021-01-01"
EXPECTED_END = "2026-07-15"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def publish_phase1_release(
    *,
    run_root: str | Path,
    repository_commit: str,
    validation_evidence: str | Path,
    accepted_at: str | None = None,
) -> dict[str, Any]:
    """Validate and atomically publish release, gap, and Dhan handoff artifacts."""

    root = Path(run_root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(root)
    if not _GIT_SHA_RE.fullmatch(repository_commit):
        raise ValueError("repository_commit must be a full lowercase Git SHA")
    accepted_at_value = accepted_at or datetime.now(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    _parse_utc_timestamp(accepted_at_value)

    final_root = root / "reports" / "final"
    completion_path = final_root / "SPAN_PHASE1_COMPLETION.json"
    pilot_path = root / "reports" / "required_pilots" / "span_required_pilots.json"
    audit_summary_path = final_root / "audit" / "span_backfill_summary.json"
    matrix_path = final_root / "audit" / "span_date_slot_matrix.parquet"
    availability_path = root / "manifests" / "availability.jsonl"
    validation_path = Path(validation_evidence).resolve()
    required_paths = (
        completion_path,
        pilot_path,
        audit_summary_path,
        matrix_path,
        availability_path,
        validation_path,
    )
    for path in required_paths:
        if not path.is_file() or path.stat().st_size <= 0:
            raise FileNotFoundError(f"required non-empty artifact missing: {path}")

    completion = _load_json(completion_path)
    pilots = _load_json(pilot_path)
    audit = _load_json(audit_summary_path)
    validation = _load_json(validation_path)
    _validate_completion(completion, pilots, audit, validation, root)

    import pyarrow as pa  # type: ignore[import-not-found]
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    matrix = pq.read_table(matrix_path)
    matrix_rows = matrix.to_pylist()
    _validate_matrix(matrix_rows)
    events = _latest_availability_events(availability_path)
    source_evidence_artifacts = _validate_source_evidence(events, availability_path)
    gap_rows = [
        _gap_record(row, events[(str(row["trading_date"]), str(row["slot"]))])
        for row in matrix_rows
        if row.get("download_state") not in DOWNLOADED_STATES
    ]
    _validate_gap_rows(gap_rows)

    compacted_root = root / "compacted"
    inventory, schema, duplicate_count = _compacted_inventory(compacted_root)
    if len(inventory) != EXPECTED_MONTHS:
        raise ValueError(
            f"monthly Parquet count={len(inventory)}, expected {EXPECTED_MONTHS}"
        )
    if sum(item["row_count"] for item in inventory) != EXPECTED_ROWS:
        raise ValueError(
            "monthly Parquet row total does not match pinned release count"
        )
    if duplicate_count != 0:
        raise ValueError(f"natural-key duplicates={duplicate_count}, expected 0")
    partials = sorted(str(path.resolve()) for path in root.rglob("*.partial"))
    if partials:
        raise ValueError(f"orphan partial files remain: {len(partials)}")

    final_root.mkdir(parents=True, exist_ok=True)
    gap_parquet = final_root / "span_source_gap_manifest.parquet"
    gap_json = final_root / "span_source_gap_manifest.json"
    _atomic_parquet(pa.Table.from_pylist(gap_rows), gap_parquet)
    gap_payload = {
        "schema_version": GAP_SCHEMA_VERSION,
        "row_count": len(gap_rows),
        "accepted_unavailable_cells": EXPECTED_ACCEPTED_UNAVAILABLE,
        "corrupt_source_cells": EXPECTED_CORRUPT,
        "source_boundary_cells": EXPECTED_SOURCE_BOUNDARIES,
        "rows": gap_rows,
    }
    _atomic_json(gap_json, gap_payload)
    gap_artifacts = {
        "json": _fingerprint(gap_json),
        "parquet": _fingerprint(gap_parquet),
    }

    immutable_evidence = {
        "technical_completion": _fingerprint(completion_path),
        "final_audit_summary": _fingerprint(audit_summary_path),
        "final_audit_matrix": _fingerprint(matrix_path),
        "required_pilots": _fingerprint(pilot_path),
        "availability_manifest": _fingerprint(availability_path),
        "validation_evidence": _fingerprint(validation_path),
    }
    source_stability = completion.get("source_stability", {})
    journal_checkpoints = {
        name: _fingerprint(root / "manifests" / f"{name}.jsonl")
        for name in ("download", "extraction")
    }
    for name, fingerprint in journal_checkpoints.items():
        recorded = (source_stability.get("after") or {}).get(name) or {}
        if (
            recorded.get("sha256") != fingerprint["sha256"]
            or recorded.get("size_bytes") != fingerprint["size_bytes"]
        ):
            raise ValueError(f"{name} journal changed after terminal finalization")

    counts = {
        "expected_cells": EXPECTED_CELLS,
        "accounted_cells": EXPECTED_CELLS,
        "downloaded_and_extracted_cells": EXPECTED_DOWNLOADED,
        "accepted_unavailable_cells": EXPECTED_ACCEPTED_UNAVAILABLE,
        "source_boundary_cells": EXPECTED_SOURCE_BOUNDARIES,
        "repeated_corrupt_source_cells": EXPECTED_CORRUPT,
        "unresolved_non_source_boundary_cells": 0,
        "source_gap_manifest_rows": EXPECTED_GAPS,
        "monthly_parquets": EXPECTED_MONTHS,
        "compacted_rows": EXPECTED_ROWS,
        "natural_key_duplicates": duplicate_count,
        "orphan_partial_files": 0,
    }
    representations = {
        "symbol": {"representation": "string", "observed_release_value": "NIFTY"},
        "instrument": {
            "representation": "string",
            "values": ["CE", "PE", "FUT"],
            "option_type_encoding": {"call": "CE", "put": "PE", "future": "FUT"},
        },
        "strike": {
            "type": "double",
            "units": "index points; FUT rows use the source representation, commonly 0.0",
        },
        "expiry": {"type": "date32[day]", "timezone": None},
        "date": {"type": "date32[day]", "timezone": None},
        "ingested_at_utc": {"type": "timestamp[us, tz=UTC]", "timezone": "UTC"},
        "span_effective_ts_ist": {
            "type": "timestamp[us, tz=Asia/Kolkata]",
            "timezone": "Asia/Kolkata",
            "nullable": True,
        },
        "slots": [slot for slot, _suffix in SLOT_SPECS],
    }
    timing_warning = (
        "Slot labels BOD/ID1/ID2/ID3/ID4/EOD do not prove exact NSE publication "
        "timestamps. Do not map minute timestamps to SPAN slots using guessed times."
    )

    release = {
        "schema_version": RELEASE_SCHEMA_VERSION,
        "release_status": "ACCEPTED_WITH_SOURCE_GAPS",
        "technical_audit_outcome": "BLOCKED_SOURCE",
        "owner_source_gap_acceptance": True,
        "accepted_by_owner": True,
        "accepted_at": accepted_at_value,
        "requested_range": {"start_date": EXPECTED_START, "end_date": EXPECTED_END},
        "dataset_root": str(compacted_root.resolve()),
        "parquet_glob": str((compacted_root / "????_??.parquet").resolve()),
        "counts": counts,
        "natural_key": list(NATURAL_KEY),
        "schema": schema,
        "representations": representations,
        "monthly_inventory": inventory,
        "source_gap_artifacts": gap_artifacts,
        "exception_artifacts": gap_artifacts,
        "audit_and_pilot_evidence": immutable_evidence,
        "source_evidence_artifacts": source_evidence_artifacts,
        "journal_checkpoints": journal_checkpoints,
        "repository_commit_sha": repository_commit,
        "test_lint_evidence": validation,
        "timing_warning": timing_warning,
        "integrity_contract": {
            "no_fake_rows": True,
            "no_forward_fill": True,
            "no_backfill": True,
            "no_interpolation": True,
            "corrupt_archives_remain_corrupt": True,
            "technical_outcome_preserved": True,
        },
    }
    release_json = final_root / "SPAN_PHASE1_RELEASE_MANIFEST.json"
    release_md = final_root / "SPAN_PHASE1_RELEASE_MANIFEST.md"
    _atomic_json(release_json, release)
    _atomic_text(release_md, _release_markdown(release))

    handoff = {
        "schema_version": HANDOFF_SCHEMA_VERSION,
        "release_status": release["release_status"],
        "technical_audit_outcome": release["technical_audit_outcome"],
        "canonical_parquet_root": release["dataset_root"],
        "parquet_glob": release["parquet_glob"],
        "monthly_file_count": EXPECTED_MONTHS,
        "monthly_inventory": inventory,
        "total_rows": EXPECTED_ROWS,
        "schema": schema,
        "natural_key": list(NATURAL_KEY),
        "natural_key_duplicate_count": duplicate_count,
        "representations": representations,
        "slot_values": [slot for slot, _suffix in SLOT_SPECS],
        "source_gap_manifest": gap_artifacts,
        "source_gap_cardinality": {
            "all_cells_without_canonical_archive": EXPECTED_GAPS,
            "accepted_unavailable": EXPECTED_ACCEPTED_UNAVAILABLE,
            "proven_source_boundaries": EXPECTED_SOURCE_BOUNDARIES,
            "corrupt_http_200": EXPECTED_CORRUPT,
        },
        "cardinality_expectations": {
            "span_natural_key": "at most one row per natural key",
            "date_contract_slots": "zero to six observations depending on official source availability",
            "gap_manifest": "exactly one row per date/slot cell without a valid canonical archive",
        },
        "join_validation_requirements": [
            "Never silently drop Dhan rows because SPAN is absent.",
            "Never create, forward-fill, backfill, or interpolate a missing SPAN observation.",
            "Preserve all six SPAN observations or widen them into six distinct slot-specific column groups.",
            "Validate join cardinality and assert no natural-key multiplication.",
            "Retain source SHA-256, source member, slot, and gap status lineage.",
            "A minute-level as-of join requires a separately documented, non-leaking timing policy.",
        ],
        "recommended_downstream_status_columns": [
            "span_join_status",
            "span_source_status",
            "span_source_boundary",
            "span_source_gap_reason",
            "span_time_slot",
        ],
        "safe_initial_integration": [
            "Preserve SPAN as six date/contract/slot observations.",
            "Alternatively widen the six slots into distinct slot-specific columns.",
        ],
        "publication_timing_warning": timing_warning,
        "release_manifest": None,
    }
    handoff_json = final_root / "DHAN_SPAN_HANDOFF.json"
    handoff_md = final_root / "DHAN_SPAN_HANDOFF.md"
    release_fp = _fingerprint(release_json)
    handoff["release_manifest"] = release_fp
    _atomic_json(handoff_json, handoff)
    _atomic_text(handoff_md, _handoff_markdown(handoff))

    return {
        "release_status": release["release_status"],
        "technical_audit_outcome": release["technical_audit_outcome"],
        "release_manifest": _fingerprint(release_json),
        "release_markdown": _fingerprint(release_md),
        "source_gap_json": _fingerprint(gap_json),
        "source_gap_parquet": _fingerprint(gap_parquet),
        "dhan_handoff_json": _fingerprint(handoff_json),
        "dhan_handoff_markdown": _fingerprint(handoff_md),
    }


def _validate_completion(
    completion: Mapping[str, Any],
    pilots: Mapping[str, Any],
    audit: Mapping[str, Any],
    validation: Mapping[str, Any],
    root: Path,
) -> None:
    if completion.get("outcome") != "BLOCKED_SOURCE":
        raise ValueError("terminal technical outcome must remain BLOCKED_SOURCE")
    if completion.get("blocked_matrix_ready") is not True:
        raise ValueError("terminal finalizer did not prove blocked-matrix readiness")
    if pilots.get("overall_status") != "PASS":
        raise ValueError("all required pilots must PASS")
    audit_expected = {
        "start_date": EXPECTED_START,
        "end_date": EXPECTED_END,
        "expected_cells": EXPECTED_CELLS,
        "accounted_cells": EXPECTED_CELLS,
        "downloaded_cells": EXPECTED_DOWNLOADED,
        "accepted_unavailable_cells": EXPECTED_ACCEPTED_UNAVAILABLE,
        "source_boundary_cells": EXPECTED_SOURCE_BOUNDARIES,
        "unresolved_non_boundary_cells": 0,
        "compacted_months": EXPECTED_MONTHS,
        "compacted_rows": EXPECTED_ROWS,
        "duplicate_natural_keys": 0,
        "outcome": "BLOCKED_SOURCE",
    }
    for key, expected in audit_expected.items():
        if audit.get(key) != expected:
            raise ValueError(f"audit {key}={audit.get(key)!r}, expected {expected!r}")
    if Path(str(pilots.get("run_root", ""))).resolve() != root:
        raise ValueError("pilot run_root does not match release root")
    required_validation = {
        "focused_tests": "PASS",
        "full_tests": "PASS",
        "ruff_check": "PASS",
        "ruff_format_check": "PASS",
        "compileall": "PASS",
        "diff_check": "PASS",
        "active_phase1_processes": 0,
    }
    for key, expected in required_validation.items():
        if validation.get(key) != expected:
            raise ValueError(
                f"validation evidence {key}={validation.get(key)!r}, expected {expected!r}"
            )


def _validate_matrix(rows: Sequence[Mapping[str, Any]]) -> None:
    if len(rows) != EXPECTED_CELLS:
        raise ValueError(f"matrix rows={len(rows)}, expected {EXPECTED_CELLS}")
    keys = [(str(row.get("trading_date")), str(row.get("slot"))) for row in rows]
    if len(set(keys)) != EXPECTED_CELLS:
        raise ValueError("matrix date/slot keys are not unique")
    downloaded = [row for row in rows if row.get("download_state") in DOWNLOADED_STATES]
    if len(downloaded) != EXPECTED_DOWNLOADED:
        raise ValueError(f"downloaded matrix cells={len(downloaded)}")
    for row in downloaded:
        if not (
            row.get("raw_integrity_ok") is True
            and row.get("fragment_exists") is True
            and row.get("extraction_state")
            in {"fragment_created", "fragment_already_valid"}
        ):
            raise ValueError(
                f"downloaded cell lacks extraction integrity: {row.get('trading_date')} {row.get('slot')}"
            )
    if (
        sum(row.get("source_boundary_proven") is True for row in rows)
        != EXPECTED_SOURCE_BOUNDARIES
    ):
        raise ValueError("source-boundary count does not match pinned release count")


def _latest_availability_events(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    events: dict[tuple[str, str], dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.endswith("\n"):
                raise ValueError(f"availability JSONL has partial line {line_number}")
            event = json.loads(line)
            key = (str(event.get("trading_date")), str(event.get("slot")))
            if key[0] == "None" or key[1] == "None":
                raise ValueError(f"availability event {line_number} lacks date/slot")
            event["_line_number"] = line_number
            events[key] = event
    return events


def _gap_record(matrix: Mapping[str, Any], event: Mapping[str, Any]) -> dict[str, Any]:
    state = str(matrix.get("download_state"))
    boundary = matrix.get("source_boundary_proven") is True
    if state == "corrupt_inner_zip":
        category = "repeated_corrupt_http_200"
    elif boundary:
        category = "repeated_http_404"
    else:
        category = "ordinary_unavailable"
    event_json = json.dumps(
        {key: value for key, value in event.items() if key != "_line_number"},
        sort_keys=True,
        separators=(",", ":"),
    )
    event_id = event.get("event_id") or sha256(event_json.encode("utf-8")).hexdigest()
    reports = event.get("reports_api_evidence") or {}
    observations = event.get("static_archive_observations") or []
    if not observations and isinstance(event.get("static_archive_evidence"), Mapping):
        observations = [event["static_archive_evidence"]]
    static_hashes = sorted(
        {
            str(item.get("body_sha256"))
            for item in observations
            if item.get("body_sha256")
        }
    )
    source_hashes = sorted(
        {
            str(item.get("source_sha256"))
            for item in (event.get("sources") or [])
            if item.get("source_sha256")
        }
    )
    snapshot_inputs = static_hashes or source_hashes
    static_snapshot = _hash_string_set(snapshot_inputs) if snapshot_inputs else None
    statuses = sorted(
        {
            int(item["http_status"])
            for item in observations
            if item.get("http_status") is not None
        }
    )
    http_status = statuses[0] if len(statuses) == 1 else matrix.get("http_status")
    report_hash = reports.get("manifest_snapshot_sha256")
    return {
        "trading_date": str(matrix.get("trading_date")),
        "slot": str(matrix.get("slot")),
        "suffix": str(matrix.get("suffix")),
        "final_download_state": state,
        "availability_classification": event.get("calendar_classification"),
        "classification_outcome": event.get("classification_outcome"),
        "source_boundary_category": event.get("calendar_classification")
        if boundary
        else None,
        "source_boundary_proven": boundary,
        "http_status": http_status,
        "evidence_basis": event.get("evidence_basis") or event.get("reason"),
        "evidence_event_id": str(event_id),
        "evidence_event_id_source": "manifest"
        if event.get("event_id")
        else "derived_event_sha256",
        "availability_event_sha256": sha256(event_json.encode("utf-8")).hexdigest(),
        "availability_manifest_line": int(event.get("_line_number", 0)),
        "reports_manifest_snapshot_sha256": report_hash,
        "static_snapshot_sha256": static_snapshot,
        "reports_or_static_snapshot_sha256": report_hash or static_snapshot,
        "gap_category": category,
        "canonical_archive_availability": "absent",
        "safe_downstream_status": "NO_SPAN_OBSERVATION_DO_NOT_FILL",
        "availability_event_type": event.get("event"),
        "availability_source_ids": [
            str(item.get("source_id"))
            for item in (event.get("sources") or [])
            if item.get("source_id")
        ],
        "static_observation_count": len(observations),
        "static_http_statuses": statuses,
        "static_payload_sha256": static_hashes,
    }


def _validate_gap_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    if len(rows) != EXPECTED_GAPS:
        raise ValueError(f"gap rows={len(rows)}, expected {EXPECTED_GAPS}")
    if len({(row["trading_date"], row["slot"]) for row in rows}) != EXPECTED_GAPS:
        raise ValueError("gap date/slot keys are not unique")
    counts = Counter(str(row["gap_category"]) for row in rows)
    expected_categories = {
        "ordinary_unavailable": 3_900,
        "repeated_http_404": 41,
        "repeated_corrupt_http_200": EXPECTED_CORRUPT,
    }
    if dict(counts) != expected_categories:
        raise ValueError(
            f"gap category counts={dict(counts)!r}, expected {expected_categories!r}"
        )
    if (
        sum(row.get("source_boundary_proven") is True for row in rows)
        != EXPECTED_SOURCE_BOUNDARIES
    ):
        raise ValueError("gap source-boundary count does not match pinned count")
    for row in rows:
        event_id = str(row.get("evidence_event_id", ""))
        if not (
            _SHA256_RE.fullmatch(event_id) or re.fullmatch(r"[0-9a-f]{32}", event_id)
        ):
            raise ValueError("gap evidence event ID is missing or invalid")
        if not _SHA256_RE.fullmatch(str(row.get("availability_event_sha256", ""))):
            raise ValueError("gap availability event fingerprint is missing or invalid")
        if not _SHA256_RE.fullmatch(
            str(row.get("reports_or_static_snapshot_sha256", ""))
        ):
            raise ValueError("gap evidence snapshot hash is missing or invalid")
        if row["gap_category"] == "repeated_http_404":
            if row.get("static_observation_count") != 3 or row.get(
                "static_http_statuses"
            ) != [404]:
                raise ValueError(
                    "repeated HTTP 404 boundary lacks three exact observations"
                )
        if row["gap_category"] == "repeated_corrupt_http_200":
            if row.get("static_http_statuses") != [200]:
                raise ValueError("corrupt source boundary is not backed by HTTP 200")


def _validate_source_evidence(
    events: Mapping[tuple[str, str], Mapping[str, Any]], availability_path: Path
) -> list[dict[str, Any]]:
    """Hash-check every unique local source document and report snapshot."""

    expected_by_path: dict[Path, str] = {}
    for event in events.values():
        for source in event.get("sources") or []:
            raw_path = Path(str(source.get("source_artifact_path", "")))
            path = (
                raw_path
                if raw_path.is_absolute()
                else availability_path.parent / raw_path
            )
            expected = str(source.get("source_sha256", ""))
            if raw_path and expected:
                expected_by_path[path.resolve()] = expected
        reports = event.get("reports_api_evidence") or {}
        if reports.get("manifest_snapshot_path") and reports.get(
            "manifest_snapshot_sha256"
        ):
            expected_by_path[Path(str(reports["manifest_snapshot_path"])).resolve()] = (
                str(reports["manifest_snapshot_sha256"])
            )
    result: list[dict[str, Any]] = []
    for path, expected in sorted(
        expected_by_path.items(), key=lambda item: str(item[0])
    ):
        if not path.is_file():
            raise FileNotFoundError(f"source evidence artifact missing: {path}")
        actual = _sha256_file(path)
        if actual != expected:
            raise ValueError(f"source evidence hash mismatch: {path}")
        result.append(_fingerprint(path))
    if not result:
        raise ValueError("no local source evidence artifacts were validated")
    return result


def _compacted_inventory(
    root: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    paths = sorted(root.glob("????_??.parquet"))
    expected = _expected_month_keys()
    if [path.stem for path in paths] != expected:
        raise ValueError(
            "compacted monthly inventory is missing, duplicated, or out of range"
        )
    inventory: list[dict[str, Any]] = []
    schema_text: str | None = None
    schema: list[dict[str, Any]] = []
    duplicates = 0
    for path in paths:
        parquet = pq.ParquetFile(path)
        arrow_schema = parquet.schema_arrow
        current_schema = str(arrow_schema)
        if schema_text is None:
            schema_text = current_schema
            schema = [
                {
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable,
                }
                for field in arrow_schema
            ]
        elif current_schema != schema_text:
            raise ValueError(f"schema drift in {path}")
        table = pq.read_table(path, columns=list(NATURAL_KEY))
        month_duplicates = _count_duplicates(table)
        duplicates += month_duplicates
        inventory.append(
            {
                "month": path.stem.replace("_", "-"),
                "path": str(path.resolve()),
                "row_count": parquet.metadata.num_rows,
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
                "natural_key_duplicates": month_duplicates,
            }
        )
    return inventory, schema, duplicates


def _count_duplicates(table: Any) -> int:
    import pyarrow.compute as pc  # type: ignore[import-not-found]

    if table.num_rows < 2:
        return 0
    ordered = table.sort_by(
        [(name, "ascending") for name in NATURAL_KEY]
    ).combine_chunks()
    equal = None
    for name in NATURAL_KEY:
        column = ordered.column(name)
        current = pc.equal(column.slice(1), column.slice(0, ordered.num_rows - 1))
        equal = current if equal is None else pc.and_(equal, current)
    return int(pc.sum(equal).as_py() or 0)


def _expected_month_keys() -> list[str]:
    result: list[str] = []
    year, month = 2021, 1
    while (year, month) <= (2026, 7):
        result.append(f"{year:04d}_{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return result


def _fingerprint(path: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


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


def _load_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def _parse_utc_timestamp(value: str) -> None:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("accepted_at must be an explicit UTC timestamp")


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(
        path, json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    with partial.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(partial, path)


def _atomic_parquet(table: Any, path: Path) -> None:
    import pyarrow.parquet as pq  # type: ignore[import-not-found]

    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".partial")
    pq.write_table(table, partial, compression="zstd")
    os.replace(partial, path)


def _release_markdown(payload: Mapping[str, Any]) -> str:
    counts = payload["counts"]
    return (
        "# SPAN Phase 1 release manifest\n\n"
        f"- Release status: **{payload['release_status']}**\n"
        f"- Technical audit outcome: **{payload['technical_audit_outcome']}**\n"
        f"- Owner source-gap acceptance: `{payload['owner_source_gap_acceptance']}`\n"
        f"- Accepted at: `{payload['accepted_at']}`\n"
        f"- Dataset root: `{payload['dataset_root']}`\n"
        f"- Requested range: `{payload['requested_range']['start_date']}` through `{payload['requested_range']['end_date']}`\n"
        f"- Monthly Parquets / rows: `{counts['monthly_parquets']}` / `{counts['compacted_rows']:,}`\n"
        f"- Downloaded and extracted cells: `{counts['downloaded_and_extracted_cells']:,}`\n"
        f"- Accepted unavailable / proven boundaries / corrupt HTTP 200: `{counts['accepted_unavailable_cells']:,}` / `{counts['source_boundary_cells']}` / `{counts['repeated_corrupt_source_cells']}`\n"
        f"- Natural-key duplicates: `{counts['natural_key_duplicates']}`\n"
        f"- Repository commit: `{payload['repository_commit_sha']}`\n\n"
        "The release decision does not alter the technical source-boundary result. "
        "No missing or corrupt observation is fabricated, filled, or reclassified as downloaded.\n\n"
        f"> {payload['timing_warning']}\n"
    )


def _handoff_markdown(payload: Mapping[str, Any]) -> str:
    return (
        "# Dhan integration handoff: NSE SPAN Phase 1\n\n"
        f"- Canonical Parquet root: `{payload['canonical_parquet_root']}`\n"
        f"- Parquet glob: `{payload['parquet_glob']}`\n"
        f"- Monthly files / rows: `{payload['monthly_file_count']}` / `{payload['total_rows']:,}`\n"
        f"- Natural key: `{', '.join(payload['natural_key'])}`\n"
        f"- Duplicate natural keys: `{payload['natural_key_duplicate_count']}`\n"
        f"- Slot values: `{', '.join(payload['slot_values'])}`\n\n"
        "Safe integration keeps the six slot observations distinct, either as rows or widened slot-specific columns. "
        "Missing SPAN observations remain explicit status, never filled values.\n\n"
        f"> **Timing warning:** {payload['publication_timing_warning']}\n"
    )
