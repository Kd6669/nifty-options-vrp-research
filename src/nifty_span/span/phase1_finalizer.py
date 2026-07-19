"""One-shot, fail-closed finalization of a quiescent Phase 1 SPAN run."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
import uuid

from .backfill_audit import audit_span_backfill
from .corrupt_recovery import validate_corrupt_recovery_report
from .manifest_exports import export_latest_manifest
from .required_pilots import (
    REQUIRED_PILOTS,
    SCHEMA_VERSION as PILOT_SCHEMA_VERSION,
    inspect_required_span_pilots,
)


PINNED_START = date(2021, 1, 1)
PINNED_END = date(2026, 7, 15)
PINNED_DATES = 2_022
PINNED_CELLS = 12_132
FINALIZER_SCHEMA_VERSION = "span-phase1-finalizer-v1"
BENCHMARK_SCHEMA_VERSION = "span-benchmark-binding-limit/v1"
REQUIRED_PILOT_IDS = frozenset(spec.pilot_id for spec in REQUIRED_PILOTS)


@dataclass(frozen=True)
class FileFingerprint:
    path: str
    exists: bool
    size_bytes: int | None
    sha256: str | None


@dataclass(frozen=True)
class Phase1FinalizationReport:
    outcome: str
    ok: bool
    completion_json: str
    completion_markdown: str
    payload: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "completion_json": self.completion_json,
            "completion_markdown": self.completion_markdown,
            "ok": self.ok,
            "outcome": self.outcome,
        }


def finalize_span_phase1(
    *,
    run_root: str | Path,
    start_date: date,
    end_date: date,
    availability_manifest: str | Path,
    benchmark_artifacts: Sequence[str | Path],
    pilot_artifacts: Sequence[str | Path],
    recovery_artifacts: Sequence[str | Path],
    commit_sha: str | None = None,
    test_result: str | None = None,
    tool_versions: Mapping[str, str] | None = None,
) -> Phase1FinalizationReport:
    """Finalize a fully quiesced run and return non-ready evidence otherwise."""
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    root = Path(run_root).resolve()
    if not root.is_dir():
        raise FileNotFoundError(root)
    paths = {
        "download": root / "manifests" / "download.jsonl",
        "extraction": root / "manifests" / "extraction.jsonl",
        "availability": Path(availability_manifest).resolve(),
    }
    for name, path in paths.items():
        if not path.is_file():
            raise FileNotFoundError(f"{name} manifest does not exist: {path}")

    final_root = root / "reports" / "final"
    export_root = root / "manifests" / "final"
    audit_root = final_root / "audit"
    before = {name: _fingerprint(path) for name, path in paths.items()}
    download_export = export_latest_manifest(
        paths["download"], export_root, manifest_kind="download"
    )
    extraction_export = export_latest_manifest(
        paths["extraction"], export_root, manifest_kind="extraction"
    )
    availability_export_path = export_root / "availability_manifest.jsonl"
    _atomic_write_bytes(availability_export_path, paths["availability"].read_bytes())
    audit = audit_span_backfill(
        start_date=start_date,
        end_date=end_date,
        raw_root=root / "raw",
        download_manifest=paths["download"],
        extraction_manifest=paths["extraction"],
        fragment_root=root / "fragments",
        compacted_root=root / "compacted",
        report_root=audit_root,
        availability_manifest=paths["availability"],
    )
    after = {name: _fingerprint(path) for name, path in paths.items()}

    source_checks = _source_stability_checks(
        before=before,
        after=after,
        download_export=download_export,
        extraction_export=extraction_export,
        audit=audit,
    )
    source_stable = all(source_checks.values())
    benchmarks, benchmarks_complete = _evidence(
        benchmark_artifacts, kind="benchmark", expected_run_root=root
    )
    pilots, pilots_complete = _evidence(
        pilot_artifacts, kind="pilot", expected_run_root=root
    )
    recoveries, recovery_complete = _recovery_evidence(
        recovery_artifacts,
        run_root=root,
        availability_manifest=paths["availability"],
        start_date=start_date,
        end_date=end_date,
    )
    evidence_complete = benchmarks_complete and pilots_complete and recovery_complete
    range_matches = start_date == PINNED_START and end_date == PINNED_END
    matrix_checks = _matrix_checks(audit)
    matrix_ready = all(matrix_checks.values())
    blocked_matrix_checks = {
        "accounted_cells_exact": int(audit.accounted_cells) == PINNED_CELLS,
        "blocked_matrix_complete": bool(audit.blocked_matrix_complete),
        "compacted_unique": bool(audit.compacted_unique),
        "downloaded_extraction_complete": bool(audit.extraction_complete),
        "expected_cells_exact": int(audit.expected_cells) == PINNED_CELLS,
        "raw_integrity_ok": bool(audit.raw_integrity_ok),
        "requested_dates_exact": int(audit.requested_dates) == PINNED_DATES,
        "source_boundary_cells_positive": int(audit.source_boundary_cells) > 0,
        "unresolved_missing_zero": int(audit.unresolved_missing_cells) == 0,
        "unresolved_non_boundary_zero": int(audit.unresolved_non_boundary_cells) == 0,
    }
    blocked_matrix_ready = all(blocked_matrix_checks.values())
    exported_artifacts = {
        "download_json": _fingerprint(Path(download_export.json_path)),
        "download_parquet": _fingerprint(Path(download_export.parquet_path)),
        "download_metadata": _fingerprint(Path(download_export.metadata_path)),
        "extraction_json": _fingerprint(Path(extraction_export.json_path)),
        "extraction_parquet": _fingerprint(Path(extraction_export.parquet_path)),
        "extraction_metadata": _fingerprint(Path(extraction_export.metadata_path)),
        "availability_jsonl": _fingerprint(availability_export_path),
    }
    audit_artifacts = {
        "matrix_parquet": _fingerprint(Path(audit.matrix_parquet)),
        "summary_json": _fingerprint(Path(audit.summary_json)),
        "audit_markdown": _fingerprint(Path(audit.audit_markdown)),
    }
    artifact_checks = {
        "all_audit_artifacts_nonempty": _all_nonempty(audit_artifacts.values()),
        "all_export_artifacts_nonempty": _all_nonempty(exported_artifacts.values()),
        "download_json_hash_matches_export": (
            exported_artifacts["download_json"].sha256 == download_export.json_sha256
        ),
        "download_parquet_hash_matches_export": (
            exported_artifacts["download_parquet"].sha256
            == download_export.parquet_sha256
        ),
        "extraction_json_hash_matches_export": (
            exported_artifacts["extraction_json"].sha256
            == extraction_export.json_sha256
        ),
        "extraction_parquet_hash_matches_export": (
            exported_artifacts["extraction_parquet"].sha256
            == extraction_export.parquet_sha256
        ),
        "availability_export_hash_matches_source": (
            exported_artifacts["availability_jsonl"].sha256
            == before["availability"].sha256
        ),
    }
    artifacts_complete = all(artifact_checks.values())
    audit_outcome = str(audit.outcome)
    if not source_stable or not artifacts_complete or not range_matches:
        outcome = "FAIL_INCOMPLETE"
    elif (
        audit_outcome == "BLOCKED_SOURCE" and blocked_matrix_ready and recovery_complete
    ):
        outcome = "BLOCKED_SOURCE"
    elif not evidence_complete:
        outcome = "FAIL_INCOMPLETE"
    elif audit_outcome == "PASS_READY" and bool(audit.ok) and matrix_ready:
        outcome = "PASS_READY"
    else:
        outcome = "FAIL_INCOMPLETE"

    payload: dict[str, Any] = {
        "artifacts": {
            "audit": _dataclass_mapping(audit_artifacts),
            "benchmarks": benchmarks,
            "exports": _dataclass_mapping(exported_artifacts),
            "pilots": pilots,
            "recovery": recoveries,
        },
        "artifact_checks": artifact_checks,
        "audit": {
            "accepted_unavailable_cells": int(audit.accepted_unavailable_cells),
            "accounted_cells": int(audit.accounted_cells),
            "audit_outcome": audit_outcome,
            "compacted_months": int(audit.compacted_months),
            "compacted_rows": int(audit.compacted_rows),
            "downloaded_cells": int(audit.downloaded_cells),
            "earliest_proven_download_date": audit.earliest_proven_download_date,
            "expected_cells": int(audit.expected_cells),
            "failed_or_incomplete_cells": int(audit.failed_or_incomplete_cells),
            "latest_proven_download_date": audit.latest_proven_download_date,
            "requested_dates": int(audit.requested_dates),
            "slot_year_counts": [
                _object_mapping(item) for item in audit.slot_year_counts
            ],
            "terminal_cells": int(audit.terminal_cells),
            "unresolved_missing_cells": int(audit.unresolved_missing_cells),
            "source_boundary_cells": int(audit.source_boundary_cells),
            "resolved_or_blocked_cells": int(audit.resolved_or_blocked_cells),
            "unresolved_non_boundary_cells": int(audit.unresolved_non_boundary_cells),
        },
        "evidence_complete": evidence_complete,
        "finalizer_schema_version": FINALIZER_SCHEMA_VERSION,
        "blocked_matrix_checks": blocked_matrix_checks,
        "blocked_matrix_ready": blocked_matrix_ready,
        "matrix_checks": matrix_checks,
        "metadata": {
            "commit_sha": commit_sha,
            "test_result": test_result,
            "tool_versions": dict(sorted((tool_versions or {}).items())),
        },
        "ok": outcome == "PASS_READY",
        "outcome": outcome,
        "pinned_contract": {
            "cells": PINNED_CELLS,
            "dates": PINNED_DATES,
            "end_date": PINNED_END.isoformat(),
            "range_matches": range_matches,
            "start_date": PINNED_START.isoformat(),
        },
        "requested_range": {
            "end_date": end_date.isoformat(),
            "start_date": start_date.isoformat(),
        },
        "source_stability": {
            "after": _dataclass_mapping(after),
            "before": _dataclass_mapping(before),
            "checks": source_checks,
            "stable": source_stable,
        },
    }
    completion_json = final_root / "SPAN_PHASE1_COMPLETION.json"
    completion_markdown = final_root / "SPAN_PHASE1_COMPLETION.md"
    _atomic_text(
        completion_json,
        json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
    )
    _atomic_text(completion_markdown, _markdown(payload))
    return Phase1FinalizationReport(
        outcome=outcome,
        ok=outcome == "PASS_READY",
        completion_json=str(completion_json),
        completion_markdown=str(completion_markdown),
        payload=payload,
    )


def _source_stability_checks(
    *,
    before: Mapping[str, FileFingerprint],
    after: Mapping[str, FileFingerprint],
    download_export: Any,
    extraction_export: Any,
    audit: Any,
) -> dict[str, bool]:
    return {
        "availability_unchanged": before["availability"] == after["availability"],
        "audit_availability_hash_matches": (
            audit.availability_manifest_sha256 == before["availability"].sha256
        ),
        "audit_download_hash_matches": audit.download_manifest_sha256
        == before["download"].sha256,
        "audit_extraction_hash_matches": (
            audit.extraction_manifest_sha256 == before["extraction"].sha256
        ),
        "download_export_is_full_file": (
            download_export.ignored_trailing_bytes == 0
            and download_export.source_prefix_bytes == before["download"].size_bytes
            and download_export.source_prefix_sha256 == before["download"].sha256
        ),
        "download_unchanged": before["download"] == after["download"],
        "extraction_export_is_full_file": (
            extraction_export.ignored_trailing_bytes == 0
            and extraction_export.source_prefix_bytes == before["extraction"].size_bytes
            and extraction_export.source_prefix_sha256 == before["extraction"].sha256
        ),
        "extraction_unchanged": before["extraction"] == after["extraction"],
    }


def _matrix_checks(audit: Any) -> dict[str, bool]:
    return {
        "accounted_cells_exact": int(audit.accounted_cells) == PINNED_CELLS,
        "audit_ok": bool(audit.ok),
        "compacted_unique": bool(audit.compacted_unique),
        "downloaded_extraction_complete": bool(audit.extraction_complete),
        "expected_cells_exact": int(audit.expected_cells) == PINNED_CELLS,
        "failed_or_incomplete_zero": int(audit.failed_or_incomplete_cells) == 0,
        "matrix_complete": bool(audit.matrix_complete),
        "raw_integrity_ok": bool(audit.raw_integrity_ok),
        "requested_dates_exact": int(audit.requested_dates) == PINNED_DATES,
        "terminal_cells_exact": int(audit.terminal_cells) == PINNED_CELLS,
        "unresolved_missing_zero": int(audit.unresolved_missing_cells) == 0,
    }


def _evidence(
    paths: Sequence[str | Path], *, kind: str, expected_run_root: Path
) -> tuple[list[dict[str, Any]], bool]:
    unique = sorted({str(Path(path).resolve()) for path in paths})
    evidence = []
    for path_text in unique:
        path = Path(path_text)
        item = asdict(_fingerprint(path))
        item["acceptance"] = _evidence_acceptance(
            path, kind=kind, expected_run_root=expected_run_root
        )
        evidence.append(item)
    return evidence, bool(evidence) and all(
        item["exists"]
        and int(item["size_bytes"] or 0) > 0
        and item["acceptance"] == "PASS"
        for item in evidence
    )


def _recovery_evidence(
    paths: Sequence[str | Path],
    *,
    run_root: Path,
    availability_manifest: Path,
    start_date: date,
    end_date: date,
) -> tuple[list[dict[str, Any]], bool]:
    unique = sorted({str(Path(path).resolve()) for path in paths})
    evidence: list[dict[str, Any]] = []
    for path_text in unique:
        path = Path(path_text)
        try:
            item = validate_corrupt_recovery_report(
                path,
                start_date=start_date,
                end_date=end_date,
                raw_root=run_root / "raw",
                download_manifest=run_root / "manifests" / "download.jsonl",
                availability_manifest=availability_manifest,
            )
            item["acceptance"] = "PASS"
        except (OSError, ValueError, RuntimeError, json.JSONDecodeError) as exc:
            item = asdict(_fingerprint(path))
            item["acceptance"] = "FAIL"
            item["validation_error"] = f"{type(exc).__name__}: {exc}"
        evidence.append(item)
    return evidence, bool(
        len(evidence) == 1
        and evidence[0]["acceptance"] == "PASS"
        and evidence[0].get("ok") is True
    )


def _evidence_acceptance(path: Path, *, kind: str, expected_run_root: Path) -> str:
    if not path.is_file() or path.stat().st_size == 0:
        return "MISSING"
    if path.suffix.lower() != ".json":
        return "UNVERIFIED_NON_JSON"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "INVALID_JSON"
    if not isinstance(payload, Mapping):
        return "INVALID_JSON"
    if kind == "benchmark":
        return _benchmark_evidence_acceptance(payload)
    if kind == "pilot":
        return _pilot_evidence_acceptance(payload, expected_run_root)
    positive, negative, unknown = _acceptance_signals(payload)
    if negative:
        return "FAIL"
    if unknown:
        return "UNVERIFIED"
    return "PASS" if positive else "UNVERIFIED"


def _benchmark_evidence_acceptance(payload: Mapping[str, Any]) -> str:
    if (
        payload.get("schema_version") != BENCHMARK_SCHEMA_VERSION
        or payload.get("overall_outcome") != "PASS_BINDING_LIMIT"
        or payload.get("ok") is not True
    ):
        return "FAIL"
    semantic = payload.get("semantic_equivalence")
    scope = payload.get("benchmark_scope")
    sources = payload.get("source_evidence")
    if not all(isinstance(item, Mapping) for item in (semantic, scope, sources)):
        return "UNVERIFIED"
    if (
        semantic.get("status") != "PASS"
        or semantic.get("duplicate_natural_keys") != 0
        or not isinstance(semantic.get("row_count"), int)
        or int(semantic["row_count"]) <= 0
        or not _is_sha256(semantic.get("canonical_semantic_sha256"))
        or scope.get("month") != "2021-01"
        or not isinstance(scope.get("raw_archives"), int)
        or int(scope["raw_archives"]) <= 0
    ):
        return "FAIL"
    for label in ("legacy", "streaming", "comparison"):
        path = Path(str(sources.get(f"{label}_json", "")))
        expected = sources.get(f"{label}_sha256")
        if not path.is_file() or not _is_sha256(expected):
            return "FAIL"
        if _sha256_file(path) != expected:
            return "FAIL"
    commit = str(payload.get("repository_commit", ""))
    return (
        "PASS"
        if len(commit) == 40 and all(c in "0123456789abcdef" for c in commit)
        else "FAIL"
    )


def _pilot_evidence_acceptance(
    payload: Mapping[str, Any], expected_run_root: Path
) -> str:
    if (
        payload.get("schema_version") != PILOT_SCHEMA_VERSION
        or payload.get("overall_status") != "PASS"
    ):
        return "FAIL"
    pilots = payload.get("pilots")
    if not isinstance(pilots, list) or not all(
        isinstance(pilot, Mapping) for pilot in pilots
    ):
        return "UNVERIFIED"
    ids = [str(pilot.get("pilot_id", "")) for pilot in pilots]
    if (
        payload.get("pilot_count") != len(REQUIRED_PILOT_IDS)
        or len(ids) != len(REQUIRED_PILOT_IDS)
        or len(set(ids)) != len(ids)
        or set(ids) != REQUIRED_PILOT_IDS
        or any(pilot.get("status") != "PASS" for pilot in pilots)
    ):
        return "FAIL"
    root = Path(str(payload.get("run_root", ""))).resolve()
    if root != expected_run_root.resolve() or not root.is_dir():
        return "FAIL"
    for pilot in pilots:
        artifacts = pilot.get("artifacts")
        if not isinstance(artifacts, Mapping) or not artifacts:
            return "UNVERIFIED"
        for evidence in artifacts.values():
            if not isinstance(evidence, Mapping) or evidence.get("exists") is not True:
                return "FAIL"
            path = Path(str(evidence.get("path", "")))
            path = path if path.is_absolute() else root / path
            if (
                not path.is_file()
                or evidence.get("size_bytes") != path.stat().st_size
                or not _is_sha256(evidence.get("sha256"))
                or _sha256_file(path) != evidence.get("sha256")
            ):
                return "FAIL"
    try:
        recomputed = inspect_required_span_pilots(expected_run_root)
    except Exception:  # noqa: BLE001 - unreadable pilot evidence must fail closed.
        return "FAIL"
    return "PASS" if dict(payload) == recomputed else "FAIL"


def _is_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _acceptance_signals(
    value: Any, path: str = "root"
) -> tuple[list[str], list[str], list[str]]:
    positive: list[str] = []
    negative: list[str] = []
    unknown: list[str] = []
    accepted = {
        "PASS",
        "PASSED",
        "PASS_READY",
        "PASS_BINDING_LIMIT",
        "READY",
        "SUCCESS",
    }
    rejected = {
        "BLOCKED",
        "BLOCKED_SOURCE",
        "FAIL",
        "FAILED",
        "FAIL_INCOMPLETE",
        "NOT_READY",
        "WAITING",
    }
    status_fields = {"gate", "outcome", "overall_outcome", "overall_status", "status"}
    boolean_fields = {"ok", "passed"}

    def visit(item: Any, location: str) -> None:
        if isinstance(item, Mapping):
            for key, child in item.items():
                key_text = str(key)
                child_path = f"{location}.{key_text}"
                if key_text in status_fields:
                    normalized = str(child or "").strip().upper()
                    if normalized in accepted:
                        positive.append(child_path)
                    elif (
                        normalized in rejected
                        or normalized.startswith("FAIL")
                        or normalized.startswith("BLOCKED")
                    ):
                        negative.append(child_path)
                    elif normalized:
                        unknown.append(child_path)
                elif key_text in boolean_fields:
                    if child is True:
                        positive.append(child_path)
                    elif child is False:
                        negative.append(child_path)
                    elif child is not None:
                        unknown.append(child_path)
                elif key_text == "status_counts" and isinstance(child, Mapping):
                    for status, count in child.items():
                        try:
                            parsed = int(count)
                        except (TypeError, ValueError):
                            unknown.append(f"{child_path}.{status}")
                            continue
                        normalized_status = str(status).upper()
                        if parsed < 0:
                            unknown.append(f"{child_path}.{status}")
                        elif parsed > 0 and (
                            normalized_status in rejected
                            or normalized_status.startswith("FAIL")
                            or normalized_status.startswith("BLOCKED")
                        ):
                            negative.append(f"{child_path}.{status}")
                        elif parsed > 0 and normalized_status in accepted:
                            positive.append(f"{child_path}.{status}")
                visit(child, child_path)
        elif isinstance(item, list):
            for index, child in enumerate(item):
                visit(child, f"{location}[{index}]")

    visit(value, path)
    return positive, negative, unknown


def _all_nonempty(fingerprints: Iterable[FileFingerprint]) -> bool:
    return all(item.exists and int(item.size_bytes or 0) > 0 for item in fingerprints)


def _fingerprint(path: Path) -> FileFingerprint:
    resolved = path.resolve()
    if not resolved.is_file():
        return FileFingerprint(str(resolved), False, None, None)
    digest = sha256()
    size = 0
    with resolved.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            size += len(block)
            digest.update(block)
    return FileFingerprint(str(resolved), True, size, digest.hexdigest())


def _object_mapping(value: Any) -> dict[str, Any]:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return dict(vars(value))


def _dataclass_mapping(values: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: asdict(value)
        if is_dataclass(value) and not isinstance(value, type)
        else value
        for key, value in sorted(values.items())
    }


def _markdown(payload: Mapping[str, Any]) -> str:
    audit = payload["audit"]
    contract = payload["pinned_contract"]
    lines = [
        "# SPAN Phase 1 Final Completion",
        "",
        f"- Outcome: **{payload['outcome']}**",
        f"- Requested range: `{payload['requested_range']['start_date']}` through "
        f"`{payload['requested_range']['end_date']}`",
        f"- Pinned range matched: `{'yes' if contract['range_matches'] else 'no'}`",
        f"- Source manifests stable: `{'yes' if payload['source_stability']['stable'] else 'no'}`",
        f"- Benchmark and pilot evidence complete: `{'yes' if payload['evidence_complete'] else 'no'}`",
        "",
        "## Exact matrix outcome",
        "",
        "| Measure | Value | Required |",
        "|---|---:|---:|",
        f"| Dates | {audit['requested_dates']:,} | {contract['dates']:,} |",
        f"| Expected cells | {audit['expected_cells']:,} | {contract['cells']:,} |",
        f"| Accounted cells | {audit['accounted_cells']:,} | {contract['cells']:,} |",
        f"| Terminal cells | {audit['terminal_cells']:,} | {contract['cells']:,} |",
        f"| Failed/incomplete | {audit['failed_or_incomplete_cells']:,} | 0 |",
        f"| Unresolved missing | {audit['unresolved_missing_cells']:,} | 0 |",
        "",
        f"Earliest/latest proven downloads: `{audit['earliest_proven_download_date'] or ''}` / "
        f"`{audit['latest_proven_download_date'] or ''}`.",
        "",
        "## Gates",
        "",
        "| Gate | Result |",
        "|---|---|",
    ]
    for name, passed in payload["matrix_checks"].items():
        lines.append(f"| {name} | {'PASS' if passed else 'FAIL'} |")
    for name, passed in payload["artifact_checks"].items():
        lines.append(f"| {name} | {'PASS' if passed else 'FAIL'} |")
    lines.extend(["", "## Slot/year coverage", ""])
    if audit["slot_year_counts"]:
        lines.extend(
            [
                "| Year | Slot | Cells | Terminal | Downloaded | Accepted unavailable | "
                "Unresolved | Failed | Extracted |",
                "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for item in audit["slot_year_counts"]:
            lines.append(
                f"| {item['year']} | {item['slot']} | {item['total_cells']} | "
                f"{item['terminal_cells']} | {item['downloaded_valid_cells']} | "
                f"{item['accepted_unavailable_cells']} | {item['unresolved_missing_cells']} | "
                f"{item['nonterminal_or_failed_cells']} | {item['extracted_valid_cells']} |"
            )
    else:
        lines.append("No slot/year rows were emitted by the audit.")
    lines.extend(
        [
            "",
            "## Source manifest stability",
            "",
            "| Manifest | Before bytes | Before SHA-256 | After bytes | After SHA-256 |",
            "|---|---:|---|---:|---|",
        ]
    )
    for name, before in payload["source_stability"]["before"].items():
        after = payload["source_stability"]["after"][name]
        lines.append(
            f"| {name} | {before['size_bytes'] or 0} | `{before['sha256'] or ''}` | "
            f"{after['size_bytes'] or 0} | `{after['sha256'] or ''}` |"
        )
    for category in ("benchmarks", "pilots", "recovery"):
        lines.extend(
            [
                "",
                f"## {category.title()} evidence",
                "",
                "| Path | Acceptance | SHA-256 | Bytes |",
                "|---|---|---|---:|",
            ]
        )
        for item in payload["artifacts"][category]:
            path = item.get("path") or item.get("artifact") or ""
            digest = item.get("sha256") or item.get("artifact_sha256") or ""
            size = item.get("size_bytes") or item.get("artifact_size_bytes") or 0
            lines.append(f"| `{path}` | {item['acceptance']} | `{digest}` | {size} |")
    lines.extend(["", "## Artifact fingerprints", ""])
    for category in ("exports", "audit"):
        lines.extend(
            [
                f"### {category.title()}",
                "",
                "| Artifact | Path | SHA-256 | Bytes |",
                "|---|---|---|---:|",
            ]
        )
        for name, item in payload["artifacts"][category].items():
            lines.append(
                f"| {name} | `{item['path']}` | `{item['sha256'] or ''}` | "
                f"{item['size_bytes'] or 0} |"
            )
        lines.append("")
    metadata = payload["metadata"]
    lines.extend(
        [
            "## Reproduction metadata",
            "",
            f"- Commit: `{metadata['commit_sha'] or ''}`",
            f"- Tests: `{metadata['test_result'] or ''}`",
            f"- Tool versions: `{json.dumps(metadata['tool_versions'], sort_keys=True)}`",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
