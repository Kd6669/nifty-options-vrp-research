"""Independent hash/metadata verifier for final BOD and six-slot SPAN releases."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .span_gold import _atomic_json, _atomic_text, _json_sha, _read_json, sha256_file
from .span_release import (
    BOD_RELEASE_VERSION,
    EXPECTED_BOD_MATCHED,
    EXPECTED_BOD_UNMATCHED,
    EXPECTED_BSM_STATUS,
    EXPECTED_ROWS,
    SIX_SLOT_RELEASE_VERSION,
)


@dataclass(frozen=True)
class SpanReleaseVerifyStats:
    status: str
    months: int
    rows: int
    bod_matched_rows: int
    bod_unmatched_rows: int
    bod_root: str
    six_slot_root: str
    audit_path: str
    audit_sha256: str


def verify_span_release(
    *,
    bod_root: str | Path,
    six_slot_root: str | Path,
    expected_months: int = 67,
    expected_rows: int = EXPECTED_ROWS,
) -> SpanReleaseVerifyStats:
    """Re-hash every month manifest/output/exception and verify Parquet metadata."""
    bod = Path(bod_root).resolve()
    six = Path(six_slot_root).resolve()
    if bod.name != f"version={BOD_RELEASE_VERSION}":
        raise ValueError("unexpected BOD release version")
    if six.name != f"version={SIX_SLOT_RELEASE_VERSION}":
        raise ValueError("unexpected six-slot release version")
    bod_terminal_path = bod / "manifests" / "span_release_terminal_audit.json"
    six_terminal_path = six / "manifests" / "span_release_terminal_audit.json"
    bod_terminal = _read_json(bod_terminal_path)
    six_terminal = _read_json(six_terminal_path)
    errors: list[str] = []
    if bod_terminal != six_terminal:
        errors.append("terminal_audits_disagree")
    terminal = bod_terminal
    if terminal.get("status") != ("PASS" if expected_months == 67 else "PILOT_PASS"):
        errors.append("producer_terminal_status")
    if int(terminal.get("months", -1)) != expected_months:
        errors.append("producer_terminal_months")
    if int(terminal.get("input_rows", -1)) != expected_rows:
        errors.append("producer_terminal_rows")

    bod_audit = _verify_root(
        root=bod,
        representation="BOD",
        terminal_months=terminal.get("bod", {}).get("month_manifests", []),
        expected_months=expected_months,
        expected_rows=expected_rows,
    )
    six_audit = _verify_root(
        root=six,
        representation="SIX_SLOT_WIDE",
        terminal_months=terminal.get("six_slot", {}).get("month_manifests", []),
        expected_months=expected_months,
        expected_rows=expected_rows,
    )
    for label, item in (("bod", bod_audit), ("six_slot", six_audit)):
        if item["errors"]:
            errors.extend(f"{label}_{error}" for error in item["errors"])
        if item["bsm_status_counts"] != terminal.get("bsm_status_counts_expected"):
            errors.append(f"{label}_bsm_status_terminal_mismatch")
    bod_status = bod_audit["slot_status_counts"].get("span_join_status", {})
    matched = int(bod_status.get("matched", 0))
    unmatched = expected_rows - matched
    if expected_months == 67:
        if matched != EXPECTED_BOD_MATCHED:
            errors.append("bod_matched_baseline")
        if unmatched != EXPECTED_BOD_UNMATCHED:
            errors.append("bod_unmatched_baseline")
        if bod_audit["bsm_status_counts"] != EXPECTED_BSM_STATUS:
            errors.append("bsm_status_contract")
    status = "FAIL" if errors else ("PASS" if expected_months == 67 else "PILOT_PASS")
    audit = {
        "schema": "dhan_span_release_independent_integrity_audit",
        "schema_version": SIX_SLOT_RELEASE_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "expected_months": expected_months,
        "expected_rows": expected_rows,
        "errors": errors,
        "producer_terminal_audit": {
            "path": str(bod_terminal_path),
            "sha256": sha256_file(bod_terminal_path),
        },
        "producer_terminal_copy": {
            "path": str(six_terminal_path),
            "sha256": sha256_file(six_terminal_path),
        },
        "bod": {**bod_audit, "matched_rows": matched, "unmatched_rows": unmatched},
        "six_slot": six_audit,
    }
    markdown = _markdown(audit)
    for root in (bod, six):
        _atomic_json(root / "manifests" / "span_release_integrity_audit.json", audit)
        _atomic_text(root / "manifests" / "span_release_integrity_audit.md", markdown)
    audit_path = bod / "manifests" / "span_release_integrity_audit.json"
    if status == "FAIL":
        raise RuntimeError("SPAN release integrity audit failed: " + ", ".join(errors))
    return SpanReleaseVerifyStats(
        status=status,
        months=expected_months,
        rows=expected_rows,
        bod_matched_rows=matched,
        bod_unmatched_rows=unmatched,
        bod_root=str(bod),
        six_slot_root=str(six),
        audit_path=str(audit_path),
        audit_sha256=sha256_file(audit_path),
    )


def _verify_root(
    *,
    root: Path,
    representation: str,
    terminal_months: list[dict[str, Any]],
    expected_months: int,
    expected_rows: int,
) -> dict[str, Any]:
    import pyarrow.parquet as pq

    errors: list[str] = []
    manifests = sorted((root / "manifests" / "months").glob("month=*.json"))
    if len(manifests) != expected_months:
        errors.append("manifest_count")
    terminal_index = {item["month"]: item for item in terminal_months}
    if len(terminal_index) != expected_months:
        errors.append("terminal_manifest_inventory")
    rows = output_bytes = exception_rows = exception_bytes = 0
    bsm_counts: Counter[str] = Counter()
    slot_counts: dict[str, Counter[str]] = {}
    verified: list[dict[str, Any]] = []
    for manifest_path in manifests:
        payload = _read_json(manifest_path)
        month = manifest_path.stem.removeprefix("month=")
        if (
            payload.get("month") != month
            or payload.get("representation") != representation
        ):
            errors.append(f"manifest_identity_{month}")
            continue
        if payload.get("lineage_sha256") != _json_sha(payload.get("lineage")):
            errors.append(f"lineage_hash_{month}")
        if payload.get("input_rows") != payload.get("output_rows"):
            errors.append(f"row_conservation_{month}")
        if any(
            int(payload.get(key, -1))
            for key in (
                "duplicate_span_keys",
                "duplicate_gap_keys",
                "primary_key_duplicate_rows",
                "cross_key_violation_rows",
            )
        ):
            errors.append(f"join_invariant_{month}")
        output = Path(payload["output_path"]).resolve()
        exception = Path(payload["exception_path"]).resolve()
        if not output.is_file() or sha256_file(output) != payload.get("output_sha256"):
            errors.append(f"output_hash_{month}")
        if not exception.is_file() or sha256_file(exception) != payload.get(
            "exception_sha256"
        ):
            errors.append(f"exception_hash_{month}")
        if output.is_file() and pq.ParquetFile(output).metadata.num_rows != int(
            payload["output_rows"]
        ):
            errors.append(f"output_metadata_{month}")
        if exception.is_file() and pq.ParquetFile(exception).metadata.num_rows != int(
            payload["exception_rows"]
        ):
            errors.append(f"exception_metadata_{month}")
        terminal_item = terminal_index.get(month)
        if terminal_item is None:
            errors.append(f"terminal_month_missing_{month}")
        else:
            if sha256_file(manifest_path) != terminal_item.get("manifest_sha256"):
                errors.append(f"manifest_hash_{month}")
            if payload.get("output_sha256") != terminal_item.get("output_sha256"):
                errors.append(f"terminal_output_hash_{month}")
            if payload.get("exception_sha256") != terminal_item.get("exception_sha256"):
                errors.append(f"terminal_exception_hash_{month}")
        rows += int(payload["output_rows"])
        output_bytes += int(payload["output_bytes"])
        exception_rows += int(payload["exception_rows"])
        exception_bytes += int(payload["exception_bytes"])
        bsm_counts.update(payload["bsm_status_counts"])
        for column, counts in payload["slot_status_counts"].items():
            slot_counts.setdefault(column, Counter()).update(counts)
        verified.append(
            {
                "month": month,
                "manifest_path": str(manifest_path),
                "manifest_sha256": sha256_file(manifest_path),
                "output_path": str(output),
                "output_sha256": payload.get("output_sha256"),
                "output_rows": payload.get("output_rows"),
            }
        )
    if rows != expected_rows:
        errors.append("aggregate_rows")
    partials = [str(path) for path in root.rglob("*.partial")]
    if partials:
        errors.append("orphan_partials")
    return {
        "representation": representation,
        "months": len(manifests),
        "rows": rows,
        "output_bytes": output_bytes,
        "exception_rows": exception_rows,
        "exception_bytes": exception_bytes,
        "bsm_status_counts": dict(bsm_counts),
        "slot_status_counts": {
            column: dict(counts) for column, counts in slot_counts.items()
        },
        "verified_months": verified,
        "orphan_partial_paths": partials,
        "errors": errors,
    }


def _markdown(audit: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Independent SPAN release integrity audit",
            "",
            f"- Status: **{audit['status']}**",
            f"- Months: {audit['bod']['months']}/{audit['expected_months']}",
            f"- Rows: {audit['bod']['rows']:,}",
            f"- BOD matched: {audit['bod']['matched_rows']:,}",
            f"- BOD unmatched: {audit['bod']['unmatched_rows']:,}",
            f"- BOD output bytes: {audit['bod']['output_bytes']:,}",
            f"- Six-slot output bytes: {audit['six_slot']['output_bytes']:,}",
            f"- Errors: {audit['errors']}",
            "",
            "Every month manifest, output Parquet, exception Parquet, manifest-listed SHA-256, "
            "and Parquet metadata row count was independently re-read.",
            "",
        ]
    )
