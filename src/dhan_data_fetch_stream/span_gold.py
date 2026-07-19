"""Immutable, month-resumable BOD SPAN enrichment for audited Dhan BSM output."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Sequence
import uuid


SPAN_GOLD_VERSION = "1.3.0"
SPAN_JOIN_POLICY = "BOD_CONSERVATIVE_UNKNOWN_EFFECTIVE_TIME"
_MONTH = re.compile(r"^\d{4}-\d{2}$")
_JOIN_KEY = ("date", "instrument", "expiry", "strike")


@dataclass(frozen=True)
class SpanGoldConfig:
    threads: int = 8
    memory_limit: str = "8GB"
    row_group_size: int = 250_000


@dataclass(frozen=True)
class SpanGoldRunStats:
    months_total: int
    months_processed: int
    months_resumed: int
    rows_total: int
    matched_rows: int
    unmatched_rows: int
    output_root: str
    terminal_audit_path: str
    terminal_status: str
    elapsed_seconds: float


def run_span_gold(
    *,
    bsm_root: str | Path,
    bsm_terminal_audit: str | Path,
    span_compacted_root: str | Path,
    span_completion: str | Path,
    span_matrix: str | Path,
    output_root: str | Path,
    months: Sequence[str] | None = None,
    config: SpanGoldConfig | None = None,
    resume: bool = True,
) -> SpanGoldRunStats:
    """Join audited BSM rows to same-date NIFTY BOD SPAN without guessing times."""
    cfg = config or SpanGoldConfig()
    _validate_config(cfg)
    bsm_root = Path(bsm_root).resolve()
    span_root = Path(span_compacted_root).resolve()
    bsm_audit_path = Path(bsm_terminal_audit).resolve()
    span_completion_path = Path(span_completion).resolve()
    matrix_path = Path(span_matrix).resolve()
    root = Path(output_root).resolve() / f"version={SPAN_GOLD_VERSION}"

    bsm_audit = _validate_bsm_audit(bsm_root, bsm_audit_path)
    span_acceptance = _validate_span_acceptance(span_completion_path, matrix_path)
    discovered = _discover_months(bsm_root, span_root)
    bsm_months = {
        str(item["month"])
        for item in (
            bsm_audit.get("months_audited") or bsm_audit.get("month_audits") or []
        )
    }
    producer_months = set(span_acceptance["months"])
    if set(discovered) != bsm_months or set(discovered) != producer_months:
        raise ValueError(
            "BSM/SPAN discovered month universe does not match terminal producers"
        )
    selected = sorted(discovered) if months is None else list(dict.fromkeys(months))
    invalid = [month for month in selected if not _MONTH.fullmatch(month)]
    missing = [month for month in selected if month not in discovered]
    if invalid:
        raise ValueError(f"invalid month selection: {invalid}")
    if missing:
        raise ValueError(
            f"selected months are absent from BSM or SPAN input: {missing}"
        )
    if not selected:
        raise ValueError("no common BSM/SPAN monthly inputs found")

    started = time.monotonic()
    processed = resumed_count = rows = matched = unmatched = 0
    month_results: list[dict[str, Any]] = []
    for month in selected:
        result = _run_month(
            month=month,
            bsm_path=discovered[month][0],
            span_path=discovered[month][1],
            root=root,
            bsm_audit_path=bsm_audit_path,
            bsm_audit=bsm_audit,
            span_completion_path=span_completion_path,
            span_acceptance=span_acceptance,
            matrix_path=matrix_path,
            config=cfg,
            resume=resume,
        )
        month_results.append(result)
        processed += int(not result["resumed"])
        resumed_count += int(result["resumed"])
        rows += int(result["output_rows"])
        matched += int(result["matched_rows"])
        unmatched += int(result["unmatched_rows"])
        _publish_status(root, selected, month_results, state="running")

    full_scope = selected == sorted(discovered)
    expected_rows = int(bsm_audit["output_rows"])
    errors: list[str] = []
    if full_scope and rows != expected_rows:
        errors.append(f"terminal_row_count_mismatch:{rows}!={expected_rows}")
    if rows != matched + unmatched:
        errors.append("terminal_match_accounting_mismatch")
    if any(int(item["duplicate_span_keys"]) for item in month_results):
        errors.append("duplicate_span_keys")
    if any(not item["row_conservation"] for item in month_results):
        errors.append("row_conservation_failed")
    if any(int(item["bod_policy_violation_rows"]) for item in month_results):
        errors.append("bod_policy_violation")
    orphan_partials = sorted(root.rglob("*.partial"))
    if orphan_partials:
        errors.append("orphan_partials_present")
    status = (
        "PASS"
        if full_scope and not errors
        else ("PILOT_PASS" if not errors else "FAIL")
    )
    audit = {
        "schema": "dhan_span_gold_terminal_audit",
        "schema_version": SPAN_GOLD_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "full_scope": full_scope,
        "join_policy": SPAN_JOIN_POLICY,
        "span_enriched": True,
        "span_primary_slot": "BOD",
        "span_intraday_sensitivity_joined": False,
        "span_phase1_outcome": span_acceptance["outcome"],
        "span_source_boundary_cells": span_acceptance["source_boundary_cells"],
        "months": len(selected),
        "expected_months": len(discovered),
        "input_rows": rows,
        "output_rows": rows,
        "matched_rows": matched,
        "unmatched_rows": unmatched,
        "match_rate": (matched / rows) if rows else None,
        "duplicate_span_keys": sum(
            int(item["duplicate_span_keys"]) for item in month_results
        ),
        "effective_time_proven": False,
        "lookahead_policy_status": "CONSERVATIVE_BOD_FALLBACK_EFFECTIVE_TIME_UNPROVEN",
        "bod_policy_violation_rows": sum(
            int(item["bod_policy_violation_rows"]) for item in month_results
        ),
        "orphan_partial_paths": [str(path) for path in orphan_partials],
        "errors": errors,
        "bsm_terminal_audit": _lineage(bsm_audit_path),
        "span_completion": _lineage(span_completion_path),
        "span_matrix": _lineage(matrix_path),
        "span_summary": span_acceptance["summary_lineage"],
        "month_manifests": [
            {
                "month": item["month"],
                "manifest_path": item["manifest_path"],
                "manifest_sha256": item["manifest_sha256"],
                "output_sha256": item["output_sha256"],
            }
            for item in month_results
        ],
        "historical_minute_futures_status": "BLOCKED_SOURCE_DHAN_EXPIRED_MINUTE_EMPTY",
    }
    audit_path = root / "manifests" / "span_gold_terminal_audit.json"
    audit_md_path = root / "manifests" / "span_gold_terminal_audit.md"
    _atomic_json(audit_path, audit)
    _atomic_text(audit_md_path, _terminal_markdown(audit))
    _publish_status(
        root, selected, month_results, state="complete", terminal_status=status
    )
    if errors:
        raise RuntimeError("SPAN gold terminal audit failed: " + ", ".join(errors))
    return SpanGoldRunStats(
        months_total=len(selected),
        months_processed=processed,
        months_resumed=resumed_count,
        rows_total=rows,
        matched_rows=matched,
        unmatched_rows=unmatched,
        output_root=str(root),
        terminal_audit_path=str(audit_path),
        terminal_status=status,
        elapsed_seconds=time.monotonic() - started,
    )


def _run_month(
    *,
    month: str,
    bsm_path: Path,
    span_path: Path,
    root: Path,
    bsm_audit_path: Path,
    bsm_audit: dict[str, Any],
    span_completion_path: Path,
    span_acceptance: dict[str, Any],
    matrix_path: Path,
    config: SpanGoldConfig,
    resume: bool,
) -> dict[str, Any]:
    year, month_number = month.split("-")
    output_path = (
        root / "gold" / f"year={year}" / f"month={month_number}" / "part-000.parquet"
    )
    exception_path = (
        root
        / "exceptions"
        / "unmatched_span"
        / f"year={year}"
        / f"month={month_number}"
        / "part-000.parquet"
    )
    manifest_path = root / "manifests" / "months" / f"month={month}.json"
    lineage = {
        "month": month,
        "version": SPAN_GOLD_VERSION,
        "join_policy": SPAN_JOIN_POLICY,
        "config": {
            "threads": config.threads,
            "memory_limit": config.memory_limit,
            "row_group_size": config.row_group_size,
        },
        "code_sha256": sha256_file(Path(__file__)),
        "bsm_input": _lineage(bsm_path),
        "span_input": _lineage(span_path),
        "bsm_terminal_audit": _lineage(bsm_audit_path),
        "span_completion": _lineage(span_completion_path),
        "span_matrix": _lineage(matrix_path),
        "span_summary": span_acceptance["summary_lineage"],
        "span_phase1_outcome": span_acceptance["outcome"],
    }
    expected_month = _bsm_month_audit(bsm_audit, month)
    if expected_month is None:
        raise ValueError(f"BSM terminal month audit missing: {month}")
    if lineage["bsm_input"]["sha256"] != expected_month.get("output_sha256"):
        raise ValueError(f"BSM terminal/month hash mismatch for {month}")
    lineage_sha = _json_sha(lineage)
    import duckdb
    import pyarrow.parquet as pq

    connection = duckdb.connect()
    try:
        connection.execute(f"SET threads={int(config.threads)}")
        connection.execute(f"SET memory_limit='{_sql_text(config.memory_limit)}'")
        connection.execute(
            "CREATE TEMP VIEW bsm AS SELECT * FROM read_parquet("
            f"{_sql_literal(str(bsm_path))}, hive_partitioning=false)"
        )
        connection.execute(
            "CREATE TEMP VIEW span_all AS SELECT * FROM read_parquet("
            f"{_sql_literal(str(span_path))}, hive_partitioning=false)"
        )
        span_columns = [
            row[0] for row in connection.execute("DESCRIBE span_all").fetchall()
        ]
        required = {
            "date",
            "time_slot",
            "symbol",
            "instrument",
            "expiry",
            "strike",
            "span_effective_ts_ist",
            "effective_time_source",
        }
        missing = sorted(required.difference(span_columns))
        if missing:
            raise ValueError(f"SPAN month {month} missing columns: {missing}")
        invalid_effective = int(
            connection.execute(
                """SELECT count(*) FROM span_all WHERE time_slot='BOD' AND symbol='NIFTY'
                   AND instrument IN ('CE','PE')
                   AND (span_effective_ts_ist IS NOT NULL OR effective_time_source <> 'unknown')"""
            ).fetchone()[0]
        )
        if invalid_effective:
            raise ValueError(
                f"BOD-only fallback requires unknown/null effective time; {month} has {invalid_effective} invalid rows"
            )
        connection.execute(
            """CREATE TEMP VIEW span_bod AS SELECT * FROM span_all
               WHERE time_slot='BOD' AND symbol='NIFTY' AND instrument IN ('CE','PE')"""
        )
        duplicate_span_keys = int(
            connection.execute(
                """SELECT coalesce(sum(n - 1),0) FROM (
                     SELECT count(*) n FROM span_bod GROUP BY date,instrument,expiry,strike HAVING n>1)"""
            ).fetchone()[0]
        )
        if duplicate_span_keys:
            raise ValueError(
                f"duplicate BOD SPAN keys for {month}: {duplicate_span_keys}"
            )
        expected_span = span_acceptance["months"][month]
        if span_path != Path(expected_span["path"]).resolve():
            raise ValueError(f"SPAN compacted path is not producer-bound for {month}")
        if sha256_file(span_path) != expected_span["sha256"]:
            raise ValueError(f"SPAN compacted hash mismatch for {month}")
        span_rows = int(
            connection.execute("SELECT count(*) FROM span_all").fetchone()[0]
        )
        if span_rows != int(expected_span["row_count"]):
            raise ValueError(f"SPAN compacted row-count mismatch for {month}")
        input_rows = int(connection.execute("SELECT count(*) FROM bsm").fetchone()[0])
        if int(expected_month["rows"]) != input_rows:
            raise ValueError(f"BSM terminal/month row mismatch for {month}")
        if resume:
            resumed = _resume_manifest(
                manifest_path, output_path, exception_path, lineage_sha
            )
            if resumed is not None:
                return resumed

        prefixed = ",\n".join(f's."{name}" AS "span_{name}"' for name in span_columns)
        phase1_outcome = _sql_literal(span_acceptance["outcome"])
        completion_sha = _sql_literal(sha256_file(span_completion_path))
        lineage_sha_literal = _sql_literal(lineage_sha)
        source_boundary_cells = int(span_acceptance["source_boundary_cells"])
        select_sql = f"""
            SELECT b.*,
              '{SPAN_JOIN_POLICY}' AS span_join_policy,
              CASE WHEN s.date IS NULL THEN 'unmatched' ELSE 'matched' END AS span_enrichment_status,
              CASE WHEN s.date IS NULL THEN 'contract_not_in_bod_span' ELSE NULL END AS span_unmatched_reason,
              {phase1_outcome} AS span_phase1_outcome,
              {source_boundary_cells}::BIGINT AS span_phase1_source_boundary_cells,
              {completion_sha} AS span_phase1_completion_sha256,
              {lineage_sha_literal} AS span_gold_lineage_sha256,
              {prefixed}
            FROM bsm b
            LEFT JOIN span_bod s
              ON b.trade_date=s.date
             AND b.actual_expiry_date=s.expiry
             AND CAST(b.strike AS DOUBLE)=s.strike
             AND s.instrument=CASE b.option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END
        """
        matched_rows = int(
            connection.execute(
                """SELECT count(*) FROM bsm b JOIN span_bod s
                     ON b.trade_date=s.date AND b.actual_expiry_date=s.expiry
                    AND CAST(b.strike AS DOUBLE)=s.strike
                    AND s.instrument=CASE b.option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END"""
            ).fetchone()[0]
        )
        unmatched_rows = input_rows - matched_rows
        adopted = _adopt_or_quarantine_publication(
            connection=connection,
            month=month,
            output_path=output_path,
            exception_path=exception_path,
            manifest_path=manifest_path,
            lineage=lineage,
            lineage_sha=lineage_sha,
            input_rows=input_rows,
            matched_rows=matched_rows,
            unmatched_rows=unmatched_rows,
            duplicate_span_keys=duplicate_span_keys,
        )
        if adopted is not None:
            return adopted
        output_path.parent.mkdir(parents=True, exist_ok=True)
        exception_path.parent.mkdir(parents=True, exist_ok=True)
        output_partial = output_path.with_name(
            f".{output_path.name}.{uuid.uuid4().hex}.partial"
        )
        exception_partial = exception_path.with_name(
            f".{exception_path.name}.{uuid.uuid4().hex}.partial"
        )
        try:
            _copy_parquet(connection, select_sql, output_partial, config.row_group_size)
            exception_sql = f"""SELECT timestamp_ist,trade_date,underlying,actual_expiry_date,
                expiry_flag,expiry_code,moneyness_label,strike,option_type,request_id,
                'contract_not_in_bod_span' AS span_unmatched_reason
                FROM ({select_sql}) q WHERE span_enrichment_status='unmatched'"""
            _copy_parquet(
                connection, exception_sql, exception_partial, config.row_group_size
            )
            _fsync_file(output_partial)
            _fsync_file(exception_partial)
            if pq.ParquetFile(output_partial).metadata.num_rows != input_rows:
                raise RuntimeError(
                    f"row conservation failed before publication for {month}"
                )
            if pq.ParquetFile(exception_partial).metadata.num_rows != unmatched_rows:
                raise RuntimeError(f"unmatched exception count failed for {month}")
            if sha256_file(bsm_path) != expected_month["output_sha256"]:
                raise RuntimeError(
                    f"BSM input changed during materialization for {month}"
                )
            if sha256_file(span_path) != expected_span["sha256"]:
                raise RuntimeError(
                    f"SPAN input changed during materialization for {month}"
                )
            os.replace(output_partial, output_path)
            os.replace(exception_partial, exception_path)
        finally:
            output_partial.unlink(missing_ok=True)
            exception_partial.unlink(missing_ok=True)
    finally:
        connection.close()

    output_sha = sha256_file(output_path)
    exception_sha = sha256_file(exception_path)
    manifest = {
        "schema": "dhan_span_gold_month_manifest",
        "schema_version": SPAN_GOLD_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "lineage": lineage,
        "lineage_sha256": lineage_sha,
        "join_policy": SPAN_JOIN_POLICY,
        "input_rows": input_rows,
        "output_rows": input_rows,
        "row_conservation": True,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "duplicate_span_keys": duplicate_span_keys,
        "effective_time_proven": False,
        "lookahead_policy_status": "CONSERVATIVE_BOD_FALLBACK_EFFECTIVE_TIME_UNPROVEN",
        "bod_policy_violation_rows": 0,
        "span_effective_time_contract": "unknown_bod_only_conservative_fallback",
        "span_intraday_sensitivity_joined": False,
        "output_path": str(output_path),
        "output_sha256": output_sha,
        "output_bytes": output_path.stat().st_size,
        "exception_path": str(exception_path),
        "exception_sha256": exception_sha,
        "exception_bytes": exception_path.stat().st_size,
        "resumed": False,
    }
    _atomic_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    manifest["manifest_sha256"] = sha256_file(manifest_path)
    return manifest


def _validate_bsm_audit(root: Path, audit_path: Path) -> dict[str, Any]:
    payload = _read_json(audit_path)
    if payload.get("status") != "PASS":
        raise ValueError("BSM terminal audit is not PASS")
    if payload.get("schema") != "dhan_bsm_v2_terminal_audit":
        raise ValueError("unexpected BSM terminal audit schema")
    if bool(payload.get("span_enriched")):
        raise ValueError("BSM input already claims SPAN enrichment")
    if int(payload.get("expected_rows", -1)) != int(payload.get("output_rows", -2)):
        raise ValueError("BSM terminal audit lacks row conservation")
    month_audits = payload.get("months_audited") or payload.get("month_audits") or []
    if len(month_audits) != int(payload.get("months", len(month_audits))):
        raise ValueError("BSM terminal month audit count mismatch")
    for item in month_audits:
        month = str(item["month"])
        year, number = month.split("-")
        path = root / f"year={year}" / f"month={number}" / "part-000.parquet"
        if not path.is_file() or sha256_file(path) != item["output_sha256"]:
            raise ValueError(f"BSM month hash mismatch: {month}")
    return payload


def _validate_span_acceptance(
    completion_path: Path, matrix_path: Path
) -> dict[str, Any]:
    payload = _read_json(completion_path)
    if payload.get("finalizer_schema_version") != "span-phase1-finalizer-v1":
        raise ValueError("unexpected SPAN finalizer schema")
    pinned = payload.get("pinned_contract") or {}
    expected_pinned = {
        "start_date": "2021-01-01",
        "end_date": "2026-07-15",
        "dates": 2022,
        "cells": 12132,
        "range_matches": True,
    }
    if any(pinned.get(key) != value for key, value in expected_pinned.items()):
        raise ValueError("SPAN pinned range/cell contract mismatch")
    outcome = str(payload.get("outcome", ""))
    required_blocked_checks = {
        "accounted_cells_exact",
        "blocked_matrix_complete",
        "compacted_unique",
        "downloaded_extraction_complete",
        "expected_cells_exact",
        "raw_integrity_ok",
        "requested_dates_exact",
        "source_boundary_cells_positive",
        "unresolved_missing_zero",
        "unresolved_non_boundary_zero",
    }
    if outcome == "BLOCKED_SOURCE":
        if not payload.get("blocked_matrix_ready"):
            raise ValueError("SPAN BLOCKED_SOURCE matrix is not ready")
        checks = payload.get("blocked_matrix_checks") or {}
        missing_checks = sorted(required_blocked_checks.difference(checks))
        if missing_checks:
            raise ValueError(f"SPAN blocked-matrix checks missing: {missing_checks}")
        failed = [key for key in required_blocked_checks if checks.get(key) is not True]
        if failed:
            raise ValueError(f"SPAN blocked-matrix checks failed: {failed}")
    elif outcome == "PASS_READY":
        required_ready_checks = {
            "accounted_cells_exact",
            "audit_ok",
            "compacted_unique",
            "downloaded_extraction_complete",
            "expected_cells_exact",
            "failed_or_incomplete_zero",
            "matrix_complete",
            "raw_integrity_ok",
            "requested_dates_exact",
            "terminal_cells_exact",
            "unresolved_missing_zero",
        }
        checks = payload.get("matrix_checks") or {}
        missing_checks = sorted(required_ready_checks.difference(checks))
        if missing_checks or any(
            checks.get(key) is not True for key in required_ready_checks
        ):
            raise ValueError("SPAN PASS_READY matrix checks are incomplete")
    else:
        raise ValueError(f"unsupported SPAN terminal outcome: {outcome}")
    if not (payload.get("source_stability") or {}).get("stable"):
        raise ValueError("SPAN source changed during finalization")
    artifact_checks = payload.get("artifact_checks") or {}
    required_artifact_checks = {
        "all_audit_artifacts_nonempty",
        "all_export_artifacts_nonempty",
        "availability_export_hash_matches_source",
        "download_json_hash_matches_export",
        "download_parquet_hash_matches_export",
        "extraction_json_hash_matches_export",
        "extraction_parquet_hash_matches_export",
    }
    if any(artifact_checks.get(key) is not True for key in required_artifact_checks):
        raise ValueError("SPAN artifact-integrity checks are incomplete")
    audit_artifacts = (payload.get("artifacts") or {}).get("audit") or {}
    matrix_artifact = audit_artifacts.get("matrix_parquet") or {}
    if Path(str(matrix_artifact.get("path", ""))).resolve() != matrix_path:
        raise ValueError("SPAN matrix path does not match terminal artifact")
    if not matrix_path.is_file() or sha256_file(matrix_path) != matrix_artifact.get(
        "sha256"
    ):
        raise ValueError("SPAN matrix hash mismatch")
    audit = payload.get("audit") or {}
    exact_audit = {
        "requested_dates": 2022,
        "expected_cells": 12132,
        "accounted_cells": 12132,
        "resolved_or_blocked_cells": 12132,
        "compacted_months": 67,
        "compacted_rows": 24870123,
        "earliest_proven_download_date": "2021-01-01",
        "latest_proven_download_date": "2026-07-15",
    }
    if any(audit.get(key) != value for key, value in exact_audit.items()):
        raise ValueError("SPAN terminal audit counts/range mismatch")
    if int(audit.get("unresolved_missing_cells", -1)) != 0:
        raise ValueError("SPAN unresolved missing cells remain")
    if int(audit.get("unresolved_non_boundary_cells", -1)) != 0:
        raise ValueError("SPAN unresolved non-boundary cells remain")
    terminal_cells = int(audit.get("terminal_cells", -1))
    source_boundary_cells = int(audit.get("source_boundary_cells", -1))
    if not 0 <= terminal_cells <= 12132 or not 0 <= source_boundary_cells <= 12132:
        raise ValueError("SPAN terminal/source-boundary counts are invalid")
    if outcome == "BLOCKED_SOURCE" and source_boundary_cells == 0:
        raise ValueError("SPAN BLOCKED_SOURCE outcome lacks source boundaries")
    if outcome == "PASS_READY" and (
        terminal_cells != 12132 or source_boundary_cells != 0
    ):
        raise ValueError(
            "SPAN PASS_READY outcome has non-terminal/source-boundary cells"
        )
    _validate_matrix_content(
        matrix_path,
        expected_source_boundaries=source_boundary_cells,
    )
    summary_artifact = audit_artifacts.get("summary_json") or {}
    summary_path = Path(str(summary_artifact.get("path", ""))).resolve()
    if not summary_path.is_file() or sha256_file(summary_path) != summary_artifact.get(
        "sha256"
    ):
        raise ValueError("SPAN producer summary hash mismatch")
    summary = _read_json(summary_path)
    summary_expected = {
        "start_date": "2021-01-01",
        "end_date": "2026-07-15",
        "requested_dates": 2022,
        "expected_cells": 12132,
        "accounted_cells": 12132,
        "resolved_or_blocked_cells": 12132,
        "terminal_cells": terminal_cells,
        "source_boundary_cells": source_boundary_cells,
        "compacted_months": 67,
        "compacted_rows": 24870123,
        "compacted_unique": True,
        "duplicate_natural_keys": 0,
        "raw_integrity_ok": True,
        "raw_integrity_failures": 0,
        "extraction_complete": True,
        "downloaded_without_valid_extraction": 0,
        "unresolved_missing_cells": 0,
        "unresolved_non_boundary_cells": 0,
        "outcome": outcome,
    }
    if any(summary.get(key) != value for key, value in summary_expected.items()):
        raise ValueError("SPAN producer summary contract mismatch")
    month_items = summary.get("months") or []
    if len(month_items) != 67:
        raise ValueError("SPAN producer summary must contain 67 compacted months")
    months: dict[str, dict[str, Any]] = {}
    for item in month_items:
        month = f"{int(item['year']):04d}-{int(item['month']):02d}"
        if (
            month in months
            or item.get("exists") is not True
            or item.get("issue") is not None
        ):
            raise ValueError(f"invalid SPAN producer month entry: {month}")
        digest = str(item.get("sha256", ""))
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError(f"invalid SPAN compacted hash for {month}")
        months[month] = {
            "path": str(Path(item["compacted_path"]).resolve()),
            "sha256": digest,
            "row_count": int(item["row_count"]),
        }
    expected_months = {
        f"{year:04d}-{month:02d}"
        for year in range(2021, 2027)
        for month in range(1, 13)
        if (year, month) <= (2026, 7)
    }
    if set(months) != expected_months:
        raise ValueError("SPAN producer compacted month range mismatch")
    if sum(item["row_count"] for item in months.values()) != 24870123:
        raise ValueError("SPAN producer compacted monthly row total mismatch")
    return {
        "outcome": outcome,
        "source_boundary_cells": source_boundary_cells,
        "summary_lineage": _lineage(summary_path),
        "months": months,
    }


def _validate_matrix_content(
    matrix_path: Path, *, expected_source_boundaries: int
) -> None:
    import pyarrow.parquet as pq

    table = pq.ParquetFile(matrix_path).read(
        columns=["trading_date", "slot", "accounted", "source_boundary_proven"]
    )
    if table.num_rows != 12132:
        raise ValueError("SPAN matrix must contain exactly 12,132 rows")
    rows = table.to_pylist()
    keys = {(row["trading_date"], row["slot"]) for row in rows}
    dates = {row["trading_date"] for row in rows}
    slots = {row["slot"] for row in rows}
    if len(keys) != 12132 or len(dates) != 2022:
        raise ValueError("SPAN matrix date/slot keys are incomplete or duplicated")
    start = date(2021, 1, 1)
    end = date(2026, 7, 15)
    expected_dates = {
        (start + timedelta(days=offset)).isoformat()
        for offset in range((end - start).days + 1)
    }
    try:
        parsed_dates = {date.fromisoformat(value).isoformat() for value in dates}
    except (TypeError, ValueError) as exc:
        raise ValueError("SPAN matrix contains invalid ISO trading dates") from exc
    if parsed_dates != expected_dates:
        raise ValueError("SPAN matrix exact daily date universe mismatch")
    if slots != {"BOD", "ID1", "ID2", "ID3", "ID4", "EOD"}:
        raise ValueError("SPAN matrix slot identities mismatch")
    if not all(row["accounted"] is True for row in rows):
        raise ValueError("SPAN matrix contains unaccounted cells")
    if (
        sum(row["source_boundary_proven"] is True for row in rows)
        != expected_source_boundaries
    ):
        raise ValueError("SPAN matrix source-boundary count mismatch")


def _discover_months(bsm_root: Path, span_root: Path) -> dict[str, tuple[Path, Path]]:
    result: dict[str, tuple[Path, Path]] = {}
    for bsm_path in sorted(bsm_root.glob("year=*/month=*/part-000.parquet")):
        year = bsm_path.parent.parent.name.removeprefix("year=")
        month_number = bsm_path.parent.name.removeprefix("month=")
        month = f"{year}-{month_number}"
        span_path = span_root / f"{year}_{month_number}.parquet"
        if span_path.is_file():
            result[month] = (bsm_path.resolve(), span_path.resolve())
    return result


def _resume_manifest(
    manifest_path: Path, output_path: Path, exception_path: Path, lineage_sha: str
) -> dict[str, Any] | None:
    if not manifest_path.is_file():
        return None
    payload = _read_json(manifest_path)
    if payload.get("lineage_sha256") != lineage_sha:
        return None
    if not output_path.is_file() or sha256_file(output_path) != payload.get(
        "output_sha256"
    ):
        return None
    if not exception_path.is_file() or sha256_file(exception_path) != payload.get(
        "exception_sha256"
    ):
        return None
    import pyarrow.parquet as pq

    if pq.ParquetFile(output_path).metadata.num_rows != int(payload["output_rows"]):
        return None
    if pq.ParquetFile(exception_path).metadata.num_rows != int(
        payload["unmatched_rows"]
    ):
        return None
    payload["resumed"] = True
    payload["manifest_path"] = str(manifest_path)
    payload["manifest_sha256"] = sha256_file(manifest_path)
    return payload


def _adopt_or_quarantine_publication(
    *,
    connection: Any,
    month: str,
    output_path: Path,
    exception_path: Path,
    manifest_path: Path,
    lineage: dict[str, Any],
    lineage_sha: str,
    input_rows: int,
    matched_rows: int,
    unmatched_rows: int,
    duplicate_span_keys: int,
) -> dict[str, Any] | None:
    """Adopt a fully validated crash pair; quarantine every other stale publication."""
    existing = [
        path for path in (output_path, exception_path, manifest_path) if path.exists()
    ]
    if not existing:
        return None
    if manifest_path.exists() or not (
        output_path.is_file() and exception_path.is_file()
    ):
        _quarantine_publication(month, output_path, exception_path, manifest_path)
        return None
    import pyarrow.parquet as pq

    try:
        if pq.ParquetFile(output_path).metadata.num_rows != input_rows:
            raise ValueError("crash-pair output row mismatch")
        if pq.ParquetFile(exception_path).metadata.num_rows != unmatched_rows:
            raise ValueError("crash-pair exception row mismatch")
        output_literal = _sql_literal(str(output_path))
        exception_literal = _sql_literal(str(exception_path))
        completion_sha = lineage["span_completion"]["sha256"]
        audit = connection.execute(
            f"""SELECT count(*) AS total,
                count(*) FILTER (WHERE span_enrichment_status='matched') AS matched,
                count(*) FILTER (WHERE span_enrichment_status='unmatched') AS unmatched,
                count(*) FILTER (WHERE span_join_policy<>'{SPAN_JOIN_POLICY}') AS policy_bad,
                count(*) FILTER (WHERE span_phase1_completion_sha256 IS DISTINCT FROM {_sql_literal(completion_sha)}) AS lineage_bad,
                count(*) FILTER (WHERE span_gold_lineage_sha256 IS DISTINCT FROM {_sql_literal(lineage_sha)}) AS gold_lineage_bad,
                count(*) FILTER (WHERE span_enrichment_status='matched' AND
                    (span_time_slot<>'BOD' OR span_effective_time_source<>'unknown'
                     OR span_span_effective_ts_ist IS NOT NULL
                     OR trade_date<>span_date OR actual_expiry_date<>span_expiry
                     OR CAST(strike AS DOUBLE)<>span_strike
                     OR CASE option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END<>span_instrument)) AS join_bad
                FROM read_parquet({output_literal}, hive_partitioning=false)"""
        ).fetchone()
        if tuple(int(value) for value in audit) != (
            input_rows,
            matched_rows,
            unmatched_rows,
            0,
            0,
            0,
            0,
        ):
            raise ValueError("crash-pair content audit mismatch")
        key_columns = (
            "timestamp_ist,trade_date,underlying,actual_expiry_date,expiry_flag,expiry_code,"
            "moneyness_label,strike,option_type,request_id,span_unmatched_reason"
        )
        mismatch = int(
            connection.execute(
                f"""SELECT count(*) FROM (
                    (SELECT {key_columns} FROM read_parquet({exception_literal}, hive_partitioning=false)
                     EXCEPT ALL
                     SELECT {key_columns} FROM read_parquet({output_literal}, hive_partitioning=false)
                     WHERE span_enrichment_status='unmatched')
                    UNION ALL
                    (SELECT {key_columns} FROM read_parquet({output_literal}, hive_partitioning=false)
                     WHERE span_enrichment_status='unmatched'
                     EXCEPT ALL
                     SELECT {key_columns} FROM read_parquet({exception_literal}, hive_partitioning=false)))"""
            ).fetchone()[0]
        )
        if mismatch:
            raise ValueError("crash-pair exception keys mismatch")
    except Exception:
        _quarantine_publication(month, output_path, exception_path, manifest_path)
        return None
    manifest = {
        "schema": "dhan_span_gold_month_manifest",
        "schema_version": SPAN_GOLD_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "lineage": lineage,
        "lineage_sha256": lineage_sha,
        "join_policy": SPAN_JOIN_POLICY,
        "input_rows": input_rows,
        "output_rows": input_rows,
        "row_conservation": True,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "duplicate_span_keys": duplicate_span_keys,
        "effective_time_proven": False,
        "lookahead_policy_status": "CONSERVATIVE_BOD_FALLBACK_EFFECTIVE_TIME_UNPROVEN",
        "bod_policy_violation_rows": 0,
        "span_effective_time_contract": "unknown_bod_only_conservative_fallback",
        "span_intraday_sensitivity_joined": False,
        "output_path": str(output_path),
        "output_sha256": sha256_file(output_path),
        "output_bytes": output_path.stat().st_size,
        "exception_path": str(exception_path),
        "exception_sha256": sha256_file(exception_path),
        "exception_bytes": exception_path.stat().st_size,
        "crash_pair_adopted": True,
        "resumed": False,
    }
    _atomic_json(manifest_path, manifest)
    manifest["resumed"] = True
    manifest["manifest_path"] = str(manifest_path)
    manifest["manifest_sha256"] = sha256_file(manifest_path)
    return manifest


def _quarantine_publication(
    month: str, output_path: Path, exception_path: Path, manifest_path: Path
) -> None:
    root = manifest_path.parents[2]
    quarantine = (
        root / "quarantine" / "incomplete_publications" / f"{month}.{uuid.uuid4().hex}"
    )
    for label, path in (
        ("output", output_path),
        ("exception", exception_path),
        ("manifest", manifest_path),
    ):
        if path.exists():
            quarantine.mkdir(parents=True, exist_ok=True)
            os.replace(path, quarantine / f"{label}.{path.name}")


def _bsm_month_audit(payload: dict[str, Any], month: str) -> dict[str, Any] | None:
    audits = payload.get("months_audited") or payload.get("month_audits") or []
    return next((item for item in audits if item.get("month") == month), None)


def _copy_parquet(connection: Any, query: str, path: Path, row_group_size: int) -> None:
    escaped = str(path).replace("'", "''")
    connection.execute(
        f"COPY ({query}) TO '{escaped}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(row_group_size)})"
    )


def _publish_status(
    root: Path,
    selected: Sequence[str],
    results: Sequence[dict[str, Any]],
    *,
    state: str,
    terminal_status: str | None = None,
) -> None:
    status = {
        "schema": "dhan_span_gold_status",
        "schema_version": SPAN_GOLD_VERSION,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "terminal_status": terminal_status,
        "months_completed": len(results),
        "months_total": len(selected),
        "rows_completed": sum(int(item["output_rows"]) for item in results),
        "matched_rows": sum(int(item["matched_rows"]) for item in results),
        "unmatched_rows": sum(int(item["unmatched_rows"]) for item in results),
        "current_month": results[-1]["month"] if results else selected[0],
        "output_root": str(root),
    }
    _atomic_json(root / "manifests" / "span_gold_status.json", status)


def _terminal_markdown(audit: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# SPAN Gold Terminal Audit",
            "",
            f"- Status: `{audit['status']}`",
            f"- Join policy: `{audit['join_policy']}`",
            f"- SPAN Phase 1 outcome: `{audit['span_phase1_outcome']}`",
            f"- Months: {audit['months']}/{audit['expected_months']}",
            f"- Rows: {audit['output_rows']:,}",
            f"- Matched: {audit['matched_rows']:,}",
            f"- Unmatched: {audit['unmatched_rows']:,}",
            f"- Match rate: {audit['match_rate']:.6%}",
            f"- Duplicate SPAN keys: {audit['duplicate_span_keys']}",
            f"- BOD policy violations: {audit['bod_policy_violation_rows']}",
            "- Exact SPAN effective time proven: no",
            "- ID1-ID4/EOD sensitivity slots joined: no",
            f"- Historical minute futures: `{audit['historical_minute_futures_status']}`",
            "",
        ]
    )


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _lineage(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def _json_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"required JSON missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _fsync_file(path: Path) -> None:
    with path.open("rb+") as handle:
        os.fsync(handle.fileno())


def _validate_config(config: SpanGoldConfig) -> None:
    if config.threads < 1:
        raise ValueError("threads must be positive")
    if config.row_group_size < 1:
        raise ValueError("row_group_size must be positive")
    if not config.memory_limit.strip():
        raise ValueError("memory_limit must be non-empty")


def _sql_text(value: str) -> str:
    return value.replace("'", "''")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
