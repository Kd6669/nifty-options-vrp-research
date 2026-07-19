"""Credential-free terminal audits for monthly pre-BSM and BSM v2 datasets."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Sequence
import uuid

from .bsm_v2_runner import (
    _discover_month_sources,
    _verify_pre_bsm_acceptance,
    sha256_file,
)


def audit_pre_bsm_v2(
    input_root: str | Path,
    *,
    expected_rows: int,
    expected_months: int | None = None,
) -> dict[str, Any]:
    """Re-hash every canonical month and aggregate join/gate coverage."""
    root = Path(input_root).resolve()
    discovered = _discover_month_sources(root)
    months = sorted(discovered)
    if expected_months is not None and len(months) != expected_months:
        raise ValueError(f"expected {expected_months} months, found {len(months)}")
    month_audits: list[dict[str, Any]] = []
    totals: Counter[str] = Counter()
    manifest_hashes: dict[str, str] = {}
    for month in months:
        accepted = _verify_pre_bsm_acceptance(discovered[month], month)
        manifest_path = Path(accepted["manifest_path"])
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        audit = {key: int(value) if isinstance(value, (int, float)) else value for key, value in manifest["audit"].items()}
        for key, value in audit.items():
            if isinstance(value, int):
                totals[key] += value
        manifest_hashes[month] = accepted["manifest_sha256"]
        month_audits.append({"month": month, **audit})
    if totals["input_rows"] != expected_rows or totals["output_rows"] != expected_rows:
        raise ValueError(
            f"terminal row total mismatch: expected={expected_rows} "
            f"input={totals['input_rows']} output={totals['output_rows']}"
        )
    if totals["canonical_regular_rows"] + totals["source_exception_rows"] != expected_rows:
        raise ValueError("terminal canonical/source-exception conservation failed")
    violations = {
        key: totals[key]
        for key in (
            "future_join_violations",
            "asof_tolerance_violations",
            "primary_key_duplicate_groups",
            "primary_key_duplicate_excess_rows",
            "duplicate_right_rows",
            "orphan_partial_count",
        )
    }
    if any(violations.values()):
        raise ValueError(f"terminal integrity violations: {violations}")
    canonical_paths = [str(paths[0]) for paths in discovered.values()]
    coverage = _coverage_counts(canonical_paths)
    payload = {
        "schema": "dhan_pre_bsm_v2_terminal_audit",
        "schema_version": "2.0.0",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS",
        "input_root": str(root),
        "expected_rows": expected_rows,
        "months": len(months),
        "totals": dict(sorted(totals.items())),
        "violations": violations,
        "coverage_counts": coverage,
        "month_audits": month_audits,
        "manifest_sha256_by_month": manifest_hashes,
        "canonical_dataset_semantic_signature": _semantic_signature(canonical_paths),
        "orphan_partial_paths": [str(path) for path in root.rglob("*.partial")],
    }
    return _publish_audit(root / "manifests" / "pre_bsm_v2_terminal_audit.json", payload)


def audit_bsm_v2(
    input_root: str | Path,
    *,
    expected_rows: int,
    expected_months: int | None = None,
    expected_ready_rows: int | None = None,
    expected_blocked_rows: int | None = None,
) -> dict[str, Any]:
    """Validate every BSM month manifest/output and aggregate solver evidence."""
    root = Path(input_root).resolve()
    manifests = sorted((root / "manifests").glob("year=*/month=*.json"))
    if expected_months is not None and len(manifests) != expected_months:
        raise ValueError(f"expected {expected_months} BSM months, found {len(manifests)}")
    rows = 0
    status_counts: Counter[str] = Counter()
    method_counts: Counter[str] = Counter()
    metrics: Counter[str] = Counter()
    months: list[dict[str, Any]] = []
    outputs: list[str] = []
    manifest_hashes: dict[str, str] = {}
    for path in manifests:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        input_rows = int(manifest.get("input_rows", -1))
        output_rows = int(manifest.get("output_rows", -2))
        output = Path(str(manifest.get("output_path", ""))).resolve()
        failures = []
        if manifest.get("schema_version") != "2.1.0":
            failures.append("schema_version")
        if input_rows < 0 or input_rows != output_rows:
            failures.append("row_conservation")
        if int(manifest.get("parquet_metadata_rows", -1)) != output_rows:
            failures.append("parquet_metadata_rows")
        if manifest.get("output_sha256") != sha256_file(output):
            failures.append("output_hash")
        if int(manifest.get("primary_key_duplicate_rows", -1)) != 0:
            failures.append("primary_key_duplicates")
        for key in (
            "quality_severe_solved_rows",
            "blocked_rows_with_finite_bsm_values",
            "solver_success_nonfinite_rows",
            "call_delta_range_violations",
            "put_delta_range_violations",
            "negative_gamma_rows",
            "negative_vega_rows",
        ):
            if int(manifest.get(key, -1)) != 0:
                failures.append(key)
        if failures:
            raise ValueError(f"BSM manifest acceptance failed for {path}: {failures}")
        rows += output_rows
        status_counts.update({str(k): int(v) for k, v in manifest["status_counts"].items()})
        method_counts.update({str(k): int(v) for k, v in manifest["solver_method_counts"].items()})
        for key in (
            "ready_input_rows",
            "blocked_input_rows",
            "eligible_rows",
            "converged_rows",
            "fallback_rows",
            "no_arbitrage_rejects",
            "quality_severe_input_rows",
            "proven_severe_input_rows",
            "quality_severe_solved_rows",
            "blocked_rows_with_finite_bsm_values",
            "solver_success_nonfinite_rows",
            "call_delta_range_violations",
            "put_delta_range_violations",
            "negative_gamma_rows",
            "negative_vega_rows",
        ):
            metrics[key] += int(manifest[key])
        outputs.append(str(output))
        manifest_hashes[str(manifest["month"])] = sha256_file(path)
        months.append(
            {
                "month": manifest["month"],
                "rows": output_rows,
                "output_sha256": manifest["output_sha256"],
                "manifest_sha256": sha256_file(path),
                "status_counts": manifest["status_counts"],
                "solver_method_counts": manifest["solver_method_counts"],
                "residual_abs_quantiles": manifest["residual_abs_quantiles"],
            }
        )
    if rows != expected_rows:
        raise ValueError(f"BSM terminal row total mismatch: expected={expected_rows} output={rows}")
    if metrics["ready_input_rows"] + metrics["blocked_input_rows"] != expected_rows:
        raise ValueError("BSM READY/BLOCKED row accounting failed")
    if metrics["eligible_rows"] != metrics["ready_input_rows"]:
        raise ValueError("BSM eligible rows disagree with patched READY rows")
    if expected_ready_rows is not None and metrics["ready_input_rows"] != expected_ready_rows:
        raise ValueError(
            f"expected {expected_ready_rows} READY rows, found {metrics['ready_input_rows']}"
        )
    if expected_blocked_rows is not None and metrics["blocked_input_rows"] != expected_blocked_rows:
        raise ValueError(
            f"expected {expected_blocked_rows} BLOCKED rows, found {metrics['blocked_input_rows']}"
        )
    partials = [str(path) for path in root.rglob("*.partial")]
    if partials:
        raise ValueError(f"BSM orphan partials found: {partials[:5]}")
    numerical = _bsm_numerical_audit(outputs)
    violating = {
        key: value
        for key, value in numerical.items()
        if key.endswith("_violation_rows") and int(value) != 0
    }
    if violating:
        raise ValueError(f"BSM numerical terminal violations: {violating}")
    payload = {
        "schema": "dhan_bsm_v2_terminal_audit",
        "schema_version": "2.1.0",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "PASS",
        "input_root": str(root),
        "expected_rows": expected_rows,
        "months": len(manifests),
        "output_rows": rows,
        "status_counts": dict(sorted(status_counts.items())),
        "solver_method_counts": dict(sorted(method_counts.items())),
        "solver_metrics": dict(sorted(metrics.items())),
        "numerical_audit": numerical,
        "months_audited": months,
        "manifest_sha256_by_month": manifest_hashes,
        "dataset_semantic_signature": _semantic_signature(outputs),
        "orphan_partial_paths": partials,
        "span_enriched": False,
    }
    return _publish_audit(root / "manifests" / "bsm_v2_terminal_audit.json", payload)


def _bsm_numerical_audit(paths: Sequence[str]) -> dict[str, Any]:
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("SET threads=6")
        connection.execute("SET memory_limit='6GB'")
        connection.from_parquet(list(paths), union_by_name=True).create_view("bsm_output")
        row = connection.execute(
            """
SELECT
  count(*) AS rows,
  count(*) FILTER (upper(bsm_gate_status)='READY') AS ready_rows,
  count(*) FILTER (upper(bsm_gate_status)='BLOCKED') AS blocked_rows,
  count(*) FILTER (quality_severe_anomaly) AS severe_rows,
  count(*) FILTER (proven_severe_payload_corruption) AS proven_severe_rows,
  count(*) FILTER (quality_severe_anomaly AND (
    bsm_solver_converged OR bsm_iv_close IS NOT NULL OR bsm_delta IS NOT NULL OR
    bsm_gamma IS NOT NULL OR bsm_vega_per_1 IS NOT NULL OR bsm_theta_per_year IS NOT NULL OR
    bsm_rho_per_1 IS NOT NULL OR bsm_price_reconstructed IS NOT NULL
  )) AS severe_solved_violation_rows,
  count(*) FILTER (upper(bsm_gate_status)='BLOCKED' AND (
    bsm_solver_converged OR bsm_iv_close IS NOT NULL OR bsm_delta IS NOT NULL OR
    bsm_gamma IS NOT NULL OR bsm_vega_per_1 IS NOT NULL OR bsm_theta_per_year IS NOT NULL OR
    bsm_rho_per_1 IS NOT NULL OR bsm_price_reconstructed IS NOT NULL
  )) AS blocked_solved_violation_rows,
  count(*) FILTER (bsm_status='ok' AND (
    NOT isfinite(bsm_iv_close) OR NOT isfinite(bsm_price_reconstructed) OR
    NOT isfinite(bsm_price_residual_abs) OR NOT isfinite(bsm_delta) OR NOT isfinite(bsm_gamma) OR
    NOT isfinite(bsm_vega_per_1) OR NOT isfinite(bsm_theta_per_year) OR NOT isfinite(bsm_rho_per_1)
  )) AS success_nonfinite_violation_rows,
  count(*) FILTER (bsm_status='ok' AND upper(option_type) IN ('CALL','CE','C')
    AND (bsm_delta < -1e-12 OR bsm_delta > 1.0+1e-12)) AS call_delta_violation_rows,
  count(*) FILTER (bsm_status='ok' AND upper(option_type) IN ('PUT','PE','P')
    AND (bsm_delta < -1.0-1e-12 OR bsm_delta > 1e-12)) AS put_delta_violation_rows,
  count(*) FILTER (bsm_status='ok' AND bsm_gamma < -1e-12) AS gamma_negative_violation_rows,
  count(*) FILTER (bsm_status='ok' AND bsm_vega_per_1 < -1e-12) AS vega_negative_violation_rows,
  count(*) FILTER (bsm_status='ok') AS solver_success_rows,
  count(*) FILTER (bsm_status='no_arbitrage_violation') AS no_arbitrage_reject_rows,
  count(*) FILTER (bsm_status='iv_solver_failed') AS solver_failed_rows,
  approx_quantile(bsm_price_residual_abs, 0.50) FILTER (bsm_status='ok') AS residual_p50,
  approx_quantile(bsm_price_residual_abs, 0.95) FILTER (bsm_status='ok') AS residual_p95,
  approx_quantile(bsm_price_residual_abs, 0.99) FILTER (bsm_status='ok') AS residual_p99,
  max(bsm_price_residual_abs) FILTER (bsm_status='ok') AS residual_max
FROM bsm_output
"""
        ).fetchone()
        names = [item[0] for item in connection.description]
        return {
            name: (int(value) if name.endswith("_rows") else float(value) if value is not None else None)
            for name, value in zip(names, row, strict=True)
        }
    finally:
        connection.close()


def _coverage_counts(paths: Sequence[str]) -> dict[str, dict[str, int]]:
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("SET threads=4")
        connection.execute("SET memory_limit='4GB'")
        relation = connection.from_parquet(list(paths), union_by_name=True)
        relation.create_view("canonical")
        result: dict[str, dict[str, int]] = {}
        for column in (
            "bsm_gate_failure_reason",
            "nifty_spot_join_failure_reason",
            "india_vix_join_status",
            "india_vix_join_failure_reason",
            "expiry_mapping_status",
            "contract_rule_status",
            "time_to_expiry_status",
        ):
            rows = connection.execute(
                f'SELECT coalesce(cast("{column}" AS VARCHAR), \'<NULL>\'), count(*) '
                f'FROM canonical GROUP BY 1 ORDER BY 1'
            ).fetchall()
            result[column] = {str(value): int(count) for value, count in rows}
        return result
    finally:
        connection.close()


def _semantic_signature(paths: Sequence[str]) -> dict[str, int]:
    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute("SET threads=4")
        relation = connection.from_parquet(list(paths), union_by_name=True)
        relation.create_view("semantic_source")
        columns = [row[0] for row in connection.execute("DESCRIBE semantic_source").fetchall()]
        quoted = ",".join('"' + column.replace('"', '""') + '"' for column in columns)
        row_count, xor_hash = connection.execute(
            f"SELECT count(*), bit_xor(hash({quoted})) FROM semantic_source"
        ).fetchone()
        return {"row_count": int(row_count), "bit_xor_duckdb_hash": int(xor_hash)}
    finally:
        connection.close()


def _publish_audit(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    markdown = path.with_suffix(".md")
    lines = [
        f"# {payload['schema']}",
        "",
        f"- Status: **{payload['status']}**",
        f"- Created UTC: `{payload['created_at_utc']}`",
        f"- Expected rows: `{payload['expected_rows']}`",
        f"- Months: `{payload['months']}`",
        f"- JSON SHA-256: `{hashlib.sha256(body.encode()).hexdigest()}`",
    ]
    markdown_body = "\n".join(lines) + "\n"
    markdown_partial = markdown.with_name(f".{markdown.name}.{uuid.uuid4().hex}.partial")
    try:
        with markdown_partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(markdown_body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(markdown_partial, markdown)
    finally:
        markdown_partial.unlink(missing_ok=True)
    return payload | {"audit_path": str(path), "audit_sha256": sha256_file(path)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("layer", choices=("pre-bsm", "bsm"))
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--expected-rows", required=True, type=int)
    parser.add_argument("--expected-months", type=int)
    parser.add_argument("--expected-ready-rows", type=int)
    parser.add_argument("--expected-blocked-rows", type=int)
    args = parser.parse_args(argv)
    function = audit_pre_bsm_v2 if args.layer == "pre-bsm" else audit_bsm_v2
    kwargs: dict[str, Any] = {
        "expected_rows": args.expected_rows,
        "expected_months": args.expected_months,
    }
    if args.layer == "bsm":
        kwargs["expected_ready_rows"] = args.expected_ready_rows
        kwargs["expected_blocked_rows"] = args.expected_blocked_rows
    result = function(args.input_root, **kwargs)
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
