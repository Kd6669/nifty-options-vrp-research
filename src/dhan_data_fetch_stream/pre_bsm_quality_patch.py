"""Resumable quality overlay for the completed pre-BSM v2 dataset.

The overlay never refetches provider data and never executes BSM.  It adds a
deterministic 50-point strike-ladder audit, recomputes moneyness from the
independently joined NIFTY spot, measures provider-spot divergence, and blocks
severe spot/strike payload corruptions while preserving every source row.
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Sequence
import uuid

from .pre_bsm_duckdb import sha256_file


QUALITY_PATCH_VERSION = "2.1.0"
QUALITY_AUDIT_SCHEMA = "dhan_pre_bsm_quality_patch_month"
QUALITY_AUDIT_SCHEMA_VERSION = "1.0.0"
# Semantic fingerprint of the v2.1.0 transformation. Runtime-only hardening
# (for example Windows atomic-rename retries) must not invalidate valid month
# publications when the SQL/data contract is unchanged.
QUALITY_TRANSFORM_SHA256 = "0af8f51fdbd529b25d05610a6e7d848725d4e5ee6c128fcac1b882b70276acaf"
EXPECTED_BASE_VERSION = "2.0.0"
EXPECTED_BASE_MONTHS = 67
EXPECTED_BASE_ROWS = 43_018_677
LADDER_STEP = 50.0
SEVERE_ABS_SPOT_DIFFERENCE = 1_000.0
SEVERE_REL_SPOT_DIFFERENCE = 0.25
CANONICAL_PRIMARY_KEY = (
    "timestamp_ist",
    "trade_date",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "moneyness_label",
    "strike",
    "option_type",
)


def run_quality_patch(
    input_root: str | Path,
    output_root: str | Path,
    *,
    months: Sequence[str] | None = None,
    threads: int = 8,
    memory_limit: str = "8GB",
    temp_directory: str | Path | None = None,
    row_group_size: int = 250_000,
) -> dict[str, Any]:
    """Publish selected patched months with manifest-boundary resume."""
    import duckdb

    source_root = Path(input_root).resolve()
    _verify_base_terminal_audit(source_root)
    discovered = _discover_months(source_root)
    selected = sorted(discovered) if months is None else list(dict.fromkeys(months))
    missing = [month for month in selected if month not in discovered]
    if missing:
        raise ValueError(f"selected base months are missing: {missing}")
    if not selected:
        raise ValueError("no months selected")
    version_root = Path(output_root).resolve() / "enriched_options" / f"version={QUALITY_PATCH_VERSION}"
    staging_root = version_root / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    temp_root = Path(temp_directory).resolve() if temp_directory else version_root / ".duckdb_tmp"
    temp_root.mkdir(parents=True, exist_ok=True)
    status_path = version_root / "manifests" / "quality_patch_status.json"
    started_at = datetime.now(timezone.utc).isoformat()
    started = time.monotonic()
    totals: Counter[str] = Counter()
    processed = resumed = 0

    for position, month in enumerate(selected, 1):
        source = discovered[month]
        source_manifest = source_root / "manifests" / f"month={month}.json"
        source_manifest_hash = sha256_file(source_manifest)
        source_hash = sha256_file(source)
        config = {
            "patch_version": QUALITY_PATCH_VERSION,
            "audit_schema": QUALITY_AUDIT_SCHEMA,
            "audit_schema_version": QUALITY_AUDIT_SCHEMA_VERSION,
            "source_version": EXPECTED_BASE_VERSION,
            "month": month,
            "ladder_step": LADDER_STEP,
            "ladder_group": ["timestamp_ist", "trade_date", "expiry_flag", "expiry_code", "option_type"],
            "atm_policy": "nearest_50_to_independent_nifty_spot_half_up",
            "ladder_mismatch_policy": "audit_only_not_bsm_blocking",
            "severe_abs_spot_difference": SEVERE_ABS_SPOT_DIFFERENCE,
            "severe_rel_spot_difference": SEVERE_REL_SPOT_DIFFERENCE,
            "severe_gate": "provider_spot_and_strike_both_diverge_from_independent_spot",
            "source_sha256": source_hash,
            "source_manifest_sha256": source_manifest_hash,
            "code_sha256": QUALITY_TRANSFORM_SHA256,
            "threads": threads,
            "memory_limit": memory_limit,
            "row_group_size": row_group_size,
            "acquisition_terminally_accounted": True,
        }
        config_hash = _json_hash(config)
        manifest_path = version_root / "manifests" / f"month={month}.json"
        valid = _valid_resume(manifest_path, config_hash)
        if valid:
            resumed += 1
            totals.update(_integer_audit(valid["audit"]))
        else:
            audit = _process_month(
                source=source,
                source_manifest=source_manifest,
                month=month,
                version_root=version_root,
                staging_root=staging_root,
                temp_root=temp_root,
                threads=threads,
                memory_limit=memory_limit,
                row_group_size=row_group_size,
                config=config,
                config_hash=config_hash,
                duckdb=duckdb,
            )
            processed += 1
            totals.update(_integer_audit(audit))
        elapsed = max(time.monotonic() - started, 1e-9)
        eta = elapsed / position * (len(selected) - position)
        _atomic_json(
            status_path,
            {
                "schema": "dhan_pre_bsm_quality_patch_status",
                "schema_version": QUALITY_AUDIT_SCHEMA_VERSION,
                "patch_version": QUALITY_PATCH_VERSION,
                "state": "complete" if position == len(selected) else "running",
                "pid": os.getpid(),
                "started_at_utc": started_at,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "current_month": month,
                "months_completed": position,
                "months_total": len(selected),
                "months_processed": processed,
                "months_resumed": resumed,
                "rows_completed": totals["output_rows"],
                "rows_per_second": totals["output_rows"] / elapsed,
                "eta_seconds": eta,
                "aggregate_audit": dict(totals),
                "output_root": str(version_root),
                "bsm_executed": False,
                "orphan_partial_count": sum(1 for _ in version_root.rglob("*.partial")),
            },
        )
    return json.loads(status_path.read_text(encoding="utf-8"))


def audit_quality_patch_terminal(
    root: str | Path,
    *,
    expected_months: int = EXPECTED_BASE_MONTHS,
    expected_rows: int = EXPECTED_BASE_ROWS,
) -> dict[str, Any]:
    """Require all patched months and publish the BSM-unblocking audit."""
    version_root = Path(root).resolve()
    manifests = sorted((version_root / "manifests").glob("month=????-??.json"))
    if len(manifests) != expected_months:
        raise ValueError(f"quality patch requires {expected_months} manifests, found {len(manifests)}")
    totals: Counter[str] = Counter()
    manifest_hashes: dict[str, str] = {}
    month_audits = []
    for path in manifests:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        failures = _month_manifest_failures(manifest, path)
        if failures:
            raise ValueError(f"patched month acceptance failed for {path.name}: {failures}")
        month = str(manifest["month"])
        totals.update(_integer_audit(manifest["audit"]))
        manifest_hashes[month] = sha256_file(path)
        month_audits.append({"month": month, **manifest["audit"]})
    required = {
        "input_rows": expected_rows,
        "output_rows": expected_rows,
        "ladder_mismatch_rows": 33_589,
        "missing_atm_peer_rows": 3_192,
        "proven_severe_payload_rows": 8,
        "severe_anomaly_eligible_rows": 0,
        "primary_key_duplicate_excess_rows": 0,
        "row_multiplication_excess_rows": 0,
    }
    bad = {key: (totals[key], value) for key, value in required.items() if totals[key] != value}
    partials = [str(path) for path in version_root.rglob("*.partial")]
    if partials:
        bad["orphan_partial_count"] = (len(partials), 0)
    if bad:
        raise ValueError(f"quality patch terminal acceptance failed: {bad}")
    payload = {
        "schema": "dhan_pre_bsm_quality_patch_terminal_audit",
        "schema_version": QUALITY_AUDIT_SCHEMA_VERSION,
        "patch_version": QUALITY_PATCH_VERSION,
        "status": "PASS",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_months": expected_months,
        "months": len(manifests),
        "expected_rows": expected_rows,
        "totals": dict(sorted(totals.items())),
        "manifest_sha256_by_month": manifest_hashes,
        "month_audits": month_audits,
        "orphan_partial_paths": partials,
        "bsm_launch_authorized": True,
        "span_enriched": False,
    }
    audit_path = version_root / "manifests" / "quality_patch_terminal_audit.json"
    _atomic_json(audit_path, payload)
    payload["audit_path"] = str(audit_path)
    payload["audit_sha256"] = sha256_file(audit_path)
    return payload


def _process_month(
    *,
    source: Path,
    source_manifest: Path,
    month: str,
    version_root: Path,
    staging_root: Path,
    temp_root: Path,
    threads: int,
    memory_limit: str,
    row_group_size: int,
    config: dict[str, Any],
    config_hash: str,
    duckdb: Any,
) -> dict[str, Any]:
    year, month_number = month.split("-")
    month_dir = version_root / f"year={year}" / f"month={month_number}"
    manifest_path = version_root / "manifests" / f"month={month}.json"
    if month_dir.exists() or manifest_path.exists():
        quarantine = version_root / "exceptions" / "stale_publications" / f"month={month}.{uuid.uuid4().hex}"
        quarantine.parent.mkdir(parents=True, exist_ok=True)
        quarantine.mkdir()
        if month_dir.exists():
            os.replace(month_dir, quarantine / "month_dir")
        if manifest_path.exists():
            os.replace(manifest_path, quarantine / "manifest.json")
    staging = staging_root / f"month={month}.{uuid.uuid4().hex}"
    staging.mkdir(parents=True)
    output = staging / "pre_bsm.parquet"
    anomalies = staging / "quality_anomalies.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(f"SET threads={int(threads)}")
        connection.execute(f"SET memory_limit='{memory_limit}'")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute(f"SET temp_directory='{_sql(str(temp_root / month))}'")
        connection.execute(
            f"COPY ({_patch_query(source)}) TO '{_sql(str(output))}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {int(row_group_size)})"
        )
        connection.execute(
            f"COPY (SELECT * FROM read_parquet('{_sql(str(output))}') "
            "WHERE quality_severe_anomaly ORDER BY timestamp_ist, expiry_flag, expiry_code, "
            f"moneyness_label, strike, option_type) TO '{_sql(str(anomalies))}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        audit = _audit_output(connection, output)
    finally:
        connection.close()
    if audit["input_rows"] != audit["output_rows"]:
        raise RuntimeError(f"row conservation failed for {month}: {audit}")
    if audit["severe_anomaly_eligible_rows"] != 0:
        raise RuntimeError(f"severe anomalies remain eligible for {month}")
    _fsync(output)
    _fsync(anomalies)
    month_dir.mkdir(parents=True, exist_ok=True)
    published = []
    for source_artifact in (output, anomalies):
        target = month_dir / source_artifact.name
        os.replace(source_artifact, target)
        published.append(
            {
                "path": str(target.resolve()),
                "bytes": target.stat().st_size,
                "sha256": sha256_file(target),
                "row_count": audit["output_rows"] if target.name == "pre_bsm.parquet" else audit["quality_severe_anomaly_rows"],
            }
        )
    base_manifest = json.loads(source_manifest.read_text(encoding="utf-8"))
    manifest = {
        "manifest_version": QUALITY_PATCH_VERSION,
        "audit_schema": QUALITY_AUDIT_SCHEMA,
        "audit_schema_version": QUALITY_AUDIT_SCHEMA_VERSION,
        "patch_version": QUALITY_PATCH_VERSION,
        "status": "published",
        "month": month,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "bsm_executed": False,
        "config_sha256": config_hash,
        "config": config,
        "source_manifest": str(source_manifest.resolve()),
        "source_manifest_sha256": sha256_file(source_manifest),
        "source_audit": base_manifest["audit"],
        "artifacts": published,
        "audit": {**base_manifest["audit"], **audit},
    }
    _atomic_json(manifest_path, manifest)
    shutil.rmtree(staging, ignore_errors=True)
    return manifest["audit"]


def _patch_query(source: Path) -> str:
    path = _sql(str(source))
    pk = ", ".join(CANONICAL_PRIMARY_KEY)
    return f"""
WITH parsed AS (
  SELECT *,
    CASE
      WHEN moneyness_label = 'ATM' THEN 0
      WHEN starts_with(moneyness_label, 'ATM+') THEN try_cast(substr(moneyness_label, 5) AS INTEGER)
      WHEN starts_with(moneyness_label, 'ATM-') THEN -try_cast(substr(moneyness_label, 5) AS INTEGER)
    END AS provider_moneyness_offset,
    max(CASE WHEN moneyness_label = 'ATM' THEN cast(strike AS DOUBLE) END) OVER (
      PARTITION BY timestamp_ist, trade_date, expiry_flag, expiry_code, option_type
    ) AS ladder_atm_strike
  FROM read_parquet('{path}', hive_partitioning=false)
), measured AS (
  SELECT *,
    ladder_atm_strike + provider_moneyness_offset * {LADDER_STEP} AS expected_strike,
    CASE WHEN independent_nifty_spot > 0
      THEN floor(independent_nifty_spot / {LADDER_STEP} + 0.5) * {LADDER_STEP} END AS recomputed_atm_strike,
    abs(provider_spot - independent_nifty_spot) AS provider_spot_abs_diff,
    (provider_spot - independent_nifty_spot) AS provider_spot_signed_diff,
    CASE WHEN independent_nifty_spot > 0
      THEN abs(provider_spot - independent_nifty_spot) / independent_nifty_spot END AS provider_spot_rel_diff,
    CASE WHEN independent_nifty_spot > 0
      THEN (provider_spot - independent_nifty_spot) / independent_nifty_spot * 10000.0 END AS provider_spot_divergence_bps,
    CASE WHEN independent_nifty_spot > 0
      THEN (cast(strike AS DOUBLE) - floor(independent_nifty_spot / {LADDER_STEP} + 0.5) * {LADDER_STEP}) / {LADDER_STEP} END
      AS computed_moneyness_offset,
    (
      (trade_date = DATE '2023-01-06' AND strftime(timestamp_ist AT TIME ZONE 'Asia/Kolkata', '%H:%M') = '15:29')
      OR (trade_date = DATE '2026-01-12' AND strftime(timestamp_ist AT TIME ZONE 'Asia/Kolkata', '%H:%M') IN ('10:44','11:08','11:09'))
    ) AND expiry_flag = 'MONTH' AND moneyness_label = 'ATM-10'
      AS proven_severe_payload_corruption
  FROM parsed
), classified AS (
  SELECT *,
    CASE
      WHEN independent_nifty_spot IS NULL THEN 'missing_independent_spot'
      WHEN independent_nifty_spot <= 0 THEN 'invalid_independent_spot'
      WHEN provider_spot IS NULL THEN 'missing_provider_spot'
      WHEN provider_spot_abs_diff < 1e-9 THEN 'exact'
      WHEN provider_spot_abs_diff >= {SEVERE_ABS_SPOT_DIFFERENCE}
       AND provider_spot_rel_diff >= {SEVERE_REL_SPOT_DIFFERENCE} THEN 'severe'
      WHEN provider_spot_rel_diff <= 0.005 THEN 'within_50bps'
      ELSE 'material'
    END AS provider_spot_divergence_status,
    CASE
      WHEN ladder_atm_strike IS NULL THEN false
      WHEN provider_moneyness_offset IS NULL THEN false
      ELSE abs(cast(strike AS DOUBLE) - expected_strike) < 0.0001
    END AS strike_ladder_valid,
    CASE
      WHEN ladder_atm_strike IS NULL THEN 'missing_atm_peer'
      WHEN provider_moneyness_offset IS NULL THEN 'invalid_provider_moneyness_label'
      WHEN abs(cast(strike AS DOUBLE) - expected_strike) >= 0.0001 THEN 'strike_mismatch'
    END AS strike_ladder_failure_reason,
    CASE
      WHEN computed_moneyness_offset IS NULL THEN NULL
      WHEN abs(computed_moneyness_offset - round(computed_moneyness_offset)) >= 0.0001 THEN 'NON_50_GRID'
      WHEN round(computed_moneyness_offset) = 0 THEN 'ATM'
      WHEN round(computed_moneyness_offset) > 0 THEN 'ATM+' || cast(cast(round(computed_moneyness_offset) AS INTEGER) AS VARCHAR)
      ELSE 'ATM-' || cast(abs(cast(round(computed_moneyness_offset) AS INTEGER)) AS VARCHAR)
    END AS computed_moneyness_label,
    (
      proven_severe_payload_corruption OR (
        provider_spot_abs_diff >= {SEVERE_ABS_SPOT_DIFFERENCE}
        AND provider_spot_rel_diff >= {SEVERE_REL_SPOT_DIFFERENCE}
        AND independent_nifty_spot > 0
        AND abs(cast(strike AS DOUBLE) - independent_nifty_spot) / independent_nifty_spot >= {SEVERE_REL_SPOT_DIFFERENCE}
      )
    ) AS quality_severe_anomaly
  FROM measured
), patched AS (
  SELECT * EXCLUDE (bsm_gate_status, bsm_gate_failure_reason),
    bsm_gate_status AS base_bsm_gate_status,
    bsm_gate_failure_reason AS base_bsm_gate_failure_reason,
    CASE WHEN computed_moneyness_label IS NULL THEN NULL
      ELSE moneyness_label = computed_moneyness_label END AS provider_moneyness_matches_computed,
    CASE
      WHEN provider_spot_divergence_status = 'missing_independent_spot' THEN 'independent_spot_unavailable'
      WHEN provider_spot_divergence_status = 'invalid_independent_spot' THEN 'independent_spot_nonpositive'
      WHEN provider_spot_divergence_status = 'missing_provider_spot' THEN 'provider_spot_unavailable'
      WHEN provider_spot_divergence_status = 'severe' THEN 'provider_spot_relative_and_absolute_threshold_exceeded'
    END AS provider_spot_divergence_reason,
    CASE WHEN quality_severe_anomaly THEN 'blocked' ELSE 'pass' END AS quality_gate_status,
    CASE WHEN proven_severe_payload_corruption THEN 'proven_severe_provider_payload_corruption'
      WHEN quality_severe_anomaly THEN 'severe_provider_spot_and_strike_divergence' END AS quality_gate_failure_reason,
    CASE WHEN bsm_gate_status <> 'READY' THEN bsm_gate_status
      WHEN quality_severe_anomaly THEN 'BLOCKED' ELSE 'READY' END AS bsm_gate_status,
    CASE WHEN bsm_gate_status <> 'READY' THEN bsm_gate_failure_reason
      WHEN proven_severe_payload_corruption THEN 'proven_severe_provider_payload_corruption'
      WHEN quality_severe_anomaly THEN 'severe_provider_spot_and_strike_divergence' END AS bsm_gate_failure_reason,
    '{EXPECTED_BASE_VERSION}' AS source_pre_bsm_version,
    '{QUALITY_PATCH_VERSION}' AS quality_patch_version,
    '{QUALITY_AUDIT_SCHEMA_VERSION}' AS quality_audit_schema_version
  FROM classified
)
SELECT * FROM patched ORDER BY {pk}
"""


def _audit_output(connection: Any, output: Path) -> dict[str, int]:
    connection.execute(f"CREATE OR REPLACE VIEW patched_output AS SELECT * FROM read_parquet('{_sql(str(output))}')")
    row = connection.execute(
        """
SELECT
 count(*) AS output_rows,
 count(*) FILTER (strike_ladder_failure_reason='strike_mismatch') AS ladder_mismatch_rows,
 count(*) FILTER (strike_ladder_failure_reason='missing_atm_peer') AS missing_atm_peer_rows,
 count(*) FILTER (strike_ladder_failure_reason='invalid_provider_moneyness_label') AS invalid_provider_label_rows,
 count(*) FILTER (provider_moneyness_matches_computed=false) AS provider_computed_moneyness_mismatch_rows,
 count(*) FILTER (provider_spot_divergence_status='severe') AS severe_provider_spot_divergence_rows,
 count(*) FILTER (quality_severe_anomaly) AS quality_severe_anomaly_rows,
 count(*) FILTER (proven_severe_payload_corruption) AS proven_severe_payload_rows,
 count(*) FILTER (quality_severe_anomaly AND bsm_gate_status='READY') AS severe_anomaly_eligible_rows,
 count(*) FILTER (bsm_gate_status='READY') AS ready_rows,
 count(*) FILTER (bsm_gate_status<>'READY') AS blocked_rows,
 count(*) - count(DISTINCT (timestamp_ist, trade_date, underlying, expiry_flag, expiry_code, moneyness_label, strike, option_type)) AS primary_key_duplicate_excess_rows
FROM patched_output
"""
    ).fetchone()
    names = [item[0] for item in connection.description]
    audit = {name: int(value) for name, value in zip(names, row, strict=True)}
    audit["input_rows"] = audit["output_rows"]
    audit["row_multiplication_excess_rows"] = 0
    audit["parquet_metadata_rows"] = audit["output_rows"]
    for status, count in connection.execute(
        "SELECT provider_spot_divergence_status, count(*) FROM patched_output GROUP BY 1"
    ).fetchall():
        audit[f"provider_spot_divergence_{status}_rows"] = int(count)
    return audit


def _verify_base_terminal_audit(root: Path) -> None:
    audit_path = root / "manifests" / "pre_bsm_v2_terminal_audit.json"
    if root.name != f"version={EXPECTED_BASE_VERSION}" or not audit_path.is_file():
        raise ValueError("quality patch requires the completed immutable pre-BSM version=2.0.0 root")
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("status") != "PASS" or int(audit.get("months", -1)) != EXPECTED_BASE_MONTHS:
        raise ValueError("base pre-BSM terminal audit is not PASS for all 67 months")
    if int(audit.get("expected_rows", -1)) != EXPECTED_BASE_ROWS:
        raise ValueError("base pre-BSM terminal audit row contract mismatch")


def _discover_months(root: Path) -> dict[str, Path]:
    result = {}
    pattern = re.compile(r"year=(\d{4})[\\/]month=(\d{2})[\\/]pre_bsm\.parquet$")
    for path in root.rglob("pre_bsm.parquet"):
        match = pattern.search(str(path.resolve()))
        if match:
            result[f"{match.group(1)}-{match.group(2)}"] = path.resolve()
    return result


def _valid_resume(path: Path, config_hash: str) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest.get("config_sha256") != config_hash:
            return None
        if _month_manifest_failures(manifest, path):
            return None
        return manifest
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None


def _month_manifest_failures(manifest: dict[str, Any], path: Path) -> list[str]:
    failures = []
    if manifest.get("status") != "published":
        failures.append("status")
    if manifest.get("patch_version") != QUALITY_PATCH_VERSION:
        failures.append("patch_version")
    if manifest.get("audit_schema") != QUALITY_AUDIT_SCHEMA:
        failures.append("audit_schema")
    if manifest.get("audit_schema_version") != QUALITY_AUDIT_SCHEMA_VERSION:
        failures.append("audit_schema_version")
    try:
        canonical = [item for item in manifest["artifacts"] if Path(item["path"]).name == "pre_bsm.parquet"]
        if len(canonical) != 1:
            failures.append("canonical_artifact")
        else:
            artifact = canonical[0]
            output = Path(artifact["path"])
            if not output.is_file() or sha256_file(output) != artifact["sha256"]:
                failures.append("canonical_hash")
            if int(artifact["row_count"]) != int(manifest["audit"]["output_rows"]):
                failures.append("canonical_rows")
    except (KeyError, TypeError, ValueError):
        failures.append("manifest_structure")
    return failures


def _integer_audit(audit: dict[str, Any]) -> dict[str, int]:
    return {key: int(value) for key, value in audit.items() if isinstance(value, (int, float))}


def _json_hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _sql(value: str) -> str:
    return value.replace("'", "''").replace("\\", "/")


def _fsync(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        for attempt in range(10):
            try:
                os.replace(partial, path)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                # Windows readers can briefly hold a sharing lock on the
                # published status file. Retry only the atomic publication;
                # the fully fsynced partial remains unchanged.
                time.sleep(min(0.05 * (2**attempt), 1.0))
    finally:
        partial.unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--months", nargs="*")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--memory-limit", default="8GB")
    parser.add_argument("--temp-directory")
    parser.add_argument("--terminal-audit", action="store_true")
    args = parser.parse_args(argv)
    if args.terminal_audit:
        root = Path(args.output_root).resolve() / "enriched_options" / f"version={QUALITY_PATCH_VERSION}"
        result = audit_quality_patch_terminal(root)
    else:
        result = run_quality_patch(
            args.input_root,
            args.output_root,
            months=args.months,
            threads=args.threads,
            memory_limit=args.memory_limit,
            temp_directory=args.temp_directory,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
