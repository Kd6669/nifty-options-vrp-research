"""Atomic, month-resumable runner for vectorized BSM v2 materialization."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from typing import Any, Sequence
import uuid

import numpy as np

from .bsm_vectorized import (
    BSM_V2_MODEL_VERSION,
    VectorizedBsmConfig,
    solve_bsm_vectorized,
)


BSM_V2_DATASET_VERSION = "2.1.0"
REQUIRED_PRE_BSM_PATCH_VERSION = "2.1.0"
REQUIRED_PRE_BSM_QUALITY_SCHEMA = "dhan_pre_bsm_quality_patch_month"
REQUIRED_PRE_BSM_QUALITY_SCHEMA_VERSION = "1.0.0"
REQUIRED_PRE_BSM_MONTHS = 67
REQUIRED_PRE_BSM_ROWS = 43_018_677
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


@dataclass(frozen=True)
class BsmMonthResult:
    month: str
    input_rows: int
    output_rows: int
    output_path: str
    output_sha256: str
    manifest_path: str
    manifest_sha256: str
    resumed: bool
    status_counts: dict[str, int]


@dataclass(frozen=True)
class BsmV2RunStats:
    months_total: int
    months_processed: int
    months_resumed: int
    rows_total: int
    status_counts: dict[str, int]
    output_root: str
    status_path: str
    status_markdown_path: str
    elapsed_seconds: float


def run_bsm_v2_root(
    input_root: str | Path,
    output_root: str | Path,
    *,
    months: Sequence[str] | None = None,
    config: VectorizedBsmConfig | None = None,
    row_group_size: int = 250_000,
) -> BsmV2RunStats:
    """Run all selected audited monthly partitions with atomic progress status."""
    cfg = config or VectorizedBsmConfig()
    discovered = _discover_month_sources(Path(input_root))
    selected = sorted(discovered) if months is None else list(dict.fromkeys(months))
    invalid = [month for month in selected if not _valid_month(month)]
    missing = [month for month in selected if month not in discovered]
    if invalid:
        raise ValueError(f"invalid month selection: {invalid}")
    if missing:
        raise ValueError(f"selected months are absent from input root: {missing}")
    if not selected:
        raise ValueError("no pre-BSM-v2 monthly Parquets found")
    version_root = Path(output_root) / f"version={BSM_V2_DATASET_VERSION}"
    status_path = version_root / "manifests" / "bsm_v2_status.json"
    status_md_path = version_root / "manifests" / "bsm_v2_status.md"
    started = time.monotonic()
    started_at_utc = datetime.now(timezone.utc).isoformat()
    processed = resumed = rows = 0
    aggregate: Counter[str] = Counter()
    method_aggregate: Counter[str] = Counter()
    metric_totals: Counter[str] = Counter()
    initial_partials = [str(path.resolve()) for path in sorted(version_root.rglob("*.partial"))]
    initial_status = {
        "schema": "dhan_vectorized_bsm_run_status",
        "schema_version": BSM_V2_DATASET_VERSION,
        "updated_at_utc": started_at_utc,
        "started_at_utc": started_at_utc,
        "pid": os.getpid(),
        "state": "running",
        "current_month": selected[0],
        "months_completed": 0,
        "months_total": len(selected),
        "months_processed": 0,
        "months_resumed": 0,
        "rows_completed": 0,
        "rows_per_second": 0.0,
        "eta_seconds": None,
        "status_counts": {},
        "solver_method_counts": {},
        "solver_metrics": {},
        "output_root": str(version_root.resolve()),
        "orphan_partial_count": len(initial_partials),
        "orphan_partial_paths": initial_partials,
    }
    _atomic_json(status_path, initial_status)
    _atomic_text(status_md_path, _status_markdown(initial_status))
    for position, month in enumerate(selected, start=1):
        result = run_bsm_v2_month(
            discovered[month],
            output_root,
            month=month,
            config=cfg,
            row_group_size=row_group_size,
        )
        processed += int(not result.resumed)
        resumed += int(result.resumed)
        rows += result.output_rows
        aggregate.update(result.status_counts)
        month_manifest = json.loads(Path(result.manifest_path).read_text(encoding="utf-8"))
        method_aggregate.update(
            {str(key): int(value) for key, value in month_manifest["solver_method_counts"].items()}
        )
        for key in (
            "ready_input_rows",
            "blocked_input_rows",
            "eligible_rows",
            "converged_rows",
            "fallback_rows",
            "no_arbitrage_rejects",
            "quality_severe_input_rows",
            "quality_severe_solved_rows",
            "blocked_rows_with_finite_bsm_values",
        ):
            metric_totals[key] += int(month_manifest[key])
        elapsed = max(time.monotonic() - started, 1.0e-9)
        row_rate = rows / elapsed
        remaining_months = len(selected) - position
        eta_seconds = None if position == 0 else elapsed / position * remaining_months
        status = {
            "schema": "dhan_vectorized_bsm_run_status",
            "schema_version": BSM_V2_DATASET_VERSION,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
            "started_at_utc": started_at_utc,
            "pid": os.getpid(),
            "state": "complete" if position == len(selected) else "running",
            "current_month": month,
            "months_completed": position,
            "months_total": len(selected),
            "months_processed": processed,
            "months_resumed": resumed,
            "rows_completed": rows,
            "rows_per_second": row_rate,
            "eta_seconds": eta_seconds,
            "status_counts": dict(sorted(aggregate.items())),
            "solver_method_counts": dict(sorted(method_aggregate.items())),
            "solver_metrics": dict(sorted(metric_totals.items())),
            "output_root": str(version_root.resolve()),
            "orphan_partial_count": sum(1 for _ in version_root.rglob("*.partial")),
            "orphan_partial_paths": [
                str(path.resolve()) for path in sorted(version_root.rglob("*.partial"))
            ],
        }
        _atomic_json(status_path, status)
        _atomic_text(status_md_path, _status_markdown(status))
    elapsed = time.monotonic() - started
    return BsmV2RunStats(
        months_total=len(selected),
        months_processed=processed,
        months_resumed=resumed,
        rows_total=rows,
        status_counts=dict(sorted(aggregate.items())),
        output_root=str(version_root),
        status_path=str(status_path),
        status_markdown_path=str(status_md_path),
        elapsed_seconds=elapsed,
    )


def run_bsm_v2_month(
    input_source: str | Path | Sequence[str | Path],
    output_root: str | Path,
    *,
    month: str,
    config: VectorizedBsmConfig | None = None,
    row_group_size: int = 250_000,
) -> BsmMonthResult:
    """Materialize one audited pre-BSM-v2 month to one atomic BSM-v2 file.

    A published month is resumed only when input hashes, numerical config,
    output hash, and Parquet row count all agree. A corrupt or lineage-stale
    output is quarantined before regeneration; a valid publication is never
    overwritten in place.
    """
    if not _valid_month(month):
        raise ValueError("month must be YYYY-MM")
    if row_group_size <= 0:
        raise ValueError("row_group_size must be positive")
    cfg = config or VectorizedBsmConfig()
    input_paths = _input_paths(input_source)
    acceptance = _verify_pre_bsm_acceptance(input_paths, month)
    input_lineage = [
        {"path": str(path.resolve()), "sha256": sha256_file(path), "bytes": path.stat().st_size}
        for path in input_paths
    ]
    config_payload = {
        "dataset_version": BSM_V2_DATASET_VERSION,
        "model_version": BSM_V2_MODEL_VERSION,
        "month": month,
        "price_input_field": "close",
        "row_group_size": row_group_size,
        "primary_key": list(CANONICAL_PRIMARY_KEY),
        "solver": asdict(cfg),
        "code_sha256": sha256_file(Path(__file__)),
        "input_lineage": input_lineage,
        "pre_bsm_acceptance": acceptance,
    }
    lineage_sha = _json_sha(config_payload)
    root = Path(output_root) / f"version={BSM_V2_DATASET_VERSION}"
    year, month_number = month.split("-")
    output_path = root / f"year={year}" / f"month={month_number}" / "part-000.parquet"
    manifest_path = root / "manifests" / f"year={year}" / f"month={month_number}.json"

    resumed = _resume_result(manifest_path, output_path, lineage_sha)
    if resumed is not None:
        return resumed
    _quarantine_stale_publication(root, month, output_path, manifest_path)

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    tables = [pq.ParquetFile(path).read() for path in input_paths]
    table = tables[0] if len(tables) == 1 else pa.concat_tables(tables, promote_options="default")
    _require_columns(table.column_names)
    sort_keys = [(name, "ascending") for name in CANONICAL_PRIMARY_KEY]
    table = table.take(pc.sort_indices(table, sort_keys=sort_keys))
    input_rows = table.num_rows
    duplicate_count = _adjacent_duplicate_count(table, CANONICAL_PRIMARY_KEY)

    spot = _float_column(table, "independent_nifty_spot")
    strike = _float_column(table, "strike")
    price = _float_column(table, "close")
    time_years = _float_column(table, "t_years_act365")
    call = _text_membership(table, "option_type", {"CALL", "CE", "C"})
    put = _text_membership(table, "option_type", {"PUT", "PE", "P"})
    regular = _text_membership(table, "session_status", {"regular_session"})
    spot_ready = _text_membership(table, "nifty_spot_join_status", {"matched"})
    expiry_ready = _text_membership(table, "expiry_mapping_status", {"resolved"})
    rule_ready = _text_membership(table, "contract_rule_status", {"resolved"})
    time_ready = _text_membership(table, "time_to_expiry_status", {"valid"})
    quality_gate_ready = _text_membership(table, "bsm_gate_status", {"ready"})
    quality_severe = _bool_column(table, "quality_severe_anomaly")
    proven_severe = _bool_column(table, "proven_severe_payload_corruption")
    expiry_present = pc.invert(pc.is_null(table["actual_expiry_timestamp_ist"])).to_numpy(
        zero_copy_only=False
    )
    lot_size = _float_column(table, "contract_lot_size")
    option_type_ready = call | put

    base_eligible = (
        regular
        & spot_ready
        & expiry_ready
        & rule_ready
        & time_ready
        & quality_gate_ready
        & expiry_present
        & np.isfinite(lot_size)
        & (lot_size > 0.0)
        & option_type_ready
    )
    ready_input_rows = int(np.count_nonzero(quality_gate_ready))
    blocked_input_rows = input_rows - ready_input_rows
    if int(np.count_nonzero(base_eligible)) != ready_input_rows:
        raise ValueError(
            "patched READY contract disagrees with numerical/session eligibility: "
            f"ready={ready_input_rows} eligible={int(np.count_nonzero(base_eligible))}"
        )
    if np.any(quality_severe & base_eligible):
        raise ValueError("quality-severe input row entered the solver-eligible population")
    failure = np.full(input_rows, None, dtype=object)
    _set_first_failure(failure, ~regular, "non_regular_session")
    _set_first_failure(failure, ~spot_ready, "independent_nifty_spot_unavailable")
    _set_first_failure(failure, ~expiry_ready | ~expiry_present, "actual_expiry_unresolved")
    _set_first_failure(failure, ~rule_ready | ~np.isfinite(lot_size) | (lot_size <= 0.0), "contract_rule_unresolved")
    _set_first_failure(failure, ~time_ready, "time_to_expiry_invalid")
    _set_first_failure(failure, ~quality_gate_ready, "pre_bsm_quality_gate_blocked")
    _set_first_failure(failure, ~option_type_ready, "invalid_option_type")

    solved = solve_bsm_vectorized(
        spot=spot,
        strike=strike,
        observed_price=price,
        time_years=time_years,
        is_call=call,
        base_eligible=base_eligible,
        base_failure_reason=failure,
        config=cfg,
    )
    columns = dict(solved.columns)
    columns["bsm_provider_iv_delta_decimal"] = _provider_iv_delta(table, columns["bsm_iv_close"])
    for name in columns:
        if name in table.column_names:
            raise ValueError(f"pre-BSM input unexpectedly already contains output column: {name}")
    for name, values in columns.items():
        table = table.append_column(name, _arrow_array(pa, values))

    converged = np.asarray(columns["bsm_solver_converged"], dtype=np.bool_)
    protected_numeric = (
        "bsm_iv_close",
        "bsm_price_reconstructed",
        "bsm_price_residual_signed",
        "bsm_price_residual_abs",
        "bsm_delta",
        "bsm_gamma",
        "bsm_theta_per_year",
        "bsm_theta_per_day_365",
        "bsm_vega_per_1",
        "bsm_vega_per_100",
        "bsm_rho_per_1",
        "bsm_rho_per_100",
    )
    finite_matrix = np.column_stack(
        [np.isfinite(np.asarray(columns[name], dtype=np.float64)) for name in protected_numeric]
    )
    blocked_with_finite = int(np.count_nonzero((~quality_gate_ready) & np.any(finite_matrix, axis=1)))
    severe_solved = int(np.count_nonzero(quality_severe & (converged | np.any(finite_matrix, axis=1))))
    if blocked_with_finite or severe_solved:
        raise RuntimeError(
            "protected BSM gate violation: "
            f"blocked_with_finite={blocked_with_finite} severe_solved={severe_solved}"
        )
    success = converged
    if np.any(success & ~np.all(finite_matrix, axis=1)):
        raise RuntimeError("solver-success row contains non-finite IV/price/Greek output")
    delta = np.asarray(columns["bsm_delta"], dtype=np.float64)
    gamma = np.asarray(columns["bsm_gamma"], dtype=np.float64)
    vega = np.asarray(columns["bsm_vega_per_1"], dtype=np.float64)
    tolerance = 1.0e-12
    if np.any(success & call & ((delta < -tolerance) | (delta > 1.0 + tolerance))):
        raise RuntimeError("converged CALL delta outside [0,1]")
    if np.any(success & put & ((delta < -1.0 - tolerance) | (delta > tolerance))):
        raise RuntimeError("converged PUT delta outside [-1,0]")
    if np.any(success & ((gamma < -tolerance) | (vega < -tolerance))):
        raise RuntimeError("converged gamma/vega negativity violation")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.partial")
    try:
        pq.write_table(
            table,
            partial,
            compression="zstd",
            row_group_size=row_group_size,
            write_statistics=True,
        )
        _fsync_file(partial)
        metadata_rows = pq.ParquetFile(partial).metadata.num_rows
        if metadata_rows != input_rows:
            raise RuntimeError(
                f"row conservation failed before publication: {input_rows=} {metadata_rows=}"
            )
        _replace_with_retry(partial, output_path)
    finally:
        partial.unlink(missing_ok=True)
    output_sha = sha256_file(output_path)
    status_counts = Counter(str(value) for value in columns["bsm_status"])
    method_counts = Counter(str(value) for value in columns["bsm_solver_method"])
    residual = np.asarray(columns["bsm_price_residual_abs"], dtype=np.float64)
    finite_residual = residual[np.isfinite(residual)]
    provider_delta = np.asarray(columns["bsm_provider_iv_delta_decimal"], dtype=np.float64)
    finite_delta = provider_delta[np.isfinite(provider_delta)]
    manifest: dict[str, Any] = {
        "schema": "dhan_vectorized_bsm_month_manifest",
        "schema_version": BSM_V2_DATASET_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "lineage_sha256": lineage_sha,
        "config": config_payload,
        "input_rows": input_rows,
        "output_rows": input_rows,
        "row_conservation": True,
        "primary_key_duplicate_rows": duplicate_count,
        "output_path": str(output_path.resolve()),
        "output_sha256": output_sha,
        "output_bytes": output_path.stat().st_size,
        "parquet_metadata_rows": pq.ParquetFile(output_path).metadata.num_rows,
        "status_counts": dict(sorted(status_counts.items())),
        "solver_method_counts": dict(sorted(method_counts.items())),
        "ready_input_rows": ready_input_rows,
        "blocked_input_rows": blocked_input_rows,
        "eligible_rows": int(np.count_nonzero(base_eligible)),
        "converged_rows": int(np.count_nonzero(converged)),
        "fallback_rows": int(np.count_nonzero(columns["bsm_solver_method"] == "brent")),
        "no_arbitrage_rejects": int(status_counts.get("no_arbitrage_violation", 0)),
        "near_expiry_rows": int(np.count_nonzero(columns["bsm_near_expiry"])),
        "quality_severe_input_rows": int(np.count_nonzero(quality_severe)),
        "proven_severe_input_rows": int(np.count_nonzero(proven_severe)),
        "quality_severe_solved_rows": severe_solved,
        "blocked_rows_with_finite_bsm_values": blocked_with_finite,
        "solver_success_nonfinite_rows": int(np.count_nonzero(success & ~np.all(finite_matrix, axis=1))),
        "call_delta_range_violations": int(
            np.count_nonzero(success & call & ((delta < -tolerance) | (delta > 1.0 + tolerance)))
        ),
        "put_delta_range_violations": int(
            np.count_nonzero(success & put & ((delta < -1.0 - tolerance) | (delta > tolerance)))
        ),
        "negative_gamma_rows": int(np.count_nonzero(success & (gamma < -tolerance))),
        "negative_vega_rows": int(np.count_nonzero(success & (vega < -tolerance))),
        "residual_abs_quantiles": _quantiles(finite_residual),
        "provider_iv_delta_decimal_quantiles": _quantiles(finite_delta),
        "provider_iv_delta_note": "computed only when provider_iv_unit explicitly declares decimal",
        "orphan_partials": [],
    }
    manifest_sha = _atomic_json(manifest_path, manifest)
    return BsmMonthResult(
        month=month,
        input_rows=input_rows,
        output_rows=input_rows,
        output_path=str(output_path),
        output_sha256=output_sha,
        manifest_path=str(manifest_path),
        manifest_sha256=manifest_sha,
        resumed=False,
        status_counts=dict(status_counts),
    )


def _require_columns(columns: Sequence[str]) -> None:
    required = set(CANONICAL_PRIMARY_KEY) | {
        "close",
        "session_status",
        "independent_nifty_spot",
        "nifty_spot_join_status",
        "actual_expiry_timestamp_ist",
        "expiry_mapping_status",
        "contract_rule_status",
        "contract_lot_size",
        "time_to_expiry_status",
        "t_years_act365",
        "bsm_gate_status",
        "bsm_gate_failure_reason",
        "quality_severe_anomaly",
        "proven_severe_payload_corruption",
        "quality_patch_version",
    }
    missing = sorted(required - set(columns))
    if missing:
        raise ValueError(f"pre-BSM-v2 input is missing required columns: {missing}")


def _float_column(table: Any, name: str) -> np.ndarray:
    import pyarrow.compute as pc

    return pc.cast(table[name], "double").to_numpy(zero_copy_only=False)


def _text_membership(table: Any, name: str, values: set[str]) -> np.ndarray:
    import pyarrow as pa
    import pyarrow.compute as pc

    upper = pc.utf8_upper(pc.cast(table[name], pa.string()))
    wanted = pa.array(sorted(value.upper() for value in values), type=pa.string())
    return pc.fill_null(pc.is_in(upper, value_set=wanted), False).to_numpy(zero_copy_only=False)


def _bool_column(table: Any, name: str) -> np.ndarray:
    import pyarrow.compute as pc

    return pc.fill_null(pc.cast(table[name], "bool"), False).to_numpy(zero_copy_only=False)


def _set_first_failure(target: np.ndarray, mask: np.ndarray, reason: str) -> None:
    selected = mask & (target == None)  # noqa: E711
    target[selected] = reason


def _provider_iv_delta(table: Any, solved_iv: np.ndarray) -> np.ndarray:
    delta = np.full(table.num_rows, np.nan)
    if "provider_iv_raw" not in table.column_names or "provider_iv_unit" not in table.column_names:
        return delta
    declared_decimal = _text_membership(table, "provider_iv_unit", {"decimal"})
    provider = _float_column(table, "provider_iv_raw")
    valid = declared_decimal & np.isfinite(provider) & np.isfinite(solved_iv)
    delta[valid] = solved_iv[valid] - provider[valid]
    return delta


def _arrow_array(pa: Any, values: np.ndarray) -> Any:
    if values.dtype.kind == "f":
        return pa.array(values, mask=~np.isfinite(values), type=pa.float64())
    if values.dtype.kind == "b":
        return pa.array(values, type=pa.bool_())
    if values.dtype.kind in {"i", "u"}:
        return pa.array(values)
    return pa.array(values, type=pa.string())


def _adjacent_duplicate_count(table: Any, primary_key: Sequence[str]) -> int:
    import pyarrow.compute as pc

    if table.num_rows < 2:
        return 0
    same = None
    for name in primary_key:
        equal = pc.fill_null(
            pc.equal(table[name].slice(1), table[name].slice(0, table.num_rows - 1)),
            False,
        )
        same = equal if same is None else pc.and_(same, equal)
    return int(pc.sum(pc.cast(same, "int64")).as_py() or 0)


def _quantiles(values: np.ndarray) -> dict[str, float | None]:
    if values.size == 0:
        return {"p50": None, "p95": None, "p99": None, "max": None}
    return {
        "p50": float(np.quantile(values, 0.50)),
        "p95": float(np.quantile(values, 0.95)),
        "p99": float(np.quantile(values, 0.99)),
        "max": float(np.max(values)),
    }


def _input_paths(source: str | Path | Sequence[str | Path]) -> list[Path]:
    items = [source] if isinstance(source, (str, Path)) else list(source)
    paths: list[Path] = []
    for item in items:
        path = Path(item)
        if path.is_dir():
            paths.extend(path.rglob("*.parquet"))
        elif path.is_file() and path.suffix.lower() in {".parquet", ".pq"}:
            paths.append(path)
        else:
            raise FileNotFoundError(path)
    return sorted(set(paths), key=lambda path: str(path.resolve()).lower())


def _discover_month_sources(root: Path) -> dict[str, list[Path]]:
    if not root.is_dir():
        raise FileNotFoundError(root)
    month_pattern = re.compile(r"year=(\d{4})[\\/]month=(\d{2})(?:[\\/]|$)")
    grouped: dict[str, list[Path]] = {}
    # Audit, duplicate, and source-exception Parquets live beside the canonical
    # publication. They are evidence, never BSM inputs.
    for path in root.rglob("pre_bsm.parquet"):
        normalized = str(path.resolve())
        if any(part in {".staging", "exceptions"} for part in path.parts):
            continue
        match = month_pattern.search(normalized)
        if match is None:
            continue
        month = f"{match.group(1)}-{match.group(2)}"
        grouped.setdefault(month, []).append(path)
    for paths in grouped.values():
        paths.sort(key=lambda path: str(path.resolve()).lower())
    return grouped


def _verify_pre_bsm_acceptance(input_paths: Sequence[Path], month: str) -> dict[str, Any]:
    """Require the producing pre-BSM month audit before consuming its data."""
    if len(input_paths) != 1 or input_paths[0].name != "pre_bsm.parquet":
        raise ValueError(
            "BSM v2 requires exactly one canonical pre_bsm.parquet per month; "
            "audit/exception Parquets are not admissible inputs"
        )
    data_path = input_paths[0].resolve()
    match = re.search(r"year=(\d{4})[\\/]month=(\d{2})[\\/]pre_bsm\.parquet$", str(data_path))
    if match is None or f"{match.group(1)}-{match.group(2)}" != month:
        raise ValueError(f"canonical pre-BSM path/month mismatch for {data_path}")
    version_root = data_path.parents[2]
    manifest_path = version_root / "manifests" / f"month={month}.json"
    terminal_audit_path = version_root / "manifests" / "quality_patch_terminal_audit.json"
    if not manifest_path.is_file():
        raise ValueError(f"pre-BSM acceptance manifest is missing: {manifest_path}")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        audit = manifest["audit"]
        config = manifest["config"]
        artifacts = manifest["artifacts"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid pre-BSM acceptance manifest: {manifest_path}") from exc
    failures: list[str] = []
    if manifest.get("status") != "published":
        failures.append("status_not_published")
    if manifest.get("patch_version") != REQUIRED_PRE_BSM_PATCH_VERSION:
        failures.append("quality_patch_version_missing_or_wrong")
    if manifest.get("audit_schema") != REQUIRED_PRE_BSM_QUALITY_SCHEMA:
        failures.append("quality_audit_schema_missing_or_wrong")
    if manifest.get("audit_schema_version") != REQUIRED_PRE_BSM_QUALITY_SCHEMA_VERSION:
        failures.append("quality_audit_schema_version_missing_or_wrong")
    if manifest.get("month") != month:
        failures.append("manifest_month_mismatch")
    if config.get("acquisition_terminally_accounted") is not True:
        failures.append("acquisition_not_terminally_accounted")
    input_rows = int(audit.get("input_rows", -1))
    output_rows = int(audit.get("output_rows", -2))
    if input_rows < 0 or input_rows != output_rows:
        failures.append("row_conservation_failed")
    if int(audit.get("canonical_regular_rows", -1)) + int(
        audit.get("source_exception_rows", -1)
    ) != input_rows:
        failures.append("canonical_exception_conservation_failed")
    for field in (
        "future_join_violations",
        "asof_tolerance_violations",
        "primary_key_duplicate_groups",
        "primary_key_duplicate_excess_rows",
    ):
        if int(audit.get(field, -1)) != 0:
            failures.append(f"{field}_nonzero_or_missing")
    if int(audit.get("parquet_metadata_rows", -1)) != output_rows:
        failures.append("parquet_metadata_row_count_mismatch")
    if int(audit.get("orphan_partial_count", -1)) != 0:
        failures.append("pre_bsm_orphan_partials_nonzero_or_missing")
    if int(audit.get("severe_anomaly_eligible_rows", -1)) != 0:
        failures.append("severe_anomaly_eligible_rows_nonzero_or_missing")
    if int(audit.get("proven_severe_payload_rows", -1)) < 0:
        failures.append("proven_severe_payload_audit_missing")
    pre_bsm_artifacts = [
        artifact for artifact in artifacts if Path(str(artifact.get("path", ""))).name == "pre_bsm.parquet"
    ]
    if len(pre_bsm_artifacts) != 1:
        failures.append("canonical_artifact_missing_or_ambiguous")
    else:
        artifact = pre_bsm_artifacts[0]
        declared_path = Path(str(artifact.get("path", ""))).resolve()
        if declared_path != data_path:
            failures.append("canonical_artifact_path_mismatch")
        if artifact.get("sha256") != sha256_file(data_path):
            failures.append("canonical_artifact_hash_mismatch")
        if int(artifact.get("row_count", -1)) != output_rows:
            failures.append("canonical_artifact_row_count_mismatch")
    try:
        terminal = json.loads(terminal_audit_path.read_text(encoding="utf-8"))
        if terminal.get("status") != "PASS":
            failures.append("quality_terminal_audit_not_pass")
        if terminal.get("schema") != "dhan_pre_bsm_quality_patch_terminal_audit":
            failures.append("quality_terminal_audit_schema_wrong")
        if terminal.get("schema_version") != REQUIRED_PRE_BSM_QUALITY_SCHEMA_VERSION:
            failures.append("quality_terminal_audit_version_wrong")
        if terminal.get("patch_version") != REQUIRED_PRE_BSM_PATCH_VERSION:
            failures.append("quality_terminal_patch_version_wrong")
        if int(terminal.get("months", -1)) != REQUIRED_PRE_BSM_MONTHS:
            failures.append("quality_terminal_month_count_wrong")
        if int(terminal.get("expected_rows", -1)) != REQUIRED_PRE_BSM_ROWS:
            failures.append("quality_terminal_row_contract_wrong")
        if terminal.get("bsm_launch_authorized") is not True:
            failures.append("quality_terminal_bsm_not_authorized")
        if terminal.get("manifest_sha256_by_month", {}).get(month) != sha256_file(manifest_path):
            failures.append("quality_terminal_manifest_lineage_mismatch")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        failures.append("quality_terminal_audit_missing_or_invalid")
    if failures:
        raise ValueError(f"pre-BSM acceptance failed for {month}: {sorted(set(failures))}")
    return {
        "manifest_path": str(manifest_path.resolve()),
        "manifest_sha256": sha256_file(manifest_path),
        "status": "accepted",
        "input_rows": input_rows,
        "output_rows": output_rows,
        "acquisition_terminally_accounted": True,
        "future_join_violations": 0,
        "asof_tolerance_violations": 0,
        "primary_key_duplicate_groups": 0,
        "primary_key_duplicate_excess_rows": 0,
        "quality_patch_version": REQUIRED_PRE_BSM_PATCH_VERSION,
        "quality_audit_schema_version": REQUIRED_PRE_BSM_QUALITY_SCHEMA_VERSION,
        "quality_terminal_audit_path": str(terminal_audit_path.resolve()),
        "quality_terminal_audit_sha256": sha256_file(terminal_audit_path),
    }


def _resume_result(manifest_path: Path, output_path: Path, lineage_sha: str) -> BsmMonthResult | None:
    if not manifest_path.is_file() or not output_path.is_file():
        return None
    try:
        import pyarrow.parquet as pq

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("lineage_sha256") != lineage_sha:
            return None
        if manifest.get("output_sha256") != sha256_file(output_path):
            return None
        rows = pq.ParquetFile(output_path).metadata.num_rows
        if rows != manifest.get("output_rows") or rows != manifest.get("input_rows"):
            return None
        return BsmMonthResult(
            month=str(manifest["month"]),
            input_rows=rows,
            output_rows=rows,
            output_path=str(output_path),
            output_sha256=str(manifest["output_sha256"]),
            manifest_path=str(manifest_path),
            manifest_sha256=sha256_file(manifest_path),
            resumed=True,
            status_counts={str(k): int(v) for k, v in manifest["status_counts"].items()},
        )
    except (KeyError, OSError, ValueError):
        return None


def _quarantine_stale_publication(root: Path, month: str, output_path: Path, manifest_path: Path) -> None:
    if not output_path.exists() and not manifest_path.exists():
        return
    quarantine = root / "exceptions" / "stale_or_corrupt" / month
    quarantine.mkdir(parents=True, exist_ok=True)
    suffix = uuid.uuid4().hex
    if output_path.exists():
        os.replace(output_path, quarantine / f"part-000.{suffix}.parquet")
    if manifest_path.exists():
        os.replace(manifest_path, quarantine / f"manifest.{suffix}.json")


def _atomic_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(partial, path)
    finally:
        partial.unlink(missing_ok=True)
    return sha256_file(path)


def _atomic_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_retry(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _status_markdown(status: dict[str, Any]) -> str:
    eta = status["eta_seconds"]
    eta_text = "n/a" if eta is None else f"{float(eta):.1f} seconds"
    return (
        "# Vectorized BSM v2 status\n\n"
        f"- State: `{status['state']}`\n"
        f"- Current month: `{status['current_month']}`\n"
        f"- Months: {status['months_completed']}/{status['months_total']}\n"
        f"- Rows: {status['rows_completed']:,}\n"
        f"- Rate: {status['rows_per_second']:,.1f} rows/second\n"
        f"- ETA: {eta_text}\n"
        f"- Status counts: `{json.dumps(status['status_counts'], sort_keys=True)}`\n"
        f"- Solver methods: `{json.dumps(status['solver_method_counts'], sort_keys=True)}`\n"
        f"- Solver metrics: `{json.dumps(status['solver_metrics'], sort_keys=True)}`\n"
    )


def _replace_with_retry(source: Path, target: Path) -> None:
    for attempt in range(10):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(min(0.05 * (2**attempt), 1.0))


def _fsync_file(path: Path) -> None:
    # Windows' CRT rejects fsync on a read-only descriptor; opening without
    # modifying the already-closed Parquet writer gives us a flushable handle.
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_sha(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _valid_month(value: str) -> bool:
    try:
        parsed = datetime.strptime(value, "%Y-%m")
    except ValueError:
        return False
    return parsed.strftime("%Y-%m") == value
