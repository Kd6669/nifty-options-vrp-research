"""Versioned, auditable pre-BSM enrichment for Dhan rolling options.

This module intentionally does not import or execute :mod:`bsm`.  Its output is
the mandatory boundary between acquired option candles and pricing: independent
NIFTY spot and INDIA VIX are joined without look-ahead, while actual expiry and
contract terms must resolve from supplied authoritative NSE dimensions.
"""

from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
import uuid
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
ENRICHMENT_VERSION = "1.0.0"
REGULAR_SESSION = "regular_session"
MATCHED = "matched"
BLOCKED = "blocked"
READY = "READY"

_REQUIRED_ENRICHED = (
    "independent_nifty_spot",
    "india_vix",
    "actual_expiry_date",
    "actual_expiry_timestamp_ist",
    "contract_lot_size",
    "mte",
    "dte",
    "t_years_act365",
)
_DEFAULT_PRIMARY_KEY = (
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
class EnrichmentBatch:
    """In-memory result of the mandatory pre-BSM stage."""

    rows: tuple[dict[str, Any], ...]
    exceptions: tuple[dict[str, Any], ...]
    duplicate_right_rows: tuple[dict[str, Any], ...]
    coverage: Mapping[str, Any]
    bsm_gate_status: str
    bsm_gate_reasons: tuple[str, ...]
    version: str = ENRICHMENT_VERSION


@dataclass(frozen=True)
class EnrichmentWriteResult:
    root: str
    manifest_path: str
    manifest_sha256: str
    manifest_hash_path: str
    parquet_paths: tuple[str, ...]
    exception_paths: tuple[str, ...]
    bsm_gate_status: str


@dataclass(frozen=True)
class _RightIndex:
    by_session: Mapping[tuple[str, str], tuple[tuple[datetime, Mapping[str, Any]], ...]]
    duplicate_keys: frozenset[tuple[str, str, datetime]]
    quarantined: tuple[dict[str, Any], ...]


def load_dimension_rows(source: Any) -> list[dict[str, Any]]:
    """Load dimension rows from mappings, iterables, JSON/JSONL, or Parquet.

    A mapping may be one row, contain a ``rows`` list, or map arbitrary keys to
    row mappings.  This keeps acquisition/research responsible for provenance
    while giving the enrichment boundary one deterministic input representation.
    """
    if source is None:
        return []
    if isinstance(source, (str, Path)):
        path = Path(source)
        suffix = path.suffix.lower()
        if suffix in {".parquet", ".pq"}:
            import pyarrow.parquet as pq

            return [dict(row) for row in pq.read_table(path).to_pylist()]
        if suffix == ".jsonl":
            return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        payload = json.loads(path.read_text(encoding="utf-8"))
        return load_dimension_rows(payload)
    if isinstance(source, Mapping):
        if "rows" in source and isinstance(source["rows"], Sequence):
            return [dict(row) for row in source["rows"]]
        if "rules" in source and isinstance(source["rules"], Sequence):
            return [dict(row) for row in source["rules"]]
        if source and all(isinstance(value, Mapping) for value in source.values()):
            rows = []
            for key, value in source.items():
                row = dict(value)
                # Support the established rolling-expiry evidence key without
                # requiring callers to duplicate it inside every mapping.
                if isinstance(key, tuple) and len(key) == 3:
                    row.setdefault("trade_date", key[0])
                    row.setdefault("expiry_flag", key[1])
                    row.setdefault("expiry_code", key[2])
                elif isinstance(key, str):
                    row.setdefault("dimension_key", key)
                rows.append(row)
            return rows
        return [dict(source)]
    return [dict(row) for row in source]


def enrich_options_pre_bsm(
    option_rows: Iterable[Mapping[str, Any]],
    spot_rows: Iterable[Mapping[str, Any]],
    vix_rows: Iterable[Mapping[str, Any]],
    contract_rules: Any,
    actual_expiries: Any,
    *,
    tolerance_seconds: float = 60.0,
    acquisition_terminally_accounted: bool = False,
) -> EnrichmentBatch:
    """Enrich options without performing any BSM calculation.

    Exact right-side timestamps win; otherwise only the latest observation at
    most ``tolerance_seconds`` backward in the same IST trade date and explicit
    session is eligible.  Non-regular option rows are retained as exceptions.
    """
    if not 0 <= tolerance_seconds <= 60:
        raise ValueError("tolerance_seconds must be between 0 and 60")

    spot_index = _build_right_index(spot_rows, source_name="nifty_spot")
    vix_index = _build_right_index(vix_rows, source_name="india_vix")
    expiry_rows = load_dimension_rows(actual_expiries)
    rule_rows = load_dimension_rows(contract_rules)
    canonical: list[dict[str, Any]] = []
    exceptions: list[dict[str, Any]] = []

    for source in option_rows:
        row = dict(source)
        row["enrichment_version"] = ENRICHMENT_VERSION
        row["provider_spot"] = source.get("provider_spot")
        if row.get("session_status") != REGULAR_SESSION:
            row.update(_empty_join("nifty_spot", "outside_regular_session"))
            row.update(_empty_join("india_vix", "outside_regular_session"))
            row.update(_empty_contract("outside_regular_session"))
            row.update(_blocked_time("outside_regular_session"))
            row["canonical_bsm_population"] = False
            row["bsm_gate_status"] = BLOCKED
            row["bsm_gate_failure_reason"] = "outside_regular_session"
            row["enrichment_exception"] = "outside_regular_session"
            exceptions.append(row)
            continue

        row["canonical_bsm_population"] = True
        timestamp, timestamp_error = _validated_row_timestamp(row)
        if timestamp_error:
            row.update(_empty_join("nifty_spot", timestamp_error))
            row.update(_empty_join("india_vix", timestamp_error))
        else:
            assert timestamp is not None
            row.update(_join_one(row, timestamp, spot_index, "close", "nifty_spot", tolerance_seconds))
            row.update(_join_one(row, timestamp, vix_index, "close", "india_vix", tolerance_seconds))

        expiry, expiry_status = _select_expiry(row, expiry_rows)
        if expiry is None:
            row.update(_empty_contract(expiry_status))
            row.update(_blocked_time(expiry_status))
        else:
            _apply_expiry(row, expiry)
            rule, rule_status = _select_rule(row, expiry, rule_rows)
            if rule is None:
                row.update(_empty_rule(rule_status))
            else:
                _apply_rule(row, rule)
            _apply_time_to_expiry(row, timestamp)

        reasons = _row_gate_reasons(row)
        row["bsm_gate_status"] = READY if not reasons else BLOCKED
        row["bsm_gate_failure_reason"] = None if not reasons else ";".join(reasons)
        canonical.append(row)

    duplicate_rows = spot_index.quarantined + vix_index.quarantined
    coverage = _coverage(canonical, exceptions, duplicate_rows)
    gate_reasons: list[str] = []
    if not acquisition_terminally_accounted:
        gate_reasons.append("options_acquisition_not_terminally_accounted")
    if any(row["bsm_gate_status"] != READY for row in canonical):
        gate_reasons.append("canonical_rows_blocked")
    if duplicate_rows:
        gate_reasons.append("duplicate_right_timestamps_quarantined")
    status = READY if not gate_reasons else BLOCKED
    return EnrichmentBatch(
        tuple(canonical),
        tuple(exceptions),
        tuple(duplicate_rows),
        coverage,
        status,
        tuple(gate_reasons),
    )


def write_enriched_partitions(
    batch: EnrichmentBatch,
    output_root: str | Path,
    *,
    primary_key: Sequence[str] = _DEFAULT_PRIMARY_KEY,
    part_id: str = "00000",
    manifest_id: str = "enrichment_manifest",
    input_lineage: Mapping[str, Any] | None = None,
) -> EnrichmentWriteResult:
    """Atomically write versioned Parquet partitions and their audit manifest."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    part_id = _safe_artifact_id(part_id)
    manifest_id = _safe_artifact_id(manifest_id)
    root = Path(output_root).resolve() / "enriched_options" / f"version={batch.version}"
    partitions: list[dict[str, Any]] = []
    parquet_paths: list[str] = []
    for trade_date, rows in sorted(_partition_rows(batch.rows).items()):
        path = root / f"trade_date={trade_date}" / f"part-{part_id}.parquet"
        # ``trade_date`` is represented by the Hive partition directory.  Do
        # not duplicate it physically with a conflicting Arrow inference.
        physical_rows = [{key: value for key, value in row.items() if key != "trade_date"} for row in rows]
        _atomic_parquet(path, physical_rows, pa=pa, pq=pq, metadata={"layer": "pre_bsm_enriched"})
        digest = sha256_file(path)
        physical_row_count = pq.ParquetFile(path).metadata.num_rows
        keys = [_row_key(row, primary_key) for row in rows]
        partitions.append(
            {
                "trade_date": trade_date,
                "path": str(path),
                "sha256": digest,
                "row_count": len(rows),
                "parquet_row_count": physical_row_count,
                "cardinality_check": "passed" if physical_row_count == len(rows) else "failed",
                "unique_primary_key_count": len(set(keys)),
                "duplicate_primary_key_count": len(keys) - len(set(keys)),
                "null_coverage": _null_coverage(rows),
            }
        )
        parquet_paths.append(str(path))

    exception_paths: list[str] = []
    exception_artifacts: list[dict[str, Any]] = []
    for name, rows in (
        ("option_source_exceptions", batch.exceptions),
        ("duplicate_right_timestamps", batch.duplicate_right_rows),
    ):
        if not rows:
            continue
        path = root / "exceptions" / f"{name}-{part_id}.parquet"
        _atomic_parquet(path, rows, pa=pa, pq=pq, metadata={"exception_type": name})
        physical_row_count = pq.ParquetFile(path).metadata.num_rows
        artifact = {
            "type": name,
            "path": str(path),
            "sha256": sha256_file(path),
            "row_count": len(rows),
            "parquet_row_count": physical_row_count,
            "cardinality_check": "passed" if physical_row_count == len(rows) else "failed",
        }
        exception_artifacts.append(artifact)
        exception_paths.append(str(path))

    total_rows = sum(item["row_count"] for item in partitions)
    total_unique = sum(item["unique_primary_key_count"] for item in partitions)
    gate_reasons = list(batch.bsm_gate_reasons)
    if total_rows != len(batch.rows):
        gate_reasons.append("parquet_manifest_row_count_mismatch")
    if total_unique != total_rows:
        gate_reasons.append("duplicate_enriched_primary_keys")
    if any(item["cardinality_check"] != "passed" for item in partitions + exception_artifacts):
        gate_reasons.append("parquet_cardinality_check_failed")
    gate_status = READY if not gate_reasons else BLOCKED
    manifest = {
        "manifest_version": "1.0.0",
        "enrichment_version": batch.version,
        "created_at_utc": datetime.now(tz=ZoneInfo("UTC")).isoformat(),
        "layer": "mandatory_pre_bsm",
        "bsm_executed": False,
        "bsm_gate": {"status": gate_status, "reasons": sorted(set(gate_reasons))},
        "canonical_row_count": len(batch.rows),
        "exception_row_count": len(batch.exceptions),
        "duplicate_right_row_count": len(batch.duplicate_right_rows),
        "input_lineage": dict(input_lineage or {}),
        "coverage": dict(batch.coverage),
        "primary_key": list(primary_key),
        "partitions": partitions,
        "exception_artifacts": exception_artifacts,
    }
    manifest_path = root / "manifests" / f"{manifest_id}.json"
    _atomic_json(manifest_path, manifest)
    manifest_digest = sha256_file(manifest_path)
    manifest_hash_path = manifest_path.with_suffix(manifest_path.suffix + ".sha256")
    _atomic_text(manifest_hash_path, f"{manifest_digest}  {manifest_path.name}\n")
    return EnrichmentWriteResult(
        str(root),
        str(manifest_path),
        manifest_digest,
        str(manifest_hash_path),
        tuple(parquet_paths),
        tuple(exception_paths),
        gate_status,
    )


def sha256_file(path: str | Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_right_index(rows: Iterable[Mapping[str, Any]], *, source_name: str) -> _RightIndex:
    staged: dict[tuple[str, str, datetime], list[Mapping[str, Any]]] = {}
    quarantined: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        timestamp, error = _validated_row_timestamp(row)
        session = str(row.get("session_status") or "")
        if error or not session:
            quarantined.append({**row, "right_source": source_name, "quarantine_reason": error or "missing_session_status"})
            continue
        assert timestamp is not None
        key = (_date_text(row["trade_date"]), session, timestamp)
        staged.setdefault(key, []).append(row)
    duplicate_keys = frozenset(key for key, values in staged.items() if len(values) > 1)
    by_session: dict[tuple[str, str], list[tuple[datetime, Mapping[str, Any]]]] = {}
    for key, values in staged.items():
        trade_date, session, timestamp = key
        if key in duplicate_keys:
            for row in values:
                quarantined.append({**row, "right_source": source_name, "quarantine_reason": "duplicate_right_timestamp"})
        by_session.setdefault((trade_date, session), []).append((timestamp, values[0]))
    return _RightIndex(
        {key: tuple(sorted(values, key=lambda pair: pair[0])) for key, values in by_session.items()},
        duplicate_keys,
        tuple(quarantined),
    )


def _join_one(
    option: Mapping[str, Any],
    option_ts: datetime,
    index: _RightIndex,
    value_field: str,
    prefix: str,
    tolerance_seconds: float,
) -> dict[str, Any]:
    trade_date = _date_text(option.get("trade_date"))
    session = str(option.get("session_status") or "")
    candidates = index.by_session.get((trade_date, session), ())
    if not candidates:
        return _empty_join(prefix, "no_right_rows_for_trade_date_session")
    timestamps = [pair[0] for pair in candidates]
    position = bisect_right(timestamps, option_ts)
    if position == 0:
        return _empty_join(prefix, "future_only_right_rows")
    right_ts, right = candidates[position - 1]
    age = (option_ts - right_ts).total_seconds()
    fields = _empty_join(prefix, None)
    fields[f"{prefix}_timestamp_ist"] = right_ts.astimezone(IST)
    fields[f"{prefix}_age_seconds"] = age
    if (trade_date, session, right_ts) in index.duplicate_keys:
        fields[f"{prefix}_join_failure_reason"] = "duplicate_right_timestamp"
        return fields
    if age > tolerance_seconds:
        fields[f"{prefix}_join_failure_reason"] = "backward_outside_tolerance"
        return fields
    value = right.get(value_field)
    if value is None or not _finite(value):
        fields[f"{prefix}_join_failure_reason"] = "right_value_missing_or_non_finite"
        return fields
    fields[prefix if prefix == "india_vix" else "independent_nifty_spot"] = float(value)
    fields[f"{prefix}_match_method"] = "exact_timestamp" if age == 0 else "backward_asof"
    fields[f"{prefix}_join_status"] = MATCHED
    fields[f"{prefix}_join_failure_reason"] = None
    return fields


def _empty_join(prefix: str, reason: str | None) -> dict[str, Any]:
    value_name = prefix if prefix == "india_vix" else "independent_nifty_spot"
    return {
        value_name: None,
        f"{prefix}_timestamp_ist": None,
        f"{prefix}_age_seconds": None,
        f"{prefix}_match_method": "none",
        f"{prefix}_join_status": BLOCKED,
        f"{prefix}_join_failure_reason": reason,
    }


def _select_expiry(option: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any] | None, str]:
    trade_date = _date_value(option.get("trade_date"))
    expiry_type = _expiry_type(option)
    try:
        expiry_code = int(option.get("expiry_code"))
    except (TypeError, ValueError):
        return None, "invalid_expiry_code"
    if trade_date is None or expiry_type is None:
        return None, "invalid_expiry_lookup_key"
    candidates = []
    for row in rows:
        if not _dimension_identity_matches(option, row):
            continue
        row_type = _normalized_expiry_type(row.get("expiry_type", row.get("expiry_flag")))
        if row_type != expiry_type or _safe_int(row.get("expiry_code")) != expiry_code:
            continue
        exact_date = _date_value(row.get("trade_date"))
        start = _date_value(row.get("valid_trade_date_from", row.get("effective_from")))
        end = _date_value(row.get("valid_trade_date_to", row.get("effective_to")))
        if exact_date == trade_date or (exact_date is None and _date_in_range(trade_date, start, end)):
            candidates.append(row)
    if not candidates:
        return None, "actual_expiry_no_match"
    if len(candidates) != 1:
        return None, "actual_expiry_ambiguous"
    candidate = candidates[0]
    if str(candidate.get("mapping_status", "proven")).lower() not in {"proven", "verified", "resolved"}:
        return None, "actual_expiry_not_proven"
    if not candidate.get("mapping_confidence") or not candidate.get("source_id"):
        return None, "actual_expiry_provenance_incomplete"
    if not _valid_sha256(candidate.get("source_sha256")):
        return None, "actual_expiry_source_hash_invalid"
    try:
        expiry_date = _date_value(candidate.get("actual_expiry_date"))
        expiry_ts = _aware_datetime(candidate.get("actual_expiry_timestamp_ist"))
    except (TypeError, ValueError):
        return None, "actual_expiry_invalid_timestamp"
    if expiry_date is None or expiry_ts.astimezone(IST).date() != expiry_date:
        return None, "actual_expiry_date_timestamp_conflict"
    return candidate, "resolved"


def _apply_expiry(target: dict[str, Any], expiry: Mapping[str, Any]) -> None:
    expiry_date = _date_value(expiry["actual_expiry_date"])
    expiry_ts = _aware_datetime(expiry["actual_expiry_timestamp_ist"])
    target.update(
        {
            "actual_expiry_date": expiry_date,
            "actual_expiry_timestamp_ist": expiry_ts.astimezone(IST),
            "expiry_type": _normalized_expiry_type(expiry.get("expiry_type", expiry.get("expiry_flag"))),
            "expiry_rule_weekday": expiry.get("expiry_rule_weekday"),
            "expiry_rule_effective_from": _date_value(expiry.get("expiry_rule_effective_from")),
            "expiry_rule_effective_to": _date_value(expiry.get("expiry_rule_effective_to")),
            "expiry_holiday_adjusted": expiry.get("expiry_holiday_adjusted"),
            "original_scheduled_expiry": _date_value(expiry.get("original_scheduled_expiry")),
            "expiry_mapping_status": "resolved",
            "expiry_mapping_confidence": expiry.get("mapping_confidence"),
            "expiry_source_id": expiry.get("source_id"),
            "expiry_circular_id": expiry.get("circular_id"),
            "expiry_source_sha256": expiry.get("source_sha256"),
        }
    )


def _select_rule(
    option: Mapping[str, Any], expiry: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> tuple[Mapping[str, Any] | None, str]:
    expiry_date = _date_value(expiry.get("actual_expiry_date"))
    expiry_type = _normalized_expiry_type(expiry.get("expiry_type", expiry.get("expiry_flag")))
    if expiry_date is None or expiry_type is None:
        return None, "contract_rule_invalid_lookup_key"
    candidates = []
    for row in rows:
        if not _dimension_identity_matches(option, row):
            continue
        row_type = _normalized_expiry_type(row.get("expiry_type", row.get("expiry_flag")))
        if row_type not in (None, expiry_type):
            continue
        exact_expiry = _date_value(row.get("actual_expiry_date"))
        start = _date_value(row.get("contract_expiry_from", row.get("effective_from")))
        end = _date_value(row.get("contract_expiry_to", row.get("effective_to")))
        if exact_expiry == expiry_date or (exact_expiry is None and _date_in_range(expiry_date, start, end)):
            candidates.append(row)
    if not candidates:
        return None, "contract_rule_no_match"
    if len(candidates) != 1:
        return None, "contract_rule_ambiguous"
    candidate = candidates[0]
    if str(candidate.get("mapping_status", "proven")).lower() not in {"proven", "verified", "resolved"}:
        return None, "contract_rule_not_proven"
    if not all(candidate.get(field) for field in ("mapping_confidence", "rule_id", "circular_id", "source_id")):
        return None, "contract_rule_provenance_incomplete"
    if not _valid_sha256(candidate.get("source_sha256")):
        return None, "contract_rule_source_hash_invalid"
    if _date_value(candidate.get("effective_from")) is None:
        return None, "contract_rule_effective_from_missing"
    lot = candidate.get("contract_lot_size", candidate.get("market_lot"))
    if lot is None or not _finite(lot) or float(lot) <= 0:
        return None, "contract_rule_missing_lot_size"
    return candidate, "resolved"


def _apply_rule(target: dict[str, Any], rule: Mapping[str, Any]) -> None:
    lot = rule.get("contract_lot_size", rule.get("market_lot"))
    if target.get("expiry_rule_weekday") is None:
        target["expiry_rule_weekday"] = rule.get("expiry_rule_weekday")
    if target.get("expiry_rule_effective_from") is None:
        target["expiry_rule_effective_from"] = _date_value(
            rule.get("expiry_rule_effective_from", rule.get("effective_from"))
        )
    if target.get("expiry_rule_effective_to") is None:
        target["expiry_rule_effective_to"] = _date_value(
            rule.get("expiry_rule_effective_to", rule.get("effective_to"))
        )
    target.update(
        {
            "contract_lot_size": float(lot),
            "market_lot": float(rule.get("market_lot", lot)),
            "contract_multiplier": _float_or_none(rule.get("contract_multiplier")),
            "trading_unit": rule.get("trading_unit"),
            "tick_size": _float_or_none(rule.get("tick_size")),
            "contract_rule_status": "resolved",
            "contract_rule_mapping_confidence": rule.get("mapping_confidence"),
            "contract_rule_id": rule.get("rule_id"),
            "contract_rule_circular_id": rule.get("circular_id"),
            "contract_rule_source_id": rule.get("source_id"),
            "contract_rule_source_sha256": rule.get("source_sha256"),
            "contract_rule_effective_from": _date_value(rule.get("effective_from")),
            "contract_rule_effective_to": _date_value(rule.get("effective_to")),
            "contract_rule_failure_reason": None,
        }
    )


def _empty_contract(reason: str) -> dict[str, Any]:
    return {
        "actual_expiry_date": None,
        "actual_expiry_timestamp_ist": None,
        "expiry_type": None,
        "expiry_rule_weekday": None,
        "expiry_rule_effective_from": None,
        "expiry_rule_effective_to": None,
        "expiry_holiday_adjusted": None,
        "original_scheduled_expiry": None,
        "expiry_mapping_status": BLOCKED,
        "expiry_mapping_confidence": None,
        "expiry_source_id": None,
        "expiry_circular_id": None,
        "expiry_source_sha256": None,
        **_empty_rule(reason),
    }


def _empty_rule(reason: str) -> dict[str, Any]:
    return {
        "contract_lot_size": None,
        "market_lot": None,
        "contract_multiplier": None,
        "trading_unit": None,
        "tick_size": None,
        "contract_rule_status": BLOCKED,
        "contract_rule_mapping_confidence": None,
        "contract_rule_id": None,
        "contract_rule_circular_id": None,
        "contract_rule_source_id": None,
        "contract_rule_source_sha256": None,
        "contract_rule_effective_from": None,
        "contract_rule_effective_to": None,
        "contract_rule_failure_reason": reason,
    }


def _apply_time_to_expiry(target: dict[str, Any], option_ts: datetime | None) -> None:
    if option_ts is None:
        target.update(_blocked_time("invalid_option_timestamp"))
        return
    try:
        expiry_ts = _aware_datetime(target.get("actual_expiry_timestamp_ist"))
    except (TypeError, ValueError):
        target.update(_blocked_time("actual_expiry_unavailable"))
        return
    minutes = (expiry_ts - option_ts).total_seconds() / 60.0
    if not math.isfinite(minutes) or minutes <= 0:
        target.update(_blocked_time("non_positive_mte"))
        return
    target.update(
        {
            "mte": minutes,
            "dte": minutes / 1440.0,
            "t_years_act365": minutes / (365.0 * 24.0 * 60.0),
            "time_to_expiry_status": "valid",
            "time_to_expiry_failure_reason": None,
        }
    )


def _blocked_time(reason: str) -> dict[str, Any]:
    return {
        "mte": None,
        "dte": None,
        "t_years_act365": None,
        "time_to_expiry_status": BLOCKED,
        "time_to_expiry_failure_reason": reason,
    }


def _row_gate_reasons(row: Mapping[str, Any]) -> list[str]:
    reasons = []
    if row.get("nifty_spot_join_status") != MATCHED:
        reasons.append("nifty_spot_join_unavailable")
    if row.get("india_vix_join_status") != MATCHED:
        reasons.append("india_vix_join_unavailable")
    if row.get("expiry_mapping_status") != "resolved":
        reasons.append(str(row.get("contract_rule_failure_reason") or "actual_expiry_unresolved"))
    if row.get("contract_rule_status") != "resolved":
        reasons.append(str(row.get("contract_rule_failure_reason") or "contract_rule_unresolved"))
    if not row.get("expiry_rule_weekday") or row.get("expiry_rule_effective_from") is None:
        reasons.append("expiry_rule_metadata_incomplete")
    if row.get("time_to_expiry_status") != "valid":
        reasons.append(str(row.get("time_to_expiry_failure_reason") or "time_to_expiry_invalid"))
    return list(dict.fromkeys(reasons))


def _coverage(rows: Sequence[Mapping[str, Any]], exceptions: Sequence[Mapping[str, Any]], duplicates: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    ready = sum(row.get("bsm_gate_status") == READY for row in rows)
    fields = {}
    for field in _REQUIRED_ENRICHED:
        populated = sum(row.get(field) is not None for row in rows)
        fields[field] = {"populated": populated, "null": total - populated, "coverage_ratio": 1.0 if total == 0 else populated / total}
    return {
        "canonical_rows": total,
        "ready_rows": ready,
        "blocked_rows": total - ready,
        "out_of_session_rows": len(exceptions),
        "duplicate_right_rows": len(duplicates),
        "required_field_coverage": fields,
    }


def _validated_row_timestamp(row: Mapping[str, Any]) -> tuple[datetime | None, str | None]:
    try:
        timestamp = _aware_datetime(row.get("timestamp_ist"))
    except (TypeError, ValueError):
        return None, "invalid_timestamp"
    declared = _date_value(row.get("trade_date"))
    if declared is None:
        return None, "invalid_trade_date"
    if timestamp.astimezone(IST).date() != declared:
        return None, "timestamp_trade_date_conflict"
    return timestamp, None


def _dimension_identity_matches(option: Mapping[str, Any], dimension: Mapping[str, Any]) -> bool:
    dimension_underlying = dimension.get("underlying")
    return dimension_underlying in (None, "", option.get("underlying"))


def _expiry_type(row: Mapping[str, Any]) -> str | None:
    return _normalized_expiry_type(row.get("expiry_type", row.get("expiry_flag")))


def _normalized_expiry_type(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    if text in {"WEEK", "WEEKLY"}:
        return "weekly"
    if text in {"MONTH", "MONTHLY"}:
        return "monthly"
    return None


def _date_in_range(value: date, start: date | None, end: date | None) -> bool:
    if start is None and end is None:
        return False
    return (start is None or value >= start) and (end is None or value <= end)


def _aware_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError("timestamp must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _date_text(value: Any) -> str:
    parsed = _date_value(value)
    return "" if parsed is None else parsed.isoformat()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _finite(value: Any) -> bool:
    try:
        return math.isfinite(float(value))
    except (TypeError, ValueError):
        return False


def _valid_sha256(value: Any) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _float_or_none(value: Any) -> float | None:
    return float(value) if value is not None and _finite(value) else None


def _partition_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        trade_date = _date_text(row.get("trade_date")) or "invalid"
        result.setdefault(trade_date, []).append(dict(row))
    return result


def _row_key(row: Mapping[str, Any], fields: Sequence[str]) -> tuple[str, ...]:
    return tuple(str(row.get(field)) for field in fields)


def _safe_artifact_id(value: str) -> str:
    text = str(value).strip()
    if not text or any(character not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for character in text):
        raise ValueError("artifact identifiers may contain only letters, numbers, hyphen, and underscore")
    return text


def _null_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, int | float]]:
    total = len(rows)
    return {
        field: {
            "populated": sum(row.get(field) is not None for row in rows),
            "null": sum(row.get(field) is None for row in rows),
            "coverage_ratio": 1.0 if total == 0 else sum(row.get(field) is not None for row in rows) / total,
        }
        for field in _REQUIRED_ENRICHED
    }


def _atomic_parquet(path: Path, rows: Sequence[Mapping[str, Any]], *, pa: Any, pq: Any, metadata: Mapping[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    table = pa.Table.from_pylist([dict(row) for row in rows])
    table = table.replace_schema_metadata(
        {
            b"enrichment_version": ENRICHMENT_VERSION.encode(),
            **{str(key).encode(): str(value).encode() for key, value in metadata.items()},
        }
    )
    try:
        pq.write_table(table, partial)
        with partial.open("r+b") as handle:
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, default=str)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def _atomic_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
