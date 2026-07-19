"""DuckDB bulk, month-resumable pre-BSM enrichment (schema version 2).

The v2 engine is deliberately additive: it reads immutable silver Parquet and
publishes a separate monthly dataset.  It never imports or executes the BSM
solver.  All large scans, joins, ordering, and Parquet writes stay inside
DuckDB; Python handles only month planning, manifests, hashes, and publication.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Iterable, Mapping, Sequence
import uuid


PRE_BSM_V2_VERSION = "2.0.0"
_MONTH_RE = re.compile(r"year=(\d{4})[\\/]month=(\d{2})")
_PK = (
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
class DuckDbPreBsmConfig:
    threads: int = 6
    memory_limit: str = "9GB"
    row_group_size: int = 250_000
    acquisition_terminally_accounted: bool = False
    version: str = PRE_BSM_V2_VERSION

    def validate(self) -> None:
        if not 1 <= self.threads <= 12:
            raise ValueError("threads must be between 1 and 12")
        if self.row_group_size < 10_000:
            raise ValueError("row_group_size must be at least 10,000")
        if not re.fullmatch(r"\d+(?:\.\d+)?(?:MB|GB)", self.memory_limit.upper()):
            raise ValueError("memory_limit must use DuckDB MB/GB syntax")
        if not re.fullmatch(r"\d+\.\d+\.\d+", self.version):
            raise ValueError("version must be semantic x.y.z")


@dataclass(frozen=True)
class DuckDbPreBsmStats:
    months_planned: int
    months_processed: int
    months_resumed: int
    input_rows: int
    output_rows: int
    canonical_regular_rows: int
    source_exception_rows: int
    ready_rows: int
    blocked_rows: int
    duplicate_right_rows: int
    output_root: str
    status_path: str
    bsm_executed: bool = False


def run_pre_bsm_duckdb(
    *,
    options_root: str | Path,
    spot_root: str | Path,
    vix_root: str | Path,
    contract_rules: str | Path,
    actual_expiries: str | Path,
    output_root: str | Path,
    temp_directory: str | Path,
    config: DuckDbPreBsmConfig | None = None,
    months: Sequence[str] | None = None,
    resume: bool = True,
) -> DuckDbPreBsmStats:
    """Materialize deterministic monthly pre-BSM v2 Parquets.

    ``actual_expiries`` is the exact Dhan expiry-code mapping dimension, keyed
    by underlying/trade_date/expiry_type/expiry_code. ``contract_rules`` is the
    effective-dated contract rule dimension keyed by actual contract expiry.
    """
    import duckdb

    cfg = config or DuckDbPreBsmConfig()
    cfg.validate()
    options_path = Path(options_root).resolve()
    spot_path = Path(spot_root).resolve()
    vix_path = Path(vix_root).resolve()
    rules_path = _required_parquet(contract_rules, "contract rules")
    expiries_path = _required_parquet(actual_expiries, "actual expiries")
    output_path = Path(output_root).resolve() / "enriched_options" / f"version={cfg.version}"
    temp_path = Path(temp_directory).resolve()
    temp_path.mkdir(parents=True, exist_ok=True)
    output_path.mkdir(parents=True, exist_ok=True)
    staging_root = output_path / ".staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    orphan_staging_quarantined = _quarantine_orphan_staging(staging_root, output_path)

    option_months = _month_files(options_path)
    selected = _validate_month_selection(months, option_months)
    spot_months = _month_files(spot_path)
    vix_months = _month_files(vix_path)
    all_vix_files = [item for files in vix_months.values() for item in files]
    vix_available_from = _minimum_trade_date(all_vix_files, duckdb=duckdb)
    dimension_hashes = {
        "actual_expiries_sha256": sha256_file(expiries_path),
        "contract_rules_sha256": sha256_file(rules_path),
    }
    config_payload = asdict(cfg) | {
        "code_sha256": sha256_file(__file__),
        "vix_policy": "contextual_not_bsm_required",
        "vix_source_available_from": vix_available_from.isoformat() if vix_available_from else None,
        "join_tolerance_seconds": 60,
        "join_direction": "backward_only",
        "same_trade_date_and_session": True,
    }
    config_hash = _json_hash(config_payload)
    status_path = output_path / "manifests" / "pre_bsm_v2_status.json"
    started_monotonic = time.monotonic()
    started_at_utc = datetime.now(timezone.utc).isoformat()
    totals = {
        "processed": 0,
        "resumed": 0,
        "input": 0,
        "output": 0,
        "canonical": 0,
        "exceptions": 0,
        "ready": 0,
        "blocked": 0,
        "duplicates": 0,
        "orphan_staging_quarantined": orphan_staging_quarantined,
    }

    for index, month in enumerate(selected, 1):
        lineage = {
            "options": _file_lineage(options_path, option_months[month]),
            "spot": _file_lineage(spot_path, spot_months.get(month, ())),
            "india_vix": _file_lineage(vix_path, vix_months.get(month, ())),
            **dimension_hashes,
        }
        input_fingerprint = _json_hash(lineage)
        year, month_number = month.split("-")
        month_dir = output_path / f"year={year}" / f"month={month_number}"
        manifest_path = output_path / "manifests" / f"month={month}.json"
        existing = (
            _validated_manifest(manifest_path, input_fingerprint, config_hash)
            if resume
            else None
        )
        if existing is not None:
            totals["resumed"] += 1
            _accumulate(totals, existing["audit"])
            _write_status(
                status_path, selected, index, totals, cfg, state="running", current_month=month,
                started_monotonic=started_monotonic, started_at_utc=started_at_utc, output_path=output_path,
            )
            continue

        _quarantine_invalid_publication(month_dir, manifest_path, output_path, month)

        staging = staging_root / f"month={month}.{uuid.uuid4().hex}"
        staging.mkdir(parents=True)
        try:
            _write_status(
                status_path, selected, index - 1, totals, cfg, state="running", current_month=month,
                started_monotonic=started_monotonic, started_at_utc=started_at_utc, output_path=output_path,
            )
            audit, artifacts = _process_month(
                option_files=option_months[month],
                spot_files=spot_months.get(month, ()),
                vix_files=vix_months.get(month, ()),
                contract_rules=rules_path,
                actual_expiries=expiries_path,
                staging=staging,
                temp_directory=temp_path,
                cfg=cfg,
                vix_available_from=vix_available_from,
                duckdb=duckdb,
            )
            if audit["input_rows"] != audit["output_rows"]:
                raise RuntimeError(f"join multiplication/row loss in {month}: {audit}")
            if audit["canonical_regular_rows"] + audit["source_exception_rows"] != audit["input_rows"]:
                raise RuntimeError(f"canonical/source-exception conservation failed in {month}")
            if audit["future_join_violations"] or audit["asof_tolerance_violations"]:
                raise RuntimeError(f"point-in-time join violation in {month}: {audit}")
            if audit["parquet_metadata_rows"] != audit["output_rows"]:
                raise RuntimeError(f"Parquet metadata row mismatch in {month}")

            month_dir.mkdir(parents=True, exist_ok=True)
            published = []
            for artifact in artifacts:
                source = staging / artifact["name"]
                target = month_dir / artifact["name"]
                os.replace(source, target)
                published.append(
                    {
                        "path": str(target),
                        "bytes": target.stat().st_size,
                        "sha256": sha256_file(target),
                        "row_count": artifact["row_count"],
                    }
                )
            manifest = {
                "manifest_version": "2.0.0",
                "status": "published",
                "month": month,
                "published_at_utc": datetime.now(timezone.utc).isoformat(),
                "bsm_executed": False,
                "input_fingerprint": input_fingerprint,
                "config_sha256": config_hash,
                "config": config_payload,
                "input_lineage": lineage,
                "artifacts": published,
                "audit": audit,
            }
            _atomic_json(manifest_path, manifest)
            totals["processed"] += 1
            _accumulate(totals, audit)
            _write_status(
                status_path, selected, index, totals, cfg, state="running", current_month=month,
                started_monotonic=started_monotonic, started_at_utc=started_at_utc, output_path=output_path,
            )
        except Exception as exc:
            _write_status(
                status_path, selected, index, totals, cfg, state="failed", current_month=month,
                started_monotonic=started_monotonic, started_at_utc=started_at_utc,
                output_path=output_path, last_error=f"{type(exc).__name__}: {exc}",
            )
            raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)

    _write_status(
        status_path, selected, len(selected), totals, cfg, state="complete",
        current_month=selected[-1], started_monotonic=started_monotonic,
        started_at_utc=started_at_utc, output_path=output_path,
    )
    return DuckDbPreBsmStats(
        months_planned=len(selected),
        months_processed=totals["processed"],
        months_resumed=totals["resumed"],
        input_rows=totals["input"],
        output_rows=totals["output"],
        canonical_regular_rows=totals["canonical"],
        source_exception_rows=totals["exceptions"],
        ready_rows=totals["ready"],
        blocked_rows=totals["blocked"],
        duplicate_right_rows=totals["duplicates"],
        output_root=str(output_path),
        status_path=str(status_path),
    )


def _process_month(
    *,
    option_files: Sequence[Path],
    spot_files: Sequence[Path],
    vix_files: Sequence[Path],
    contract_rules: Path,
    actual_expiries: Path,
    staging: Path,
    temp_directory: Path,
    cfg: DuckDbPreBsmConfig,
    vix_available_from: date | None,
    duckdb: Any,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    database = staging / "work.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(f"SET threads={cfg.threads}")
        connection.execute(f"SET memory_limit={_sql_literal(cfg.memory_limit.upper())}")
        connection.execute(f"SET temp_directory={_sql_literal(str(temp_directory))}")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET TimeZone='Asia/Kolkata'")
        connection.from_parquet([str(path) for path in option_files], union_by_name=True, filename=True).create_view(
            "option_raw", replace=True
        )
        _right_view(connection, "spot_raw", spot_files)
        _right_view(connection, "vix_raw", vix_files)
        connection.from_parquet(str(actual_expiries), union_by_name=True).create_view("expiry_raw", replace=True)
        connection.from_parquet(str(contract_rules), union_by_name=True).create_view("rule_raw", replace=True)
        connection.execute(_DIMENSION_SQL)
        connection.execute(_RIGHT_SQL)
        connection.execute(
            _ENRICH_SQL.format(
                version=_sql_literal(cfg.version),
                terminal="TRUE" if cfg.acquisition_terminally_accounted else "FALSE",
                vix_available_from=(
                    f"DATE {_sql_literal(vix_available_from.isoformat())}" if vix_available_from else "NULL::DATE"
                ),
            )
        )

        duplicate_path = staging / "right_duplicate_conflicts.parquet"
        source_exception_path = staging / "source_exceptions.parquet"
        pk_path = staging / "primary_key_duplicates.parquet"
        data_path = staging / "pre_bsm.parquet"
        _copy_query(connection, "SELECT * FROM right_duplicate_conflicts ORDER BY dataset, trade_date, session_status, timestamp_ist", duplicate_path, cfg)
        _copy_query(connection, "SELECT * FROM enriched WHERE NOT canonical_bsm_population ORDER BY " + ", ".join(_PK) + ", request_id", source_exception_path, cfg)
        _copy_query(connection, "SELECT " + ", ".join(_PK) + ", count(*) AS duplicate_count FROM enriched GROUP BY ALL HAVING count(*) > 1 ORDER BY " + ", ".join(_PK), pk_path, cfg)
        _copy_query(connection, "SELECT * FROM enriched ORDER BY " + ", ".join(_PK) + ", request_id", data_path, cfg)
        for path in (duplicate_path, source_exception_path, pk_path, data_path):
            _fsync_file(path)

        # DuckDB returns a row tuple; use cursor description for stable names.
        cursor = connection.execute(
            """
            SELECT
              count(*)::BIGINT AS output_rows,
              count(*) FILTER (canonical_bsm_population)::BIGINT AS canonical_regular_rows,
              count(*) FILTER (NOT canonical_bsm_population)::BIGINT AS source_exception_rows,
              count(*) FILTER (bsm_gate_status = 'READY')::BIGINT AS ready_rows,
              count(*) FILTER (bsm_gate_status <> 'READY')::BIGINT AS blocked_rows,
              count(*) FILTER (nifty_spot_join_status = 'MATCHED')::BIGINT AS spot_matched_rows,
              count(*) FILTER (india_vix_join_status = 'MATCHED')::BIGINT AS vix_matched_rows,
              count(*) FILTER (india_vix_join_status = 'source_unavailable')::BIGINT AS vix_source_unavailable_rows,
              count(*) FILTER (expiry_mapping_status = 'resolved')::BIGINT AS expiry_resolved_rows,
              count(*) FILTER (contract_rule_status = 'resolved')::BIGINT AS rule_resolved_rows,
              count(*) FILTER (time_to_expiry_status = 'valid')::BIGINT AS positive_mte_rows,
              count(*) FILTER (nifty_spot_timestamp_ist > timestamp_ist OR india_vix_timestamp_ist > timestamp_ist)::BIGINT AS future_join_violations,
              count(*) FILTER (
                (nifty_spot_join_status = 'MATCHED' AND nifty_spot_age_seconds > 60)
                OR (india_vix_join_status = 'MATCHED' AND india_vix_age_seconds > 60)
              )::BIGINT AS asof_tolerance_violations
            FROM enriched
            """
        )
        values = cursor.fetchone()
        audit = {column[0]: value for column, value in zip(cursor.description, values, strict=True)}
        audit.update(
            {
                "input_rows": connection.execute("SELECT count(*) FROM option_raw").fetchone()[0],
                "duplicate_right_rows": connection.execute("SELECT count(*) FROM right_duplicate_conflicts").fetchone()[0],
                "primary_key_duplicate_groups": connection.execute("SELECT count(*) FROM read_parquet(?)", [str(pk_path)]).fetchone()[0],
                "primary_key_duplicate_excess_rows": connection.execute(
                    "SELECT coalesce(sum(duplicate_count - 1), 0)::BIGINT FROM read_parquet(?)", [str(pk_path)]
                ).fetchone()[0],
                "parquet_metadata_rows": connection.execute("SELECT num_rows FROM parquet_file_metadata(?)", [str(data_path)]).fetchone()[0],
                "orphan_partial_count": 0,
                "deterministic_order": True,
                "vix_policy": "contextual_not_bsm_required",
            }
        )
        artifacts = [
            {"name": path.name, "row_count": connection.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]}
            for path in (data_path, duplicate_path, source_exception_path, pk_path)
        ]
        return audit, artifacts
    finally:
        connection.close()
        database.unlink(missing_ok=True)
        for wal in staging.glob("work.duckdb*"):
            wal.unlink(missing_ok=True)


def _right_view(connection: Any, name: str, files: Sequence[Path]) -> None:
    if files:
        connection.from_parquet([str(path) for path in files], union_by_name=True, filename=True).create_view(
            name, replace=True
        )
        return
    connection.execute(
        f"""CREATE OR REPLACE TEMP TABLE {name} (
        request_id VARCHAR, provider VARCHAR, timestamp_ist TIMESTAMPTZ,
        trade_date DATE, session_status VARCHAR, underlying VARCHAR,
        open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT,
        security_id VARCHAR, open_interest BIGINT, filename VARCHAR)"""
    )


def _copy_query(connection: Any, query: str, path: Path, cfg: DuckDbPreBsmConfig) -> None:
    connection.execute(
        f"COPY ({query}) TO {_sql_literal(str(path))} "
        f"(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE {cfg.row_group_size})"
    )


_DIMENSION_SQL = """
CREATE OR REPLACE TEMP VIEW expiry_ranked AS
SELECT
  *,
  count(*) OVER mapping_window AS mapping_match_count,
  row_number() OVER mapping_window AS mapping_rank
FROM expiry_raw
WINDOW mapping_window AS (
  PARTITION BY upper(underlying), try_cast(trade_date AS DATE), lower(expiry_type), expiry_code
  ORDER BY actual_expiry_date, source_id
);

CREATE OR REPLACE TEMP VIEW resolved_dimension AS
WITH candidate AS (
  SELECT
    e.* EXCLUDE (mapping_match_count, mapping_rank),
    e.mapping_match_count,
    r.contract_lot_size AS rule_contract_lot_size,
    r.market_lot AS rule_market_lot,
    r.contract_multiplier AS rule_contract_multiplier,
    r.trading_unit AS rule_trading_unit,
    r.tick_size AS rule_tick_size,
    r.rule_id AS contract_rule_id,
    r.circular_id AS contract_rule_circular_id,
    r.source_id AS contract_rule_source_id,
    r.source_sha256 AS contract_rule_source_sha256,
    try_cast(r.effective_from AS DATE) AS contract_rule_effective_from,
    try_cast(r.effective_to AS DATE) AS contract_rule_effective_to,
    r.mapping_status AS contract_rule_mapping_status,
    r.mapping_confidence AS contract_rule_mapping_confidence,
    count(r.rule_id) OVER rule_window AS rule_match_count,
    row_number() OVER rule_window AS rule_rank
  FROM expiry_ranked e
  LEFT JOIN rule_raw r ON
    upper(r.underlying) = upper(e.underlying)
    AND (r.expiry_type IS NULL OR lower(r.expiry_type) = lower(e.expiry_type))
    AND try_cast(e.actual_expiry_date AS DATE) >= try_cast(r.contract_expiry_from AS DATE)
    AND (r.contract_expiry_to IS NULL OR try_cast(e.actual_expiry_date AS DATE) <= try_cast(r.contract_expiry_to AS DATE))
  WHERE e.mapping_rank = 1
  WINDOW rule_window AS (
    PARTITION BY upper(e.underlying), try_cast(e.trade_date AS DATE), lower(e.expiry_type), e.expiry_code
    ORDER BY r.contract_expiry_from, r.rule_id
  )
)
SELECT * FROM candidate WHERE rule_rank = 1;
"""


_RIGHT_SQL = """
CREATE OR REPLACE TEMP VIEW spot_counted AS
SELECT *, count(*) OVER (PARTITION BY trade_date, session_status, timestamp_ist) AS duplicate_count,
       row_number() OVER (PARTITION BY trade_date, session_status, timestamp_ist ORDER BY filename, request_id) AS duplicate_rank
FROM spot_raw;
CREATE OR REPLACE TEMP VIEW vix_counted AS
SELECT *, count(*) OVER (PARTITION BY trade_date, session_status, timestamp_ist) AS duplicate_count,
       row_number() OVER (PARTITION BY trade_date, session_status, timestamp_ist ORDER BY filename, request_id) AS duplicate_rank
FROM vix_raw;
CREATE OR REPLACE TEMP VIEW spot_one AS SELECT * FROM spot_counted WHERE duplicate_rank = 1;
CREATE OR REPLACE TEMP VIEW vix_one AS SELECT * FROM vix_counted WHERE duplicate_rank = 1;
CREATE OR REPLACE TEMP VIEW spot_availability AS
SELECT trade_date, session_status, count(*) AS available_rows, min(timestamp_ist) AS min_timestamp
FROM spot_one GROUP BY trade_date, session_status;
CREATE OR REPLACE TEMP VIEW vix_availability AS
SELECT trade_date, session_status, count(*) AS available_rows, min(timestamp_ist) AS min_timestamp
FROM vix_one GROUP BY trade_date, session_status;
CREATE OR REPLACE TEMP VIEW right_duplicate_conflicts AS
SELECT 'nifty_spot' AS dataset, trade_date, session_status, timestamp_ist, request_id, filename AS source_file,
       close AS provider_close, duplicate_count
FROM spot_counted WHERE duplicate_count > 1
UNION ALL
SELECT 'india_vix', trade_date, session_status, timestamp_ist, request_id, filename,
       close, duplicate_count
FROM vix_counted WHERE duplicate_count > 1;
"""


_ENRICH_SQL = """
CREATE OR REPLACE TEMP VIEW option_spot AS
SELECT o.*, o.filename AS source_option_file,
       s.timestamp_ist AS spot_ts, s.close AS spot_close, s.duplicate_count AS spot_duplicate_count
FROM option_raw o
ASOF LEFT JOIN spot_one s ON
  o.timestamp_ist >= s.timestamp_ist
  AND o.trade_date = s.trade_date
  AND o.session_status = s.session_status
  AND upper(o.underlying) = upper(s.underlying);

CREATE OR REPLACE TEMP VIEW option_rights AS
SELECT os.*, v.timestamp_ist AS vix_ts, v.close AS vix_close, v.duplicate_count AS vix_duplicate_count
FROM option_spot os
ASOF LEFT JOIN vix_one v ON
  os.timestamp_ist >= v.timestamp_ist
  AND os.trade_date = v.trade_date
  AND os.session_status = v.session_status;

CREATE OR REPLACE TEMP VIEW base_enriched AS
SELECT
  o.* EXCLUDE (filename, spot_ts, spot_close, spot_duplicate_count, vix_ts, vix_close, vix_duplicate_count),
  o.source_option_file,
  {version} AS enrichment_version,
  o.session_status = 'regular_session' AS canonical_bsm_population,
  CASE WHEN o.spot_ts IS NOT NULL AND o.spot_duplicate_count = 1
             AND epoch(o.timestamp_ist - o.spot_ts) BETWEEN 0 AND 60
             AND isfinite(o.spot_close)
       THEN o.spot_close END AS independent_nifty_spot,
  o.spot_ts AS nifty_spot_timestamp_ist,
  CASE WHEN o.spot_ts IS NOT NULL THEN epoch(o.timestamp_ist - o.spot_ts) END AS nifty_spot_age_seconds,
  CASE WHEN o.spot_ts IS NOT NULL AND o.spot_duplicate_count = 1
             AND epoch(o.timestamp_ist - o.spot_ts) BETWEEN 0 AND 60 AND isfinite(o.spot_close)
       THEN CASE WHEN o.timestamp_ist = o.spot_ts THEN 'exact_timestamp' ELSE 'backward_asof' END ELSE 'none' END AS nifty_spot_match_method,
  CASE WHEN o.spot_ts IS NOT NULL AND o.spot_duplicate_count = 1
             AND epoch(o.timestamp_ist - o.spot_ts) BETWEEN 0 AND 60 AND isfinite(o.spot_close)
       THEN 'MATCHED' ELSE 'BLOCKED' END AS nifty_spot_join_status,
  CASE
    WHEN o.spot_ts IS NULL AND coalesce(sa.available_rows, 0) = 0 THEN 'no_right_rows_for_trade_date_session'
    WHEN o.spot_ts IS NULL AND sa.min_timestamp > o.timestamp_ist THEN 'future_only_right_rows'
    WHEN o.spot_duplicate_count > 1 THEN 'duplicate_right_timestamp'
    WHEN epoch(o.timestamp_ist - o.spot_ts) > 60 THEN 'backward_outside_tolerance'
    WHEN NOT isfinite(o.spot_close) THEN 'right_value_missing_or_non_finite'
    ELSE NULL END AS nifty_spot_join_failure_reason,
  CASE WHEN o.vix_ts IS NOT NULL AND o.vix_duplicate_count = 1
             AND epoch(o.timestamp_ist - o.vix_ts) BETWEEN 0 AND 60 AND isfinite(o.vix_close)
       THEN o.vix_close END AS india_vix,
  o.vix_ts AS india_vix_timestamp_ist,
  CASE WHEN o.vix_ts IS NOT NULL THEN epoch(o.timestamp_ist - o.vix_ts) END AS india_vix_age_seconds,
  CASE WHEN o.vix_ts IS NOT NULL AND o.vix_duplicate_count = 1
             AND epoch(o.timestamp_ist - o.vix_ts) BETWEEN 0 AND 60 AND isfinite(o.vix_close)
       THEN CASE WHEN o.timestamp_ist = o.vix_ts THEN 'exact_timestamp' ELSE 'backward_asof' END ELSE 'none' END AS india_vix_match_method,
  CASE
    WHEN o.vix_ts IS NOT NULL AND o.vix_duplicate_count = 1
         AND epoch(o.timestamp_ist - o.vix_ts) BETWEEN 0 AND 60 AND isfinite(o.vix_close) THEN 'MATCHED'
    WHEN {vix_available_from} IS NULL OR o.trade_date < {vix_available_from} THEN 'source_unavailable'
    ELSE 'BLOCKED' END AS india_vix_join_status,
  CASE
    WHEN o.vix_ts IS NOT NULL AND o.vix_duplicate_count = 1
         AND epoch(o.timestamp_ist - o.vix_ts) BETWEEN 0 AND 60 AND isfinite(o.vix_close) THEN NULL
    WHEN {vix_available_from} IS NULL OR o.trade_date < {vix_available_from} THEN 'provider_history_unavailable'
    WHEN o.vix_ts IS NULL AND coalesce(va.available_rows, 0) = 0 THEN 'no_right_rows_for_trade_date_session'
    WHEN o.vix_ts IS NULL AND va.min_timestamp > o.timestamp_ist THEN 'future_only_right_rows'
    WHEN o.vix_duplicate_count > 1 THEN 'duplicate_right_timestamp'
    WHEN epoch(o.timestamp_ist - o.vix_ts) > 60 THEN 'backward_outside_tolerance'
    WHEN NOT isfinite(o.vix_close) THEN 'right_value_missing_or_non_finite'
    ELSE NULL END AS india_vix_join_failure_reason,
  CASE WHEN {vix_available_from} IS NULL OR o.trade_date < {vix_available_from}
       THEN 'DHAN_INDIA_VIX_PROVEN_LOWER_BOUNDARY' ELSE 'DHAN_INTRADAY' END AS india_vix_source_provenance,
  {vix_available_from} AS india_vix_source_available_from,
  try_cast(d.actual_expiry_date AS DATE) AS actual_expiry_date,
  try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) AS actual_expiry_timestamp_ist,
  lower(d.expiry_type) AS expiry_type,
  d.expiry_rule_weekday,
  try_cast(d.expiry_rule_effective_from AS DATE) AS expiry_rule_effective_from,
  try_cast(d.expiry_rule_effective_to AS DATE) AS expiry_rule_effective_to,
  d.expiry_holiday_adjusted,
  try_cast(d.original_scheduled_expiry AS DATE) AS original_scheduled_expiry,
  CASE WHEN d.mapping_match_count = 1 AND lower(d.mapping_status) IN ('proven','verified','resolved')
             AND d.mapping_confidence IS NOT NULL AND d.source_id IS NOT NULL
             AND length(d.source_sha256) = 64 AND try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) IS NOT NULL
       THEN 'resolved' ELSE 'BLOCKED' END AS expiry_mapping_status,
  d.mapping_confidence AS expiry_mapping_confidence,
  d.source_id AS expiry_source_id,
  d.circular_id AS expiry_circular_id,
  d.source_sha256 AS expiry_source_sha256,
  CASE WHEN d.rule_match_count = 1 AND lower(d.contract_rule_mapping_status) IN ('proven','verified','resolved')
             AND d.contract_rule_mapping_confidence IS NOT NULL AND d.contract_rule_id IS NOT NULL
             AND d.contract_rule_circular_id IS NOT NULL AND d.contract_rule_source_id IS NOT NULL
             AND length(d.contract_rule_source_sha256) = 64 AND d.rule_contract_lot_size > 0
       THEN d.rule_contract_lot_size::DOUBLE END AS contract_lot_size,
  CASE WHEN d.rule_match_count = 1 THEN d.rule_market_lot::DOUBLE END AS market_lot,
  d.rule_contract_multiplier::DOUBLE AS contract_multiplier,
  d.rule_trading_unit AS trading_unit,
  d.rule_tick_size::DOUBLE AS tick_size,
  CASE WHEN d.rule_match_count = 1 AND lower(d.contract_rule_mapping_status) IN ('proven','verified','resolved')
             AND d.rule_contract_lot_size > 0 THEN 'resolved' ELSE 'BLOCKED' END AS contract_rule_status,
  d.contract_rule_mapping_confidence,
  d.contract_rule_id,
  d.contract_rule_circular_id,
  d.contract_rule_source_id,
  d.contract_rule_source_sha256,
  d.contract_rule_effective_from,
  d.contract_rule_effective_to,
  CASE WHEN d.mapping_match_count IS NULL THEN 'actual_expiry_no_match'
       WHEN d.mapping_match_count <> 1 THEN 'actual_expiry_ambiguous'
       WHEN d.rule_match_count = 0 THEN 'contract_rule_no_match'
       WHEN d.rule_match_count <> 1 THEN 'contract_rule_ambiguous'
       WHEN d.rule_contract_lot_size IS NULL OR d.rule_contract_lot_size <= 0 THEN 'contract_rule_missing_lot_size'
       ELSE NULL END AS contract_rule_failure_reason,
  CASE WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) > o.timestamp_ist
       THEN epoch(try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) - o.timestamp_ist) / 60.0 END AS mte,
  CASE WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) > o.timestamp_ist
       THEN epoch(try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) - o.timestamp_ist) / 86400.0 END AS dte,
  CASE WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) > o.timestamp_ist
       THEN epoch(try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) - o.timestamp_ist) / (365.0 * 86400.0) END AS t_years_act365,
  CASE WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) > o.timestamp_ist THEN 'valid' ELSE 'BLOCKED' END AS time_to_expiry_status,
  CASE WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) IS NULL THEN 'actual_expiry_unavailable'
       WHEN try_cast(d.actual_expiry_timestamp_ist AS TIMESTAMPTZ) <= o.timestamp_ist THEN 'non_positive_mte'
       ELSE NULL END AS time_to_expiry_failure_reason
FROM option_rights o
LEFT JOIN spot_availability sa USING (trade_date, session_status)
LEFT JOIN vix_availability va USING (trade_date, session_status)
LEFT JOIN resolved_dimension d ON
  upper(o.underlying) = upper(d.underlying)
  AND o.trade_date = try_cast(d.trade_date AS DATE)
  AND CASE WHEN upper(o.expiry_flag) = 'WEEK' THEN 'weekly' WHEN upper(o.expiry_flag) = 'MONTH' THEN 'monthly' END = lower(d.expiry_type)
  AND o.expiry_code = d.expiry_code;

CREATE OR REPLACE TEMP VIEW enriched AS
SELECT b.*,
  CASE WHEN {terminal}
             AND canonical_bsm_population
             AND nifty_spot_join_status = 'MATCHED'
             AND expiry_mapping_status = 'resolved'
             AND contract_rule_status = 'resolved'
             AND time_to_expiry_status = 'valid'
             AND close IS NOT NULL AND isfinite(close) AND close > 0
             AND strike IS NOT NULL AND strike > 0
       THEN 'READY' ELSE 'BLOCKED' END AS bsm_gate_status,
  nullif(concat_ws(';',
    CASE WHEN NOT {terminal} THEN 'acquisition_not_terminally_accounted' END,
    CASE WHEN NOT canonical_bsm_population THEN 'outside_regular_session' END,
    CASE WHEN nifty_spot_join_status <> 'MATCHED' THEN 'nifty_spot_join_unavailable' END,
    CASE WHEN expiry_mapping_status <> 'resolved' THEN coalesce(contract_rule_failure_reason, 'actual_expiry_unresolved') END,
    CASE WHEN contract_rule_status <> 'resolved' THEN coalesce(contract_rule_failure_reason, 'contract_rule_unresolved') END,
    CASE WHEN time_to_expiry_status <> 'valid' THEN coalesce(time_to_expiry_failure_reason, 'time_to_expiry_invalid') END,
    CASE WHEN close IS NULL OR NOT isfinite(close) OR close <= 0 THEN 'option_close_invalid' END,
    CASE WHEN strike IS NULL OR strike <= 0 THEN 'strike_invalid' END
  ), '') AS bsm_gate_failure_reason
FROM base_enriched b;
"""


def _month_files(root: Path) -> dict[str, tuple[Path, ...]]:
    grouped: dict[str, list[Path]] = {}
    if not root.is_dir():
        return {}
    for path in sorted(root.rglob("*.parquet")):
        match = _MONTH_RE.search(str(path))
        if match:
            grouped.setdefault(f"{match.group(1)}-{match.group(2)}", []).append(path.resolve())
    return {month: tuple(files) for month, files in grouped.items()}


def _validate_month_selection(
    requested: Sequence[str] | None, available: Mapping[str, Sequence[Path]]
) -> list[str]:
    if not available:
        raise ValueError("no partitioned option Parquet files found")
    if not requested:
        return sorted(available)
    selected = []
    for month in requested:
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            raise ValueError(f"invalid month {month!r}; expected YYYY-MM")
        if month not in available:
            raise ValueError(f"requested option month is unavailable: {month}")
        selected.append(month)
    return sorted(dict.fromkeys(selected))


def _minimum_trade_date(files: Sequence[Path], *, duckdb: Any) -> date | None:
    if not files:
        return None
    connection = duckdb.connect()
    try:
        connection.from_parquet([str(path) for path in files], union_by_name=True).create_view("vix_all")
        return connection.execute("SELECT min(trade_date) FROM vix_all").fetchone()[0]
    finally:
        connection.close()


def _file_lineage(root: Path, files: Iterable[Path]) -> dict[str, Any]:
    entries = [
        {
            "relative_path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(files)
    ]
    return {"root": str(root), "file_count": len(entries), "files": entries}


def _validated_manifest(path: Path, input_fingerprint: str, config_hash: str) -> Mapping[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("status") != "published":
            return None
        if payload.get("input_fingerprint") != input_fingerprint or payload.get("config_sha256") != config_hash:
            return None
        for artifact in payload.get("artifacts", ()):
            target = Path(artifact["path"])
            if not target.is_file() or sha256_file(target) != artifact.get("sha256"):
                return None
        return payload
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def _accumulate(totals: dict[str, int], audit: Mapping[str, Any]) -> None:
    totals["input"] += int(audit["input_rows"])
    totals["output"] += int(audit["output_rows"])
    totals["canonical"] += int(audit["canonical_regular_rows"])
    totals["exceptions"] += int(audit["source_exception_rows"])
    totals["ready"] += int(audit["ready_rows"])
    totals["blocked"] += int(audit["blocked_rows"])
    totals["duplicates"] += int(audit["duplicate_right_rows"])


def _write_status(
    path: Path,
    months: Sequence[str],
    current_index: int,
    totals: Mapping[str, int],
    cfg: DuckDbPreBsmConfig,
    *,
    state: str,
    current_month: str,
    started_monotonic: float,
    started_at_utc: str,
    output_path: Path,
    last_error: str | None = None,
) -> None:
    elapsed = max(0.0, time.monotonic() - started_monotonic)
    remaining_months = max(0, len(months) - current_index)
    eta = (elapsed / current_index * remaining_months) if current_index else None
    payload = {
        "status_version": "2.0.0",
        "state": state,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "started_at_utc": started_at_utc,
        "pid": os.getpid(),
        "elapsed_seconds": elapsed,
        "rows_per_second": totals["output"] / elapsed if elapsed > 0 else 0.0,
        "eta_seconds": eta,
        "current_month": current_month,
        "orphan_partial_count": sum(1 for _ in output_path.rglob("*.partial")),
        "orphan_staging_quarantined_count": totals["orphan_staging_quarantined"],
        "bsm_executed": False,
        "months_total": len(months),
        "months_seen": current_index,
        "months_processed": totals["processed"],
        "months_resumed": totals["resumed"],
        "input_rows": totals["input"],
        "output_rows": totals["output"],
        "ready_rows": totals["ready"],
        "blocked_rows": totals["blocked"],
        "config": asdict(cfg),
    }
    if last_error is not None:
        payload["last_error"] = last_error
    _atomic_json(path, payload)
    lines = ["# pre-BSM v2 status", "", *(f"- {key}: {value}" for key, value in payload.items() if key != "config")]
    _atomic_text(path.with_suffix(".md"), "\n".join(lines) + "\n")


def _quarantine_invalid_publication(
    month_dir: Path, manifest_path: Path, output_path: Path, month: str
) -> None:
    if not month_dir.exists() and not manifest_path.exists():
        return
    quarantine = output_path / "exceptions" / "replaced_publications" / f"month={month}.{uuid.uuid4().hex}"
    quarantine.mkdir(parents=True, exist_ok=False)
    if month_dir.exists():
        os.replace(month_dir, quarantine / "month_partition")
    if manifest_path.exists():
        os.replace(manifest_path, quarantine / manifest_path.name)


def _quarantine_orphan_staging(staging_root: Path, output_path: Path) -> int:
    stale = sorted(path for path in staging_root.iterdir() if path.is_dir())
    if not stale:
        return 0
    quarantine_root = output_path / "exceptions" / "orphan_staging"
    quarantine_root.mkdir(parents=True, exist_ok=True)
    for path in stale:
        target = quarantine_root / f"{path.name}.{uuid.uuid4().hex}"
        os.replace(path, target)
    return len(stale)


def _required_parquet(path: str | Path, label: str) -> Path:
    result = Path(path).resolve()
    if not result.is_file() or result.suffix.lower() != ".parquet":
        raise ValueError(f"{label} must be an existing Parquet file: {result}")
    return result


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _json_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _fsync_file(path: Path) -> None:
    with path.open("r+b") as handle:
        os.fsync(handle.fileno())


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_text(path, json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n")


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with partial.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)
