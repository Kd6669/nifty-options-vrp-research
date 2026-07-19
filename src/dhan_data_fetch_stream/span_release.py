"""Final Dhan BSM plus accepted-with-source-gaps NSE SPAN release builder."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import time
from typing import Any, Sequence
import uuid

from .span_gold import (
    _atomic_json,
    _atomic_text,
    _copy_parquet,
    _discover_months,
    _fsync_file,
    _json_sha,
    _lineage,
    _read_json,
    _sql_literal,
    _sql_text,
    _validate_bsm_audit,
    sha256_file,
)


BOD_RELEASE_VERSION = "1.4.0"
SIX_SLOT_RELEASE_VERSION = "2.0.0"
BOD_JOIN_POLICY = "BOD_CONSERVATIVE_UNKNOWN_EFFECTIVE_TIME_FINAL_RELEASE"
SIX_SLOT_JOIN_POLICY = "SIX_SLOT_STATIC_RESEARCH_NO_PUBLICATION_TIME_INFERENCE"
EXPECTED_HANDOFF_SHA256 = (
    "4efa9bc83b0750d0c439ac9535dfc961dec88c090cce4e86071507c75c4a43df"
)
EXPECTED_RELEASE_SHA256 = (
    "669977281800aa67f331dd1b1da933b00fa3da6a29fe96a75d90ac776c02f9e0"
)
EXPECTED_GAP_SHA256 = "55d634198f2658992c0d927d08a0fc8c8bc7c34a28af662c27a2e5eec9cfc133"
EXPECTED_SPAN_COMMIT = "b862d6bf7590ff62097ac373d40fd85e1be6480f"
EXPECTED_ROWS = 43_018_677
EXPECTED_BSM_STATUS = {
    "ok": 33_281_564,
    "no_arbitrage_violation": 9_688_658,
    "iv_solver_failed": 48,
    "blocked": 48_407,
}
EXPECTED_BOD_MATCHED = 42_718_832
EXPECTED_BOD_UNMATCHED = 299_845
SLOTS = ("BOD", "ID1", "ID2", "ID3", "ID4", "EOD")
_MONTH = re.compile(r"^\d{4}-\d{2}$")
_BSM_KEY = (
    "timestamp_ist",
    "trade_date",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "moneyness_label",
    "strike",
    "option_type",
)
_SPAN_REQUIRED = {
    "date",
    "time_slot",
    "symbol",
    "instrument",
    "expiry",
    "strike",
    "price",
    "delta",
    "implied_vol",
    "price_scan_range",
    "vol_scan_range",
    "cvf",
    "composite_delta",
    "source_file",
    "source_sha256",
    "source_member",
    "effective_time_source",
    "span_effective_ts_ist",
} | {f"s{index}" for index in range(1, 17)}
_GAP_REQUIRED = {
    "trading_date",
    "slot",
    "classification_outcome",
    "source_boundary_category",
    "source_boundary_proven",
    "evidence_basis",
    "evidence_event_id",
    "availability_event_sha256",
    "gap_category",
    "safe_downstream_status",
    "final_download_state",
}


@dataclass(frozen=True)
class SpanReleaseConfig:
    threads: int = 8
    memory_limit: str = "8GB"
    row_group_size: int = 250_000


@dataclass(frozen=True)
class SpanReleaseStats:
    months_total: int
    months_processed: int
    months_resumed: int
    rows_total: int
    bod_matched_rows: int
    bod_unmatched_rows: int
    bod_output_root: str
    six_slot_output_root: str
    terminal_status: str
    terminal_audit_path: str
    elapsed_seconds: float


def run_span_release(
    *,
    bsm_root: str | Path,
    bsm_terminal_audit: str | Path,
    span_compacted_root: str | Path,
    span_release_manifest: str | Path,
    span_handoff: str | Path,
    span_source_gap_manifest: str | Path,
    bod_output_root: str | Path,
    six_slot_output_root: str | Path,
    months: Sequence[str] | None = None,
    config: SpanReleaseConfig | None = None,
    resume: bool = True,
) -> SpanReleaseStats:
    """Publish final BOD and static six-slot representations without changing Dhan values."""
    cfg = config or SpanReleaseConfig()
    _validate_config(cfg)
    bsm_root = Path(bsm_root).resolve()
    bsm_audit_path = Path(bsm_terminal_audit).resolve()
    span_root = Path(span_compacted_root).resolve()
    release_path = Path(span_release_manifest).resolve()
    handoff_path = Path(span_handoff).resolve()
    gap_path = Path(span_source_gap_manifest).resolve()
    bod_root = Path(bod_output_root).resolve() / f"version={BOD_RELEASE_VERSION}"
    six_root = (
        Path(six_slot_output_root).resolve() / f"version={SIX_SLOT_RELEASE_VERSION}"
    )

    # These validations re-hash every producer month before any output is written.
    bsm_audit = _validate_bsm_audit(bsm_root, bsm_audit_path)
    release = _validate_final_span_release(
        span_root=span_root,
        release_path=release_path,
        handoff_path=handoff_path,
        gap_path=gap_path,
    )
    discovered = _discover_months(bsm_root, span_root)
    bsm_months = {str(item["month"]) for item in bsm_audit["months_audited"]}
    if set(discovered) != bsm_months or set(discovered) != set(release["months"]):
        raise ValueError("BSM/SPAN/release month universes disagree")
    selected = sorted(discovered) if months is None else list(dict.fromkeys(months))
    invalid = [month for month in selected if not _MONTH.fullmatch(month)]
    missing = [month for month in selected if month not in discovered]
    if invalid or missing or not selected:
        raise ValueError(
            f"invalid or unavailable month selection: invalid={invalid} missing={missing}"
        )

    started = time.monotonic()
    month_results: list[dict[str, Any]] = []
    processed = resumed_count = 0
    for month in selected:
        result = _run_month(
            month=month,
            bsm_path=discovered[month][0],
            span_path=discovered[month][1],
            bsm_audit=bsm_audit,
            bsm_audit_path=bsm_audit_path,
            release=release,
            release_path=release_path,
            handoff_path=handoff_path,
            gap_path=gap_path,
            bod_root=bod_root,
            six_root=six_root,
            config=cfg,
            resume=resume,
        )
        month_results.append(result)
        processed += int(not result["fully_resumed"])
        resumed_count += int(result["fully_resumed"])
        _publish_status(bod_root, six_root, selected, month_results, "running")

    full_scope = selected == sorted(discovered)
    audit = _terminal_audit(
        selected=selected,
        full_scope=full_scope,
        results=month_results,
        bsm_audit=bsm_audit,
        release=release,
        bsm_audit_path=bsm_audit_path,
        release_path=release_path,
        handoff_path=handoff_path,
        gap_path=gap_path,
        bod_root=bod_root,
        six_root=six_root,
    )
    _publish_status(
        bod_root,
        six_root,
        selected,
        month_results,
        "complete",
        terminal_status=audit["status"],
    )
    if audit["status"] == "FAIL":
        raise RuntimeError(
            "final SPAN release audit failed: " + ", ".join(audit["errors"])
        )
    return SpanReleaseStats(
        months_total=len(selected),
        months_processed=processed,
        months_resumed=resumed_count,
        rows_total=int(audit["input_rows"]),
        bod_matched_rows=int(audit["bod"]["matched_rows"]),
        bod_unmatched_rows=int(audit["bod"]["unmatched_rows"]),
        bod_output_root=str(bod_root),
        six_slot_output_root=str(six_root),
        terminal_status=str(audit["status"]),
        terminal_audit_path=str(
            six_root / "manifests" / "span_release_terminal_audit.json"
        ),
        elapsed_seconds=time.monotonic() - started,
    )


def _run_month(
    *,
    month: str,
    bsm_path: Path,
    span_path: Path,
    bsm_audit: dict[str, Any],
    bsm_audit_path: Path,
    release: dict[str, Any],
    release_path: Path,
    handoff_path: Path,
    gap_path: Path,
    bod_root: Path,
    six_root: Path,
    config: SpanReleaseConfig,
    resume: bool,
) -> dict[str, Any]:
    expected_bsm = next(
        item for item in bsm_audit["months_audited"] if item["month"] == month
    )
    expected_span = release["months"][month]
    if sha256_file(bsm_path) != expected_bsm["output_sha256"]:
        raise ValueError(f"BSM producer hash changed for {month}")
    if sha256_file(span_path) != expected_span["sha256"]:
        raise ValueError(f"SPAN release hash changed for {month}")
    lineage_base = {
        "month": month,
        "code_sha256": sha256_file(Path(__file__)),
        "bsm_input": _lineage(bsm_path),
        "span_input": _lineage(span_path),
        "bsm_terminal_audit": _lineage(bsm_audit_path),
        "span_release_manifest": _lineage(release_path),
        "span_handoff": _lineage(handoff_path),
        "span_source_gap_manifest": _lineage(gap_path),
        "span_producer_commit": release["producer_commit"],
        "span_release_status": release["release_status"],
        "span_technical_audit_outcome": release["technical_outcome"],
        "config": {
            "threads": config.threads,
            "memory_limit": config.memory_limit,
            "row_group_size": config.row_group_size,
        },
    }
    bod_lineage = {
        **lineage_base,
        "representation": "BOD",
        "version": BOD_RELEASE_VERSION,
        "join_policy": BOD_JOIN_POLICY,
    }
    six_lineage = {
        **lineage_base,
        "representation": "SIX_SLOT_WIDE",
        "version": SIX_SLOT_RELEASE_VERSION,
        "join_policy": SIX_SLOT_JOIN_POLICY,
    }
    bod_paths = _paths(bod_root, month)
    six_paths = _paths(six_root, month)
    bod_resumed = (
        _resume_manifest(bod_paths, _json_sha(bod_lineage)) if resume else None
    )
    six_resumed = (
        _resume_manifest(six_paths, _json_sha(six_lineage)) if resume else None
    )
    if bod_resumed is not None and six_resumed is not None:
        return {
            "month": month,
            "fully_resumed": True,
            "bod": bod_resumed,
            "six_slot": six_resumed,
        }

    import duckdb

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
        connection.execute(
            "CREATE TEMP VIEW gaps AS SELECT *, CAST(trading_date AS DATE) AS gap_date "
            f"FROM read_parquet({_sql_literal(str(gap_path))}, hive_partitioning=false)"
        )
        span_columns = [
            row[0] for row in connection.execute("DESCRIBE span_all").fetchall()
        ]
        gap_columns = [row[0] for row in connection.execute("DESCRIBE gaps").fetchall()]
        if missing := sorted(_SPAN_REQUIRED.difference(span_columns)):
            raise ValueError(f"SPAN schema missing fields for {month}: {missing}")
        if missing := sorted(_GAP_REQUIRED.difference(gap_columns)):
            raise ValueError(f"SPAN gap schema missing fields: {missing}")
        connection.execute(
            "CREATE TEMP VIEW span_options AS SELECT * FROM span_all "
            "WHERE symbol='NIFTY' AND instrument IN ('CE','PE')"
        )
        duplicate_span_keys = int(
            connection.execute(
                """SELECT coalesce(sum(n-1),0) FROM (
                SELECT count(*) n FROM span_options
                GROUP BY date,time_slot,symbol,instrument,expiry,strike HAVING n>1)"""
            ).fetchone()[0]
        )
        duplicate_gap_keys = int(
            connection.execute(
                """SELECT coalesce(sum(n-1),0) FROM (
                SELECT count(*) n FROM gaps GROUP BY gap_date,slot HAVING n>1)"""
            ).fetchone()[0]
        )
        if duplicate_span_keys or duplicate_gap_keys:
            raise ValueError(
                f"duplicate SPAN/gap right keys for {month}: "
                f"{duplicate_span_keys}/{duplicate_gap_keys}"
            )
        invalid_timing = int(
            connection.execute(
                """SELECT count(*) FROM span_options
                WHERE effective_time_source<>'unknown' OR span_effective_ts_ist IS NOT NULL"""
            ).fetchone()[0]
        )
        if invalid_timing:
            raise ValueError(f"unproven SPAN timing contract changed for {month}")
        input_rows = int(connection.execute("SELECT count(*) FROM bsm").fetchone()[0])
        span_rows = int(
            connection.execute("SELECT count(*) FROM span_all").fetchone()[0]
        )
        if input_rows != int(expected_bsm["rows"]):
            raise ValueError(f"BSM month row mismatch for {month}")
        if span_rows != int(expected_span["row_count"]):
            raise ValueError(f"SPAN month row mismatch for {month}")

        if bod_resumed is None:
            bod_resumed = _materialize(
                connection=connection,
                month=month,
                representation="BOD",
                paths=bod_paths,
                lineage=bod_lineage,
                select_sql=_bod_sql(span_columns, bod_lineage),
                exception_filter="span_join_status<>'matched'",
                exception_columns=_bod_exception_columns(),
                input_rows=input_rows,
                duplicate_span_keys=duplicate_span_keys,
                duplicate_gap_keys=duplicate_gap_keys,
                bsm_path=bsm_path,
                expected_bsm_sha=expected_bsm["output_sha256"],
                span_path=span_path,
                expected_span_sha=expected_span["sha256"],
                config=config,
            )
        if six_resumed is None:
            six_resumed = _materialize(
                connection=connection,
                month=month,
                representation="SIX_SLOT_WIDE",
                paths=six_paths,
                lineage=six_lineage,
                select_sql=_six_slot_sql(span_columns, six_lineage),
                exception_filter="span_unmatched_slot_count>0",
                exception_columns=_six_exception_columns(),
                input_rows=input_rows,
                duplicate_span_keys=duplicate_span_keys,
                duplicate_gap_keys=duplicate_gap_keys,
                bsm_path=bsm_path,
                expected_bsm_sha=expected_bsm["output_sha256"],
                span_path=span_path,
                expected_span_sha=expected_span["sha256"],
                config=config,
            )
    finally:
        connection.close()
    return {
        "month": month,
        "fully_resumed": bool(bod_resumed["resumed"] and six_resumed["resumed"]),
        "bod": bod_resumed,
        "six_slot": six_resumed,
    }


def _materialize(
    *,
    connection: Any,
    month: str,
    representation: str,
    paths: dict[str, Path],
    lineage: dict[str, Any],
    select_sql: str,
    exception_filter: str,
    exception_columns: str,
    input_rows: int,
    duplicate_span_keys: int,
    duplicate_gap_keys: int,
    bsm_path: Path,
    expected_bsm_sha: str,
    span_path: Path,
    expected_span_sha: str,
    config: SpanReleaseConfig,
) -> dict[str, Any]:
    _quarantine_stale(paths)
    paths["output"].parent.mkdir(parents=True, exist_ok=True)
    paths["exception"].parent.mkdir(parents=True, exist_ok=True)
    output_partial = paths["output"].with_name(
        f".{paths['output'].name}.{uuid.uuid4().hex}.partial"
    )
    exception_partial = paths["exception"].with_name(
        f".{paths['exception'].name}.{uuid.uuid4().hex}.partial"
    )
    try:
        _copy_parquet(connection, select_sql, output_partial, config.row_group_size)
        exception_sql = (
            f"SELECT {exception_columns} FROM read_parquet("
            f"{_sql_literal(str(output_partial))}, hive_partitioning=false) "
            f"WHERE {exception_filter}"
        )
        _copy_parquet(
            connection, exception_sql, exception_partial, config.row_group_size
        )
        _fsync_file(output_partial)
        _fsync_file(exception_partial)
        audit = _audit_materialized(
            connection,
            output_partial,
            exception_partial,
            representation,
            input_rows,
        )
        if audit["output_rows"] != input_rows:
            raise RuntimeError(f"row conservation failed for {representation} {month}")
        if any(
            int(audit[key])
            for key in (
                "primary_key_duplicate_rows",
                "cross_key_violation_rows",
            )
        ):
            raise RuntimeError(f"join invariants failed for {representation} {month}")
        if sha256_file(bsm_path) != expected_bsm_sha:
            raise RuntimeError(f"BSM input changed during {month} publication")
        if sha256_file(span_path) != expected_span_sha:
            raise RuntimeError(f"SPAN input changed during {month} publication")
        os.replace(output_partial, paths["output"])
        os.replace(exception_partial, paths["exception"])
    finally:
        output_partial.unlink(missing_ok=True)
        exception_partial.unlink(missing_ok=True)
    manifest = {
        "schema": "dhan_span_final_release_month_manifest",
        "schema_version": (
            BOD_RELEASE_VERSION if representation == "BOD" else SIX_SLOT_RELEASE_VERSION
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "representation": representation,
        "lineage": lineage,
        "lineage_sha256": _json_sha(lineage),
        "input_rows": input_rows,
        "output_rows": audit["output_rows"],
        "exception_rows": audit["exception_rows"],
        "row_conservation": audit["output_rows"] == input_rows,
        "duplicate_span_keys": duplicate_span_keys,
        "duplicate_gap_keys": duplicate_gap_keys,
        "primary_key_duplicate_rows": audit["primary_key_duplicate_rows"],
        "cross_key_violation_rows": audit["cross_key_violation_rows"],
        "bsm_status_counts": audit["bsm_status_counts"],
        "slot_status_counts": audit["slot_status_counts"],
        "output_path": str(paths["output"]),
        "output_sha256": sha256_file(paths["output"]),
        "output_bytes": paths["output"].stat().st_size,
        "exception_path": str(paths["exception"]),
        "exception_sha256": sha256_file(paths["exception"]),
        "exception_bytes": paths["exception"].stat().st_size,
        "resumed": False,
    }
    _atomic_json(paths["manifest"], manifest)
    manifest["manifest_path"] = str(paths["manifest"])
    manifest["manifest_sha256"] = sha256_file(paths["manifest"])
    return manifest


def _audit_materialized(
    connection: Any,
    output: Path,
    exception: Path,
    representation: str,
    expected_rows: int,
) -> dict[str, Any]:
    output_literal = _sql_literal(str(output))
    exception_literal = _sql_literal(str(exception))
    relation = f"read_parquet({output_literal}, hive_partitioning=false)"
    output_rows = int(
        connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
    )
    exception_rows = int(
        connection.execute(
            f"SELECT count(*) FROM read_parquet({exception_literal}, hive_partitioning=false)"
        ).fetchone()[0]
    )
    key_columns = ",".join(_BSM_KEY)
    duplicate_rows = int(
        connection.execute(
            f"""SELECT coalesce(sum(n-1),0) FROM (
            SELECT count(*) n FROM {relation} GROUP BY {key_columns} HAVING n>1)"""
        ).fetchone()[0]
    )
    if representation == "BOD":
        cross = int(
            connection.execute(
                f"""SELECT count(*) FROM {relation} WHERE span_join_status='matched' AND (
                trade_date<>span_date OR actual_expiry_date<>span_expiry
                OR CAST(strike AS DOUBLE)<>span_strike
                OR CASE option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END<>span_instrument
                OR span_time_slot<>'BOD')"""
            ).fetchone()[0]
        )
        slot_columns = ("span_join_status",)
    else:
        predicates = []
        for slot in SLOTS:
            prefix = f"span_{slot.lower()}"
            predicates.append(
                f"({prefix}_join_status='matched' AND (trade_date<>{prefix}_date "
                f"OR actual_expiry_date<>{prefix}_expiry "
                f"OR CAST(strike AS DOUBLE)<>{prefix}_strike "
                f"OR CASE option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END<>{prefix}_instrument "
                f"OR {prefix}_time_slot<>'{slot}'))"
            )
        cross = int(
            connection.execute(
                f"SELECT count(*) FROM {relation} WHERE " + " OR ".join(predicates)
            ).fetchone()[0]
        )
        slot_columns = tuple(f"span_{slot.lower()}_join_status" for slot in SLOTS)
    bsm_counts = _counts(connection, relation, "bsm_status")
    slot_counts = {
        column: _counts(connection, relation, column) for column in slot_columns
    }
    if sum(bsm_counts.values()) != expected_rows:
        raise RuntimeError("BSM status accounting changed in materialized output")
    return {
        "output_rows": output_rows,
        "exception_rows": exception_rows,
        "primary_key_duplicate_rows": duplicate_rows,
        "cross_key_violation_rows": cross,
        "bsm_status_counts": bsm_counts,
        "slot_status_counts": slot_counts,
    }


def _bod_sql(span_columns: Sequence[str], lineage: dict[str, Any]) -> str:
    alias = "s"
    gap = "g"
    special = {
        "time_slot",
        "source_file",
        "source_sha256",
        "source_member",
        "effective_time_source",
        "span_effective_ts_ist",
    }
    prefixed = [
        f'{alias}."{name}" AS "span_{name}"'
        for name in span_columns
        if name not in special
    ]
    columns = [
        "b.*",
        f"'{BOD_JOIN_POLICY}' AS span_join_policy",
        _status(alias, gap, "span_join_status"),
        "CASE WHEN s.date IS NULL THEN 'unmatched' ELSE 'matched' END AS span_enrichment_status",
        _reason(alias, gap, "span_unmatched_reason"),
        _source_status(alias, gap, "span_source_status"),
        "coalesce(g.source_boundary_proven,false) AS span_source_boundary",
        "g.gap_category AS span_source_gap_reason",
        "s.source_file AS span_source_file",
        "coalesce(s.source_sha256,g.availability_event_sha256) AS span_source_sha256",
        "s.source_member AS span_source_member",
        "coalesce(s.effective_time_source,'unknown') AS span_effective_time_source",
        "s.span_effective_ts_ist AS span_effective_ts_ist",
        "'BOD' AS span_time_slot",
        "g.classification_outcome AS span_gap_classification_outcome",
        "g.source_boundary_category AS span_source_boundary_category",
        "g.evidence_basis AS span_source_gap_evidence_basis",
        "g.evidence_event_id AS span_source_gap_evidence_event_id",
        *_release_columns(lineage),
        *prefixed,
    ]
    return (
        "SELECT "
        + ",\n".join(columns)
        + " FROM bsm b LEFT JOIN span_options s ON "
        + _join("s", "BOD")
        + " LEFT JOIN gaps g ON b.trade_date=g.gap_date AND g.slot='BOD'"
    )


def _six_slot_sql(span_columns: Sequence[str], lineage: dict[str, Any]) -> str:
    columns = ["b.*", f"'{SIX_SLOT_JOIN_POLICY}' AS span_join_policy"]
    joins: list[str] = []
    unmatched_terms: list[str] = []
    special = {
        "time_slot",
        "source_file",
        "source_sha256",
        "source_member",
        "effective_time_source",
        "span_effective_ts_ist",
    }
    for slot in SLOTS:
        slug = slot.lower()
        alias = f"s_{slug}"
        gap = f"g_{slug}"
        prefix = f"span_{slug}"
        columns.extend(
            [
                _status(alias, gap, f"{prefix}_join_status"),
                _source_status(alias, gap, f"{prefix}_source_status"),
                f"coalesce({gap}.source_boundary_proven,false) AS {prefix}_source_boundary",
                f"{gap}.gap_category AS {prefix}_source_gap_reason",
                f"{alias}.source_file AS {prefix}_source_file",
                f"coalesce({alias}.source_sha256,{gap}.availability_event_sha256) AS {prefix}_source_sha256",
                f"{alias}.source_member AS {prefix}_source_member",
                f"coalesce({alias}.effective_time_source,'unknown') AS {prefix}_effective_time_source",
                f"{alias}.span_effective_ts_ist AS {prefix}_span_effective_ts_ist",
                f"'{slot}' AS {prefix}_time_slot",
                f"{gap}.classification_outcome AS {prefix}_gap_classification_outcome",
                f"{gap}.source_boundary_category AS {prefix}_source_boundary_category",
                f"{gap}.evidence_basis AS {prefix}_source_gap_evidence_basis",
                f"{gap}.evidence_event_id AS {prefix}_source_gap_evidence_event_id",
            ]
        )
        columns.extend(
            f'{alias}."{name}" AS "{prefix}_{name}"'
            for name in span_columns
            if name not in special
        )
        unmatched_terms.append(f"CASE WHEN {alias}.date IS NULL THEN 1 ELSE 0 END")
        joins.append(
            f" LEFT JOIN span_options {alias} ON {_join(alias, slot)}"
            f" LEFT JOIN gaps {gap} ON b.trade_date={gap}.gap_date AND {gap}.slot='{slot}'"
        )
    columns.extend(_release_columns(lineage))
    columns.append(" + ".join(unmatched_terms) + " AS span_unmatched_slot_count")
    return "SELECT " + ",\n".join(columns) + " FROM bsm b" + "".join(joins)


def _release_columns(lineage: dict[str, Any]) -> list[str]:
    return [
        f"{_sql_literal(lineage['span_release_status'])} AS span_release_status",
        f"{_sql_literal(lineage['span_technical_audit_outcome'])} AS span_technical_audit_outcome",
        f"{_sql_literal(lineage['span_release_manifest']['sha256'])} AS span_release_manifest_sha256",
        f"{_sql_literal(lineage['span_handoff']['sha256'])} AS span_handoff_sha256",
        f"{_sql_literal(lineage['span_source_gap_manifest']['sha256'])} AS span_source_gap_manifest_sha256",
        f"{_sql_literal(_json_sha(lineage))} AS span_gold_lineage_sha256",
        "false AS span_slot_publication_times_proven",
        "false AS span_intraday_asof_join_performed",
    ]


def _join(alias: str, slot: str) -> str:
    return (
        f"b.trade_date={alias}.date AND b.actual_expiry_date={alias}.expiry "
        f"AND CAST(b.strike AS DOUBLE)={alias}.strike "
        f"AND {alias}.instrument=CASE b.option_type WHEN 'CALL' THEN 'CE' WHEN 'PUT' THEN 'PE' ELSE NULL END "
        f"AND {alias}.time_slot='{slot}'"
    )


def _status(alias: str, gap: str, output: str) -> str:
    return (
        f"CASE WHEN {alias}.date IS NOT NULL THEN 'matched' "
        f"WHEN {gap}.gap_date IS NOT NULL THEN 'source_gap' "
        f"ELSE 'unmatched_contract' END AS {output}"
    )


def _reason(alias: str, gap: str, output: str) -> str:
    return (
        f"CASE WHEN {alias}.date IS NOT NULL THEN NULL "
        f"WHEN {gap}.gap_date IS NOT NULL THEN {gap}.gap_category "
        f"ELSE 'contract_not_in_span_slot' END AS {output}"
    )


def _source_status(alias: str, gap: str, output: str) -> str:
    return (
        f"CASE WHEN {alias}.date IS NOT NULL THEN 'AVAILABLE_CANONICAL' "
        f"WHEN {gap}.gap_date IS NOT NULL THEN {gap}.safe_downstream_status "
        f"ELSE 'CANONICAL_ARCHIVE_CONTRACT_ABSENT' END AS {output}"
    )


def _bod_exception_columns() -> str:
    return (
        ",".join(_BSM_KEY)
        + ",request_id,span_join_status,span_unmatched_reason,span_source_status,"
        "span_source_boundary,span_source_gap_reason"
    )


def _six_exception_columns() -> str:
    columns = [*_BSM_KEY, "request_id", "span_unmatched_slot_count"]
    for slot in SLOTS:
        prefix = f"span_{slot.lower()}"
        columns.extend(
            [
                f"{prefix}_join_status",
                f"{prefix}_source_status",
                f"{prefix}_source_boundary",
                f"{prefix}_source_gap_reason",
            ]
        )
    return ",".join(columns)


def _validate_final_span_release(
    *, span_root: Path, release_path: Path, handoff_path: Path, gap_path: Path
) -> dict[str, Any]:
    import pyarrow.parquet as pq

    actual_hashes = {
        "handoff": sha256_file(handoff_path),
        "release": sha256_file(release_path),
        "gap": sha256_file(gap_path),
    }
    expected_hashes = {
        "handoff": EXPECTED_HANDOFF_SHA256,
        "release": EXPECTED_RELEASE_SHA256,
        "gap": EXPECTED_GAP_SHA256,
    }
    if actual_hashes != expected_hashes:
        raise ValueError(f"final SPAN release hashes disagree: {actual_hashes}")
    handoff = _read_json(handoff_path)
    release = _read_json(release_path)
    if handoff.get("schema_version") != "dhan-span-handoff/v1":
        raise ValueError("unexpected Dhan SPAN handoff schema")
    if release.get("schema_version") != "span-phase1-release/v1":
        raise ValueError("unexpected final SPAN release schema")
    if handoff.get("release_status") != "ACCEPTED_WITH_SOURCE_GAPS":
        raise ValueError("Dhan handoff is not owner-accepted")
    if release.get("release_status") != "ACCEPTED_WITH_SOURCE_GAPS":
        raise ValueError("SPAN release is not owner-accepted")
    if handoff.get("technical_audit_outcome") != "BLOCKED_SOURCE":
        raise ValueError("Dhan handoff lost BLOCKED_SOURCE outcome")
    if release.get("technical_audit_outcome") != "BLOCKED_SOURCE":
        raise ValueError("SPAN release lost BLOCKED_SOURCE outcome")
    if release.get("repository_commit_sha") != EXPECTED_SPAN_COMMIT:
        raise ValueError("SPAN producer commit mismatch")
    if handoff.get("release_manifest", {}).get("sha256") != EXPECTED_RELEASE_SHA256:
        raise ValueError("handoff does not pin the supplied release manifest")
    if (
        handoff.get("source_gap_manifest", {}).get("parquet", {}).get("sha256")
        != EXPECTED_GAP_SHA256
    ):
        raise ValueError("handoff does not pin the supplied gap manifest")
    if (
        release.get("source_gap_artifacts", {}).get("parquet", {}).get("sha256")
        != EXPECTED_GAP_SHA256
    ):
        raise ValueError("release does not pin the supplied gap manifest")
    expected_counts = {
        "monthly_parquets": 67,
        "compacted_rows": 24_870_123,
        "natural_key_duplicates": 0,
        "expected_cells": 12_132,
        "accounted_cells": 12_132,
        "source_gap_manifest_rows": 3_993,
        "accepted_unavailable_cells": 3_941,
        "repeated_corrupt_source_cells": 52,
        "source_boundary_cells": 93,
        "orphan_partial_files": 0,
        "unresolved_non_source_boundary_cells": 0,
    }
    counts = release.get("counts") or {}
    if any(counts.get(key) != value for key, value in expected_counts.items()):
        raise ValueError("final SPAN release count contract mismatch")
    integrity = release.get("integrity_contract") or {}
    if not integrity or not all(value is True for value in integrity.values()):
        raise ValueError("final SPAN integrity contract is incomplete")
    handoff_items = {item["month"]: item for item in handoff["monthly_inventory"]}
    release_items = {item["month"]: item for item in release["monthly_inventory"]}
    if len(handoff_items) != 67 or handoff_items != release_items:
        raise ValueError("handoff/release monthly inventories disagree")
    months: dict[str, dict[str, Any]] = {}
    for month, item in sorted(release_items.items()):
        path = Path(item["path"]).resolve()
        if path.parent != span_root or not path.is_file():
            raise ValueError(f"SPAN release path mismatch for {month}")
        if int(item.get("natural_key_duplicates", -1)) != 0:
            raise ValueError(f"SPAN producer duplicate key claim for {month}")
        if path.stat().st_size != int(item["size_bytes"]):
            raise ValueError(f"SPAN size mismatch for {month}")
        if sha256_file(path) != item["sha256"]:
            raise ValueError(f"SPAN hash mismatch for {month}")
        if pq.ParquetFile(path).metadata.num_rows != int(item["row_count"]):
            raise ValueError(f"SPAN Parquet metadata mismatch for {month}")
        months[month] = dict(item)
    gap_table = pq.read_table(
        gap_path,
        columns=[
            "trading_date",
            "slot",
            "classification_outcome",
            "source_boundary_proven",
            "final_download_state",
        ],
    )
    gap_rows = gap_table.to_pylist()
    if len(gap_rows) != 3_993:
        raise ValueError("source-gap row count mismatch")
    if len({(row["trading_date"], row["slot"]) for row in gap_rows}) != 3_993:
        raise ValueError("source-gap date/slot keys are duplicated")
    corrupt = sum(
        row["final_download_state"] == "corrupt_inner_zip" for row in gap_rows
    )
    accepted = len(gap_rows) - corrupt
    boundaries = sum(row["source_boundary_proven"] is True for row in gap_rows)
    if (accepted, corrupt, boundaries) != (3_941, 52, 93):
        raise ValueError("source-gap classification totals mismatch")
    return {
        "release_status": release["release_status"],
        "technical_outcome": release["technical_audit_outcome"],
        "producer_commit": release["repository_commit_sha"],
        "months": months,
        "gap_counts": {
            "total": 3_993,
            "accepted_unavailable": accepted,
            "corrupt_http_200": corrupt,
            "source_boundaries": boundaries,
        },
    }


def _terminal_audit(
    *,
    selected: Sequence[str],
    full_scope: bool,
    results: Sequence[dict[str, Any]],
    bsm_audit: dict[str, Any],
    release: dict[str, Any],
    bsm_audit_path: Path,
    release_path: Path,
    handoff_path: Path,
    gap_path: Path,
    bod_root: Path,
    six_root: Path,
) -> dict[str, Any]:
    bod = _aggregate_representation(results, "bod")
    six = _aggregate_representation(results, "six_slot")
    expected_status = Counter()
    for item in bsm_audit["months_audited"]:
        if item["month"] in selected:
            expected_status.update(item["status_counts"])
    errors: list[str] = []
    expected_rows = sum(
        int(item["rows"])
        for item in bsm_audit["months_audited"]
        if item["month"] in selected
    )
    for label, aggregate, root in (("bod", bod, bod_root), ("six_slot", six, six_root)):
        if aggregate["output_rows"] != expected_rows:
            errors.append(f"{label}_row_conservation")
        if aggregate["bsm_status_counts"] != dict(expected_status):
            errors.append(f"{label}_bsm_status_changed")
        if aggregate["duplicate_span_keys"] or aggregate["duplicate_gap_keys"]:
            errors.append(f"{label}_right_key_duplicates")
        if aggregate["primary_key_duplicate_rows"]:
            errors.append(f"{label}_primary_key_multiplication")
        if aggregate["cross_key_violation_rows"]:
            errors.append(f"{label}_cross_key_violation")
        partials = [str(path) for path in root.rglob("*.partial")]
        aggregate["orphan_partial_paths"] = partials
        if partials:
            errors.append(f"{label}_orphan_partials")
    bod_status = bod["slot_status_counts"].get("span_join_status", {})
    bod["matched_rows"] = int(bod_status.get("matched", 0))
    bod["unmatched_rows"] = expected_rows - bod["matched_rows"]
    if full_scope:
        if expected_rows != EXPECTED_ROWS:
            errors.append("full_input_row_total")
        if dict(expected_status) != EXPECTED_BSM_STATUS:
            errors.append("full_bsm_status_contract")
        if bod["matched_rows"] != EXPECTED_BOD_MATCHED:
            errors.append("bod_matched_baseline")
        if bod["unmatched_rows"] != EXPECTED_BOD_UNMATCHED:
            errors.append("bod_unmatched_baseline")
    status = "FAIL" if errors else ("PASS" if full_scope else "PILOT_PASS")
    audit = {
        "schema": "dhan_span_final_release_terminal_audit",
        "schema_version": SIX_SLOT_RELEASE_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "full_scope": full_scope,
        "months": len(selected),
        "expected_months": 67,
        "input_rows": expected_rows,
        "errors": errors,
        "bsm_status_counts_expected": dict(expected_status),
        "bod": bod,
        "six_slot": six,
        "source_gap_coverage": release["gap_counts"],
        "span_release_status": release["release_status"],
        "span_technical_audit_outcome": release["technical_outcome"],
        "publication_timing_proven": False,
        "minute_asof_join_performed": False,
        "bsm_terminal_audit": _lineage(bsm_audit_path),
        "span_release_manifest": _lineage(release_path),
        "span_handoff": _lineage(handoff_path),
        "span_source_gap_manifest": _lineage(gap_path),
        "historical_minute_futures_status": "BLOCKED_SOURCE_DHAN_EXPIRED_MINUTE_EMPTY",
    }
    markdown = _terminal_markdown(audit)
    for root in (bod_root, six_root):
        _atomic_json(root / "manifests" / "span_release_terminal_audit.json", audit)
        _atomic_text(root / "manifests" / "span_release_terminal_audit.md", markdown)
        _atomic_json(
            root / "manifests" / "source_gap_coverage_audit.json",
            {
                "schema": "dhan_span_source_gap_coverage_audit",
                "schema_version": SIX_SLOT_RELEASE_VERSION,
                "status": status,
                "input_gap_cells": release["gap_counts"],
                "joined_row_status_counts": (
                    bod["slot_status_counts"]
                    if root == bod_root
                    else six["slot_status_counts"]
                ),
            },
        )
    return audit


def _aggregate_representation(
    results: Sequence[dict[str, Any]], key: str
) -> dict[str, Any]:
    aggregate: dict[str, Any] = {
        "output_rows": 0,
        "exception_rows": 0,
        "output_bytes": 0,
        "exception_bytes": 0,
        "duplicate_span_keys": 0,
        "duplicate_gap_keys": 0,
        "primary_key_duplicate_rows": 0,
        "cross_key_violation_rows": 0,
        "bsm_status_counts": {},
        "slot_status_counts": {},
        "month_manifests": [],
    }
    bsm_counts: Counter[str] = Counter()
    slot_counts: dict[str, Counter[str]] = {}
    for result in results:
        item = result[key]
        for name in (
            "output_rows",
            "exception_rows",
            "output_bytes",
            "exception_bytes",
            "duplicate_span_keys",
            "duplicate_gap_keys",
            "primary_key_duplicate_rows",
            "cross_key_violation_rows",
        ):
            aggregate[name] += int(item[name])
        bsm_counts.update(item["bsm_status_counts"])
        for column, counts in item["slot_status_counts"].items():
            slot_counts.setdefault(column, Counter()).update(counts)
        aggregate["month_manifests"].append(
            {
                "month": item["month"],
                "manifest_path": item["manifest_path"],
                "manifest_sha256": item["manifest_sha256"],
                "output_sha256": item["output_sha256"],
                "exception_sha256": item["exception_sha256"],
            }
        )
    aggregate["bsm_status_counts"] = dict(bsm_counts)
    aggregate["slot_status_counts"] = {
        column: dict(counts) for column, counts in slot_counts.items()
    }
    return aggregate


def _paths(root: Path, month: str) -> dict[str, Path]:
    year, number = month.split("-")
    return {
        "root": root,
        "output": root
        / "gold"
        / f"year={year}"
        / f"month={number}"
        / "part-000.parquet",
        "exception": root
        / "exceptions"
        / "unmatched_span"
        / f"year={year}"
        / f"month={number}"
        / "part-000.parquet",
        "manifest": root / "manifests" / "months" / f"month={month}.json",
    }


def _resume_manifest(paths: dict[str, Path], lineage_sha: str) -> dict[str, Any] | None:
    manifest_path = paths["manifest"]
    if not manifest_path.is_file():
        return None
    payload = _read_json(manifest_path)
    if payload.get("lineage_sha256") != lineage_sha:
        return None
    for label in ("output", "exception"):
        path = paths[label]
        if not path.is_file() or sha256_file(path) != payload.get(f"{label}_sha256"):
            return None
    import pyarrow.parquet as pq

    if pq.ParquetFile(paths["output"]).metadata.num_rows != int(payload["output_rows"]):
        return None
    if pq.ParquetFile(paths["exception"]).metadata.num_rows != int(
        payload["exception_rows"]
    ):
        return None
    payload["resumed"] = True
    payload["manifest_path"] = str(manifest_path)
    payload["manifest_sha256"] = sha256_file(manifest_path)
    return payload


def _quarantine_stale(paths: dict[str, Path]) -> None:
    existing = [
        paths[name]
        for name in ("output", "exception", "manifest")
        if paths[name].exists()
    ]
    if not existing:
        return
    month = paths["manifest"].stem.removeprefix("month=")
    quarantine = (
        paths["root"]
        / "quarantine"
        / "stale_publications"
        / f"{month}.{uuid.uuid4().hex}"
    )
    quarantine.mkdir(parents=True, exist_ok=True)
    for path in existing:
        os.replace(path, quarantine / path.name)


def _counts(connection: Any, relation: str, column: str) -> dict[str, int]:
    rows = connection.execute(
        f'SELECT "{column}",count(*) FROM {relation} GROUP BY 1'
    ).fetchall()
    return {str(key): int(value) for key, value in rows}


def _publish_status(
    bod_root: Path,
    six_root: Path,
    selected: Sequence[str],
    results: Sequence[dict[str, Any]],
    state: str,
    *,
    terminal_status: str | None = None,
) -> None:
    status = {
        "schema": "dhan_span_final_release_status",
        "schema_version": SIX_SLOT_RELEASE_VERSION,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "terminal_status": terminal_status,
        "months_completed": len(results),
        "months_total": len(selected),
        "rows_completed": sum(int(item["bod"]["output_rows"]) for item in results),
        "current_month": results[-1]["month"] if results else selected[0],
        "bod_output_root": str(bod_root),
        "six_slot_output_root": str(six_root),
    }
    for root in (bod_root, six_root):
        _atomic_json(root / "manifests" / "span_release_status.json", status)


def _terminal_markdown(audit: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Final Dhan plus SPAN Release Terminal Audit",
            "",
            f"- Status: `{audit['status']}`",
            f"- Months: {audit['months']}/{audit['expected_months']}",
            f"- Dhan input rows: {audit['input_rows']:,}",
            f"- BOD matched: {audit['bod']['matched_rows']:,}",
            f"- BOD unmatched: {audit['bod']['unmatched_rows']:,}",
            f"- Source-gap cells: {audit['source_gap_coverage']['total']:,}",
            f"- Accepted unavailable: {audit['source_gap_coverage']['accepted_unavailable']:,}",
            f"- Corrupt HTTP-200: {audit['source_gap_coverage']['corrupt_http_200']:,}",
            f"- Proven source boundaries: {audit['source_gap_coverage']['source_boundaries']:,}",
            "- Publication timing proven: no",
            "- Minute-level SPAN as-of join performed: no",
            f"- Errors: {audit['errors']}",
            "",
        ]
    )


def _validate_config(config: SpanReleaseConfig) -> None:
    if (
        config.threads < 1
        or config.row_group_size < 1
        or not config.memory_limit.strip()
    ):
        raise ValueError("invalid SPAN release runtime configuration")
