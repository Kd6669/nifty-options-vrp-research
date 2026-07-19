"""Point-in-time strict and six-slot research SPAN timing releases.

The official NSE Clearing schedule is a reference-price schedule, not evidence
of archive availability.  Historical rows therefore enter the strict as-of
representation only when an explicitly zoned file-created timestamp or an
archive-SHA first-seen observation proves availability.
"""

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
    _fsync_file,
    _json_sha,
    _read_json,
    _sql_literal,
    _sql_text,
    sha256_file,
)
from .span_release import EXPECTED_BSM_STATUS, EXPECTED_ROWS, SLOTS, _BSM_KEY


STRICT_VERSION = "1.0.0"
RESEARCH_VERSION = "2.1.0"
BASE_VERSION = "2.0.0"
STRICT_POLICY = "PROVEN_EFFECTIVE_TIMESTAMP_BACKWARD_ASOF_ONLY"
RESEARCH_POLICY = "SIX_SLOT_REFERENCE_SCHEDULE_RESEARCH_ONLY"
OFFICIAL_TIMING_URL = (
    "https://www.nseclearing.in/sites/default/files/2025-08/"
    "NCL%20-%20FAQ%20RISK%20MANAGEMENT.pdf"
)
OFFICIAL_TIMING_SHA256 = (
    "ae443f77d0202eeda8b2b07fd17defe344688bc044b265e6880d3197cdd7986c"
)
REFERENCE_TIMES = {
    "BOD": None,
    "ID1": "11:00:00",
    "ID2": "12:30:00",
    "ID3": "14:00:00",
    "ID4": "15:30:00",
    "EOD": None,
}
_MONTH = re.compile(r"^\d{4}-\d{2}$")
_TIMING_FIELDS = (
    "reference_ts_ist",
    "file_created_ts_ist",
    "first_seen_ts_ist",
    "effective_ts_ist",
    "timing_source",
    "timing_confidence",
    "age_seconds",
)
_SELECTED_SPAN_FIELDS = (
    "date",
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
    *(f"s{index}" for index in range(1, 17)),
    "composite_delta",
    "source_file",
    "source_sha256",
    "source_member",
    "source_status",
    "source_boundary",
    "source_gap_reason",
    "gap_classification_outcome",
    "source_boundary_category",
    "source_gap_evidence_basis",
    "source_gap_evidence_event_id",
    "effective_time_source",
    "span_effective_ts_ist",
    "span_file_created",
)


@dataclass(frozen=True)
class SpanTimingConfig:
    threads: int = 8
    memory_limit: str = "8GB"
    row_group_size: int = 250_000


@dataclass(frozen=True)
class SpanTimingStats:
    months_total: int
    months_processed: int
    months_resumed: int
    rows_total: int
    strict_matched_rows: int
    strict_output_root: str
    research_output_root: str
    terminal_status: str
    terminal_audit_path: str
    elapsed_seconds: float


def run_span_timing_release(
    *,
    base_six_slot_root: str | Path,
    strict_output_root: str | Path,
    research_output_root: str | Path,
    official_timing_document: str | Path,
    first_seen_manifest: str | Path | None = None,
    months: Sequence[str] | None = None,
    config: SpanTimingConfig | None = None,
    resume: bool = True,
) -> SpanTimingStats:
    """Publish immutable strict and reference-only research timing releases."""
    cfg = config or SpanTimingConfig()
    _validate_config(cfg)
    base_root = Path(base_six_slot_root).resolve()
    if base_root.name != f"version={BASE_VERSION}":
        raise ValueError(f"base six-slot input must be version={BASE_VERSION}")
    timing_document = Path(official_timing_document).resolve()
    if not timing_document.is_file():
        raise FileNotFoundError(timing_document)
    if sha256_file(timing_document) != OFFICIAL_TIMING_SHA256:
        raise ValueError("official NSE Clearing timing document hash mismatch")
    first_seen_path = (
        Path(first_seen_manifest).resolve() if first_seen_manifest else None
    )
    if first_seen_path is not None and not first_seen_path.is_file():
        raise FileNotFoundError(first_seen_path)
    strict_root = Path(strict_output_root).resolve() / f"version={STRICT_VERSION}"
    research_root = Path(research_output_root).resolve() / f"version={RESEARCH_VERSION}"

    base_months = _discover_base_months(base_root)
    selected = sorted(base_months) if months is None else list(dict.fromkeys(months))
    invalid = [month for month in selected if not _MONTH.fullmatch(month)]
    missing = [month for month in selected if month not in base_months]
    if not selected or invalid or missing:
        raise ValueError(
            f"invalid or unavailable months: invalid={invalid} missing={missing}"
        )

    started = time.monotonic()
    results: list[dict[str, Any]] = []
    processed = resumed_count = 0
    for month in selected:
        result = _run_month(
            month=month,
            base_manifest_path=base_months[month],
            strict_root=strict_root,
            research_root=research_root,
            timing_document=timing_document,
            first_seen_path=first_seen_path,
            config=cfg,
            resume=resume,
        )
        results.append(result)
        processed += int(not result["fully_resumed"])
        resumed_count += int(result["fully_resumed"])
        _publish_status(strict_root, research_root, selected, results, "running")

    audit = _terminal_audit(
        selected=selected,
        full_scope=len(selected) == 67 and len(base_months) == 67,
        results=results,
        strict_root=strict_root,
        research_root=research_root,
        timing_document=timing_document,
        first_seen_path=first_seen_path,
    )
    _publish_status(
        strict_root,
        research_root,
        selected,
        results,
        "complete",
        terminal_status=audit["status"],
    )
    if audit["status"] == "FAIL":
        raise RuntimeError(
            "SPAN timing release audit failed: " + ", ".join(audit["errors"])
        )
    return SpanTimingStats(
        months_total=len(selected),
        months_processed=processed,
        months_resumed=resumed_count,
        rows_total=int(audit["input_rows"]),
        strict_matched_rows=int(audit["strict"]["strict_matched_rows"]),
        strict_output_root=str(strict_root),
        research_output_root=str(research_root),
        terminal_status=str(audit["status"]),
        terminal_audit_path=str(
            strict_root / "manifests" / "span_timing_terminal_audit.json"
        ),
        elapsed_seconds=time.monotonic() - started,
    )


def _discover_base_months(root: Path) -> dict[str, Path]:
    discovered: dict[str, Path] = {}
    for path in sorted((root / "manifests" / "months").glob("month=*.json")):
        month = path.stem.removeprefix("month=")
        if _MONTH.fullmatch(month):
            discovered[month] = path.resolve()
    if not discovered:
        raise ValueError(f"no base six-slot manifests under {root}")
    return discovered


def _run_month(
    *,
    month: str,
    base_manifest_path: Path,
    strict_root: Path,
    research_root: Path,
    timing_document: Path,
    first_seen_path: Path | None,
    config: SpanTimingConfig,
    resume: bool,
) -> dict[str, Any]:
    base = _validate_base_manifest(base_manifest_path, month)
    base_output = Path(base["output_path"]).resolve()
    lineage_base = {
        "month": month,
        "code_sha256": sha256_file(Path(__file__)),
        "base_six_slot_manifest": _file_lineage(base_manifest_path),
        "base_six_slot_output": _file_lineage(base_output),
        "base_six_slot_lineage_sha256": base["lineage_sha256"],
        "official_timing_source": {
            "url": OFFICIAL_TIMING_URL,
            "document": _file_lineage(timing_document),
            "interpretation": "reference_price_schedule_not_file_arrival",
        },
        "first_seen_manifest": _file_lineage(first_seen_path)
        if first_seen_path
        else None,
        "reference_schedule": REFERENCE_TIMES,
        "activation_rounding": "ceil_to_next_dhan_minute_never_backward",
        "config": {
            "threads": config.threads,
            "memory_limit": config.memory_limit,
            "row_group_size": config.row_group_size,
        },
    }
    research_lineage = {
        **lineage_base,
        "representation": "SIX_SLOT_RESEARCH",
        "version": RESEARCH_VERSION,
        "join_policy": RESEARCH_POLICY,
    }
    research_paths = _paths(research_root, month)
    research = None

    import duckdb

    connection = duckdb.connect()
    try:
        connection.execute(f"SET threads={int(config.threads)}")
        connection.execute(f"SET memory_limit='{_sql_text(config.memory_limit)}'")
        connection.execute("SET TimeZone='Asia/Kolkata'")
        connection.execute("SET preserve_insertion_order=false")
        temp_directory = research_root / "tmp" / f"month={month}"
        temp_directory.mkdir(parents=True, exist_ok=True)
        connection.execute(f"SET temp_directory='{_sql_text(str(temp_directory))}'")
        connection.execute(
            "CREATE TEMP VIEW base AS SELECT * FROM read_parquet("
            f"{_sql_literal(str(base_output))}, hive_partitioning=false)"
        )
        _create_first_seen_view(connection, first_seen_path)
        base_columns = [
            row[0] for row in connection.execute("DESCRIBE base").fetchall()
        ]
        _validate_base_columns(base_columns)
        historical_no_proof = (
            first_seen_path is None
            and int(
                connection.execute(
                    "SELECT count(*) FROM base WHERE "
                    + " OR ".join(
                        f"coalesce(span_{slot.lower()}_span_file_created,'')<>''"
                        for slot in SLOTS
                    )
                ).fetchone()[0]
            )
            == 0
        )
        research_lineage["historical_no_proof_fast_path"] = historical_no_proof
        if historical_no_proof:
            connection.execute("SET preserve_insertion_order=true")
        research = (
            _resume_manifest(research_paths, _json_sha(research_lineage))
            if resume
            else None
        )
        if research is None:
            research = _materialize_research(
                connection=connection,
                month=month,
                paths=research_paths,
                lineage=research_lineage,
                select_sql=(
                    _research_no_proof_sql() if historical_no_proof else _research_sql()
                ),
                expected_rows=int(base["output_rows"]),
                base_output=base_output,
                expected_base_sha=str(base["output_sha256"]),
                config=config,
            )

        strict_lineage = {
            **lineage_base,
            "representation": "POINT_IN_TIME_STRICT",
            "version": STRICT_VERSION,
            "join_policy": STRICT_POLICY,
            "research_month_manifest_sha256": research["manifest_sha256"],
            "research_output_sha256": research["output_sha256"],
            "historical_no_proof_fast_path": historical_no_proof,
        }
        strict_paths = _paths(strict_root, month)
        strict = (
            _resume_manifest(strict_paths, _json_sha(strict_lineage))
            if resume
            else None
        )
        if strict is None:
            if historical_no_proof:
                strict_sql = _strict_no_proof_sql(base_columns)
            else:
                connection.execute(
                    "CREATE OR REPLACE TEMP VIEW research AS SELECT * FROM read_parquet("
                    f"{_sql_literal(str(research_paths['output']))}, hive_partitioning=false)"
                )
                strict_sql = _strict_sql(base_columns)
            strict = _materialize_strict(
                connection=connection,
                month=month,
                paths=strict_paths,
                lineage=strict_lineage,
                select_sql=strict_sql,
                expected_rows=int(base["output_rows"]),
                research_output=research_paths["output"],
                expected_research_sha=str(research["output_sha256"]),
                config=config,
            )
    finally:
        connection.close()
    return {
        "month": month,
        "fully_resumed": bool(research["resumed"] and strict["resumed"]),
        "research": research,
        "strict": strict,
    }


def _validate_base_manifest(path: Path, month: str) -> dict[str, Any]:
    import pyarrow.parquet as pq

    payload = _read_json(path)
    if payload.get("schema_version") != BASE_VERSION:
        raise ValueError(f"base manifest version mismatch for {month}")
    if (
        payload.get("month") != month
        or payload.get("representation") != "SIX_SLOT_WIDE"
    ):
        raise ValueError(f"base manifest identity mismatch for {month}")
    if payload.get("lineage_sha256") != _json_sha(payload.get("lineage")):
        raise ValueError(f"base lineage hash mismatch for {month}")
    output = Path(payload["output_path"]).resolve()
    if not output.is_file() or sha256_file(output) != payload.get("output_sha256"):
        raise ValueError(f"base output hash mismatch for {month}")
    if pq.ParquetFile(output).metadata.num_rows != int(payload["output_rows"]):
        raise ValueError(f"base Parquet metadata mismatch for {month}")
    if payload.get("input_rows") != payload.get("output_rows"):
        raise ValueError(f"base row conservation mismatch for {month}")
    if any(
        int(payload.get(key, -1))
        for key in (
            "duplicate_span_keys",
            "duplicate_gap_keys",
            "primary_key_duplicate_rows",
            "cross_key_violation_rows",
        )
    ):
        raise ValueError(f"base join invariant mismatch for {month}")
    return payload


def _create_first_seen_view(connection: Any, path: Path | None) -> None:
    if path is None:
        connection.execute(
            "CREATE TEMP TABLE first_seen(trading_date DATE,time_slot VARCHAR,"
            "source_sha256 VARCHAR,first_seen_ts_ist TIMESTAMPTZ)"
        )
        return
    literal = _sql_literal(str(path))
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        source = f"read_parquet({literal}, hive_partitioning=false)"
    elif suffix in {".json", ".jsonl", ".ndjson"}:
        source = f"read_json_auto({literal})"
    else:
        raise ValueError("first-seen manifest must be Parquet or JSON/JSONL")
    connection.execute(
        "CREATE TEMP VIEW first_seen_raw AS SELECT CAST(trading_date AS DATE) trading_date,"
        "CAST(time_slot AS VARCHAR) time_slot,CAST(source_sha256 AS VARCHAR) source_sha256,"
        f"CAST(first_seen_ts_ist AS TIMESTAMPTZ) first_seen_ts_ist FROM {source}"
    )
    invalid = int(
        connection.execute(
            "SELECT count(*) FROM first_seen_raw WHERE time_slot NOT IN "
            "('BOD','ID1','ID2','ID3','ID4','EOD') OR source_sha256 !~ '^[0-9a-f]{64}$' "
            "OR first_seen_ts_ist IS NULL OR CAST(first_seen_ts_ist AS DATE)<trading_date"
        ).fetchone()[0]
    )
    duplicates = int(
        connection.execute(
            "SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM first_seen_raw "
            "GROUP BY trading_date,time_slot,source_sha256 HAVING n>1)"
        ).fetchone()[0]
    )
    if invalid or duplicates:
        raise ValueError(
            f"invalid first-seen evidence: invalid={invalid} duplicates={duplicates}"
        )
    connection.execute("CREATE TEMP VIEW first_seen AS SELECT * FROM first_seen_raw")


def _research_sql() -> str:
    joins: list[str] = []
    evidence: list[str] = ["b.*"]
    activated: list[str] = ["e.*"]
    hidden: list[str] = []
    for slot in SLOTS:
        slug = slot.lower()
        prefix = f"span_{slug}"
        fs = f"fs_{slug}"
        ref = _reference_expr(slot)
        raw_created = f"try_cast(b.{prefix}_span_file_created AS TIMESTAMPTZ)"
        eod_created_floor = (
            " AND "
            + raw_created
            + ">=timezone('Asia/Kolkata',CAST(b.trade_date AS TIMESTAMP)+INTERVAL '15:30:00')"
            if slot == "EOD"
            else ""
        )
        valid_created = (
            f"CASE WHEN regexp_matches(coalesce(b.{prefix}_span_file_created,''),"
            "'(Z|[+-][0-9]{2}:[0-9]{2})$') "
            f"AND CAST({raw_created} AS DATE)=b.trade_date{eod_created_floor} "
            f"THEN {raw_created} END"
        )
        eod_seen_floor = (
            f" AND {fs}.first_seen_ts_ist>=timezone('Asia/Kolkata',"
            "CAST(b.trade_date AS TIMESTAMP)+INTERVAL '15:30:00')"
            if slot == "EOD"
            else ""
        )
        valid_seen = (
            f"CASE WHEN CAST({fs}.first_seen_ts_ist AS DATE)=b.trade_date "
            f"{eod_seen_floor} THEN {fs}.first_seen_ts_ist END"
        )
        evidence.extend(
            [
                f"{ref} AS __{slug}_reference",
                f"{valid_created} AS __{slug}_created",
                f"{valid_seen} AS __{slug}_seen",
            ]
        )
        hidden.extend(
            [
                f"__{slug}_reference",
                f"__{slug}_created",
                f"__{slug}_seen",
                f"__{slug}_effective",
            ]
        )
        joins.append(
            f" LEFT JOIN first_seen {fs} ON b.trade_date={fs}.trading_date "
            f"AND {fs}.time_slot='{slot}' AND b.{prefix}_source_sha256={fs}.source_sha256"
        )
        raw_effective = (
            f"CASE WHEN e.__{slug}_created IS NOT NULL OR e.__{slug}_seen IS NOT NULL "
            f"THEN greatest(e.__{slug}_reference,e.__{slug}_created,e.__{slug}_seen) END"
        )
        activated.append(f"{_ceil_minute(raw_effective)} AS __{slug}_effective")

    final_columns = [f"a.* EXCLUDE ({','.join(hidden)})"]
    for slot in SLOTS:
        slug = slot.lower()
        prefix = f"span_{slug}"
        final_columns.extend(
            [
                f"a.__{slug}_reference AS {prefix}_reference_ts_ist",
                f"a.__{slug}_created AS {prefix}_file_created_ts_ist",
                f"a.__{slug}_seen AS {prefix}_first_seen_ts_ist",
                f"a.__{slug}_effective AS {prefix}_effective_ts_ist",
                f"CASE WHEN a.__{slug}_seen IS NOT NULL THEN 'nse_endpoint_first_seen_sha' "
                f"WHEN a.__{slug}_created IS NOT NULL THEN 'span_file_created' "
                f"ELSE 'official_reference_schedule' END AS {prefix}_timing_source",
                f"CASE WHEN a.__{slug}_seen IS NOT NULL THEN 'observed_first_seen' "
                f"WHEN a.__{slug}_created IS NOT NULL THEN 'file_created_proven' "
                f"ELSE 'reference_only' END AS {prefix}_timing_confidence",
                f"CASE WHEN a.__{slug}_effective<=a.timestamp_ist THEN "
                f"date_diff('second',a.__{slug}_effective,a.timestamp_ist) END AS {prefix}_age_seconds",
            ]
        )
    return (
        "WITH evidence AS (SELECT "
        + ",".join(evidence)
        + " FROM base b"
        + "".join(joins)
        + "), activated AS (SELECT "
        + ",".join(activated)
        + " FROM evidence e) SELECT "
        + ",".join(final_columns)
        + " FROM activated a ORDER BY "
        + ",".join(_BSM_KEY)
    )


def _research_no_proof_sql() -> str:
    """Project reference-only timing without joins or a redundant wide sort."""
    columns = ["b.*"]
    for slot in SLOTS:
        prefix = f"span_{slot.lower()}"
        columns.extend(
            [
                f"{_reference_expr(slot)} AS {prefix}_reference_ts_ist",
                f"CAST(NULL AS TIMESTAMPTZ) AS {prefix}_file_created_ts_ist",
                f"CAST(NULL AS TIMESTAMPTZ) AS {prefix}_first_seen_ts_ist",
                f"CAST(NULL AS TIMESTAMPTZ) AS {prefix}_effective_ts_ist",
                f"'official_reference_schedule' AS {prefix}_timing_source",
                f"'reference_only' AS {prefix}_timing_confidence",
                f"CAST(NULL AS BIGINT) AS {prefix}_age_seconds",
            ]
        )
    return "SELECT " + ",".join(columns) + " FROM base b"


def _strict_sql(base_columns: Sequence[str]) -> str:
    bsm_columns = list(base_columns[: base_columns.index("span_join_policy")])
    eligible = [
        f"CASE WHEN span_{slot.lower()}_timing_confidence<>'reference_only' "
        f"AND span_{slot.lower()}_effective_ts_ist<=timestamp_ist "
        f"THEN span_{slot.lower()}_effective_ts_ist END"
        for slot in SLOTS
    ]
    choice_cases = []
    for slot in reversed(SLOTS):
        slug = slot.lower()
        choice_cases.append(
            f"WHEN __strict_effective=span_{slug}_effective_ts_ist "
            f"AND span_{slug}_timing_confidence<>'reference_only' THEN '{slot}'"
        )
    selected = []
    for field in _SELECTED_SPAN_FIELDS:
        alias = (
            "span_file_created_raw" if field == "span_file_created" else f"span_{field}"
        )
        selected.append(_slot_case(field, alias))
    statuses = [f"span_{slot.lower()}_join_status" for slot in SLOTS]
    any_matched = " OR ".join(f"{column}='matched'" for column in statuses)
    any_gap = " OR ".join(f"{column}='source_gap'" for column in statuses)
    timing_selected = [
        _slot_case("reference_ts_ist", "span_reference_ts_ist"),
        _slot_case("file_created_ts_ist", "span_file_created_ts_ist"),
        _slot_case("first_seen_ts_ist", "span_first_seen_ts_ist"),
        "__strict_effective AS span_effective_ts_ist",
        _slot_case("timing_source", "span_timing_source", default="'none'"),
        _slot_case("timing_confidence", "span_timing_confidence", default="'unproven'"),
        "__strict_slot AS span_time_slot",
        "CASE WHEN __strict_effective IS NOT NULL THEN "
        "date_diff('second',__strict_effective,timestamp_ist) END AS span_age_seconds",
    ]
    passthrough = [
        "span_release_status",
        "span_technical_audit_outcome",
        "span_release_manifest_sha256",
        "span_handoff_sha256",
        "span_source_gap_manifest_sha256",
        "span_gold_lineage_sha256",
    ]
    columns = [f'"{name}"' for name in bsm_columns]
    columns.extend(
        [
            f"'{STRICT_POLICY}' AS span_join_policy",
            f"CASE WHEN __strict_slot IS NOT NULL THEN 'matched' WHEN {any_matched} "
            f"THEN 'timing_unproven' WHEN {any_gap} THEN 'source_gap' "
            "ELSE 'unmatched_contract' END AS span_join_status",
            "CASE WHEN __strict_slot IS NOT NULL THEN 'matched' ELSE 'unmatched' END "
            "AS span_enrichment_status",
            f"CASE WHEN __strict_slot IS NOT NULL THEN NULL WHEN {any_matched} "
            "THEN 'historical_arrival_timestamp_unproven' "
            f"WHEN {any_gap} THEN 'span_source_gap' ELSE 'contract_not_in_span_slots' END "
            "AS span_unmatched_reason",
            _static_count_expr("matched", "span_static_available_slot_count"),
            _static_count_expr("source_gap", "span_static_source_gap_slot_count"),
            _static_count_expr(
                "unmatched_contract", "span_static_unmatched_contract_slot_count"
            ),
            *selected,
            *timing_selected,
            *passthrough,
            "(__strict_slot IS NOT NULL) AS span_slot_publication_times_proven",
            "true AS span_intraday_asof_join_performed",
        ]
    )
    return (
        "WITH choice AS (SELECT r.*,greatest("
        + ",".join(eligible)
        + ") AS __strict_effective FROM research r), chosen AS (SELECT c.*,CASE "
        + " ".join(choice_cases)
        + " END AS __strict_slot FROM choice c) SELECT "
        + ",".join(columns)
        + " FROM chosen ORDER BY "
        + ",".join(_BSM_KEY)
    )


def _strict_no_proof_sql(base_columns: Sequence[str]) -> str:
    bsm_columns = list(base_columns[: base_columns.index("span_join_policy")])
    statuses = [f"span_{slot.lower()}_join_status" for slot in SLOTS]
    any_matched = " OR ".join(f"{column}='matched'" for column in statuses)
    any_gap = " OR ".join(f"{column}='source_gap'" for column in statuses)
    selected = []
    for field in _SELECTED_SPAN_FIELDS:
        alias = (
            "span_file_created_raw" if field == "span_file_created" else f"span_{field}"
        )
        selected.append(f"CAST(NULL AS {_span_field_type(field)}) AS {alias}")
    columns = [f'"{name}"' for name in bsm_columns]
    columns.extend(
        [
            f"'{STRICT_POLICY}' AS span_join_policy",
            f"CASE WHEN {any_matched} THEN 'timing_unproven' WHEN {any_gap} "
            "THEN 'source_gap' ELSE 'unmatched_contract' END AS span_join_status",
            "'unmatched' AS span_enrichment_status",
            f"CASE WHEN {any_matched} THEN 'historical_arrival_timestamp_unproven' "
            f"WHEN {any_gap} THEN 'span_source_gap' ELSE 'contract_not_in_span_slots' END "
            "AS span_unmatched_reason",
            _static_count_expr("matched", "span_static_available_slot_count"),
            _static_count_expr("source_gap", "span_static_source_gap_slot_count"),
            _static_count_expr(
                "unmatched_contract", "span_static_unmatched_contract_slot_count"
            ),
            *selected,
            "CAST(NULL AS TIMESTAMPTZ) AS span_reference_ts_ist",
            "CAST(NULL AS TIMESTAMPTZ) AS span_file_created_ts_ist",
            "CAST(NULL AS TIMESTAMPTZ) AS span_first_seen_ts_ist",
            "CAST(NULL AS TIMESTAMPTZ) AS span_effective_ts_ist",
            "'none' AS span_timing_source",
            "'unproven' AS span_timing_confidence",
            "CAST(NULL AS VARCHAR) AS span_time_slot",
            "CAST(NULL AS BIGINT) AS span_age_seconds",
            "span_release_status",
            "span_technical_audit_outcome",
            "span_release_manifest_sha256",
            "span_handoff_sha256",
            "span_source_gap_manifest_sha256",
            "span_gold_lineage_sha256",
            "false AS span_slot_publication_times_proven",
            "true AS span_intraday_asof_join_performed",
        ]
    )
    return "SELECT " + ",".join(columns) + " FROM base"


def _static_count_expr(status: str, alias: str) -> str:
    terms = [
        f"CASE WHEN span_{slot.lower()}_join_status='{status}' THEN 1 ELSE 0 END"
        for slot in SLOTS
    ]
    return "(" + "+".join(terms) + f") AS {alias}"


def _span_field_type(field: str) -> str:
    if field in {"date", "expiry"}:
        return "DATE"
    if field in {"span_effective_ts_ist"}:
        return "TIMESTAMPTZ"
    if field == "source_boundary":
        return "BOOLEAN"
    if field in {
        "strike",
        "price",
        "delta",
        "implied_vol",
        "price_scan_range",
        "vol_scan_range",
        "cvf",
        "composite_delta",
        *(f"s{index}" for index in range(1, 17)),
    }:
        return "DOUBLE"
    return "VARCHAR"


def _slot_case(field: str, alias: str, *, default: str = "NULL") -> str:
    cases = " ".join(
        f"WHEN '{slot}' THEN span_{slot.lower()}_{field}" for slot in SLOTS
    )
    return f"CASE __strict_slot {cases} ELSE {default} END AS {alias}"


def _reference_expr(slot: str) -> str:
    reference = REFERENCE_TIMES[slot]
    if reference is None:
        return "CAST(NULL AS TIMESTAMPTZ)"
    return (
        "timezone('Asia/Kolkata',CAST(b.trade_date AS TIMESTAMP)+INTERVAL '"
        + reference
        + "')"
    )


def _ceil_minute(expression: str) -> str:
    return (
        f"CASE WHEN {expression} IS NOT NULL THEN date_trunc('minute',{expression})+"
        f"CASE WHEN {expression}>date_trunc('minute',{expression}) "
        "THEN INTERVAL '1 minute' ELSE INTERVAL '0 minute' END END"
    )


def _materialize_research(
    *,
    connection: Any,
    month: str,
    paths: dict[str, Path],
    lineage: dict[str, Any],
    select_sql: str,
    expected_rows: int,
    base_output: Path,
    expected_base_sha: str,
    config: SpanTimingConfig,
) -> dict[str, Any]:
    return _materialize(
        connection=connection,
        month=month,
        representation="SIX_SLOT_RESEARCH",
        paths=paths,
        lineage=lineage,
        select_sql=select_sql,
        expected_rows=expected_rows,
        input_path=base_output,
        expected_input_sha=expected_base_sha,
        config=config,
    )


def _materialize_strict(
    *,
    connection: Any,
    month: str,
    paths: dict[str, Path],
    lineage: dict[str, Any],
    select_sql: str,
    expected_rows: int,
    research_output: Path,
    expected_research_sha: str,
    config: SpanTimingConfig,
) -> dict[str, Any]:
    return _materialize(
        connection=connection,
        month=month,
        representation="POINT_IN_TIME_STRICT",
        paths=paths,
        lineage=lineage,
        select_sql=select_sql,
        expected_rows=expected_rows,
        input_path=research_output,
        expected_input_sha=expected_research_sha,
        config=config,
    )


def _materialize(
    *,
    connection: Any,
    month: str,
    representation: str,
    paths: dict[str, Path],
    lineage: dict[str, Any],
    select_sql: str,
    expected_rows: int,
    input_path: Path,
    expected_input_sha: str,
    config: SpanTimingConfig,
) -> dict[str, Any]:
    _quarantine_stale(paths)
    paths["output"].parent.mkdir(parents=True, exist_ok=True)
    paths["manifest"].parent.mkdir(parents=True, exist_ok=True)
    partial = paths["output"].with_name(
        f".{paths['output'].name}.{uuid.uuid4().hex}.partial"
    )
    try:
        _copy_parquet(connection, select_sql, partial, config.row_group_size)
        _fsync_file(partial)
        audit = _audit_output(connection, partial, representation, expected_rows)
        if audit["output_rows"] != expected_rows or audit["primary_key_duplicate_rows"]:
            raise RuntimeError(f"row/key invariant failed for {representation} {month}")
        if sha256_file(input_path) != expected_input_sha:
            raise RuntimeError(f"input changed during {representation} {month}")
        os.replace(partial, paths["output"])
    finally:
        partial.unlink(missing_ok=True)
    manifest = {
        "schema": "dhan_span_timing_release_month_manifest",
        "schema_version": STRICT_VERSION
        if representation == "POINT_IN_TIME_STRICT"
        else RESEARCH_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "month": month,
        "representation": representation,
        "lineage": lineage,
        "lineage_sha256": _json_sha(lineage),
        "input_rows": expected_rows,
        "output_rows": audit["output_rows"],
        "row_conservation": audit["output_rows"] == expected_rows,
        "primary_key_duplicate_rows": audit["primary_key_duplicate_rows"],
        "bsm_status_counts": audit["bsm_status_counts"],
        "timing_audit": audit["timing_audit"],
        "timing_coverage": audit["timing_coverage"],
        "output_path": str(paths["output"]),
        "output_sha256": sha256_file(paths["output"]),
        "output_bytes": paths["output"].stat().st_size,
        "resumed": False,
    }
    _atomic_json(paths["manifest"], manifest)
    manifest["manifest_path"] = str(paths["manifest"])
    manifest["manifest_sha256"] = sha256_file(paths["manifest"])
    return manifest


def _audit_output(
    connection: Any, output: Path, representation: str, expected_rows: int
) -> dict[str, Any]:
    relation = f"read_parquet({_sql_literal(str(output))}, hive_partitioning=false)"
    rows = int(connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])
    key_columns = ",".join(_BSM_KEY)
    duplicates = int(
        connection.execute(
            f"SELECT coalesce(sum(n-1),0) FROM (SELECT count(*) n FROM {relation} "
            f"GROUP BY {key_columns} HAVING n>1)"
        ).fetchone()[0]
    )
    bsm = _counts(connection, relation, "bsm_status")
    if sum(bsm.values()) != expected_rows:
        raise RuntimeError("BSM status accounting changed")
    if representation == "POINT_IN_TIME_STRICT":
        timing = {
            "strict_matched_rows": _scalar(
                connection, relation, "span_join_status='matched'"
            ),
            "future_effective_rows": _scalar(
                connection,
                relation,
                "span_effective_ts_ist IS NOT NULL AND span_effective_ts_ist>timestamp_ist",
            ),
            "reference_only_selected_rows": _scalar(
                connection, relation, "span_timing_confidence='reference_only'"
            ),
            "negative_age_rows": _scalar(connection, relation, "span_age_seconds<0"),
            "id1_early_rows": _slot_early(connection, relation, "ID1", "11:00:00"),
            "id2_early_rows": _slot_early(connection, relation, "ID2", "12:30:00"),
            "id3_early_rows": _slot_early(connection, relation, "ID3", "14:00:00"),
            "id4_early_rows": _slot_early(connection, relation, "ID4", "15:30:00"),
            "eod_before_close_rows": _slot_early(
                connection, relation, "EOD", "15:30:00"
            ),
        }
        coverage = _coverage(connection, relation, strict=True)
    else:
        timing = {
            **_research_source_timing_audit(connection, relation),
            "proven_effective_rows": sum(
                _scalar(
                    connection,
                    relation,
                    f"span_{slot.lower()}_effective_ts_ist IS NOT NULL",
                )
                for slot in SLOTS
            ),
        }
        coverage = _coverage(connection, relation, strict=False)
    return {
        "output_rows": rows,
        "primary_key_duplicate_rows": duplicates,
        "bsm_status_counts": bsm,
        "timing_audit": timing,
        "timing_coverage": coverage,
    }


def _research_source_timing_audit(connection: Any, relation: str) -> dict[str, int]:
    observations = []
    for order, slot in enumerate(SLOTS):
        prefix = f"span_{slot.lower()}"
        observations.append(
            f"SELECT DISTINCT trade_date,'{slot}' time_slot,{order} slot_order,"
            f"{prefix}_source_sha256 source_sha256,"
            f"{prefix}_span_file_created raw_created,"
            f"{prefix}_file_created_ts_ist created_ts,"
            f"{prefix}_reference_ts_ist reference_ts,"
            f"{prefix}_effective_ts_ist effective_ts FROM {relation} "
            f"WHERE {prefix}_join_status='matched'"
        )
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW timing_observations AS "
        + " UNION ALL ".join(observations)
    )
    (
        source_observations,
        raw_created,
        valid_created,
        invalid_created,
        floor_violations,
    ) = (
        int(value)
        for value in connection.execute(
            "SELECT count(*),"
            "count(*) FILTER (WHERE raw_created IS NOT NULL AND raw_created<>''),"
            "count(*) FILTER (WHERE created_ts IS NOT NULL),"
            "count(*) FILTER (WHERE raw_created IS NOT NULL AND raw_created<>'' "
            "AND created_ts IS NULL),"
            "count(*) FILTER (WHERE effective_ts IS NOT NULL AND reference_ts IS NOT NULL "
            "AND effective_ts<reference_ts) FROM timing_observations"
        ).fetchone()
    )
    conflicts = int(
        connection.execute(
            "SELECT count(*) FROM (SELECT trade_date,time_slot,source_sha256 "
            "FROM timing_observations WHERE created_ts IS NOT NULL GROUP BY ALL "
            "HAVING count(DISTINCT created_ts)>1)"
        ).fetchone()[0]
    )
    monotonic = int(
        connection.execute(
            "SELECT count(*) FROM timing_observations a JOIN timing_observations b "
            "ON a.trade_date=b.trade_date AND a.slot_order<b.slot_order "
            "WHERE a.created_ts IS NOT NULL AND b.created_ts IS NOT NULL "
            "AND a.created_ts>b.created_ts"
        ).fetchone()[0]
    )
    return {
        "source_observations": source_observations,
        "raw_created_source_observations": raw_created,
        "valid_created_source_observations": valid_created,
        "invalid_created_source_observations": invalid_created,
        "created_timestamp_conflicts": conflicts,
        "slot_monotonicity_violations": monotonic,
        "reference_floor_violations": floor_violations,
    }


def _coverage(connection: Any, relation: str, *, strict: bool) -> list[dict[str, Any]]:
    if strict:
        query = (
            "SELECT year(trade_date) AS coverage_year,"
            "coalesce(span_time_slot,'NONE') AS time_slot,"
            "span_timing_source timing_source,span_timing_confidence timing_confidence,"
            f"span_join_status,count(*) AS row_count FROM {relation} "
            "GROUP BY ALL ORDER BY ALL"
        )
        names = (
            "year",
            "time_slot",
            "timing_source",
            "timing_confidence",
            "join_status",
            "rows",
        )
        return [
            dict(zip(names, row, strict=True))
            for row in connection.execute(query).fetchall()
        ]
    unions = []
    for slot in SLOTS:
        prefix = f"span_{slot.lower()}"
        unions.append(
            f"SELECT year(trade_date) AS coverage_year,'{slot}' AS time_slot,"
            f"{prefix}_timing_source AS timing_source,"
            f"{prefix}_timing_confidence timing_confidence,{prefix}_join_status join_status,"
            "count(*) AS row_count "
            f"FROM {relation} GROUP BY ALL"
        )
    names = (
        "year",
        "time_slot",
        "timing_source",
        "timing_confidence",
        "join_status",
        "rows",
    )
    return [
        dict(zip(names, row, strict=True))
        for row in connection.execute(
            " UNION ALL ".join(unions) + " ORDER BY ALL"
        ).fetchall()
    ]


def _slot_early(connection: Any, relation: str, slot: str, time_text: str) -> int:
    return _scalar(
        connection,
        relation,
        f"span_time_slot='{slot}' AND timestamp_ist<"
        f"timezone('Asia/Kolkata',CAST(trade_date AS TIMESTAMP)+INTERVAL '{time_text}')",
    )


def _scalar(connection: Any, relation: str, predicate: str) -> int:
    return int(
        connection.execute(
            f"SELECT count(*) FROM {relation} WHERE {predicate}"
        ).fetchone()[0]
    )


def _terminal_audit(
    *,
    selected: Sequence[str],
    full_scope: bool,
    results: Sequence[dict[str, Any]],
    strict_root: Path,
    research_root: Path,
    timing_document: Path,
    first_seen_path: Path | None,
) -> dict[str, Any]:
    strict = _aggregate(results, "strict")
    research = _aggregate(results, "research")
    errors: list[str] = []
    expected_rows = strict["output_rows"]
    for label, aggregate in (("strict", strict), ("research", research)):
        if aggregate["output_rows"] != expected_rows:
            errors.append(f"{label}_row_conservation")
        if aggregate["primary_key_duplicate_rows"]:
            errors.append(f"{label}_primary_key_duplicates")
        if aggregate["bsm_status_counts"] != strict["bsm_status_counts"]:
            errors.append(f"{label}_bsm_status_changed")
    zero_keys = (
        "future_effective_rows",
        "reference_only_selected_rows",
        "negative_age_rows",
        "id1_early_rows",
        "id2_early_rows",
        "id3_early_rows",
        "id4_early_rows",
        "eod_before_close_rows",
    )
    if any(int(strict["timing_audit"].get(key, 0)) for key in zero_keys):
        errors.append("strict_timing_invariant")
    if first_seen_path is None and strict["strict_matched_rows"]:
        errors.append("unproven_historical_strict_match")
    if first_seen_path is None and research["timing_audit"].get(
        "proven_effective_rows"
    ):
        errors.append("unexpected_historical_timing_evidence")
    research_zero_keys = (
        "invalid_created_source_observations",
        "created_timestamp_conflicts",
        "slot_monotonicity_violations",
        "reference_floor_violations",
    )
    if any(int(research["timing_audit"].get(key, 0)) for key in research_zero_keys):
        errors.append("research_timing_evidence_invariant")
    partials = [
        str(path)
        for root in (strict_root, research_root)
        for path in root.rglob("*.partial")
    ]
    if partials:
        errors.append("orphan_partials")
    if full_scope:
        if len(selected) != 67 or expected_rows != EXPECTED_ROWS:
            errors.append("full_scope_count_contract")
        if strict["bsm_status_counts"] != EXPECTED_BSM_STATUS:
            errors.append("full_scope_bsm_status_contract")
    status = "FAIL" if errors else ("PASS" if full_scope else "PILOT_PASS")
    audit = {
        "schema": "dhan_span_timing_terminal_audit",
        "schema_version": RESEARCH_VERSION,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "full_scope": full_scope,
        "months": len(selected),
        "expected_months": 67,
        "input_rows": expected_rows,
        "errors": errors,
        "strict": strict,
        "research": research,
        "official_timing_source": {
            "url": OFFICIAL_TIMING_URL,
            "document": _file_lineage(timing_document),
            "policy": "reference schedule only; files are run shortly thereafter",
        },
        "first_seen_manifest": _file_lineage(first_seen_path)
        if first_seen_path
        else None,
        "historical_timing_conclusion": (
            "NO_PROVEN_FILE_CREATED_OR_FIRST_SEEN_TIMESTAMPS"
            if first_seen_path is None and strict["strict_matched_rows"] == 0
            else "PROVEN_TIMING_AVAILABLE"
        ),
        "orphan_partial_paths": partials,
    }
    markdown = _terminal_markdown(audit)
    for root in (strict_root, research_root):
        _atomic_json(root / "manifests" / "span_timing_terminal_audit.json", audit)
        _atomic_text(root / "manifests" / "span_timing_terminal_audit.md", markdown)
        _atomic_json(
            root / "manifests" / "span_timing_coverage_audit.json",
            {
                "schema": "dhan_span_timing_coverage_audit",
                "status": status,
                "strict": strict["timing_coverage"],
                "research": research["timing_coverage"],
            },
        )
    return audit


def _aggregate(results: Sequence[dict[str, Any]], key: str) -> dict[str, Any]:
    bsm: Counter[str] = Counter()
    timing: Counter[str] = Counter()
    coverage: list[dict[str, Any]] = []
    aggregate = {
        "output_rows": 0,
        "output_bytes": 0,
        "primary_key_duplicate_rows": 0,
        "strict_matched_rows": 0,
        "months": [],
    }
    for result in results:
        item = result[key]
        aggregate["output_rows"] += int(item["output_rows"])
        aggregate["output_bytes"] += int(item["output_bytes"])
        aggregate["primary_key_duplicate_rows"] += int(
            item["primary_key_duplicate_rows"]
        )
        bsm.update(item["bsm_status_counts"])
        timing.update(
            {name: int(value) for name, value in item["timing_audit"].items()}
        )
        coverage.extend(item["timing_coverage"])
        aggregate["months"].append(
            {
                "month": result["month"],
                "output_rows": item["output_rows"],
                "output_sha256": item["output_sha256"],
                "manifest_sha256": item["manifest_sha256"],
            }
        )
    aggregate["bsm_status_counts"] = dict(bsm)
    aggregate["timing_audit"] = dict(timing)
    aggregate["strict_matched_rows"] = int(timing.get("strict_matched_rows", 0))
    aggregate["timing_coverage"] = _merge_coverage(coverage)
    return aggregate


def _merge_coverage(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[Any, ...]] = Counter()
    for row in rows:
        key = (
            row["year"],
            row["time_slot"],
            row["timing_source"],
            row["timing_confidence"],
            row["join_status"],
        )
        counts[key] += int(row["rows"])
    return [
        {
            "year": key[0],
            "time_slot": key[1],
            "timing_source": key[2],
            "timing_confidence": key[3],
            "join_status": key[4],
            "rows": value,
        }
        for key, value in sorted(
            counts.items(), key=lambda item: tuple(str(v) for v in item[0])
        )
    ]


def _paths(root: Path, month: str) -> dict[str, Path]:
    year, number = month.split("-")
    return {
        "root": root,
        "output": root
        / "gold"
        / f"year={year}"
        / f"month={number}"
        / "part-000.parquet",
        "manifest": root / "manifests" / "months" / f"month={month}.json",
    }


def _resume_manifest(paths: dict[str, Path], lineage_sha: str) -> dict[str, Any] | None:
    import pyarrow.parquet as pq

    path = paths["manifest"]
    if not path.is_file():
        return None
    payload = _read_json(path)
    if payload.get("lineage_sha256") != lineage_sha:
        return None
    output = paths["output"]
    if not output.is_file() or sha256_file(output) != payload.get("output_sha256"):
        return None
    if pq.ParquetFile(output).metadata.num_rows != int(payload["output_rows"]):
        return None
    payload["resumed"] = True
    payload["manifest_path"] = str(path)
    payload["manifest_sha256"] = sha256_file(path)
    return payload


def _quarantine_stale(paths: dict[str, Path]) -> None:
    existing = [paths[name] for name in ("output", "manifest") if paths[name].exists()]
    partials = list(paths["output"].parent.glob(f".{paths['output'].name}.*.partial"))
    if not existing and not partials:
        return
    month = paths["manifest"].stem.removeprefix("month=")
    quarantine = (
        paths["root"]
        / "quarantine"
        / "stale_publications"
        / f"{month}.{uuid.uuid4().hex[:8]}"
    )
    quarantine.mkdir(parents=True, exist_ok=True)
    for label, path in (("output", paths["output"]), ("manifest", paths["manifest"])):
        if path.exists():
            suffix = ".parquet" if label == "output" else ".json"
            os.replace(path, quarantine / f"{label}{suffix}")
    for index, path in enumerate(partials):
        os.replace(path, quarantine / f"partial-{index}.partial.quarantined")


def _publish_status(
    strict_root: Path,
    research_root: Path,
    selected: Sequence[str],
    results: Sequence[dict[str, Any]],
    state: str,
    *,
    terminal_status: str | None = None,
) -> None:
    payload = {
        "schema": "dhan_span_timing_release_status",
        "schema_version": RESEARCH_VERSION,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "terminal_status": terminal_status,
        "months_completed": len(results),
        "months_total": len(selected),
        "current_month": results[-1]["month"] if results else None,
        "rows_completed": sum(int(item["strict"]["output_rows"]) for item in results),
        "strict_matched_rows": sum(
            int(item["strict"]["timing_audit"]["strict_matched_rows"])
            for item in results
        ),
        "strict_output_root": str(strict_root),
        "research_output_root": str(research_root),
    }
    for root in (strict_root, research_root):
        _atomic_json(root / "manifests" / "span_timing_status.json", payload)


def _terminal_markdown(audit: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# SPAN timing terminal audit",
            "",
            f"- Status: **{audit['status']}**",
            f"- Months: {audit['months']}/{audit['expected_months']}",
            f"- Rows: {audit['input_rows']:,}",
            f"- Strict matches: {audit['strict']['strict_matched_rows']:,}",
            f"- Historical timing: {audit['historical_timing_conclusion']}",
            f"- Strict duplicate keys: {audit['strict']['primary_key_duplicate_rows']}",
            f"- Research duplicate keys: {audit['research']['primary_key_duplicate_rows']}",
            f"- Orphan partials: {len(audit['orphan_partial_paths'])}",
            f"- Errors: {audit['errors']}",
            "",
            "The NSE Clearing schedule identifies reference-price times. It does not prove file arrival; "
            "the strict representation only selects explicitly proven effective/first-seen timestamps.",
            "",
        ]
    )


def _counts(connection: Any, relation: str, column: str) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in connection.execute(
            f'SELECT "{column}",count(*) FROM {relation} GROUP BY 1'
        ).fetchall()
    }


def _file_lineage(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return {
        "path": str(path),
        "sha256": sha256_file(path),
        "bytes": path.stat().st_size,
    }


def _validate_base_columns(columns: Sequence[str]) -> None:
    required = {
        "span_join_policy",
        "timestamp_ist",
        "trade_date",
        "bsm_status",
        *_BSM_KEY,
    }
    for slot in SLOTS:
        prefix = f"span_{slot.lower()}"
        required.update(
            {
                f"{prefix}_join_status",
                f"{prefix}_source_sha256",
                f"{prefix}_span_file_created",
                f"{prefix}_time_slot",
            }
        )
    missing = sorted(required.difference(columns))
    if missing:
        raise ValueError(f"base six-slot schema missing columns: {missing}")


def _validate_config(config: SpanTimingConfig) -> None:
    if not 1 <= config.threads <= 12:
        raise ValueError("threads must be in [1,12]")
    if not re.fullmatch(r"[1-9][0-9]*(MB|GB)", config.memory_limit):
        raise ValueError("memory_limit must be a positive MB/GB DuckDB limit")
    if not 10_000 <= config.row_group_size <= 1_000_000:
        raise ValueError("row_group_size out of bounds")
