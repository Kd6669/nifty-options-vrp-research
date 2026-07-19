"""Independent, read-only audit for the partitioned NIFTY BOD-SPAN gold release."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq


EXPECTED_ROWS = 43_018_677
EXPECTED_MONTHS = 67
DEFAULT_SESSION_CALENDAR = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "evidence"
    / "span_availability"
    / "reviewed_import_2021_2026.json"
)
SESSION_CALENDAR_SCHEMA = "span-availability-import/v1"
PRIMARY_KEY = (
    "timestamp_ist",
    "trade_date",
    "underlying",
    "expiry_flag",
    "expiry_code",
    "moneyness_label",
    "strike",
    "option_type",
)


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return value.as_posix()
    return str(value)


def _records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    columns = [item[0] for item in cursor.description]
    return [
        {key: value for key, value in zip(columns, row, strict=True)}
        for row in cursor.fetchall()
    ]


def _scalar(connection: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return connection.execute(sql).fetchone()[0]


def _safe_sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _discover(dataset_root: Path) -> tuple[list[Path], dict[str, Any]]:
    files = sorted(dataset_root.glob("year=*/month=*/part-*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"no year=*/month=*/part-*.parquet files found below {dataset_root}"
        )

    schemas: dict[str, int] = {}
    metadata_rows = 0
    row_groups = 0
    file_records: list[dict[str, Any]] = []
    corpus_digest = hashlib.sha256()
    for path in files:
        parquet = pq.ParquetFile(path)
        schema_text = str(parquet.schema_arrow)
        schema_sha = hashlib.sha256(schema_text.encode("utf-8")).hexdigest()
        schemas[schema_sha] = schemas.get(schema_sha, 0) + 1
        file_sha = sha256_file(path)
        relative = path.relative_to(dataset_root).as_posix()
        size = path.stat().st_size
        metadata_rows += parquet.metadata.num_rows
        row_groups += parquet.metadata.num_row_groups
        corpus_digest.update(f"{relative}\0{size}\0{file_sha}\n".encode())
        file_records.append(
            {
                "path": relative,
                "bytes": size,
                "rows": parquet.metadata.num_rows,
                "row_groups": parquet.metadata.num_row_groups,
                "columns": parquet.metadata.num_columns,
                "sha256": file_sha,
                "schema_sha256": schema_sha,
            }
        )

    return files, {
        "file_count": len(files),
        "total_bytes": sum(item["bytes"] for item in file_records),
        "metadata_rows": metadata_rows,
        "row_groups": row_groups,
        "schema_count": len(schemas),
        "schema_sha256_counts": schemas,
        "corpus_sha256": corpus_digest.hexdigest(),
        "files": file_records,
    }


def audit_trading_session_coverage(
    observed_dates: Iterable[date],
    *,
    first_date: date,
    last_date: date,
    calendar_evidence: Path,
) -> dict[str, Any]:
    """Reconcile observed dates to the retained official NSE F&O calendar evidence."""

    if first_date > last_date:
        raise ValueError(f"first date {first_date} must be <= last date {last_date}")
    evidence_path = calendar_evidence.resolve()
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    if payload.get("schema_version") != SESSION_CALENDAR_SCHEMA:
        raise ValueError(
            f"session calendar must use schema_version {SESSION_CALENDAR_SCHEMA!r}"
        )

    reviewed = payload.get("reviewed_coverage")
    if not isinstance(reviewed, dict):
        raise ValueError("session calendar requires reviewed_coverage")
    reviewed_start = date.fromisoformat(str(reviewed.get("date_from", "")))
    reviewed_end = date.fromisoformat(str(reviewed.get("date_to", "")))
    if first_date < reviewed_start or last_date > reviewed_end:
        raise ValueError(
            "dataset date range falls outside reviewed session-calendar coverage: "
            f"{first_date}..{last_date} versus {reviewed_start}..{reviewed_end}"
        )

    sources = payload.get("sources")
    if not isinstance(sources, list):
        raise ValueError("session calendar requires sources")
    source_ids = {str(item.get("id", "")) for item in sources if isinstance(item, dict)}

    weekly_rules = payload.get("weekly_rules")
    if not isinstance(weekly_rules, list):
        raise ValueError("session calendar requires weekly_rules")
    weekend_contracts = [
        item
        for item in weekly_rules
        if isinstance(item, dict)
        and item.get("market_state") == "closed"
        and item.get("classification") == "official_weekend"
        and set(item.get("weekdays", [])) == {"Saturday", "Sunday"}
        and date.fromisoformat(str(item.get("date_from", ""))) <= first_date
        and date.fromisoformat(str(item.get("date_to", ""))) >= last_date
    ]
    if len(weekend_contracts) != 1:
        raise ValueError(
            "session calendar must contain exactly one official weekend contract "
            "covering the audited dataset range"
        )
    if set(str(value) for value in weekend_contracts[0].get("source_ids", [])) - source_ids:
        raise ValueError("session calendar weekend contract references unknown sources")

    explicit_dates: dict[date, dict[str, Any]] = {}
    raw_dates = payload.get("dates")
    if not isinstance(raw_dates, list):
        raise ValueError("session calendar requires dates")
    for index, item in enumerate(raw_dates):
        if not isinstance(item, dict):
            raise ValueError(f"session calendar dates[{index}] must be an object")
        trading_date = date.fromisoformat(str(item.get("date", "")))
        if trading_date in explicit_dates:
            raise ValueError(f"duplicate session-calendar date {trading_date}")
        market_state = str(item.get("market_state", ""))
        if market_state not in {
            "closed",
            "regular_trading_day",
            "special_trading_session",
            "trading_source_boundary",
        }:
            raise ValueError(f"unsupported market_state {market_state!r} on {trading_date}")
        referenced_sources = set(str(value) for value in item.get("source_ids", []))
        if not referenced_sources or referenced_sources - source_ids:
            raise ValueError(
                f"session-calendar date {trading_date} lacks known source provenance"
            )
        explicit_dates[trading_date] = item

    expected: dict[date, dict[str, Any]] = {}
    closed_dates = 0
    cursor = first_date
    while cursor <= last_date:
        explicit = explicit_dates.get(cursor)
        if explicit is not None:
            market_state = str(explicit["market_state"])
            if market_state == "closed":
                closed_dates += 1
            else:
                expected[cursor] = {
                    "market_state": market_state,
                    "classification": str(
                        explicit.get("classification") or "official_trading_session"
                    ),
                    "reason": str(explicit.get("reason", "")),
                    "source_ids": [str(value) for value in explicit["source_ids"]],
                }
        elif cursor.weekday() < 5:
            expected[cursor] = {
                "market_state": "regular_trading_day",
                "classification": "official_regular_weekday",
                "reason": "Expected under the retained NSE F&O weekly trading contract.",
                "source_ids": [
                    str(value) for value in weekend_contracts[0].get("source_ids", [])
                ],
            }
        else:
            closed_dates += 1
        cursor += timedelta(days=1)

    observed = set(observed_dates)
    missing_dates = sorted(set(expected) - observed)
    unexpected_dates = sorted(observed - set(expected))

    def session_record(value: date, details: dict[str, Any]) -> dict[str, Any]:
        return {
            "date": value.isoformat(),
            "weekday": value.strftime("%A"),
            **details,
        }

    missing = [session_record(value, expected[value]) for value in missing_dates]
    unexpected = [
        session_record(
            value,
            {
                "calendar_market_state": str(
                    explicit_dates.get(value, {}).get(
                        "market_state",
                        "closed_weekend" if value.weekday() >= 5 else "unknown",
                    )
                ),
                "reason": str(explicit_dates.get(value, {}).get("reason", "")),
                "source_ids": [
                    str(item)
                    for item in explicit_dates.get(value, {}).get("source_ids", [])
                ],
            },
        )
        for value in unexpected_dates
    ]

    annual: list[dict[str, Any]] = []
    for year in range(first_date.year, last_date.year + 1):
        expected_year = {value for value in expected if value.year == year}
        observed_year = {value for value in observed if value.year == year}
        matched_year = expected_year & observed_year
        annual.append(
            {
                "year": year,
                "expected_sessions": len(expected_year),
                "observed_sessions": len(observed_year),
                "matched_expected_sessions": len(matched_year),
                "missing_sessions": len(expected_year - observed_year),
                "unexpected_observed_sessions": len(observed_year - expected_year),
                "coverage_pct": round(
                    100.0 * len(matched_year) / len(expected_year), 6
                )
                if expected_year
                else None,
            }
        )

    matched = len(set(expected) & observed)
    return {
        "calendar_evidence_path": evidence_path.as_posix(),
        "calendar_evidence_sha256": sha256_file(evidence_path),
        "calendar_schema_version": payload["schema_version"],
        "reviewed_coverage": {
            "date_from": reviewed_start.isoformat(),
            "date_to": reviewed_end.isoformat(),
        },
        "audited_range": {
            "date_from": first_date.isoformat(),
            "date_to": last_date.isoformat(),
        },
        "expected_sessions": len(expected),
        "observed_trade_dates": len(observed),
        "matched_expected_sessions": matched,
        "missing_session_count": len(missing),
        "unexpected_observed_session_count": len(unexpected),
        "coverage_pct": round(100.0 * matched / len(expected), 6) if expected else None,
        "calendar_closed_dates_in_range": closed_dates,
        "by_year": annual,
        "missing_sessions": missing,
        "unexpected_observed_sessions": unexpected,
    }


def run_audit(
    dataset_root: Path,
    *,
    expected_rows: int = EXPECTED_ROWS,
    expected_months: int = EXPECTED_MONTHS,
    session_calendar: Path = DEFAULT_SESSION_CALENDAR,
) -> dict[str, Any]:
    dataset_root = dataset_root.resolve()
    files, storage = _discover(dataset_root)
    glob = _safe_sql_path(dataset_root / "year=*" / "month=*" / "part-*.parquet")

    connection = duckdb.connect()
    connection.execute("SET threads=8")
    connection.execute("SET memory_limit='8GB'")
    connection.execute(
        f"CREATE VIEW gold AS SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
    )

    summary = _records(
        connection,
        """
        SELECT
          count(*)::BIGINT AS rows,
          min(timestamp_ist) AS first_timestamp_ist,
          max(timestamp_ist) AS last_timestamp_ist,
          min(trade_date) AS first_trade_date,
          max(trade_date) AS last_trade_date,
          count(DISTINCT trade_date)::BIGINT AS trade_dates,
          count(DISTINCT request_id)::BIGINT AS request_ids,
          count(DISTINCT schema_version)::BIGINT AS schema_versions,
          count(DISTINCT year || '-' || month)::BIGINT AS hive_months
        FROM gold
        """,
    )[0]

    duplicate_excess_rows = int(
        _scalar(
            connection,
            f"""
            SELECT coalesce(sum(n - 1), 0)::BIGINT
            FROM (
              SELECT count(*)::BIGINT AS n
              FROM gold
              GROUP BY {', '.join(PRIMARY_KEY)}
              HAVING count(*) > 1
            )
            """,
        )
    )

    status_counts = {
        "bsm_status": _records(
            connection,
            "SELECT bsm_status AS status, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "bsm_gate_status": _records(
            connection,
            "SELECT bsm_gate_status AS status, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "nifty_spot_join_status": _records(
            connection,
            "SELECT nifty_spot_join_status AS status, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "india_vix_join_status": _records(
            connection,
            "SELECT india_vix_join_status AS status, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "span_join_status": _records(
            connection,
            "SELECT span_join_status AS status, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
    }

    coverage = {
        "by_year": _records(
            connection,
            """
            SELECT year::INTEGER AS year, count(*)::BIGINT AS rows,
                   count(DISTINCT trade_date)::BIGINT AS trade_dates,
                   min(trade_date) AS first_trade_date, max(trade_date) AS last_trade_date
            FROM gold GROUP BY 1 ORDER BY 1
            """,
        ),
        "by_option_type": _records(
            connection,
            "SELECT option_type, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "by_expiry_flag": _records(
            connection,
            "SELECT expiry_flag, count(*)::BIGINT AS rows FROM gold GROUP BY 1 ORDER BY 1",
        ),
        "post_tuesday_migration_expiry_weekday": _records(
            connection,
            """
            SELECT dayname(actual_expiry_date) AS actual_expiry_weekday,
                   expiry_holiday_adjusted,
                   count(*)::BIGINT AS rows
            FROM gold
            WHERE actual_expiry_date >= DATE '2025-09-02'
            GROUP BY 1,2 ORDER BY 1,2
            """,
        ),
    }
    observed_trade_dates = [
        item["trade_date"]
        for item in _records(
            connection,
            "SELECT DISTINCT trade_date FROM gold ORDER BY trade_date",
        )
    ]
    coverage["trading_sessions"] = audit_trading_session_coverage(
        observed_trade_dates,
        first_date=summary["first_trade_date"],
        last_date=summary["last_trade_date"],
        calendar_evidence=session_calendar,
    )

    invariants = _records(
        connection,
        """
        SELECT
          count(*) FILTER (WHERE timestamp_ist IS NULL OR trade_date IS NULL OR
            underlying IS NULL OR actual_expiry_date IS NULL OR strike IS NULL OR
            option_type IS NULL)::BIGINT AS null_primary_fields,
          count(*) FILTER (WHERE volume < 0)::BIGINT AS negative_volume,
          count(*) FILTER (WHERE open_interest < 0)::BIGINT AS negative_open_interest,
          count(*) FILTER (WHERE open < 0 OR high < 0 OR low < 0 OR close < 0 OR
            high < greatest(open, low, close) OR low > least(open, high, close))::BIGINT
            AS ohlc_violations,
          count(*) FILTER (WHERE contract_lot_size IS NULL OR contract_lot_size <= 0)::BIGINT
            AS invalid_lot_size,
          count(*) FILTER (WHERE nifty_spot_age_seconds < 0)::BIGINT AS future_spot_joins,
          count(*) FILTER (WHERE nifty_spot_join_status='matched' AND nifty_spot_age_seconds > 60)::BIGINT
            AS over_tolerance_spot_joins,
          count(*) FILTER (WHERE bsm_status='ok' AND (mte <= 0 OR t_years_act365 <= 0))::BIGINT
            AS successful_bsm_nonpositive_time,
          count(*) FILTER (WHERE bsm_status='ok' AND
            (bsm_iv_close IS NULL OR NOT isfinite(bsm_iv_close) OR bsm_delta IS NULL OR
             NOT isfinite(bsm_delta) OR bsm_gamma IS NULL OR NOT isfinite(bsm_gamma) OR
             bsm_vega_per_1 IS NULL OR NOT isfinite(bsm_vega_per_1)))::BIGINT
            AS successful_bsm_nonfinite,
          count(*) FILTER (WHERE bsm_status='ok' AND (bsm_delta < -1 OR bsm_delta > 1))::BIGINT
            AS delta_range_violations,
          count(*) FILTER (WHERE bsm_status='ok' AND bsm_gamma < 0)::BIGINT AS negative_gamma,
          count(*) FILTER (WHERE bsm_status='ok' AND bsm_vega_per_1 < 0)::BIGINT AS negative_vega,
          count(*) FILTER (WHERE bsm_status='ok' AND
            (abs(bsm_rate_cc - 0.10) > 1e-12 OR abs(bsm_dividend_yield) > 1e-12))::BIGINT
            AS bsm_parameter_violations,
          count(*) FILTER (WHERE quality_severe_anomaly AND bsm_status='ok')::BIGINT
            AS severe_anomaly_solved,
          count(*) FILTER (WHERE bsm_status='blocked' AND
            (bsm_iv_close IS NOT NULL OR bsm_delta IS NOT NULL OR bsm_gamma IS NOT NULL OR
             bsm_vega_per_1 IS NOT NULL))::BIGINT AS blocked_rows_with_bsm_values,
          count(*) FILTER (WHERE span_intraday_asof_join_performed)::BIGINT
            AS bod_intraday_asof_join_rows,
          count(*) FILTER (WHERE span_slot_publication_times_proven)::BIGINT
            AS bod_publication_time_claim_rows,
          count(*) FILTER (WHERE span_effective_ts_ist IS NOT NULL)::BIGINT
            AS bod_effective_timestamp_rows,
          count(*) FILTER (WHERE span_join_status='matched' AND span_time_slot <> 'BOD')::BIGINT
            AS non_bod_rows_in_bod_release
        FROM gold
        """,
    )[0]
    invariants["primary_key_duplicate_excess_rows"] = duplicate_excess_rows

    bsm_residuals = _records(
        connection,
        """
        SELECT
          count(*)::BIGINT AS solved_rows,
          quantile_cont(bsm_price_residual_abs, 0.50) AS p50,
          quantile_cont(bsm_price_residual_abs, 0.95) AS p95,
          quantile_cont(bsm_price_residual_abs, 0.99) AS p99,
          max(bsm_price_residual_abs) AS maximum
        FROM gold WHERE bsm_status='ok'
        """,
    )[0]

    schema_names = pq.ParquetFile(files[0]).schema_arrow.names
    limitations = {
        "observed_bid_ask_present": "bid" in {name.lower() for name in schema_names}
        and "ask" in {name.lower() for name in schema_names},
        "historical_expired_futures_minute_data_present": False,
        "historical_span_arrival_times_proven": bool(
            _scalar(
                connection,
                "SELECT coalesce(bool_or(span_slot_publication_times_proven), false) FROM gold",
            )
        ),
        "rolling_surface_scope": "Dhan ATM plus/minus 10 rolling moneyness; not an absolute-strike full chain",
        "cost_model_included": False,
        "missing_or_invalid_tick_size_rows": int(
            _scalar(
                connection,
                "SELECT count(*)::BIGINT FROM gold WHERE tick_size IS NULL OR tick_size <= 0",
            )
        ),
    }

    hard_failures: list[str] = []
    if int(summary["rows"]) != expected_rows:
        hard_failures.append("row_count_mismatch")
    if int(summary["hive_months"]) != expected_months or storage["file_count"] != expected_months:
        hard_failures.append("month_partition_mismatch")
    if storage["metadata_rows"] != int(summary["rows"]):
        hard_failures.append("parquet_metadata_row_mismatch")
    if storage["schema_count"] != 1:
        hard_failures.append("schema_drift")
    if coverage["trading_sessions"]["missing_session_count"] != 0:
        hard_failures.append("missing_trading_sessions")
    if coverage["trading_sessions"]["unexpected_observed_session_count"] != 0:
        hard_failures.append("unexpected_observed_trading_sessions")
    for key, value in invariants.items():
        if int(value or 0) != 0:
            hard_failures.append(key)

    verdict = "PASS_WITH_DECLARED_LIMITATIONS" if not hard_failures else "FAIL"
    connection.close()
    return {
        "audit_schema": "nifty_options_gold_audit",
        "audit_schema_version": "1.1.0",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_release": "nifty_gold_span_bod_20210101_20260715/version=1.4.0",
        "dataset_root_local": dataset_root.as_posix(),
        "verdict": verdict,
        "hard_failures": sorted(set(hard_failures)),
        "expected": {"rows": expected_rows, "months": expected_months},
        "summary": summary,
        "storage": storage,
        "coverage": coverage,
        "status_counts": status_counts,
        "invariants": invariants,
        "bsm_residuals": bsm_residuals,
        "limitations": limitations,
        "readiness": {
            "options_spot_contract_bsm": "READY_WITH_ROW_LEVEL_STATUS_GATES",
            "bod_span": "STATIC_CONSERVATIVE_FALLBACK_ONLY",
            "strict_point_in_time_span": "USE_SEPARATE_STRICT_RELEASE",
            "six_slot_span": "REFERENCE_ONLY_SENSITIVITY_INPUT",
            "execution_cost_calibration": "EXTERNAL_CONSERVATIVE_MODEL_REQUIRED",
            "historical_expired_futures_1m": "SOURCE_BLOCKED",
        },
    }


def _markdown_table(records: Iterable[dict[str, Any]], columns: list[str]) -> str:
    items = list(records)
    if not items:
        return "_No rows._"
    header = "| " + " | ".join(columns) + " |"
    divider = "|" + "|".join("---" for _ in columns) + "|"
    rows = [
        "| " + " | ".join(str(item.get(column, "")) for column in columns) + " |"
        for item in items
    ]
    return "\n".join([header, divider, *rows])


def render_markdown(audit: dict[str, Any]) -> str:
    summary = audit["summary"]
    storage = audit["storage"]
    limitations = audit["limitations"]
    sessions = audit["coverage"]["trading_sessions"]
    return f"""# NIFTY gold dataset audit

Audit verdict: **{audit['verdict']}**

This is a read-only independent scan of the hash-pinned BOD-SPAN convenience gold release. The
audit does not promote BOD SPAN to strict point-in-time data; the historical archive has no proven
arrival timestamp.

## Identity and coverage

| Field | Value |
|---|---:|
| Release | `{audit['dataset_release']}` |
| Monthly Parquet files | {storage['file_count']} |
| Rows | {summary['rows']:,} |
| Bytes | {storage['total_bytes']:,} |
| Dataset corpus SHA-256 | `{storage['corpus_sha256']}` |
| First timestamp | {summary['first_timestamp_ist']} |
| Last timestamp | {summary['last_timestamp_ist']} |
| Trade dates | {summary['trade_dates']:,} |
| Schema variants | {storage['schema_count']} |

### Annual coverage

{_markdown_table(audit['coverage']['by_year'], ['year', 'rows', 'trade_dates', 'first_trade_date', 'last_trade_date'])}

## Trading-session completeness

Expected sessions are generated for the observed dataset range from the retained, source-backed NSE
F&O calendar: ordinary Monday-Friday sessions, less explicitly declared holidays, plus explicitly
notified special weekend and Muhurat sessions.

| Field | Value |
|---|---:|
| Audited range | {sessions['audited_range']['date_from']} through {sessions['audited_range']['date_to']} |
| Expected NSE F&O sessions | {sessions['expected_sessions']:,} |
| Observed trade dates | {sessions['observed_trade_dates']:,} |
| Matched expected sessions | {sessions['matched_expected_sessions']:,} |
| Missing trading sessions | **{sessions['missing_session_count']:,}** |
| Unexpected observed sessions | {sessions['unexpected_observed_session_count']:,} |
| Session coverage | {sessions['coverage_pct']}% |
| Calendar evidence SHA-256 | `{sessions['calendar_evidence_sha256']}` |

### Session coverage by year

{_markdown_table(sessions['by_year'], ['year', 'expected_sessions', 'observed_sessions', 'matched_expected_sessions', 'missing_sessions', 'unexpected_observed_sessions', 'coverage_pct'])}

### Missing trading-session list

{_markdown_table(sessions['missing_sessions'], ['date', 'weekday', 'market_state', 'classification', 'reason', 'source_ids'])}

### Unexpected observed-session list

{_markdown_table(sessions['unexpected_observed_sessions'], ['date', 'weekday', 'calendar_market_state', 'reason', 'source_ids'])}

## Integrity gates

{_markdown_table([{'check': key, 'violations': value} for key, value in audit['invariants'].items()], ['check', 'violations'])}

All integrity gates must be zero. The Parquet metadata row total, full DuckDB scan, partition count,
and schema fingerprint must also reconcile.

## BSM outcome

{_markdown_table(audit['status_counts']['bsm_status'], ['status', 'rows'])}

| Residual statistic | Value |
|---|---:|
| Solved rows | {audit['bsm_residuals']['solved_rows']:,} |
| p50 absolute price residual | {audit['bsm_residuals']['p50']} |
| p95 absolute price residual | {audit['bsm_residuals']['p95']} |
| p99 absolute price residual | {audit['bsm_residuals']['p99']} |
| Maximum absolute price residual | {audit['bsm_residuals']['maximum']} |

Blocked, no-arbitrage, and solver-failed rows remain visible. They are not silently discarded or
assigned fabricated Greeks.

## Join coverage

### Independent NIFTY spot

{_markdown_table(audit['status_counts']['nifty_spot_join_status'], ['status', 'rows'])}

### INDIA VIX

{_markdown_table(audit['status_counts']['india_vix_join_status'], ['status', 'rows'])}

### Conservative BOD SPAN

{_markdown_table(audit['status_counts']['span_join_status'], ['status', 'rows'])}

## Expiry mechanics

After the Tuesday migration, the actual expiry weekday distribution—including explicitly tagged
holiday adjustments—is:

{_markdown_table(audit['coverage']['post_tuesday_migration_expiry_weekday'], ['actual_expiry_weekday', 'expiry_holiday_adjusted', 'rows'])}

The expiry and lot-size fields come from the saved official rule dimensions, not from the stale
rejected NIFTY Parquet.

## Declared limitations

- Observed historical bid/ask present: **{limitations['observed_bid_ask_present']}**.
- Historical expired-futures minute data present: **{limitations['historical_expired_futures_minute_data_present']}**.
- Historical SPAN arrival times proven: **{limitations['historical_span_arrival_times_proven']}**.
- Option surface: {limitations['rolling_surface_scope']}.
- Transaction-cost model included: **{limitations['cost_model_included']}**.
- Rows without a usable effective-dated tick size: **{limitations['missing_or_invalid_tick_size_rows']:,}**.

These limitations do not invalidate OHLC/spot/contract/BSM hypothesis work, but they constrain later
execution, margin, and capacity claims. Slippage and fee assumptions must be conservative and tested
separately. Strict point-in-time SPAN users must consume the separate strict release, whose historical
SPAN matches are intentionally zero until arrival evidence exists.

## Readiness decision

{_markdown_table([{'component': key, 'status': value} for key, value in audit['readiness'].items()], ['component', 'status'])}

**Decision:** the data foundation may proceed to hypothesis design only if these limitations are
accepted as part of the research contract. They must not be hidden or retroactively filled with
synthetic source data.
"""


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-root", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, default=EXPECTED_ROWS)
    parser.add_argument("--expected-months", type=int, default=EXPECTED_MONTHS)
    parser.add_argument(
        "--session-calendar",
        type=Path,
        default=DEFAULT_SESSION_CALENDAR,
        help="Retained reviewed NSE F&O session-calendar evidence JSON.",
    )
    args = parser.parse_args()

    audit = run_audit(
        args.dataset_root,
        expected_rows=args.expected_rows,
        expected_months=args.expected_months,
        session_calendar=args.session_calendar,
    )
    _write_text(
        args.output_json,
        json.dumps(audit, indent=2, sort_keys=True, default=_json_default) + "\n",
    )
    _write_text(args.output_markdown, render_markdown(audit))
    print(json.dumps({"verdict": audit["verdict"], "json": str(args.output_json), "markdown": str(args.output_markdown)}))
    if audit["verdict"] == "FAIL":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
