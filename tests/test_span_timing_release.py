from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pyarrow as pa

from dhan_data_fetch_stream.span_timing_release import (
    _SELECTED_SPAN_FIELDS,
    _coverage,
    _create_first_seen_view,
    _paths,
    _quarantine_stale,
    _research_no_proof_sql,
    _research_sql,
    _strict_no_proof_sql,
    _strict_sql,
)
from dhan_data_fetch_stream.span_release import SLOTS


IST = ZoneInfo("Asia/Kolkata")


def test_stale_partial_quarantine_is_scoped_to_the_active_month(
    tmp_path: Path,
) -> None:
    january = _paths(tmp_path, "2025-01")
    february = _paths(tmp_path, "2025-02")
    january["output"].parent.mkdir(parents=True)
    february["output"].parent.mkdir(parents=True)
    january_partial = january["output"].with_name(".part-000.parquet.jan.partial")
    february_partial = february["output"].with_name(".part-000.parquet.feb.partial")
    january_partial.touch()
    february_partial.touch()

    _quarantine_stale(january)

    assert not january_partial.exists()
    assert february_partial.exists()
    quarantined = list((tmp_path / "quarantine").rglob("*.partial.quarantined"))
    assert len(quarantined) == 1
    assert quarantined[0].name == "partial-0.partial.quarantined"
    assert len(quarantined[0].parent.name) == len("2025-01.") + 8


def test_coverage_queries_are_valid_on_duckdb_1_5() -> None:
    connection = _connection(_base_table([datetime(2025, 1, 2, 11, 5, tzinfo=IST)]))
    try:
        _create_first_seen_view(connection, None)
        columns = [row[0] for row in connection.execute("DESCRIBE base").fetchall()]
        connection.execute("CREATE TEMP TABLE research AS " + _research_no_proof_sql())
        connection.execute(
            "CREATE TEMP TABLE strict AS " + _strict_no_proof_sql(columns)
        )
        assert (
            sum(row["rows"] for row in _coverage(connection, "strict", strict=True))
            == 1
        )
        assert (
            sum(row["rows"] for row in _coverage(connection, "research", strict=False))
            == 6
        )
    finally:
        connection.close()


def test_reference_schedule_is_not_strict_arrival_evidence() -> None:
    connection = _connection(_base_table([datetime(2025, 1, 2, 11, 5, tzinfo=IST)]))
    try:
        _create_first_seen_view(connection, None)
        columns = [row[0] for row in connection.execute("DESCRIBE base").fetchall()]
        connection.execute("CREATE TEMP TABLE research AS " + _research_sql())
        connection.execute(
            "CREATE TEMP TABLE research_fast AS " + _research_no_proof_sql()
        )
        assert [
            row[0] for row in connection.execute("DESCRIBE research").fetchall()
        ] == [row[0] for row in connection.execute("DESCRIBE research_fast").fetchall()]
        assert (
            connection.execute(
                "SELECT count(*) FROM ((SELECT * FROM research EXCEPT ALL "
                "SELECT * FROM research_fast) UNION ALL (SELECT * FROM research_fast "
                "EXCEPT ALL SELECT * FROM research))"
            ).fetchone()[0]
            == 0
        )
        row = connection.execute(
            "SELECT span_id1_reference_ts_ist,span_id1_effective_ts_ist,"
            "span_id1_timing_source,span_id1_timing_confidence FROM research"
        ).fetchone()
        assert row[0] == datetime(2025, 1, 2, 11, 0, tzinfo=IST)
        assert row[1:] == (None, "official_reference_schedule", "reference_only")
        connection.execute("CREATE TEMP TABLE strict AS " + _strict_sql(columns))
        connection.execute(
            "CREATE TEMP TABLE strict_fast AS " + _strict_no_proof_sql(columns)
        )
        strict = connection.execute(
            "SELECT span_join_status,span_time_slot,span_effective_ts_ist,"
            "span_unmatched_reason FROM strict"
        ).fetchone()
        assert strict == (
            "timing_unproven",
            None,
            None,
            "historical_arrival_timestamp_unproven",
        )
        assert [row[0] for row in connection.execute("DESCRIBE strict").fetchall()] == [
            row[0] for row in connection.execute("DESCRIBE strict_fast").fetchall()
        ]
        assert (
            connection.execute(
                "SELECT count(*) FROM ((SELECT * FROM strict EXCEPT ALL SELECT * FROM strict_fast) "
                "UNION ALL (SELECT * FROM strict_fast EXCEPT ALL SELECT * FROM strict))"
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()


def test_explicit_created_time_rounds_forward_and_never_activates_early() -> None:
    table = _base_table(
        [
            datetime(2025, 1, 2, 11, 0, tzinfo=IST),
            datetime(2025, 1, 2, 11, 1, tzinfo=IST),
        ],
        id1_created="2025-01-02T11:00:30+05:30",
    )
    connection = _connection(table)
    try:
        _create_first_seen_view(connection, None)
        columns = [row[0] for row in connection.execute("DESCRIBE base").fetchall()]
        connection.execute("CREATE TEMP TABLE research AS " + _research_sql())
        assert connection.execute(
            "SELECT DISTINCT span_id1_effective_ts_ist FROM research"
        ).fetchone()[0] == datetime(2025, 1, 2, 11, 1, tzinfo=IST)
        connection.execute("CREATE TEMP TABLE strict AS " + _strict_sql(columns))
        rows = connection.execute(
            "SELECT timestamp_ist,span_join_status,span_time_slot,span_age_seconds "
            "FROM strict ORDER BY timestamp_ist"
        ).fetchall()
        assert rows[0][1:] == ("timing_unproven", None, None)
        assert rows[1][1:] == ("matched", "ID1", 0)
    finally:
        connection.close()


def test_naive_created_time_is_invalid_and_not_selected() -> None:
    connection = _connection(
        _base_table(
            [datetime(2025, 1, 2, 12, 0, tzinfo=IST)],
            id1_created="2025-01-02T11:00:30",
        )
    )
    try:
        _create_first_seen_view(connection, None)
        columns = [row[0] for row in connection.execute("DESCRIBE base").fetchall()]
        connection.execute("CREATE TEMP TABLE research AS " + _research_sql())
        assert connection.execute(
            "SELECT span_id1_file_created_ts_ist,span_id1_effective_ts_ist "
            "FROM research"
        ).fetchone() == (None, None)
        connection.execute("CREATE TEMP TABLE strict AS " + _strict_sql(columns))
        assert connection.execute(
            "SELECT span_join_status,span_time_slot FROM strict"
        ).fetchone() == ("timing_unproven", None)
    finally:
        connection.close()


def test_eod_never_activates_before_market_close() -> None:
    early = _connection(
        _base_table(
            [datetime(2025, 1, 2, 15, 31, tzinfo=IST)],
            eod_created="2025-01-02T15:00:00+05:30",
        )
    )
    try:
        _create_first_seen_view(early, None)
        columns = [row[0] for row in early.execute("DESCRIBE base").fetchall()]
        early.execute("CREATE TEMP TABLE research AS " + _research_sql())
        early.execute("CREATE TEMP TABLE strict AS " + _strict_sql(columns))
        assert (
            early.execute("SELECT span_eod_effective_ts_ist FROM research").fetchone()[
                0
            ]
            is None
        )
        assert early.execute("SELECT span_time_slot FROM strict").fetchone()[0] is None
    finally:
        early.close()

    valid = _connection(
        _base_table(
            [datetime(2025, 1, 2, 15, 31, tzinfo=IST)],
            eod_created="2025-01-02T15:30:30+05:30",
        )
    )
    try:
        _create_first_seen_view(valid, None)
        columns = [row[0] for row in valid.execute("DESCRIBE base").fetchall()]
        valid.execute("CREATE TEMP TABLE research AS " + _research_sql())
        valid.execute("CREATE TEMP TABLE strict AS " + _strict_sql(columns))
        assert valid.execute(
            "SELECT span_time_slot,span_effective_ts_ist FROM strict"
        ).fetchone() == ("EOD", datetime(2025, 1, 2, 15, 31, tzinfo=IST))
    finally:
        valid.close()


def _connection(table: pa.Table) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("SET TimeZone='Asia/Kolkata'")
    connection.register("base_arrow", table)
    connection.execute("CREATE TEMP VIEW base AS SELECT * FROM base_arrow")
    return connection


def _base_table(
    timestamps: list[datetime],
    *,
    id1_created: str | None = None,
    eod_created: str | None = None,
) -> pa.Table:
    count = len(timestamps)
    values: dict[str, list[object]] = {
        "timestamp_ist": timestamps,
        "trade_date": [date(2025, 1, 2)] * count,
        "underlying": ["NIFTY"] * count,
        "expiry_flag": ["WEEK"] * count,
        "expiry_code": [0] * count,
        "moneyness_label": ["ATM"] * count,
        "strike": [24000.0] * count,
        "option_type": ["CALL"] * count,
        "actual_expiry_date": [date(2025, 1, 2)] * count,
        "request_id": [f"request-{index}" for index in range(count)],
        "bsm_status": ["ok"] * count,
        "span_join_policy": ["base"] * count,
    }
    for slot in SLOTS:
        prefix = f"span_{slot.lower()}"
        field_values: dict[str, object] = {
            "date": date(2025, 1, 2),
            "symbol": "NIFTY",
            "instrument": "CE",
            "expiry": date(2025, 1, 2),
            "strike": 24000.0,
            "source_file": f"nsccl.20250102.{slot}.zip",
            "source_sha256": "a" * 64,
            "source_member": "nsccl.spn",
            "source_status": "AVAILABLE_CANONICAL",
            "source_boundary": False,
            "source_gap_reason": None,
            "gap_classification_outcome": None,
            "source_boundary_category": None,
            "source_gap_evidence_basis": None,
            "source_gap_evidence_event_id": None,
            "effective_time_source": "unknown",
            "span_effective_ts_ist": None,
            # Keep a string Arrow type even when a synthetic slot has no value.
            "span_file_created": (
                (id1_created or "")
                if slot == "ID1"
                else (eod_created or "")
                if slot == "EOD"
                else ""
            ),
        }
        for field in _SELECTED_SPAN_FIELDS:
            value = field_values.get(field, 0.1)
            values[f"{prefix}_{field}"] = [value] * count
        values[f"{prefix}_join_status"] = ["matched"] * count
        values[f"{prefix}_time_slot"] = [slot] * count
    values.update(
        {
            "span_release_status": ["ACCEPTED_WITH_SOURCE_GAPS"] * count,
            "span_technical_audit_outcome": ["BLOCKED_SOURCE"] * count,
            "span_release_manifest_sha256": ["b" * 64] * count,
            "span_handoff_sha256": ["c" * 64] * count,
            "span_source_gap_manifest_sha256": ["d" * 64] * count,
            "span_gold_lineage_sha256": ["e" * 64] * count,
        }
    )
    return pa.table(values)
