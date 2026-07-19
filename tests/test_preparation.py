from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from dhan_data_fetch_stream.preparation import ExpiryEvidence, prepare_dhan_partition


IST = ZoneInfo("Asia/Kolkata")


def _option(timestamp: str, *, session: str = "regular_session") -> dict[str, object]:
    return {
        "timestamp_ist": timestamp,
        "trade_date": "2026-07-14",
        "session_status": session,
        "expiry_flag": "WEEK",
        "expiry_code": 1,
        "strike": 25000.0,
        "option_type": "CALL",
        "close": 160.0,
        "provider_iv_raw": 18.2,
    }


def test_preparation_is_span_pending_and_never_guesses_expiry() -> None:
    option = _option("2026-07-14T09:16:00+05:30")
    spot = {
        "timestamp_ist": "2026-07-14T09:16:00+05:30",
        "trade_date": "2026-07-14",
        "session_status": "regular_session",
        "close": 25050.0,
    }
    vix = {**spot, "close": 13.2}

    batch = prepare_dhan_partition([option], [spot], [vix])

    assert batch.readiness == "SPAN_PENDING"
    assert batch.rows[0]["span_enrichment_status"] == "SPAN_PENDING"
    assert batch.rows[0]["spot"] == 25050.0
    assert batch.rows[0]["india_vix"] == 13.2
    assert batch.rows[0]["bsm_status"] == "blocked"
    assert batch.rows[0]["bsm_failure_reason"] == "actual_expiry_unverified"


def test_preparation_solves_only_with_verified_actual_expiry() -> None:
    option = _option("2026-07-14T09:16:00+05:30")
    spot = {
        "timestamp_ist": "2026-07-14T09:16:00+05:30",
        "trade_date": "2026-07-14",
        "session_status": "regular_session",
        "close": 25050.0,
    }
    evidence = {
        ("2026-07-14", "WEEK", 1): ExpiryEvidence(
            datetime(2026, 7, 21, 15, 30, tzinfo=IST), True, "a" * 64
        )
    }

    batch = prepare_dhan_partition([option], [spot], [], expiry_evidence=evidence)

    assert batch.rows[0]["bsm_status"] == "ok"
    assert batch.rows[0]["bsm_iv_close"] is not None
    assert batch.rows[0]["expiry_evidence_sha256"] == "a" * 64


def test_preparation_quarantines_outside_session_and_does_not_join_it() -> None:
    batch = prepare_dhan_partition(
        [_option("2026-07-14T18:00:00+05:30", session="outside_regular_session")],
        [],
        [],
    )

    assert batch.rows == ()
    assert batch.exceptions[0]["preparation_exception"] == "outside_regular_session"
