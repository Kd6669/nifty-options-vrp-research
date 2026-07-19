from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
import json

from tools.audit_gold_dataset import (
    SESSION_CALENDAR_SCHEMA,
    audit_trading_session_coverage,
)


def test_session_coverage_honours_holidays_special_sessions_and_missing_dates() -> None:
    with TemporaryDirectory() as tmp:
        calendar = Path(tmp) / "calendar.json"
        calendar.write_text(
            json.dumps(
                {
                    "schema_version": SESSION_CALENDAR_SCHEMA,
                    "reviewed_coverage": {
                        "date_from": "2024-01-01",
                        "date_to": "2024-01-31",
                    },
                    "sources": [{"id": "official-source"}],
                    "weekly_rules": [
                        {
                            "id": "weekend-contract",
                            "date_from": "2024-01-01",
                            "date_to": "2024-01-31",
                            "weekdays": ["Saturday", "Sunday"],
                            "market_state": "closed",
                            "classification": "official_weekend",
                            "source_ids": ["official-source"],
                        }
                    ],
                    "dates": [
                        {
                            "date": "2024-01-03",
                            "market_state": "closed",
                            "classification": "official_holiday",
                            "reason": "Official holiday.",
                            "source_ids": ["official-source"],
                        },
                        {
                            "date": "2024-01-06",
                            "market_state": "special_trading_session",
                            "reason": "Official Saturday session.",
                            "source_ids": ["official-source"],
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )

        result = audit_trading_session_coverage(
            [
                date(2024, 1, 1),
                date(2024, 1, 2),
                date(2024, 1, 4),
                date(2024, 1, 5),
                date(2024, 1, 6),
                date(2024, 1, 7),
            ],
            first_date=date(2024, 1, 1),
            last_date=date(2024, 1, 8),
            calendar_evidence=calendar,
        )

    assert result["expected_sessions"] == 6
    assert result["matched_expected_sessions"] == 5
    assert result["missing_session_count"] == 1
    assert result["missing_sessions"][0]["date"] == "2024-01-08"
    assert result["missing_sessions"][0]["market_state"] == "regular_trading_day"
    assert result["unexpected_observed_session_count"] == 1
    assert result["unexpected_observed_sessions"][0]["date"] == "2024-01-07"


def test_session_coverage_rejects_unreviewed_dataset_range() -> None:
    with TemporaryDirectory() as tmp:
        calendar = Path(tmp) / "calendar.json"
        calendar.write_text(
            json.dumps(
                {
                    "schema_version": SESSION_CALENDAR_SCHEMA,
                    "reviewed_coverage": {
                        "date_from": "2024-01-01",
                        "date_to": "2024-01-31",
                    },
                    "sources": [{"id": "official-source"}],
                    "weekly_rules": [],
                    "dates": [],
                }
            ),
            encoding="utf-8",
        )

        try:
            audit_trading_session_coverage(
                [date(2023, 12, 29)],
                first_date=date(2023, 12, 29),
                last_date=date(2024, 1, 2),
                calendar_evidence=calendar,
            )
        except ValueError as exc:
            assert "outside reviewed session-calendar coverage" in str(exc)
        else:
            raise AssertionError("unreviewed date range should fail closed")
