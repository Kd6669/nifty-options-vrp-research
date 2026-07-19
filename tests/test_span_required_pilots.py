from __future__ import annotations

import calendar
from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from nifty_span.span.backfill_downloader import SLOT_SPECS
from nifty_span.span.required_pilots import audit_required_span_pilots


class SpanRequiredPilotTests(unittest.TestCase):
    def test_waiting_when_monthly_evidence_has_not_been_published(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = audit_required_span_pilots(Path(tmp))

        self.assertEqual(result.overall_status, "WAITING")
        self.assertEqual(
            result.payload["status_counts"], {"FAIL": 0, "PASS": 0, "WAITING": 4}
        )
        self.assertTrue(
            all(pilot["status"] == "WAITING" for pilot in result.payload["pilots"])
        )

    def test_special_saturday_source_and_rows_are_explicitly_retained(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_all_passing_months(root)
            result = audit_required_span_pilots(root)

        self.assertEqual(result.overall_status, "PASS")
        special = _pilot(result.payload, "special_session_2024_03")
        self.assertEqual(special["status"], "PASS")
        self.assertEqual(special["special_session"]["date"], "2024-03-02")
        self.assertEqual(special["special_session"]["weekday"], "Saturday")
        self.assertTrue(special["special_session"]["source_exists"])
        self.assertTrue(special["special_session"]["retained"])
        self.assertEqual(special["special_session"]["compacted_nifty_rows"], 3)
        self.assertEqual(
            special["compacted"]["instrument_presence"],
            {
                "CE": {"present": True, "row_count": 1},
                "PE": {"present": True, "row_count": 1},
                "FUT": {"present": True, "row_count": 1},
            },
        )

    def test_special_session_source_without_special_date_rows_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            regular_digest = "a" * 64
            missing_special_digest = "b" * 64
            regular = date(2024, 3, 4)
            rows = _instrument_rows(regular, regular_digest)
            _write_month(
                root,
                2024,
                3,
                rows,
                extra_matrix_sources={
                    (date(2024, 3, 2), "BOD"): (missing_special_digest, 1)
                },
            )
            result = audit_required_span_pilots(root)

        special = _pilot(result.payload, "special_session_2024_03")
        self.assertEqual(special["status"], "FAIL")
        self.assertTrue(special["special_session"]["source_exists"])
        self.assertEqual(special["special_session"]["compacted_nifty_rows"], 0)
        self.assertTrue(
            any("zero compacted NIFTY rows" in reason for reason in special["reasons"])
        )

    def test_expiry_transition_lists_only_observed_option_values_and_weekdays(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            observed = date(2025, 9, 1)
            digest = "c" * 64
            rows = [
                _row(observed, "CE", date(2025, 9, 4), 24_000.0, digest),
                _row(observed, "PE", date(2025, 9, 4), 24_000.0, digest),
                _row(observed, "CE", date(2025, 9, 30), 25_000.0, digest),
                _row(observed, "PE", date(2025, 9, 30), 25_000.0, digest),
                _row(observed, "FUT", date(2025, 9, 30), 0.0, digest),
            ]
            _write_month(root, 2025, 9, rows)
            result = audit_required_span_pilots(root)

        pilot = _pilot(result.payload, "expiry_regime_2025_09")
        self.assertEqual(pilot["status"], "PASS")
        self.assertEqual(
            [
                (item["expiry_date"], item["weekday"])
                for item in pilot["observed_option_expiries"]
            ],
            [("2025-09-04", "Thursday"), ("2025-09-30", "Tuesday")],
        )
        self.assertTrue(
            any(
                "do not assert an official expiry rule" in limitation
                for limitation in pilot["evidence_limitations"]
            )
        )

    def test_missing_required_instrument_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            observed = date(2026, 6, 1)
            digest = "d" * 64
            rows = [
                _row(observed, "CE", date(2026, 6, 25), 25_000.0, digest),
                _row(observed, "PE", date(2026, 6, 25), 25_000.0, digest),
            ]
            _write_month(root, 2026, 6, rows)
            result = audit_required_span_pilots(root)

        pilot = _pilot(result.payload, "ordinary_recent_2026_06")
        self.assertEqual(pilot["status"], "FAIL")
        self.assertFalse(pilot["compacted"]["instrument_presence"]["FUT"]["present"])
        self.assertIn("compacted month has no NIFTY FUT rows", pilot["reasons"])

    def test_proven_source_boundary_month_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_all_passing_months(root)
            _write_month(
                root,
                2024,
                3,
                _instrument_rows(date(2024, 3, 2), "1" * 64),
                source_boundary_cells={(date(2024, 3, 2), "ID1")},
            )
            result = audit_required_span_pilots(root)

        self.assertEqual(result.overall_status, "PASS")
        special = _pilot(result.payload, "special_session_2024_03")
        self.assertEqual(special["status"], "PASS")
        self.assertEqual(special["monthly_audit"]["outcome"], "BLOCKED_SOURCE")
        self.assertEqual(special["monthly_audit"]["source_boundary_cells"], 1)
        self.assertTrue(special["special_session"]["retained"])
        self.assertTrue(
            any(
                "BLOCKED_SOURCE is accepted only" in contract
                for contract in result.payload["evidence_contract"]
            )
        )

    def test_unproven_source_boundary_month_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_all_passing_months(root)
            _write_month(
                root,
                2024,
                3,
                _instrument_rows(date(2024, 3, 2), "1" * 64),
                source_boundary_cells={(date(2024, 3, 2), "ID1")},
                prove_source_boundaries=False,
            )
            result = audit_required_span_pilots(root)

        self.assertEqual(result.overall_status, "FAIL")
        special = _pilot(result.payload, "special_session_2024_03")
        self.assertEqual(special["status"], "FAIL")
        self.assertTrue(
            any(
                "lacks explicit source-boundary proof" in reason
                for reason in special["reasons"]
            )
        )

    def test_outputs_are_byte_deterministic_and_report_input_hashes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_all_passing_months(root)
            first = audit_required_span_pilots(root)
            first_json = Path(first.json_path).read_bytes()
            first_markdown = Path(first.markdown_path).read_bytes()
            second = audit_required_span_pilots(root)

            self.assertEqual(Path(second.json_path).read_bytes(), first_json)
            self.assertEqual(Path(second.markdown_path).read_bytes(), first_markdown)
            for pilot in second.payload["pilots"]:
                for artifact in pilot["artifacts"].values():
                    self.assertRegex(artifact["sha256"], r"^[0-9a-f]{64}$")
                self.assertRegex(pilot["source_archive_set_sha256"], r"^[0-9a-f]{64}$")
                self.assertTrue(pilot["compacted"]["natural_key_unique"])


def _write_all_passing_months(root: Path) -> None:
    _write_month(
        root,
        2021,
        1,
        _instrument_rows(date(2021, 1, 4), "0" * 64),
    )
    _write_month(
        root,
        2024,
        3,
        _instrument_rows(date(2024, 3, 2), "1" * 64),
    )
    _write_month(
        root,
        2025,
        9,
        _instrument_rows(date(2025, 9, 1), "2" * 64),
    )
    _write_month(
        root,
        2026,
        6,
        _instrument_rows(date(2026, 6, 1), "3" * 64),
    )


def _instrument_rows(day: date, digest: str) -> list[dict[str, object]]:
    expiry = date(
        day.year, day.month, min(25, calendar.monthrange(day.year, day.month)[1])
    )
    return [
        _row(day, "CE", expiry, 25_000.0, digest),
        _row(day, "PE", expiry, 25_000.0, digest),
        _row(day, "FUT", expiry, 0.0, digest),
    ]


def _row(
    day: date,
    instrument: str,
    expiry: date,
    strike: float,
    digest: str,
    slot: str = "BOD",
) -> dict[str, object]:
    return {
        "date": day,
        "time_slot": slot,
        "symbol": "NIFTY",
        "instrument": instrument,
        "expiry": expiry,
        "strike": strike,
        "source_sha256": digest,
    }


def _write_month(
    root: Path,
    year: int,
    month: int,
    compacted_rows: list[dict[str, object]],
    *,
    extra_matrix_sources: dict[tuple[date, str], tuple[str, int]] | None = None,
    source_boundary_cells: set[tuple[date, str]] | None = None,
    prove_source_boundaries: bool = True,
) -> None:
    key = f"{year:04d}_{month:02d}"
    report_root = root / "reports" / "monthly" / key
    compacted_root = root / "compacted"
    report_root.mkdir(parents=True, exist_ok=True)
    compacted_root.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(compacted_rows), compacted_root / f"{key}.parquet"
    )

    source_cells: dict[tuple[date, str], tuple[str, int]] = {}
    for row in compacted_rows:
        cell = (row["date"], str(row["time_slot"]))
        digest = str(row["source_sha256"])
        previous = source_cells.get(cell)
        if previous is not None and previous[0] != digest:
            raise AssertionError("fixture cell cannot contain multiple source hashes")
        source_cells[cell] = (digest, (previous[1] if previous else 0) + 1)
    source_cells.update(extra_matrix_sources or {})
    boundaries = source_boundary_cells or set()
    if boundaries & set(source_cells):
        raise AssertionError("source-boundary fixture cells cannot also be downloaded")

    days = calendar.monthrange(year, month)[1]
    matrix_rows: list[dict[str, object]] = []
    for day_number in range(1, days + 1):
        day = date(year, month, day_number)
        for order, (slot, suffix) in enumerate(SLOT_SPECS):
            source = source_cells.get((day, slot))
            source_boundary = (day, slot) in boundaries
            matrix_rows.append(
                {
                    "trading_date": day.isoformat(),
                    "slot": slot,
                    "suffix": suffix,
                    "slot_order": order,
                    "download_state": "downloaded" if source else "slot_not_returned",
                    "terminal": True,
                    "source_sha256": source[0] if source else None,
                    "raw_integrity_ok": True if source else None,
                    "extraction_state": "fragment_created"
                    if source
                    else "not_applicable",
                    "fragment_exists": True if source else None,
                    "row_count": source[1] if source else None,
                    "accounted": True,
                    "audit_disposition": (
                        "downloaded_extracted"
                        if source
                        else "source_boundary"
                        if source_boundary
                        else "accepted_absence"
                    ),
                    "classification_outcome": (
                        None
                        if source
                        else "source_boundary"
                        if source_boundary
                        else "accepted_absence"
                    ),
                    "source_boundary_proven": (
                        source_boundary and prove_source_boundaries
                    ),
                    "availability_event_type": (
                        None
                        if source
                        else "official_source_repeated_static_boundary"
                        if source_boundary
                        else "availability_classification"
                    ),
                }
            )
    matrix_path = report_root / "span_date_slot_matrix.parquet"
    pq.write_table(pa.Table.from_pylist(matrix_rows), matrix_path)
    expected_cells = days * len(SLOT_SPECS)
    summary = {
        "start_date": date(year, month, 1).isoformat(),
        "end_date": date(year, month, days).isoformat(),
        "requested_dates": days,
        "expected_cells": expected_cells,
        "accounted_cells": expected_cells,
        "terminal_cells": expected_cells,
        "downloaded_cells": len(source_cells),
        "unavailable_cells": expected_cells - len(source_cells),
        "failed_or_incomplete_cells": 0,
        "source_boundary_cells": len(boundaries),
        "resolved_or_blocked_cells": expected_cells,
        "unresolved_non_boundary_cells": 0,
        "ambiguous_source_cells": 0,
        "unresolved_missing_cells": 0,
        "raw_integrity_failures": 0,
        "downloaded_without_valid_extraction": 0,
        "compacted_rows": len(compacted_rows),
        "duplicate_natural_keys": 0,
        "matrix_complete": True,
        "blocked_matrix_complete": bool(boundaries),
        "raw_integrity_ok": True,
        "extraction_complete": True,
        "compacted_unique": True,
        "outcome": "BLOCKED_SOURCE" if boundaries else "PASS_READY",
        "ok": not boundaries,
    }
    (report_root / "span_backfill_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _pilot(payload: dict[str, object], pilot_id: str) -> dict[str, object]:
    return next(
        pilot
        for pilot in payload["pilots"]  # type: ignore[union-attr]
        if pilot["pilot_id"] == pilot_id
    )


if __name__ == "__main__":
    unittest.main()
