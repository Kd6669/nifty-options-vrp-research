from __future__ import annotations

from datetime import date, datetime
import hashlib
from pathlib import Path
import tempfile
import unittest
from zoneinfo import ZoneInfo

from dhan_data_fetch_stream.span_interface import (
    SPAN_SLOT_LABELS,
    SpanManifest,
    select_effective_span_manifest,
    validate_join_cardinality,
    verify_span_input,
)


UTC = ZoneInfo("UTC")


class SpanInterfaceTests(unittest.TestCase):
    def test_official_slot_mapping_keeps_i5_id4_and_s_eod(self) -> None:
        self.assertEqual(SPAN_SLOT_LABELS["i5"], "ID4")
        self.assertEqual(SPAN_SLOT_LABELS["s"], "EOD")

    def test_manifest_is_strict_about_hash_and_cardinality(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate join keys"):
            SpanManifest.from_mapping(_manifest_values(row_count=2, unique_key_count=1, duplicate_key_count=1))
        with self.assertRaisesRegex(ValueError, "sha256"):
            SpanManifest.from_mapping(_manifest_values(sha256="not-a-hash"))

    def test_hash_verification_detects_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "span.zip"
            path.write_bytes(b"audited span bytes")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest = SpanManifest.from_mapping(_manifest_values(source_path=str(path), sha256=digest))
            verified = verify_span_input(manifest)
            path.write_bytes(b"tampered")
            tampered = verify_span_input(manifest)

        self.assertTrue(verified.ok)
        self.assertEqual(verified.status, "verified")
        self.assertFalse(tampered.ok)
        self.assertIn("sha256_mismatch", tampered.errors)

    def test_point_in_time_selector_never_uses_later_or_other_day_slot(self) -> None:
        bod = SpanManifest.from_mapping(
            _manifest_values(slot_code="i1", effective_at="2026-07-14T03:30:00+00:00")
        )
        id1 = SpanManifest.from_mapping(
            _manifest_values(slot_code="i2", effective_at="2026-07-14T06:30:00+00:00")
        )
        other_day = SpanManifest.from_mapping(
            _manifest_values(
                business_date="2026-07-15",
                slot_code="s",
                effective_at="2026-07-14T05:00:00+00:00",
            )
        )

        selected = select_effective_span_manifest(
            [bod, id1, other_day],
            observation_ts=datetime(2026, 7, 14, 5, 0, tzinfo=UTC),
            business_date=date(2026, 7, 14),
        )

        self.assertEqual(selected, bod)

    def test_unknown_time_allows_only_conservative_bod_fallback(self) -> None:
        bod = SpanManifest.from_mapping(
            _manifest_values(slot_code="i1", effective_at=None, effective_time_source="unknown")
        )
        unknown_id4 = SpanManifest.from_mapping(
            _manifest_values(slot_code="i5", effective_at=None, effective_time_source="unknown")
        )

        selected = select_effective_span_manifest(
            [unknown_id4, bod],
            observation_ts=datetime(2026, 7, 14, 5, 0, tzinfo=UTC),
            business_date="2026-07-14",
        )

        self.assertEqual(selected, bod)

    def test_eod_is_never_eligible_during_intraday_even_if_bad_effective_time_says_so(self) -> None:
        bod = SpanManifest.from_mapping(
            _manifest_values(slot_code="i1", effective_at="2026-07-14T03:30:00+00:00")
        )
        eod = SpanManifest.from_mapping(
            _manifest_values(slot_code="s", effective_at="2026-07-14T04:00:00+00:00")
        )

        selected = select_effective_span_manifest(
            [bod, eod],
            observation_ts=datetime(2026, 7, 14, 5, 0, tzinfo=UTC),
            business_date="2026-07-14",
        )

        self.assertEqual(selected, bod)

    def test_manifest_requires_phase1_acceptance_and_producer_evidence(self) -> None:
        with self.assertRaisesRegex(ValueError, "not accepted"):
            SpanManifest.from_mapping(_manifest_values(phase1_acceptance_status="pending"))
        with self.assertRaisesRegex(ValueError, "producer_evidence_sha256"):
            SpanManifest.from_mapping(_manifest_values(producer_evidence_sha256="missing"))

    def test_cardinality_reports_duplicate_span_keys_and_unmatched_left(self) -> None:
        report = validate_join_cardinality(
            [("A",), ("B",), ("C",)],
            [("A",), ("A",), ("B",)],
        )

        self.assertEqual(report.status, "duplicate_span_keys")
        self.assertEqual(report.duplicate_span_keys, 1)
        self.assertEqual(report.matched_left_rows, 2)
        self.assertEqual(report.unmatched_left_rows, 1)


def _manifest_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "interface_version": "1.0",
        "business_date": "2026-07-14",
        "slot_code": "i5",
        "effective_at": "2026-07-14T09:00:00+00:00",
        "effective_time_source": "official_metadata",
        "source_path": "span.zip",
        "sha256": "a" * 64,
        "row_count": 1,
        "unique_key_count": 1,
        "duplicate_key_count": 0,
        "key_fields": ["business_date", "instrument_key"],
        "phase1_acceptance_status": "accepted",
        "producer_evidence_sha256": "b" * 64,
    }
    values.update(overrides)
    return values


if __name__ == "__main__":
    unittest.main()
