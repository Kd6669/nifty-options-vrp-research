from __future__ import annotations

import unittest

from nifty_span.span.phase1_release import (
    _expected_month_keys,
    _gap_record,
)


class SpanPhase1ReleaseTests(unittest.TestCase):
    def test_expected_month_inventory_is_exact_and_contiguous(self) -> None:
        months = _expected_month_keys()
        self.assertEqual(len(months), 67)
        self.assertEqual(months[0], "2021_01")
        self.assertEqual(months[-1], "2026_07")
        self.assertEqual(len(set(months)), 67)

    def test_ordinary_unavailable_gets_deterministic_evidence_id(self) -> None:
        row = _gap_record(
            {
                "trading_date": "2021-01-02",
                "slot": "BOD",
                "suffix": "i1",
                "download_state": "not_returned_http_404",
                "source_boundary_proven": False,
                "http_status": 404,
            },
            {
                "_line_number": 1,
                "event": "availability_classification",
                "calendar_classification": "official_weekend",
                "classification_outcome": "accepted_absence",
                "reason": "official recurring closure",
                "sources": [{"source_id": "rule", "source_sha256": "a" * 64}],
            },
        )
        self.assertEqual(row["gap_category"], "ordinary_unavailable")
        self.assertEqual(row["evidence_event_id_source"], "derived_event_sha256")
        self.assertRegex(row["evidence_event_id"], r"^[0-9a-f]{64}$")
        self.assertRegex(row["reports_or_static_snapshot_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(
            row["safe_downstream_status"], "NO_SPAN_OBSERVATION_DO_NOT_FILL"
        )

    def test_repeated_corrupt_http_200_preserves_source_boundary(self) -> None:
        row = _gap_record(
            {
                "trading_date": "2022-04-20",
                "slot": "ID4",
                "suffix": "i5",
                "download_state": "corrupt_inner_zip",
                "source_boundary_proven": True,
                "http_status": 200,
            },
            {
                "_line_number": 2,
                "event": "official_source_repeated_static_boundary",
                "event_id": "b" * 32,
                "calendar_classification": "official_source_repeated_static_boundary",
                "classification_outcome": "source_boundary",
                "evidence_basis": "repeated_http200_corrupt_inner_zip",
                "reports_api_evidence": {"manifest_snapshot_sha256": "c" * 64},
                "static_archive_observations": [
                    {"http_status": 200, "body_sha256": "d" * 64},
                    {"http_status": 200, "body_sha256": "d" * 64},
                    {"http_status": 200, "body_sha256": "d" * 64},
                ],
            },
        )
        self.assertEqual(row["gap_category"], "repeated_corrupt_http_200")
        self.assertEqual(row["evidence_event_id"], "b" * 32)
        self.assertTrue(row["source_boundary_proven"])
        self.assertEqual(row["static_observation_count"], 3)
        self.assertEqual(row["static_http_statuses"], [200])
        self.assertEqual(row["canonical_archive_availability"], "absent")


if __name__ == "__main__":
    unittest.main()
