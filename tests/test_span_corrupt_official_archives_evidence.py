from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase
from urllib.parse import urlparse
import json
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_ROOT = REPO_ROOT / "docs" / "evidence" / "span_availability"
JSON_PATH = EVIDENCE_ROOT / "NSE_SPAN_2021_CORRUPT_OFFICIAL_ARCHIVES_EVIDENCE.json"
MARKDOWN_PATH = EVIDENCE_ROOT / "NSE_SPAN_2021_CORRUPT_OFFICIAL_ARCHIVES_EVIDENCE.md"
REVIEWED_IMPORT_PATH = EVIDENCE_ROOT / "reviewed_import_2021_2026.json"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
CRC32_RE = re.compile(r"^[0-9a-f]{8}$")
EXPECTED_CELLS = [
    (
        "2021-10-11",
        "EOD",
        "s",
        7_768_558,
        "cb0266e876b3ff5f071bd4032a7bb92d395ee15b64d5d7177b2d1b19ef72cc27",
    ),
    (
        "2021-11-01",
        "ID4",
        "i5",
        7_768_558,
        "05e8970fc52e81e1f14b579490dfa28e5c489a150f730df5f32e378b8031a8b9",
    ),
    (
        "2021-12-01",
        "ID4",
        "i5",
        3_851_638,
        "9c91e58ad48b3ff1e3ba34b0b4c54cc612cc0b169a1f70b4dd023e399cc59173",
    ),
    (
        "2021-12-30",
        "ID4",
        "i5",
        6_005_944,
        "220eec6fe7359e8b20b9548431e6fb6f9c424d4f4c87a0bc6be694cde932e676",
    ),
]


class SpanCorruptOfficialArchivesEvidenceTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence = json.loads(JSON_PATH.read_text(encoding="utf-8"))

    def test_strict_fail_closed_disposition(self) -> None:
        disposition = self.evidence["disposition"]
        self.assertEqual(
            self.evidence["schema_version"], "span-source-corruption-evidence/v1"
        )
        self.assertEqual(
            disposition["strict_verdict"],
            "CURRENT_OFFICIAL_PUBLIC_SOURCE_CORRUPTION_PROVEN",
        )
        self.assertTrue(disposition["current_official_public_source_corruption_proven"])
        self.assertFalse(
            disposition["current_official_public_usable_archive_available"]
        )
        self.assertTrue(disposition["static_and_reports_api_payload_equality_proven"])
        self.assertFalse(disposition["historical_nonpublication_proven"])
        self.assertFalse(disposition["historical_original_archive_state_proven"])
        self.assertEqual(disposition["phase1_acceptance"], "BLOCKED_FOR_FOUR_CELLS")
        self.assertEqual(
            disposition["classification_action"], "DO_NOT_MARK_USABLE_OR_DOWNLOADED"
        )
        self.assertEqual(
            disposition["availability_import_action"],
            "DO_NOT_ALTER_REVIEWED_AVAILABILITY_IMPORT",
        )

    def test_exact_cells_hashes_and_zip_failures(self) -> None:
        results = self.evidence["affected_archive_results"]
        actual = [
            (
                item["trading_date"],
                item["slot"],
                item["suffix"],
                item["full_response"]["body_size_bytes"],
                item["full_response"]["body_sha256"],
            )
            for item in results
        ]
        self.assertEqual(actual, EXPECTED_CELLS)

        for item in results:
            range_response = item["range_response"]
            full_response = item["full_response"]
            validation = item["zipfile_validation"]
            self.assertEqual(range_response["http_status"], 206)
            self.assertEqual(range_response["content_type"], "application/zip")
            self.assertEqual(range_response["body_size_bytes"], 16)
            self.assertRegex(range_response["content_range"], r"^bytes 0-15/\d+$")
            self.assertTrue(range_response["first_16_bytes_hex"].startswith("504b0304"))
            self.assertEqual(
                range_response["first_16_bytes_hex"],
                full_response["first_16_bytes_hex"],
            )
            self.assertEqual(full_response["http_status"], 200)
            self.assertEqual(full_response["content_type"], "application/zip")
            self.assertTrue(SHA256_RE.fullmatch(full_response["body_sha256"]))
            self.assertFalse(validation["is_zipfile"])
            self.assertFalse(validation["open_succeeded"])
            self.assertEqual(validation["exception_class"], "zipfile.BadZipFile")
            self.assertEqual(
                validation["failure_stage"], "central-directory validation"
            )
            self.assertTrue(validation["truncation_proven"])
            self.assertEqual(validation["failure_mode"], "mid-deflate truncation")
            self.assertRegex(validation["local_file_header_crc32"], CRC32_RE)
            self.assertGreater(validation["local_file_header_compressed_size"], 0)
            self.assertGreater(validation["local_file_header_uncompressed_size"], 0)
            self.assertEqual(
                validation["observed_body_end_offset_exclusive"],
                full_response["body_size_bytes"],
            )
            self.assertEqual(
                validation["bytes_short_of_declared_data_end"],
                validation["declared_compressed_data_end_offset_exclusive"]
                - validation["observed_body_end_offset_exclusive"],
            )
            self.assertGreater(validation["bytes_short_of_declared_data_end"], 0)
            self.assertFalse(
                validation["central_directory_signature_in_last_128_bytes"]
            )
            self.assertFalse(
                validation["end_of_central_directory_signature_in_last_128_bytes"]
            )

    def test_reports_api_inner_members_exactly_match_static_payloads(self) -> None:
        for item in self.evidence["affected_archive_results"]:
            full_response = item["full_response"]
            api = item["reports_api_comparison"]
            self.assertEqual(api["outer_http_status"], 200)
            self.assertEqual(api["outer_content_type"], "application/zip")
            self.assertGreater(api["outer_body_size_bytes"], 0)
            self.assertTrue(SHA256_RE.fullmatch(api["outer_body_sha256"]))
            self.assertEqual(
                api["inner_member_name"],
                f"nsccl.{item['trading_date'].replace('-', '')}.{item['suffix']}.zip",
            )
            self.assertEqual(
                api["inner_member_size_bytes"], full_response["body_size_bytes"]
            )
            self.assertEqual(api["inner_member_sha256"], full_response["body_sha256"])
            self.assertTrue(api["matches_static_body_size"])
            self.assertTrue(api["matches_static_body_sha256"])
            self.assertRegex(api["outer_member_crc32"], CRC32_RE)
            self.assertEqual(api["other_returned_slot_archives_count"], 5)
            self.assertTrue(api["other_returned_slot_archives_valid"])

            observed = api["observed_at_utc_range"]
            first = datetime.fromisoformat(observed["first"].replace("Z", "+00:00"))
            last = datetime.fromisoformat(observed["last"].replace("Z", "+00:00"))
            self.assertEqual(first.tzinfo, UTC)
            self.assertEqual(last.tzinfo, UTC)
            self.assertLess(first, last)

    def test_all_twenty_valid_siblings_are_exact_official_path_controls(self) -> None:
        controls = self.evidence["valid_sibling_controls"]
        self.assertEqual(controls["count"], 20)
        self.assertEqual(len(controls["archives"]), 20)
        self.assertEqual(
            {(item["trading_date"], item["suffix"]) for item in controls["archives"]},
            {
                (date, suffix)
                for date in {cell[0] for cell in EXPECTED_CELLS}
                for suffix in {"i1", "i2", "i3", "i4", "i5", "s"}
                if (date, suffix) not in {(cell[0], cell[2]) for cell in EXPECTED_CELLS}
            },
        )
        for archive in controls["archives"]:
            self.assertGreater(archive["size_bytes"], 0)
            self.assertTrue(SHA256_RE.fullmatch(archive["sha256"]))

        contract = controls["validation_contract"]
        self.assertTrue(contract["static_and_reports_api_inner_size_equal"])
        self.assertTrue(contract["static_and_reports_api_inner_sha256_equal"])
        self.assertTrue(contract["zipfile_is_zipfile"])
        self.assertIsNone(contract["zipfile_testzip_result"])
        self.assertTrue(contract["expected_single_spn_member"])
        self.assertEqual(contract["xml_root"], "spanFile")

    def test_all_network_evidence_is_official_and_timestamp_is_fail_closed(
        self,
    ) -> None:
        self.assertEqual(
            urlparse(
                self.evidence["probe_method"]["reports_api_request"]["url"]
            ).hostname,
            "www.nseindia.com",
        )
        for item in self.evidence["affected_archive_results"]:
            self.assertEqual(
                urlparse(item["static_url"]).hostname, "nsearchives.nseindia.com"
            )
            refs = item["observation_time_refs"]
            self.assertEqual(refs["range"], "observation_times.static_range_probes")
            self.assertEqual(refs["full"], "observation_times.static_full_downloads")

        observations = self.evidence["observation_times"]
        static_range = observations["static_range_probes"]
        first = datetime.fromisoformat(
            static_range["observed_at_utc_first"].replace("Z", "+00:00")
        )
        last = datetime.fromisoformat(
            static_range["observed_at_utc_last"].replace("Z", "+00:00")
        )
        self.assertEqual(first.tzinfo, UTC)
        self.assertEqual(last.tzinfo, UTC)
        self.assertLess(first, last)

        static_full = observations["static_full_downloads"]
        self.assertIsNone(static_full["observed_at_utc"])
        self.assertIs(static_full["timestamp_not_recorded"], True)
        self.assertIn("cannot be recovered", static_full["limitation"])
        self.assertIn("not", static_full["limitation"])

    def test_markdown_preserves_inference_boundary_and_import_is_unchanged(
        self,
    ) -> None:
        markdown = MARKDOWN_PATH.read_text(encoding="utf-8")
        self.assertIn("CURRENT_OFFICIAL_PUBLIC_SOURCE_CORRUPTION_PROVEN", markdown)
        self.assertIn("does **not** prove historical nonpublication", markdown)
        self.assertIn("DO_NOT_MARK_USABLE_OR_DOWNLOADED", markdown)
        self.assertIn("byte-for-byte identical", markdown)
        self.assertIn("Until then these four cells block Phase 1 acceptance", markdown)

        reviewed = json.loads(REVIEWED_IMPORT_PATH.read_text(encoding="utf-8"))
        affected_dates = {cell[0] for cell in EXPECTED_CELLS}
        imported_dates = {item["date"] for item in reviewed["dates"]}
        self.assertTrue(affected_dates.isdisjoint(imported_dates))
