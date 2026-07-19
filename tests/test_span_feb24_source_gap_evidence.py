from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest import TestCase
from urllib.parse import urlparse
import json
import re


REPO_ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_ROOT = REPO_ROOT / "docs" / "evidence" / "span_availability"
JSON_PATH = EVIDENCE_ROOT / "NSE_SPAN_2021-02-24_SOURCE_GAP_EVIDENCE.json"
MARKDOWN_PATH = EVIDENCE_ROOT / "NSE_SPAN_2021-02-24_SOURCE_GAP_EVIDENCE.md"
REVIEWED_IMPORT_PATH = EVIDENCE_ROOT / "reviewed_import_2021_2026.json"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
OFFICIAL_HOSTS = {"www.nseindia.com", "nsearchives.nseindia.com", "www.nseclearing.in"}


class SpanFeb24SourceGapEvidenceTests(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.evidence = json.loads(JSON_PATH.read_text(encoding="utf-8"))

    def test_strict_verdict_and_missing_slot_contract(self) -> None:
        evidence = self.evidence
        self.assertEqual(evidence["schema_version"], "span-source-gap-evidence/v1")
        self.assertEqual(evidence["trading_date"], "2021-02-24")
        self.assertEqual(
            evidence["disposition"]["strict_verdict"],
            "STILL_UNRESOLVED_HISTORICAL_NONPUBLICATION",
        )
        self.assertFalse(evidence["disposition"]["historical_nonpublication_proven"])
        self.assertTrue(
            evidence["disposition"]["current_official_public_unavailability_proven"]
        )
        self.assertEqual(
            evidence["disposition"]["classification_action"],
            "DO_NOT_ALTER_AVAILABILITY_CLASSIFICATIONS",
        )
        self.assertEqual(
            [
                (cell["slot"], cell["suffix"])
                for cell in evidence["scope"]["missing_slots"]
            ],
            [("ID1", "i2"), ("ID2", "i3"), ("ID3", "i4"), ("ID4", "i5")],
        )

    def test_retained_archive_and_static_probe_facts_are_well_formed(self) -> None:
        retained = self.evidence["authoritative_local_evidence"]["retained_archives"]
        self.assertEqual([archive["suffix"] for archive in retained], ["i1", "s"])
        for archive in retained:
            self.assertTrue(SHA256_RE.fullmatch(archive["sha256"]))
            self.assertTrue(archive["zip_crc_ok"])
            self.assertIsNone(archive["zip_testzip_result"])
            self.assertEqual(len(archive["first_16_bytes_hex"]), 32)
            self.assertEqual(len(archive["members"]), 1)
            self.assertRegex(archive["members"][0]["crc32"], r"^[0-9a-f]{8}$")

        probes = self.evidence["official_static_archive_probes"]["target_date_results"]
        self.assertEqual(
            [probe["suffix"] for probe in probes], ["i1", "i2", "i3", "i4", "i5", "s"]
        )
        self.assertEqual(
            [probe["http_status"] for probe in probes], [206, 404, 404, 404, 404, 206]
        )
        missing_hashes = {
            probe["body_sha256"] for probe in probes if probe["http_status"] == 404
        }
        self.assertEqual(
            missing_hashes,
            {"a7bb36b894dc0a4db8dca1c046711db8a7c2710dd15475163128a6800edee37f"},
        )
        for probe in probes:
            self.assertTrue(SHA256_RE.fullmatch(probe["body_sha256"]))
            self.assertEqual(
                urlparse(probe["url"]).hostname, "nsearchives.nseindia.com"
            )

    def test_all_cited_sources_are_official_and_markdown_preserves_limit(self) -> None:
        urls = [
            item["url"] for item in self.evidence["official_reports_api_observations"]
        ]
        urls.extend(item["url"] for item in self.evidence["official_documents"])
        urls.extend(
            item["url"]
            for item in self.evidence["official_static_archive_probes"][
                "target_date_results"
            ]
        )
        for url in urls:
            self.assertIn(urlparse(url).hostname, OFFICIAL_HOSTS)

        markdown = MARKDOWN_PATH.read_text(encoding="utf-8")
        self.assertIn("STILL_UNRESOLVED_HISTORICAL_NONPUBLICATION", markdown)
        self.assertIn("Historical nonpublication is **not** proven", markdown)
        self.assertIn(
            "DO_NOT_ALTER_AVAILABILITY_CLASSIFICATIONS", json.dumps(self.evidence)
        )
        self.assertIn("/FAOFTP/FAOCOMMON/Parameter", markdown)

    def test_live_probe_timestamp_is_recorded_or_explicitly_unavailable(self) -> None:
        live_probe = next(
            item
            for item in self.evidence["official_reports_api_observations"]
            if item["id"] == "four-intraday-live-recheck"
        )
        self._assert_timestamp_or_explicit_limitation(live_probe["observation_time"])
        self.assertEqual(live_probe["method"], "GET")
        self.assertEqual(
            live_probe["request_headers"]["Referer"],
            "https://www.nseindia.com/all-reports-derivatives",
        )

        static = self.evidence["official_static_archive_probes"]
        self._assert_timestamp_or_explicit_limitation(static["observation_time"])
        self.assertEqual(static["request_method"], "GET")
        self.assertEqual(static["request_headers"]["Range"], "bytes=0-15")
        expected_ref = "official_static_archive_probes.observation_time"
        self.assertEqual(
            static["positive_control"]["observation_time_ref"], expected_ref
        )
        for result in static["target_date_results"]:
            self.assertEqual(result["observation_time_ref"], expected_ref)

    def test_evidence_does_not_change_reviewed_availability_import(self) -> None:
        reviewed = json.loads(REVIEWED_IMPORT_PATH.read_text(encoding="utf-8"))
        feb24 = [item for item in reviewed["dates"] if item["date"] == "2021-02-24"]
        self.assertEqual(feb24, [])

    def _assert_timestamp_or_explicit_limitation(
        self, observation: dict[str, object]
    ) -> None:
        timestamp = observation.get("observed_at_utc")
        if isinstance(timestamp, str):
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            self.assertEqual(parsed.tzinfo, UTC)
            return
        self.assertIsNone(timestamp)
        self.assertIs(observation.get("timestamp_not_recorded"), True)
        limitation = observation.get("limitation")
        self.assertIsInstance(limitation, str)
        self.assertIn("cannot be recovered", limitation)
        self.assertIn("not", limitation)
