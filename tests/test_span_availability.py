from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
import json

from nifty_span.span.availability import (
    IMPORT_SCHEMA,
    import_and_classify_availability,
    load_availability_events,
)
from nifty_span.span.backfill_audit import audit_span_backfill
from nifty_span.span.backfill_downloader import SLOT_SPECS


class SpanAvailabilityTests(TestCase):
    def test_provenance_backed_closed_day_accounts_for_all_missing_cells(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2026-01-26")
            import_path = _availability_import(
                root,
                trading_date="2026-01-26",
                market_state="closed",
                classification="official_holiday",
            )
            availability = root / "manifests" / "availability.jsonl"
            report = import_and_classify_availability(
                start_date=date(2026, 1, 26),
                end_date=date(2026, 1, 26),
                import_path=import_path,
                download_manifest=download,
                availability_manifest=availability,
                provenance_root=root / "sources",
            )

            self.assertEqual(report.classified_missing_cells, 6)
            self.assertEqual(report.unresolved_missing_cells, 0)
            events = load_availability_events(availability)
            self.assertEqual(len(events), 6)
            source = events[("2026-01-26", "BOD")]["sources"][0]
            self.assertEqual(len(source["source_sha256"]), 64)
            self.assertTrue((availability.parent / source["source_artifact_path"]).is_file())

            audit = _audit(root, download, availability, date(2026, 1, 26))
            self.assertEqual(audit.outcome, "PASS_READY")
            self.assertEqual(audit.ambiguous_source_cells, 0)

    def test_saturday_special_session_is_not_accepted_as_a_weekend_absence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2024-03-02")
            import_path = _availability_import(
                root,
                trading_date="2024-03-02",
                market_state="special_trading_session",
                classification=None,
                weekly_rule=True,
            )
            availability = root / "availability.jsonl"
            report = import_and_classify_availability(
                start_date=date(2024, 3, 2),
                end_date=date(2024, 3, 2),
                import_path=import_path,
                download_manifest=download,
                availability_manifest=availability,
                provenance_root=root / "sources",
            )

            self.assertEqual(report.unresolved_missing_cells, 6)
            audit = _audit(root, download, availability, date(2024, 3, 2))
            self.assertEqual(audit.outcome, "FAIL_INCOMPLETE")
            self.assertEqual(audit.ambiguous_source_cells, 6)

    def test_source_backed_weekly_contract_accepts_ordinary_weekend(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2024-03-09")
            import_path = _availability_import(
                root,
                trading_date="2024-03-08",
                market_state="regular_trading_day",
                classification=None,
                weekly_rule=True,
            )
            availability = root / "availability.jsonl"
            report = import_and_classify_availability(
                start_date=date(2024, 3, 9),
                end_date=date(2024, 3, 9),
                import_path=import_path,
                download_manifest=download,
                availability_manifest=availability,
                provenance_root=root / "sources",
            )

            self.assertEqual(report.classified_missing_cells, 6)
            self.assertEqual(report.unresolved_missing_cells, 0)
            events = load_availability_events(availability)
            self.assertEqual(events[("2024-03-09", "BOD")]["calendar_classification"], "official_weekend")

    def test_confirmed_trading_day_source_boundary_is_blocked_not_passed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2021-01-01")
            import_path = _availability_import(
                root,
                trading_date="2021-01-01",
                market_state="trading_source_boundary",
                classification=None,
            )
            availability = root / "availability.jsonl"
            report = import_and_classify_availability(
                start_date=date(2021, 1, 1),
                end_date=date(2021, 1, 1),
                import_path=import_path,
                download_manifest=download,
                availability_manifest=availability,
                provenance_root=root / "sources",
            )

            self.assertEqual(report.source_boundary_cells, 6)
            audit = _audit(root, download, availability, date(2021, 1, 1))
            self.assertEqual(audit.outcome, "BLOCKED_SOURCE")
            self.assertFalse(audit.ok)

    def test_raw_self_asserted_holiday_is_not_independent_evidence(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(
                root, "2026-01-26", raw_classification="official_holiday"
            )
            audit = _audit(root, download, None, date(2026, 1, 26))
            self.assertEqual(audit.outcome, "FAIL_INCOMPLETE")
            self.assertEqual(audit.ambiguous_source_cells, 6)

    def test_tampered_retained_source_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2026-01-26")
            import_path = _availability_import(
                root,
                trading_date="2026-01-26",
                market_state="closed",
                classification="official_holiday",
            )
            availability = root / "availability.jsonl"
            import_and_classify_availability(
                start_date=date(2026, 1, 26),
                end_date=date(2026, 1, 26),
                import_path=import_path,
                download_manifest=download,
                availability_manifest=availability,
                provenance_root=root / "sources",
            )
            retained = next((root / "sources").iterdir())
            retained.write_bytes(b"tampered")

            with self.assertRaisesRegex(ValueError, "wrong SHA-256"):
                load_availability_events(availability)

    def test_import_rejects_non_official_source_host(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            download = _missing_download_manifest(root, "2026-01-26")
            import_path = _availability_import(
                root,
                trading_date="2026-01-26",
                market_state="closed",
                classification="official_holiday",
                source_url="https://example.com/holidays.pdf",
            )
            with self.assertRaisesRegex(ValueError, "not an approved official"):
                import_and_classify_availability(
                    start_date=date(2026, 1, 26),
                    end_date=date(2026, 1, 26),
                    import_path=import_path,
                    download_manifest=download,
                    availability_manifest=root / "availability.jsonl",
                    provenance_root=root / "sources",
                )


def _missing_download_manifest(
    root: Path, trading_date: str, raw_classification: str | None = None
) -> Path:
    path = root / "download.jsonl"
    events = []
    for slot, suffix in SLOT_SPECS:
        event = {
            "trading_date": trading_date,
            "slot": slot,
            "suffix": suffix,
            "state": "not_returned_http_404",
            "terminal": True,
            "http_status": 404,
            "observed_at_utc": f"{trading_date}T12:00:00+00:00",
        }
        if raw_classification:
            event["calendar_classification"] = raw_classification
        events.append(event)
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    return path


def _availability_import(
    root: Path,
    *,
    trading_date: str,
    market_state: str,
    classification: str | None,
    source_url: str = "https://nsearchives.nseindia.com/content/circulars/example.pdf",
    weekly_rule: bool = False,
) -> Path:
    source = root / "official-source.pdf"
    source.write_bytes(b"fixture representing retained official source bytes")
    entry = {
        "date": trading_date,
        "market_state": market_state,
        "reason": "Human-reviewed official circular evidence for the fixture date.",
        "source_ids": ["official-circular"],
    }
    if classification is not None:
        entry["classification"] = classification
    payload = {
        "schema_version": IMPORT_SCHEMA,
        "sources": [
            {
                "id": "official-circular",
                "url": source_url,
                "path": str(source),
                "fetched_at_utc": "2026-07-15T10:00:00+00:00",
            }
        ],
        "dates": [entry],
    }
    if weekly_rule:
        payload["weekly_rules"] = [
            {
                "id": "official-fo-weekend-contract",
                "date_from": "2021-01-01",
                "date_to": "2026-12-31",
                "weekdays": ["Saturday", "Sunday"],
                "market_state": "closed",
                "classification": "official_weekend",
                "reason": "Official F&O regulation excludes Saturdays and Sundays.",
                "source_ids": ["official-circular"],
            }
        ]
    path = root / "availability-import.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _audit(
    root: Path,
    download: Path,
    availability: Path | None,
    trading_date: date,
):
    return audit_span_backfill(
        start_date=trading_date,
        end_date=trading_date,
        raw_root=root / "raw",
        download_manifest=download,
        extraction_manifest=root / "extraction.jsonl",
        fragment_root=root / "fragments",
        compacted_root=root / "compacted",
        report_root=root / "reports",
        availability_manifest=availability,
    )
