from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
import json
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from nifty_span.span.backfill_audit import audit_span_backfill
from nifty_span.span.backfill_downloader import SLOT_SPECS
from nifty_span.span.streaming_extractor import span_arrow_schema


class SpanBackfillAuditTests(TestCase):
    def test_complete_six_slot_chain_passes_and_writes_all_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw"
            fragments = root / "fragments"
            compacted = root / "compacted"
            reports = root / "reports"
            download_manifest = root / "download.jsonl"
            extraction_manifest = root / "extraction.jsonl"
            rows = []
            for order, (slot, suffix) in enumerate(SLOT_SPECS):
                archive = _write_raw(raw, date(2026, 6, 25), suffix)
                digest = _digest(archive)
                _append(
                    download_manifest,
                    {
                        "trading_date": "2026-06-25",
                        "slot": slot,
                        "suffix": suffix,
                        "state": "downloaded",
                        "terminal": True,
                        "observed_at_utc": "2026-06-25T12:00:00+00:00",
                        "path": archive.relative_to(raw).as_posix(),
                        "sha256": digest,
                        "size_bytes": archive.stat().st_size,
                    },
                )
                fragment = fragments / f"{slot}.parquet"
                fragment.parent.mkdir(parents=True, exist_ok=True)
                row = _row(slot, order, digest, archive.relative_to(raw).as_posix())
                fragment_schema = span_arrow_schema().with_metadata(
                    {
                        b"source_sha256": digest.encode(),
                        b"parser_version": b"span-stream-v1",
                        b"schema_version": b"span-arrow-schema-v1",
                        b"symbols_filter": b'["NIFTY"]',
                        b"extraction_identity": b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    }
                )
                pq.write_table(pa.Table.from_pylist([row], schema=fragment_schema), fragment)
                rows.append(row)
                _append(
                    extraction_manifest,
                    {
                        "event": "fragment_created",
                        "date": "2026-06-25",
                        "slot": slot,
                        "source_sha256": digest,
                        "fragment_path": str(fragment),
                        "fragment_sha256": _digest(fragment),
                        "fragment_size_bytes": fragment.stat().st_size,
                        "parser_version": "span-stream-v1",
                        "schema_version": "span-arrow-schema-v1",
                        "symbols_filter": ["NIFTY"],
                        "extraction_identity": "a" * 64,
                        "instrument_counts": {row["instrument"]: 1},
                        "row_count": 1,
                    },
                )
            compacted.mkdir(parents=True)
            pq.write_table(
                pa.Table.from_pylist(rows, schema=span_arrow_schema()),
                compacted / "2026_06.parquet",
            )

            report = audit_span_backfill(
                start_date=date(2026, 6, 25),
                end_date=date(2026, 6, 25),
                raw_root=raw,
                download_manifest=download_manifest,
                extraction_manifest=extraction_manifest,
                fragment_root=fragments,
                compacted_root=compacted,
                report_root=reports,
            )

            self.assertTrue(report.ok, report.to_dict(include_cells=False))
            self.assertEqual(report.expected_cells, 6)
            self.assertEqual(report.downloaded_cells, 6)
            self.assertEqual(report.compacted_rows, 6)
            self.assertEqual(report.earliest_proven_download_date, "2026-06-25")
            self.assertEqual(report.latest_proven_download_date, "2026-06-25")
            self.assertEqual(report.download_manifest_sha256, _digest(download_manifest))
            self.assertEqual(report.extraction_manifest_sha256, _digest(extraction_manifest))
            self.assertIsNone(report.availability_manifest_sha256)
            self.assertEqual(len(report.slot_year_counts), 6)
            for coverage in report.slot_year_counts:
                self.assertEqual(coverage.year, 2026)
                self.assertEqual(coverage.total_cells, 1)
                self.assertEqual(coverage.terminal_cells, 1)
                self.assertEqual(coverage.downloaded_valid_cells, 1)
                self.assertEqual(coverage.raw_missing_response_cells, 0)
                self.assertEqual(coverage.accepted_unavailable_cells, 0)
                self.assertEqual(coverage.unresolved_missing_cells, 0)
                self.assertEqual(coverage.manifest_missing_cells, 0)
                self.assertEqual(coverage.nonterminal_or_failed_cells, 0)
                self.assertEqual(coverage.extracted_valid_cells, 1)
                self.assertEqual(coverage.download_state_counts, {"downloaded": 1})
                self.assertEqual(coverage.extraction_state_counts, {"fragment_created": 1})
            self.assertTrue(Path(report.matrix_parquet).is_file())
            self.assertTrue(Path(report.summary_json).is_file())
            self.assertTrue(Path(report.audit_markdown).is_file())
            self.assertEqual(pq.read_table(report.matrix_parquet).num_rows, 6)
            summary = json.loads(Path(report.summary_json).read_text(encoding="utf-8"))
            self.assertEqual(summary["earliest_proven_download_date"], "2026-06-25")
            self.assertEqual(len(summary["slot_year_counts"]), 6)
            markdown = Path(report.audit_markdown).read_text(encoding="utf-8")
            self.assertIn("## Manifest fingerprints", markdown)
            self.assertIn("## Slot/year latest-state coverage", markdown)
            self.assertIn("| 2026 | BOD | i1 | 1 | 1 | 1 |", markdown)

    def test_http_404_cells_remain_neutral_but_fully_accounted(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            for slot, suffix in SLOT_SPECS:
                _append(
                    manifest,
                    {
                        "trading_date": "2026-06-21",
                        "slot": slot,
                        "suffix": suffix,
                        "state": "not_returned_http_404",
                        "terminal": True,
                        "observed_at_utc": "2026-06-21T12:00:00+00:00",
                        "http_status": 404,
                    },
                )
            report = audit_span_backfill(
                start_date=date(2026, 6, 21),
                end_date=date(2026, 6, 21),
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "missing-extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports",
            )
            self.assertFalse(report.ok)
            self.assertTrue(report.matrix_complete)
            self.assertEqual(report.ambiguous_source_cells, 6)
            self.assertEqual(report.unavailable_cells, 6)
            self.assertEqual(report.raw_missing_response_cells, 6)
            self.assertEqual(report.accepted_unavailable_cells, 0)
            self.assertEqual(report.unresolved_missing_cells, 6)
            self.assertEqual({cell.download_state for cell in report.cells}, {"not_returned_http_404"})
            markdown = Path(report.audit_markdown).read_text(encoding="utf-8")
            self.assertIn("without independent calendar or source-boundary", markdown)

    def test_downloaded_archive_without_extraction_fails_closed(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw"
            manifest = root / "download.jsonl"
            archive = _write_raw(raw, date(2026, 6, 25), "i1")
            _append(
                manifest,
                {
                    "trading_date": "2026-06-25",
                    "slot": "BOD",
                    "suffix": "i1",
                    "state": "downloaded",
                    "terminal": True,
                    "observed_at_utc": "2026-06-25T12:00:00+00:00",
                    "path": archive.relative_to(raw).as_posix(),
                    "sha256": _digest(archive),
                    "size_bytes": archive.stat().st_size,
                },
            )
            for slot, suffix in SLOT_SPECS[1:]:
                _append(
                    manifest,
                    {
                        "trading_date": "2026-06-25",
                        "slot": slot,
                        "suffix": suffix,
                        "state": "slot_not_returned",
                        "terminal": True,
                        "observed_at_utc": "2026-06-25T12:00:00+00:00",
                    },
                )
            report = audit_span_backfill(
                start_date=date(2026, 6, 25),
                end_date=date(2026, 6, 25),
                raw_root=raw,
                download_manifest=manifest,
                extraction_manifest=root / "missing.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports",
            )
            self.assertFalse(report.ok)
            self.assertEqual(report.downloaded_without_valid_extraction, 1)

    def test_pinned_full_range_has_exact_expected_matrix_size(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            report = audit_span_backfill(
                start_date=date(2021, 1, 1),
                end_date=date(2026, 7, 15),
                raw_root=root / "raw",
                download_manifest=root / "missing.jsonl",
                extraction_manifest=root / "missing-extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports",
            )
            self.assertEqual(report.requested_dates, 2_022)
            self.assertEqual(report.expected_cells, 12_132)
            self.assertEqual(pq.read_table(report.matrix_parquet).num_rows, 12_132)
            self.assertFalse(report.ok)

    def test_slot_year_counts_preserve_exact_latest_states_and_failure_categories(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            manifest = root / "download.jsonl"
            for slot, suffix in SLOT_SPECS:
                _append(
                    manifest,
                    {
                        "trading_date": "2025-12-31",
                        "slot": slot,
                        "suffix": suffix,
                        "state": "slot_not_returned",
                        "terminal": True,
                    },
                )
            _append(
                manifest,
                {
                    "trading_date": "2026-01-01",
                    "slot": "BOD",
                    "suffix": "i1",
                    "state": "transport_error",
                    "terminal": False,
                },
            )
            _append(
                manifest,
                {
                    "trading_date": "2026-01-01",
                    "slot": "ID1",
                    "suffix": "i2",
                    "state": "slot_not_returned",
                    "terminal": True,
                },
            )
            availability = root / "availability.jsonl"
            source = root / "official-review.html"
            source.write_text("reviewed official closure evidence", encoding="utf-8")
            _append(
                availability,
                {
                    "schema_version": "span-availability-event/v1",
                    "event": "availability_classification",
                    "trading_date": "2025-12-31",
                    "slot": "BOD",
                    "download_state": "slot_not_returned",
                    "market_state": "closed",
                    "classification_outcome": "accepted_absence",
                    "calendar_classification": "official_holiday",
                    "sources": [
                        {
                            "source_id": "official-review",
                            "source_url": "https://www.nseindia.com/official-review",
                            "source_fetched_at_utc": "2026-07-15T10:00:00+00:00",
                            "source_sha256": _digest(source),
                            "source_artifact_path": str(source),
                        }
                    ],
                },
            )

            report = audit_span_backfill(
                start_date=date(2025, 12, 31),
                end_date=date(2026, 1, 1),
                raw_root=root / "raw",
                download_manifest=manifest,
                extraction_manifest=root / "missing-extraction.jsonl",
                fragment_root=root / "fragments",
                compacted_root=root / "compacted",
                report_root=root / "reports",
                availability_manifest=availability,
            )

            by_year_slot = {
                (coverage.year, coverage.slot): coverage
                for coverage in report.slot_year_counts
            }
            self.assertEqual(len(by_year_slot), 12)
            self.assertEqual(
                by_year_slot[(2025, "BOD")].download_state_counts,
                {"slot_not_returned": 1},
            )
            self.assertEqual(by_year_slot[(2025, "BOD")].raw_missing_response_cells, 1)
            self.assertEqual(by_year_slot[(2025, "BOD")].accepted_unavailable_cells, 1)
            self.assertEqual(by_year_slot[(2025, "BOD")].unresolved_missing_cells, 0)
            self.assertEqual(by_year_slot[(2025, "BOD")].nonterminal_or_failed_cells, 0)
            self.assertEqual(by_year_slot[(2025, "ID1")].raw_missing_response_cells, 1)
            self.assertEqual(by_year_slot[(2025, "ID1")].accepted_unavailable_cells, 0)
            self.assertEqual(by_year_slot[(2025, "ID1")].unresolved_missing_cells, 1)
            self.assertEqual(by_year_slot[(2025, "ID1")].nonterminal_or_failed_cells, 1)
            self.assertEqual(
                by_year_slot[(2026, "BOD")].download_state_counts,
                {"transport_error": 1},
            )
            self.assertEqual(by_year_slot[(2026, "BOD")].nonterminal_or_failed_cells, 1)
            self.assertEqual(
                by_year_slot[(2026, "ID1")].download_state_counts,
                {"slot_not_returned": 1},
            )
            self.assertEqual(by_year_slot[(2026, "ID1")].raw_missing_response_cells, 1)
            self.assertEqual(by_year_slot[(2026, "ID1")].accepted_unavailable_cells, 0)
            self.assertEqual(by_year_slot[(2026, "ID1")].unresolved_missing_cells, 1)
            self.assertEqual(
                by_year_slot[(2026, "ID2")].download_state_counts,
                {"manifest_cell_missing": 1},
            )
            self.assertEqual(by_year_slot[(2026, "ID2")].manifest_missing_cells, 1)
            self.assertEqual(by_year_slot[(2026, "ID2")].nonterminal_or_failed_cells, 1)
            self.assertIsNone(report.earliest_proven_download_date)
            self.assertIsNone(report.latest_proven_download_date)
            self.assertEqual(report.raw_missing_response_cells, 7)
            self.assertEqual(report.accepted_unavailable_cells, 1)
            self.assertEqual(report.unresolved_missing_cells, 6)
            self.assertEqual(report.download_manifest_sha256, _digest(manifest))
            self.assertEqual(report.availability_manifest_sha256, _digest(availability))


def _write_raw(root: Path, day: date, suffix: str) -> Path:
    directory = root / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"nsccl.{day:%Y%m%d}.{suffix}.zip"
    member_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"nsccl.{day:%Y%m%d}.{member_suffix}.spn", b"<spanFile />")
    return path


def _row(slot: str, order: int, digest: str, source_file: str) -> dict[str, object]:
    row: dict[str, object] = {
        "date": date(2026, 6, 25),
        "time_slot": slot,
        "symbol": "NIFTY",
        "instrument": ("CE", "PE", "FUT")[order % 3],
        "expiry": date(2026, 6, 30),
        "strike": float(order * 50),
        "price": 100.0,
        "delta": 0.5,
        "implied_vol": 0.2,
        "price_scan_range": 0.1,
        "vol_scan_range": 0.2,
        "cvf": 1.0,
        "composite_delta": 0.5,
        "source_file": source_file,
        "source_sha256": digest,
        "source_member": "fixture.spn",
        "parser_version": "span-stream-v1",
        "ingested_at_utc": datetime(2026, 6, 25, 12, tzinfo=timezone.utc),
        "slot_order": order,
        "span_file_created": None,
        "span_effective_ts_ist": None,
        "effective_time_source": "unknown",
    }
    row.update({f"s{index}": float(index) for index in range(1, 17)})
    return row


def _append(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def _digest(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
