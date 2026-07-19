from __future__ import annotations

from datetime import date, datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import zipfile

from nifty_span.span.extractor import parse_span_zip
from nifty_span.span.streaming_extractor import (
    BUSINESS_FIELDS,
    LINEAGE_FIELDS,
    ManifestArchive,
    PARSER_VERSION,
    SpanManifestError,
    SpanNaturalKeyConflictError,
    _extract_one_archive,
    _extraction_identity,
    _normalize_symbols,
    compact_span_month,
    extract_manifest_archives,
    iter_span_rows,
    load_manifest_archives,
    span_arrow_schema,
)


class SpanStreamingExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow is required")

    def test_streams_manifest_archive_with_deterministic_lineage_and_rerun(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(
                archive,
                _xml(
                    price=100.5,
                    portfolio_count=3,
                    effective="2025-01-02T08:30:00+05:30",
                ),
            )
            archive_digest = _sha256(archive)
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive, observed="2025-01-02T03:30:00Z")
            resumed = _event(raw, archive, observed="2025-02-01T03:30:00Z")
            resumed["state"] = "downloaded_existing"
            with manifest.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(resumed) + "\n")
            # This valid, but out-of-range archive proves bounded manifest discovery.
            old_archive = raw / "2024" / "12" / "31" / "nsccl.20241231.i1.zip"
            _write_archive(old_archive, _xml(price=99.0, portfolio_count=1))
            _manifest_event(manifest, raw, old_archive, day="2024-12-31")
            fragments = root / "fragments"
            extraction_manifest = root / "extraction.jsonl"

            first = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=fragments,
                extraction_manifest=extraction_manifest,
                batch_rows=1,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
            )
            fragment = Path(first.fragments[0])
            original_bytes = fragment.read_bytes()
            original_manifest = extraction_manifest.read_bytes()
            second = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=fragments,
                extraction_manifest=extraction_manifest,
                batch_rows=2,
                start_date=date(2025, 1, 1),
                end_date=date(2025, 1, 31),
            )
            table = pq.read_table(fragment)
            rows = table.to_pylist()
            rerun_bytes = fragment.read_bytes()
            rerun_manifest = extraction_manifest.read_bytes()

        self.assertTrue(first.ok)
        self.assertEqual(first.manifest_archive_count, 1)
        self.assertEqual(first.created_fragment_count, 1)
        self.assertEqual(first.row_count, 3)
        self.assertEqual(second.created_fragment_count, 0)
        self.assertEqual(second.skipped_fragment_count, 1)
        self.assertEqual(rerun_bytes, original_bytes)
        self.assertEqual(rerun_manifest, original_manifest)
        self.assertEqual(len(rows), 3)
        self.assertEqual(tuple(span_arrow_schema().names), BUSINESS_FIELDS + LINEAGE_FIELDS)
        self.assertEqual(rows[0]["source_sha256"], archive_digest)
        self.assertEqual(rows[0]["source_file"], "2025/01/02/nsccl.20250102.i1.zip")
        self.assertEqual(rows[0]["source_member"], "nsccl.20250102.i1.spn")
        self.assertEqual(rows[0]["ingested_at_utc"].isoformat(), "2025-01-02T03:30:00+00:00")
        self.assertEqual(rows[0]["slot_order"], 0)
        self.assertEqual(rows[0]["span_file_created"], "2025-01-02T08:45:00+05:30")
        self.assertEqual(rows[0]["span_effective_ts_ist"].isoformat(), "2025-01-02T08:30:00+05:30")
        self.assertEqual(rows[0]["effective_time_source"], "span_effective_timestamp_explicit_offset")

    def test_resume_journals_valid_orphan_fragment_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml(portfolio_count=2))
            download_manifest = root / "download.jsonl"
            _manifest_event(download_manifest, raw, archive)
            extraction_manifest = root / "extraction.jsonl"
            common = {
                "download_manifest": download_manifest,
                "raw_root": raw,
                "fragment_root": root / "fragments",
                "extraction_manifest": extraction_manifest,
                "start_date": date(2025, 1, 1),
                "end_date": date(2025, 1, 31),
            }

            first = extract_manifest_archives(**common)
            fragment = Path(first.fragments[0])
            fragment_bytes = fragment.read_bytes()
            extraction_manifest.write_text("", encoding="utf-8")

            recovered = extract_manifest_archives(**common)
            recovered_events = [
                json.loads(line)
                for line in extraction_manifest.read_text(encoding="utf-8").splitlines()
            ]
            recovered_manifest = extraction_manifest.read_bytes()
            repeated = extract_manifest_archives(**common)
            repeated_manifest = extraction_manifest.read_bytes()
            repeated_fragment_bytes = fragment.read_bytes()
            archive_sha256 = _sha256(archive)
            fragment_sha256 = _sha256(fragment)

        self.assertTrue(recovered.ok)
        self.assertEqual(recovered.created_fragment_count, 0)
        self.assertEqual(recovered.skipped_fragment_count, 1)
        self.assertEqual(repeated.skipped_fragment_count, 1)
        self.assertEqual(repeated_fragment_bytes, fragment_bytes)
        self.assertEqual(repeated_manifest, recovered_manifest)
        self.assertEqual(len(recovered_events), 1)
        event = recovered_events[0]
        self.assertEqual(event["event"], "fragment_already_valid")
        self.assertEqual(event["date"], "2025-01-02")
        self.assertEqual(event["slot"], "BOD")
        self.assertEqual(event["source_sha256"], archive_sha256)
        self.assertEqual(event["fragment_sha256"], fragment_sha256)
        self.assertEqual(event["row_count"], 2)

    def test_standard_fast_path_matches_general_xml_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            fast_path = root / "nsccl.20250102.i1.zip"
            fallback = root / "fallback" / "nsccl.20250102.i1.zip"
            xml = _xml(portfolio_count=3)
            _write_archive(fast_path, xml)
            _write_archive(fallback, xml.replace("<oopPf>", '<oopPf format="general">'))

            def rows(path: Path) -> list[dict[str, object]]:
                archive = ManifestArchive(
                    date(2025, 1, 2),
                    "BOD",
                    "i1",
                    path,
                    path.name,
                    _sha256(path),
                    datetime(2025, 1, 2, 3, 30, tzinfo=timezone.utc),
                )
                return list(iter_span_rows(path, archive=archive))

            fast_rows = rows(fast_path)
            fallback_rows = rows(fallback)

        self.assertEqual(len(fast_rows), 3)
        self.assertEqual(
            [{field: row[field] for field in BUSINESS_FIELDS} for row in fast_rows],
            [{field: row[field] for field in BUSINESS_FIELDS} for row in fallback_rows],
        )

    def test_streaming_parser_matches_legacy_parser_and_independent_golden_rows(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "nsccl.20250102.i1.zip"
            _write_archive(path, _golden_equivalence_xml())
            archive = ManifestArchive(
                date(2025, 1, 2),
                "BOD",
                "i1",
                path,
                path.name,
                _sha256(path),
                datetime(2025, 1, 2, 3, 30, tzinfo=timezone.utc),
            )

            legacy = _normalized_business_rows(
                parse_span_zip(path, symbols_filter=("NIFTY",))
            )
            streaming = _normalized_business_rows(
                list(iter_span_rows(path, archive=archive, symbols_filter=("NIFTY",)))
            )
            golden = _normalized_business_rows(_golden_business_rows())

        self.assertEqual(len(legacy), 3)
        self.assertEqual(streaming, legacy)
        self.assertEqual(legacy, golden)

    def test_scenario_columns_one_through_sixteen_survive_fragment_and_compaction(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _golden_equivalence_xml())
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)

            extraction = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                symbols_filter=("NIFTY",),
                batch_rows=2,
                max_workers=1,
            )
            fragment_rows = pq.read_table(extraction.fragments[0]).to_pylist()
            compacted = compact_span_month(
                fragment_root=root / "fragments",
                parquet_root=root / "monthly",
                quarantine_root=root / "quarantine",
                year=2025,
                month=1,
            )
            compacted_rows = pq.read_table(compacted.output_path).to_pylist()

        fragment_ce = next(
            row for row in fragment_rows if row["instrument"] == "CE" and row["strike"] == 24000.0
        )
        compacted_ce = next(
            row for row in compacted_rows if row["instrument"] == "CE" and row["strike"] == 24000.0
        )
        scenario_fields = tuple(f"s{index}" for index in range(1, 17))
        self.assertTrue(extraction.ok)
        self.assertEqual(extraction.row_count, 3)
        self.assertEqual(compacted.input_row_count, 3)
        self.assertEqual(compacted.output_row_count, 3)
        self.assertEqual(
            tuple(fragment_ce[field] for field in scenario_fields),
            EXPECTED_SCENARIOS,
        )
        self.assertEqual(
            tuple(compacted_ce[field] for field in scenario_fields),
            EXPECTED_SCENARIOS,
        )

    def test_xml_entity_declaration_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            xml = _xml().replace("<root", '<!DOCTYPE root [<!ENTITY symbol "NIFTY">]><root').replace(
                "NIFTY", "&symbol;"
            )
            _write_archive(archive, xml)
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            report = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
            )
            event = json.loads((root / "extract.jsonl").read_text(encoding="utf-8"))

        self.assertFalse(report.ok)
        self.assertEqual(event["error_type"], "SpanExtractionError")
        self.assertIn("entity declarations", event["error"])

    def test_naive_span_created_time_is_not_promoted_to_effective_timestamp(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml(created="2025-01-02T08:45:00"))
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            report = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
            )
            row = pq.read_table(report.fragments[0]).to_pylist()[0]

        self.assertIsNone(row["span_effective_ts_ist"])
        self.assertEqual(row["effective_time_source"], "unknown")

    def test_manifest_range_filter_and_raw_layout_enforcement(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml())
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)

            self.assertEqual(
                load_manifest_archives(manifest, raw, start_date=date(2025, 1, 3)),
                (),
            )
            bad = root / "bad.jsonl"
            event = _event(raw, archive)
            event["path"] = str(archive.resolve())
            event["trading_date"] = "2025-01-03"
            bad.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaises(SpanManifestError):
                load_manifest_archives(bad, raw)

    def test_latest_manifest_failure_suppresses_stale_success_and_identity_is_strict(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml())
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            failed = _event(raw, archive)
            failed["state"] = "local_file_invalid"
            with manifest.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(failed) + "\n")
            self.assertEqual(load_manifest_archives(manifest, raw), ())

            mismatch = root / "mismatch.jsonl"
            bad = _event(raw, archive)
            bad["slot"] = "ID1"
            mismatch.write_text(json.dumps(bad) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(SpanManifestError, "slot/suffix mismatch"):
                load_manifest_archives(mismatch, raw)

            wrong_name = raw / "2025" / "01" / "02" / "copy.i1.zip"
            _write_archive(wrong_name, _xml())
            wrong = root / "wrong-name.jsonl"
            _manifest_event(wrong, raw, wrong_name)
            with self.assertRaisesRegex(SpanManifestError, "filename mismatch"):
                load_manifest_archives(wrong, raw)

    def test_parser_and_filter_identity_produce_distinct_validated_fragments(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml())
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            kwargs = {
                "download_manifest": manifest,
                "raw_root": raw,
                "fragment_root": root / "fragments",
                "extraction_manifest": root / "extract.jsonl",
                "max_workers": 2,
            }
            first = extract_manifest_archives(**kwargs, parser_version="parser-a", symbols_filter=("NIFTY",))
            parser_changed = extract_manifest_archives(
                **kwargs, parser_version="parser-b", symbols_filter=("NIFTY",)
            )
            filter_changed = extract_manifest_archives(
                **kwargs, parser_version="parser-a", symbols_filter=("BANKNIFTY", "NIFTY")
            )
            paths = {first.fragments[0], parser_changed.fragments[0], filter_changed.fragments[0]}
            events = [json.loads(line) for line in (root / "extract.jsonl").read_text().splitlines()]
            metadata = [pq.read_metadata(path).metadata or {} for path in paths]

        self.assertEqual(len(paths), 3)
        self.assertTrue(all(report.created_fragment_count == 1 for report in (first, parser_changed, filter_changed)))
        self.assertEqual(len(events), 3)
        self.assertTrue(all(len(event["fragment_sha256"]) == 64 for event in events))
        self.assertTrue(all(event["fragment_size_bytes"] > 0 for event in events))
        self.assertTrue(all(event["row_count"] == 1 for event in events))
        self.assertTrue(all(event["instrument_counts"] == {"CE": 1, "FUT": 0, "PE": 0} for event in events))
        self.assertEqual(len({item[b"extraction_identity"] for item in metadata}), 3)
        self.assertTrue(all(item[b"row_count"] == b"1" for item in metadata))

    def test_parallel_parent_manifest_order_is_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            manifest = root / "download.jsonl"
            for day in ("2025-01-03", "2025-01-01", "2025-01-02"):
                compact = day.replace("-", "")
                archive = raw / "2025" / "01" / day[-2:] / f"nsccl.{compact}.i1.zip"
                _write_archive(archive, _xml())
                _manifest_event(manifest, raw, archive, day=day)
            report = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                max_workers=2,
                batch_rows=1,
            )
            events = [json.loads(line) for line in (root / "extract.jsonl").read_text().splitlines()]

        self.assertTrue(report.ok)
        self.assertEqual(report.created_fragment_count, 3)
        self.assertEqual([event["date"] for event in events], ["2025-01-01", "2025-01-02", "2025-01-03"])

    def test_concurrent_publish_is_atomic_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml(portfolio_count=20))
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            barrier = threading.Barrier(2)
            real_link = os.link

            def racing_link(source: object, destination: object, *args: object, **kwargs: object) -> None:
                barrier.wait(timeout=10)
                real_link(source, destination, *args, **kwargs)

            manifest_archive = load_manifest_archives(manifest, raw)[0]
            symbols = _normalize_symbols(("NIFTY",))
            identity = _extraction_identity(PARSER_VERSION, symbols)

            def run(_: str):
                return _extract_one_archive(
                    manifest_archive,
                    root / "fragments",
                    symbols,
                    3,
                    PARSER_VERSION,
                    identity,
                )

            with patch("nifty_span.span.streaming_extractor.os.link", side_effect=racing_link):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    reports = tuple(pool.map(run, ("a", "b")))
            fragments = list((root / "fragments").rglob("*.parquet"))
            fragment_digest = hashlib.sha256(fragments[0].read_bytes()).hexdigest()

        self.assertEqual(sum(result.state == "created" for result in reports), 1)
        self.assertEqual(sum(result.state == "skipped" for result in reports), 1)
        self.assertEqual(len(fragments), 1)
        created = next(result for result in reports if result.state == "created")
        self.assertEqual(created.fragment_sha256, fragment_digest)

    def test_sha_mismatch_is_reported_and_leaves_no_partial_fragment(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml())
            manifest = root / "download.jsonl"
            event = _event(raw, archive)
            event["sha256"] = "0" * 64
            manifest.write_text(json.dumps(event) + "\n", encoding="utf-8")

            report = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
            )

        self.assertFalse(report.ok)
        self.assertEqual(report.failed_archive_count, 1)
        self.assertFalse(any((root / "fragments").rglob("*.partial")))

    def test_zero_requested_symbol_rows_fail_as_coverage_anomaly(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            archive = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(archive, _xml().replace("NIFTY", "BANKNIFTY"))
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, archive)
            report = extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
            )
            events = [json.loads(line) for line in (root / "extract.jsonl").read_text().splitlines()]

        self.assertFalse(report.ok)
        self.assertEqual(report.row_count, 0)
        self.assertEqual(report.failed_archive_count, 1)
        self.assertEqual(events[0]["event"], "fragment_failed")
        self.assertIn("coverage anomaly", events[0]["error"])

    def test_month_compaction_deduplicates_identical_business_rows(self) -> None:
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            first = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(first, _xml())
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, first)
            extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                parser_version="parser-a",
            )
            extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                parser_version="parser-b",
            )

            compacted = compact_span_month(
                fragment_root=root / "fragments",
                parquet_root=root / "monthly",
                quarantine_root=root / "quarantine",
                year=2025,
                month=1,
            )
            unchanged = compact_span_month(
                fragment_root=root / "fragments",
                parquet_root=root / "monthly",
                quarantine_root=root / "quarantine",
                year=2025,
                month=1,
            )
            output_rows = pq.read_table(compacted.output_path).num_rows

        self.assertEqual(compacted.fragment_count, 2)
        self.assertEqual(compacted.input_row_count, 2)
        self.assertEqual(compacted.duplicate_row_count, 1)
        self.assertEqual(output_rows, 1)
        self.assertTrue(compacted.changed)
        self.assertFalse(unchanged.changed)

    def test_conflicting_natural_key_is_quarantined_and_month_fails_closed(self) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw = root / "raw"
            first = raw / "2025" / "01" / "02" / "nsccl.20250102.i1.zip"
            _write_archive(first, _xml(price=100.0))
            manifest = root / "download.jsonl"
            _manifest_event(manifest, raw, first)
            extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                parser_version="parser-a",
            )
            extract_manifest_archives(
                download_manifest=manifest,
                raw_root=raw,
                fragment_root=root / "fragments",
                extraction_manifest=root / "extract.jsonl",
                parser_version="parser-b",
            )
            fragment_paths = sorted((root / "fragments").rglob("*.parquet"))
            changed = pq.read_table(fragment_paths[1])
            price_index = changed.schema.get_field_index("price")
            changed = changed.set_column(price_index, "price", pa.array([101.0]))
            pq.write_table(changed, fragment_paths[1], compression="zstd")

            with self.assertRaises(SpanNaturalKeyConflictError) as caught:
                compact_span_month(
                    fragment_root=root / "fragments",
                    parquet_root=root / "monthly",
                    quarantine_root=root / "quarantine",
                    year=2025,
                    month=1,
                )

            quarantine = caught.exception.quarantine_path
            self.assertTrue(quarantine.exists())
            self.assertEqual(pq.read_table(quarantine).num_rows, 2)
            self.assertFalse((root / "monthly" / "2025_01.parquet").exists())


def _event(raw: Path, archive: Path, *, day: str = "2025-01-02", observed: str = "2025-01-02T03:30:00Z") -> dict[str, object]:
    return {
        "observed_at_utc": observed,
        "trading_date": day,
        "slot": "BOD",
        "suffix": "i1",
        "state": "downloaded",
        "path": archive.relative_to(raw).as_posix(),
        "sha256": _sha256(archive),
        "size_bytes": archive.stat().st_size,
        "terminal": True,
        "outer_member": archive.name,
        "inner_spn": archive.stem + ".spn",
    }


def _manifest_event(
    manifest: Path,
    raw: Path,
    archive: Path,
    *,
    day: str = "2025-01-02",
    observed: str = "2025-01-02T03:30:00Z",
) -> None:
    with manifest.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(_event(raw, archive, day=day, observed=observed)) + "\n")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_archive(path: Path, xml: str, *, zip_comment: bytes = b"") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    parts = path.name.split(".")
    day = next((part for part in parts if len(part) == 8 and part.isdigit()), "20250102")
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr(f"nsccl.{day}.i1.spn", xml)
        zipped.comment = zip_comment


EXPECTED_SCENARIOS = (
    1.0,
    2.0,
    3.0,
    4.0,
    5.0,
    6.0,
    7.0,
    8.0,
    9.0,
    10.0,
    11.0,
    12.0,
    13.0,
    14.0,
    15.0,
    16.0,
)
FUT_SCENARIOS = (
    101.0,
    102.0,
    103.0,
    104.0,
    105.0,
    106.0,
    107.0,
    108.0,
    109.0,
    110.0,
    111.0,
    112.0,
    113.0,
    114.0,
    115.0,
    116.0,
)
PE_SCENARIOS = (
    -1.0,
    -2.0,
    -3.0,
    -4.0,
    -5.0,
    -6.0,
    -7.0,
    -8.0,
    -9.0,
    -10.0,
    -11.0,
    -12.0,
    -13.0,
    -14.0,
    -15.0,
    -16.0,
)


def _normalized_business_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    normalized = [{field: row[field] for field in BUSINESS_FIELDS} for row in rows]
    return sorted(
        normalized,
        key=lambda row: (
            row["date"],
            row["time_slot"],
            row["symbol"],
            row["instrument"],
            row["expiry"],
            row["strike"],
        ),
    )


def _golden_business_rows() -> list[dict[str, object]]:
    return [
        _golden_row(
            instrument="FUT",
            expiry=date(2025, 2, 27),
            strike=0.0,
            price=23888.5,
            delta=1.0,
            implied_vol=0.0,
            price_scan=1200.0,
            vol_scan=11.0,
            cvf=2.5,
            scenarios=FUT_SCENARIOS,
            composite_delta=0.91,
        ),
        _golden_row(
            instrument="CE",
            expiry=date(2025, 1, 30),
            strike=24000.0,
            price=123.45,
            delta=0.52,
            implied_vol=0.1875,
            price_scan=950.0,
            vol_scan=8.5,
            cvf=1.75,
            scenarios=EXPECTED_SCENARIOS,
            composite_delta=0.48,
        ),
        _golden_row(
            instrument="PE",
            expiry=date(2025, 1, 30),
            strike=23500.0,
            price=87.65,
            delta=-0.31,
            implied_vol=0.205,
            price_scan=950.0,
            vol_scan=8.5,
            cvf=1.75,
            scenarios=PE_SCENARIOS,
            composite_delta=-0.29,
        ),
    ]


def _golden_row(
    *,
    instrument: str,
    expiry: date,
    strike: float,
    price: float,
    delta: float,
    implied_vol: float,
    price_scan: float,
    vol_scan: float,
    cvf: float,
    scenarios: tuple[float, ...],
    composite_delta: float,
) -> dict[str, object]:
    row: dict[str, object] = {
        "date": date(2025, 1, 2),
        "time_slot": "BOD",
        "symbol": "NIFTY",
        "instrument": instrument,
        "expiry": expiry,
        "strike": strike,
        "price": price,
        "delta": delta,
        "implied_vol": implied_vol,
        "price_scan_range": price_scan,
        "vol_scan_range": vol_scan,
        "cvf": cvf,
        "composite_delta": composite_delta,
    }
    row.update({f"s{index}": value for index, value in enumerate(scenarios, start=1)})
    return row


def _golden_equivalence_xml() -> str:
    return """<root fileCreated="2025-01-02T08:45:00+05:30">
  <futPf>
    <pfCode>NIFTY</pfCode><cvf>2.5</cvf>
    <fut><pe>20250227</pe><p>23888.5</p><d>1</d>
      <scanRate><priceScan>1200</priceScan><volScan>11</volScan></scanRate>
      <ra><a>101</a><a>102</a><a>103</a><a>104</a><a>105</a><a>106</a><a>107</a><a>108</a><a>109</a><a>110</a><a>111</a><a>112</a><a>113</a><a>114</a><a>115</a><a>116</a><d>0.91</d></ra>
    </fut>
  </futPf>
  <oopPf>
    <pfCode>NIFTY</pfCode><cvf>3</cvf>
    <series><pe>20250130</pe><cvf>1.75</cvf>
      <scanRate><priceScan>950</priceScan><volScan>8.5</volScan></scanRate>
      <opt><o>C</o><k>24000</k><p>123.45</p><d>0.52</d><v>0.1875</v>
        <ra><a>1</a><a>2</a><a>3</a><a>4</a><a>5</a><a>6</a><a>7</a><a>8</a><a>9</a><a>10</a><a>11</a><a>12</a><a>13</a><a>14</a><a>15</a><a>16</a><d>0.48</d></ra>
      </opt>
      <opt><o>P</o><k>23500</k><p>87.65</p><d>-0.31</d><v>0.205</v>
        <ra><a>-1</a><a>-2</a><a>-3</a><a>-4</a><a>-5</a><a>-6</a><a>-7</a><a>-8</a><a>-9</a><a>-10</a><a>-11</a><a>-12</a><a>-13</a><a>-14</a><a>-15</a><a>-16</a><d>-0.29</d></ra>
      </opt>
    </series>
  </oopPf>
  <oopPf>
    <pfCode>BANKNIFTY</pfCode><cvf>1</cvf>
    <series><pe>20250130</pe><scanRate><priceScan>2000</priceScan><volScan>15</volScan></scanRate>
      <opt><o>C</o><k>51000</k><p>210</p><d>0.5</d><v>0.22</v>
        <ra><a>999</a><d>0.5</d></ra>
      </opt>
    </series>
  </oopPf>
</root>"""


def _xml(
    *,
    price: float = 100.5,
    portfolio_count: int = 1,
    created: str = "2025-01-02T08:45:00+05:30",
    effective: str | None = None,
) -> str:
    portfolios = []
    for index in range(portfolio_count):
        portfolios.append(
            f"""
  <oopPf>
    <pfCode>NIFTY</pfCode><cvf>1</cvf>
    <series><pe>20250109</pe><cvf>1</cvf>
      <scanRate><priceScan>100</priceScan><volScan>10</volScan></scanRate>
      <opt><o>C</o><k>{24000 + index}</k><p>{price}</p><d>0.5</d><v>0.18</v>
        <ra>{''.join(f'<a>{value}</a>' for value in range(1, 17))}<d>0.5</d></ra>
      </opt>
    </series>
  </oopPf>"""
        )
    effective_attr = "" if effective is None else f' effectiveTimestamp="{effective}"'
    return f'<root fileCreated="{created}"{effective_attr}>{"".join(portfolios)}</root>'


if __name__ == "__main__":
    unittest.main()
