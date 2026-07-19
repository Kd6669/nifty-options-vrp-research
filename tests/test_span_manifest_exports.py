from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from nifty_span.span.manifest_exports import (
    _atomic_write_bytes,
    export_latest_manifest,
    read_stable_jsonl_prefix,
)


class SpanManifestExportTests(unittest.TestCase):
    def test_download_export_is_deterministic_and_selects_latest_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "download.jsonl"
            events = [
                _download_event("2025-01-02", "ID1", "i2", "retrying_transport_error", 1),
                _download_event("2025-01-02", "BOD", "i1", "downloaded", 1),
                _download_event("2025-01-02", "ID1", "i2", "downloaded", 2),
            ]
            _write_jsonl(source, events)

            first = export_latest_manifest(source, root / "out", manifest_kind="download")
            first_json = Path(first.json_path).read_bytes()
            first_parquet = Path(first.parquet_path).read_bytes()
            first_metadata = Path(first.metadata_path).read_bytes()
            second = export_latest_manifest(source, root / "out", manifest_kind="download")

            self.assertEqual(Path(second.json_path).read_bytes(), first_json)
            self.assertEqual(Path(second.parquet_path).read_bytes(), first_parquet)
            self.assertEqual(Path(second.metadata_path).read_bytes(), first_metadata)
            self.assertEqual(second.latest_row_count, 2)
            self.assertEqual(second.superseded_event_count, 1)
            table = pq.read_table(second.parquet_path)
            self.assertEqual(table.column("slot").to_pylist(), ["BOD", "ID1"])
            self.assertEqual(table.column("attempt").to_pylist(), [1, 2])
            self.assertEqual(table.column("state").to_pylist(), ["downloaded", "downloaded"])

    def test_schema_preserves_types_and_lossless_nested_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "download.jsonl"
            event = _download_event("2025-01-02", "BOD", "i1", "downloaded", 3)
            event.update(
                {
                    "custom_future_field": {"answer": 42},
                    "members": ["nsccl.20250102.i1.spn"],
                    "response": {"content_length": 123, "magic_hex": "504b0304"},
                    "size_bytes": 999,
                    "zip_crc_ok": True,
                }
            )
            _write_jsonl(source, [event])
            report = export_latest_manifest(source, root / "out", manifest_kind="download")

            table = pq.read_table(report.parquet_path)
            schema = table.schema
            self.assertEqual(schema.field("trading_date").type, pa.date32())
            self.assertEqual(schema.field("attempt").type, pa.int32())
            self.assertEqual(schema.field("size_bytes").type, pa.int64())
            self.assertEqual(schema.field("terminal").type, pa.bool_())
            self.assertEqual(schema.field("members").type, pa.list_(pa.string()))
            self.assertEqual(table.column("trading_date")[0].as_py(), date(2025, 1, 2))
            restored = json.loads(table.column("event_json")[0].as_py())
            self.assertEqual(restored, event)

    def test_extraction_identity_keeps_distinct_source_hashes_and_latest_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "extraction.jsonl"
            events = [
                _extraction_event("a" * 64, "fragment_created", row_count=12),
                _extraction_event("b" * 64, "fragment_created", row_count=13),
                _extraction_event("a" * 64, "fragment_failed", error="checksum changed"),
            ]
            _write_jsonl(source, events)
            report = export_latest_manifest(source, root / "out", manifest_kind="extraction")

            table = pq.read_table(report.parquet_path)
            self.assertEqual(report.latest_row_count, 2)
            self.assertEqual(table.column("source_sha256").to_pylist(), ["a" * 64, "b" * 64])
            self.assertEqual(table.column("event").to_pylist(), ["fragment_failed", "fragment_created"])
            self.assertEqual(table.column("row_count").to_pylist(), [None, 13])

    def test_trailing_line_is_excluded_but_malformed_stable_line_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "download.jsonl"
            complete = json.dumps(_download_event("2025-01-02", "BOD", "i1", "downloaded", 1))
            trailing = b'{"trading_date":"2025-01-03"'
            source.write_bytes((complete + "\n").encode() + trailing)

            snapshot = read_stable_jsonl_prefix(source)
            self.assertEqual(snapshot.event_count, 1)
            self.assertEqual(snapshot.ignored_trailing_bytes, len(trailing))
            report = export_latest_manifest(source, root / "out", manifest_kind="download")
            self.assertEqual(report.latest_row_count, 1)
            self.assertEqual(report.ignored_trailing_bytes, len(trailing))

            source.write_bytes(b"{not-json}\n")
            with self.assertRaisesRegex(ValueError, "invalid stable manifest JSON"):
                read_stable_jsonl_prefix(source)

    def test_atomic_replacement_keeps_old_target_when_replace_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            target = Path(temp) / "manifest.json"
            target.write_bytes(b"old")
            with patch("nifty_span.span.manifest_exports.os.replace", side_effect=OSError("blocked")):
                with self.assertRaisesRegex(OSError, "blocked"):
                    _atomic_write_bytes(target, b"new")
            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(list(target.parent.glob("*.partial")), [])

    def test_metadata_hashes_and_counts_match_published_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "download.jsonl"
            _write_jsonl(
                source,
                [_download_event("2025-01-02", "BOD", "i1", "downloaded", 1)],
            )
            report = export_latest_manifest(source, root / "out", manifest_kind="download")
            metadata = json.loads(Path(report.metadata_path).read_text(encoding="utf-8"))

            self.assertEqual(metadata["source"]["event_count"], 1)
            self.assertEqual(metadata["source"]["latest_row_count"], 1)
            for kind, path in (("json", report.json_path), ("parquet", report.parquet_path)):
                digest = hashlib.sha256(Path(path).read_bytes()).hexdigest()
                self.assertEqual(metadata["artifacts"][kind]["sha256"], digest)


def _download_event(day: str, slot: str, suffix: str, state: str, attempt: int) -> dict[str, object]:
    return {
        "schema_version": 1,
        "event_id": f"event-{slot}-{attempt}",
        "run_id": "run-1",
        "observed_at_utc": "2025-01-02T03:30:00Z",
        "trading_date": day,
        "slot": slot,
        "suffix": suffix,
        "state": state,
        "terminal": state == "downloaded",
        "attempt": attempt,
        "http_status": 200 if state == "downloaded" else None,
    }


def _extraction_event(
    digest: str, event: str, *, row_count: int | None = None, error: str | None = None
) -> dict[str, object]:
    return {
        "event": event,
        "date": "2025-01-02",
        "slot": "BOD",
        "source_file": "nsccl.20250102.i1.zip",
        "source_sha256": digest,
        "fragment_path": f"2025/01/02/{digest[:8]}.parquet",
        "fragment_sha256": "c" * 64 if event == "fragment_created" else None,
        "fragment_size_bytes": 456 if event == "fragment_created" else None,
        "parser_version": "test-parser",
        "schema_version": "span-arrow-schema-v1",
        "symbols_filter": ["NIFTY"],
        "extraction_identity": "identity-1",
        "ingested_at_utc": "2025-01-02T03:30:00+00:00",
        "row_count": row_count,
        "instrument_counts": {"CE": 5, "FUT": 1, "PE": 6} if row_count else None,
        "error_type": "ValueError" if error else None,
        "error": error,
    }


def _write_jsonl(path: Path, events: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n" for event in events),
        encoding="utf-8",
    )


if __name__ == "__main__":
    unittest.main()
