from __future__ import annotations

from contextlib import redirect_stdout
from datetime import date
from hashlib import sha256
from io import StringIO
import multiprocessing
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from unittest import TestCase
import json
import zipfile

import pyarrow.parquet as pq

from nifty_span.cli import span_backfill_main
from nifty_span.span.backfill import (
    SpanExtractionLockTimeout,
    extract_and_compact_span_range,
    extraction_compaction_lock,
)
from nifty_span.span.backfill_audit import audit_span_backfill
from nifty_span.span.backfill_downloader import SLOT_SPECS


def _hold_extraction_lock(
    extraction_manifest: str, ready: object, release: object, crash: bool
) -> None:
    with extraction_compaction_lock(
        extraction_manifest, timeout_seconds=5.0, poll_seconds=0.02
    ):
        ready.set()  # type: ignore[attr-defined]
        if crash:
            os._exit(0)
        if not release.wait(20):  # type: ignore[attr-defined]
            os._exit(3)


class SpanBackfillPipelineTests(TestCase):
    def test_second_process_cannot_enter_extract_compact_transaction(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            extraction_manifest = root / "manifests" / "extraction.jsonl"
            context = multiprocessing.get_context("spawn")
            ready = context.Event()
            release = context.Event()
            holder = context.Process(
                target=_hold_extraction_lock,
                args=(str(extraction_manifest), ready, release, False),
            )
            holder.start()
            try:
                self.assertTrue(ready.wait(10), "lock holder did not become ready")
                started = time.monotonic()
                with self.assertRaises(SpanExtractionLockTimeout):
                    extract_and_compact_span_range(
                        start_date=date(2025, 1, 2),
                        end_date=date(2025, 1, 2),
                        raw_root=root / "raw",
                        download_manifest=root / "download.jsonl",
                        fragment_root=root / "fragments",
                        extraction_manifest=extraction_manifest,
                        compacted_root=root / "compacted",
                        quarantine_root=root / "quarantine",
                        lock_timeout_seconds=0.20,
                        lock_poll_seconds=0.02,
                    )
                self.assertGreaterEqual(time.monotonic() - started, 0.15)
            finally:
                release.set()
                holder.join(10)
                if holder.is_alive():
                    holder.terminate()
                    holder.join(5)
            self.assertEqual(holder.exitcode, 0)

    def test_extraction_lock_is_released_when_holder_process_crashes(self) -> None:
        with TemporaryDirectory() as tmp:
            extraction_manifest = Path(tmp) / "manifests" / "extraction.jsonl"
            context = multiprocessing.get_context("spawn")
            ready = context.Event()
            release = context.Event()
            holder = context.Process(
                target=_hold_extraction_lock,
                args=(str(extraction_manifest), ready, release, True),
            )
            holder.start()
            self.assertTrue(ready.wait(10), "crashing lock holder did not become ready")
            holder.join(10)
            if holder.is_alive():
                holder.terminate()
                holder.join(5)
                self.fail("crashing lock holder did not exit")
            self.assertEqual(holder.exitcode, 0)

            with extraction_compaction_lock(
                extraction_manifest, timeout_seconds=1.0, poll_seconds=0.02
            ) as lock_path:
                self.assertTrue(lock_path.is_file())

    def test_extract_compact_audit_and_idempotent_rerun(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = _fixture(root)
            first = extract_and_compact_span_range(
                start_date=date(2025, 1, 2),
                end_date=date(2025, 1, 2),
                raw_root=paths["raw"],
                download_manifest=paths["download"],
                fragment_root=paths["fragments"],
                extraction_manifest=paths["extraction"],
                compacted_root=paths["compacted"],
                quarantine_root=paths["quarantine"],
                batch_rows=1,
            )
            compact_path = Path(first.compacted_months[0].output_path)
            first_hash = sha256(compact_path.read_bytes()).hexdigest()
            extraction_manifest_bytes = paths["extraction"].read_bytes()
            second = extract_and_compact_span_range(
                start_date=date(2025, 1, 2),
                end_date=date(2025, 1, 2),
                raw_root=paths["raw"],
                download_manifest=paths["download"],
                fragment_root=paths["fragments"],
                extraction_manifest=paths["extraction"],
                compacted_root=paths["compacted"],
                quarantine_root=paths["quarantine"],
                batch_rows=2,
            )
            audit = audit_span_backfill(
                start_date=date(2025, 1, 2),
                end_date=date(2025, 1, 2),
                raw_root=paths["raw"],
                download_manifest=paths["download"],
                extraction_manifest=paths["extraction"],
                fragment_root=paths["fragments"],
                compacted_root=paths["compacted"],
                report_root=paths["reports"],
            )
            table = pq.read_table(compact_path)

            self.assertTrue(first.ok)
            self.assertEqual(first.extraction.created_fragment_count, 6)
            self.assertEqual(second.extraction.skipped_fragment_count, 6)
            self.assertFalse(second.compacted_months[0].changed)
            self.assertEqual(paths["extraction"].read_bytes(), extraction_manifest_bytes)
            self.assertEqual(sha256(compact_path.read_bytes()).hexdigest(), first_hash)
            self.assertTrue(audit.ok)
            self.assertEqual(set(table.column("instrument").to_pylist()), {"CE", "PE", "FUT"})
            self.assertEqual(table.column("span_effective_ts_ist").null_count, table.num_rows)
            self.assertEqual(set(table.column("effective_time_source").to_pylist()), {"unknown"})

    def test_audit_cli_mode_emits_machine_readable_failure(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = StringIO()
            with redirect_stdout(output):
                code = span_backfill_main(
                    [
                        "audit",
                        "--start-date", "2025-01-02",
                        "--end-date", "2025-01-02",
                        "--raw-root", str(root / "raw"),
                        "--download-manifest", str(root / "missing-download.jsonl"),
                        "--extraction-manifest", str(root / "missing-extraction.jsonl"),
                        "--fragment-root", str(root / "fragments"),
                        "--parquet-root", str(root / "compacted"),
                        "--report-root", str(root / "reports"),
                        "--json",
                    ]
                )
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 1)
            self.assertEqual(payload["expected_cells"], 6)
            self.assertFalse(payload["ok"])


def _fixture(root: Path) -> dict[str, Path]:
    raw = root / "raw"
    download = root / "download.jsonl"
    directory = raw / "2025" / "01" / "02"
    directory.mkdir(parents=True)
    events = []
    for slot, suffix in SLOT_SPECS:
        archive = directory / f"nsccl.20250102.{suffix}.zip"
        member_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else "s"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
            zipped.writestr(f"nsccl.20250102.{member_suffix}.spn", _span_xml())
        events.append({
            "observed_at_utc": "2025-01-02T03:30:00Z",
            "trading_date": "2025-01-02",
            "slot": slot,
            "suffix": suffix,
            "state": "downloaded",
            "terminal": True,
            "path": archive.relative_to(raw).as_posix(),
            "sha256": sha256(archive.read_bytes()).hexdigest(),
            "size_bytes": archive.stat().st_size,
        })
    download.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    return {
        "raw": raw,
        "download": download,
        "fragments": root / "fragments",
        "extraction": root / "extraction.jsonl",
        "compacted": root / "compacted",
        "quarantine": root / "quarantine",
        "reports": root / "reports",
    }


def _span_xml() -> str:
    risk = "".join(f"<a>{value}</a>" for value in range(1, 17))
    return f"""<spanFile fileCreated="2025-01-02T08:45:00+05:30">
  <futPf><pfCode>NIFTY</pfCode><cvf>1</cvf><fut><pe>20250130</pe><p>24000</p><d>1</d>
    <scanRate><priceScan>100</priceScan><volScan>10</volScan></scanRate><ra>{risk}<d>1</d></ra>
  </fut></futPf>
  <oopPf><pfCode>NIFTY</pfCode><cvf>1</cvf><series><pe>20250109</pe>
    <scanRate><priceScan>100</priceScan><volScan>10</volScan></scanRate>
    <opt><o>C</o><k>24000</k><p>100</p><d>0.5</d><v>0.18</v><ra>{risk}<d>0.5</d></ra></opt>
    <opt><o>P</o><k>24000</k><p>90</p><d>-0.5</d><v>0.19</v><ra>{risk}<d>-0.5</d></ra></opt>
  </series></oopPf>
</spanFile>"""
