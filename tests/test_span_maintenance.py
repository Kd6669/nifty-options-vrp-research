from __future__ import annotations

from datetime import date
import tempfile
import unittest
import zipfile
from pathlib import Path

from nifty_span.span.downloader import SpanDownloadDayResult
from nifty_span.span.maintenance import run_span_maintenance_once, write_span_maintenance_report


class SpanMaintenanceTests(unittest.TestCase):
    def test_maintenance_extracts_new_raw_zip_and_selects_latest_slot(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow is not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_root = root / "raw"
            day_dir = raw_root / "2025" / "01" / "02"
            day_dir.mkdir(parents=True)
            _write_span_zip(day_dir / "nsccl.20250102.i1.zip")
            _write_span_zip(day_dir / "nsccl.20250102.i4.zip")
            parquet_dir = root / "parquet"

            def fake_download(*, trading_date: date, output_root: Path) -> SpanDownloadDayResult:
                return SpanDownloadDayResult(
                    trading_date=trading_date.isoformat(),
                    status="skipped_existing",
                    extracted_files=2,
                    output_dir=str(output_root / "2025" / "01" / "02"),
                )

            report = run_span_maintenance_once(
                trading_date=date(2025, 1, 2),
                raw_root=raw_root,
                parquet_dir=parquet_dir,
                preferred_time_slot="LATEST",
                symbols_filter=("NIFTY",),
                max_workers=1,
                download_fn=fake_download,
            )

        self.assertTrue(report.ok)
        self.assertTrue(report.changed)
        self.assertEqual(report.parsed_slots, ("BOD", "ID3"))
        self.assertEqual(report.selected_time_slot, "ID3")
        self.assertEqual(report.raw_zip_count, 2)

    def test_maintenance_report_write_is_json(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow is not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            raw_root = root / "raw"
            day_dir = raw_root / "2025" / "01" / "02"
            day_dir.mkdir(parents=True)
            _write_span_zip(day_dir / "nsccl.20250102.i1.zip")
            out = root / "span_maintenance_latest.json"

            def fake_download(*, trading_date: date, output_root: Path) -> SpanDownloadDayResult:
                return SpanDownloadDayResult(
                    trading_date=trading_date.isoformat(),
                    status="skipped_existing",
                    extracted_files=1,
                    output_dir=str(output_root / "2025" / "01" / "02"),
                )

            report = run_span_maintenance_once(
                trading_date=date(2025, 1, 2),
                raw_root=raw_root,
                parquet_dir=root / "parquet",
                symbols_filter=("NIFTY",),
                max_workers=1,
                download_fn=fake_download,
            )
            write_span_maintenance_report(report, out)
            payload = out.read_text(encoding="utf-8")

        self.assertIn('"selected_time_slot": "BOD"', payload)


def _write_span_zip(path: Path) -> None:
    xml = """
<root>
  <oopPf>
    <pfCode>NIFTY</pfCode>
    <cvf>1</cvf>
    <series>
      <pe>20250109</pe>
      <cvf>1</cvf>
      <scanRate><priceScan>100</priceScan><volScan>10</volScan></scanRate>
      <opt>
        <o>C</o>
        <k>24000</k>
        <p>100.5</p>
        <d>0.5</d>
        <v>0.18</v>
        <ra>
          <a>1</a><a>2</a><a>3</a><a>4</a>
          <a>5</a><a>6</a><a>7</a><a>8</a>
          <a>9</a><a>10</a><a>11</a><a>12</a>
          <a>13</a><a>14</a><a>15</a><a>16</a>
          <d>0.5</d>
        </ra>
      </opt>
    </series>
  </oopPf>
</root>
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(path.with_suffix(".spn").name, xml)
