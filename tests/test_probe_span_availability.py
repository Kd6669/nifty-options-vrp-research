from __future__ import annotations

from datetime import date
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
import zipfile


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "probe_span_availability.py"
SPEC = importlib.util.spec_from_file_location("probe_span_availability", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - import machinery guard.
    raise RuntimeError(f"could not load {SCRIPT_PATH}")
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class SpanAvailabilityProbeTests(unittest.TestCase):
    def test_six_slot_nested_zip_response(self) -> None:
        body = _outer_zip("20250102", ("i1", "i2", "i3", "i4", "i5", "s"))

        result = _inspect(body)

        self.assertEqual(result["returned_suffixes"], ["i1", "i2", "i3", "i4", "i5", "s"])
        self.assertEqual(result["missing_suffixes"], [])
        self.assertEqual(result["returned_slots"], ["BOD", "ID1", "ID2", "ID3", "ID4", "EOD"])
        self.assertEqual(result["valid_suffixes"], result["returned_suffixes"])
        self.assertEqual(result["validation_errors"], [])
        self.assertIsNone(result["outer_zip"]["testzip_bad_member"])
        expected_spn_names = {
            "nsccl.20250102.i01.spn",
            "nsccl.20250102.i02.spn",
            "nsccl.20250102.i03.spn",
            "nsccl.20250102.i04.spn",
            "nsccl.20250102.i05.spn",
            "nsccl.20250102.s.spn",
        }
        actual_spn_names = {
            member["inner_zip"]["spn_members"][0]["name"]
            for member in result["outer_zip"]["members"]
        }
        self.assertEqual(actual_spn_names, expected_spn_names)
        for member in result["outer_zip"]["members"]:
            self.assertTrue(member["safe_basename"])
            self.assertTrue(member["date_matches"])
            self.assertTrue(member["valid"])
            self.assertIsNone(member["inner_zip"]["testzip_bad_member"])
            spn = member["inner_zip"]["spn_members"][0]
            self.assertEqual(spn["xml_root"], "spanFile")
            self.assertEqual(spn["nifty_counts"], {"FUT": 1, "CE": 2, "PE": 1})
            self.assertEqual(spn["nifty_total_rows"], 4)

    def test_two_slot_response_reports_explicit_missing_slots(self) -> None:
        body = _outer_zip("20250102", ("i1", "s"))

        result = _inspect(body)

        self.assertEqual(result["returned_suffixes"], ["i1", "s"])
        self.assertEqual(result["missing_suffixes"], ["i2", "i3", "i4", "i5"])
        self.assertEqual(result["returned_slots"], ["BOD", "EOD"])
        self.assertEqual(result["missing_slots"], ["ID1", "ID2", "ID3", "ID4"])
        self.assertEqual(result["validation_errors"], [])

    def test_non_zip_response_has_bounded_preview(self) -> None:
        body = b'{"error":"Not Found, may be some files are unavailable","show":true}'

        result = _inspect(body, status=404, content_type="application/json; charset=utf-8", preview_bytes=20)

        self.assertFalse(result["response"]["is_zip"])
        self.assertEqual(result["response"]["preview_bytes"], 20)
        self.assertEqual(result["response"]["preview_utf8"], body[:20].decode())
        self.assertIsNone(result["outer_zip"])
        self.assertEqual(result["missing_suffixes"], ["i1", "i2", "i3", "i4", "i5", "s"])

    def test_outer_filename_date_mismatch_is_rejected(self) -> None:
        body = _outer_zip(
            "20250102",
            ("i1",),
            outer_name_override={"i1": "nsccl.20250103.i1.zip"},
        )

        result = _inspect(body)

        member = result["outer_zip"]["members"][0]
        self.assertTrue(member["safe_basename"])
        self.assertTrue(member["name_valid"])
        self.assertFalse(member["date_matches"])
        self.assertFalse(member["valid"])
        self.assertEqual(result["returned_suffixes"], [])
        self.assertTrue(any("date does not match" in item for item in result["validation_errors"]))

    def test_corrupt_inner_zip_is_returned_but_invalid(self) -> None:
        body = _outer_zip("20250102", ("i1",), corrupt_suffix="i1")

        result = _inspect(body)

        member = result["outer_zip"]["members"][0]
        self.assertEqual(result["returned_suffixes"], ["i1"])
        self.assertEqual(result["valid_suffixes"], [])
        self.assertEqual(result["invalid_suffixes"], ["i1"])
        self.assertFalse(member["inner_zip"]["is_zip"])
        self.assertFalse(member["valid"])
        self.assertTrue(any("inner member is not a ZIP" in item for item in result["validation_errors"]))

    def test_output_json_is_replaced_without_temp_residue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "nested" / "probe.json"
            target.parent.mkdir()
            target.write_text('{"stale":true}\n', encoding="utf-8")

            probe.write_json_atomic(target, {"fresh": True})

            self.assertEqual(json.loads(target.read_text(encoding="utf-8")), {"fresh": True})
            self.assertEqual(list(target.parent.glob(f".{target.name}.*.tmp")), [])


def _inspect(
    body: bytes,
    *,
    status: int = 200,
    content_type: str = "application/zip",
    preview_bytes: int = 500,
):
    return probe.inspect_response(
        trading_date=date(2025, 1, 2),
        status_code=status,
        headers={"Content-Type": content_type, "Content-Length": str(len(body))},
        body=body,
        requested_utc="2026-07-15T12:00:00.000Z",
        finished_utc="2026-07-15T12:00:01.000Z",
        elapsed_seconds=1.0,
        preview_bytes=preview_bytes,
    )


def _outer_zip(
    date_tag: str,
    suffixes: tuple[str, ...],
    *,
    outer_name_override: dict[str, str] | None = None,
    corrupt_suffix: str | None = None,
) -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as outer:
        for suffix in suffixes:
            outer_name = (outer_name_override or {}).get(suffix, f"nsccl.{date_tag}.{suffix}.zip")
            inner_bytes = b"this is not a ZIP" if suffix == corrupt_suffix else _inner_zip(date_tag, suffix)
            outer.writestr(outer_name, inner_bytes)
    return stream.getvalue()


def _inner_zip(date_tag: str, suffix: str) -> bytes:
    inner_suffix = f"i0{suffix[1]}" if suffix.startswith("i") else suffix
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_DEFLATED) as inner:
        inner.writestr(f"nsccl.{date_tag}.{inner_suffix}.spn", _span_xml())
    return stream.getvalue()


def _span_xml() -> bytes:
    return b"""<spanFile>
  <futPf><pfCode>NIFTY</pfCode><fut><pe>20250130</pe></fut></futPf>
  <futPf><pfCode>BANKNIFTY</pfCode><fut><pe>20250130</pe></fut></futPf>
  <oopPf><pfCode>NIFTY</pfCode><series><pe>20250130</pe>
    <opt><o>C</o></opt><opt><o>CALL</o></opt><opt><o>P</o></opt>
  </series></oopPf>
</spanFile>"""


if __name__ == "__main__":
    unittest.main()
