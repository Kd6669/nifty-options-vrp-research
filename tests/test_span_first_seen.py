from __future__ import annotations

from datetime import datetime
from io import BytesIO
import json
from pathlib import Path
from zoneinfo import ZoneInfo
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from dhan_data_fetch_stream.span_first_seen import poll_span_archive_first_seen


IST = ZoneInfo("Asia/Kolkata")


def test_first_valid_archive_sha_is_atomic_and_idempotent(tmp_path: Path) -> None:
    payload = _zip_bytes()
    manifest = tmp_path / "first_seen.json"
    archive_dir = tmp_path / "archives"
    opener = _opener(payload)
    today = datetime.now(IST).date().isoformat()
    first = poll_span_archive_first_seen(
        url="https://www.nseclearing.in/reports/nsccl.test.zip",
        trading_date=today,
        time_slot="ID1",
        manifest_path=manifest,
        archive_dir=archive_dir,
        poll_seconds=0,
        max_attempts=1,
        opener=opener,
    )
    second = poll_span_archive_first_seen(
        url="https://www.nseclearing.in/reports/nsccl.test.zip",
        trading_date=today,
        time_slot="ID1",
        manifest_path=manifest,
        archive_dir=archive_dir,
        poll_seconds=0,
        max_attempts=1,
        opener=opener,
    )
    assert first.resumed is False
    assert second.resumed is True
    assert second.first_seen_ts_ist == first.first_seen_ts_ist
    assert Path(first.archive_path or "").read_bytes() == payload
    records = json.loads(manifest.read_text(encoding="utf-8"))
    assert len(records) == 1
    assert records[0]["source_sha256"] == first.source_sha256
    assert not list(tmp_path.rglob("*.partial"))


def test_http_200_non_zip_is_rejected_without_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "first_seen.json"
    with pytest.raises(RuntimeError, match="not a ZIP"):
        poll_span_archive_first_seen(
            url="https://www.nseclearing.in/reports/error.zip",
            trading_date=datetime.now(IST).date().isoformat(),
            time_slot="EOD",
            manifest_path=manifest,
            poll_seconds=0,
            max_attempts=1,
            opener=_opener(b"<html>temporary error</html>"),
        )
    assert not manifest.exists()


def test_non_official_domain_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="official NSE"):
        poll_span_archive_first_seen(
            url="https://example.com/archive.zip",
            trading_date=datetime.now(IST).date().isoformat(),
            time_slot="BOD",
            manifest_path=tmp_path / "first_seen.json",
            max_attempts=1,
        )


def _zip_bytes() -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        archive.writestr("nsccl.spn", "verified SPAN fixture")
    return buffer.getvalue()


def _opener(payload: bytes):
    def open_response(_request, *, timeout):
        assert timeout > 0
        return _Response(payload)

    return open_response


class _Response:
    status = 200

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def getcode(self) -> int:
        return self.status

    def read(self) -> bytes:
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None
