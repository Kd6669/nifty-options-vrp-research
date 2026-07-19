from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import asyncio
import io
import json
import zipfile


API_URL = "https://www.nseindia.com/api/reports"
HOME_URL = "https://www.nseindia.com"
REFERER = "https://www.nseindia.com/all-reports-derivatives"
ARCHIVES_PAYLOAD = json.dumps(
    [
        {"name": "F&O - Span Risk Parameter File (1st intra-day)", "type": "archives", "category": "derivatives", "section": "equity"},
        {"name": "F&O - Span Risk Parameter File (2nd intra-day)", "type": "archives", "category": "derivatives", "section": "equity"},
        {"name": "F&O - Span Risk Parameter File (3rd intra-day)", "type": "archives", "category": "derivatives", "section": "equity"},
        {"name": "F&O - Span Risk Parameter File (4th intra-day)", "type": "archives", "category": "derivatives", "section": "equity"},
        {"name": "F&O - Span Risk Parameter File (End of day)", "type": "archives", "category": "derivatives", "section": "equity"},
        {"name": "F&O - Span Risk Parameter File (Begin of day)", "type": "archives", "category": "derivatives", "section": "equity"},
    ]
)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": REFERER,
    "Connection": "keep-alive",
}


@dataclass(frozen=True)
class SpanDownloadDayResult:
    trading_date: str
    status: str
    extracted_files: int
    output_dir: str
    http_status: int | None = None
    error: str | None = None


def download_span_range(
    *,
    start_date: date,
    end_date: date,
    output_root: str | Path,
    max_concurrent: int = 2,
) -> list[SpanDownloadDayResult]:
    return asyncio.run(
        _download_span_range_async(
            start_date=start_date,
            end_date=end_date,
            output_root=Path(output_root),
            max_concurrent=max_concurrent,
        )
    )


def download_span_day(*, trading_date: date, output_root: str | Path) -> SpanDownloadDayResult:
    return download_span_range(start_date=trading_date, end_date=trading_date, output_root=output_root)[0]


async def _download_span_range_async(
    *,
    start_date: date,
    end_date: date,
    output_root: Path,
    max_concurrent: int,
) -> list[SpanDownloadDayResult]:
    try:
        from curl_cffi.requests import AsyncSession  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - dependency is present in the desktop runtime.
        raise RuntimeError("curl_cffi is required for NSE SPAN downloads") from exc

    output_root.mkdir(parents=True, exist_ok=True)
    results: list[SpanDownloadDayResult] = []
    lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, int(max_concurrent)))
    async with AsyncSession(impersonate="chrome124", headers=HEADERS) as session:
        await session.get(HOME_URL, timeout=20)
        tasks = [
            _download_one(session, day, output_root, semaphore, results, lock)
            for day in _iter_dates(start_date, end_date)
        ]
        await asyncio.gather(*tasks)
    return sorted(results, key=lambda item: item.trading_date)


async def _download_one(session: object, day: date, output_root: Path, semaphore: asyncio.Semaphore, results: list[SpanDownloadDayResult], lock: asyncio.Lock) -> None:
    dest = output_root / f"{day.year}" / f"{day.month:02d}" / f"{day.day:02d}"
    if len(tuple(dest.glob("*.zip"))) >= 6:
        result = SpanDownloadDayResult(day.isoformat(), "skipped_existing", 6, str(dest))
    else:
        async with semaphore:
            result = await _fetch_day(session, day, dest)
    async with lock:
        results.append(result)


async def _fetch_day(session: object, day: date, dest: Path) -> SpanDownloadDayResult:
    date_str = day.strftime("%d-%b-%Y")
    try:
        resp = await session.get(
            API_URL,
            params={"archives": ARCHIVES_PAYLOAD, "date": date_str, "type": "Archives"},
            timeout=180,
        )
        status_code = int(getattr(resp, "status_code", 0) or 0)
        content = bytes(getattr(resp, "content", b"") or b"")
        if status_code == 200 and len(content) > 512:
            dest.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                archive.extractall(dest)
                count = len(archive.namelist())
            return SpanDownloadDayResult(day.isoformat(), "downloaded", count, str(dest), status_code)
        if status_code == 404 or len(content) <= 512:
            return SpanDownloadDayResult(day.isoformat(), "not_found_or_non_trading_day", 0, str(dest), status_code)
        return SpanDownloadDayResult(day.isoformat(), "http_error", 0, str(dest), status_code, f"http {status_code}")
    except Exception as exc:  # noqa: BLE001 - downloader reports per-day failures.
        return SpanDownloadDayResult(day.isoformat(), "error", 0, str(dest), None, f"{type(exc).__name__}: {exc}")


def _iter_dates(start: date, end: date) -> tuple[date, ...]:
    if start > end:
        raise ValueError(f"start date {start} must be <= end date {end}")
    out = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return tuple(out)
