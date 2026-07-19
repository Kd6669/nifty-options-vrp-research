from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import date
import errno
import os
from pathlib import Path
import time
from typing import Any, Iterator, Sequence

from .backfill_audit import SpanBackfillAuditReport, audit_span_backfill
from .backfill_downloader import BackfillConfig, SpanBackfillReport, download_span_backfill
from .streaming_extractor import (
    SpanMonthCompactionReport,
    SpanStreamingExtractionReport,
    compact_span_month,
    extract_manifest_archives,
)


EXTRACTION_LOCK_FILENAME = ".span-extract-compact.lock"
EXTRACTION_LOCK_TIMEOUT_SECONDS = 21_600.0
EXTRACTION_LOCK_POLL_SECONDS = 0.25


class SpanExtractionLockTimeout(RuntimeError):
    """Another process retained the extraction/compaction transaction lock."""


@contextmanager
def extraction_compaction_lock(
    extraction_manifest: str | Path,
    *,
    timeout_seconds: float = EXTRACTION_LOCK_TIMEOUT_SECONDS,
    poll_seconds: float = EXTRACTION_LOCK_POLL_SECONDS,
) -> Iterator[Path]:
    """Hold one OS-released lock for a manifest's full extract/compact transaction."""
    if timeout_seconds <= 0:
        raise ValueError("lock timeout_seconds must be > 0")
    if poll_seconds <= 0:
        raise ValueError("lock poll_seconds must be > 0")
    lock_path = Path(extraction_manifest).resolve().parent / EXTRACTION_LOCK_FILENAME
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b", buffering=0)
    acquired = False
    try:
        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b"\0")
            handle.flush()
            os.fsync(handle.fileno())
        deadline = time.monotonic() + timeout_seconds
        while True:
            try:
                _lock_file_nonblocking(handle)
                acquired = True
                break
            except OSError as exc:
                if not _is_lock_contention(exc):
                    raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SpanExtractionLockTimeout(
                    f"timed out after {timeout_seconds:.3f}s waiting for {lock_path}"
                )
            time.sleep(min(poll_seconds, remaining))
        yield lock_path
    finally:
        if acquired:
            _unlock_file(handle)
        handle.close()


def _lock_file_nonblocking(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock_file(handle: Any) -> None:
    handle.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _is_lock_contention(exc: OSError) -> bool:
    return isinstance(exc, BlockingIOError) or exc.errno in {
        errno.EACCES,
        errno.EAGAIN,
        errno.EDEADLK,
    } or getattr(exc, "winerror", None) in {33, 36}


@dataclass(frozen=True)
class SpanExtractRangeReport:
    start_date: str
    end_date: str
    extraction: SpanStreamingExtractionReport
    compacted_months: tuple[SpanMonthCompactionReport, ...]

    @property
    def ok(self) -> bool:
        return self.extraction.ok

    def to_dict(self) -> dict[str, Any]:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "ok": self.ok,
            "extraction": asdict(self.extraction),
            "compacted_months": [asdict(month) for month in self.compacted_months],
        }


@dataclass(frozen=True)
class SpanBackfillPipelineReport:
    download: SpanBackfillReport
    extract: SpanExtractRangeReport
    audit: SpanBackfillAuditReport

    @property
    def ok(self) -> bool:
        return self.download.failed_slots == 0 and self.extract.ok and self.audit.ok

    def to_dict(self, *, include_cells: bool = False) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "download": self.download.to_dict(),
            "extract": self.extract.to_dict(),
            "audit": self.audit.to_dict(include_cells=include_cells),
        }


def extract_and_compact_span_range(
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    fragment_root: str | Path,
    extraction_manifest: str | Path,
    compacted_root: str | Path,
    quarantine_root: str | Path,
    symbols: Sequence[str] = ("NIFTY",),
    batch_rows: int = 50_000,
    parse_workers: int = 4,
    lock_timeout_seconds: float = EXTRACTION_LOCK_TIMEOUT_SECONDS,
    lock_poll_seconds: float = EXTRACTION_LOCK_POLL_SECONDS,
) -> SpanExtractRangeReport:
    if start_date > end_date:
        raise ValueError(f"start date {start_date} must be <= end date {end_date}")
    with extraction_compaction_lock(
        extraction_manifest,
        timeout_seconds=lock_timeout_seconds,
        poll_seconds=lock_poll_seconds,
    ):
        extraction = extract_manifest_archives(
            download_manifest=download_manifest,
            raw_root=raw_root,
            fragment_root=fragment_root,
            extraction_manifest=extraction_manifest,
            symbols_filter=tuple(symbols),
            batch_rows=batch_rows,
            start_date=start_date,
            end_date=end_date,
            max_workers=parse_workers,
        )
        compactions: list[SpanMonthCompactionReport] = []
        fragment_path = Path(fragment_root)
        for year, month in _iter_months(start_date, end_date):
            month_dir = fragment_path / f"{year:04d}" / f"{month:02d}"
            if not month_dir.is_dir() or not any(month_dir.glob("*/*.parquet")):
                continue
            compactions.append(
                compact_span_month(
                    fragment_root=fragment_root,
                    parquet_root=compacted_root,
                    quarantine_root=quarantine_root,
                    year=year,
                    month=month,
                )
            )
        return SpanExtractRangeReport(
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            extraction=extraction,
            compacted_months=tuple(compactions),
        )


def run_span_backfill_pipeline(
    *,
    start_date: date,
    end_date: date,
    raw_root: str | Path,
    download_manifest: str | Path,
    fragment_root: str | Path,
    extraction_manifest: str | Path,
    compacted_root: str | Path,
    quarantine_root: str | Path,
    report_root: str | Path,
    availability_manifest: str | Path | None = None,
    symbols: Sequence[str] = ("NIFTY",),
    batch_rows: int = 50_000,
    parse_workers: int = 4,
    download_config: BackfillConfig | None = None,
) -> SpanBackfillPipelineReport:
    download = download_span_backfill(
        start_date=start_date,
        end_date=end_date,
        output_root=raw_root,
        manifest_path=download_manifest,
        config=download_config,
    )
    extraction = extract_and_compact_span_range(
        start_date=start_date,
        end_date=end_date,
        raw_root=raw_root,
        download_manifest=download_manifest,
        fragment_root=fragment_root,
        extraction_manifest=extraction_manifest,
        compacted_root=compacted_root,
        quarantine_root=quarantine_root,
        symbols=symbols,
        batch_rows=batch_rows,
        parse_workers=parse_workers,
    )
    audit = audit_span_backfill(
        start_date=start_date,
        end_date=end_date,
        raw_root=raw_root,
        download_manifest=download_manifest,
        extraction_manifest=extraction_manifest,
        fragment_root=fragment_root,
        compacted_root=compacted_root,
        report_root=report_root,
        availability_manifest=availability_manifest,
    )
    return SpanBackfillPipelineReport(download, extraction, audit)


def _iter_months(start: date, end: date) -> tuple[tuple[int, int], ...]:
    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return tuple(months)
