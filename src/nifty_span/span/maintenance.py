from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable
import json
import time

from .contracts import SLOT_ORDER
from .downloader import SpanDownloadDayResult, download_span_day
from .extractor import DEFAULT_SYMBOLS_FILTER, SpanExtractionReport, extract_span_archives
from .parquet import span_day_status


DownloadSpanFn = Callable[..., SpanDownloadDayResult]
ExtractSpanFn = Callable[..., SpanExtractionReport]


@dataclass(frozen=True)
class SpanMaintenanceReport:
    generated_at: str
    trading_date: str
    ok: bool
    changed: bool
    raw_root: str
    parquet_dir: str
    requested_time_slot: str
    selected_time_slot: str | None
    parsed_slots: tuple[str, ...]
    raw_zip_count: int
    raw_zip_names: tuple[str, ...]
    download: dict[str, Any]
    extraction: dict[str, Any] | None
    status: dict[str, Any]
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(asdict(self))


def run_span_maintenance_once(
    *,
    trading_date: date,
    raw_root: str | Path = Path("data/span/raw"),
    parquet_dir: str | Path = Path("data/span/parquet"),
    preferred_time_slot: str = "LATEST",
    symbols_filter: tuple[str, ...] = DEFAULT_SYMBOLS_FILTER,
    max_workers: int = 4,
    download_fn: DownloadSpanFn = download_span_day,
    extract_fn: ExtractSpanFn = extract_span_archives,
) -> SpanMaintenanceReport:
    raw_root_path = Path(raw_root)
    parquet_dir_path = Path(parquet_dir)
    before_raw = _raw_zip_names(raw_root_path, trading_date)
    before_slots = available_span_slots(parquet_dir_path, trading_date)
    generated_at = datetime.now(timezone.utc).isoformat()
    extraction: SpanExtractionReport | None = None
    error: str | None = None
    try:
        download = download_fn(trading_date=trading_date, output_root=raw_root_path)
        if download.status in {"downloaded", "skipped_existing", "not_found_or_non_trading_day"}:
            extraction = extract_fn(
                span_data_dir=raw_root_path,
                parquet_dir=parquet_dir_path,
                symbols_filter=tuple(str(symbol).upper().strip() for symbol in symbols_filter),
                trading_date=trading_date,
                max_workers=max_workers,
            )
    except Exception as exc:  # noqa: BLE001 - maintainer reports and keeps the loop alive.
        download = SpanDownloadDayResult(
            trading_date=trading_date.isoformat(),
            status="error",
            extracted_files=0,
            output_dir=str(_raw_day_dir(raw_root_path, trading_date)),
            error=f"{type(exc).__name__}: {exc}",
        )
        error = download.error

    status = span_day_status(
        trading_date=trading_date,
        parquet_dir=parquet_dir_path,
        raw_root=raw_root_path,
        preferred_time_slot=preferred_time_slot,
    )
    raw_names = _raw_zip_names(raw_root_path, trading_date)
    parsed_slots = available_span_slots(parquet_dir_path, trading_date)
    extraction_failed = extraction is not None and extraction.failed_zip_count > 0
    download_failed = download.status not in {"downloaded", "skipped_existing", "not_found_or_non_trading_day"}
    ok = bool(status.ready and not download_failed and not extraction_failed)
    changed = raw_names != before_raw or parsed_slots != before_slots
    return SpanMaintenanceReport(
        generated_at=generated_at,
        trading_date=trading_date.isoformat(),
        ok=ok,
        changed=changed,
        raw_root=str(raw_root_path),
        parquet_dir=str(parquet_dir_path),
        requested_time_slot=preferred_time_slot,
        selected_time_slot=status.selected_time_slot,
        parsed_slots=parsed_slots,
        raw_zip_count=len(raw_names),
        raw_zip_names=raw_names,
        download=asdict(download),
        extraction=None if extraction is None else asdict(extraction),
        status=status.to_dict(),
        error=error,
    )


def run_span_maintenance_loop(
    *,
    trading_date: date,
    raw_root: str | Path = Path("data/span/raw"),
    parquet_dir: str | Path = Path("data/span/parquet"),
    preferred_time_slot: str = "LATEST",
    symbols_filter: tuple[str, ...] = DEFAULT_SYMBOLS_FILTER,
    max_workers: int = 4,
    interval_seconds: float = 300.0,
    iterations: int = 0,
    report_out: str | Path = Path("reports/span_maintenance_latest.json"),
    emit_json: bool = False,
) -> SpanMaintenanceReport:
    last_report: SpanMaintenanceReport | None = None
    count = 0
    while True:
        last_report = run_span_maintenance_once(
            trading_date=trading_date,
            raw_root=raw_root,
            parquet_dir=parquet_dir,
            preferred_time_slot=preferred_time_slot,
            symbols_filter=symbols_filter,
            max_workers=max_workers,
        )
        write_span_maintenance_report(last_report, report_out)
        if emit_json:
            print(json.dumps(last_report.to_dict(), sort_keys=True), flush=True)
        else:
            print(
                "span_maintenance "
                f"ok={last_report.ok} changed={last_report.changed} "
                f"selected={last_report.selected_time_slot or '-'} raw_zips={last_report.raw_zip_count}",
                flush=True,
            )
        count += 1
        if iterations > 0 and count >= iterations:
            return last_report
        time.sleep(max(1.0, float(interval_seconds)))


def write_span_maintenance_report(report: SpanMaintenanceReport, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(target)
    return target


def available_span_slots(parquet_dir: str | Path, trading_date: date) -> tuple[str, ...]:
    month_path = Path(parquet_dir) / f"{trading_date:%Y_%m}.parquet"
    if not month_path.exists():
        return ()
    try:
        import pyarrow.parquet as pq  # type: ignore[import-not-found]

        table = pq.read_table(month_path, columns=["date", "time_slot"], filters=[("date", "=", trading_date)])
        values = {str(item) for item in table.column("time_slot").to_pylist() if item not in (None, "")}
    except Exception:
        return ()
    order = {slot: idx for idx, slot in enumerate(SLOT_ORDER)}
    return tuple(sorted(values, key=lambda slot: (order.get(slot, 999), slot)))


def _raw_zip_names(raw_root: Path, trading_date: date) -> tuple[str, ...]:
    day_dir = _raw_day_dir(raw_root, trading_date)
    if not day_dir.exists():
        return ()
    return tuple(sorted(path.name for path in day_dir.glob("*.zip")))


def _raw_day_dir(raw_root: Path, trading_date: date) -> Path:
    return raw_root / f"{trading_date:%Y}" / f"{trading_date:%m}" / f"{trading_date:%d}"


def _jsonable(value: Any) -> Any:
    if hasattr(value, "value"):
        return value.value
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
