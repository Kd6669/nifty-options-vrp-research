from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from .contracts import RISK_COLUMNS, SpanContract, SpanData, SpanReadiness, normalize_slot_request, slot_fallback_order


class SpanParquetReader:
    @staticmethod
    def load(
        parquet_dir: str | Path,
        trading_date: date,
        *,
        time_slot: str = "BOD",
    ) -> SpanData:
        root = Path(parquet_dir)
        month_path = span_month_file(root, trading_date)
        if not month_path.exists():
            raise FileNotFoundError(f"SPAN parquet file missing for {trading_date}: {month_path}")

        try:
            import pyarrow.parquet as pq  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - dependency is present in the desktop runtime.
            raise RuntimeError("pyarrow is required to read SPAN parquet files") from exc

        table = pq.read_table(month_path, filters=[("date", "=", trading_date)])
        if table.num_rows == 0:
            raise FileNotFoundError(f"SPAN parquet has no rows for trading_date={trading_date}: {month_path}")

        columns = table.to_pydict()
        requested_slot = normalize_slot_request(time_slot)
        selected_slot = _select_slot(columns.get("time_slot", ()), requested_slot)
        if selected_slot is None:
            raise FileNotFoundError(
                f"SPAN parquet has no matching rows for trading_date={trading_date} "
                f"requested_slot={requested_slot!r}: {month_path}"
            )

        contracts: dict[tuple[str, str, date, float], SpanContract] = {}
        slots = tuple(columns.get("time_slot", ()) or ())
        for idx, raw_slot in enumerate(slots):
            if str(raw_slot or "").upper().strip() != selected_slot:
                continue
            symbol = _text_at(columns, "symbol", idx).upper()
            instrument = _text_at(columns, "instrument", idx).upper()
            expiry_value = _value_at(columns, "expiry", idx)
            if not symbol or instrument not in {"FUT", "CE", "PE"} or expiry_value is None:
                continue
            expiry_date = _coerce_date(expiry_value)
            strike = 0.0 if instrument == "FUT" else _normalize_strike(_value_at(columns, "strike", idx, 0.0))
            risk_array = tuple(float(_value_at(columns, col, idx, 0.0) or 0.0) for col in RISK_COLUMNS)
            contracts[(symbol, instrument, expiry_date, strike)] = SpanContract(
                risk_array=risk_array,
                price_scan_range=float(_value_at(columns, "price_scan_range", idx, 0.0) or 0.0),
                vol_scan_range=float(_value_at(columns, "vol_scan_range", idx, 0.0) or 0.0),
                price=float(_value_at(columns, "price", idx, 0.0) or 0.0),
                delta=float(_value_at(columns, "delta", idx, 0.0) or 0.0),
                implied_vol=float(_value_at(columns, "implied_vol", idx, 0.0) or 0.0),
                cvf=float(_value_at(columns, "cvf", idx, 1.0) or 1.0),
                composite_delta=float(_value_at(columns, "composite_delta", idx, 0.0) or 0.0),
            )

        return SpanData(
            contracts,
            selected_time_slot=selected_slot,
            trading_date=trading_date,
            source_path=str(month_path),
        )


def span_day_status(
    *,
    trading_date: date,
    parquet_dir: str | Path,
    preferred_time_slot: str = "BOD",
    raw_root: str | Path | None = None,
) -> SpanReadiness:
    parquet_root = Path(parquet_dir)
    month_file = span_month_file(parquet_root, trading_date)
    raw_day_dir = None if raw_root is None else span_day_dir(Path(raw_root), trading_date)
    zip_count = None
    raw_exists = None
    if raw_day_dir is not None:
        raw_exists = raw_day_dir.exists()
        zip_count = len(tuple(raw_day_dir.glob("*.zip"))) if raw_exists else 0
    try:
        data = SpanParquetReader.load(
            parquet_root,
            trading_date,
            time_slot=preferred_time_slot,
        )
        return SpanReadiness(
            date=trading_date.isoformat(),
        requested_time_slot=normalize_slot_request(preferred_time_slot),
            selected_time_slot=data.selected_time_slot,
            loaded=True,
            row_count=len(data),
            parquet_dir=str(parquet_root),
            month_file=str(month_file),
            month_file_exists=month_file.exists(),
            raw_day_dir=None if raw_day_dir is None else str(raw_day_dir),
            raw_day_dir_exists=raw_exists,
            zip_count=zip_count,
        )
    except Exception as exc:  # noqa: BLE001 - readiness should report, not explode.
        return SpanReadiness(
            date=trading_date.isoformat(),
            requested_time_slot=normalize_slot_request(preferred_time_slot),
            selected_time_slot="",
            loaded=False,
            row_count=0,
            parquet_dir=str(parquet_root),
            month_file=str(month_file),
            month_file_exists=month_file.exists(),
            raw_day_dir=None if raw_day_dir is None else str(raw_day_dir),
            raw_day_dir_exists=raw_exists,
            zip_count=zip_count,
            error=f"{type(exc).__name__}: {exc}",
        )


def span_month_file(parquet_dir: Path, trading_date: date) -> Path:
    return parquet_dir / f"{trading_date.year}_{trading_date.month:02d}.parquet"


def span_day_dir(raw_root: Path, trading_date: date) -> Path:
    return raw_root / f"{trading_date.year}" / f"{trading_date.month:02d}" / f"{trading_date.day:02d}"


def _select_slot(raw_slots: Any, preferred: str) -> str | None:
    available = {str(slot or "").upper().strip() for slot in raw_slots if str(slot or "").strip()}
    return next((slot for slot in slot_fallback_order(preferred) if slot in available), None)


def _value_at(columns: dict[str, list[Any]], name: str, idx: int, default: Any = None) -> Any:
    values = columns.get(name)
    if values is None or idx >= len(values):
        return default
    return values[idx]


def _text_at(columns: dict[str, list[Any]], name: str, idx: int) -> str:
    return str(_value_at(columns, name, idx, "") or "").strip()


def _normalize_strike(value: Any) -> float:
    return round(float(value or 0.0), 2)


def _coerce_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    maybe_py = getattr(value, "as_py", None)
    if callable(maybe_py):
        return _coerce_date(maybe_py())
    text = str(value or "").strip()
    compact = text.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
    return date.fromisoformat(text)
