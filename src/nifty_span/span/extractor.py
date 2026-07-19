from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
import gc
import xml.etree.ElementTree as ET
import zipfile


DEFAULT_SYMBOLS_FILTER = ("NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY")
SUFFIX_TO_SLOT = {
    "i1": "BOD",
    "i2": "ID1",
    "i3": "ID2",
    "i4": "ID3",
    "i5": "ID4",
    "s": "EOD",
}
FLUSH_ROWS = 200_000


@dataclass(frozen=True)
class SpanExtractionReport:
    raw_zip_count: int
    processed_zip_count: int
    failed_zip_count: int
    row_count: int
    parquet_dir: str
    symbols: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return self.failed_zip_count == 0 and self.processed_zip_count > 0


def parse_span_zip(zip_path: str | Path, symbols_filter: tuple[str, ...] | None = None) -> list[dict[str, Any]]:
    path = Path(zip_path)
    allowed_symbols = {str(sym).upper().strip() for sym in (symbols_filter or DEFAULT_SYMBOLS_FILTER)}
    parts = path.stem.split(".")
    if len(parts) < 3:
        return []
    trading_date = _parse_yyyymmdd(parts[1])
    slot = SUFFIX_TO_SLOT.get(parts[2], parts[2]).upper()
    if trading_date is None:
        return []

    rows: list[dict[str, Any]] = []
    try:
        with zipfile.ZipFile(path) as zf:
            spn_names = [name for name in zf.namelist() if name.endswith(".spn")]
            if not spn_names:
                return []
            with zf.open(spn_names[0]) as handle:
                raw = handle.read()
        root = ET.fromstring(raw)

        for fut_pf in root.iter("futPf"):
            symbol = (fut_pf.findtext("pfCode") or "").strip().upper()
            if symbol not in allowed_symbols:
                continue
            pf_cvf = _float(fut_pf.findtext("cvf"), 1.0)
            for fut in fut_pf.findall("fut"):
                expiry = _parse_yyyymmdd(fut.findtext("pe"))
                if expiry is None:
                    continue
                scan = fut.find("scanRate")
                risk_array, composite_delta = _extract_risk_array(fut.find("ra"))
                rows.append(
                    _row(
                        trading_date=trading_date,
                        slot=slot,
                        symbol=symbol,
                        instrument="FUT",
                        expiry=expiry,
                        strike=0.0,
                        price=_float(fut.findtext("p")),
                        delta=_float(fut.findtext("d")),
                        implied_vol=0.0,
                        price_scan_range=_float(scan.findtext("priceScan")) if scan is not None else 0.0,
                        vol_scan_range=_float(scan.findtext("volScan")) if scan is not None else 0.0,
                        cvf=_float(fut.findtext("cvf"), pf_cvf),
                        risk_array=risk_array,
                        composite_delta=composite_delta,
                    )
                )

        for oop_pf in root.iter("oopPf"):
            symbol = (oop_pf.findtext("pfCode") or "").strip().upper()
            if symbol not in allowed_symbols:
                continue
            pf_cvf = _float(oop_pf.findtext("cvf"), 1.0)
            for series in oop_pf.findall("series"):
                expiry = _parse_yyyymmdd(series.findtext("pe"))
                if expiry is None:
                    continue
                scan = series.find("scanRate")
                price_scan = _float(scan.findtext("priceScan")) if scan is not None else 0.0
                vol_scan = _float(scan.findtext("volScan")) if scan is not None else 0.0
                series_cvf = _float(series.findtext("cvf"), pf_cvf)
                for opt in series.findall("opt"):
                    raw_option = opt.findtext("o") or ""
                    instrument = "CE" if raw_option.upper().startswith("C") else "PE"
                    risk_array, composite_delta = _extract_risk_array(opt.find("ra"))
                    rows.append(
                        _row(
                            trading_date=trading_date,
                            slot=slot,
                            symbol=symbol,
                            instrument=instrument,
                            expiry=expiry,
                            strike=_float(opt.findtext("k")),
                            price=_float(opt.findtext("p")),
                            delta=_float(opt.findtext("d")),
                            implied_vol=_float(opt.findtext("v")),
                            price_scan_range=price_scan,
                            vol_scan_range=vol_scan,
                            cvf=series_cvf,
                            risk_array=risk_array,
                            composite_delta=composite_delta,
                        )
                    )
    finally:
        try:
            del root
        except NameError:
            pass
        gc.collect()
    return rows


def extract_span_archives(
    *,
    span_data_dir: str | Path,
    parquet_dir: str | Path,
    symbols_filter: tuple[str, ...] = DEFAULT_SYMBOLS_FILTER,
    trading_date: date | None = None,
    max_workers: int = 4,
) -> SpanExtractionReport:
    try:
        import pandas as pd  # type: ignore[import-not-found]
        import pyarrow as pa  # type: ignore[import-not-found]
        import pyarrow.parquet as pq  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - dependency is present in the desktop runtime.
        raise RuntimeError("pandas and pyarrow are required to extract SPAN parquet files") from exc

    raw_root = Path(span_data_dir)
    out_root = Path(parquet_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    zip_paths = sorted(raw_root.rglob("*.zip"))
    if trading_date is not None:
        tag = trading_date.strftime("%Y%m%d")
        zip_paths = [path for path in zip_paths if tag in path.stem]

    buffers: dict[str, list[dict[str, Any]]] = defaultdict(list)
    processed = 0
    failed = 0
    row_count = 0
    with ProcessPoolExecutor(max_workers=max(1, int(max_workers))) as pool:
        futures = {pool.submit(parse_span_zip, str(path), symbols_filter): path for path in zip_paths}
        for future in as_completed(futures):
            try:
                rows = future.result()
                processed += 1
                row_count += len(rows)
                for row in rows:
                    key = row["date"].strftime("%Y_%m")
                    buffers[key].append(row)
            except Exception:
                failed += 1

    schema = _schema(pa)
    for key, rows in buffers.items():
        if not rows:
            continue
        frame = pd.DataFrame(rows)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["expiry"] = pd.to_datetime(frame["expiry"]).dt.date
        frame = frame.sort_values(["date", "time_slot", "symbol", "expiry", "strike"], ignore_index=True)
        table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False, safe=False)
        path = out_root / f"{key}.parquet"
        if path.exists():
            existing = pq.read_table(path, schema=schema)
            table = pa.concat_tables([existing, table])
            data = table.to_pandas()
            data = data.drop_duplicates(
                subset=["date", "time_slot", "symbol", "instrument", "expiry", "strike"],
                keep="last",
            ).sort_values(["date", "time_slot", "symbol", "expiry", "strike"], ignore_index=True)
            table = pa.Table.from_pandas(data, schema=schema, preserve_index=False, safe=False)
        pq.write_table(table, path, compression="snappy", use_dictionary=["time_slot", "symbol", "instrument"])

    return SpanExtractionReport(
        raw_zip_count=len(zip_paths),
        processed_zip_count=processed,
        failed_zip_count=failed,
        row_count=row_count,
        parquet_dir=str(out_root),
        symbols=tuple(symbols_filter),
    )


def _schema(pa: Any) -> Any:
    return pa.schema(
        [
            pa.field("date", pa.date32()),
            pa.field("time_slot", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("instrument", pa.string()),
            pa.field("expiry", pa.date32()),
            pa.field("strike", pa.float64()),
            pa.field("price", pa.float64()),
            pa.field("delta", pa.float64()),
            pa.field("implied_vol", pa.float64()),
            pa.field("price_scan_range", pa.float64()),
            pa.field("vol_scan_range", pa.float64()),
            pa.field("cvf", pa.float64()),
            *(pa.field(f"s{i}", pa.float64()) for i in range(1, 17)),
            pa.field("composite_delta", pa.float64()),
        ]
    )


def _row(
    *,
    trading_date: date,
    slot: str,
    symbol: str,
    instrument: str,
    expiry: date,
    strike: float,
    price: float,
    delta: float,
    implied_vol: float,
    price_scan_range: float,
    vol_scan_range: float,
    cvf: float,
    risk_array: tuple[float, ...],
    composite_delta: float,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "date": trading_date,
        "time_slot": slot,
        "symbol": symbol,
        "instrument": instrument,
        "expiry": expiry,
        "strike": strike,
        "price": price,
        "delta": delta,
        "implied_vol": implied_vol,
        "price_scan_range": price_scan_range,
        "vol_scan_range": vol_scan_range,
        "cvf": cvf,
        "composite_delta": composite_delta,
    }
    payload.update({f"s{i + 1}": risk_array[i] for i in range(16)})
    return payload


def _parse_yyyymmdd(value: str | None) -> date | None:
    text = str(value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def _float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(value) if value not in (None, "") else float(default)
    except (TypeError, ValueError):
        return float(default)


def _extract_risk_array(ra_elem: Any) -> tuple[tuple[float, ...], float]:
    if ra_elem is None:
        return tuple(0.0 for _ in range(16)), 0.0
    values = [_float(item.text) for item in ra_elem.findall("a")]
    values.extend(0.0 for _ in range(max(0, 16 - len(values))))
    return tuple(values[:16]), _float(ra_elem.findtext("d"))
