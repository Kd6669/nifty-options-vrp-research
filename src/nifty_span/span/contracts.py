from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


RISK_COLUMNS = tuple(f"s{i}" for i in range(1, 17))
SLOT_ORDER = ("BOD", "ID1", "ID2", "ID3", "ID4", "EOD")
LATEST_SLOT_REQUESTS = {"LATEST", "LATEST_AVAILABLE", "AUTO", "INTRADAY_LATEST"}
SLOT_ALIASES = {
    "S1": "BOD",
    "I1": "BOD",
    "1": "BOD",
    "S2": "ID1",
    "I2": "ID1",
    "2": "ID1",
    "S3": "ID2",
    "I3": "ID2",
    "3": "ID2",
    "S4": "ID3",
    "I4": "ID3",
    "4": "ID3",
    "S5": "ID4",
    "I5": "ID4",
    "5": "ID4",
}


@dataclass(frozen=True)
class SpanContract:
    risk_array: tuple[float, ...]
    price_scan_range: float = 0.0
    vol_scan_range: float = 0.0
    price: float = 0.0
    delta: float = 0.0
    implied_vol: float = 0.0
    cvf: float = 1.0
    composite_delta: float = 0.0


class SpanData:
    """In-memory lookup table for one trading date and one selected SPAN slot."""

    def __init__(
        self,
        contracts: dict[tuple[str, str, date, float], SpanContract] | None = None,
        *,
        selected_time_slot: str = "",
        trading_date: date | None = None,
        source_path: str | None = None,
    ) -> None:
        self._contracts = dict(contracts or {})
        self.selected_time_slot = str(selected_time_slot or "").upper().strip()
        self.trading_date = trading_date
        self.source_path = source_path

    def __len__(self) -> int:
        return len(self._contracts)

    @property
    def contracts(self) -> dict[tuple[str, str, date, float], SpanContract]:
        return dict(self._contracts)

    def lookup_option(
        self,
        symbol: str,
        opt_type: str,
        expiry: str | date,
        strike: float,
    ) -> SpanContract | None:
        instrument = _normalize_option_type(opt_type)
        if instrument is None:
            return None
        expiry_date = _coerce_date(expiry)
        if expiry_date is None:
            return None
        return self._contracts.get(
            (
                str(symbol or "").upper().strip(),
                instrument,
                expiry_date,
                _normalize_strike(strike),
            )
        )

    def lookup_future(self, symbol: str, expiry: str | date) -> SpanContract | None:
        expiry_date = _coerce_date(expiry)
        if expiry_date is None:
            return None
        return self._contracts.get(
            (
                str(symbol or "").upper().strip(),
                "FUT",
                expiry_date,
                0.0,
            )
        )


@dataclass(frozen=True)
class SpanReadiness:
    date: str
    requested_time_slot: str
    selected_time_slot: str
    loaded: bool
    row_count: int
    parquet_dir: str
    month_file: str
    month_file_exists: bool
    raw_day_dir: str | None = None
    raw_day_dir_exists: bool | None = None
    zip_count: int | None = None
    error: str | None = None

    @property
    def ready(self) -> bool:
        return self.loaded and self.row_count > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "requested_time_slot": self.requested_time_slot,
            "selected_time_slot": self.selected_time_slot,
            "loaded": self.loaded,
            "row_count": self.row_count,
            "parquet_dir": self.parquet_dir,
            "month_file": self.month_file,
            "month_file_exists": self.month_file_exists,
            "raw_day_dir": self.raw_day_dir,
            "raw_day_dir_exists": self.raw_day_dir_exists,
            "zip_count": self.zip_count,
            "error": self.error,
            "ready": self.ready,
        }


@dataclass(frozen=True)
class SpanMarginBreakdown:
    margin: float
    source: str
    scan_scenarios: tuple[float, ...]
    m_span: float
    credit_sum: float
    long_premium: float
    long_option_value: float
    net_option_value: float
    s_net_raw: float
    s_net_clamped: float
    elm_required: float
    elm_plus_long_prem: float
    add_on_margin: float
    delivery_margin: float
    crystallized_obligation_margin: float
    cross_margin_benefit: float
    minimum_total_margin_floor: float
    span_time_slot: str
    span_trading_date: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "margin": self.margin,
            "source": self.source,
            "scan_scenarios": list(self.scan_scenarios),
            "m_span": self.m_span,
            "credit_sum": self.credit_sum,
            "long_premium": self.long_premium,
            "long_option_value": self.long_option_value,
            "net_option_value": self.net_option_value,
            "s_net_raw": self.s_net_raw,
            "s_net_clamped": self.s_net_clamped,
            "elm_required": self.elm_required,
            "elm_plus_long_prem": self.elm_plus_long_prem,
            "add_on_margin": self.add_on_margin,
            "delivery_margin": self.delivery_margin,
            "crystallized_obligation_margin": self.crystallized_obligation_margin,
            "cross_margin_benefit": self.cross_margin_benefit,
            "minimum_total_margin_floor": self.minimum_total_margin_floor,
            "span_time_slot": self.span_time_slot,
            "span_trading_date": self.span_trading_date,
        }


def slot_fallback_order(preferred: str) -> tuple[str, ...]:
    requested = normalize_slot_request(preferred)
    if requested in LATEST_SLOT_REQUESTS:
        return tuple(reversed(SLOT_ORDER))
    return (requested,)


def normalize_slot_request(preferred: str) -> str:
    requested = str(preferred or "LATEST").upper().strip()
    return SLOT_ALIASES.get(requested, requested)


def _normalize_strike(value: float) -> float:
    return round(float(value or 0.0), 2)


def _normalize_option_type(value: str) -> str | None:
    normalized = str(value or "").upper().strip()
    return {"C": "CE", "CE": "CE", "CALL": "CE", "P": "PE", "PE": "PE", "PUT": "PE"}.get(normalized)


def _coerce_date(value: str | date | Any) -> date | None:
    if isinstance(value, date):
        return value
    maybe_py = getattr(value, "as_py", None)
    if callable(maybe_py):
        return _coerce_date(maybe_py())
    text = str(value or "").strip()
    if not text:
        return None
    compact = text.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return date(int(compact[:4]), int(compact[4:6]), int(compact[6:8]))
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None
