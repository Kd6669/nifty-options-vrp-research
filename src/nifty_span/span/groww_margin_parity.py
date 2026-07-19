from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import json
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from nifty_span.broker_accounting import GrowwFnoChargeLeg, estimate_groww_margin_api_charges
from nifty_span.oms.prices import round_broker_limit_price
from nifty_span.span.margin_model_a import SpanMarginError, margin_for_candidate_legs
from nifty_span.span.parquet import SpanParquetReader


INDEX_SYMBOLS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "SENSEX", "BANKEX"})
INDEX_ELM_BASE = 0.02
STOCK_ELM_BASE = 0.035
EXPIRY_DAY_EXTRA = 0.02
INDEX_DEEP_OTM_RATE = 0.03
INDEX_DEEP_OTM_THRESH = 0.10
STOCK_DEEP_OTM_RATE = 0.0525
STOCK_DEEP_OTM_THRESH = 0.30


DEFAULT_SPAN_PARQUET_DIR = Path(os.environ.get("NIFTY_SPAN_PARQUET_DIR", "data/span/parquet"))
DEFAULT_CHAIN_ROOT = Path(os.environ.get("NIFTY_CHAIN_ROOT", "data/nifty_weekly_depth_ticks"))


@dataclass(frozen=True)
class ChainSnapshot:
    trading_date: date
    timestamp: str
    snapshot_ts_ms: int | None
    underlying: str
    expiry: str
    spot: float
    rows: tuple[dict[str, Any], ...]
    source_path: str


@dataclass(frozen=True)
class BasketLeg:
    symbol: str
    side: str
    option_type: str
    strike: float
    price: float
    lot_size: int
    qty_ratio: int
    trading_symbol: str
    expiry: str | None = None
    instrument: str = "OPT"
    instrument_token: str = ""

    @property
    def quantity(self) -> int:
        return int(self.lot_size) * int(self.qty_ratio)

    def to_margin_leg(self) -> dict[str, Any]:
        is_future = self.instrument.upper().startswith("FUT") or self.option_type.upper() == "FUT"
        return {
            "side": self.side,
            "option_type": self.option_type,
            "strike": self.strike,
            "lot_size": self.lot_size,
            "qty_ratio": self.qty_ratio,
            "limit_price": self.price,
            "expiry": self.expiry,
            "instrument": self.instrument,
            "is_option": not is_future,
        }

    def to_charge_leg(self, *, broker_rounded_price: bool = False) -> GrowwFnoChargeLeg:
        price = round_broker_limit_price(self.price, self.side) if broker_rounded_price else self.price
        return GrowwFnoChargeLeg(
            side=self.side,
            instrument=self.instrument,
            price=price,
            quantity=self.quantity,
            exchange="NSE",
        )


@dataclass(frozen=True)
class BasketSpec:
    basket_id: str
    family: str
    description: str
    lots: int
    legs: tuple[BasketLeg, ...]


@dataclass(frozen=True)
class LocalMarginComponents:
    total_requirement: float
    span_required: float
    scan_risk_before_nov: float
    short_option_credit: float
    option_buy_premium: float
    exposure_required: float
    brokerage_and_charges: float
    selected_span_slot: str
    span_trading_date: str
    active_scenario: int
    scan_scenarios: tuple[float, ...] = field(default_factory=tuple)
    error: str | None = None


@dataclass(frozen=True)
class GrowwMarginComponents:
    total_requirement: float = math.nan
    span_required: float = math.nan
    exposure_required: float = math.nan
    option_buy_premium: float = math.nan
    brokerage_and_charges: float = math.nan
    physical_delivery_margin_requirement: float = math.nan
    raw: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True)
class MarginParityRow:
    basket_id: str
    family: str
    description: str
    lots: int
    leg_count: int
    leg_symbols: tuple[str, ...]
    exposure_risk_quantity: int
    local: LocalMarginComponents | None
    groww: GrowwMarginComponents | None
    diff_total: float | None
    diff_total_pct: float | None
    verdict: str

    def to_flat_dict(self) -> dict[str, Any]:
        local = self.local or LocalMarginComponents(
            total_requirement=math.nan,
            span_required=math.nan,
            scan_risk_before_nov=math.nan,
            short_option_credit=math.nan,
            option_buy_premium=math.nan,
            exposure_required=math.nan,
            brokerage_and_charges=math.nan,
            selected_span_slot="",
            span_trading_date="",
            active_scenario=0,
            error="local_margin_not_run",
        )
        groww = self.groww or GrowwMarginComponents(error="groww_not_polled")
        return {
            "basket_id": self.basket_id,
            "family": self.family,
            "description": self.description,
            "lots": self.lots,
            "leg_count": self.leg_count,
            "leg_symbols": "|".join(self.leg_symbols),
            "exposure_risk_quantity": self.exposure_risk_quantity,
            "local_total_requirement": local.total_requirement,
            "local_span_required": local.span_required,
            "local_scan_risk_before_nov": local.scan_risk_before_nov,
            "local_short_option_credit": local.short_option_credit,
            "local_option_buy_premium": local.option_buy_premium,
            "local_exposure_required": local.exposure_required,
            "local_exposure_ref_avg": _exposure_reference_avg(self.exposure_risk_quantity, local.exposure_required),
            "local_brokerage_and_charges": local.brokerage_and_charges,
            "local_span_slot": local.selected_span_slot,
            "local_active_scenario": local.active_scenario,
            "local_error": local.error or "",
            "groww_total_requirement": groww.total_requirement,
            "groww_span_required": groww.span_required,
            "groww_exposure_required": groww.exposure_required,
            "groww_option_buy_premium": groww.option_buy_premium,
            "groww_brokerage_and_charges": groww.brokerage_and_charges,
            "groww_exposure_ref_avg": _exposure_reference_avg(self.exposure_risk_quantity, groww.exposure_required),
            "diff_exposure_ref_avg": _exposure_reference_diff(
                self.exposure_risk_quantity,
                local.exposure_required,
                groww.exposure_required,
            ),
            "groww_physical_delivery_margin_requirement": groww.physical_delivery_margin_requirement,
            "groww_error": groww.error or "",
            "diff_total": "" if self.diff_total is None else self.diff_total,
            "diff_total_pct": "" if self.diff_total_pct is None else self.diff_total_pct,
            "verdict": self.verdict,
        }


@dataclass(frozen=True)
class MarginParityRunReport:
    ok: bool
    snapshot: dict[str, Any]
    basket_count: int
    groww_polled: bool
    warn_count: int
    fail_count: int
    output_dir: str
    csv_path: str
    jsonl_path: str
    markdown_path: str
    rows: tuple[MarginParityRow, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["rows"] = [row.to_flat_dict() for row in self.rows]
        return payload


def run_margin_parity_check(
    *,
    chain_parquet: Path,
    span_parquet_dir: Path = DEFAULT_SPAN_PARQUET_DIR,
    output_dir: Path,
    trading_date: date | None = None,
    expiry: str | None = None,
    timestamp: str | None = None,
    underlying: str = "NIFTY",
    span_time_slot: str = "LATEST",
    lots: Sequence[int] = (1, 3),
    max_baskets: int = 0,
    poll_groww: bool = False,
    estimate_groww_charges: bool = False,
    future_trading_symbol: str | None = None,
    future_expiry: str | None = None,
    future_price: float | None = None,
    future_lot_size: int | None = None,
    warn_abs_inr: float = 500.0,
    fail_abs_inr: float = 2000.0,
    warn_pct: float = 0.02,
    fail_pct: float = 0.05,
) -> MarginParityRunReport:
    snapshot = load_chain_snapshot(
        chain_parquet=chain_parquet,
        trading_date=trading_date,
        expiry=expiry,
        timestamp=timestamp,
        underlying=underlying,
    )
    baskets = generate_margin_test_baskets(
        snapshot,
        lots=tuple(lots),
        future_trading_symbol=future_trading_symbol,
        future_expiry=future_expiry,
        future_price=future_price,
        future_lot_size=future_lot_size,
    )
    if max_baskets > 0:
        baskets = baskets[:max_baskets]

    span_data = SpanParquetReader.load(
        span_parquet_dir,
        snapshot.trading_date,
        time_slot=span_time_slot,
    )
    adapter: _GrowwMarginClient | None = None
    if poll_groww:
        adapter = _GrowwMarginClient.from_env()

    rows: list[MarginParityRow] = []
    for basket in baskets:
        local = _local_margin(
            basket,
            snapshot=snapshot,
            span_data=span_data,
            estimate_groww_charges=estimate_groww_charges,
        )
        groww = _poll_groww_margin(adapter, basket) if adapter is not None else None
        rows.append(
            _parity_row(
                basket=basket,
                local=local,
                groww=groww,
                warn_abs_inr=warn_abs_inr,
                fail_abs_inr=fail_abs_inr,
                warn_pct=warn_pct,
                fail_pct=fail_pct,
            )
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "groww_span_margin_parity_summary.csv"
    jsonl_path = output_dir / "groww_span_margin_parity_raw.jsonl"
    markdown_path = output_dir / "GROWW_SPAN_MARGIN_PARITY_REPORT.md"
    _write_csv(csv_path, rows)
    _write_jsonl(jsonl_path, rows)
    _write_markdown(
        markdown_path,
        rows=rows,
        snapshot=snapshot,
        span_parquet_dir=span_parquet_dir,
        span_time_slot=span_time_slot,
        groww_polled=poll_groww,
        warn_abs_inr=warn_abs_inr,
        fail_abs_inr=fail_abs_inr,
        warn_pct=warn_pct,
        fail_pct=fail_pct,
        estimate_groww_charges=estimate_groww_charges,
    )

    fail_count = sum(1 for row in rows if row.verdict == "FAIL")
    warn_count = sum(1 for row in rows if row.verdict == "WARN")
    return MarginParityRunReport(
        ok=fail_count == 0,
        snapshot={
            "trading_date": snapshot.trading_date.isoformat(),
            "timestamp": snapshot.timestamp,
            "snapshot_ts_ms": snapshot.snapshot_ts_ms,
            "underlying": snapshot.underlying,
            "expiry": snapshot.expiry,
            "spot": snapshot.spot,
            "source_path": snapshot.source_path,
            "quote_rows": len(snapshot.rows),
        },
        basket_count=len(rows),
        groww_polled=poll_groww,
        warn_count=warn_count,
        fail_count=fail_count,
        output_dir=str(output_dir),
        csv_path=str(csv_path),
        jsonl_path=str(jsonl_path),
        markdown_path=str(markdown_path),
        rows=tuple(rows),
    )


def load_chain_snapshot(
    *,
    chain_parquet: Path,
    trading_date: date | None = None,
    expiry: str | None = None,
    timestamp: str | None = None,
    underlying: str = "NIFTY",
) -> ChainSnapshot:
    try:
        import pandas as pd  # type: ignore[import-not-found]
    except ImportError as exc:  # pragma: no cover - caller environment issue.
        raise RuntimeError("pandas and pyarrow are required to read Groww chain parquet") from exc

    columns = [
        "session_date",
        "snapshot_time",
        "snapshot_ts_ms",
        "underlying",
        "expiry",
        "underlying_ltp",
        "strike",
        "option_type",
        "trading_symbol",
        "lot_size",
        "ltp",
    ]
    frame = pd.read_parquet(chain_parquet, columns=columns)
    if frame.empty:
        raise ValueError(f"chain parquet has no rows: {chain_parquet}")
    frame = frame[frame["underlying"].astype(str).str.upper() == str(underlying).upper()]
    if expiry:
        frame = frame[frame["expiry"].astype(str) == str(expiry)]
    if trading_date:
        frame = frame[frame["session_date"].astype(str) == trading_date.isoformat()]
    if frame.empty:
        raise ValueError("no rows remain after underlying/date/expiry filters")

    if timestamp:
        times = frame["snapshot_time"].astype(str)
        if timestamp in set(times):
            selected_ts = timestamp
        else:
            target = pd.to_datetime(timestamp)
            parsed = pd.to_datetime(times, errors="coerce")
            idx = (parsed - target).abs().idxmin()
            selected_ts = str(frame.loc[idx, "snapshot_time"])
    else:
        selected_ts = str(frame["snapshot_time"].iloc[-1])
    snap = frame[frame["snapshot_time"].astype(str) == selected_ts].copy()
    snap["strike"] = pd.to_numeric(snap["strike"], errors="coerce")
    snap["ltp"] = pd.to_numeric(snap["ltp"], errors="coerce")
    snap["lot_size"] = pd.to_numeric(snap["lot_size"], errors="coerce").fillna(0).astype(int)
    snap = snap[(snap["strike"].notna()) & (snap["ltp"].notna()) & (snap["ltp"] > 0) & (snap["lot_size"] > 0)]
    if snap.empty:
        raise ValueError(f"selected snapshot has no executable positive-LTP rows: {selected_ts}")
    session_date = date.fromisoformat(str(snap["session_date"].iloc[0]))
    selected_expiry = str(snap["expiry"].iloc[0])
    spot = float(pd.to_numeric(snap["underlying_ltp"], errors="coerce").dropna().iloc[-1])
    snapshot_ts_ms = _optional_int(snap["snapshot_ts_ms"].dropna().iloc[-1] if snap["snapshot_ts_ms"].notna().any() else None)
    records = tuple(dict(row) for row in snap.to_dict("records"))
    return ChainSnapshot(
        trading_date=session_date,
        timestamp=selected_ts,
        snapshot_ts_ms=snapshot_ts_ms,
        underlying=str(snap["underlying"].iloc[0]).upper(),
        expiry=selected_expiry,
        spot=spot,
        rows=records,
        source_path=str(chain_parquet),
    )


def generate_margin_test_baskets(
    snapshot: ChainSnapshot,
    *,
    lots: Sequence[int] = (1, 3),
    future_trading_symbol: str | None = None,
    future_expiry: str | None = None,
    future_price: float | None = None,
    future_lot_size: int | None = None,
) -> list[BasketSpec]:
    quote_map = _quote_map(snapshot.rows)
    strikes = sorted({strike for strike, opt_type in quote_map if opt_type in {"CE", "PE"}})
    if not strikes:
        raise ValueError("snapshot has no option strikes")
    atm = min(strikes, key=lambda strike: abs(strike - snapshot.spot))

    def strike_at(offset_steps: int) -> float:
        idx = strikes.index(atm)
        target_idx = max(0, min(len(strikes) - 1, idx + offset_steps))
        return float(strikes[target_idx])

    def leg(side: str, opt_type: str, offset: int, lot_mult: int) -> BasketLeg:
        strike = strike_at(offset)
        row = quote_map.get((strike, opt_type))
        if row is None:
            raise KeyError(f"missing quote for {strike:g}{opt_type}")
        return BasketLeg(
            symbol=snapshot.underlying,
            side=side,
            option_type=opt_type,
            strike=strike,
            price=float(row["ltp"]),
            lot_size=int(row["lot_size"]),
            qty_ratio=int(lot_mult),
            trading_symbol=str(row["trading_symbol"]),
            expiry=snapshot.expiry,
        )

    def fut_leg(side: str, lot_mult: int) -> BasketLeg:
        if not future_trading_symbol or not future_expiry or not future_price or not future_lot_size:
            raise KeyError("future leg requested without full futures trading symbol/expiry/price/lot_size")
        return BasketLeg(
            symbol=snapshot.underlying,
            side=side,
            option_type="FUT",
            strike=0.0,
            price=float(future_price),
            lot_size=int(future_lot_size),
            qty_ratio=int(lot_mult),
            trading_symbol=str(future_trading_symbol),
            expiry=str(future_expiry),
            instrument="FUT",
        )

    templates: list[tuple[str, str, str, tuple[tuple[str, str, int], ...]]] = [
        ("naked_short_ce_atm", "naked_short", "Naked short ATM call", (("SELL", "CE", 0),)),
        ("naked_short_pe_atm", "naked_short", "Naked short ATM put", (("SELL", "PE", 0),)),
        ("naked_short_ce_otm2", "naked_short", "Naked short OTM call", (("SELL", "CE", 2),)),
        ("naked_short_pe_otm2", "naked_short", "Naked short OTM put", (("SELL", "PE", -2),)),
        ("naked_long_ce_atm", "naked_long", "Naked long ATM call", (("BUY", "CE", 0),)),
        ("naked_long_pe_atm", "naked_long", "Naked long ATM put", (("BUY", "PE", 0),)),
        ("short_straddle_atm", "short_straddle", "Short ATM straddle", (("SELL", "CE", 0), ("SELL", "PE", 0))),
        ("short_strangle_2w", "short_strangle", "Short 2-strike strangle", (("SELL", "CE", 2), ("SELL", "PE", -2))),
        ("short_strangle_4w", "short_strangle", "Short 4-strike strangle", (("SELL", "CE", 4), ("SELL", "PE", -4))),
        ("long_strangle_2w", "long_strangle", "Long 2-strike strangle", (("BUY", "CE", 2), ("BUY", "PE", -2))),
        ("call_credit_spread_2w", "vertical_credit", "Short call spread", (("SELL", "CE", 1), ("BUY", "CE", 3))),
        ("put_credit_spread_2w", "vertical_credit", "Short put spread", (("SELL", "PE", -1), ("BUY", "PE", -3))),
        ("call_debit_spread_2w", "vertical_debit", "Long call spread", (("BUY", "CE", 1), ("SELL", "CE", 3))),
        ("put_debit_spread_2w", "vertical_debit", "Long put spread", (("BUY", "PE", -1), ("SELL", "PE", -3))),
        (
            "iron_condor_2x4",
            "hedged_short",
            "Short strangle with long wings",
            (("BUY", "PE", -4), ("SELL", "PE", -2), ("SELL", "CE", 2), ("BUY", "CE", 4)),
        ),
        (
            "iron_fly_2w",
            "hedged_short",
            "Short straddle with long wings",
            (("BUY", "PE", -2), ("SELL", "PE", 0), ("SELL", "CE", 0), ("BUY", "CE", 2)),
        ),
    ]

    baskets: list[BasketSpec] = []
    for lot_mult in lots:
        for basket_id, family, description, legs in templates:
            try:
                basket_legs = tuple(leg(side, opt_type, offset, int(lot_mult)) for side, opt_type, offset in legs)
            except KeyError:
                continue
            suffix = f"{basket_id}_lots{int(lot_mult)}"
            baskets.append(
                BasketSpec(
                    basket_id=suffix,
                    family=family,
                    description=description,
                    lots=int(lot_mult),
                    legs=basket_legs,
                )
            )
        if future_trading_symbol and future_expiry and future_price and future_lot_size:
            futures_templates: list[tuple[str, str, str, tuple[Any, ...]]] = [
                ("naked_long_future", "naked_future", "Naked long NIFTY future", (("FUT", "BUY"),)),
                ("naked_short_future", "naked_future", "Naked short NIFTY future", (("FUT", "SELL"),)),
                (
                    "long_future_short_call_atm",
                    "future_option_hedge",
                    "Long future hedged with short ATM call",
                    (("FUT", "BUY"), ("OPT", "SELL", "CE", 0)),
                ),
                (
                    "short_future_short_put_atm",
                    "future_option_hedge",
                    "Short future hedged with short ATM put",
                    (("FUT", "SELL"), ("OPT", "SELL", "PE", 0)),
                ),
                (
                    "short_strangle_long_future",
                    "beta_hedged_short",
                    "Short strangle with long future beta overlay",
                    (("OPT", "SELL", "CE", 2), ("OPT", "SELL", "PE", -2), ("FUT", "BUY")),
                ),
                (
                    "short_strangle_short_future",
                    "beta_hedged_short",
                    "Short strangle with short future beta overlay",
                    (("OPT", "SELL", "CE", 2), ("OPT", "SELL", "PE", -2), ("FUT", "SELL")),
                ),
            ]
            for basket_id, family, description, legs in futures_templates:
                basket_legs = []
                for raw_leg in legs:
                    if raw_leg[0] == "FUT":
                        basket_legs.append(fut_leg(raw_leg[1], int(lot_mult)))
                    else:
                        _, side, opt_type, offset = raw_leg
                        basket_legs.append(leg(side, opt_type, offset, int(lot_mult)))
                baskets.append(
                    BasketSpec(
                        basket_id=f"{basket_id}_lots{int(lot_mult)}",
                        family=family,
                        description=description,
                        lots=int(lot_mult),
                        legs=tuple(basket_legs),
                    )
                )
    return baskets


def _local_margin(
    basket: BasketSpec,
    *,
    snapshot: ChainSnapshot,
    span_data: Any,
    estimate_groww_charges: bool = False,
) -> LocalMarginComponents:
    try:
        breakdown = margin_for_candidate_legs(
            legs=[leg.to_margin_leg() for leg in basket.legs],
            span_data=span_data,
            index=snapshot.underlying,
            expiry=snapshot.expiry,
            spot=snapshot.spot,
            eval_dt=datetime.fromisoformat(f"{snapshot.trading_date.isoformat()}T09:15:00"),
            prev_close_spot=snapshot.spot,
        )
    except (SpanMarginError, ValueError, TypeError, KeyError) as exc:
        return LocalMarginComponents(
            total_requirement=math.nan,
            span_required=math.nan,
            scan_risk_before_nov=math.nan,
            short_option_credit=math.nan,
            option_buy_premium=math.nan,
            exposure_required=math.nan,
            brokerage_and_charges=0.0,
            selected_span_slot=str(getattr(span_data, "selected_time_slot", "")),
            span_trading_date=str(getattr(span_data, "trading_date", "")),
            active_scenario=0,
            error=f"{type(exc).__name__}: {exc}",
        )
    long_premium = sum(
        leg.price * leg.quantity for leg in basket.legs if leg.side == "BUY" and leg.instrument != "FUT"
    )
    exposure = max(0.0, float(breakdown.elm_plus_long_prem) - float(long_premium))
    scenarios = tuple(float(value) for value in breakdown.scan_scenarios)
    active_scenario = int(max(range(len(scenarios)), key=lambda idx: scenarios[idx]) + 1) if scenarios else 0
    charges = (
        estimate_groww_margin_api_charges(
            [leg.to_charge_leg(broker_rounded_price=True) for leg in basket.legs]
        ).total
        if estimate_groww_charges
        else 0.0
    )
    return LocalMarginComponents(
        total_requirement=float(breakdown.margin) + float(charges),
        span_required=float(breakdown.s_net_clamped),
        scan_risk_before_nov=float(breakdown.m_span),
        short_option_credit=float(breakdown.credit_sum),
        option_buy_premium=float(long_premium),
        exposure_required=float(exposure),
        brokerage_and_charges=float(charges),
        selected_span_slot=str(breakdown.span_time_slot),
        span_trading_date=str(breakdown.span_trading_date),
        active_scenario=active_scenario,
        scan_scenarios=scenarios,
    )


def _poll_groww_margin(adapter: "_GrowwMarginClient" | None, basket: BasketSpec) -> GrowwMarginComponents:
    if adapter is None:
        return GrowwMarginComponents(error="groww_poll_disabled")
    try:
        raw = adapter.quote_basket_margin(basket)
    except Exception as exc:  # noqa: BLE001 - diagnostic should record broker failure.
        return GrowwMarginComponents(error=f"{type(exc).__name__}: {exc}")
    payload = _unwrap_payload(raw)
    return GrowwMarginComponents(
        total_requirement=_float_field(payload, "total_requirement", "total_margin", "required_margin", "margin_required"),
        span_required=_float_field(payload, "span_required", "span_margin_required"),
        exposure_required=_float_field(payload, "exposure_required", "exposure_margin_required"),
        option_buy_premium=_float_field(payload, "option_buy_premium"),
        brokerage_and_charges=_float_field(payload, "brokerage_and_charges"),
        physical_delivery_margin_requirement=_float_field(payload, "physical_delivery_margin_requirement"),
        raw=dict(raw),
    )


@dataclass(frozen=True)
class _GrowwMarginClient:
    access_token: str

    @classmethod
    def from_env(cls) -> "_GrowwMarginClient":
        token = str(os.environ.get("GROWW_ACCESS_TOKEN", "") or "").strip()
        if not token:
            raise ValueError("GROWW_ACCESS_TOKEN is required when --poll-groww is enabled")
        return cls(access_token=token)

    def quote_basket_margin(self, basket: BasketSpec) -> Mapping[str, Any]:
        try:
            from growwapi import GrowwAPI  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - operator environment issue.
            raise RuntimeError("growwapi package is required when --poll-groww is enabled") from exc
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            api = GrowwAPI(self.access_token)
        response = api.get_order_margin_details(segment="FNO", orders=_groww_margin_orders_payload(basket))
        return response if isinstance(response, Mapping) else {"payload": response}


def _groww_margin_orders_payload(basket: BasketSpec) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for leg in basket.legs:
        orders.append(
            {
                "trading_symbol": leg.trading_symbol,
                "transaction_type": leg.side.upper(),
                "quantity": int(leg.quantity),
                "price": round_broker_limit_price(leg.price, leg.side),
                "order_type": "LIMIT",
                "product": "MIS",
                "exchange": "NSE",
                "segment": "FNO",
            }
        )
    return orders


def _parity_row(
    *,
    basket: BasketSpec,
    local: LocalMarginComponents,
    groww: GrowwMarginComponents | None,
    warn_abs_inr: float,
    fail_abs_inr: float,
    warn_pct: float,
    fail_pct: float,
) -> MarginParityRow:
    diff: float | None = None
    diff_pct: float | None = None
    verdict = "LOCAL_ONLY"
    if local.error:
        verdict = "LOCAL_ERROR"
    if groww is not None:
        if groww.error:
            verdict = "BROKER_ERROR"
        elif _finite(local.total_requirement) and _finite(groww.total_requirement):
            diff = float(local.total_requirement) - float(groww.total_requirement)
            base = max(abs(float(groww.total_requirement)), 1.0)
            diff_pct = abs(diff) / base
            if abs(diff) >= fail_abs_inr or diff_pct >= fail_pct:
                verdict = "FAIL"
            elif abs(diff) >= warn_abs_inr or diff_pct >= warn_pct:
                verdict = "WARN"
            else:
                verdict = "PASS"
        else:
            verdict = "MISSING_NUMERIC"
    return MarginParityRow(
        basket_id=basket.basket_id,
        family=basket.family,
        description=basket.description,
        lots=basket.lots,
        leg_count=len(basket.legs),
        leg_symbols=tuple(leg.trading_symbol for leg in basket.legs),
        exposure_risk_quantity=_exposure_risk_quantity(basket),
        local=local,
        groww=groww,
        diff_total=diff,
        diff_total_pct=diff_pct,
        verdict=verdict,
    )


def _exposure_risk_quantity(basket: BasketSpec) -> int:
    quantity = 0
    for leg in basket.legs:
        is_future = leg.instrument.upper().startswith("FUT") or leg.option_type.upper() == "FUT"
        if is_future or leg.side.upper() == "SELL":
            quantity += int(leg.quantity)
    return int(quantity)


def _exposure_reference_avg(exposure_risk_quantity: int, exposure_required: float) -> float:
    if exposure_risk_quantity <= 0 or not _finite(exposure_required):
        return math.nan
    return float(exposure_required) / (INDEX_ELM_BASE * float(exposure_risk_quantity))


def _exposure_reference_diff(
    exposure_risk_quantity: int,
    local_exposure_required: float,
    groww_exposure_required: float,
) -> float:
    local_ref = _exposure_reference_avg(exposure_risk_quantity, local_exposure_required)
    groww_ref = _exposure_reference_avg(exposure_risk_quantity, groww_exposure_required)
    if not _finite(local_ref) or not _finite(groww_ref):
        return math.nan
    return float(local_ref) - float(groww_ref)


def _quote_map(rows: Iterable[Mapping[str, Any]]) -> dict[tuple[float, str], Mapping[str, Any]]:
    out: dict[tuple[float, str], Mapping[str, Any]] = {}
    for row in rows:
        option_type = str(row.get("option_type", "")).upper().strip()
        if option_type not in {"CE", "PE"}:
            continue
        strike = round(float(row.get("strike") or 0.0), 2)
        if strike <= 0:
            continue
        ltp = float(row.get("ltp") or 0.0)
        lot_size = int(row.get("lot_size") or 0)
        trading_symbol = str(row.get("trading_symbol", "") or "").strip()
        if ltp <= 0 or lot_size <= 0 or not trading_symbol:
            continue
        out[(strike, option_type)] = row
    return out


def _write_csv(path: Path, rows: Sequence[MarginParityRow]) -> None:
    flat = [row.to_flat_dict() for row in rows]
    fieldnames = list(flat[0].keys()) if flat else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flat)


def _write_jsonl(path: Path, rows: Sequence[MarginParityRow]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(asdict(row)), sort_keys=True) + "\n")


def _write_markdown(
    path: Path,
    *,
    rows: Sequence[MarginParityRow],
    snapshot: ChainSnapshot,
    span_parquet_dir: Path,
    span_time_slot: str,
    groww_polled: bool,
    estimate_groww_charges: bool,
    warn_abs_inr: float,
    fail_abs_inr: float,
    warn_pct: float,
    fail_pct: float,
) -> None:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row.verdict] = counts.get(row.verdict, 0) + 1
    lines = [
        "# Groww SPAN Basket Margin Parity Report",
        "",
        "## Snapshot",
        "",
        f"- Trading date: `{snapshot.trading_date.isoformat()}`",
        f"- Timestamp: `{snapshot.timestamp}`",
        f"- Underlying / expiry: `{snapshot.underlying}` / `{snapshot.expiry}`",
        f"- Spot: `{snapshot.spot:.2f}`",
        f"- Chain source: `{snapshot.source_path}`",
        f"- SPAN parquet dir: `{span_parquet_dir}`",
        f"- SPAN slot request: `{span_time_slot}`",
        f"- Groww polled: `{groww_polled}`",
        f"- Local Groww charge estimate: `{estimate_groww_charges}`",
        f"- Warning threshold: `{warn_abs_inr:.0f}` INR or `{warn_pct:.2%}`",
        f"- Failure threshold: `{fail_abs_inr:.0f}` INR or `{fail_pct:.2%}`",
        "",
        "## Verdict Counts",
        "",
    ]
    for key in sorted(counts):
        lines.append(f"- `{key}`: `{counts[key]}`")
    lines.extend(
        [
            "",
            "## Rows",
            "",
            "| Basket | Family | Local Total | Groww Total | Diff | Diff % | Verdict |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]
    )
    for row in rows:
        flat = row.to_flat_dict()
        lines.append(
            "| {basket_id} | {family} | {local_total_requirement:.2f} | {groww_total_requirement} | {diff_total} | {diff_total_pct} | {verdict} |".format(
                basket_id=row.basket_id,
                family=row.family,
                local_total_requirement=_fmt_float(flat["local_total_requirement"]),
                groww_total_requirement=_fmt_optional(flat["groww_total_requirement"]),
                diff_total=_fmt_optional(flat["diff_total"]),
                diff_total_pct=_fmt_pct(flat["diff_total_pct"]),
                verdict=row.verdict,
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `local_span_required` is local SPAN after the short-option NOV/credit adjustment used by the current live SPAN Model-A code.",
            "- `local_exposure_required` is ELM/exposure separated from long option premium.",
            "- `local_option_buy_premium` is premium debit for long option legs.",
            "- `local_brokerage_and_charges` is zero unless `--estimate-groww-charges` is enabled; when enabled it estimates Groww margin API's broker-reserve style `brokerage_and_charges`.",
            "- Groww fields are broker truth for the submitted payload shape; raw responses are retained in JSONL.",
            "- If Groww is not polled, the report validates local decomposition only and marks rows `LOCAL_ONLY`.",
        ]
    )
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _unwrap_payload(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = raw.get("payload") if isinstance(raw, Mapping) else None
    return payload if isinstance(payload, Mapping) else raw


def _float_field(payload: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return math.nan


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _finite(value: float | None) -> bool:
    return value is not None and math.isfinite(float(value))


def _fmt_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return math.nan
    return number


def _fmt_optional(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return ""
    return f"{number:.2f}"


def _fmt_pct(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if not math.isfinite(number):
        return ""
    return f"{number:.2%}"


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
