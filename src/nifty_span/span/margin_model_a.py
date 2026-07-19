from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Mapping, Sequence

from .contracts import SpanData, SpanMarginBreakdown


INDEX_SYMBOLS = frozenset({"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50", "SENSEX", "BANKEX"})
INDEX_ELM_BASE = 0.02
STOCK_ELM_BASE = 0.035
EXPIRY_DAY_EXTRA = 0.02
INDEX_DEEP_OTM_RATE = 0.03
INDEX_DEEP_OTM_THRESH = 0.10
STOCK_DEEP_OTM_RATE = 0.0525
STOCK_DEEP_OTM_THRESH = 0.30


class SpanMarginError(ValueError):
    pass


@dataclass(frozen=True)
class SpanLegSpec:
    symbol: str
    opt_type: str
    expiry: str
    strike: float
    qty_sign: int
    qty_ratio: int
    lot_size: int
    entry_price: float
    is_option: bool = True
    additional_margin: float = 0.0
    additional_margin_rate: float = 0.0
    delivery_margin: float = 0.0
    crystallized_obligation_margin: float = 0.0
    cross_margin_benefit: float = 0.0
    minimum_total_margin_rate: float = 0.0


def margin_for_candidate_legs(
    *,
    legs: Sequence[Mapping[str, Any]],
    span_data: SpanData,
    index: str,
    expiry: str,
    spot: float,
    eval_dt: datetime | None = None,
    prev_close_spot: float | None = None,
) -> SpanMarginBreakdown:
    specs: list[SpanLegSpec] = []
    expiry_key = _expiry_key(expiry)
    if expiry_key is None:
        raise SpanMarginError(f"invalid expiry for SPAN lookup: {expiry!r}")
    for raw in legs:
        side = str(raw.get("side", "")).upper().strip()
        qty_sign = _qty_sign_from_side(side)
        instrument = str(raw.get("instrument", raw.get("instrument_type", "")) or "").upper().strip()
        option_type = str(raw.get("option_type", "")).upper().strip()
        is_option = not (
            raw.get("is_option") is False
            or instrument.startswith("FUT")
            or option_type in {"FUT", "FUTIDX", "FUTSTK"}
        )
        strike = float(raw.get("strike", 0.0) or 0.0)
        lot_size = int(raw.get("lot_size") or 0)
        if lot_size <= 0:
            raise SpanMarginError(f"missing lot_size for SPAN leg {raw!r}")
        leg_expiry_key = _expiry_key(raw.get("expiry", expiry))
        if leg_expiry_key is None:
            raise SpanMarginError(f"invalid expiry for SPAN leg {raw!r}")
        specs.append(
            SpanLegSpec(
                symbol=index,
                opt_type=option_type if is_option else "FUT",
                expiry=leg_expiry_key,
                strike=strike,
                qty_sign=qty_sign,
                qty_ratio=max(1, int(raw.get("qty_ratio", 1) or 1)),
                lot_size=lot_size,
                entry_price=float(raw.get("limit_price", raw.get("entry_price", 0.0)) or 0.0),
                is_option=is_option,
                additional_margin=float(raw.get("additional_margin", 0.0) or 0.0),
                additional_margin_rate=float(raw.get("additional_margin_rate", 0.0) or 0.0),
                delivery_margin=float(raw.get("delivery_margin", raw.get("physical_delivery_margin", 0.0)) or 0.0),
                crystallized_obligation_margin=float(raw.get("crystallized_obligation_margin", 0.0) or 0.0),
                cross_margin_benefit=float(raw.get("cross_margin_benefit", 0.0) or 0.0),
                minimum_total_margin_rate=float(raw.get("minimum_total_margin_rate", 0.0) or 0.0),
            )
        )
    return margin_for_leg_specs(
        specs,
        span_data=span_data,
        spot=spot,
        eval_dt=eval_dt,
        prev_close_spot=prev_close_spot,
    )


def margin_for_leg_specs(
    legs: Sequence[SpanLegSpec],
    *,
    span_data: SpanData,
    spot: float,
    eval_dt: datetime | None = None,
    prev_close_spot: float | None = None,
) -> SpanMarginBreakdown:
    if not legs:
        raise SpanMarginError("no legs supplied for SPAN margin")
    if spot <= 0.0:
        raise SpanMarginError("spot must be positive for SPAN margin")
    if span_data.trading_date is not None and eval_dt is not None and eval_dt.date() != span_data.trading_date:
        raise SpanMarginError(
            f"SPAN trading date {span_data.trading_date} does not match eval date {eval_dt.date()}"
        )

    scenario_totals = [0.0] * 16
    credit_sum = 0.0
    long_premium = 0.0
    long_option_value = 0.0
    has_margin_risk_leg = False
    resolved_eval_dt = eval_dt or datetime.combine(span_data.trading_date or date.today(), time(hour=9, minute=15))
    ref_spot = float(prev_close_spot) if prev_close_spot else float(spot)

    for leg in legs:
        if (not leg.is_option) or leg.qty_sign == -1:
            has_margin_risk_leg = True
        if leg.is_option:
            contract = span_data.lookup_option(leg.symbol, leg.opt_type, leg.expiry, leg.strike)
        else:
            contract = span_data.lookup_future(leg.symbol, leg.expiry)
        if contract is None:
            raise SpanMarginError(
                f"missing SPAN contract {leg.symbol} {leg.opt_type} {leg.expiry} {leg.strike:g}"
            )
        qty_per_lot = float(leg.qty_sign) * float(leg.qty_ratio) * float(leg.lot_size)
        for idx, value in enumerate(contract.risk_array[:16]):
            scenario_totals[idx] += qty_per_lot * float(value)
        quantity = abs(float(leg.qty_ratio) * float(leg.lot_size))
        premium = quantity * float(leg.entry_price)
        span_option_value = quantity * _span_option_price(contract=contract, fallback_price=leg.entry_price)
        if leg.is_option and leg.qty_sign == -1:
            credit_sum += span_option_value
        if leg.is_option and leg.qty_sign == +1:
            long_premium += premium
            long_option_value += span_option_value

    m_span = max(max(scenario_totals) if scenario_totals else 0.0, 0.0)
    net_option_value = long_option_value - credit_sum
    if has_margin_risk_leg:
        s_net_raw = m_span - net_option_value
        s_net = max(s_net_raw, 0.0)
    else:
        s_net_raw = 0.0
        s_net = 0.0
    elm = _portfolio_elm(legs, spot=float(spot), ref_spot=ref_spot, eval_dt=resolved_eval_dt)
    elm_plus_long_prem = elm + long_premium
    add_on_margin = _add_on_margin(legs, spot=float(spot), ref_spot=ref_spot)
    delivery_margin = sum(max(0.0, float(leg.delivery_margin)) for leg in legs)
    crystallized_margin = sum(max(0.0, float(leg.crystallized_obligation_margin)) for leg in legs)
    cross_margin_benefit = sum(max(0.0, float(leg.cross_margin_benefit)) for leg in legs)
    minimum_total_margin_floor = _minimum_total_margin_floor(legs, spot=float(spot), ref_spot=ref_spot)
    margin_before_floor = s_net + elm_plus_long_prem + add_on_margin + delivery_margin + crystallized_margin
    margin = max(margin_before_floor - cross_margin_benefit, minimum_total_margin_floor, 1.0)
    return SpanMarginBreakdown(
        margin=float(margin),
        source="span_model_a",
        scan_scenarios=tuple(float(value) for value in scenario_totals),
        m_span=float(m_span),
        credit_sum=float(credit_sum),
        long_premium=float(long_premium),
        long_option_value=float(long_option_value),
        net_option_value=float(net_option_value),
        s_net_raw=float(s_net_raw),
        s_net_clamped=float(s_net),
        elm_required=float(elm),
        elm_plus_long_prem=float(elm_plus_long_prem),
        add_on_margin=float(add_on_margin),
        delivery_margin=float(delivery_margin),
        crystallized_obligation_margin=float(crystallized_margin),
        cross_margin_benefit=float(cross_margin_benefit),
        minimum_total_margin_floor=float(minimum_total_margin_floor),
        span_time_slot=str(span_data.selected_time_slot or ""),
        span_trading_date=span_data.trading_date.isoformat() if span_data.trading_date is not None else "",
    )


def _span_option_price(*, contract: Any, fallback_price: float) -> float:
    price = float(getattr(contract, "price", 0.0) or 0.0)
    if price > 0.0:
        return price
    return float(fallback_price)


def _portfolio_elm(legs: Sequence[SpanLegSpec], *, spot: float, ref_spot: float, eval_dt: datetime) -> float:
    option_elm = 0.0
    futures_by_symbol: dict[str, list[SpanLegSpec]] = {}
    for leg in legs:
        if leg.is_option:
            option_elm += _elm_rate(leg, spot=spot, ref_spot=ref_spot, eval_dt=eval_dt) * _elm_notional(
                leg, spot=spot, ref_spot=ref_spot
            )
        else:
            futures_by_symbol.setdefault(leg.symbol.upper(), []).append(leg)
    futures_elm = sum(
        _futures_elm_with_calendar_spreads(symbol, symbol_legs, spot=spot, eval_dt=eval_dt)
        for symbol, symbol_legs in futures_by_symbol.items()
    )
    return float(option_elm + futures_elm)


def _futures_elm_with_calendar_spreads(
    symbol: str,
    legs: Sequence[SpanLegSpec],
    *,
    spot: float,
    eval_dt: datetime,
) -> float:
    if not legs:
        return 0.0
    base_rate = INDEX_ELM_BASE if symbol.upper() in INDEX_SYMBOLS else STOCK_ELM_BASE
    positions: list[dict[str, Any]] = []
    by_expiry: dict[date, dict[str, float]] = {}
    for leg in legs:
        expiry_date = _expiry_date(leg.expiry)
        if expiry_date is None:
            continue
        signed_qty = float(leg.qty_sign) * float(leg.qty_ratio) * float(leg.lot_size)
        if abs(signed_qty) <= 0.0:
            continue
        price = _future_notional_price(leg, spot=spot)
        bucket = by_expiry.setdefault(expiry_date, {"qty": 0.0, "notional": 0.0})
        bucket["qty"] += signed_qty
        bucket["notional"] += abs(signed_qty) * price
    for expiry_date, bucket in by_expiry.items():
        qty = float(bucket["qty"])
        if abs(qty) <= 0.0:
            continue
        positions.append(
            {
                "expiry": expiry_date,
                "qty": qty,
                "price": float(bucket["notional"]) / abs(qty),
            }
        )
    positions.sort(key=lambda item: item["expiry"])
    elm = 0.0
    is_index = symbol.upper() in INDEX_SYMBOLS
    for i, near in enumerate(positions):
        if abs(float(near["qty"])) <= 0.0:
            continue
        for far in positions[i + 1 :]:
            if abs(float(near["qty"])) <= 0.0:
                break
            if abs(float(far["qty"])) <= 0.0 or float(near["qty"]) * float(far["qty"]) >= 0.0:
                continue
            if not _calendar_spread_eligible(near_expiry=near["expiry"], eval_date=eval_dt.date(), is_index=is_index):
                continue
            paired_qty = min(abs(float(near["qty"])), abs(float(far["qty"])))
            elm += base_rate * float(far["price"]) * paired_qty / 3.0
            near["qty"] = float(near["qty"]) - (paired_qty if float(near["qty"]) > 0 else -paired_qty)
            far["qty"] = float(far["qty"]) - (paired_qty if float(far["qty"]) > 0 else -paired_qty)
    for position in positions:
        elm += base_rate * float(position["price"]) * abs(float(position["qty"]))
    return float(elm)


def _calendar_spread_eligible(*, near_expiry: date, eval_date: date, is_index: bool) -> bool:
    if is_index:
        return near_expiry > eval_date
    return near_expiry >= eval_date


def _elm_notional(leg: SpanLegSpec, *, spot: float, ref_spot: float) -> float:
    quantity = abs(float(leg.qty_ratio) * float(leg.lot_size))
    if leg.is_option:
        return float(ref_spot) * quantity
    return _future_notional_price(leg, spot=spot) * quantity


def _future_notional_price(leg: SpanLegSpec, *, spot: float) -> float:
    if float(leg.entry_price) > 0.0:
        return float(leg.entry_price)
    return float(spot)


def _add_on_margin(legs: Sequence[SpanLegSpec], *, spot: float, ref_spot: float) -> float:
    total = 0.0
    for leg in legs:
        total += max(0.0, float(leg.additional_margin))
        rate = max(0.0, float(leg.additional_margin_rate))
        if rate > 0.0:
            total += rate * _elm_notional(leg, spot=spot, ref_spot=ref_spot)
    return float(total)


def _minimum_total_margin_floor(legs: Sequence[SpanLegSpec], *, spot: float, ref_spot: float) -> float:
    floor = 0.0
    for leg in legs:
        rate = max(0.0, float(leg.minimum_total_margin_rate))
        if rate > 0.0:
            floor += rate * _elm_notional(leg, spot=spot, ref_spot=ref_spot)
    return float(floor)


def _qty_sign_from_side(side: str) -> int:
    if side == "SELL":
        return -1
    if side == "BUY":
        return +1
    raise SpanMarginError(f"unsupported SPAN leg side: {side!r}")


def _expiry_key(value: str | date) -> str | None:
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    text = str(value or "").strip()
    compact = text.replace("-", "")
    if len(compact) == 8 and compact.isdigit():
        return compact
    return None


def _expiry_date(value: str | date) -> date | None:
    key = _expiry_key(value)
    if key is None:
        return None
    return date(int(key[:4]), int(key[4:6]), int(key[6:8]))


def _elm_rate(leg: SpanLegSpec, *, spot: float, ref_spot: float, eval_dt: datetime) -> float:
    if leg.is_option and leg.qty_sign == +1:
        return 0.0
    is_index = leg.symbol.upper() in INDEX_SYMBOLS
    base = INDEX_ELM_BASE if is_index else STOCK_ELM_BASE
    rate = base
    if leg.is_option and leg.qty_sign == -1 and is_index:
        expiry_date = _expiry_date(leg.expiry)
        if expiry_date == eval_dt.date():
            rate = base + EXPIRY_DAY_EXTRA
        if expiry_date is not None and expiry_date > _add_months(eval_dt.date(), 9):
            rate = max(rate, 0.05)
    if leg.is_option and leg.qty_sign == -1 and ref_spot > 0:
        otm_pct = abs(float(leg.strike) - float(ref_spot)) / float(ref_spot)
        if is_index and otm_pct > INDEX_DEEP_OTM_THRESH:
            rate = max(rate, INDEX_DEEP_OTM_RATE)
        elif (not is_index) and otm_pct > STOCK_DEEP_OTM_THRESH:
            rate = max(rate, STOCK_DEEP_OTM_RATE)
    return float(rate)


def _add_months(value: date, months: int) -> date:
    month = value.month - 1 + months
    year = value.year + month // 12
    month = month % 12 + 1
    day = min(value.day, _days_in_month(year, month))
    return date(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    next_month = date(year if month < 12 else year + 1, month + 1 if month < 12 else 1, 1)
    return (next_month - date(year, month, 1)).days
