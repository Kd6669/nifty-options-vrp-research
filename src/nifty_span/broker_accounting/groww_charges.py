from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class GrowwFnoChargeLeg:
    side: str
    instrument: str
    price: float
    quantity: int
    exchange: str = "NSE"

    @property
    def turnover(self) -> float:
        return abs(float(self.price) * float(self.quantity))

    @property
    def is_buy(self) -> bool:
        return self.side.upper().strip() == "BUY"

    @property
    def is_sell(self) -> bool:
        return self.side.upper().strip() == "SELL"

    @property
    def is_future(self) -> bool:
        text = self.instrument.upper().strip()
        return text in {"FUT", "FUTIDX", "FUTSTK"} or text.startswith("FUT")

    @property
    def is_option(self) -> bool:
        return not self.is_future


@dataclass(frozen=True)
class GrowwFnoChargeRates:
    brokerage_per_order: float = 20.0
    margin_api_turnover_reserve_rate: float = 0.0007
    futures_stt_sell_rate: float = 0.0002
    options_stt_sell_rate: float = 0.001
    futures_stamp_buy_rate: float = 0.00002
    options_stamp_buy_rate: float = 0.00003
    futures_exchange_nse_rate: float = 0.0000173
    options_exchange_nse_rate: float = 0.0003503
    futures_exchange_bse_rate: float = 0.0
    options_exchange_bse_rate: float = 0.000325
    sebi_turnover_rate: float = 0.000001
    futures_ipft_nse_rate: float = 0.000001
    options_ipft_nse_rate: float = 0.000005
    gst_rate: float = 0.18
    physical_delivery_brokerage_per_order: float = 20.0


@dataclass(frozen=True)
class GrowwFnoChargeBreakdown:
    brokerage: float
    stt_ctt: float
    stamp_duty: float
    exchange_transaction: float
    sebi_turnover: float
    ipft: float
    gst: float
    physical_delivery_brokerage: float = 0.0
    margin_api_turnover_reserve: float = 0.0
    broker_reported: float = 0.0

    @property
    def total(self) -> float:
        return (
            float(self.brokerage)
            + float(self.stt_ctt)
            + float(self.stamp_duty)
            + float(self.exchange_transaction)
            + float(self.sebi_turnover)
            + float(self.ipft)
            + float(self.gst)
            + float(self.physical_delivery_brokerage)
            + float(self.margin_api_turnover_reserve)
            + float(self.broker_reported)
        )

    def to_dict(self, *, rounded: bool = False) -> dict[str, float]:
        payload = asdict(self)
        payload["total"] = self.total
        if rounded:
            return {key: round(float(value), 2) for key, value in payload.items()}
        return {key: float(value) for key, value in payload.items()}


def groww_fno_charge_leg_from_mapping(raw: Mapping[str, object]) -> GrowwFnoChargeLeg:
    instrument = str(raw.get("instrument", raw.get("instrument_type", raw.get("option_type", ""))) or "")
    return GrowwFnoChargeLeg(
        side=str(raw.get("side", "") or ""),
        instrument=instrument,
        price=float(raw.get("price", raw.get("limit_price", raw.get("entry_price", 0.0))) or 0.0),
        quantity=int(raw.get("quantity", raw.get("qty", raw.get("lot_size", 0))) or 0),
        exchange=str(raw.get("exchange", "NSE") or "NSE"),
    )


def estimate_groww_fno_charges(
    legs: Sequence[GrowwFnoChargeLeg | Mapping[str, object]],
    *,
    rates: GrowwFnoChargeRates | None = None,
    include_physical_delivery_brokerage: bool = False,
) -> GrowwFnoChargeBreakdown:
    """Estimate Groww F&O charges for a basket.

    This is a deterministic estimate from published charge categories. When the
    Groww margin API returns `brokerage_and_charges`, that broker-reported value
    remains the source of truth for live routing and parity checks.
    """

    rate = rates or GrowwFnoChargeRates()
    parsed = tuple(_coerce_leg(leg) for leg in legs)
    brokerage = sum(rate.brokerage_per_order for leg in parsed if leg.turnover > 0.0)
    stt_ctt = sum(_stt_ctt(leg, rate) for leg in parsed)
    stamp_duty = sum(_stamp_duty(leg, rate) for leg in parsed)
    exchange_transaction = sum(_exchange_transaction(leg, rate) for leg in parsed)
    sebi_turnover = sum(rate.sebi_turnover_rate * leg.turnover for leg in parsed)
    ipft = sum(_ipft(leg, rate) for leg in parsed)
    gst = rate.gst_rate * (brokerage + exchange_transaction + sebi_turnover + ipft)
    physical_delivery = (
        sum(rate.physical_delivery_brokerage_per_order for leg in parsed if leg.turnover > 0.0)
        if include_physical_delivery_brokerage
        else 0.0
    )
    return GrowwFnoChargeBreakdown(
        brokerage=float(brokerage),
        stt_ctt=float(stt_ctt),
        stamp_duty=float(stamp_duty),
        exchange_transaction=float(exchange_transaction),
        sebi_turnover=float(sebi_turnover),
        ipft=float(ipft),
        gst=float(gst),
        physical_delivery_brokerage=float(physical_delivery),
    )


def estimate_groww_margin_api_charges(
    legs: Sequence[GrowwFnoChargeLeg | Mapping[str, object]],
    *,
    rates: GrowwFnoChargeRates | None = None,
) -> GrowwFnoChargeBreakdown:
    """Estimate Groww margin API `brokerage_and_charges`.

    Live basket-margin evidence shows Groww's margin API aggregate behaves like
    a broker reserve of roughly `20 INR per leg + 0.07% of leg turnover`,
    independent of instrument type and transaction side. Keep this separate from
    the statutory/public charge estimator above.
    """

    rate = rates or GrowwFnoChargeRates()
    parsed = tuple(_coerce_leg(leg) for leg in legs)
    brokerage = sum(rate.brokerage_per_order for leg in parsed if leg.turnover > 0.0)
    reserve = sum(rate.margin_api_turnover_reserve_rate * leg.turnover for leg in parsed)
    return GrowwFnoChargeBreakdown(
        brokerage=float(brokerage),
        stt_ctt=0.0,
        stamp_duty=0.0,
        exchange_transaction=0.0,
        sebi_turnover=0.0,
        ipft=0.0,
        gst=0.0,
        margin_api_turnover_reserve=float(reserve),
    )


def broker_reported_groww_charges(payload: Mapping[str, object]) -> GrowwFnoChargeBreakdown:
    """Wrap Groww's aggregate charge field without decomposing it."""

    charges = float(payload.get("brokerage_and_charges", 0.0) or 0.0)
    return GrowwFnoChargeBreakdown(
        brokerage=0.0,
        stt_ctt=0.0,
        stamp_duty=0.0,
        exchange_transaction=0.0,
        sebi_turnover=0.0,
        ipft=0.0,
        gst=0.0,
        broker_reported=charges,
    )


def _coerce_leg(leg: GrowwFnoChargeLeg | Mapping[str, object]) -> GrowwFnoChargeLeg:
    if isinstance(leg, GrowwFnoChargeLeg):
        return leg
    return groww_fno_charge_leg_from_mapping(leg)


def _stt_ctt(leg: GrowwFnoChargeLeg, rates: GrowwFnoChargeRates) -> float:
    if not leg.is_sell:
        return 0.0
    if leg.is_future:
        return rates.futures_stt_sell_rate * leg.turnover
    return rates.options_stt_sell_rate * leg.turnover


def _stamp_duty(leg: GrowwFnoChargeLeg, rates: GrowwFnoChargeRates) -> float:
    if not leg.is_buy:
        return 0.0
    if leg.is_future:
        return rates.futures_stamp_buy_rate * leg.turnover
    return rates.options_stamp_buy_rate * leg.turnover


def _exchange_transaction(leg: GrowwFnoChargeLeg, rates: GrowwFnoChargeRates) -> float:
    if leg.exchange.upper().strip() == "BSE":
        return (rates.futures_exchange_bse_rate if leg.is_future else rates.options_exchange_bse_rate) * leg.turnover
    return (rates.futures_exchange_nse_rate if leg.is_future else rates.options_exchange_nse_rate) * leg.turnover


def _ipft(leg: GrowwFnoChargeLeg, rates: GrowwFnoChargeRates) -> float:
    if leg.exchange.upper().strip() != "NSE":
        return 0.0
    return (rates.futures_ipft_nse_rate if leg.is_future else rates.options_ipft_nse_rate) * leg.turnover
