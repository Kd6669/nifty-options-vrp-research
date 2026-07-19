from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Sequence

from nifty_span.broker_accounting import (
    GrowwFnoChargeBreakdown,
    GrowwFnoChargeLeg,
    GrowwFnoChargeRates,
    estimate_groww_fno_charges,
)


@dataclass(frozen=True)
class ExecutedLeg:
    side: str
    instrument: str
    price: float
    quantity: int
    slippage_per_unit: float = 0.0
    exchange: str = "NSE"

    def as_charge_leg(self) -> GrowwFnoChargeLeg:
        return GrowwFnoChargeLeg(
            side=self.side,
            instrument=self.instrument,
            price=self.price,
            quantity=self.quantity,
            exchange=self.exchange,
        )


@dataclass(frozen=True)
class BasketExecutionCost:
    charges: GrowwFnoChargeBreakdown
    slippage: float

    @property
    def total(self) -> float:
        return self.charges.total + self.slippage

    def to_dict(self) -> dict[str, object]:
        return {
            "charges": self.charges.to_dict(),
            "slippage": float(self.slippage),
            "total": float(self.total),
        }


@dataclass(frozen=True)
class RoundTripExecutionCost:
    entry: BasketExecutionCost
    exit: BasketExecutionCost

    @property
    def total_charges(self) -> float:
        return self.entry.charges.total + self.exit.charges.total

    @property
    def total_slippage(self) -> float:
        return self.entry.slippage + self.exit.slippage

    @property
    def total(self) -> float:
        return self.entry.total + self.exit.total

    def to_dict(self) -> dict[str, object]:
        return {
            "entry": self.entry.to_dict(),
            "exit": self.exit.to_dict(),
            "total_charges": float(self.total_charges),
            "total_slippage": float(self.total_slippage),
            "total": float(self.total),
        }


def groww_fno_rates_for_date(value: date | datetime | str) -> GrowwFnoChargeRates:
    """Return the statutory option STT regime applicable on a trade date.

    Other fields retain the pinned Groww/NSE model values. Brokerage remains a
    deployable-current assumption across the historical research sample; only
    the legislated option-premium STT transitions are made point-in-time here.
    """

    parsed = date.fromisoformat(str(value)[:10])
    if parsed < date(2023, 4, 1):
        option_stt = 0.0005
    elif parsed < date(2024, 10, 1):
        option_stt = 0.000625
    elif parsed < date(2026, 4, 1):
        option_stt = 0.001
    else:
        option_stt = 0.0015
    base = GrowwFnoChargeRates()
    return GrowwFnoChargeRates(
        **{
            **base.__dict__,
            "options_stt_sell_rate": option_stt,
        }
    )


def estimate_basket_execution_cost(
    legs: Sequence[ExecutedLeg],
    *,
    rates: GrowwFnoChargeRates | None = None,
) -> BasketExecutionCost:
    if not legs:
        raise ValueError("at least one execution leg is required")
    for leg in legs:
        if leg.quantity <= 0:
            raise ValueError("execution quantity must be positive")
        if leg.price < 0.0 or leg.slippage_per_unit < 0.0:
            raise ValueError("price and slippage must be non-negative")
    charges = estimate_groww_fno_charges(tuple(leg.as_charge_leg() for leg in legs), rates=rates)
    slippage = sum(float(leg.quantity) * float(leg.slippage_per_unit) for leg in legs)
    return BasketExecutionCost(charges=charges, slippage=float(slippage))


def estimate_round_trip_execution_cost(
    *,
    entry_legs: Sequence[ExecutedLeg],
    exit_legs: Sequence[ExecutedLeg],
    rates: GrowwFnoChargeRates | None = None,
    entry_rates: GrowwFnoChargeRates | None = None,
    exit_rates: GrowwFnoChargeRates | None = None,
) -> RoundTripExecutionCost:
    if rates is not None and (entry_rates is not None or exit_rates is not None):
        raise ValueError("use either shared rates or entry/exit rates, not both")
    return RoundTripExecutionCost(
        entry=estimate_basket_execution_cost(entry_legs, rates=entry_rates or rates),
        exit=estimate_basket_execution_cost(exit_legs, rates=exit_rates or rates),
    )


__all__ = [
    "BasketExecutionCost",
    "ExecutedLeg",
    "RoundTripExecutionCost",
    "estimate_basket_execution_cost",
    "estimate_round_trip_execution_cost",
    "groww_fno_rates_for_date",
]
