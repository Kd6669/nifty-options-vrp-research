from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Iterable


@dataclass(frozen=True)
class NiftySlippageParameters:
    """Pinned NIFTY calibration from deployment-live-model.

    Rates are decimal fractions. ``base_bps`` retains the upstream name even
    though the stored value is applied directly as a decimal rate.
    """

    base_bps: float = 0.001599
    gamma_constant: float = 0.045543
    depth_constant: float = 1.501812
    vix_convexity: float = 1.5
    baseline_vix: float = 15.0
    min_tick: float = 0.05
    stale_turnover_threshold: float = 0.001
    stale_multiplier: float = 1.5
    minimum_hours_left: float = 0.1
    minimum_log_oi: float = 0.1


@dataclass(frozen=True)
class SlippageBreakdown:
    close: float
    volume: float
    open_interest: float
    minutes_to_expiry: float
    india_vix: float
    turnover_ratio: float
    vix_multiplier: float
    time_multiplier: float
    stale_multiplier: float
    depth_multiplier: float
    base_spread: float
    time_penalty: float
    stale_penalty: float
    depth_penalty: float
    slippage_per_unit: float
    bid_proxy: float
    ask_proxy: float

    @property
    def component_sum(self) -> float:
        return self.base_spread + self.time_penalty + self.stale_penalty + self.depth_penalty

    @property
    def is_executable_proxy(self) -> bool:
        return self.bid_proxy >= 0.0 and self.slippage_per_unit < self.close

    def to_dict(self) -> dict[str, float | bool]:
        payload: dict[str, float | bool] = {
            key: float(value) for key, value in asdict(self).items()
        }
        payload["component_sum"] = float(self.component_sum)
        payload["is_executable_proxy"] = self.is_executable_proxy
        return payload


@dataclass(frozen=True)
class ParticipationImpactParameters:
    """Transparent capacity sensitivity around the pinned one-lot spread.

    The deterministic ladder is anchored so its *added* impact equals the
    pinned one-lot slippage at ``ladder_parity_lots``.  Volume and OI enter as
    separate square-root participation terms on quantity above the first lot;
    they no longer multiply and artificially amplify the ladder.

    This remains an assumption-driven sensitivity until order-book or realized
    fill data are available.  The 60-lot default records the research capacity
    anchor explicitly instead of inheriting the old ten-percent-per-lot
    simulator placeholder.
    """

    ladder_parity_lots: float = 60.0
    volume_participation_weight: float = 1.0
    oi_participation_weight: float = 1.0
    participation_exponent: float = 0.5


@dataclass(frozen=True)
class ParticipationImpactBreakdown:
    quantity: int
    lot_size: int
    lots: float
    volume: float
    open_interest: float
    volume_participation: float
    oi_participation: float
    incremental_quantity: int
    incremental_volume_participation: float
    incremental_oi_participation: float
    liquidity_multiplier: float
    average_incremental_lot_steps: float
    ladder_impact_ratio: float
    volume_impact_ratio: float
    oi_impact_ratio: float
    participation_impact_ratio: float
    total_impact_ratio: float
    base_slippage_per_unit: float
    ladder_impact_per_unit: float
    volume_impact_per_unit: float
    oi_impact_per_unit: float
    impact_per_unit: float
    adjusted_slippage_per_unit: float

    def to_dict(self) -> dict[str, float | int]:
        return asdict(self)


def estimate_participation_impact(
    *,
    base_slippage_per_unit: float,
    quantity: int,
    lot_size: int,
    volume: float | None,
    open_interest: float | None,
    parameters: ParticipationImpactParameters | None = None,
) -> ParticipationImpactBreakdown:
    """Scale modeled slippage for order size, minute volume, and contract OI.

    The first lot retains the pinned one-lot slippage.  Above one lot, added
    impact is the sum of:

    ``(lots - 1) / (ladder_parity_lots - 1)``
        A slow deterministic ladder, equal to one base-slippage unit at the
        disclosed parity anchor.

    ``weight * incremental_participation ** exponent``
        Separate minute-volume and OI capacity terms based only on quantity
        above the already-priced first lot.
    """

    if not math.isfinite(float(base_slippage_per_unit)) or base_slippage_per_unit < 0:
        raise ValueError("base_slippage_per_unit must be finite and non-negative")
    if quantity <= 0 or lot_size <= 0:
        raise ValueError("quantity and lot_size must be positive")
    params = parameters or ParticipationImpactParameters()
    if not math.isfinite(params.ladder_parity_lots) or params.ladder_parity_lots <= 1.0:
        raise ValueError("ladder_parity_lots must be finite and greater than one")
    if params.volume_participation_weight < 0 or params.oi_participation_weight < 0:
        raise ValueError("participation weights must be non-negative")
    if not 0.0 < params.participation_exponent <= 1.0:
        raise ValueError("participation_exponent must be in (0, 1]")
    volume_value = max(_finite_or(volume, 0.0), 0.0)
    oi_value = max(_finite_or(open_interest, 0.0), 0.0)
    lots = float(quantity) / float(lot_size)
    volume_participation = min(float(quantity) / max(volume_value, 1.0), 1.0)
    oi_participation = min(float(quantity) / max(oi_value, 1.0), 1.0)
    incremental_quantity = max(int(quantity) - int(lot_size), 0)
    incremental_volume_participation = min(
        float(incremental_quantity) / max(volume_value, 1.0), 1.0
    )
    incremental_oi_participation = min(float(incremental_quantity) / max(oi_value, 1.0), 1.0)
    ladder_impact_ratio = max(lots - 1.0, 0.0) / (params.ladder_parity_lots - 1.0)
    volume_impact_ratio = params.volume_participation_weight * (
        incremental_volume_participation**params.participation_exponent
    )
    oi_impact_ratio = params.oi_participation_weight * (
        incremental_oi_participation**params.participation_exponent
    )
    participation_impact_ratio = volume_impact_ratio + oi_impact_ratio
    total_impact_ratio = ladder_impact_ratio + participation_impact_ratio
    liquidity_multiplier = 1.0 + participation_impact_ratio
    average_steps = max(lots - 1.0, 0.0) / 2.0
    base_slippage = float(base_slippage_per_unit)
    ladder_impact = base_slippage * ladder_impact_ratio
    volume_impact = base_slippage * volume_impact_ratio
    oi_impact = base_slippage * oi_impact_ratio
    impact = ladder_impact + volume_impact + oi_impact
    return ParticipationImpactBreakdown(
        quantity=int(quantity),
        lot_size=int(lot_size),
        lots=lots,
        volume=volume_value,
        open_interest=oi_value,
        volume_participation=volume_participation,
        oi_participation=oi_participation,
        incremental_quantity=incremental_quantity,
        incremental_volume_participation=incremental_volume_participation,
        incremental_oi_participation=incremental_oi_participation,
        liquidity_multiplier=liquidity_multiplier,
        average_incremental_lot_steps=average_steps,
        ladder_impact_ratio=ladder_impact_ratio,
        volume_impact_ratio=volume_impact_ratio,
        oi_impact_ratio=oi_impact_ratio,
        participation_impact_ratio=participation_impact_ratio,
        total_impact_ratio=total_impact_ratio,
        base_slippage_per_unit=base_slippage,
        ladder_impact_per_unit=ladder_impact,
        volume_impact_per_unit=volume_impact,
        oi_impact_per_unit=oi_impact,
        impact_per_unit=float(impact),
        adjusted_slippage_per_unit=base_slippage + float(impact),
    )


def estimate_nifty_option_slippage(
    *,
    close: float,
    volume: float | None,
    open_interest: float | None,
    minutes_to_expiry: float | None,
    india_vix: float | None,
    parameters: NiftySlippageParameters | None = None,
) -> SlippageBreakdown:
    """Estimate one-sided adverse fill distance from the observed close.

    Missing volume/OI are conservatively mapped to zero, missing time to the
    upstream near-expiry floor, and missing VIX to the calibration baseline.
    The returned bid/ask are synthetic audit proxies, not market quotes.
    """

    params = parameters or NiftySlippageParameters()
    close_value = _finite_or(close, float("nan"))
    if not math.isfinite(close_value) or close_value <= 0.0:
        raise ValueError("close must be finite and positive")

    volume_value = max(_finite_or(volume, 0.0), 0.0)
    oi_value = max(_finite_or(open_interest, 0.0), 0.0)
    minutes_value = max(_finite_or(minutes_to_expiry, 0.0), 0.0)
    vix_value = max(_finite_or(india_vix, params.baseline_vix), 0.0)

    vix_multiplier = (vix_value / params.baseline_vix) ** params.vix_convexity
    base_spread = max(params.min_tick, close_value * params.base_bps * vix_multiplier)
    hours_left = max(minutes_value / 60.0, params.minimum_hours_left)
    time_multiplier = 1.0 + params.gamma_constant / math.sqrt(hours_left)
    turnover_ratio = volume_value / (oi_value + 1.0)
    stale_multiplier = (
        params.stale_multiplier if turnover_ratio < params.stale_turnover_threshold else 1.0
    )
    log_oi = max(math.log(oi_value + 1.0), params.minimum_log_oi)
    depth_multiplier = 1.0 + params.depth_constant / log_oi

    after_time = base_spread * time_multiplier
    after_stale = after_time * stale_multiplier
    total = after_stale * depth_multiplier
    time_penalty = after_time - base_spread
    stale_penalty = after_stale - after_time
    depth_penalty = total - after_stale

    return SlippageBreakdown(
        close=close_value,
        volume=volume_value,
        open_interest=oi_value,
        minutes_to_expiry=minutes_value,
        india_vix=vix_value,
        turnover_ratio=turnover_ratio,
        vix_multiplier=vix_multiplier,
        time_multiplier=time_multiplier,
        stale_multiplier=stale_multiplier,
        depth_multiplier=depth_multiplier,
        base_spread=base_spread,
        time_penalty=time_penalty,
        stale_penalty=stale_penalty,
        depth_penalty=depth_penalty,
        slippage_per_unit=total,
        bid_proxy=close_value - total,
        ask_proxy=close_value + total,
    )


def estimate_nifty_option_slippage_many(
    rows: Iterable[dict[str, float | None]],
    *,
    parameters: NiftySlippageParameters | None = None,
) -> tuple[SlippageBreakdown, ...]:
    return tuple(
        estimate_nifty_option_slippage(
            close=float(row["close"]),
            volume=row.get("volume"),
            open_interest=row.get("open_interest"),
            minutes_to_expiry=row.get("minutes_to_expiry"),
            india_vix=row.get("india_vix"),
            parameters=parameters,
        )
        for row in rows
    )


def _finite_or(value: float | None, fallback: float) -> float:
    if value is None:
        return fallback
    parsed = float(value)
    return parsed if math.isfinite(parsed) else fallback


__all__ = [
    "NiftySlippageParameters",
    "ParticipationImpactBreakdown",
    "ParticipationImpactParameters",
    "SlippageBreakdown",
    "estimate_participation_impact",
    "estimate_nifty_option_slippage",
    "estimate_nifty_option_slippage_many",
]
