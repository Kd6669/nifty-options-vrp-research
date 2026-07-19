from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
import math


DEFAULT_NSE_FNO_TICK_SIZE = 0.05


def round_broker_limit_price(
    price: float,
    side: str | None = None,
    *,
    tick_size: float = DEFAULT_NSE_FNO_TICK_SIZE,
) -> float:
    """Round a limit price to the broker/NSE F&O tick size."""

    value = float(price)
    tick = float(tick_size)
    if value <= 0.0 or not math.isfinite(value):
        raise ValueError("limit price must be a positive finite number")
    if tick <= 0.0 or not math.isfinite(tick):
        raise ValueError("tick_size must be positive and finite")

    decimal_price = Decimal(str(value))
    decimal_tick = Decimal(str(tick))
    units = decimal_price / decimal_tick
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "SELL":
        rounded_units = units.to_integral_value(rounding=ROUND_CEILING)
    elif normalized_side == "BUY":
        rounded_units = units.to_integral_value(rounding=ROUND_FLOOR)
    else:
        rounded_units = units.to_integral_value(rounding=ROUND_HALF_UP)
    return float(rounded_units * decimal_tick)
