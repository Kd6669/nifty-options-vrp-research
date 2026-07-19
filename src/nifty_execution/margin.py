from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping, Sequence

from nifty_span.span import SpanData, SpanMarginBreakdown, margin_for_candidate_legs


def estimate_defined_risk_margin(
    *,
    legs: Sequence[Mapping[str, Any]],
    span_data: SpanData,
    expiry: str,
    spot: float,
    eval_dt: datetime,
    prev_close_spot: float | None = None,
) -> SpanMarginBreakdown:
    """Research-facing NIFTY adapter for the pinned SPAN Model-A engine."""

    return margin_for_candidate_legs(
        legs=legs,
        span_data=span_data,
        index="NIFTY",
        expiry=expiry,
        spot=spot,
        eval_dt=eval_dt,
        prev_close_spot=prev_close_spot,
    )


def return_on_margin(*, net_pnl: float, margin: float) -> float:
    if margin <= 0.0:
        raise ValueError("margin must be positive")
    return float(net_pnl) / float(margin)


__all__ = ["estimate_defined_risk_margin", "return_on_margin"]
