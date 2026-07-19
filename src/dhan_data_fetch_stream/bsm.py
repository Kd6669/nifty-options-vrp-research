"""Auditable Black-Scholes-Merton pricing, IV, and Greeks for NIFTY options.

The public analysis function deliberately requires a timezone-aware, independently
verified expiry timestamp.  NIFTY expiry is accepted only at 15:30 Asia/Kolkata;
the caller remains responsible for supplying the actual exchange expiry date
(including holiday adjustments).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
from types import MappingProxyType
from typing import Any, Mapping
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
RISK_FREE_RATE = 0.10
DIVIDEND_YIELD = 0.0
ACT_365_DAYS = 365.0
MIN_IV = 1.0e-4
MAX_IV = 5.0
DEFAULT_NEAR_EXPIRY_SECONDS = 24 * 60 * 60
BSM_MODEL_VERSION = "dhan_bsm_r10_q0_act365_v1"


@dataclass(frozen=True)
class Greeks:
    delta: float
    gamma: float
    theta_per_year: float
    theta_per_day_365: float
    vega_per_1: float
    vega_per_100: float
    rho_per_1: float
    rho_per_100: float


@dataclass(frozen=True)
class IvResult:
    status: str
    reason: str | None
    implied_volatility: float | None
    model_price: float | None
    residual: float | None
    iterations: int
    no_arbitrage_lower: float | None
    no_arbitrage_upper: float | None


@dataclass(frozen=True)
class BsmAnalysis:
    status: str
    reason: str | None
    option_type: str | None
    time_to_expiry_years: float | None
    near_expiry: bool
    implied_volatility: float | None
    model_price: float | None
    residual: float | None
    greeks: Greeks | None
    no_arbitrage_lower: float | None
    no_arbitrage_upper: float | None
    iterations: int
    provider_fields: Mapping[str, Any]
    rate_cc: float = RISK_FREE_RATE
    dividend_yield: float = DIVIDEND_YIELD
    time_basis: str = "ACT/365"
    model_version: str = BSM_MODEL_VERSION
    expiry_ts: datetime | None = None
    numerical_epsilon_applied: bool = False


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def normalize_option_type(option_type: str) -> str:
    normalized = str(option_type).strip().upper()
    if normalized in {"CE", "CALL", "C"}:
        return "CALL"
    if normalized in {"PE", "PUT", "P"}:
        return "PUT"
    raise ValueError("option_type must be CALL/CE or PUT/PE")


def bsm_price(
    spot: float,
    strike: float,
    volatility: float,
    time_years: float,
    option_type: str,
    *,
    rate: float = RISK_FREE_RATE,
    dividend_yield: float = DIVIDEND_YIELD,
) -> float:
    kind = normalize_option_type(option_type)
    _validate_positive_finite("spot", spot)
    _validate_positive_finite("strike", strike)
    _validate_positive_finite("volatility", volatility)
    _validate_positive_finite("time_years", time_years)
    d1, d2 = _d1_d2(spot, strike, volatility, time_years, rate, dividend_yield)
    spot_pv = spot * math.exp(-dividend_yield * time_years)
    strike_pv = strike * math.exp(-rate * time_years)
    if kind == "CALL":
        return spot_pv * normal_cdf(d1) - strike_pv * normal_cdf(d2)
    return strike_pv * normal_cdf(-d2) - spot_pv * normal_cdf(-d1)


def no_arbitrage_bounds(
    spot: float,
    strike: float,
    time_years: float,
    option_type: str,
    *,
    rate: float = RISK_FREE_RATE,
    dividend_yield: float = DIVIDEND_YIELD,
) -> tuple[float, float]:
    kind = normalize_option_type(option_type)
    _validate_positive_finite("spot", spot)
    _validate_positive_finite("strike", strike)
    _validate_positive_finite("time_years", time_years)
    spot_pv = spot * math.exp(-dividend_yield * time_years)
    strike_pv = strike * math.exp(-rate * time_years)
    if kind == "CALL":
        return max(0.0, spot_pv - strike_pv), spot_pv
    return max(0.0, strike_pv - spot_pv), strike_pv


def bsm_greeks(
    spot: float,
    strike: float,
    volatility: float,
    time_years: float,
    option_type: str,
    *,
    rate: float = RISK_FREE_RATE,
    dividend_yield: float = DIVIDEND_YIELD,
) -> Greeks:
    kind = normalize_option_type(option_type)
    _validate_positive_finite("spot", spot)
    _validate_positive_finite("strike", strike)
    _validate_positive_finite("volatility", volatility)
    _validate_positive_finite("time_years", time_years)
    d1, d2 = _d1_d2(spot, strike, volatility, time_years, rate, dividend_yield)
    sqrt_t = math.sqrt(time_years)
    spot_discount = math.exp(-dividend_yield * time_years)
    strike_discount = math.exp(-rate * time_years)
    density = normal_pdf(d1)
    gamma = spot_discount * density / (spot * volatility * sqrt_t)
    vega = spot * spot_discount * density * sqrt_t
    common_theta = -(spot * spot_discount * density * volatility) / (2.0 * sqrt_t)
    if kind == "CALL":
        delta = spot_discount * normal_cdf(d1)
        theta = (
            common_theta
            - rate * strike * strike_discount * normal_cdf(d2)
            + dividend_yield * spot * spot_discount * normal_cdf(d1)
        )
        rho = strike * time_years * strike_discount * normal_cdf(d2)
    else:
        delta = spot_discount * (normal_cdf(d1) - 1.0)
        theta = (
            common_theta
            + rate * strike * strike_discount * normal_cdf(-d2)
            - dividend_yield * spot * spot_discount * normal_cdf(-d1)
        )
        rho = -strike * time_years * strike_discount * normal_cdf(-d2)
    return Greeks(
        delta=delta,
        gamma=gamma,
        theta_per_year=theta,
        theta_per_day_365=theta / ACT_365_DAYS,
        vega_per_1=vega,
        vega_per_100=vega / 100.0,
        rho_per_1=rho,
        rho_per_100=rho / 100.0,
    )


def solve_implied_volatility(
    *,
    spot: float,
    strike: float,
    observed_price: float,
    time_years: float,
    option_type: str,
    rate: float = RISK_FREE_RATE,
    dividend_yield: float = DIVIDEND_YIELD,
    min_iv: float = MIN_IV,
    max_iv: float = MAX_IV,
    price_tolerance: float = 1.0e-8,
    iv_tolerance: float = 1.0e-10,
    max_iterations: int = 100,
) -> IvResult:
    try:
        kind = normalize_option_type(option_type)
        _validate_positive_finite("spot", spot)
        _validate_positive_finite("strike", strike)
        _validate_positive_finite("time_years", time_years)
        _validate_positive_finite("min_iv", min_iv)
        _validate_positive_finite("max_iv", max_iv)
        if not math.isfinite(observed_price) or observed_price < 0.0:
            raise ValueError("observed_price must be finite and non-negative")
        if min_iv >= max_iv:
            raise ValueError("min_iv must be smaller than max_iv")
    except (TypeError, ValueError) as exc:
        return IvResult("invalid_input", str(exc), None, None, None, 0, None, None)

    lower, upper = no_arbitrage_bounds(
        spot, strike, time_years, kind, rate=rate, dividend_yield=dividend_yield
    )
    arb_epsilon = max(price_tolerance, 1.0e-12)
    if observed_price < lower - arb_epsilon:
        return IvResult(
            "no_arbitrage_violation",
            "observed_price_below_lower_bound",
            None,
            None,
            None,
            0,
            lower,
            upper,
        )
    if observed_price > upper + arb_epsilon:
        return IvResult(
            "no_arbitrage_violation",
            "observed_price_above_upper_bound",
            None,
            None,
            None,
            0,
            lower,
            upper,
        )

    def objective(volatility: float) -> float:
        return bsm_price(
            spot,
            strike,
            volatility,
            time_years,
            kind,
            rate=rate,
            dividend_yield=dividend_yield,
        ) - observed_price

    a, b = float(min_iv), float(max_iv)
    fa, fb = objective(a), objective(b)
    if abs(fa) <= price_tolerance:
        return IvResult("ok", None, a, fa + observed_price, fa, 0, lower, upper)
    if abs(fb) <= price_tolerance:
        return IvResult("ok", None, b, fb + observed_price, fb, 0, lower, upper)
    if fa * fb > 0.0:
        reason = "price_below_min_iv_price" if fa > 0.0 else "price_above_max_iv_price"
        return IvResult("iv_not_bracketed", reason, None, None, None, 0, lower, upper)

    # Brent-Dekker method.  It combines bisection's guaranteed bracket with
    # inverse interpolation/secant steps and is dependency-free.
    c, fc = a, fa
    d = e = b - a
    for iteration in range(1, max_iterations + 1):
        if fb * fc > 0.0:
            c, fc = a, fa
            d = e = b - a
        if abs(fc) < abs(fb):
            a, b, c = b, c, b
            fa, fb, fc = fb, fc, fb
        tolerance = 2.0 * math.ulp(1.0) * abs(b) + 0.5 * iv_tolerance
        midpoint = 0.5 * (c - b)
        if abs(midpoint) <= tolerance or abs(fb) <= price_tolerance:
            model_price = fb + observed_price
            return IvResult("ok", None, b, model_price, fb, iteration, lower, upper)
        if abs(e) >= tolerance and abs(fa) > abs(fb):
            s = fb / fa
            if a == c:
                p = 2.0 * midpoint * s
                q = 1.0 - s
            else:
                q = fa / fc
                r = fb / fc
                p = s * (2.0 * midpoint * q * (q - r) - (b - a) * (r - 1.0))
                q = (q - 1.0) * (r - 1.0) * (s - 1.0)
            if p > 0.0:
                q = -q
            else:
                p = -p
            if 2.0 * p < min(3.0 * midpoint * q - abs(tolerance * q), abs(e * q)):
                e, d = d, p / q
            else:
                d = midpoint
                e = midpoint
        else:
            d = midpoint
            e = midpoint
        a, fa = b, fb
        b += d if abs(d) > tolerance else math.copysign(tolerance, midpoint)
        fb = objective(b)
    model_price = fb + observed_price
    return IvResult(
        "solver_failed",
        "maximum_iterations_exceeded",
        None,
        model_price,
        fb,
        max_iterations,
        lower,
        upper,
    )


def analyze_option(
    *,
    spot: float,
    strike: float,
    observed_price: float,
    option_type: str,
    valuation_ts: datetime,
    expiry_ts: datetime,
    expiry_verified: bool,
    provider_fields: Mapping[str, Any] | None = None,
    rate: float = RISK_FREE_RATE,
    dividend_yield: float = DIVIDEND_YIELD,
    near_expiry_seconds: float = DEFAULT_NEAR_EXPIRY_SECONDS,
) -> BsmAnalysis:
    preserved = MappingProxyType(dict(provider_fields or {}))
    try:
        kind = normalize_option_type(option_type)
        _validate_expiry(valuation_ts, expiry_ts, expiry_verified)
    except (TypeError, ValueError) as exc:
        return BsmAnalysis(
            "invalid_input",
            str(exc),
            None,
            None,
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            preserved,
            rate_cc=rate,
            dividend_yield=dividend_yield,
            expiry_ts=expiry_ts if isinstance(expiry_ts, datetime) else None,
        )
    elapsed_seconds = (expiry_ts.astimezone(IST) - valuation_ts.astimezone(IST)).total_seconds()
    if elapsed_seconds <= 0.0:
        return BsmAnalysis(
            "post_expiry",
            "valuation_at_or_after_expiry",
            kind,
            0.0,
            True,
            None,
            None,
            None,
            None,
            None,
            None,
            0,
            preserved,
            rate_cc=rate,
            dividend_yield=dividend_yield,
            expiry_ts=expiry_ts,
        )
    time_years = elapsed_seconds / (ACT_365_DAYS * 24.0 * 60.0 * 60.0)
    near_expiry = elapsed_seconds <= float(near_expiry_seconds)
    iv = solve_implied_volatility(
        spot=spot,
        strike=strike,
        observed_price=observed_price,
        time_years=time_years,
        option_type=kind,
        rate=rate,
        dividend_yield=dividend_yield,
    )
    if iv.status != "ok" or iv.implied_volatility is None:
        return BsmAnalysis(
            iv.status,
            iv.reason,
            kind,
            time_years,
            near_expiry,
            None,
            iv.model_price,
            iv.residual,
            None,
            iv.no_arbitrage_lower,
            iv.no_arbitrage_upper,
            iv.iterations,
            preserved,
            rate_cc=rate,
            dividend_yield=dividend_yield,
            expiry_ts=expiry_ts,
        )
    greeks = bsm_greeks(
        spot,
        strike,
        iv.implied_volatility,
        time_years,
        kind,
        rate=rate,
        dividend_yield=dividend_yield,
    )
    return BsmAnalysis(
        "ok",
        None,
        kind,
        time_years,
        near_expiry,
        iv.implied_volatility,
        iv.model_price,
        iv.residual,
        greeks,
        iv.no_arbitrage_lower,
        iv.no_arbitrage_upper,
        iv.iterations,
        preserved,
        rate_cc=rate,
        dividend_yield=dividend_yield,
        expiry_ts=expiry_ts,
    )


def bsm_output_record(analysis: BsmAnalysis) -> dict[str, Any]:
    """Materialize explicit, unit-labelled BSM columns for a gold-prep row.

    Provider values remain nested under ``provider_fields`` with their original
    names; reconstructed values always use the ``bsm_`` namespace.
    """
    greeks = analysis.greeks
    signed_residual = analysis.residual
    return {
        "bsm_status": analysis.status,
        "bsm_failure_reason": analysis.reason,
        "bsm_iv_close": analysis.implied_volatility,
        "bsm_iv_unit": "decimal",
        "bsm_price_reconstructed": analysis.model_price,
        "bsm_price_residual_signed": signed_residual,
        "bsm_price_residual_abs": None if signed_residual is None else abs(signed_residual),
        "bsm_delta": None if greeks is None else greeks.delta,
        "bsm_gamma": None if greeks is None else greeks.gamma,
        "bsm_theta_per_year": None if greeks is None else greeks.theta_per_year,
        "bsm_theta_per_day_365": None if greeks is None else greeks.theta_per_day_365,
        "bsm_vega_per_1": None if greeks is None else greeks.vega_per_1,
        "bsm_vega_per_100": None if greeks is None else greeks.vega_per_100,
        "bsm_rho_per_1": None if greeks is None else greeks.rho_per_1,
        "bsm_rho_per_100": None if greeks is None else greeks.rho_per_100,
        "bsm_rate_cc": analysis.rate_cc,
        "bsm_dividend_yield": analysis.dividend_yield,
        "bsm_time_basis": analysis.time_basis,
        "bsm_expiry_ts": None if analysis.expiry_ts is None else analysis.expiry_ts.isoformat(),
        "bsm_model_version": analysis.model_version,
        "bsm_near_expiry": analysis.near_expiry,
        "bsm_numerical_epsilon_applied": analysis.numerical_epsilon_applied,
        "provider_fields": dict(analysis.provider_fields),
    }


def _d1_d2(
    spot: float,
    strike: float,
    volatility: float,
    time_years: float,
    rate: float,
    dividend_yield: float,
) -> tuple[float, float]:
    sqrt_t = math.sqrt(time_years)
    d1 = (
        math.log(spot / strike)
        + (rate - dividend_yield + 0.5 * volatility * volatility) * time_years
    ) / (volatility * sqrt_t)
    return d1, d1 - volatility * sqrt_t


def _validate_positive_finite(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0.0:
        raise ValueError(f"{name} must be finite and positive")


def _validate_expiry(valuation_ts: datetime, expiry_ts: datetime, expiry_verified: bool) -> None:
    if not isinstance(valuation_ts, datetime) or not isinstance(expiry_ts, datetime):
        raise TypeError("valuation_ts and expiry_ts must be datetime values")
    if valuation_ts.tzinfo is None or valuation_ts.utcoffset() is None:
        raise ValueError("valuation_ts must be timezone-aware")
    if expiry_ts.tzinfo is None or expiry_ts.utcoffset() is None:
        raise ValueError("expiry_ts must be timezone-aware")
    if expiry_verified is not True:
        raise ValueError("expiry date must be independently verified")
    expiry_ist = expiry_ts.astimezone(IST)
    if (expiry_ist.hour, expiry_ist.minute, expiry_ist.second, expiry_ist.microsecond) != (15, 30, 0, 0):
        raise ValueError("verified expiry timestamp must be exactly 15:30:00 Asia/Kolkata")
