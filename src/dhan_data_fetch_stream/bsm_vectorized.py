"""Compiled, vectorized Black-Scholes-Merton analysis for monthly Arrow batches.

The whole eligible population is solved with bounded NumPy Newton iterations.
Only rows which remain bracketed but have not converged are sent to the audited
scalar Brent-Dekker implementation in :mod:`dhan_data_fetch_stream.bsm`.
"""

from __future__ import annotations

from dataclasses import dataclass
import math

import numba
import numpy as np

from .bsm import (
    ACT_365_DAYS,
    DIVIDEND_YIELD,
    MAX_IV,
    MIN_IV,
    RISK_FREE_RATE,
    solve_implied_volatility,
)


BSM_V2_MODEL_VERSION = "dhan_bsm_r10_q0_act365_vectorized_v2"


@numba.vectorize([numba.float64(numba.float64)], nopython=True, cache=True)
def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


@dataclass(frozen=True)
class VectorizedBsmConfig:
    """Numerical contract for the vectorized independent IV solver."""

    rate: float = RISK_FREE_RATE
    dividend_yield: float = DIVIDEND_YIELD
    min_iv: float = MIN_IV
    max_iv: float = MAX_IV
    initial_iv: float = 0.20
    price_tolerance: float = 1.0e-8
    iv_tolerance: float = 1.0e-10
    max_newton_iterations: int = 20
    max_brent_iterations: int = 100
    min_vega: float = 1.0e-12
    near_expiry_seconds: float = 24.0 * 60.0 * 60.0


@dataclass(frozen=True)
class VectorizedBsmResult:
    """Column arrays ready to append to an Arrow monthly partition."""

    columns: dict[str, np.ndarray]


def solve_bsm_vectorized(
    *,
    spot: np.ndarray,
    strike: np.ndarray,
    observed_price: np.ndarray,
    time_years: np.ndarray,
    is_call: np.ndarray,
    base_eligible: np.ndarray | None = None,
    base_failure_reason: np.ndarray | None = None,
    config: VectorizedBsmConfig | None = None,
) -> VectorizedBsmResult:
    """Solve independent IV and Greeks without a row-wise all-population UDF.

    ``is_call`` must be boolean. Invalid option-type rows should be excluded by
    ``base_eligible`` and supplied with an explicit ``base_failure_reason``.
    Volatility is decimal, vega/rho are emitted per 1.00 and per 0.01 change,
    and theta is emitted per ACT/365 year and per calendar day.
    """
    cfg = config or VectorizedBsmConfig()
    s = np.asarray(spot, dtype=np.float64)
    k = np.asarray(strike, dtype=np.float64)
    price = np.asarray(observed_price, dtype=np.float64)
    t = np.asarray(time_years, dtype=np.float64)
    call = np.asarray(is_call, dtype=np.bool_)
    _validate_equal_lengths(s, k, price, t, call)
    size = len(s)
    eligible = (
        np.ones(size, dtype=np.bool_)
        if base_eligible is None
        else np.asarray(base_eligible, dtype=np.bool_).copy()
    )
    if len(eligible) != size:
        raise ValueError("base_eligible length does not match inputs")
    reasons = np.full(size, None, dtype=object)
    if base_failure_reason is not None:
        supplied = np.asarray(base_failure_reason, dtype=object)
        if len(supplied) != size:
            raise ValueError("base_failure_reason length does not match inputs")
        reasons[:] = supplied

    finite = np.isfinite(s) & np.isfinite(k) & np.isfinite(price) & np.isfinite(t)
    positive = (s > 0.0) & (k > 0.0) & (price >= 0.0) & (t > 0.0)
    invalid_numeric = eligible & ~finite
    reasons[invalid_numeric] = "non_finite_bsm_input"
    invalid_positive = eligible & finite & ~positive
    reasons[invalid_positive & (t <= 0.0)] = "non_positive_or_post_expiry_time"
    reasons[invalid_positive & (t > 0.0)] = "non_positive_bsm_input"
    eligible &= finite & positive

    lower = np.full(size, np.nan)
    upper = np.full(size, np.nan)
    if np.any(eligible):
        spot_pv = s[eligible] * np.exp(-cfg.dividend_yield * t[eligible])
        strike_pv = k[eligible] * np.exp(-cfg.rate * t[eligible])
        lower[eligible] = np.where(
            call[eligible],
            np.maximum(0.0, spot_pv - strike_pv),
            np.maximum(0.0, strike_pv - spot_pv),
        )
        upper[eligible] = np.where(call[eligible], spot_pv, strike_pv)
    epsilon = max(cfg.price_tolerance, 1.0e-12)
    below = eligible & (price < lower - epsilon)
    above = eligible & (price > upper + epsilon)
    reasons[below] = "observed_price_below_lower_bound"
    reasons[above] = "observed_price_above_upper_bound"
    arb_valid = eligible & ~below & ~above

    iv = np.full(size, np.nan)
    model_price = np.full(size, np.nan)
    residual = np.full(size, np.nan)
    iterations = np.zeros(size, dtype=np.int32)
    method = np.full(size, "none", dtype=object)
    converged = np.zeros(size, dtype=np.bool_)
    active = arb_valid.copy()
    iv[active] = np.clip(cfg.initial_iv, cfg.min_iv, cfg.max_iv)

    # Each iteration evaluates every still-active row in compiled/vectorized
    # kernels. Bounds are maintained so Newton cannot escape the IV contract.
    for iteration in range(1, cfg.max_newton_iterations + 1):
        indices = np.flatnonzero(active)
        if indices.size == 0:
            break
        p, vega, *_ = _price_and_greeks_arrays(
            s[indices], k[indices], iv[indices], t[indices], call[indices], cfg
        )
        error = p - price[indices]
        model_price[indices] = p
        residual[indices] = error
        iterations[indices] = iteration
        done = np.abs(error) <= cfg.price_tolerance
        if np.any(done):
            done_indices = indices[done]
            converged[done_indices] = True
            method[done_indices] = "newton"
            reasons[done_indices] = None
            active[done_indices] = False
        remaining = ~done
        if not np.any(remaining):
            continue
        rem_indices = indices[remaining]
        rem_error = error[remaining]
        rem_vega = vega[remaining]
        safe = np.isfinite(rem_vega) & (np.abs(rem_vega) >= cfg.min_vega)
        if np.any(safe):
            candidate = iv[rem_indices[safe]] - rem_error[safe] / rem_vega[safe]
            candidate = np.clip(candidate, cfg.min_iv, cfg.max_iv)
            stalled = np.abs(candidate - iv[rem_indices[safe]]) <= cfg.iv_tolerance
            iv[rem_indices[safe]] = candidate
            active[rem_indices[safe][stalled]] = False
        active[rem_indices[~safe]] = False

    # Brent is deliberately the sparse fallback, not an all-row Python UDF.
    fallback = arb_valid & ~converged
    for index in np.flatnonzero(fallback):
        answer = solve_implied_volatility(
            spot=float(s[index]),
            strike=float(k[index]),
            observed_price=float(price[index]),
            time_years=float(t[index]),
            option_type="CALL" if call[index] else "PUT",
            rate=cfg.rate,
            dividend_yield=cfg.dividend_yield,
            min_iv=cfg.min_iv,
            max_iv=cfg.max_iv,
            price_tolerance=cfg.price_tolerance,
            iv_tolerance=cfg.iv_tolerance,
            max_iterations=cfg.max_brent_iterations,
        )
        method[index] = "brent"
        iterations[index] += answer.iterations
        if answer.status == "ok" and answer.implied_volatility is not None:
            converged[index] = True
            iv[index] = answer.implied_volatility
            model_price[index] = float(answer.model_price)
            residual[index] = float(answer.residual)
            reasons[index] = None
        else:
            iv[index] = np.nan
            model_price[index] = np.nan if answer.model_price is None else answer.model_price
            residual[index] = np.nan if answer.residual is None else answer.residual
            reasons[index] = answer.reason or answer.status

    status = np.full(size, "blocked", dtype=object)
    status[invalid_numeric | invalid_positive] = "invalid_input"
    status[below | above] = "no_arbitrage_violation"
    status[arb_valid & ~converged] = "iv_solver_failed"
    status[converged] = "ok"
    unresolved_reason = (reasons == None) & ~converged  # noqa: E711
    reasons[unresolved_reason] = "pre_bsm_eligibility_failed"

    delta = np.full(size, np.nan)
    gamma = np.full(size, np.nan)
    theta_year = np.full(size, np.nan)
    vega_per_1 = np.full(size, np.nan)
    rho_per_1 = np.full(size, np.nan)
    if np.any(converged):
        indices = np.flatnonzero(converged)
        (
            final_price,
            final_vega,
            final_delta,
            final_gamma,
            final_theta,
            final_rho,
        ) = _price_and_greeks_arrays(
            s[indices], k[indices], iv[indices], t[indices], call[indices], cfg
        )
        model_price[indices] = final_price
        residual[indices] = final_price - price[indices]
        delta[indices] = final_delta
        gamma[indices] = final_gamma
        theta_year[indices] = final_theta
        vega_per_1[indices] = final_vega
        rho_per_1[indices] = final_rho

    near_expiry = np.isfinite(t) & (t > 0.0) & (
        t * ACT_365_DAYS * 24.0 * 60.0 * 60.0 <= cfg.near_expiry_seconds
    )
    columns = {
        "bsm_status": status,
        "bsm_failure_reason": reasons,
        "bsm_solver_method": method,
        "bsm_solver_iterations": iterations,
        "bsm_solver_converged": converged,
        "bsm_iv_close": iv,
        "bsm_iv_unit": np.full(size, "decimal", dtype=object),
        "bsm_price_input_field": np.full(size, "close", dtype=object),
        "bsm_price_reconstructed": model_price,
        "bsm_price_residual_signed": residual,
        "bsm_price_residual_abs": np.abs(residual),
        "bsm_no_arbitrage_lower": lower,
        "bsm_no_arbitrage_upper": upper,
        "bsm_delta": delta,
        "bsm_gamma": gamma,
        "bsm_theta_per_year": theta_year,
        "bsm_theta_per_day_365": theta_year / ACT_365_DAYS,
        "bsm_vega_per_1": vega_per_1,
        "bsm_vega_per_100": vega_per_1 / 100.0,
        "bsm_rho_per_1": rho_per_1,
        "bsm_rho_per_100": rho_per_1 / 100.0,
        "bsm_rate_cc": np.full(size, cfg.rate),
        "bsm_dividend_yield": np.full(size, cfg.dividend_yield),
        "bsm_time_basis": np.full(size, "ACT/365", dtype=object),
        "bsm_near_expiry": near_expiry,
        "bsm_model_version": np.full(size, BSM_V2_MODEL_VERSION, dtype=object),
    }
    return VectorizedBsmResult(columns)


def _price_and_greeks_arrays(
    spot: np.ndarray,
    strike: np.ndarray,
    volatility: np.ndarray,
    time_years: np.ndarray,
    is_call: np.ndarray,
    config: VectorizedBsmConfig,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    sqrt_t = np.sqrt(time_years)
    spot_discount = np.exp(-config.dividend_yield * time_years)
    strike_discount = np.exp(-config.rate * time_years)
    d1 = (
        np.log(spot / strike)
        + (config.rate - config.dividend_yield + 0.5 * volatility * volatility) * time_years
    ) / (volatility * sqrt_t)
    d2 = d1 - volatility * sqrt_t
    cdf_d1 = _normal_cdf(d1)
    cdf_d2 = _normal_cdf(d2)
    cdf_neg_d1 = _normal_cdf(-d1)
    cdf_neg_d2 = _normal_cdf(-d2)
    density = np.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
    spot_pv = spot * spot_discount
    strike_pv = strike * strike_discount
    price = np.where(
        is_call,
        spot_pv * cdf_d1 - strike_pv * cdf_d2,
        strike_pv * cdf_neg_d2 - spot_pv * cdf_neg_d1,
    )
    gamma = spot_discount * density / (spot * volatility * sqrt_t)
    vega = spot * spot_discount * density * sqrt_t
    common_theta = -(spot * spot_discount * density * volatility) / (2.0 * sqrt_t)
    theta = np.where(
        is_call,
        common_theta
        - config.rate * strike * strike_discount * cdf_d2
        + config.dividend_yield * spot * spot_discount * cdf_d1,
        common_theta
        + config.rate * strike * strike_discount * cdf_neg_d2
        - config.dividend_yield * spot * spot_discount * cdf_neg_d1,
    )
    delta = np.where(
        is_call,
        spot_discount * cdf_d1,
        spot_discount * (cdf_d1 - 1.0),
    )
    rho = np.where(
        is_call,
        strike * time_years * strike_discount * cdf_d2,
        -strike * time_years * strike_discount * cdf_neg_d2,
    )
    return price, vega, delta, gamma, theta, rho


def _validate_equal_lengths(*arrays: np.ndarray) -> None:
    lengths = {len(array) for array in arrays}
    if len(lengths) != 1:
        raise ValueError(f"all input arrays must have equal length; got {sorted(lengths)}")
