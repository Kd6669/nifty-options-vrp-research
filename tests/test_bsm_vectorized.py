from __future__ import annotations

import math

import numpy as np

from dhan_data_fetch_stream.bsm import bsm_greeks, bsm_price
from dhan_data_fetch_stream.bsm_vectorized import (
    VectorizedBsmConfig,
    solve_bsm_vectorized,
)


def test_vectorized_solver_matches_scalar_call_and_put_formulas() -> None:
    rng = np.random.default_rng(20260716)
    size = 100
    spot = rng.uniform(18_000.0, 28_000.0, size)
    # Keep fixtures numerically identifiable at the requested price tolerance;
    # ultra-deep options can have indistinguishable prices across many IVs.
    strike = spot * rng.uniform(0.9, 1.1, size)
    time_years = rng.uniform(0.03, 0.3, size)
    true_iv = rng.uniform(0.06, 1.25, size)
    is_call = np.arange(size) % 2 == 0
    observed = np.array(
        [
            bsm_price(s, k, iv, t, "CALL" if call else "PUT")
            for s, k, iv, t, call in zip(
                spot, strike, true_iv, time_years, is_call, strict=True
            )
        ]
    )

    result = solve_bsm_vectorized(
        spot=spot,
        strike=strike,
        observed_price=observed,
        time_years=time_years,
        is_call=is_call,
    ).columns

    assert set(result["bsm_status"]) == {"ok"}
    np.testing.assert_allclose(result["bsm_iv_close"], true_iv, rtol=0.0, atol=2.0e-8)
    np.testing.assert_allclose(result["bsm_price_reconstructed"], observed, atol=1.0e-7)
    for index in (0, 1, 48, 99):
        scalar = bsm_greeks(
            spot[index],
            strike[index],
            result["bsm_iv_close"][index],
            time_years[index],
            "CALL" if is_call[index] else "PUT",
        )
        assert math.isclose(result["bsm_delta"][index], scalar.delta, abs_tol=1.0e-12)
        assert math.isclose(result["bsm_gamma"][index], scalar.gamma, abs_tol=1.0e-12)
        assert math.isclose(
            result["bsm_theta_per_year"][index], scalar.theta_per_year, abs_tol=1.0e-10
        )
        assert math.isclose(result["bsm_vega_per_1"][index], scalar.vega_per_1, abs_tol=1.0e-10)
        assert math.isclose(result["bsm_rho_per_1"][index], scalar.rho_per_1, abs_tol=1.0e-10)


def test_only_newton_nonconvergence_uses_brent_fallback() -> None:
    price = bsm_price(25_000.0, 27_000.0, 0.85, 0.05, "CALL")
    result = solve_bsm_vectorized(
        spot=np.array([25_000.0]),
        strike=np.array([27_000.0]),
        observed_price=np.array([price]),
        time_years=np.array([0.05]),
        is_call=np.array([True]),
        config=VectorizedBsmConfig(max_newton_iterations=1),
    ).columns

    assert result["bsm_status"][0] == "ok"
    assert result["bsm_solver_method"][0] == "brent"
    assert result["bsm_solver_converged"][0]
    assert math.isclose(result["bsm_iv_close"][0], 0.85, abs_tol=1.0e-9)


def test_explicit_input_no_arbitrage_and_pre_bsm_gates() -> None:
    result = solve_bsm_vectorized(
        spot=np.array([100.0, 100.0, 100.0, 100.0]),
        strike=np.array([100.0, 100.0, 100.0, 100.0]),
        observed_price=np.array([101.0, 5.0, 5.0, 5.0]),
        time_years=np.array([0.1, 0.0, 0.1, 0.1]),
        is_call=np.array([True, True, True, True]),
        base_eligible=np.array([True, True, False, False]),
        base_failure_reason=np.array(
            [None, None, "independent_nifty_spot_unavailable", "india_vix_source_unavailable"],
            dtype=object,
        ),
    ).columns

    assert list(result["bsm_status"]) == [
        "no_arbitrage_violation",
        "invalid_input",
        "blocked",
        "blocked",
    ]
    assert result["bsm_failure_reason"][0] == "observed_price_above_upper_bound"
    assert result["bsm_failure_reason"][1] == "non_positive_or_post_expiry_time"
    assert result["bsm_failure_reason"][2] == "independent_nifty_spot_unavailable"
    assert result["bsm_failure_reason"][3] == "india_vix_source_unavailable"
    assert not np.any(result["bsm_solver_converged"])


def test_unit_labels_and_near_expiry_outputs_are_unambiguous() -> None:
    time_years = 30.0 / (365.0 * 24.0 * 60.0)
    price = bsm_price(25_000.0, 25_000.0, 0.2, time_years, "PUT")
    result = solve_bsm_vectorized(
        spot=np.array([25_000.0]),
        strike=np.array([25_000.0]),
        observed_price=np.array([price]),
        time_years=np.array([time_years]),
        is_call=np.array([False]),
    ).columns

    assert result["bsm_near_expiry"][0]
    assert result["bsm_iv_unit"][0] == "decimal"
    assert result["bsm_price_input_field"][0] == "close"
    assert result["bsm_time_basis"][0] == "ACT/365"
    assert result["bsm_vega_per_100"][0] == result["bsm_vega_per_1"][0] / 100.0
    assert result["bsm_rho_per_100"][0] == result["bsm_rho_per_1"][0] / 100.0
    assert result["bsm_theta_per_day_365"][0] == result["bsm_theta_per_year"][0] / 365.0
