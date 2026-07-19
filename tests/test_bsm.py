from __future__ import annotations

from datetime import datetime, timedelta
import math
import unittest
from zoneinfo import ZoneInfo

from dhan_data_fetch_stream.bsm import (
    IST,
    analyze_option,
    bsm_greeks,
    bsm_output_record,
    bsm_price,
    no_arbitrage_bounds,
    solve_implied_volatility,
)


class BsmTests(unittest.TestCase):
    def test_call_put_prices_and_parity(self) -> None:
        call = bsm_price(100.0, 100.0, 0.2, 1.0, "CE")
        put = bsm_price(100.0, 100.0, 0.2, 1.0, "PE")

        self.assertAlmostEqual(call, 13.2696765847, places=8)
        self.assertAlmostEqual(put, 3.7534183885, places=8)
        self.assertAlmostEqual(call - put, 100.0 - 100.0 * math.exp(-0.10), places=10)

    def test_call_put_greeks_have_expected_signs_and_units(self) -> None:
        call = bsm_greeks(100.0, 100.0, 0.2, 1.0, "CALL")
        put = bsm_greeks(100.0, 100.0, 0.2, 1.0, "PUT")

        self.assertGreater(call.delta, 0.0)
        self.assertLess(put.delta, 0.0)
        self.assertAlmostEqual(call.gamma, put.gamma, places=12)
        self.assertAlmostEqual(call.vega_per_100, call.vega_per_1 / 100.0)
        self.assertAlmostEqual(put.rho_per_100, put.rho_per_1 / 100.0)
        self.assertAlmostEqual(call.theta_per_day_365, call.theta_per_year / 365.0)

    def test_brent_solver_recovers_decimal_iv(self) -> None:
        market = bsm_price(25000.0, 25100.0, 0.1735, 12.0 / 365.0, "CALL")

        result = solve_implied_volatility(
            spot=25000.0,
            strike=25100.0,
            observed_price=market,
            time_years=12.0 / 365.0,
            option_type="CE",
        )

        self.assertEqual(result.status, "ok")
        self.assertAlmostEqual(result.implied_volatility or 0.0, 0.1735, places=8)
        self.assertAlmostEqual(result.residual or 0.0, 0.0, places=7)

    def test_no_arbitrage_violation_is_explicit(self) -> None:
        lower, _ = no_arbitrage_bounds(100.0, 50.0, 1.0, "CALL")
        result = solve_implied_volatility(
            spot=100.0,
            strike=50.0,
            observed_price=lower - 1.0,
            time_years=1.0,
            option_type="CALL",
        )

        self.assertEqual(result.status, "no_arbitrage_violation")
        self.assertEqual(result.reason, "observed_price_below_lower_bound")
        self.assertIsNone(result.implied_volatility)

    def test_analysis_requires_verified_1530_ist_expiry(self) -> None:
        valuation = datetime(2026, 7, 14, 10, 0, tzinfo=IST)
        wrong_expiry = datetime(2026, 7, 14, 15, 29, tzinfo=IST)

        result = analyze_option(
            spot=25000.0,
            strike=25000.0,
            observed_price=100.0,
            option_type="CALL",
            valuation_ts=valuation,
            expiry_ts=wrong_expiry,
            expiry_verified=True,
        )

        self.assertEqual(result.status, "invalid_input")
        self.assertIn("15:30", result.reason or "")

    def test_analysis_preserves_provider_fields_and_marks_near_expiry(self) -> None:
        expiry = datetime(2026, 7, 14, 15, 30, tzinfo=IST)
        valuation = expiry - timedelta(hours=2)
        price = bsm_price(25000.0, 25000.0, 0.20, 2.0 / (365.0 * 24.0), "PUT")
        provider = {"provider_iv": 18.7, "provider_delta": -0.4}

        result = analyze_option(
            spot=25000.0,
            strike=25000.0,
            observed_price=price,
            option_type="PE",
            valuation_ts=valuation,
            expiry_ts=expiry,
            expiry_verified=True,
            provider_fields=provider,
        )
        provider["provider_iv"] = 99.0

        self.assertEqual(result.status, "ok")
        self.assertTrue(result.near_expiry)
        self.assertAlmostEqual(result.implied_volatility or 0.0, 0.20, places=7)
        self.assertEqual(result.provider_fields["provider_iv"], 18.7)
        self.assertIsNotNone(result.greeks)

    def test_no_post_expiry_calculation(self) -> None:
        expiry = datetime(2026, 7, 14, 15, 30, tzinfo=IST)
        result = analyze_option(
            spot=25000.0,
            strike=25000.0,
            observed_price=1.0,
            option_type="CALL",
            valuation_ts=expiry,
            expiry_ts=expiry,
            expiry_verified=True,
        )

        self.assertEqual(result.status, "post_expiry")
        self.assertIsNone(result.implied_volatility)
        self.assertIsNone(result.greeks)

    def test_utc_expiry_equivalent_to_1530_ist_is_accepted(self) -> None:
        utc = ZoneInfo("UTC")
        expiry = datetime(2026, 7, 14, 10, 0, tzinfo=utc)
        valuation = datetime(2026, 7, 14, 9, 0, tzinfo=utc)
        price = bsm_price(100.0, 100.0, 0.2, 1.0 / (365.0 * 24.0), "CALL")

        result = analyze_option(
            spot=100.0,
            strike=100.0,
            observed_price=price,
            option_type="CALL",
            valuation_ts=valuation,
            expiry_ts=expiry,
            expiry_verified=True,
        )

        self.assertEqual(result.status, "ok")

    def test_output_record_has_explicit_model_metadata_and_separate_provider_fields(self) -> None:
        expiry = datetime(2026, 7, 14, 15, 30, tzinfo=IST)
        valuation = expiry - timedelta(days=2)
        price = bsm_price(25000.0, 25100.0, 0.18, 2.0 / 365.0, "CALL")
        analysis = analyze_option(
            spot=25000.0,
            strike=25100.0,
            observed_price=price,
            option_type="CALL",
            valuation_ts=valuation,
            expiry_ts=expiry,
            expiry_verified=True,
            provider_fields={"iv": 18.2, "delta": 0.41},
        )

        record = bsm_output_record(analysis)

        self.assertAlmostEqual(record["bsm_iv_close"], 0.18, places=7)
        self.assertEqual(record["bsm_iv_unit"], "decimal")
        self.assertEqual(record["bsm_rate_cc"], 0.10)
        self.assertEqual(record["bsm_dividend_yield"], 0.0)
        self.assertEqual(record["bsm_time_basis"], "ACT/365")
        self.assertIn("15:30:00+05:30", record["bsm_expiry_ts"])
        self.assertFalse(record["bsm_numerical_epsilon_applied"])
        self.assertAlmostEqual(record["bsm_price_residual_abs"], 0.0, places=7)
        self.assertIn("bsm_theta_per_day_365", record)
        self.assertIn("bsm_vega_per_1", record)
        self.assertIn("bsm_rho_per_100", record)
        self.assertEqual(record["provider_fields"], {"iv": 18.2, "delta": 0.41})
        self.assertNotIn("iv", {key for key in record if key != "provider_fields"})


if __name__ == "__main__":
    unittest.main()
