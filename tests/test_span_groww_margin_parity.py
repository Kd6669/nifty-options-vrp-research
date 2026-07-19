from __future__ import annotations

from datetime import date

from nifty_span.span.groww_margin_parity import (
    ChainSnapshot,
    GrowwMarginComponents,
    LocalMarginComponents,
    _parity_row,
    _unwrap_payload,
    generate_margin_test_baskets,
)


def _snapshot() -> ChainSnapshot:
    rows = []
    for strike in range(23900, 24451, 50):
        for option_type in ("CE", "PE"):
            rows.append(
                {
                    "session_date": "2026-06-23",
                    "snapshot_time": "2026-06-23T09:30:00+05:30",
                    "snapshot_ts_ms": 1,
                    "underlying": "NIFTY",
                    "expiry": "2026-06-23",
                    "underlying_ltp": 24125.0,
                    "strike": float(strike),
                    "option_type": option_type,
                    "trading_symbol": f"NIFTY26JUN{strike}{option_type}",
                    "lot_size": 75,
                    "ltp": 100.0 + abs(strike - 24125.0) / 10.0,
                }
            )
    return ChainSnapshot(
        trading_date=date(2026, 6, 23),
        timestamp="2026-06-23T09:30:00+05:30",
        snapshot_ts_ms=1,
        underlying="NIFTY",
        expiry="2026-06-23",
        spot=24125.0,
        rows=tuple(rows),
        source_path="memory",
    )


def test_generate_margin_test_baskets_covers_required_cases() -> None:
    baskets = generate_margin_test_baskets(_snapshot(), lots=(1, 3))
    families = {basket.family for basket in baskets}

    assert "naked_short" in families
    assert "naked_long" in families
    assert "short_strangle" in families
    assert "vertical_credit" in families
    assert "vertical_debit" in families
    assert "hedged_short" in families
    assert {basket.lots for basket in baskets} == {1, 3}
    assert any(len(basket.legs) == 4 for basket in baskets)


def test_generate_margin_test_baskets_can_include_future_beta_cases() -> None:
    baskets = generate_margin_test_baskets(
        _snapshot(),
        lots=(1,),
        future_trading_symbol="NIFTY26JUNFUT",
        future_expiry="2026-06-30",
        future_price=24150.0,
        future_lot_size=75,
    )
    families = {basket.family for basket in baskets}

    assert "naked_future" in families
    assert "future_option_hedge" in families
    assert "beta_hedged_short" in families
    assert any(leg.instrument == "FUT" and leg.expiry == "2026-06-30" for basket in baskets for leg in basket.legs)


def test_basket_leg_converts_to_groww_charge_leg() -> None:
    basket = generate_margin_test_baskets(_snapshot(), lots=(3,))[0]
    leg = basket.legs[0].to_charge_leg()

    assert leg.side == basket.legs[0].side
    assert leg.instrument == basket.legs[0].instrument
    assert leg.price == basket.legs[0].price
    assert leg.quantity == basket.legs[0].lot_size * basket.legs[0].qty_ratio
    assert leg.exchange == "NSE"


def test_groww_payload_unwraps_curl_shape_but_preserves_sdk_shape() -> None:
    assert _unwrap_payload({"payload": {"total_requirement": 10.0}})["total_requirement"] == 10.0
    assert _unwrap_payload({"total_requirement": 20.0})["total_requirement"] == 20.0


def test_parity_row_verdict_pass_warn_fail_thresholds() -> None:
    basket = generate_margin_test_baskets(_snapshot(), lots=(1,))[0]
    local = LocalMarginComponents(
        total_requirement=100_000.0,
        span_required=80_000.0,
        scan_risk_before_nov=70_000.0,
        short_option_credit=10_000.0,
        option_buy_premium=0.0,
        exposure_required=20_000.0,
        brokerage_and_charges=0.0,
        selected_span_slot="BOD",
        span_trading_date="2026-06-23",
        active_scenario=1,
    )

    passed = _parity_row(
        basket=basket,
        local=local,
        groww=GrowwMarginComponents(total_requirement=99_900.0),
        warn_abs_inr=500.0,
        fail_abs_inr=2_000.0,
        warn_pct=0.02,
        fail_pct=0.05,
    )
    warned = _parity_row(
        basket=basket,
        local=local,
        groww=GrowwMarginComponents(total_requirement=99_000.0),
        warn_abs_inr=500.0,
        fail_abs_inr=2_000.0,
        warn_pct=0.02,
        fail_pct=0.05,
    )
    failed = _parity_row(
        basket=basket,
        local=local,
        groww=GrowwMarginComponents(total_requirement=95_000.0),
        warn_abs_inr=500.0,
        fail_abs_inr=2_000.0,
        warn_pct=0.02,
        fail_pct=0.05,
    )

    assert passed.verdict == "PASS"
    assert warned.verdict == "WARN"
    assert failed.verdict == "FAIL"

    flat = passed.to_flat_dict()
    assert flat["exposure_risk_quantity"] == basket.legs[0].quantity
    assert round(flat["local_exposure_ref_avg"], 2) == round(20_000.0 / (0.02 * basket.legs[0].quantity), 2)
