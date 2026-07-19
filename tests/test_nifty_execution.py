from __future__ import annotations

from datetime import date, datetime
import math

import pytest

from nifty_execution import (
    GROWW_COST_MARGIN_SOURCE,
    NIFTY_SLIPPAGE_SOURCE,
    ExecutedLeg,
    ParticipationImpactParameters,
    estimate_basket_execution_cost,
    estimate_defined_risk_margin,
    estimate_nifty_option_slippage,
    estimate_participation_impact,
    estimate_round_trip_execution_cost,
    groww_fno_rates_for_date,
    return_on_margin,
)
from nifty_span.span import SpanContract, SpanData


def test_source_pins_are_immutable_commits() -> None:
    assert GROWW_COST_MARGIN_SOURCE.commit == "b9de06a2b6f6d7e13489c1e42ba4ddfc8621bb6b"
    assert NIFTY_SLIPPAGE_SOURCE.commit == "dc3f56d1a1d602d15e11463521b3604e1c997411"
    assert "/commit/" in GROWW_COST_MARGIN_SOURCE.commit_url


def test_slippage_matches_pinned_nifty_formula_and_component_sum() -> None:
    result = estimate_nifty_option_slippage(
        close=100.0,
        volume=200.0,
        open_interest=10_000.0,
        minutes_to_expiry=120.0,
        india_vix=15.0,
    )
    base = 100.0 * 0.001599
    time_multiplier = 1.0 + 0.045543 / math.sqrt(2.0)
    depth_multiplier = 1.0 + 1.501812 / math.log(10_001.0)
    expected = base * time_multiplier * depth_multiplier

    assert result.stale_multiplier == 1.0
    assert result.slippage_per_unit == pytest.approx(expected)
    assert result.component_sum == pytest.approx(expected)
    assert result.bid_proxy == pytest.approx(100.0 - expected)
    assert result.ask_proxy == pytest.approx(100.0 + expected)


def test_slippage_penalises_stale_and_shallow_contracts() -> None:
    liquid = estimate_nifty_option_slippage(
        close=50.0,
        volume=5_000.0,
        open_interest=50_000.0,
        minutes_to_expiry=180.0,
        india_vix=15.0,
    )
    stale = estimate_nifty_option_slippage(
        close=50.0,
        volume=0.0,
        open_interest=50_000.0,
        minutes_to_expiry=180.0,
        india_vix=15.0,
    )
    missing_liquidity = estimate_nifty_option_slippage(
        close=50.0,
        volume=None,
        open_interest=None,
        minutes_to_expiry=180.0,
        india_vix=None,
    )

    assert stale.stale_multiplier == 1.5
    assert stale.slippage_per_unit == pytest.approx(1.5 * liquid.slippage_per_unit)
    assert missing_liquidity.depth_multiplier > stale.depth_multiplier
    assert missing_liquidity.slippage_per_unit > stale.slippage_per_unit


def test_cost_adapter_preserves_groww_charge_model_and_adds_slippage() -> None:
    legs = (
        ExecutedLeg("BUY", "OPT", 38.9, 195, slippage_per_unit=0.10),
        ExecutedLeg("SELL", "OPT", 60.9, 195, slippage_per_unit=0.12),
        ExecutedLeg("SELL", "OPT", 76.1, 195, slippage_per_unit=0.11),
        ExecutedLeg("BUY", "OPT", 42.9, 195, slippage_per_unit=0.09),
    )
    result = estimate_basket_execution_cost(legs)

    assert round(result.charges.total, 2) == 139.53
    assert result.slippage == pytest.approx(195 * (0.10 + 0.12 + 0.11 + 0.09))
    assert result.total == pytest.approx(result.charges.total + result.slippage)


@pytest.mark.parametrize(
    ("trade_date", "expected"),
    [
        ("2023-03-31", 0.0005),
        ("2023-04-01", 0.000625),
        ("2024-10-01", 0.001),
        ("2026-04-01", 0.0015),
    ],
)
def test_date_aware_option_stt_regimes(trade_date: str, expected: float) -> None:
    assert groww_fno_rates_for_date(trade_date).options_stt_sell_rate == expected


def test_participation_impact_preserves_one_lot_and_penalises_capacity() -> None:
    one = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=65,
        lot_size=65,
        volume=65_000,
        open_interest=650_000,
    )
    eleven = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=715,
        lot_size=65,
        volume=65_000,
        open_interest=650_000,
    )
    shallow = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=715,
        lot_size=65,
        volume=715,
        open_interest=715,
    )
    assert one.impact_per_unit == 0.0
    assert eleven.adjusted_slippage_per_unit > one.adjusted_slippage_per_unit
    assert shallow.impact_per_unit > eleven.impact_per_unit


def test_participation_ladder_reaches_base_slippage_at_sixty_lots() -> None:
    result = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=60 * 65,
        lot_size=65,
        volume=1e18,
        open_interest=1e18,
    )

    assert result.ladder_impact_ratio == pytest.approx(1.0)
    assert result.ladder_impact_per_unit == pytest.approx(0.20)
    assert result.participation_impact_ratio == pytest.approx(0.0, abs=1e-6)
    assert result.impact_per_unit == pytest.approx(0.20, abs=1e-6)


def test_participation_terms_are_additive_and_auditable() -> None:
    result = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=6 * 65,
        lot_size=65,
        volume=65_000,
        open_interest=650_000,
    )

    assert result.ladder_impact_ratio == pytest.approx(5.0 / 59.0)
    assert result.incremental_volume_participation == pytest.approx(325 / 65_000)
    assert result.incremental_oi_participation == pytest.approx(325 / 650_000)
    assert result.total_impact_ratio == pytest.approx(
        result.ladder_impact_ratio + result.volume_impact_ratio + result.oi_impact_ratio
    )
    assert result.impact_per_unit == pytest.approx(
        result.ladder_impact_per_unit + result.volume_impact_per_unit + result.oi_impact_per_unit
    )


def test_participation_parity_anchor_is_configurable() -> None:
    result = estimate_participation_impact(
        base_slippage_per_unit=0.20,
        quantity=70 * 65,
        lot_size=65,
        volume=1e18,
        open_interest=1e18,
        parameters=ParticipationImpactParameters(ladder_parity_lots=70.0),
    )

    assert result.ladder_impact_ratio == pytest.approx(1.0)


def test_round_trip_cost_counts_both_baskets() -> None:
    entry = (ExecutedLeg("BUY", "OPT", 10.0, 75, slippage_per_unit=0.1),)
    exit_legs = (ExecutedLeg("SELL", "OPT", 12.0, 75, slippage_per_unit=0.2),)
    result = estimate_round_trip_execution_cost(entry_legs=entry, exit_legs=exit_legs)

    assert result.total_slippage == pytest.approx(22.5)
    assert result.total_charges == pytest.approx(
        result.entry.charges.total + result.exit.charges.total
    )
    assert result.total == pytest.approx(result.total_charges + result.total_slippage)


def test_margin_adapter_uses_span_model_a_and_expiry_day_elm() -> None:
    expiry = date(2025, 1, 9)
    leg = {
        "side": "SELL",
        "instrument": "OPT",
        "option_type": "CE",
        "strike": 24_000.0,
        "lot_size": 75,
        "entry_price": 100.0,
        "expiry": expiry.isoformat(),
    }
    contract = SpanContract(tuple(-10.0 for _ in range(16)), price=100.0)
    before_data = SpanData(
        {("NIFTY", "CE", expiry, 24_000.0): contract},
        selected_time_slot="ID1",
        trading_date=date(2025, 1, 8),
    )
    expiry_data = SpanData(
        {("NIFTY", "CE", expiry, 24_000.0): contract},
        selected_time_slot="ID2",
        trading_date=expiry,
    )

    before = estimate_defined_risk_margin(
        legs=(leg,),
        span_data=before_data,
        expiry=expiry.isoformat(),
        spot=24_000.0,
        eval_dt=datetime(2025, 1, 8, 12, 0),
    )
    on_expiry = estimate_defined_risk_margin(
        legs=(leg,),
        span_data=expiry_data,
        expiry=expiry.isoformat(),
        spot=24_000.0,
        eval_dt=datetime(2025, 1, 9, 12, 0),
    )

    assert before.source == "span_model_a"
    assert on_expiry.elm_required > before.elm_required
    assert on_expiry.margin > before.margin
    assert on_expiry.span_time_slot == "ID2"
    assert return_on_margin(net_pnl=1_000.0, margin=on_expiry.margin) == pytest.approx(
        1_000.0 / on_expiry.margin
    )


def test_invalid_execution_inputs_fail_closed() -> None:
    with pytest.raises(ValueError, match="positive"):
        estimate_nifty_option_slippage(
            close=0.0,
            volume=1.0,
            open_interest=1.0,
            minutes_to_expiry=60.0,
            india_vix=15.0,
        )
    with pytest.raises(ValueError, match="quantity"):
        estimate_basket_execution_cost((ExecutedLeg("BUY", "OPT", 10.0, 0),))
    with pytest.raises(ValueError, match="margin"):
        return_on_margin(net_pnl=10.0, margin=0.0)
