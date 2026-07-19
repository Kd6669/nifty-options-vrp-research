from __future__ import annotations

import pandas as pd
import pytest

from research.phase8.run_gated_capital_backtest import (
    STRUCTURE_NAMES,
    build_gated_events,
    simulate_portfolios,
    summarize_capacity,
)


def _features(timestamps: pd.Series) -> pd.DataFrame:
    rows = []
    for index, timestamp in enumerate(timestamps):
        rows.append(
            {
                "entry_ts": timestamp,
                "spot": 20_000.0,
                "atm_iv": 0.20,
                "trailing_rv_act365": 0.18,
                "signal_vrp_var_act365": 0.01,
                "vrp_tod_percentile": 0.90,
                "vrp_q5": 0.90,
                "q_velocity_5m": 0.01,
                "q_acceleration_5m": 0.01,
                "vrp_velocity_5m": 0.01,
                "vrp_acceleration_5m": 0.01,
                "spot_return_5m": 0.0,
                "spot_return_15m": 0.0,
                "spot_return_30m": 0.0,
                "iv_change_5m": [-0.04, 0.00, 0.01, 0.02, -1.0][index],
                "iv_change_15m": [-0.03, 0.00, 0.01, 0.02, -1.0][index],
                "iv_change_30m": 0.0,
                "rv_change_5m": [-0.02, 0.00, 0.01, 0.02, -1.0][index],
                "rv_change_15m": 0.0,
                "rv_change_30m": 0.0,
                "put_skew": 0.01,
                "call_skew": -0.01,
                "risk_reversal": 0.02,
                "smile_curvature": 0.01,
                "atm_ce_pe_gap": 0.0,
                "atm_iv_tod_percentile": 0.5,
                "minute_of_day": 660,
            }
        )
    return pd.DataFrame(rows)


def test_gate_thresholds_are_fit_only_on_2021_2023() -> None:
    timestamps = pd.to_datetime(
        [
            "2021-01-01 10:00+05:30",
            "2022-01-01 10:00+05:30",
            "2023-01-01 10:00+05:30",
            "2023-02-01 10:00+05:30",
            "2024-01-01 10:00+05:30",
        ],
        utc=True,
    )
    observations = pd.DataFrame(
        {
            "trade_id": range(1, 6),
            "entry_ts": timestamps,
            "trade_date": [str(value.date()) for value in timestamps],
            "entry_dte": 7.0,
            "span_time_slot": "ID1",
            "signal_family": "upper85_up",
            "horizon_minutes": 60,
        }
    )

    events, thresholds = build_gated_events(observations, _features(pd.Series(timestamps)))

    assert thresholds["iv_change_5m"] == pytest.approx(-0.01)
    assert thresholds["iv_change_15m"] == pytest.approx(-0.0075)
    assert thresholds["rv_change_5m"] == pytest.approx(-0.005)
    assert events.loc[events["year"].eq(2024), "gate_pass"].item() is False


def _surface() -> pd.DataFrame:
    rows = []
    for structure in STRUCTURE_NAMES:
        for lots in range(1, 4):
            rows.append(
                {
                    "trade_id": 1,
                    "trade_date": "2022-01-01",
                    "entry_ts": pd.Timestamp("2022-01-01 10:00", tz="Asia/Kolkata"),
                    "split": "discovery_2021_2023",
                    "structure": structure,
                    "lots": lots,
                    "gross_pnl_rupees": 1_000.0 * lots,
                    "net_pnl_rupees": 900.0 * lots,
                    "turnover_rupees": 10_000.0 * lots,
                    "base_slippage_rupees": 10.0 * lots,
                    "ladder_impact_rupees": 1.0 * lots,
                    "volume_impact_rupees": 1.0 * lots,
                    "oi_impact_rupees": 1.0 * lots,
                    "impact_rupees": 3.0 * lots,
                    "slippage_rupees": 13.0 * lots,
                    "charges_rupees": 87.0 * lots,
                    "brokerage_rupees": 40.0,
                    "stt_rupees": 10.0,
                    "stamp_duty_rupees": 1.0,
                    "exchange_charges_rupees": 5.0,
                    "sebi_charges_rupees": 1.0,
                    "ipft_rupees": 1.0,
                    "gst_rupees": 10.0,
                    "total_cost_rupees": 100.0 * lots,
                    "margin_rupees": 100_000.0 * lots,
                    "max_loss_rupees": 10_000.0 * lots,
                    "entry_dte": 7.0,
                    "span_time_slot": "ID1",
                }
            )
    return pd.DataFrame(rows)


def test_balanced_sizing_uses_minimum_of_margin_risk_and_capacity() -> None:
    events = pd.DataFrame(
        {
            "trade_id": [1],
            "trade_date": ["2022-01-01"],
            "entry_ts": [pd.Timestamp("2022-01-01 10:00", tz="Asia/Kolkata")],
            "split": ["discovery_2021_2023"],
        }
    )
    caps = {structure: 3 for structure in STRUCTURE_NAMES}

    result = simulate_portfolios(_surface(), events, caps, initial_capital=1_000_000.0)
    balanced = result.loc[
        result["structure"].eq("short_iron_fly") & result["policy"].eq("balanced")
    ].iloc[0]

    assert balanced["margin_cap_lots"] == 5
    assert balanced["risk_cap_lots"] == 2
    assert balanced["capacity_cap_lots"] == 3
    assert balanced["lots"] == 2


def test_capacity_selection_ignores_later_splits() -> None:
    surface = _surface()
    surface = pd.concat(
        [
            surface,
            surface.assign(
                trade_id=2,
                split="validation_2024",
                net_pnl_rupees=lambda frame: -1_000_000.0 * frame["lots"],
            ),
        ],
        ignore_index=True,
    )

    _, caps = summarize_capacity(surface)

    assert caps == {structure: 3 for structure in STRUCTURE_NAMES}
