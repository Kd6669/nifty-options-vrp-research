from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from research.phase5.build_final_attempt_dataset import build_causal_features
from research.phase5.run_final_attempt_strategy import (
    FEATURES,
    fit_ridge,
    select_non_overlapping,
)


def test_causal_feature_lags_require_exact_prior_timestamp(tmp_path: Path) -> None:
    timestamps = pd.to_datetime(
        ["2025-01-02 04:30:00Z", "2025-01-02 04:35:00Z", "2025-01-02 04:45:00Z"]
    )
    curve = pd.DataFrame(
        {
            "entry_id": [1, 2, 3],
            "entry_ts": timestamps,
            "trade_date": [pd.Timestamp("2025-01-02").date()] * 3,
            "entry_time": ["10:00", "10:05", "10:15"],
            "research_dte": [5.0] * 3,
            "spot": [100.0, 101.0, 103.0],
            "atm_iv": [0.2, 0.21, 0.23],
            "trailing_rv_act365": [0.1, 0.11, 0.13],
            "signal_vrp_var_act365": [0.03] * 3,
            "vrp_tod_percentile": [0.5] * 3,
            "vrp_q5": [0.5] * 3,
            "q_velocity_5m": [0.0] * 3,
            "q_acceleration_5m": [0.0] * 3,
            "q_acceleration_tod_percentile": [0.5] * 3,
            "vrp_velocity_5m": [0.0] * 3,
            "vrp_acceleration_5m": [0.0] * 3,
            "next_entry_ts": timestamps,
            "next_entry_time": ["10:00", "10:05", "10:15"],
            "next_short_pnl_points": [0.0] * 3,
            "next_short_return_on_max_loss": [0.0] * 3,
            "next_long_pnl_points": [0.0] * 3,
            "next_long_return_on_max_loss": [0.0] * 3,
        }
    )
    surface = pd.DataFrame({"timestamp_ist": timestamps})
    for column in (
        "put_skew",
        "call_skew",
        "risk_reversal",
        "smile_curvature",
        "atm_ce_pe_gap",
        "atm_iv_tod_percentile",
    ):
        surface[column] = 0.0
    curve_path = tmp_path / "curve.parquet"
    surface_path = tmp_path / "surface.parquet"
    curve.to_parquet(curve_path, index=False)
    surface.to_parquet(surface_path, index=False)
    result = build_causal_features(curve_path, surface_path)
    assert len(result) == 2
    row_1015 = result.loc[result["entry_time"].eq("10:15")].iloc[0]
    assert row_1015["spot_return_15m"] == pytest.approx(0.03)
    assert pd.isna(row_1015["spot_return_5m"])


def test_selector_uses_highest_score_and_prevents_overlap() -> None:
    base = {feature: 0.0 for feature in FEATURES}
    rows = []
    for entry, exit_, structure, predicted, hurdle in (
        ("2025-01-02 04:30Z", "2025-01-02 05:30Z", "bull_call_spread", 3.0, 1.0),
        ("2025-01-02 04:30Z", "2025-01-02 06:30Z", "bear_put_spread", 2.0, 1.0),
        ("2025-01-02 04:45Z", "2025-01-02 05:45Z", "bull_call_spread", 5.0, 1.0),
        ("2025-01-02 05:30Z", "2025-01-02 06:30Z", "bear_put_spread", 2.0, 1.0),
    ):
        rows.append(
            {
                **base,
                "entry_ts": entry,
                "exit_ts": exit_,
                "structure": structure,
                "horizon_minutes": 60,
                "predicted_gross_points": predicted,
                "causal_cost_hurdle_points": hurdle,
            }
        )
    selected = select_non_overlapping(pd.DataFrame(rows), 1.0)
    assert selected["structure"].tolist() == ["bull_call_spread", "bear_put_spread"]
    assert len(selected) == 2


def test_ridge_fits_simple_linear_target() -> None:
    rows = []
    for value in (-2.0, -1.0, 0.0, 1.0, 2.0):
        row = {feature: 0.0 for feature in FEATURES}
        row["atm_iv"] = value
        row["gross_pnl_points"] = 1.0 + 2.0 * value
        rows.append(row)
    frame = pd.DataFrame(rows)
    model = fit_ridge(frame, alpha=0.0)
    assert model.predict(frame) == pytest.approx(frame["gross_pnl_points"].to_numpy())
