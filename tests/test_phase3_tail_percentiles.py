from __future__ import annotations

from pathlib import Path

import pandas as pd

from research.phase3.run_tail_percentile_backtests import build_tail_events
from research.phase3.run_full_strategy_backtest import _fill_price


def test_build_tail_events_uses_first_daily_exact_cross_and_next_minute(tmp_path: Path) -> None:
    timestamps = pd.date_range("2026-01-01 09:15", periods=9, freq="min", tz="UTC")
    q5 = [0.60, 0.74, 0.76, 0.73, 0.77, 0.80, 0.72, 0.76, 0.70]
    frame = pd.DataFrame(
        {
            "entry_ts": timestamps,
            "trade_date": "2026-01-01",
            "entry_time": [value.strftime("%H:%M") for value in timestamps],
            "signal_vrp_var_act365": range(len(timestamps)),
            "vrp_tod_percentile": q5,
            "vrp_q5": q5,
            "next_entry_ts": timestamps.to_series(index=range(len(timestamps))).shift(-1),
            "next_entry_time": [
                timestamps[index + 1].strftime("%H:%M") if index + 1 < len(timestamps) else None
                for index in range(len(timestamps))
            ],
            "next_short_pnl_points": 1.0,
            "next_short_return_on_max_loss": 0.01,
            "next_long_pnl_points": -1.0,
            "next_long_return_on_max_loss": -0.01,
        }
    )
    path = tmp_path / "curve.parquet"
    frame.to_parquet(path, index=False)

    events = build_tail_events(path, thresholds=(0.75,))

    assert list(events["direction"]) == ["down", "up"]
    down = events.loc[events["direction"].eq("down")].iloc[0]
    up = events.loc[events["direction"].eq("up")].iloc[0]
    assert down["signal_ts"] == timestamps[3]
    assert up["signal_ts"] == timestamps[2]
    assert down["entry_ts"] == timestamps[4]
    assert up["entry_ts"] == timestamps[3]
    assert down["exit_ts"] == timestamps[4] + pd.Timedelta(minutes=60)
    assert up["signal_vrp_percentile_q5"] == 0.76


def test_build_tail_events_rejects_noncontiguous_previous_observation(tmp_path: Path) -> None:
    timestamps = pd.to_datetime(
        [
            "2026-01-01 09:15:00Z",
            "2026-01-01 09:17:00Z",
            "2026-01-01 09:18:00Z",
            "2026-01-01 09:19:00Z",
        ]
    )
    frame = pd.DataFrame(
        {
            "entry_ts": timestamps,
            "trade_date": "2026-01-01",
            "entry_time": ["09:15", "09:17", "09:18", "09:19"],
            "signal_vrp_var_act365": [1.0, 2.0, 3.0, 4.0],
            "vrp_tod_percentile": [0.70, 0.80, 0.70, 0.70],
            "vrp_q5": [0.70, 0.80, 0.70, 0.70],
            "next_entry_ts": [timestamps[1], timestamps[2], timestamps[3], pd.NaT],
            "next_entry_time": ["09:17", "09:18", "09:19", None],
            "next_short_pnl_points": [1.0, 1.0, 1.0, None],
            "next_short_return_on_max_loss": [0.01, 0.01, 0.01, None],
            "next_long_pnl_points": [-1.0, -1.0, -1.0, None],
            "next_long_return_on_max_loss": [-0.01, -0.01, -0.01, None],
        }
    )
    path = tmp_path / "curve.parquet"
    frame.to_parquet(path, index=False)

    events = build_tail_events(path, thresholds=(0.75,))

    assert len(events) == 1
    assert events.iloc[0]["direction"] == "down"
    assert events.iloc[0]["signal_ts"] == timestamps[2]


def test_minimum_sell_fill_is_opt_in() -> None:
    assert _fill_price(0.10, 0.20, "SELL", minimum_sell_fill=0.05) == 0.05
