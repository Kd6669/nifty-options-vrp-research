from __future__ import annotations

import pandas as pd

from research.phase6.run_vrp_reversal_test import build_reversal_events


def test_reversal_event_state_machine_and_first_daily_rule() -> None:
    timestamps = pd.to_datetime(
        [
            "2025-01-02 04:30Z",
            "2025-01-02 04:35Z",
            "2025-01-02 04:40Z",
            "2025-01-02 04:45Z",
            "2025-01-03 04:30Z",
            "2025-01-03 04:35Z",
            "2025-01-03 04:40Z",
            "2025-01-03 04:45Z",
        ]
    )
    frame = pd.DataFrame(
        {
            "trade_date": [pd.Timestamp("2025-01-02").date()] * 4
            + [pd.Timestamp("2025-01-03").date()] * 4,
            "entry_ts": timestamps,
            "vrp_q5": [0.50, 0.92, 0.95, 0.84, 0.50, 0.08, 0.05, 0.16],
            "q_velocity_5m": [0.0, 0.2, 0.03, -0.11, 0.0, -0.2, -0.03, 0.11],
            "signal_vrp_var_act365": [0.02] * 4 + [-0.02] * 4,
        }
    )
    events = build_reversal_events(frame)
    assert events["reversal_type"].tolist() == ["top_to_zero", "bottom_to_zero"]
    assert events["entry_ts"].tolist() == [timestamps[3] + pd.Timedelta(minutes=1), timestamps[7] + pd.Timedelta(minutes=1)]
