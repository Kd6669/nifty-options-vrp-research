from __future__ import annotations

import pandas as pd

from research.phase4.run_multiday_vrp_feasibility import summarize


def test_multiday_summary_applies_coverage_gate() -> None:
    rows = []
    for trade_id in (1, 2):
        for leg in ("p_m3", "p_m1", "c_p1", "c_p3"):
            rows.append(
                {
                    "signal_family": "lower10_down",
                    "hold_sessions": 1,
                    "trade_id": trade_id,
                    "leg": leg,
                    "multiday_exit_close": 1.0 if trade_id == 1 else None,
                }
            )
    frame = pd.DataFrame(rows)
    tradebook = pd.DataFrame(
        {
            "signal_family": ["lower10_down"],
            "hold_sessions": [1],
            "gross_pnl_points": [2.0],
            "cost_hurdle_points": [1.0],
            "net_pnl_rupees": [50.0],
        }
    )
    result = summarize(frame, tradebook)
    assert result["coverage"][0]["exact_contract_coverage"] == 0.5
    assert result["interpretation_gate"]["eligible_cells"] == []
