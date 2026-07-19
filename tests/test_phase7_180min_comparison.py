from __future__ import annotations

import pandas as pd

from research.phase7.run_180min_signal_comparison import (
    attach_signal_mappings,
    deduplicate_executions,
)


def test_execution_deduplication_and_mapping() -> None:
    timestamp = pd.Timestamp("2025-01-02 04:30Z")
    membership = pd.DataFrame(
        {
            "membership_id": [1, 2],
            "trade_date": [timestamp.date(), timestamp.date()],
            "entry_ts": [timestamp, timestamp],
            "signal_name": ["zero_up", "q85_up"],
            "signal_group": ["zero_crossing", "percentile_crossing"],
            "requested_structure": ["short_iron_condor", "short_iron_condor"],
        }
    )
    executions, keyed = deduplicate_executions(membership)
    assert len(executions) == 1
    outcomes = pd.DataFrame(
        {
            "trade_id": [1, 1],
            "trade_date": [str(timestamp.date()), str(timestamp.date())],
            "entry_ts": [timestamp, timestamp],
            "signal_family": ["phase7_180min", "phase7_180min"],
            "structure": ["short_iron_condor", "long_iron_condor"],
            "net_pnl_rupees": [1.0, -1.0],
        }
    )
    attached = attach_signal_mappings(keyed, outcomes)
    assert len(attached) == 4
    assert attached["mapping"].value_counts().to_dict() == {"requested": 2, "inverse": 2}
    assert "entry_ts" in attached
    assert "trade_date" in attached
    assert not any(column.endswith(("_x", "_y")) for column in attached.columns)
