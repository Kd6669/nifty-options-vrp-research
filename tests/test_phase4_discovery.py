from __future__ import annotations

from pathlib import Path

from research.phase4.run_cost_aware_discovery import build_discovery_events


def test_discovery_event_contract_uses_two_fixed_signal_families() -> None:
    path = Path("audit/phase2_vrp_session_curve_features.parquet")
    if not path.exists():
        return
    events = build_discovery_events(path)
    assert set(events["signal_family"]) == {"upper85_up", "lower10_down"}
    assert not events.duplicated(["signal_family", "trade_date"]).any()
