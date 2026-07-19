from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from research.module5_final_submission.analysis import RESULT_ROOT, verify_submission


ROOT = Path(__file__).resolve().parents[1]


def test_final_submission_manifest_and_counts() -> None:
    assert verify_submission(ROOT) == []
    trades = pd.read_csv(ROOT / RESULT_ROOT / "trades/final_trade_sheet.csv")
    assert len(trades) == 132
    assert int(trades["executed"].sum()) == 86
    assert int((~trades["executed"].astype(bool)).sum()) == 46


def test_trade_economics_reconcile() -> None:
    trades = pd.read_csv(ROOT / RESULT_ROOT / "trades/final_trade_sheet.csv")
    executed = trades.loc[trades["executed"].astype(bool)]
    expected = executed["gross_pnl_rupees"] - executed["total_cost_rupees"]
    np.testing.assert_allclose(expected, executed["net_pnl_rupees"], atol=1e-6, rtol=0)


def test_execution_decay_baseline_matches_summary() -> None:
    decay = pd.read_csv(ROOT / RESULT_ROOT / "robustness/execution_decay.csv")
    baseline = decay.loc[decay["slippage_multiplier"].eq(1.0), "net_pnl_rupees"].item()
    summary = json.loads((ROOT / RESULT_ROOT / "summary.json").read_text(encoding="utf-8"))
    assert abs(baseline - summary["metrics"]["net_profit_rupees"]) < 1e-6


def test_decision_remains_shadow_only() -> None:
    summary = json.loads((ROOT / RESULT_ROOT / "summary.json").read_text(encoding="utf-8"))
    assert summary["decision"] == "SHADOW_ONLY_NOT_LIVE_CAPITAL_APPROVED"
    assert summary["hypothesis_result"] == "STANDALONE_INTRADAY_VRP_RULE_REJECTED_NET_OF_COSTS"
