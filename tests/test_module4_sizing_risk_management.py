from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from research.module4_sizing_risk_management.closeout import (
    CONFIG_ID,
    DECISION,
    PROFILE,
    build_closeout,
    verify_manifest,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
RESULT_ROOT = REPO_ROOT / "research/module4_sizing_risk_management/results"


def test_closeout_reconciles_frozen_candidate() -> None:
    result = build_closeout(REPO_ROOT)

    assert result["decision"] == DECISION
    assert result["recommended_candidate"]["profile"] == PROFILE
    assert int(result["recommended_candidate"]["config_id"]) == CONFIG_ID
    assert result["recomputed_trade_sheet"]["signals"] == 132
    assert result["recomputed_trade_sheet"]["executed_trades"] == 86
    assert all(result["reconciliation"].values())


def test_trade_sheet_contains_skips_costs_and_caps() -> None:
    trades = pd.read_csv(RESULT_ROOT / "trades/recommended_trade_sheet.csv")
    executed = trades.loc[trades["executed"]]
    skipped = trades.loc[~trades["executed"]]

    assert len(trades) == 132
    assert len(executed) == 86
    assert len(skipped) == 46
    assert executed["total_cost_rupees"].gt(0).all()
    assert executed["margin_utilization"].max() <= 0.35 + 1e-12
    assert executed["cash_risk_utilization"].max() <= 0.04 + 1e-12
    assert skipped["skip_reason"].eq("confidence_score_at_or_below_0.40").all()


def test_acceptance_boundary_is_explicit() -> None:
    contract = json.loads(
        (REPO_ROOT / "research/module4_sizing_risk_management/contracts/strategy.json").read_text(
            encoding="utf-8"
        )
    )

    assert contract["selection"]["config_id"] == CONFIG_ID
    assert contract["acceptance"]["phase9_rank_gate"].startswith("FAIL")
    assert contract["acceptance"]["deployment"] == "NOT_APPROVED"
    assert contract["acceptance"]["allowed_next_use"] == "frozen_forward_shadow_test"


def test_saved_manifest_verifies() -> None:
    assert verify_manifest(REPO_ROOT) == []
