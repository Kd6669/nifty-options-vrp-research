from __future__ import annotations

import pandas as pd
import pytest

from research.phase3.run_full_strategy_backtest import (
    _distribution,
    _fill_price,
    _max_drawdown,
    _theoretical_risk,
)


def test_adverse_fill_moves_against_each_side() -> None:
    assert _fill_price(100.0, 0.25, "BUY") == 100.25
    assert _fill_price(100.0, 0.25, "SELL") == 99.75
    with pytest.raises(ValueError, match="negative"):
        _fill_price(0.10, 0.20, "SELL")


def test_iron_condor_theoretical_risk_is_bounded() -> None:
    legs = pd.DataFrame(
        [
            {"leg": "p_m3", "option_type": "PUT", "strike": 97.0, "entry_close": 0.5},
            {"leg": "p_m1", "option_type": "PUT", "strike": 99.0, "entry_close": 1.0},
            {"leg": "c_p1", "option_type": "CALL", "strike": 101.0, "entry_close": 1.0},
            {"leg": "c_p3", "option_type": "CALL", "strike": 103.0, "entry_close": 0.5},
        ]
    )
    weights = {"p_m3": 1, "p_m1": -1, "c_p1": -1, "c_p3": 1}

    max_loss, max_profit = _theoretical_risk(legs, weights)

    assert max_loss == pytest.approx(1.0)
    assert max_profit == pytest.approx(1.0)


def test_distribution_and_drawdown_are_deterministic() -> None:
    values = pd.Series([100.0, -40.0, -80.0, 30.0])

    distribution = _distribution(values)
    drawdown = _max_drawdown(values)

    assert distribution["sum"] == 10.0
    assert distribution["win_rate"] == 0.5
    assert drawdown["amount"] == -120.0
    assert drawdown["peak_trade"] == 1
    assert drawdown["trough_trade"] == 3
