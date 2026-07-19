from __future__ import annotations

import numpy as np
import pytest

from research.phase10.run_sizing_exploration import (
    SizingConfig,
    effective_risk_fraction,
    generate_configs,
    risk_cap_with_cost_reserve,
    score_scaled_risk_fraction,
)


def _config(**overrides: object) -> SizingConfig:
    values = {
        "config_id": 1,
        "margin_fraction": 0.5,
        "max_risk_fraction": 0.02,
        "score_floor": 0.2,
        "score_power": 1.0,
        "drawdown_brake_threshold": 0.01,
        "drawdown_brake_multiplier": 0.5,
        "losing_streak_trigger": 2,
    }
    values.update(overrides)
    return SizingConfig(**values)  # type: ignore[arg-type]


def test_grid_size_is_frozen() -> None:
    assert len(generate_configs()) == 17_640


def test_score_scaling_respects_floor_power_and_maximum() -> None:
    assert score_scaled_risk_fraction(0.2, score_floor=0.2, score_power=1.0, maximum=0.02) == 0.0
    assert score_scaled_risk_fraction(0.6, score_floor=0.2, score_power=1.0, maximum=0.02) == pytest.approx(0.01)
    assert score_scaled_risk_fraction(1.0, score_floor=0.2, score_power=2.0, maximum=0.02) == pytest.approx(0.02)


def test_drawdown_and_streak_brakes_multiply_causally() -> None:
    config = _config()

    assert effective_risk_fraction(config, score=1.0, current_drawdown=0.0, losing_streak=0) == pytest.approx(0.02)
    assert effective_risk_fraction(config, score=1.0, current_drawdown=-0.02, losing_streak=0) == pytest.approx(0.01)
    assert effective_risk_fraction(config, score=1.0, current_drawdown=-0.02, losing_streak=2) == pytest.approx(0.005)


def test_risk_cap_includes_non_linear_cost_reserve() -> None:
    reserve = np.array([0.0, 100.0, 250.0, 450.0])

    assert risk_cap_with_cost_reserve(
        equity=100_000.0,
        risk_fraction=0.025,
        max_loss_per_lot=1_000.0,
        cost_reserve=reserve,
    ) == 2
