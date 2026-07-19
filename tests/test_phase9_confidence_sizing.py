from __future__ import annotations

import pandas as pd
import pytest

from research.phase9.run_confidence_sizing import (
    build_scores,
    discovery_ecdf,
    risk_fraction_for_score,
    spearman_rank_correlation,
)


def test_discovery_ecdf_does_not_refit_on_later_values() -> None:
    reference = pd.Series([1.0, 2.0, 3.0, 4.0])
    values = pd.Series([0.0, 2.0, 5.0, 1000.0])

    result = discovery_ecdf(reference, values)

    assert result.tolist() == pytest.approx([0.0, 0.5, 1.0, 1.0])


def test_risk_ladder_is_monotone_and_capped_at_two_percent() -> None:
    scores = [0.0, 0.2, 0.21, 0.4, 0.41, 0.6, 0.61, 0.8, 0.81, 1.0]
    fractions = [risk_fraction_for_score(score) for score in scores]

    assert fractions == [0.0, 0.0, 0.005, 0.005, 0.01, 0.01, 0.015, 0.015, 0.02, 0.02]
    assert fractions == sorted(fractions)


def test_spearman_uses_rank_order_not_scale() -> None:
    score = pd.Series([0.1, 0.2, 0.3, 0.4])

    assert spearman_rank_correlation(score, pd.Series([1.0, 10.0, 100.0, 1000.0])) == pytest.approx(1.0)
    assert spearman_rank_correlation(score, pd.Series([1000.0, 100.0, 10.0, 1.0])) == pytest.approx(-1.0)


def test_score_percentiles_are_fit_on_discovery_only() -> None:
    rows = []
    for index, split in enumerate(
        ["discovery_2021_2023"] * 4 + ["validation_2024"]
    ):
        rows.append(
            {
                "trade_id": index,
                "entry_ts": pd.Timestamp("2023-01-01", tz="UTC") + pd.Timedelta(days=index),
                "trade_date": "2023-01-01",
                "split": split,
                "gate_pass": True,
                "gate_cushion": [0.1, 0.2, 0.3, 0.4, 100.0][index],
                "atm_iv": [0.3, 0.25, 0.2, 0.15, 0.01][index],
                "trailing_rv_act365": [0.3, 0.25, 0.2, 0.15, 0.01][index],
                "entry_dte": [20.0, 15.0, 10.0, 5.0, 1.0][index],
                "minute_of_day": 14 * 60,
            }
        )

    scored = build_scores(pd.DataFrame(rows))
    later = scored.loc[scored["split"].eq("validation_2024")].iloc[0]

    assert later["gate_cushion_only_score"] == pytest.approx(1.0)
    assert later["regime_composite_score"] == pytest.approx(1.0)
