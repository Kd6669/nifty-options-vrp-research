"""Test causal confidence-ranked sizing for the gated short iron fly."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase8.run_gated_capital_backtest import (
    CAPACITY_IMPACT_PARAMETERS,
    INITIAL_CAPITAL,
    MAX_CAPACITY_LOTS,
    _daily_curve,
    _execution_record,
    _json_safe,
    summarize_portfolio,
)


SCHEMA_VERSION = "phase9-confidence-sizing/v1"
STRUCTURE = "short_iron_fly"
MARGIN_FRACTION = 0.50
BOOTSTRAP_SAMPLES = 5_000
PERMUTATION_SAMPLES = 10_000
SEED = 20_260_719
SCORE_TYPES = ("gate_cushion_only", "regime_composite")
SPLIT_ORDER = (
    "discovery_2021_2023",
    "validation_2024",
    "confirmation_2025_2026",
    "holdout_2024_2026",
    "full_sample",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def discovery_ecdf(reference: pd.Series, values: pd.Series) -> pd.Series:
    """Return causal percentiles using only the supplied discovery reference."""

    clean = np.sort(pd.to_numeric(reference, errors="coerce").dropna().to_numpy(dtype=float))
    targets = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if clean.size == 0:
        raise ValueError("discovery ECDF requires at least one finite reference value")
    percentiles = np.searchsorted(clean, targets, side="right") / clean.size
    percentiles[~np.isfinite(targets)] = np.nan
    return pd.Series(percentiles, index=values.index, dtype=float)


def build_scores(events: pd.DataFrame) -> pd.DataFrame:
    """Build the frozen gate-only and regime-composite entry-time confidence scores."""

    scored = events.loc[events["gate_pass"]].copy().sort_values("entry_ts")
    discovery = scored.loc[scored["split"].eq("discovery_2021_2023")]
    scored["gate_strength"] = discovery_ecdf(discovery["gate_cushion"], scored["gate_cushion"])
    scored["low_iv_strength"] = 1.0 - discovery_ecdf(discovery["atm_iv"], scored["atm_iv"])
    scored["low_rv_strength"] = 1.0 - discovery_ecdf(
        discovery["trailing_rv_act365"], scored["trailing_rv_act365"]
    )
    scored["low_dte_strength"] = 1.0 - discovery_ecdf(
        discovery["entry_dte"], scored["entry_dte"]
    )
    scored["time_strength"] = np.select(
        [scored["minute_of_day"] > 13 * 60, scored["minute_of_day"] < 11 * 60],
        [1.0, 0.5],
        default=0.0,
    )
    scored["gate_cushion_only_raw"] = scored["gate_strength"]
    scored["regime_composite_raw"] = (
        0.50 * scored["gate_strength"]
        + 0.15 * scored["low_iv_strength"]
        + 0.15 * scored["low_rv_strength"]
        + 0.10 * scored["low_dte_strength"]
        + 0.10 * scored["time_strength"]
    )
    for score_type in SCORE_TYPES:
        raw = f"{score_type}_raw"
        scored[f"{score_type}_score"] = discovery_ecdf(
            scored.loc[scored["split"].eq("discovery_2021_2023"), raw], scored[raw]
        ).clip(0.0, 1.0)
    return scored


def risk_fraction_for_score(score: float) -> float:
    """Frozen monotone risk ladder; the bottom discovery quintile is a no-trade switch."""

    if not math.isfinite(score) or score <= 0.20:
        return 0.0
    if score <= 0.40:
        return 0.005
    if score <= 0.60:
        return 0.010
    if score <= 0.80:
        return 0.015
    return 0.020


def spearman_rank_correlation(x: pd.Series, y: pd.Series) -> float:
    pair = pd.concat([x, y], axis=1).dropna()
    if len(pair) < 3:
        return float("nan")
    left = pair.iloc[:, 0].rank(method="average").to_numpy(dtype=float)
    right = pair.iloc[:, 1].rank(method="average").to_numpy(dtype=float)
    if np.std(left) == 0.0 or np.std(right) == 0.0:
        return float("nan")
    return float(np.corrcoef(left, right)[0, 1])


def _rank_inference(
    score: pd.Series,
    outcome: pd.Series,
    *,
    seed: int,
) -> tuple[float, float, float, float]:
    pair = pd.concat([score, outcome], axis=1).dropna().reset_index(drop=True)
    observed = spearman_rank_correlation(pair.iloc[:, 0], pair.iloc[:, 1])
    if len(pair) < 5 or not math.isfinite(observed):
        return observed, float("nan"), float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    bootstrap = []
    for _ in range(BOOTSTRAP_SAMPLES):
        indexes = rng.integers(0, len(pair), len(pair))
        value = spearman_rank_correlation(
            pair.iloc[indexes, 0].reset_index(drop=True),
            pair.iloc[indexes, 1].reset_index(drop=True),
        )
        if math.isfinite(value):
            bootstrap.append(value)
    lower, upper = np.quantile(bootstrap, [0.025, 0.975])
    permuted_at_least_observed = 0
    for _ in range(PERMUTATION_SAMPLES):
        value = spearman_rank_correlation(
            pair.iloc[:, 0], pd.Series(rng.permutation(pair.iloc[:, 1].to_numpy()))
        )
        permuted_at_least_observed += int(value >= observed)
    p_one_sided = (permuted_at_least_observed + 1) / (PERMUTATION_SAMPLES + 1)
    return observed, float(lower), float(upper), float(p_one_sided)


def build_fly_cost_surface(
    observations: pd.DataFrame,
    phase4_tradebook: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    gated = events.set_index("trade_id")
    selected_observations = observations.loc[
        observations["trade_id"].isin(gated.index)
        & observations["signal_family"].eq("upper85_up")
        & observations["horizon_minutes"].eq(60)
    ].copy()
    selected_tradebook = phase4_tradebook.loc[
        phase4_tradebook["trade_id"].isin(gated.index)
        & phase4_tradebook["signal_family"].eq("upper85_up")
        & phase4_tradebook["horizon_minutes"].eq(60)
        & phase4_tradebook["structure"].eq(STRUCTURE)
    ].set_index("trade_id")
    weights = STRUCTURES[STRUCTURE]
    rows: list[dict[str, Any]] = []
    for trade_id, full_part in selected_observations.groupby("trade_id", sort=True):
        part = full_part.loc[full_part["leg"].isin(weights)].copy()
        if len(part) != len(weights) or part["exit_close"].isna().any():
            continue
        meta = selected_tradebook.loc[int(trade_id)].copy()
        meta["split"] = gated.loc[int(trade_id), "split"]
        meta["span_time_slot"] = part.iloc[0]["span_time_slot"]
        for lots in range(1, MAX_CAPACITY_LOTS + 1):
            rows.append(_execution_record(part, structure=STRUCTURE, lots=lots, metadata=meta))
    return pd.DataFrame(rows).sort_values(["trade_id", "lots"])


def attach_one_lot_outcomes(scored: pd.DataFrame, surface: pd.DataFrame) -> pd.DataFrame:
    columns = {
        "gross_pnl_rupees": "one_lot_gross_pnl",
        "net_pnl_rupees": "one_lot_net_pnl",
        "total_cost_rupees": "one_lot_total_cost",
        "margin_rupees": "one_lot_margin",
        "max_loss_rupees": "one_lot_max_loss",
    }
    one = surface.loc[surface["lots"].eq(1), ["trade_id", *columns]].rename(columns=columns)
    result = scored.merge(one, on="trade_id", how="inner")
    result["one_lot_net_risk_return"] = (
        result["one_lot_net_pnl"] / result["one_lot_max_loss"].clip(lower=1e-12)
    )
    return result


def _split_parts(scored: pd.DataFrame) -> dict[str, pd.DataFrame]:
    return {
        "discovery_2021_2023": scored.loc[scored["split"].eq("discovery_2021_2023")],
        "validation_2024": scored.loc[scored["split"].eq("validation_2024")],
        "confirmation_2025_2026": scored.loc[
            scored["split"].eq("confirmation_2025_2026")
        ],
        "holdout_2024_2026": scored.loc[~scored["split"].eq("discovery_2021_2023")],
        "full_sample": scored,
    }


def build_rank_diagnostics(scored: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rank_rows: list[dict[str, Any]] = []
    quintile_rows: list[dict[str, Any]] = []
    for score_index, score_type in enumerate(SCORE_TYPES):
        score_column = f"{score_type}_score"
        for split_index, (split, part) in enumerate(_split_parts(scored).items()):
            rho, lower, upper, p_value = _rank_inference(
                part[score_column],
                part["one_lot_net_pnl"],
                seed=SEED + score_index * 100 + split_index,
            )
            rank_rows.append(
                {
                    "score_type": score_type,
                    "split": split,
                    "trades": int(len(part)),
                    "rho_one_lot_net_pnl": rho,
                    "rho_net_bootstrap_ci_low": lower,
                    "rho_net_bootstrap_ci_high": upper,
                    "net_permutation_p_one_sided": p_value,
                    "rho_one_lot_gross_pnl": spearman_rank_correlation(
                        part[score_column], part["one_lot_gross_pnl"]
                    ),
                    "rho_one_lot_net_risk_return": spearman_rank_correlation(
                        part[score_column], part["one_lot_net_risk_return"]
                    ),
                }
            )
            bucketed = part.assign(
                score_quintile=pd.cut(
                    part[score_column],
                    [0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
                    labels=("Q1", "Q2", "Q3", "Q4", "Q5"),
                    include_lowest=True,
                )
            )
            for quintile, cell in bucketed.groupby(
                "score_quintile", observed=True, sort=True
            ):
                quintile_rows.append(
                    {
                        "score_type": score_type,
                        "split": split,
                        "score_quintile": str(quintile),
                        "trades": int(len(cell)),
                        "mean_score": float(cell[score_column].mean()),
                        "mean_one_lot_gross_pnl": float(cell["one_lot_gross_pnl"].mean()),
                        "mean_one_lot_net_pnl": float(cell["one_lot_net_pnl"].mean()),
                        "median_one_lot_net_pnl": float(cell["one_lot_net_pnl"].median()),
                        "net_win_rate": float((cell["one_lot_net_pnl"] > 0).mean()),
                        "mean_net_risk_return": float(cell["one_lot_net_risk_return"].mean()),
                    }
                )
    return pd.DataFrame(rank_rows), pd.DataFrame(quintile_rows)


def simulate_confidence_portfolio(
    surface: pd.DataFrame,
    scored: pd.DataFrame,
    *,
    score_type: str,
    capacity_cap: int,
    initial_capital: float,
) -> pd.DataFrame:
    lookup = surface.set_index(["trade_id", "lots"])
    one_lot = surface.loc[surface["lots"].eq(1)].set_index("trade_id")
    score_column = f"{score_type}_score"
    equity = float(initial_capital)
    rows: list[dict[str, Any]] = []
    copy_columns = (
        "gross_pnl_rupees",
        "net_pnl_rupees",
        "turnover_rupees",
        "base_slippage_rupees",
        "ladder_impact_rupees",
        "volume_impact_rupees",
        "oi_impact_rupees",
        "impact_rupees",
        "slippage_rupees",
        "charges_rupees",
        "brokerage_rupees",
        "stt_rupees",
        "stamp_duty_rupees",
        "exchange_charges_rupees",
        "sebi_charges_rupees",
        "ipft_rupees",
        "gst_rupees",
        "total_cost_rupees",
        "margin_rupees",
        "max_loss_rupees",
        "entry_dte",
        "span_time_slot",
    )
    for event in scored.sort_values("entry_ts").itertuples(index=False):
        trade_id = int(event.trade_id)
        one = one_lot.loc[trade_id]
        score = float(getattr(event, score_column))
        risk_fraction = risk_fraction_for_score(score)
        margin_cap = math.floor(
            equity * MARGIN_FRACTION / max(float(one["margin_rupees"]), 1e-12)
        )
        risk_cap = (
            0
            if risk_fraction == 0.0
            else math.floor(
                equity * risk_fraction / max(float(one["max_loss_rupees"]), 1e-12)
            )
        )
        lots = max(min(capacity_cap, margin_cap, risk_cap), 0)
        common = {
            "trade_id": trade_id,
            "trade_date": str(event.trade_date),
            "entry_ts": pd.Timestamp(event.entry_ts),
            "split": str(event.split),
            "structure": STRUCTURE,
            "policy": score_type,
            "confidence_score": score,
            "risk_fraction": risk_fraction,
            "margin_fraction": MARGIN_FRACTION,
            "capacity_cap_lots": capacity_cap,
            "margin_cap_lots": margin_cap,
            "risk_cap_lots": risk_cap,
            "lots": lots,
            "equity_before": equity,
        }
        if lots <= 0:
            rows.append(
                {
                    **common,
                    "executed": False,
                    "skip_reason": "confidence_switch_or_cap_below_one_lot",
                    **{column: 0.0 for column in copy_columns},
                    "equity_after": equity,
                    "margin_utilization": 0.0,
                    "max_loss_utilization": 0.0,
                }
            )
            continue
        selected = lookup.loc[(trade_id, lots)]
        before = equity
        equity += float(selected["net_pnl_rupees"])
        rows.append(
            {
                **common,
                "executed": True,
                "skip_reason": "",
                **{column: selected[column] for column in copy_columns},
                "equity_after": equity,
                "margin_utilization": float(selected["margin_rupees"]) / before,
                "max_loss_utilization": float(selected["max_loss_rupees"]) / before,
            }
        )
    return pd.DataFrame(rows)


def _economic_rows(
    portfolios: pd.DataFrame,
    baseline: pd.DataFrame,
    initial_capital: float,
) -> tuple[pd.DataFrame, dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    details: dict[str, dict[str, Any]] = {}
    sources = [("fixed_balanced", baseline)] + [
        (score_type, portfolios.loc[portfolios["policy"].eq(score_type)])
        for score_type in SCORE_TYPES
    ]
    for policy, part in sources:
        metrics = summarize_portfolio(part, initial_capital)
        details[policy] = metrics
        rows.append({"policy": policy, "split": "full_sample", **metrics})
        for split, cell in _split_parts(part).items():
            if split == "full_sample":
                continue
            executed = cell.loc[cell["executed"]]
            rows.append(
                {
                    "policy": policy,
                    "split": split,
                    "eligible_signals": int(len(cell)),
                    "executed_trades": int(len(executed)),
                    "gross_pnl": float(executed["gross_pnl_rupees"].sum()),
                    "total_cost": float(executed["total_cost_rupees"].sum()),
                    "net_profit": float(executed["net_pnl_rupees"].sum()),
                    "average_net_trade": float(executed["net_pnl_rupees"].mean()),
                    "win_rate": float((executed["net_pnl_rupees"] > 0).mean()),
                    "turnover": float(executed["turnover_rupees"].sum()),
                    "average_lots": float(executed["lots"].mean()),
                }
            )
    return pd.DataFrame(rows), details


def _evaluate_pass(
    rank: pd.DataFrame,
    economic: pd.DataFrame,
    quintiles: pd.DataFrame,
) -> dict[str, Any]:
    primary = rank.loc[rank["score_type"].eq("regime_composite")].set_index("split")
    holdout = primary.loc["holdout_2024_2026"]
    rho_by_split = {
        split: float(primary.loc[split, "rho_one_lot_net_pnl"])
        for split in ("validation_2024", "confirmation_2025_2026")
    }
    holdout_quintiles = quintiles.loc[
        quintiles["score_type"].eq("regime_composite")
        & quintiles["split"].eq("holdout_2024_2026")
    ].set_index("score_quintile")
    top_bottom_order = (
        "Q1" in holdout_quintiles.index
        and "Q5" in holdout_quintiles.index
        and float(holdout_quintiles.loc["Q5", "mean_one_lot_net_pnl"])
        > float(holdout_quintiles.loc["Q1", "mean_one_lot_net_pnl"])
    )
    economics = economic.loc[economic["split"].eq("holdout_2024_2026")].set_index("policy")
    primary_net = float(economics.loc["regime_composite", "net_profit"])
    baseline_net = float(economics.loc["fixed_balanced", "net_profit"])
    criteria = {
        "combined_holdout_rho_at_least_0_20": float(holdout["rho_one_lot_net_pnl"]) >= 0.20,
        "combined_holdout_bootstrap_ci_low_above_zero": float(
            holdout["rho_net_bootstrap_ci_low"]
        )
        > 0.0,
        "combined_holdout_one_sided_permutation_p_at_most_0_05": float(
            holdout["net_permutation_p_one_sided"]
        )
        <= 0.05,
        "positive_rho_in_2024_and_2025_2026": all(value > 0.0 for value in rho_by_split.values()),
        "holdout_top_quintile_mean_exceeds_bottom": bool(top_bottom_order),
        "holdout_confidence_sizing_net_positive": primary_net > 0.0,
        "holdout_confidence_sizing_beats_fixed_balanced": primary_net > baseline_net,
    }
    return {
        "verdict": "PASS" if all(criteria.values()) else "FAIL",
        "criteria": criteria,
        "combined_holdout_rho": float(holdout["rho_one_lot_net_pnl"]),
        "combined_holdout_bootstrap_ci": [
            float(holdout["rho_net_bootstrap_ci_low"]),
            float(holdout["rho_net_bootstrap_ci_high"]),
        ],
        "combined_holdout_permutation_p_one_sided": float(
            holdout["net_permutation_p_one_sided"]
        ),
        "rho_by_later_split": rho_by_split,
        "holdout_confidence_sizing_net": primary_net,
        "holdout_fixed_balanced_net": baseline_net,
    }


def render_report(result: dict[str, Any]) -> str:
    verdict = result["pass_evaluation"]
    rank_rows = result["rank_summary"]
    economic_rows = result["economic_summary"]
    lines = [
        "# Phase 9 — confidence-ranked sizing diagnostic",
        "",
        "## Verdict",
        "",
        f"**{verdict['verdict']}** under the frozen rank-correlation and economic gates.",
        "",
        "The rank test uses one-lot outcomes before confidence controls quantity. This prevents an ",
        "endogenous correlation between a higher score, more lots, and larger rupee P&L.",
        "",
        "## Frozen score and sizing contract",
        "",
        "- Primary score: 50% gate cushion, 15% inverse IV percentile, 15% inverse RV percentile, ",
        "  10% inverse DTE percentile, and 10% entry-time score.",
        "- Comparator: gate-cushion percentile only.",
        "- Every percentile transform uses the 2021–2023 discovery distribution only.",
        "- Score quintile risk ladder: 0%, 0.5%, 1.0%, 1.5%, and 2.0% of current equity.",
        "- Lots remain capped by 50% entry SPAN and the 76-lot discovery capacity ceiling.",
        "- Exact quantity-aware costs are recomputed at the selected integer lot count.",
        "",
        "## Rank correlation",
        "",
        "| Score | Split | N | Spearman rho: net | 95% bootstrap CI | One-sided p | Rho: risk return |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for row in rank_rows:
        lines.append(
            f"| {row['score_type']} | {row['split']} | {row['trades']} | "
            f"{row['rho_one_lot_net_pnl']:.3f} | "
            f"[{row['rho_net_bootstrap_ci_low']:.3f}, {row['rho_net_bootstrap_ci_high']:.3f}] | "
            f"{row['net_permutation_p_one_sided']:.4f} | "
            f"{row['rho_one_lot_net_risk_return']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Capital-path comparison",
            "",
            "| Policy | Split | Eligible | Executed | Net P&L | Mean executed trade | Win rate | Average lots |",
            "|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in economic_rows:
        lines.append(
            f"| {row['policy']} | {row['split']} | {row['eligible_signals']} | "
            f"{row['executed_trades']} | ₹{row['net_profit']:,.2f} | "
            f"₹{row['average_net_trade']:,.2f} | {row['win_rate']:.2%} | "
            f"{row['average_lots']:.2f} |"
        )
    lines.extend(
        [
            "",
            "## Frozen pass criteria",
            "",
        ]
    )
    criterion_order = (
        "combined_holdout_rho_at_least_0_20",
        "combined_holdout_bootstrap_ci_low_above_zero",
        "combined_holdout_one_sided_permutation_p_at_most_0_05",
        "positive_rho_in_2024_and_2025_2026",
        "holdout_top_quintile_mean_exceeds_bottom",
        "holdout_confidence_sizing_net_positive",
        "holdout_confidence_sizing_beats_fixed_balanced",
    )
    for name in criterion_order:
        passed = verdict["criteria"][name]
        lines.append(f"- [{'x' if passed else ' '}] {name}")
    lines.extend(
        [
            "",
            "## Interpretation boundary",
            "",
            "The regime directions were identified after inspecting the existing sample. Therefore, ",
            "even a statistical pass here would be a research pass rather than pristine OOS evidence. ",
            "A deployment claim still requires untouched forward data.",
            "",
            "## Reproduce",
            "",
            "```powershell",
            "python -m research.phase9.run_confidence_sizing",
            "python -m pytest tests/test_phase9_confidence_sizing.py -q",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    event_path: Path,
    observation_path: Path,
    phase4_tradebook_path: Path,
    phase8_tradebook_path: Path,
    phase8_tearsheet_path: Path,
    scored_event_path: Path,
    rank_path: Path,
    quintile_path: Path,
    tradebook_path: Path,
    economic_path: Path,
    equity_path: Path,
    tearsheet_path: Path,
    report_path: Path,
    manifest_path: Path,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, Any]:
    events = pd.read_csv(event_path, parse_dates=["entry_ts"])
    observations = pd.read_parquet(observation_path)
    observations["entry_ts"] = pd.to_datetime(observations["entry_ts"], utc=True)
    phase4_tradebook = pd.read_csv(phase4_tradebook_path)
    phase8_tradebook = pd.read_csv(phase8_tradebook_path, parse_dates=["entry_ts"])
    phase8_tearsheet = json.loads(phase8_tearsheet_path.read_text(encoding="utf-8"))
    capacity_cap = int(phase8_tearsheet["capacity_caps"][STRUCTURE])
    scored = build_scores(events)
    surface = build_fly_cost_surface(observations, phase4_tradebook, scored)
    scored = attach_one_lot_outcomes(scored, surface)
    rank, quintiles = build_rank_diagnostics(scored)
    portfolios = pd.concat(
        [
            simulate_confidence_portfolio(
                surface,
                scored,
                score_type=score_type,
                capacity_cap=capacity_cap,
                initial_capital=initial_capital,
            )
            for score_type in SCORE_TYPES
        ],
        ignore_index=True,
    )
    baseline = phase8_tradebook.loc[
        phase8_tradebook["structure"].eq(STRUCTURE)
        & phase8_tradebook["policy"].eq("balanced")
    ].copy()
    economic, policy_details = _economic_rows(portfolios, baseline, initial_capital)
    pass_evaluation = _evaluate_pass(rank, economic, quintiles)
    equity_rows = []
    for policy, part in [("fixed_balanced", baseline)] + [
        (score_type, portfolios.loc[portfolios["policy"].eq(score_type)])
        for score_type in SCORE_TYPES
    ]:
        curve = _daily_curve(part, initial_capital)
        curve.insert(0, "policy", policy)
        equity_rows.append(curve)
    equity = pd.concat(equity_rows, ignore_index=True)
    result = {
        "schema_version": SCHEMA_VERSION,
        "initial_capital": initial_capital,
        "structure": STRUCTURE,
        "capacity_cap_lots": capacity_cap,
        "score_weights": {
            "gate_strength": 0.50,
            "inverse_iv_percentile": 0.15,
            "inverse_rv_percentile": 0.15,
            "inverse_dte_percentile": 0.10,
            "time_score": 0.10,
        },
        "risk_ladder": {"Q1": 0.0, "Q2": 0.005, "Q3": 0.010, "Q4": 0.015, "Q5": 0.020},
        "rank_pass_thresholds": {
            "holdout_rho_minimum": 0.20,
            "bootstrap_ci_low_must_exceed": 0.0,
            "one_sided_permutation_p_maximum": 0.05,
            "both_later_split_rhos_must_be_positive": True,
        },
        "pass_evaluation": pass_evaluation,
        "rank_summary": rank.to_dict(orient="records"),
        "economic_summary": economic.loc[
            economic["split"].isin(("full_sample", "holdout_2024_2026"))
        ].to_dict(orient="records"),
        "policy_details": policy_details,
        "limitations": [
            "Regime directions are post-hoc; later calendar labels are not pristine OOS evidence.",
            "The structural risk budget excludes transaction costs and intratrade SPAN expansion.",
            "Historical close, minute volume and OI replace observed order-book fills.",
        ],
    }
    outputs = (
        scored_event_path,
        rank_path,
        quintile_path,
        tradebook_path,
        economic_path,
        equity_path,
        tearsheet_path,
        report_path,
        manifest_path,
    )
    for path in outputs:
        path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(scored_event_path, index=False)
    rank.to_csv(rank_path, index=False)
    quintiles.to_csv(quintile_path, index=False)
    portfolios.to_csv(tradebook_path, index=False)
    economic.to_csv(economic_path, index=False)
    equity.to_csv(equity_path, index=False)
    tearsheet_path.write_text(
        json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report = "\n".join(line.rstrip() for line in render_report(result).splitlines()) + "\n"
    report_path.write_text(report, encoding="utf-8", newline="\n")
    output_without_manifest = outputs[:-1]
    manifest = {
        "schema_version": "phase9-confidence-sizing-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)}
            for path in (
                event_path,
                observation_path,
                phase4_tradebook_path,
                phase8_tradebook_path,
                phase8_tearsheet_path,
            )
        ],
        "outputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)}
            for path in output_without_manifest
        ],
        "impact_model": {
            "name": "additive_ladder_participation_v2",
            "parameters": asdict(CAPACITY_IMPACT_PARAMETERS),
        },
        "verdict": pass_evaluation["verdict"],
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", type=Path, default=Path("audit/phase8_gated_events.csv"))
    parser.add_argument(
        "--observations", type=Path, default=Path("audit/phase4_cost_aware_observations.parquet")
    )
    parser.add_argument(
        "--phase4-tradebook", type=Path, default=Path("audit/phase4_cost_aware_tradebook.csv")
    )
    parser.add_argument(
        "--phase8-tradebook", type=Path, default=Path("audit/phase8_gated_capital_tradebook.csv")
    )
    parser.add_argument(
        "--phase8-tearsheet", type=Path, default=Path("audit/phase8_gated_tearsheet.json")
    )
    parser.add_argument("--scored-events", type=Path, default=Path("audit/phase9_scored_events.csv"))
    parser.add_argument("--rank", type=Path, default=Path("audit/phase9_rank_correlation.csv"))
    parser.add_argument("--quintiles", type=Path, default=Path("audit/phase9_score_quintiles.csv"))
    parser.add_argument(
        "--tradebook", type=Path, default=Path("audit/phase9_confidence_tradebook.csv")
    )
    parser.add_argument("--economic", type=Path, default=Path("audit/phase9_economic_summary.csv"))
    parser.add_argument("--equity", type=Path, default=Path("audit/phase9_equity_curve.csv"))
    parser.add_argument("--tearsheet", type=Path, default=Path("audit/phase9_tearsheet.json"))
    parser.add_argument(
        "--report", type=Path, default=Path("docs/research/PHASE9_CONFIDENCE_SIZING.md")
    )
    parser.add_argument("--manifest", type=Path, default=Path("audit/phase9_manifest.json"))
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        event_path=args.events,
        observation_path=args.observations,
        phase4_tradebook_path=args.phase4_tradebook,
        phase8_tradebook_path=args.phase8_tradebook,
        phase8_tearsheet_path=args.phase8_tearsheet,
        scored_event_path=args.scored_events,
        rank_path=args.rank,
        quintile_path=args.quintiles,
        tradebook_path=args.tradebook,
        economic_path=args.economic,
        equity_path=args.equity,
        tearsheet_path=args.tearsheet,
        report_path=args.report,
        manifest_path=args.manifest,
        initial_capital=args.initial_capital,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
