"""Explore margin-efficient sizing heuristics for the fixed Phase 9 fly score."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import itertools
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.phase8.run_gated_capital_backtest import (
    CAPACITY_IMPACT_PARAMETERS,
    INITIAL_CAPITAL,
    _json_safe,
)
from research.phase9.run_confidence_sizing import (
    STRUCTURE,
    build_fly_cost_surface,
    spearman_rank_correlation,
)


SCHEMA_VERSION = "phase10-sizing-exploration/v1"
CAPACITY_CAP = 76
MIN_DISCOVERY_TRADES = 40
MARGIN_FRACTIONS = (0.25, 0.35, 0.50, 0.65, 0.80, 1.00)
MAX_RISK_FRACTIONS = (0.005, 0.010, 0.015, 0.020, 0.025, 0.030, 0.040)
SCORE_FLOORS = (0.0, 0.20, 0.40, 0.60, 0.80)
SCORE_POWERS = (0.0, 0.5, 1.0, 2.0)
BRAKE_VARIANTS = (
    (None, 1.0),
    (0.005, 0.50),
    (0.005, 0.25),
    (0.010, 0.50),
    (0.010, 0.25),
    (0.015, 0.50),
    (0.015, 0.25),
)
STREAK_TRIGGERS = (None, 2, 3)


@dataclass(frozen=True)
class SizingConfig:
    config_id: int
    margin_fraction: float
    max_risk_fraction: float
    score_floor: float
    score_power: float
    drawdown_brake_threshold: float | None
    drawdown_brake_multiplier: float
    losing_streak_trigger: int | None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def generate_configs() -> list[SizingConfig]:
    configs = []
    product = itertools.product(
        MARGIN_FRACTIONS,
        MAX_RISK_FRACTIONS,
        SCORE_FLOORS,
        SCORE_POWERS,
        BRAKE_VARIANTS,
        STREAK_TRIGGERS,
    )
    for config_id, (margin, risk, floor, power, brake, streak) in enumerate(product):
        threshold, multiplier = brake
        configs.append(
            SizingConfig(
                config_id=config_id,
                margin_fraction=margin,
                max_risk_fraction=risk,
                score_floor=floor,
                score_power=power,
                drawdown_brake_threshold=threshold,
                drawdown_brake_multiplier=multiplier,
                losing_streak_trigger=streak,
            )
        )
    return configs


def score_scaled_risk_fraction(
    score: float,
    *,
    score_floor: float,
    score_power: float,
    maximum: float,
) -> float:
    if not math.isfinite(score) or score <= score_floor:
        return 0.0
    normalized = min(max((score - score_floor) / max(1.0 - score_floor, 1e-12), 0.0), 1.0)
    multiplier = 1.0 if score_power == 0.0 else normalized**score_power
    return maximum * multiplier


def effective_risk_fraction(
    config: SizingConfig,
    *,
    score: float,
    current_drawdown: float,
    losing_streak: int,
) -> float:
    fraction = score_scaled_risk_fraction(
        score,
        score_floor=config.score_floor,
        score_power=config.score_power,
        maximum=config.max_risk_fraction,
    )
    threshold = config.drawdown_brake_threshold
    if threshold is not None and current_drawdown <= -threshold:
        fraction *= config.drawdown_brake_multiplier
    if config.losing_streak_trigger is not None and losing_streak >= config.losing_streak_trigger:
        fraction *= 0.50
    return fraction


def risk_cap_with_cost_reserve(
    *,
    equity: float,
    risk_fraction: float,
    max_loss_per_lot: float,
    cost_reserve: np.ndarray,
) -> int:
    if risk_fraction <= 0.0:
        return 0
    lots = np.arange(1, len(cost_reserve), dtype=float)
    requirements = max_loss_per_lot * lots + cost_reserve[1:]
    return int(np.count_nonzero(requirements <= equity * risk_fraction))


def build_cost_reserve(surface: pd.DataFrame) -> pd.DataFrame:
    discovery = surface.loc[surface["split"].eq("discovery_2021_2023")]
    reserve = (
        discovery.groupby("lots", as_index=False)["total_cost_rupees"]
        .quantile(0.95)
        .rename(columns={"total_cost_rupees": "discovery_q95_total_cost_reserve"})
        .sort_values("lots")
    )
    reserve["reserve_per_lot"] = reserve["discovery_q95_total_cost_reserve"] / reserve["lots"]
    return reserve


def _max_drawdown(pnl: np.ndarray, initial_capital: float) -> tuple[float, float]:
    equity = initial_capital + np.cumsum(pnl)
    if equity.size == 0:
        return 0.0, 0.0
    equity = np.concatenate(([initial_capital], equity))
    peak = np.maximum.accumulate(equity)
    drawdown = equity - peak
    pct = equity / peak - 1.0
    return float(drawdown.min()), float(pct.min())


def _cvar(values: np.ndarray, fraction: float = 0.05) -> float:
    clean = values[np.isfinite(values)]
    if clean.size == 0:
        return 0.0
    count = max(1, math.ceil(fraction * clean.size))
    return float(np.sort(clean)[:count].mean())


def prepare_matrices(
    surface: pd.DataFrame,
    scored: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, np.ndarray]]:
    events = scored.sort_values("entry_ts").reset_index(drop=True).copy()
    trade_ids = events["trade_id"].astype(int).tolist()
    matrices: dict[str, np.ndarray] = {}
    value_columns = (
        "net_pnl_rupees",
        "gross_pnl_rupees",
        "total_cost_rupees",
        "turnover_rupees",
        "margin_rupees",
    )
    for column in value_columns:
        pivot = surface.pivot(index="trade_id", columns="lots", values=column).reindex(trade_ids)
        matrix = np.zeros((len(events), 101), dtype=float)
        matrix[:, 1:] = pivot.reindex(columns=range(1, 101)).to_numpy(dtype=float)
        matrices[column] = matrix
    one = surface.loc[surface["lots"].eq(1)].set_index("trade_id").reindex(trade_ids)
    matrices["margin_per_lot"] = one["margin_rupees"].to_numpy(dtype=float)
    matrices["max_loss_per_lot"] = one["max_loss_rupees"].to_numpy(dtype=float)
    return events, matrices


def _segment_metrics(
    pnl: np.ndarray,
    gross: np.ndarray,
    cost: np.ndarray,
    turnover: np.ndarray,
    margin: np.ndarray,
    lots: np.ndarray,
    mask: np.ndarray,
    initial_capital: float,
) -> dict[str, float | int]:
    selected_pnl = pnl[mask]
    selected_lots = lots[mask]
    executed = selected_lots > 0
    executed_pnl = selected_pnl[executed]
    drawdown, drawdown_pct = _max_drawdown(selected_pnl, initial_capital)
    total_margin = float(margin[mask].sum())
    return {
        "signals": int(mask.sum()),
        "trades": int(executed.sum()),
        "net": float(selected_pnl.sum()),
        "gross": float(gross[mask].sum()),
        "cost": float(cost[mask].sum()),
        "turnover": float(turnover[mask].sum()),
        "sum_margin": total_margin,
        "margin_efficiency": float(selected_pnl.sum() / total_margin) if total_margin > 0 else 0.0,
        "max_drawdown": drawdown,
        "max_drawdown_pct": drawdown_pct,
        "cvar05_trade": _cvar(executed_pnl),
        "win_rate": float((executed_pnl > 0).mean()) if executed_pnl.size else 0.0,
        "average_lots": float(selected_lots[executed].mean()) if executed.any() else 0.0,
        "maximum_lots": int(selected_lots.max()) if selected_lots.size else 0,
    }


def simulate_grid(
    events: pd.DataFrame,
    matrices: dict[str, np.ndarray],
    configs: list[SizingConfig],
    cost_reserve: np.ndarray,
    *,
    initial_capital: float,
) -> pd.DataFrame:
    score = events["regime_composite_score"].to_numpy(dtype=float)
    split = events["split"].astype(str).to_numpy()
    year = pd.to_datetime(events["trade_date"]).dt.year.to_numpy(dtype=int)
    masks = {
        "discovery": split == "discovery_2021_2023",
        "validation": split == "validation_2024",
        "confirmation": split == "confirmation_2025_2026",
        "holdout": split != "discovery_2021_2023",
        "full": np.ones(len(events), dtype=bool),
    }
    rows: list[dict[str, Any]] = []
    for config in configs:
        equity = float(initial_capital)
        peak = equity
        losing_streak = 0
        pnl = np.zeros(len(events), dtype=float)
        gross = np.zeros(len(events), dtype=float)
        cost = np.zeros(len(events), dtype=float)
        turnover = np.zeros(len(events), dtype=float)
        margin = np.zeros(len(events), dtype=float)
        lots = np.zeros(len(events), dtype=int)
        margin_utilization = np.zeros(len(events), dtype=float)
        for index in range(len(events)):
            before = equity
            current_drawdown = equity / peak - 1.0
            risk_fraction = effective_risk_fraction(
                config,
                score=score[index],
                current_drawdown=current_drawdown,
                losing_streak=losing_streak,
            )
            margin_cap = math.floor(
                equity * config.margin_fraction
                / max(float(matrices["margin_per_lot"][index]), 1e-12)
            )
            risk_cap = risk_cap_with_cost_reserve(
                equity=equity,
                risk_fraction=risk_fraction,
                max_loss_per_lot=float(matrices["max_loss_per_lot"][index]),
                cost_reserve=cost_reserve,
            )
            selected_lots = max(min(CAPACITY_CAP, margin_cap, risk_cap), 0)
            lots[index] = selected_lots
            if selected_lots == 0:
                continue
            pnl[index] = matrices["net_pnl_rupees"][index, selected_lots]
            gross[index] = matrices["gross_pnl_rupees"][index, selected_lots]
            cost[index] = matrices["total_cost_rupees"][index, selected_lots]
            turnover[index] = matrices["turnover_rupees"][index, selected_lots]
            margin[index] = matrices["margin_rupees"][index, selected_lots]
            margin_utilization[index] = margin[index] / before
            equity += pnl[index]
            peak = max(peak, equity)
            losing_streak = losing_streak + 1 if pnl[index] < 0 else 0
        row: dict[str, Any] = asdict(config)
        for label, mask in masks.items():
            metrics = _segment_metrics(
                pnl, gross, cost, turnover, margin, lots, mask, initial_capital
            )
            row.update({f"{label}_{name}": value for name, value in metrics.items()})
        row["discovery_2021_net"] = float(pnl[year == 2021].sum())
        row["discovery_2022_net"] = float(pnl[year == 2022].sum())
        row["discovery_2023_net"] = float(pnl[year == 2023].sum())
        row["discovery_worst_year_net"] = min(
            row["discovery_2021_net"],
            row["discovery_2022_net"],
            row["discovery_2023_net"],
        )
        row["full_average_margin_utilization"] = float(
            margin_utilization[lots > 0].mean() if (lots > 0).any() else 0.0
        )
        row["full_maximum_margin_utilization"] = float(margin_utilization.max())
        rows.append(row)
    return pd.DataFrame(rows)


def select_profiles(grid: pd.DataFrame) -> pd.DataFrame:
    base = grid.loc[
        (grid["discovery_trades"] >= MIN_DISCOVERY_TRADES)
        & (grid["discovery_net"] > 0.0)
    ].copy()
    selections: list[dict[str, Any]] = []
    for budget in (0.005, 0.010, 0.015, 0.020):
        eligible = base.loc[base["discovery_max_drawdown_pct"].abs() <= budget]
        if len(eligible):
            best = eligible.loc[eligible["discovery_net"].idxmax()].to_dict()
            best["profile"] = f"max_return_dd_{budget:.1%}"
            selections.append(best)
    efficient = base.loc[base["discovery_max_drawdown_pct"].abs() <= 0.015]
    if len(efficient):
        best = efficient.loc[efficient["discovery_margin_efficiency"].idxmax()].to_dict()
        best["profile"] = "max_margin_efficiency_dd_1.5%"
        selections.append(best)
    stable = base.loc[base["discovery_max_drawdown_pct"].abs() <= 0.020]
    if len(stable):
        best = stable.loc[stable["discovery_worst_year_net"].idxmax()].to_dict()
        best["profile"] = "max_worst_year_dd_2.0%"
        selections.append(best)
    controlled = base.loc[
        (base["discovery_max_drawdown_pct"].abs() <= 0.010)
        & (
            base["drawdown_brake_threshold"].notna()
            | base["losing_streak_trigger"].notna()
        )
    ]
    if len(controlled):
        best = controlled.loc[controlled["discovery_net"].idxmax()].to_dict()
        best["profile"] = "active_brake_max_return_dd_1.0%"
        selections.append(best)
    smooth = base.loc[
        (base["discovery_max_drawdown_pct"].abs() <= 0.010) & (base["score_power"] > 0.0)
    ]
    if len(smooth):
        best = smooth.loc[smooth["discovery_net"].idxmax()].to_dict()
        best["profile"] = "smooth_confidence_max_return_dd_1.0%"
        selections.append(best)
    low_margin = base.loc[
        (base["discovery_max_drawdown_pct"].abs() <= 0.010)
        & (base["margin_fraction"] <= 0.35)
    ]
    if len(low_margin):
        best = low_margin.loc[low_margin["discovery_net"].idxmax()].to_dict()
        best["profile"] = "low_margin_max_return_dd_1.0%"
        selections.append(best)
    return pd.DataFrame(selections).drop_duplicates("profile").sort_values("profile")


def build_dimension_summary(grid: pd.DataFrame) -> pd.DataFrame:
    universe = grid.loc[
        (grid["discovery_trades"] >= MIN_DISCOVERY_TRADES)
        & (grid["discovery_net"] > 0.0)
        & (grid["discovery_max_drawdown_pct"].abs() <= 0.020)
    ].copy()
    universe["both_later_positive"] = (
        (universe["validation_net"] > 0.0) & (universe["confirmation_net"] > 0.0)
    )
    rows: list[dict[str, Any]] = []
    for dimension in (
        "score_floor",
        "score_power",
        "margin_fraction",
        "max_risk_fraction",
        "drawdown_brake_threshold",
        "losing_streak_trigger",
    ):
        values = universe[dimension].astype(object).where(universe[dimension].notna(), "none")
        for value, part in universe.assign(_dimension_value=values).groupby(
            "_dimension_value", sort=True
        ):
            rows.append(
                {
                    "dimension": dimension,
                    "value": str(value),
                    "policies": int(len(part)),
                    "median_discovery_net": float(part["discovery_net"].median()),
                    "median_holdout_net": float(part["holdout_net"].median()),
                    "positive_holdout_rate": float((part["holdout_net"] > 0.0).mean()),
                    "positive_both_later_rate": float(part["both_later_positive"].mean()),
                    "median_full_drawdown_pct": float(part["full_max_drawdown_pct"].median()),
                    "median_full_margin_efficiency": float(
                        part["full_margin_efficiency"].median()
                    ),
                }
            )
    return pd.DataFrame(rows)


def robustness_summary(grid: pd.DataFrame) -> dict[str, Any]:
    universe = grid.loc[
        (grid["discovery_trades"] >= MIN_DISCOVERY_TRADES)
        & (grid["discovery_net"] > 0.0)
        & (grid["discovery_max_drawdown_pct"].abs() <= 0.020)
    ].copy()
    cutoff = float(universe["discovery_net"].quantile(0.90))
    top = universe.loc[universe["discovery_net"] >= cutoff]
    return {
        "eligible_policy_count": int(len(universe)),
        "positive_holdout_rate": float((universe["holdout_net"] > 0).mean()),
        "positive_both_later_splits_rate": float(
            ((universe["validation_net"] > 0) & (universe["confirmation_net"] > 0)).mean()
        ),
        "discovery_holdout_net_spearman": spearman_rank_correlation(
            universe["discovery_net"], universe["holdout_net"]
        ),
        "top_discovery_decile_count": int(len(top)),
        "top_discovery_decile_median_holdout_net": float(top["holdout_net"].median()),
        "top_discovery_decile_positive_holdout_rate": float((top["holdout_net"] > 0).mean()),
    }


def detailed_selected_tradebook(
    profiles: pd.DataFrame,
    configs: list[SizingConfig],
    events: pd.DataFrame,
    matrices: dict[str, np.ndarray],
    cost_reserve: np.ndarray,
    *,
    initial_capital: float,
) -> pd.DataFrame:
    by_id = {config.config_id: config for config in configs}
    score = events["regime_composite_score"].to_numpy(dtype=float)
    rows: list[dict[str, Any]] = []
    for profile in profiles.itertuples(index=False):
        config = by_id[int(profile.config_id)]
        equity = float(initial_capital)
        peak = equity
        losing_streak = 0
        for index, event in enumerate(events.itertuples(index=False)):
            before = equity
            drawdown = equity / peak - 1.0
            risk_fraction = effective_risk_fraction(
                config,
                score=score[index],
                current_drawdown=drawdown,
                losing_streak=losing_streak,
            )
            margin_cap = math.floor(
                equity * config.margin_fraction
                / max(float(matrices["margin_per_lot"][index]), 1e-12)
            )
            risk_cap = risk_cap_with_cost_reserve(
                equity=equity,
                risk_fraction=risk_fraction,
                max_loss_per_lot=float(matrices["max_loss_per_lot"][index]),
                cost_reserve=cost_reserve,
            )
            lots = max(min(CAPACITY_CAP, margin_cap, risk_cap), 0)
            net = float(matrices["net_pnl_rupees"][index, lots]) if lots else 0.0
            margin = float(matrices["margin_rupees"][index, lots]) if lots else 0.0
            equity += net
            peak = max(peak, equity)
            losing_streak = losing_streak + 1 if lots and net < 0 else 0
            rows.append(
                {
                    "profile": profile.profile,
                    "config_id": config.config_id,
                    "trade_id": int(event.trade_id),
                    "trade_date": str(event.trade_date),
                    "entry_ts": event.entry_ts,
                    "split": str(event.split),
                    "confidence_score": score[index],
                    "risk_fraction": risk_fraction,
                    "drawdown_before": drawdown,
                    "losing_streak_before": losing_streak if lots == 0 else max(losing_streak - 1, 0),
                    "margin_cap_lots": margin_cap,
                    "risk_cap_lots": risk_cap,
                    "lots": lots,
                    "gross_pnl_rupees": float(matrices["gross_pnl_rupees"][index, lots])
                    if lots
                    else 0.0,
                    "total_cost_rupees": float(matrices["total_cost_rupees"][index, lots])
                    if lots
                    else 0.0,
                    "net_pnl_rupees": net,
                    "turnover_rupees": float(matrices["turnover_rupees"][index, lots])
                    if lots
                    else 0.0,
                    "margin_rupees": margin,
                    "equity_before": before,
                    "equity_after": equity,
                    "margin_utilization": margin / before if before > 0 else 0.0,
                }
            )
    return pd.DataFrame(rows)


def render_report(result: dict[str, Any]) -> str:
    robustness = result["robustness"]
    lines = [
        "# Phase 10 — margin-efficient sizing exploration",
        "",
        "## Scope",
        "",
        "The Phase 9 composite score is frozen. This module changes only sizing and risk mechanics. ",
        "All profile selection uses 2021–2023; 2024–2026 is reported afterward without retuning.",
        "",
        f"- Grid size: {result['grid_size']:,} policies.",
        "- Initial margin/capital pool: ₹10,00,000.",
        "- Margin ceilings: 25%, 35%, 50%, 65%, 80%, and 100%.",
        "- Maximum structural-risk ceilings: 0.5% through 4.0%.",
        "- Score floors, nonlinear score powers, drawdown brakes, and losing-streak brakes.",
        "- Risk cap includes the discovery 95th-percentile round-trip cost reserve for each lot count.",
        "",
        "## Discovery-selected profiles",
        "",
        "| Profile | Margin cap | Max risk | Floor | Power | DD brake | Streak | Discovery net | Discovery DD | Holdout net | 2024 | 2025–26 | Full return | Full DD |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["profiles"]:
        threshold = row["drawdown_brake_threshold"]
        brake = "none" if threshold is None else f"{threshold:.1%}×{row['drawdown_brake_multiplier']:.2f}"
        streak = "none" if row["losing_streak_trigger"] is None else str(int(row["losing_streak_trigger"]))
        lines.append(
            f"| {row['profile']} | {row['margin_fraction']:.0%} | "
            f"{row['max_risk_fraction']:.1%} | {row['score_floor']:.0%} | {row['score_power']:.1f} | "
            f"{brake} | {streak} | ₹{row['discovery_net']:,.0f} | "
            f"{row['discovery_max_drawdown_pct']:.2%} | ₹{row['holdout_net']:,.0f} | "
            f"₹{row['validation_net']:,.0f} | ₹{row['confirmation_net']:,.0f} | "
            f"{row['full_return']:.2%} | {row['full_max_drawdown_pct']:.2%} |"
        )
    lines.extend(
        [
            "",
            "## Grid robustness",
            "",
            f"- Discovery-eligible policies: {robustness['eligible_policy_count']:,}.",
            f"- Positive combined holdout: {robustness['positive_holdout_rate']:.1%}.",
            f"- Positive in both 2024 and 2025–2026: {robustness['positive_both_later_splits_rate']:.1%}.",
            f"- Discovery-versus-holdout policy-net rank correlation: {robustness['discovery_holdout_net_spearman']:.3f}.",
            f"- Top discovery decile median holdout net: ₹{robustness['top_discovery_decile_median_holdout_net']:,.0f}.",
            "",
            "## Parameter-neighborhood diagnostics",
            "",
            "| Dimension | Value | Policies | Median holdout | Positive holdout | Positive both later | Median full DD |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in result["dimension_summary"]:
        if row["dimension"] not in {
            "score_floor",
            "margin_fraction",
            "drawdown_brake_threshold",
        }:
            continue
        lines.append(
            f"| {row['dimension']} | {row['value']} | {row['policies']} | "
            f"₹{row['median_holdout_net']:,.0f} | {row['positive_holdout_rate']:.1%} | "
            f"{row['positive_both_later_rate']:.1%} | {row['median_full_drawdown_pct']:.2%} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "These are exploratory policies over a score that itself did not pass the strict Phase 9 ",
            "bootstrap gate. A strong historical profile can be retained for forward shadow testing, ",
            "but must not be promoted by selecting whichever row looks best on 2024–2026.",
            "",
            "## Reproduce",
            "",
            "```powershell",
            "python -m research.phase10.run_sizing_exploration",
            "python -m pytest tests/test_phase10_sizing_exploration.py -q",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    scored_event_path: Path,
    observation_path: Path,
    phase4_tradebook_path: Path,
    surface_path: Path,
    reserve_path: Path,
    grid_path: Path,
    profile_path: Path,
    dimension_path: Path,
    selected_tradebook_path: Path,
    tearsheet_path: Path,
    report_path: Path,
    manifest_path: Path,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, Any]:
    scored = pd.read_csv(scored_event_path, parse_dates=["entry_ts"])
    observations = pd.read_parquet(observation_path)
    observations["entry_ts"] = pd.to_datetime(observations["entry_ts"], utc=True)
    phase4_tradebook = pd.read_csv(phase4_tradebook_path)
    surface_path.parent.mkdir(parents=True, exist_ok=True)
    surface = build_fly_cost_surface(observations, phase4_tradebook, scored)
    surface.to_parquet(surface_path, index=False)
    reserve = build_cost_reserve(surface)
    reserve.to_csv(reserve_path, index=False)
    reserve_array = np.zeros(101, dtype=float)
    reserve_array[reserve["lots"].to_numpy(dtype=int)] = reserve[
        "discovery_q95_total_cost_reserve"
    ].to_numpy(dtype=float)
    events, matrices = prepare_matrices(surface, scored)
    configs = generate_configs()
    grid = simulate_grid(events, matrices, configs, reserve_array, initial_capital=initial_capital)
    sample_years = max(
        (pd.to_datetime(events["trade_date"]).max() - pd.to_datetime(events["trade_date"]).min()).days
        / 365.25,
        1 / 365.25,
    )
    grid["full_return"] = grid["full_net"] / initial_capital
    grid["full_cagr"] = (1.0 + grid["full_return"]) ** (1.0 / sample_years) - 1.0
    profiles = select_profiles(grid)
    dimension_summary = build_dimension_summary(grid)
    robustness = robustness_summary(grid)
    selected_tradebook = detailed_selected_tradebook(
        profiles,
        configs,
        events,
        matrices,
        reserve_array,
        initial_capital=initial_capital,
    )
    result = {
        "schema_version": SCHEMA_VERSION,
        "initial_capital": initial_capital,
        "structure": STRUCTURE,
        "capacity_cap_lots": CAPACITY_CAP,
        "grid_size": len(configs),
        "minimum_discovery_trades": MIN_DISCOVERY_TRADES,
        "selection_rule": "select only on 2021-2023 under fixed drawdown/profile objectives",
        "cost_reserve": "2021-2023 q95 exact round-trip total cost by integer lot count",
        "profiles": _json_safe(profiles.to_dict(orient="records")),
        "dimension_summary": _json_safe(dimension_summary.to_dict(orient="records")),
        "robustness": robustness,
        "limitations": [
            "The frozen Phase 9 score failed its strict combined-holdout bootstrap criterion.",
            "Grid breadth creates multiple-testing risk; profile selection is discovery-only.",
            "Drawdown and streak brakes observe end-of-trade equity, not intratrade MTM paths.",
            "SPAN is entry-time only; intratrade expansion and forced liquidation are unavailable.",
        ],
    }
    outputs = (
        surface_path,
        reserve_path,
        grid_path,
        profile_path,
        dimension_path,
        selected_tradebook_path,
        tearsheet_path,
        report_path,
    )
    for path in (*outputs, manifest_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    grid.to_csv(grid_path, index=False)
    profiles.to_csv(profile_path, index=False)
    dimension_summary.to_csv(dimension_path, index=False)
    selected_tradebook.to_csv(selected_tradebook_path, index=False)
    tearsheet_path.write_text(
        json.dumps(_json_safe(result), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report = "\n".join(line.rstrip() for line in render_report(result).splitlines()) + "\n"
    report_path.write_text(report, encoding="utf-8", newline="\n")
    manifest = {
        "schema_version": "phase10-sizing-exploration-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)}
            for path in (scored_event_path, observation_path, phase4_tradebook_path)
        ],
        "outputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)} for path in outputs
        ],
        "impact_model": {
            "name": "additive_ladder_participation_v2",
            "parameters": asdict(CAPACITY_IMPACT_PARAMETERS),
        },
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scored-events", type=Path, default=Path("audit/phase9_scored_events.csv")
    )
    parser.add_argument(
        "--observations", type=Path, default=Path("audit/phase4_cost_aware_observations.parquet")
    )
    parser.add_argument(
        "--phase4-tradebook", type=Path, default=Path("audit/phase4_cost_aware_tradebook.csv")
    )
    parser.add_argument("--surface", type=Path, default=Path("audit/phase10_fly_cost_surface.parquet"))
    parser.add_argument("--reserve", type=Path, default=Path("audit/phase10_cost_reserve.csv"))
    parser.add_argument("--grid", type=Path, default=Path("audit/phase10_sizing_grid.csv"))
    parser.add_argument("--profiles", type=Path, default=Path("audit/phase10_selected_profiles.csv"))
    parser.add_argument(
        "--dimension-summary",
        type=Path,
        default=Path("audit/phase10_dimension_summary.csv"),
    )
    parser.add_argument(
        "--selected-tradebook",
        type=Path,
        default=Path("audit/phase10_selected_profile_tradebook.csv"),
    )
    parser.add_argument("--tearsheet", type=Path, default=Path("audit/phase10_tearsheet.json"))
    parser.add_argument(
        "--report", type=Path, default=Path("docs/research/PHASE10_SIZING_EXPLORATION.md")
    )
    parser.add_argument("--manifest", type=Path, default=Path("audit/phase10_manifest.json"))
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        scored_event_path=args.scored_events,
        observation_path=args.observations,
        phase4_tradebook_path=args.phase4_tradebook,
        surface_path=args.surface,
        reserve_path=args.reserve,
        grid_path=args.grid,
        profile_path=args.profiles,
        dimension_path=args.dimension_summary,
        selected_tradebook_path=args.selected_tradebook,
        tearsheet_path=args.tearsheet,
        report_path=args.report,
        manifest_path=args.manifest,
        initial_capital=args.initial_capital,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
