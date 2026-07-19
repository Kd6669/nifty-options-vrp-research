"""Test causal intraday VRP-percentile curve crossings against 60-minute condor P&L.

This is an exploratory extension to the preregistered zero-crossing hypothesis.
It builds one causal VRP-percentile curve per session, derives exact-lag
velocity and acceleration, detects first daily percentile-level crossings, and
then attaches the structure path beginning at the next executable minute.

The output remains frictionless.  It may support a future confidence or sizing
rule, but it is not a leverage instruction until charges, slippage, margin,
tail-risk, and genuinely out-of-sample stability are applied.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from .analyze_intraday_volatility import _causal_percentile
except ImportError:  # Support direct execution by file path.
    from analyze_intraday_volatility import _causal_percentile


THRESHOLDS = (0.10, 0.25, 0.50, 0.75, 0.90)
ACCELERATION_BINS = (-np.inf, 0.25, 0.50, 0.75, np.inf)
ACCELERATION_LABELS = ("q00_25", "q25_50", "q50_75", "q75_100")
ENTRY_CUTOFF = "14:15"

SOURCE_COLUMNS = [
    "entry_id",
    "entry_ts",
    "trade_date",
    "entry_time",
    "research_dte",
    "spot",
    "atm_iv",
    "trailing_rv_act365",
    "signal_vrp_var_act365",
    "vrp_tod_percentile",
    "short_iron_condor__pnl_points",
    "short_iron_condor__return_on_max_loss",
    "long_iron_condor__pnl_points",
    "long_iron_condor__return_on_max_loss",
]


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _exact_lag(frame: pd.DataFrame, column: str, minutes: int) -> pd.Series:
    grouped = frame.groupby("trade_date", sort=False)
    lagged = grouped[column].shift(minutes)
    lagged_ts = grouped["entry_ts"].shift(minutes)
    elapsed = (frame["entry_ts"] - lagged_ts).dt.total_seconds() / 60
    return lagged.where(elapsed == minutes)


def _exact_lead(frame: pd.DataFrame, column: str, minutes: int = 1) -> pd.Series:
    grouped = frame.groupby("trade_date", sort=False)
    leading = grouped[column].shift(-minutes)
    leading_ts = grouped["entry_ts"].shift(-minutes)
    elapsed = (leading_ts - frame["entry_ts"]).dt.total_seconds() / 60
    return leading.where(elapsed == minutes)


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return {"count": 0}
    tail_count = max(1, int(np.ceil(len(clean) * 0.05)))
    ordered = clean.sort_values()
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "std": float(clean.std()),
        "win_rate": float((clean > 0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "p95": float(clean.quantile(0.95)),
        "cvar05": float(ordered.iloc[:tail_count].mean()),
    }


def _bootstrap_mean_ci(
    values: pd.Series,
    *,
    seed: int = 20260718,
    samples: int = 2000,
) -> tuple[float, float]:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
    if len(clean) < 10:
        return float("nan"), float("nan")
    rng = np.random.default_rng(seed)
    means = np.empty(samples, dtype=float)
    for index in range(samples):
        means[index] = rng.choice(clean, size=len(clean), replace=True).mean()
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def _summarize_events(events: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for keys, group in events.groupby(group_columns, observed=True, sort=True):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=True))
        for structure in ("short", "long"):
            pnl = group[f"{structure}_pnl_points"]
            roml = group[f"{structure}_return_on_max_loss"]
            pnl_ci = _bootstrap_mean_ci(pnl)
            roml_ci = _bootstrap_mean_ci(roml)
            row = {
                **base,
                "structure": f"{structure}_iron_condor",
                "dates": int(group.loc[pnl.notna(), "trade_date"].nunique()),
                "signal_q5_median": float(group["signal_q5"].median()),
                "directional_velocity_median": float(
                    group["directional_q_velocity_5m"].median()
                ),
                "directional_acceleration_median": float(
                    group["directional_q_acceleration_5m"].median()
                ),
                "pnl_points": _distribution(pnl),
                "return_on_max_loss": _distribution(roml),
                "pnl_mean_bootstrap_95": [pnl_ci[0], pnl_ci[1]],
                "return_mean_bootstrap_95": [roml_ci[0], roml_ci[1]],
            }
            rows.append(row)
    return pd.DataFrame(rows)


def _correlations(curves: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    features = [
        "vrp_tod_percentile",
        "vrp_q5",
        "q_velocity_5m",
        "q_acceleration_5m",
        "vrp_velocity_5m",
        "vrp_acceleration_5m",
    ]
    states = {
        "all": pd.Series(True, index=curves.index),
        "lower_tail_q10": curves["vrp_q5"] <= 0.10,
        "upper_tail_q90": curves["vrp_q5"] >= 0.90,
    }
    for state, mask in states.items():
        subset = curves.loc[mask]
        for feature in features:
            for structure in ("short", "long"):
                outcome = f"next_{structure}_pnl_points"
                pair = subset[[feature, outcome]].dropna()
                if len(pair) < 3:
                    continue
                rows.append(
                    {
                        "state": state,
                        "feature": feature,
                        "structure": f"{structure}_iron_condor",
                        "observations": int(len(pair)),
                        "pearson": float(pair[feature].corr(pair[outcome], method="pearson")),
                        "spearman": float(pair[feature].corr(pair[outcome], method="spearman")),
                    }
                )
    return rows


def _event_correlations(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    features = [
        "signal_q5",
        "directional_q_velocity_5m",
        "directional_q_acceleration_5m",
        "directional_acceleration_percentile",
    ]
    for (threshold, direction), group in events.groupby(
        ["threshold", "direction"], sort=True
    ):
        for feature in features:
            for structure in ("short", "long"):
                outcome = f"{structure}_pnl_points"
                pair = group[[feature, outcome]].dropna()
                if len(pair) < 3:
                    continue
                rows.append(
                    {
                        "threshold": float(threshold),
                        "direction": str(direction),
                        "feature": feature,
                        "structure": f"{structure}_iron_condor",
                        "observations": int(len(pair)),
                        "pearson": float(pair[feature].corr(pair[outcome], method="pearson")),
                        "spearman": float(pair[feature].corr(pair[outcome], method="spearman")),
                    }
                )
    return rows


def _candidate_ladder(summary: pd.DataFrame) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    definitions = (
        ("upper_up", "up", "short_iron_condor", (0.50, 0.75, 0.90)),
        ("lower_down", "down", "long_iron_condor", (0.50, 0.25, 0.10)),
    )
    for ladder, direction, structure, ordered_thresholds in definitions:
        means = []
        for confidence_step, threshold in enumerate(ordered_thresholds, start=1):
            selected = summary[
                summary["direction"].eq(direction)
                & np.isclose(summary["threshold"].astype(float), threshold)
                & summary["structure"].eq(structure)
            ]
            if selected.empty:
                continue
            row = selected.iloc[0]
            distribution = row["return_on_max_loss"]
            mean_value = float(distribution["mean"])
            means.append(mean_value)
            candidates.append(
                {
                    "ladder": ladder,
                    "confidence_step": confidence_step,
                    "threshold": threshold,
                    "direction": direction,
                    "candidate_structure": structure,
                    "dates": int(row["dates"]),
                    "gross_mean_return_on_max_loss": mean_value,
                    "gross_median_return_on_max_loss": float(distribution["median"]),
                    "gross_cvar05_return_on_max_loss": float(distribution["cvar05"]),
                    "mean_bootstrap_95": row["return_mean_bootstrap_95"],
                }
            )
        monotonic = bool(len(means) == len(ordered_thresholds) and np.all(np.diff(means) > 0))
        for candidate in candidates:
            if candidate["ladder"] == ladder:
                candidate["strictly_monotonic_gross_mean"] = monotonic
    return candidates


def _paired_ladder_comparisons(events: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    definitions = (
        ("upper_up", "up", "short", ((0.50, 0.75), (0.75, 0.90))),
        ("lower_down", "down", "long", ((0.50, 0.25), (0.25, 0.10))),
    )
    for ladder, direction, structure, steps in definitions:
        subset = events[events["direction"].eq(direction)]
        for outcome in ("pnl_points", "return_on_max_loss"):
            pivot = subset.pivot(
                index="trade_date",
                columns="threshold",
                values=f"{structure}_{outcome}",
            )
            for from_threshold, to_threshold in steps:
                difference = (pivot[to_threshold] - pivot[from_threshold]).dropna()
                ci = _bootstrap_mean_ci(difference, samples=5000)
                rows.append(
                    {
                        "ladder": ladder,
                        "structure": f"{structure}_iron_condor",
                        "outcome": outcome,
                        "from_threshold": from_threshold,
                        "to_threshold": to_threshold,
                        "paired_dates": int(len(difference)),
                        "mean_deeper_minus_shallower": float(difference.mean()),
                        "median_deeper_minus_shallower": float(difference.median()),
                        "deeper_better_rate": float((difference > 0).mean()),
                        "mean_difference_bootstrap_95": [ci[0], ci[1]],
                    }
                )
    return rows


def analyze(source_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    frame = pd.read_parquet(
        source_path,
        columns=SOURCE_COLUMNS,
        filters=[("horizon_minutes", "=", 60)],
    )
    frame["entry_ts"] = pd.to_datetime(frame["entry_ts"], utc=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    frame = frame.sort_values(["trade_date", "entry_ts"]).reset_index(drop=True)

    grouped = frame.groupby("trade_date", sort=False)
    q5 = grouped["vrp_tod_percentile"].rolling(5, min_periods=5).median().reset_index(
        level=0, drop=True
    )
    q5_start_ts = grouped["entry_ts"].shift(4)
    q5_elapsed = (frame["entry_ts"] - q5_start_ts).dt.total_seconds() / 60
    frame["vrp_q5"] = q5.where(q5_elapsed == 4)

    frame["q_velocity_5m"] = frame["vrp_q5"] - _exact_lag(frame, "vrp_q5", 5)
    frame["q_acceleration_5m"] = (
        frame["vrp_q5"]
        - 2 * _exact_lag(frame, "vrp_q5", 5)
        + _exact_lag(frame, "vrp_q5", 10)
    )
    frame["vrp_velocity_5m"] = frame["signal_vrp_var_act365"] - _exact_lag(
        frame, "signal_vrp_var_act365", 5
    )
    frame["vrp_acceleration_5m"] = (
        frame["signal_vrp_var_act365"]
        - 2 * _exact_lag(frame, "signal_vrp_var_act365", 5)
        + _exact_lag(frame, "signal_vrp_var_act365", 10)
    )
    frame["q_acceleration_tod_percentile"] = _causal_percentile(
        frame["q_acceleration_5m"].to_numpy(dtype=float),
        frame["entry_time"].to_numpy(),
        frame["trade_date"].to_numpy(),
    )

    for structure in ("short", "long"):
        for suffix in ("pnl_points", "return_on_max_loss"):
            source = f"{structure}_iron_condor__{suffix}"
            frame[f"next_{structure}_{suffix}"] = _exact_lead(frame, source)
    frame["next_entry_ts"] = _exact_lead(frame, "entry_ts")
    frame["next_entry_time"] = _exact_lead(frame, "entry_time")

    curve_columns = [
        "entry_id",
        "entry_ts",
        "trade_date",
        "entry_time",
        "research_dte",
        "spot",
        "atm_iv",
        "trailing_rv_act365",
        "signal_vrp_var_act365",
        "vrp_tod_percentile",
        "vrp_q5",
        "q_velocity_5m",
        "q_acceleration_5m",
        "q_acceleration_tod_percentile",
        "vrp_velocity_5m",
        "vrp_acceleration_5m",
        "next_entry_ts",
        "next_entry_time",
        "next_short_pnl_points",
        "next_short_return_on_max_loss",
        "next_long_pnl_points",
        "next_long_return_on_max_loss",
    ]
    curve_path = output_dir / "phase2_vrp_session_curve_features.parquet"
    frame[curve_columns].to_parquet(curve_path, index=False, compression="zstd")

    previous_q5 = _exact_lag(frame, "vrp_q5", 1)
    event_frames: list[pd.DataFrame] = []
    base_columns = [
        "trade_date",
        "entry_ts",
        "entry_time",
        "next_entry_ts",
        "next_entry_time",
        "research_dte",
        "signal_vrp_var_act365",
        "vrp_q5",
        "q_velocity_5m",
        "q_acceleration_5m",
        "q_acceleration_tod_percentile",
        "vrp_velocity_5m",
        "vrp_acceleration_5m",
        "next_short_pnl_points",
        "next_short_return_on_max_loss",
        "next_long_pnl_points",
        "next_long_return_on_max_loss",
    ]
    for threshold in THRESHOLDS:
        directions = {
            "up": (previous_q5 < threshold) & (frame["vrp_q5"] >= threshold),
            "down": (previous_q5 > threshold) & (frame["vrp_q5"] <= threshold),
        }
        for direction, mask in directions.items():
            selected = frame.loc[mask, base_columns].copy()
            selected["threshold"] = threshold
            selected["direction"] = direction
            sign = 1.0 if direction == "up" else -1.0
            selected["directional_q_velocity_5m"] = sign * selected["q_velocity_5m"]
            selected["directional_q_acceleration_5m"] = sign * selected[
                "q_acceleration_5m"
            ]
            selected["directional_acceleration_percentile"] = (
                selected["q_acceleration_tod_percentile"]
                if direction == "up"
                else 1.0 - selected["q_acceleration_tod_percentile"]
            )
            event_frames.append(selected)

    events = pd.concat(event_frames, ignore_index=True)
    events = events[
        events["next_entry_time"].le(ENTRY_CUTOFF)
        & events["next_short_pnl_points"].notna()
        & events["next_long_pnl_points"].notna()
    ].copy()
    events = events.sort_values(
        ["trade_date", "threshold", "direction", "entry_ts"]
    )
    events = events.drop_duplicates(["trade_date", "threshold", "direction"], keep="first")
    events = events.rename(
        columns={
            "entry_ts": "signal_ts",
            "entry_time": "signal_time",
            "vrp_q5": "signal_q5",
            "next_short_pnl_points": "short_pnl_points",
            "next_short_return_on_max_loss": "short_return_on_max_loss",
            "next_long_pnl_points": "long_pnl_points",
            "next_long_return_on_max_loss": "long_return_on_max_loss",
        }
    )
    events["year"] = events["trade_date"].str[:4].astype(int)
    events["acceleration_bin"] = pd.cut(
        events["directional_acceleration_percentile"],
        bins=ACCELERATION_BINS,
        labels=ACCELERATION_LABELS,
        include_lowest=True,
    )
    event_path = output_dir / "phase2_vrp_percentile_crossing_events.parquet"
    events.to_parquet(event_path, index=False, compression="zstd")

    crossing_summary = _summarize_events(events, ["threshold", "direction"])
    acceleration_summary = _summarize_events(
        events.dropna(subset=["acceleration_bin"]),
        ["threshold", "direction", "acceleration_bin"],
    )
    yearly_summary = _summarize_events(events, ["year", "threshold", "direction"])
    correlations = _correlations(frame)
    event_correlations = _event_correlations(events)
    ladders = _candidate_ladder(crossing_summary)
    paired_ladders = _paired_ladder_comparisons(events)
    inverse_error = float(
        (events["short_pnl_points"] + events["long_pnl_points"]).abs().max()
    )
    entry_delays = (
        pd.to_datetime(events["next_entry_ts"], utc=True)
        - pd.to_datetime(events["signal_ts"], utc=True)
    ).dt.total_seconds() / 60
    report = {
        "contract": {
            "source": str(source_path.resolve()),
            "horizon_minutes": 60,
            "vrp_curve": (
                "causal same-minute-of-day percentile of ATM_IV^2 minus trailing "
                "60-minute ACT/365 RV^2"
            ),
            "curve_smoother": "trailing 5-minute median; exact contiguous observations",
            "velocity": "q5(t) - q5(t-5 minutes)",
            "acceleration": "q5(t) - 2*q5(t-5) + q5(t-10 minutes)",
            "crossings": "first crossing per trade_date, threshold, and direction",
            "execution": "signal at t; frictionless structure path entered at exact t+1",
            "entry_cutoff": ENTRY_CUTOFF,
            "cost_status": "no charges, slippage, or SPAN margin applied",
            "expiry_status": "nearest-listed-expiry research proxy, not proven contract identity",
            "independence_warning": (
                "threshold cells overlap across dates and thresholds; do not sum them as "
                "independent trades"
            ),
        },
        "coverage": {
            "curve_rows": int(len(frame)),
            "curve_dates": int(frame["trade_date"].nunique()),
            "ranked_curve_rows": int(frame["vrp_q5"].notna().sum()),
            "first_crossing_events": int(len(events)),
            "event_dates": int(events["trade_date"].nunique()),
            "first_date": str(frame["trade_date"].min()),
            "last_date": str(frame["trade_date"].max()),
        },
        "invariants": {
            "maximum_long_short_pnl_inverse_error": inverse_error,
            "minimum_signal_to_entry_minutes": float(entry_delays.min()),
            "maximum_signal_to_entry_minutes": float(entry_delays.max()),
            "duplicate_date_threshold_direction_rows": int(
                events.duplicated(["trade_date", "threshold", "direction"]).sum()
            ),
        },
        "artifacts": {
            "session_curve_features": str(curve_path.resolve()),
            "crossing_events": str(event_path.resolve()),
        },
        "crossing_summary": _records(crossing_summary),
        "acceleration_summary": _records(acceleration_summary),
        "yearly_crossing_summary": _records(yearly_summary),
        "minute_grid_correlations": correlations,
        "first_crossing_correlations": event_correlations,
        "candidate_confidence_ladders": ladders,
        "paired_session_ladder_comparisons": paired_ladders,
    }
    report_path = output_dir / "phase2_vrp_curve_crossings.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--structures",
        type=Path,
        default=Path("audit/phase2_defined_risk_structure_paths.parquet"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("audit"))
    return parser


def main() -> None:
    args = _parser().parse_args()
    report = analyze(args.structures.resolve(), args.output_dir.resolve())
    print(json.dumps(report["coverage"], indent=2))
    print(json.dumps(report["candidate_confidence_ladders"], indent=2))


if __name__ == "__main__":
    main()
