"""Build causal features and cost-aware vertical-spread labels for Phase 5."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nifty_execution import (
    ExecutedLeg,
    estimate_round_trip_execution_cost,
    groww_fno_rates_for_date,
)
from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase3.run_full_strategy_backtest import _fill_price, _theoretical_risk
from research.phase4.run_cost_aware_discovery import _load_observations


SCHEMA_VERSION = "phase5-final-attempt-dataset/v1"
HORIZONS = (60, 120, 180)
STRUCTURE_NAMES = ("bull_call_spread", "bear_put_spread")
MINIMUM_SELL_FILL = 0.05
SURFACE_FEATURES = (
    "put_skew",
    "call_skew",
    "risk_reversal",
    "smile_curvature",
    "atm_ce_pe_gap",
    "atm_iv_tod_percentile",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def build_causal_features(curve_path: Path, surface_path: Path) -> pd.DataFrame:
    curve = pd.read_parquet(curve_path).sort_values(["trade_date", "entry_ts"]).copy()
    curve["entry_ts"] = pd.to_datetime(curve["entry_ts"], utc=True)
    curve["trade_date"] = pd.to_datetime(curve["trade_date"]).dt.date
    curve["iv_minus_rv"] = curve["atm_iv"] - curve["trailing_rv_act365"]
    valid_ratio = (curve["atm_iv"] > 0) & (curve["trailing_rv_act365"] > 0)
    curve["log_iv_rv"] = np.where(
        valid_ratio,
        np.log(curve["atm_iv"] / curve["trailing_rv_act365"]),
        np.nan,
    )
    lag_source = curve[
        ["trade_date", "entry_ts", "spot", "atm_iv", "trailing_rv_act365", "log_iv_rv"]
    ].copy()
    for minutes in (5, 15, 30):
        lag = lag_source.copy()
        lag["entry_ts"] = lag["entry_ts"] + pd.Timedelta(minutes=minutes)
        lag = lag.rename(
            columns={
                "spot": f"spot_lag_{minutes}",
                "atm_iv": f"atm_iv_lag_{minutes}",
                "trailing_rv_act365": f"rv_lag_{minutes}",
                "log_iv_rv": f"log_iv_rv_lag_{minutes}",
            }
        )
        curve = curve.merge(lag, on=["trade_date", "entry_ts"], how="left")
        curve[f"spot_return_{minutes}m"] = (
            curve["spot"] / curve[f"spot_lag_{minutes}"] - 1.0
        )
        curve[f"iv_change_{minutes}m"] = (
            curve["atm_iv"] - curve[f"atm_iv_lag_{minutes}"]
        )
        curve[f"rv_change_{minutes}m"] = (
            curve["trailing_rv_act365"] - curve[f"rv_lag_{minutes}"]
        )
        curve[f"log_iv_rv_change_{minutes}m"] = (
            curve["log_iv_rv"] - curve[f"log_iv_rv_lag_{minutes}"]
        )
        curve = curve.drop(
            columns=[
                f"spot_lag_{minutes}",
                f"atm_iv_lag_{minutes}",
                f"rv_lag_{minutes}",
                f"log_iv_rv_lag_{minutes}",
            ]
        )
    surface = pd.read_parquet(
        surface_path,
        columns=["timestamp_ist", *SURFACE_FEATURES],
    ).rename(columns={"timestamp_ist": "entry_ts"})
    surface["entry_ts"] = pd.to_datetime(surface["entry_ts"], utc=True)
    curve = curve.merge(surface, on="entry_ts", how="left", validate="many_to_one")
    local_ts = curve["entry_ts"].dt.tz_convert("Asia/Kolkata")
    minute_of_day = local_ts.dt.hour * 60 + local_ts.dt.minute
    curve["minute_of_day"] = minute_of_day
    phase = 2.0 * np.pi * minute_of_day / (24.0 * 60.0)
    curve["tod_sin"] = np.sin(phase)
    curve["tod_cos"] = np.cos(phase)
    curve = curve.loc[local_ts.dt.minute.mod(15).eq(0)].copy()
    curve = curve.loc[
        curve["atm_iv"].gt(0)
        & curve["trailing_rv_act365"].gt(0)
        & curve["signal_vrp_var_act365"].notna()
    ].copy()
    curve["trade_id"] = np.arange(1, len(curve) + 1, dtype=np.int64)
    return curve


def _actual_and_causal_cost(
    part: pd.DataFrame,
    weights: dict[str, int],
) -> tuple[float, float]:
    first = part.iloc[0]
    lot_size = int(first.lot_size)
    actual_entry: list[ExecutedLeg] = []
    actual_exit: list[ExecutedLeg] = []
    causal_exit: list[ExecutedLeg] = []
    for row in part.itertuples(index=False):
        weight = int(weights[str(row.leg)])
        quantity = abs(weight) * lot_size
        entry_side = "BUY" if weight > 0 else "SELL"
        exit_side = "SELL" if weight > 0 else "BUY"
        entry_fill = _fill_price(
            float(row.entry_close),
            float(row.entry_slippage_per_unit),
            entry_side,
            minimum_sell_fill=MINIMUM_SELL_FILL,
        )
        exit_fill = _fill_price(
            float(row.exit_close),
            float(row.exit_slippage_per_unit),
            exit_side,
            minimum_sell_fill=MINIMUM_SELL_FILL,
        )
        causal_exit_fill = _fill_price(
            float(row.entry_close),
            float(row.entry_slippage_per_unit),
            exit_side,
            minimum_sell_fill=MINIMUM_SELL_FILL,
        )
        actual_entry.append(
            ExecutedLeg(
                entry_side,
                "OPT",
                entry_fill,
                quantity,
                slippage_per_unit=float(row.entry_slippage_per_unit),
            )
        )
        actual_exit.append(
            ExecutedLeg(
                exit_side,
                "OPT",
                exit_fill,
                quantity,
                slippage_per_unit=float(row.exit_slippage_per_unit),
            )
        )
        causal_exit.append(
            ExecutedLeg(
                exit_side,
                "OPT",
                causal_exit_fill,
                quantity,
                slippage_per_unit=float(row.entry_slippage_per_unit),
            )
        )
    entry_rates = groww_fno_rates_for_date(pd.Timestamp(first.entry_ts).date())
    actual = estimate_round_trip_execution_cost(
        entry_legs=actual_entry,
        exit_legs=actual_exit,
        entry_rates=entry_rates,
        exit_rates=groww_fno_rates_for_date(pd.Timestamp(first.horizon_exit_ts).date()),
    )
    causal = estimate_round_trip_execution_cost(
        entry_legs=actual_entry,
        exit_legs=causal_exit,
        entry_rates=entry_rates,
        exit_rates=entry_rates,
    )
    return float(actual.total), float(causal.total)


def build_labels(observations: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (trade_id, horizon), full_part in observations.groupby(
        ["trade_id", "horizon_minutes"], sort=True
    ):
        for structure in STRUCTURE_NAMES:
            weights = {leg: int(value) for leg, value in STRUCTURES[structure].items()}
            part = full_part.loc[full_part["leg"].isin(weights)].copy()
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            first = part.iloc[0]
            lot_size = int(first.lot_size)
            gross_points = 0.0
            entry_value = 0.0
            greek_totals = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
            for row in part.itertuples(index=False):
                weight = weights[str(row.leg)]
                gross_points += weight * (float(row.exit_close) - float(row.entry_close))
                entry_value += weight * float(row.entry_close)
                greek_totals["delta"] += weight * float(row.entry_delta)
                greek_totals["gamma"] += weight * float(row.entry_gamma)
                greek_totals["theta"] += weight * float(row.entry_theta_day)
                greek_totals["vega"] += weight * float(row.entry_vega_vol_point)
            actual_cost, causal_cost = _actual_and_causal_cost(part, weights)
            max_loss, max_profit = _theoretical_risk(part, weights)
            gross_rupees = gross_points * lot_size
            rows.append(
                {
                    "trade_id": int(trade_id),
                    "trade_date": str(first.trade_date),
                    "entry_ts": pd.Timestamp(first.entry_ts),
                    "exit_ts": pd.Timestamp(first.horizon_exit_ts),
                    "structure": structure,
                    "horizon_minutes": int(horizon),
                    "lot_size": lot_size,
                    "entry_credit_points": max(-entry_value, 0.0),
                    "entry_debit_points": max(entry_value, 0.0),
                    "max_loss_points": max_loss,
                    "max_profit_points": max_profit,
                    "net_delta_per_unit": greek_totals["delta"],
                    "net_gamma_per_unit": greek_totals["gamma"],
                    "net_theta_points_per_day": greek_totals["theta"],
                    "net_vega_points_per_vol_point": greek_totals["vega"],
                    "causal_cost_hurdle_points": causal_cost / lot_size,
                    "actual_cost_hurdle_points": actual_cost / lot_size,
                    "gross_pnl_points": gross_points,
                    "gross_pnl_rupees": gross_rupees,
                    "total_cost_rupees": actual_cost,
                    "net_pnl_rupees": gross_rupees - actual_cost,
                }
            )
    return pd.DataFrame(rows)


def run(
    *,
    gold_root: Path,
    curve_path: Path,
    surface_path: Path,
    dataset_path: Path,
    observations_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    features = build_causal_features(curve_path, surface_path)
    events = features[["trade_id", "entry_ts", "trade_date"]].copy()
    events["signal_family"] = "phase5_grid"
    observations = _load_observations(
        events,
        gold_glob=str(gold_root / "year=*" / "month=*" / "part-*.parquet"),
        surface_path=surface_path,
    )
    labels = build_labels(observations)
    drop_columns = {
        "next_entry_ts",
        "next_entry_time",
        "next_short_pnl_points",
        "next_short_return_on_max_loss",
        "next_long_pnl_points",
        "next_long_return_on_max_loss",
    }
    feature_columns = [
        column
        for column in features.columns
        if column not in drop_columns and column not in labels.columns
    ]
    dataset = labels.merge(
        features[["trade_id", *feature_columns]],
        on="trade_id",
        how="left",
        validate="many_to_one",
    )
    coverage = []
    expected = int(features["trade_id"].nunique())
    for (structure, horizon), part in dataset.groupby(
        ["structure", "horizon_minutes"], sort=True
    ):
        coverage.append(
            {
                "structure": structure,
                "horizon_minutes": int(horizon),
                "complete_labels": int(len(part)),
                "expected_candidates": expected,
                "coverage": float(len(part) / expected),
            }
        )
    summary = {
        "schema_version": SCHEMA_VERSION,
        "candidate_timestamps": expected,
        "dataset_rows": int(len(dataset)),
        "observation_rows": int(len(observations)),
        "date_min": str(features["trade_date"].min()),
        "date_max": str(features["trade_date"].max()),
        "coverage": coverage,
        "limitations": [
            "Candidate labels may overlap; the strategy evaluator enforces one live trade at a time.",
            "The causal hurdle proxies exit liquidity with entry liquidity; actual labels use observed exit liquidity.",
            "The rolling surface creates horizon-dependent exact-contract attrition.",
        ],
    }
    for path in (dataset_path, observations_path, summary_path, manifest_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(dataset_path, index=False, compression="zstd")
    observations.to_parquet(observations_path, index=False, compression="zstd")
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = {
        "schema_version": "phase5-final-attempt-dataset-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(dataset_path.resolve()), "sha256": _sha256(dataset_path)},
            {"path": str(observations_path.resolve()), "sha256": _sha256(observations_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
        ],
        "gold_root": str(gold_root.resolve()),
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", required=True, type=Path)
    parser.add_argument(
        "--curve-path",
        type=Path,
        default=Path("audit/phase2_vrp_session_curve_features.parquet"),
    )
    parser.add_argument(
        "--surface-path",
        type=Path,
        default=Path("audit/phase2_intraday_iv_surface.parquet"),
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("audit/phase5_final_attempt_dataset.parquet"),
    )
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase5_final_attempt_observations.parquet"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase5_final_attempt_dataset_summary.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase5_final_attempt_dataset_manifest.json"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        gold_root=args.gold_root,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        dataset_path=args.dataset,
        observations_path=args.observations,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
