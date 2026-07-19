"""Run the frozen Phase 5 ridge/cost-gate selector and locked confirmation test."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase4.run_cost_aware_discovery import _margin_for_structure


SCHEMA_VERSION = "phase5-final-attempt-strategy/v1"
RIDGE_ALPHAS = (0.0, 0.1, 1.0, 10.0, 100.0)
GATE_MULTIPLIERS = (1.0, 1.25, 1.5, 2.0)
MIN_VALIDATION_TRADES = 50
MIN_CONFIRMATION_TRADES = 100
BOOTSTRAP_REPLICATIONS = 5000
BOOTSTRAP_SEED = 20260718
PROTOCOL_PATH = Path("docs/research/PHASE5_FINAL_ATTEMPT_PROTOCOL.md")
FEATURES = (
    "atm_iv",
    "trailing_rv_act365",
    "iv_minus_rv",
    "signal_vrp_var_act365",
    "log_iv_rv",
    "vrp_tod_percentile",
    "vrp_q5",
    "q_velocity_5m",
    "q_acceleration_5m",
    "q_acceleration_tod_percentile",
    "vrp_velocity_5m",
    "vrp_acceleration_5m",
    "spot_return_5m",
    "spot_return_15m",
    "spot_return_30m",
    "iv_change_5m",
    "iv_change_15m",
    "iv_change_30m",
    "rv_change_5m",
    "rv_change_15m",
    "rv_change_30m",
    "log_iv_rv_change_5m",
    "log_iv_rv_change_15m",
    "log_iv_rv_change_30m",
    "put_skew",
    "call_skew",
    "risk_reversal",
    "smile_curvature",
    "atm_ce_pe_gap",
    "atm_iv_tod_percentile",
    "research_dte",
    "tod_sin",
    "tod_cos",
    "entry_credit_points",
    "entry_debit_points",
    "max_loss_points",
    "max_profit_points",
    "net_delta_per_unit",
    "net_gamma_per_unit",
    "net_theta_points_per_day",
    "net_vega_points_per_vol_point",
    "causal_cost_hurdle_points",
)


@dataclass(frozen=True)
class RidgeModel:
    medians: np.ndarray
    means: np.ndarray
    scales: np.ndarray
    coefficients: np.ndarray

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        values = frame.loc[:, FEATURES].apply(pd.to_numeric, errors="coerce").to_numpy(float)
        values = np.where(np.isfinite(values), values, self.medians)
        standardized = (values - self.means) / self.scales
        design = np.column_stack([np.ones(len(standardized)), standardized])
        return design @ self.coefficients

    def to_dict(self) -> dict[str, Any]:
        return {
            "features": list(FEATURES),
            "medians": self.medians.tolist(),
            "means": self.means.tolist(),
            "scales": self.scales.tolist(),
            "intercept": float(self.coefficients[0]),
            "standardized_coefficients": {
                feature: float(value)
                for feature, value in zip(FEATURES, self.coefficients[1:], strict=True)
            },
        }


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


def fit_ridge(frame: pd.DataFrame, alpha: float) -> RidgeModel:
    values = frame.loc[:, FEATURES].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    medians = np.nanmedian(values, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    values = np.where(np.isfinite(values), values, medians)
    means = values.mean(axis=0)
    scales = values.std(axis=0)
    scales = np.where(scales > 1e-12, scales, 1.0)
    standardized = (values - means) / scales
    design = np.column_stack([np.ones(len(standardized)), standardized])
    target = pd.to_numeric(frame["gross_pnl_points"], errors="raise").to_numpy(float)
    penalty = np.eye(design.shape[1]) * float(alpha)
    penalty[0, 0] = 0.0
    coefficients = np.linalg.pinv(design.T @ design + penalty) @ design.T @ target
    return RidgeModel(medians, means, scales, coefficients)


def fit_cell_models(frame: pd.DataFrame, alpha: float) -> dict[tuple[str, int], RidgeModel]:
    models = {}
    for (structure, horizon), part in frame.groupby(
        ["structure", "horizon_minutes"], sort=True
    ):
        models[(str(structure), int(horizon))] = fit_ridge(part, alpha)
    return models


def add_predictions(
    frame: pd.DataFrame,
    models: dict[tuple[str, int], RidgeModel],
) -> pd.DataFrame:
    parts = []
    for key, part in frame.groupby(["structure", "horizon_minutes"], sort=True):
        normalized = (str(key[0]), int(key[1]))
        if normalized not in models:
            continue
        predicted = part.copy()
        predicted["predicted_gross_points"] = models[normalized].predict(part)
        parts.append(predicted)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def select_non_overlapping(frame: pd.DataFrame, gate_multiplier: float) -> pd.DataFrame:
    candidates = frame.copy()
    candidates["predicted_excess_points"] = candidates["predicted_gross_points"] - (
        float(gate_multiplier) * candidates["causal_cost_hurdle_points"]
    )
    candidates["entry_ts"] = pd.to_datetime(candidates["entry_ts"], utc=True)
    candidates["exit_ts"] = pd.to_datetime(candidates["exit_ts"], utc=True)
    candidates = candidates.loc[candidates["predicted_excess_points"].gt(0)].copy()
    candidates = candidates.sort_values(
        ["entry_ts", "predicted_excess_points", "structure", "horizon_minutes"],
        ascending=[True, False, True, True],
    )
    selected_indices = []
    active_until: pd.Timestamp | None = None
    for _entry_ts, part in candidates.groupby("entry_ts", sort=True):
        best = part.iloc[0]
        entry_ts = pd.Timestamp(best["entry_ts"])
        if active_until is not None and entry_ts < active_until:
            continue
        selected_indices.append(best.name)
        active_until = pd.Timestamp(best["exit_ts"])
    return candidates.loc[selected_indices].sort_values("entry_ts").reset_index(drop=True)


def _basic_metrics(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"trades": 0}
    return {
        "trades": int(len(frame)),
        "gross_pnl_rupees": float(frame["gross_pnl_rupees"].sum()),
        "total_cost_rupees": float(frame["total_cost_rupees"].sum()),
        "net_pnl_rupees": float(frame["net_pnl_rupees"].sum()),
        "mean_net_pnl_rupees": float(frame["net_pnl_rupees"].mean()),
        "median_net_pnl_rupees": float(frame["net_pnl_rupees"].median()),
        "net_win_rate": float(frame["net_pnl_rupees"].gt(0).mean()),
        "mean_predicted_gross_points": float(frame["predicted_gross_points"].mean()),
        "mean_predicted_excess_points": float(frame["predicted_excess_points"].mean()),
    }


def validation_grid(
    train: pd.DataFrame,
    validation: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    rows = []
    for alpha in RIDGE_ALPHAS:
        models = fit_cell_models(train, alpha)
        predicted = add_predictions(validation, models)
        for gate in GATE_MULTIPLIERS:
            selected = select_non_overlapping(predicted, gate)
            metrics = _basic_metrics(selected)
            rows.append({"alpha": alpha, "gate_multiplier": gate, **metrics})
    grid = pd.DataFrame(rows)
    eligible = grid.loc[grid["trades"].ge(MIN_VALIDATION_TRADES)].copy()
    if eligible.empty:
        winner = grid.sort_values(
            ["trades", "mean_net_pnl_rupees", "alpha", "gate_multiplier"],
            ascending=[False, False, True, True],
        ).iloc[0]
    else:
        winner = eligible.sort_values(
            ["mean_net_pnl_rupees", "net_pnl_rupees", "trades", "alpha", "gate_multiplier"],
            ascending=[False, False, False, True, True],
        ).iloc[0]
    return grid, {
        "alpha": float(winner["alpha"]),
        "gate_multiplier": float(winner["gate_multiplier"]),
    }


def _block_bootstrap_mean_ci(frame: pd.DataFrame) -> tuple[float, float]:
    if frame.empty:
        return float("nan"), float("nan")
    daily = [part["net_pnl_rupees"].to_numpy(float) for _, part in frame.groupby("trade_date")]
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    means = np.empty(BOOTSTRAP_REPLICATIONS)
    for index in range(BOOTSTRAP_REPLICATIONS):
        sampled = rng.integers(0, len(daily), size=len(daily))
        total = sum(float(daily[item].sum()) for item in sampled)
        count = sum(int(len(daily[item])) for item in sampled)
        means[index] = total / count
    return float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))


def _add_margin(
    selected: pd.DataFrame,
    observations: pd.DataFrame,
) -> pd.DataFrame:
    output = selected.copy()
    indexed = {
        (int(trade_id), int(horizon)): part
        for (trade_id, horizon), part in observations.groupby(
            ["trade_id", "horizon_minutes"], sort=False
        )
    }
    margins = []
    for row in output.itertuples(index=False):
        weights = {
            leg: int(value) for leg, value in STRUCTURES[str(row.structure)].items()
        }
        full_part = indexed[(int(row.trade_id), int(row.horizon_minutes))]
        part = full_part.loc[full_part["leg"].isin(weights)].copy()
        margins.append(_margin_for_structure(part, weights))
    output["margin_rupees"] = margins
    output["net_return_on_margin"] = output["net_pnl_rupees"] / output["margin_rupees"]
    return output


def _coverage_gate(
    selected: pd.DataFrame,
    coverage: dict[tuple[str, int], float],
) -> tuple[bool, list[dict[str, Any]]]:
    composition = []
    for (structure, horizon), part in selected.groupby(
        ["structure", "horizon_minutes"], sort=True
    ):
        share = float(len(part) / len(selected)) if len(selected) else 0.0
        cell_coverage = float(coverage[(str(structure), int(horizon))])
        composition.append(
            {
                "structure": str(structure),
                "horizon_minutes": int(horizon),
                "trades": int(len(part)),
                "trade_share": share,
                "unconditional_label_coverage": cell_coverage,
                "material_cell_pass": bool(share < 0.10 or cell_coverage >= 0.80),
            }
        )
    return all(item["material_cell_pass"] for item in composition), composition


def evaluate_acceptance(
    selected: pd.DataFrame,
    coverage: dict[tuple[str, int], float],
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    lower, upper = _block_bootstrap_mean_ci(selected)
    local = selected.copy()
    local["entry_ts"] = pd.to_datetime(local["entry_ts"], utc=True)
    local["month"] = (
        local["entry_ts"].dt.tz_convert("Asia/Kolkata").dt.tz_localize(None).dt.to_period("M").astype(str)
    )
    local["year"] = local["entry_ts"].dt.year
    monthly = (
        local.groupby("month", as_index=False)
        .agg(
            trades=("trade_id", "size"),
            gross_pnl_rupees=("gross_pnl_rupees", "sum"),
            total_cost_rupees=("total_cost_rupees", "sum"),
            net_pnl_rupees=("net_pnl_rupees", "sum"),
        )
        .sort_values("month")
    )
    yearly = (
        local.groupby("year", as_index=False)
        .agg(trades=("trade_id", "size"), net_pnl_rupees=("net_pnl_rupees", "sum"))
        .sort_values("year")
    )
    positive_months = monthly.loc[monthly["net_pnl_rupees"].gt(0), "net_pnl_rupees"]
    concentration = (
        float(positive_months.max() / positive_months.sum())
        if not positive_months.empty and positive_months.sum() > 0
        else float("nan")
    )
    coverage_pass, composition = _coverage_gate(local, coverage)
    gates = {
        "minimum_trades": bool(len(local) >= MIN_CONFIRMATION_TRADES),
        "positive_mean_net": bool(local["net_pnl_rupees"].mean() > 0),
        "bootstrap_ci_lower_above_zero": bool(lower > 0),
        "positive_aggregate_net": bool(local["net_pnl_rupees"].sum() > 0),
        "positive_month_fraction_at_least_60pct": bool(
            monthly["net_pnl_rupees"].gt(0).mean() >= 0.60
        ),
        "every_year_positive": bool(yearly["net_pnl_rupees"].gt(0).all()),
        "positive_month_concentration_at_most_40pct": bool(
            np.isfinite(concentration) and concentration <= 0.40
        ),
        "material_cell_coverage_at_least_80pct": coverage_pass,
    }
    return (
        {
            "passed": bool(all(gates.values())),
            "gates": gates,
            "bootstrap_mean_net_95pct_ci": [lower, upper],
            "positive_month_fraction": float(monthly["net_pnl_rupees"].gt(0).mean()),
            "positive_month_concentration": concentration,
            "composition": composition,
        },
        monthly,
        yearly,
    )


def build_calibration(frame: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    parts = []
    correlations = []
    for (structure, horizon), part in frame.groupby(
        ["structure", "horizon_minutes"], sort=True
    ):
        local = part.copy()
        correlation = local["predicted_gross_points"].corr(local["gross_pnl_points"])
        correlations.append(
            {
                "structure": str(structure),
                "horizon_minutes": int(horizon),
                "observations": int(len(local)),
                "predicted_vs_realized_gross_correlation": float(correlation),
            }
        )
        local["prediction_decile"] = pd.qcut(
            local["predicted_gross_points"], 10, labels=False, duplicates="drop"
        )
        parts.append(
            local.groupby("prediction_decile", as_index=False)
            .agg(
                observations=("trade_id", "size"),
                mean_predicted_gross_points=("predicted_gross_points", "mean"),
                mean_realized_gross_points=("gross_pnl_points", "mean"),
                mean_net_pnl_rupees=("net_pnl_rupees", "mean"),
            )
            .assign(structure=str(structure), horizon_minutes=int(horizon))
        )
    calibration = pd.concat(parts, ignore_index=True)
    return calibration, correlations


def run(
    *,
    dataset_path: Path,
    observations_path: Path,
    dataset_summary_path: Path,
    validation_grid_path: Path,
    tradebook_path: Path,
    monthly_path: Path,
    yearly_path: Path,
    calibration_path: Path,
    models_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    dataset = pd.read_parquet(dataset_path)
    dataset["trade_date"] = pd.to_datetime(dataset["trade_date"])
    train = dataset.loc[dataset["trade_date"].lt("2024-01-01")].copy()
    validation = dataset.loc[
        dataset["trade_date"].ge("2024-01-01")
        & dataset["trade_date"].lt("2025-01-01")
    ].copy()
    confirmation = dataset.loc[dataset["trade_date"].ge("2025-01-01")].copy()
    grid, winner = validation_grid(train, validation)
    development = pd.concat([train, validation], ignore_index=True)
    models = fit_cell_models(development, winner["alpha"])
    predicted = add_predictions(confirmation, models)
    calibration, correlations = build_calibration(predicted)
    selected = select_non_overlapping(predicted, winner["gate_multiplier"])
    observations = pd.read_parquet(observations_path)
    selected = _add_margin(selected, observations)
    dataset_summary = json.loads(dataset_summary_path.read_text(encoding="utf-8"))
    coverage = {
        (str(row["structure"]), int(row["horizon_minutes"])): float(row["coverage"])
        for row in dataset_summary["coverage"]
    }
    acceptance, monthly, yearly = evaluate_acceptance(selected, coverage)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "protocol": str(PROTOCOL_PATH),
        "split_rows": {
            "train": int(len(train)),
            "validation": int(len(validation)),
            "confirmation": int(len(confirmation)),
        },
        "selected_validation_config": winner,
        "confirmation": {
            **_basic_metrics(selected),
            "date_min": str(selected["trade_date"].min()) if len(selected) else None,
            "date_max": str(selected["trade_date"].max()) if len(selected) else None,
            "mean_margin_rupees": float(selected["margin_rupees"].mean()),
            "mean_net_return_on_margin": float(
                selected["net_return_on_margin"].mean()
            ),
        },
        "prediction_correlations": correlations,
        "acceptance": acceptance,
        "decision": "PASS" if acceptance["passed"] else "FAIL_CLOSE_HYPOTHESIS_FAMILY",
        "limitations": [
            "The confirmation period is post-selection walk-forward evidence, not pristine OOS.",
            "Ridge and gate hyperparameters are selected once on calendar 2024.",
            "Any material reliance on a below-80%-coverage cell fails the strategy regardless of P&L.",
        ],
    }
    model_payload = {
        "schema_version": "phase5-final-attempt-models/v1",
        "alpha": winner["alpha"],
        "gate_multiplier": winner["gate_multiplier"],
        "models": {
            f"{structure}__{horizon}": model.to_dict()
            for (structure, horizon), model in models.items()
        },
    }
    for path in (
        validation_grid_path,
        tradebook_path,
        monthly_path,
        yearly_path,
        calibration_path,
        models_path,
        summary_path,
        manifest_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    grid.to_csv(validation_grid_path, index=False)
    selected.to_csv(tradebook_path, index=False)
    monthly.to_csv(monthly_path, index=False)
    yearly.to_csv(yearly_path, index=False)
    calibration.to_csv(calibration_path, index=False)
    models_path.write_text(
        json.dumps(_json_safe(model_payload), indent=2, sort_keys=True), encoding="utf-8"
    )
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = {
        "schema_version": "phase5-final-attempt-strategy-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(dataset_path.resolve()), "sha256": _sha256(dataset_path)},
            {"path": str(observations_path.resolve()), "sha256": _sha256(observations_path)},
            {
                "path": str(dataset_summary_path.resolve()),
                "sha256": _sha256(dataset_summary_path),
            },
            {"path": str(PROTOCOL_PATH.resolve()), "sha256": _sha256(PROTOCOL_PATH)},
        ],
        "outputs": [
            {"path": str(validation_grid_path.resolve()), "sha256": _sha256(validation_grid_path)},
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
            {"path": str(monthly_path.resolve()), "sha256": _sha256(monthly_path)},
            {"path": str(yearly_path.resolve()), "sha256": _sha256(yearly_path)},
            {"path": str(calibration_path.resolve()), "sha256": _sha256(calibration_path)},
            {"path": str(models_path.resolve()), "sha256": _sha256(models_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
        ],
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset", type=Path, default=Path("audit/phase5_final_attempt_dataset.parquet")
    )
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase5_final_attempt_observations.parquet"),
    )
    parser.add_argument(
        "--dataset-summary",
        type=Path,
        default=Path("audit/phase5_final_attempt_dataset_summary.json"),
    )
    parser.add_argument(
        "--validation-grid",
        type=Path,
        default=Path("audit/phase5_final_attempt_validation_grid.csv"),
    )
    parser.add_argument(
        "--tradebook",
        type=Path,
        default=Path("audit/phase5_final_attempt_tradebook.csv"),
    )
    parser.add_argument(
        "--monthly",
        type=Path,
        default=Path("audit/phase5_final_attempt_monthly.csv"),
    )
    parser.add_argument(
        "--yearly",
        type=Path,
        default=Path("audit/phase5_final_attempt_yearly.csv"),
    )
    parser.add_argument(
        "--calibration",
        type=Path,
        default=Path("audit/phase5_final_attempt_calibration.csv"),
    )
    parser.add_argument(
        "--models",
        type=Path,
        default=Path("audit/phase5_final_attempt_models.json"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase5_final_attempt_summary.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase5_final_attempt_manifest.json"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        dataset_path=args.dataset,
        observations_path=args.observations,
        dataset_summary_path=args.dataset_summary,
        validation_grid_path=args.validation_grid,
        tradebook_path=args.tradebook,
        monthly_path=args.monthly,
        yearly_path=args.yearly,
        calibration_path=args.calibration,
        models_path=args.models,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
