"""Compare all existing VRP entry signals on one cost-aware 180-minute horizon."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase2.close_hypothesis_formulation import build_first_daily_crossings
from research.phase3.run_tail_percentile_backtests import build_tail_events
from research.phase4.run_cost_aware_discovery import (
    _load_observations,
    _margin_for_structure,
    _trade_record,
)
from research.phase6.run_vrp_reversal_test import build_reversal_events


SCHEMA_VERSION = "phase7-180min-signal-comparison/v1"
PROTOCOL_PATH = Path("docs/research/PHASE7_180MIN_COMPARISON_PROTOCOL.md")
HORIZON_MINUTES = 180
BOOTSTRAP_REPLICATIONS = 5000
BOOTSTRAP_SEED = 20260718


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


def build_signal_memberships(
    *,
    structure_path: Path,
    curve_path: Path,
) -> pd.DataFrame:
    zero = build_first_daily_crossings(structure_path).copy()
    zero["entry_ts"] = pd.to_datetime(zero["next_entry_ts"], utc=True)
    zero["signal_ts"] = pd.to_datetime(zero["entry_ts"], utc=True) - pd.Timedelta(minutes=1)
    zero["signal_name"] = zero["vrp_crossing"].map(
        {"cross_up": "zero_up", "cross_down": "zero_down"}
    )
    zero["signal_group"] = "zero_crossing"
    zero["requested_structure"] = zero["vrp_crossing"].map(
        {"cross_up": "short_iron_condor", "cross_down": "long_iron_condor"}
    )
    zero = zero[
        [
            "trade_date",
            "signal_ts",
            "entry_ts",
            "signal_name",
            "signal_group",
            "requested_structure",
        ]
    ]

    tail = build_tail_events(curve_path).copy()
    tail["signal_name"] = tail.apply(
        lambda row: f"q{int(round(float(row.threshold) * 100)):02d}_{row.direction}",
        axis=1,
    )
    tail["signal_group"] = "percentile_crossing"
    tail["requested_structure"] = "short_iron_condor"
    tail = tail[
        [
            "trade_date",
            "signal_ts",
            "entry_ts",
            "signal_name",
            "signal_group",
            "requested_structure",
        ]
    ]

    curve = pd.read_parquet(curve_path)
    reversal = build_reversal_events(curve).copy()
    reversal["signal_name"] = reversal["reversal_type"].map(
        {"top_to_zero": "reversal_top", "bottom_to_zero": "reversal_bottom"}
    )
    reversal["signal_group"] = "tail_reversal"
    reversal["requested_structure"] = reversal["reversal_type"].map(
        {"top_to_zero": "long_iron_condor", "bottom_to_zero": "short_iron_condor"}
    )
    reversal = reversal[
        [
            "trade_date",
            "signal_ts",
            "entry_ts",
            "signal_name",
            "signal_group",
            "requested_structure",
        ]
    ]

    membership = pd.concat([zero, tail, reversal], ignore_index=True)
    membership["entry_ts"] = pd.to_datetime(membership["entry_ts"], utc=True)
    membership["signal_ts"] = pd.to_datetime(membership["signal_ts"], utc=True)
    membership["trade_date"] = pd.to_datetime(membership["trade_date"]).dt.date
    membership = membership.sort_values(["entry_ts", "signal_group", "signal_name"])
    membership["membership_id"] = np.arange(1, len(membership) + 1, dtype=np.int64)
    return membership.reset_index(drop=True)


def deduplicate_executions(membership: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    executions = (
        membership[["trade_date", "entry_ts"]]
        .drop_duplicates()
        .sort_values("entry_ts")
        .reset_index(drop=True)
    )
    executions["trade_id"] = np.arange(1, len(executions) + 1, dtype=np.int64)
    executions["signal_family"] = "phase7_180min"
    keyed = membership.merge(
        executions[["trade_date", "entry_ts", "trade_id"]],
        on=["trade_date", "entry_ts"],
        how="left",
        validate="many_to_one",
    )
    return executions, keyed


def build_execution_outcomes(observations: pd.DataFrame) -> pd.DataFrame:
    rows = []
    selected = observations.loc[observations["horizon_minutes"].eq(HORIZON_MINUTES)].copy()
    for trade_id, full_part in selected.groupby("trade_id", sort=True):
        for structure in ("short_iron_condor", "long_iron_condor"):
            weights = {leg: int(value) for leg, value in STRUCTURES[structure].items()}
            part = full_part.loc[full_part["leg"].isin(weights)].copy()
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            margin = _margin_for_structure(part, weights)
            rows.append(
                _trade_record(
                    part,
                    structure=structure,
                    weights=weights,
                    margin=margin,
                )
            )
    return pd.DataFrame(rows)


def attach_signal_mappings(
    membership: pd.DataFrame,
    outcomes: pd.DataFrame,
) -> pd.DataFrame:
    # Membership owns the event timestamp/date columns.  The execution outcome repeats
    # them for auditability, but retaining both sides would create pandas ``_x``/``_y``
    # names and make the downstream schema depend on the merge implementation.
    repeated_execution_columns = [
        column
        for column in ("trade_date", "signal_family", "entry_ts")
        if column in outcomes.columns
    ]
    outcome_payload = outcomes.drop(columns=repeated_execution_columns)
    frame = membership.merge(
        outcome_payload,
        on="trade_id",
        how="left",
        validate="many_to_many",
    )
    frame = frame.loc[frame["structure"].notna()].copy()
    frame["mapping"] = np.where(
        frame["structure"].eq(frame["requested_structure"]), "requested", "inverse"
    )
    return frame.sort_values(["mapping", "signal_name", "entry_ts"]).reset_index(drop=True)


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return {"count": 0}
    return {
        "count": int(len(clean)),
        "sum": float(clean.sum()),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "win_rate": float(clean.gt(0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p95": float(clean.quantile(0.95)),
    }


def _bootstrap_mean_ci(frame: pd.DataFrame) -> list[float]:
    daily = [part["net_pnl_rupees"].to_numpy(float) for _, part in frame.groupby("trade_date")]
    if not daily:
        return [float("nan"), float("nan")]
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    means = np.empty(BOOTSTRAP_REPLICATIONS)
    for index in range(BOOTSTRAP_REPLICATIONS):
        sampled = rng.integers(0, len(daily), size=len(daily))
        total = sum(float(daily[item].sum()) for item in sampled)
        count = sum(len(daily[item]) for item in sampled)
        means[index] = total / count
    return [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))]


def summarize(membership: pd.DataFrame, tradebook: pd.DataFrame) -> dict[str, Any]:
    expected = membership.groupby("signal_name")["membership_id"].nunique().to_dict()
    cells = []
    for (mapping, group, signal), part in tradebook.groupby(
        ["mapping", "signal_group", "signal_name"], sort=True
    ):
        cells.append(
            {
                "mapping": str(mapping),
                "signal_group": str(group),
                "signal_name": str(signal),
                "expected_events": int(expected[str(signal)]),
                "trades": int(len(part)),
                "unevaluated_events": int(expected[str(signal)] - len(part)),
                "coverage": float(len(part) / expected[str(signal)]),
                "gross_pnl_points": _distribution(part["gross_pnl_points"]),
                "gross_pnl_rupees": _distribution(part["gross_pnl_rupees"]),
                "cost_rupees": _distribution(part["total_cost_rupees"]),
                "net_pnl_rupees": _distribution(part["net_pnl_rupees"]),
                "net_return_on_margin": _distribution(part["net_return_on_margin"]),
                "bootstrap_mean_net_95pct_ci": _bootstrap_mean_ci(part),
            }
        )
    credible = [
        cell
        for cell in cells
        if cell["trades"] >= 100
        and cell["coverage"] >= 0.80
        and cell["net_pnl_rupees"]["mean"] > 0
        and cell["bootstrap_mean_net_95pct_ci"][0] > 0
    ]
    ranking = sorted(
        cells,
        key=lambda cell: (
            cell["net_pnl_rupees"]["mean"],
            cell["coverage"],
            cell["trades"],
        ),
        reverse=True,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": str(PROTOCOL_PATH),
        "horizon_minutes": HORIZON_MINUTES,
        "membership_rows": int(len(membership)),
        "unique_entry_timestamps": int(membership["trade_id"].nunique()),
        "cells": cells,
        "ranking": [
            {
                "mapping": cell["mapping"],
                "signal_name": cell["signal_name"],
                "expected_events": cell["expected_events"],
                "trades": cell["trades"],
                "unevaluated_events": cell["unevaluated_events"],
                "coverage": cell["coverage"],
                "mean_gross_rupees": cell["gross_pnl_rupees"]["mean"],
                "mean_cost_rupees": cell["cost_rupees"]["mean"],
                "mean_net_rupees": cell["net_pnl_rupees"]["mean"],
                "bootstrap_mean_net_95pct_ci": cell["bootstrap_mean_net_95pct_ci"],
            }
            for cell in ranking
        ],
        "credible_positive_cells": credible,
        "decision": "NO_CREDIBLE_180MIN_EDGE" if not credible else "RESEARCH_LEAD_ONLY",
        "limitations": [
            "Signal cells overlap and must not be aggregated as a portfolio.",
            "Every signal family was already explored before this unified comparison.",
            "The 180-minute exact-contract coverage is non-random and generally below the primary boundary.",
            "Results beyond 180 minutes are outside the current dataset's defensible scope.",
        ],
    }


def run(
    *,
    gold_root: Path,
    structure_path: Path,
    curve_path: Path,
    surface_path: Path,
    membership_path: Path,
    execution_path: Path,
    observations_path: Path,
    tradebook_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    membership = build_signal_memberships(structure_path=structure_path, curve_path=curve_path)
    executions, membership = deduplicate_executions(membership)
    observations = _load_observations(
        executions,
        gold_glob=str(gold_root / "year=*" / "month=*" / "part-*.parquet"),
        surface_path=surface_path,
    )
    outcomes = build_execution_outcomes(observations)
    tradebook = attach_signal_mappings(membership, outcomes)
    summary = summarize(membership, tradebook)
    for path in (
        membership_path,
        execution_path,
        observations_path,
        tradebook_path,
        summary_path,
        manifest_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    membership.to_csv(membership_path, index=False)
    executions.to_csv(execution_path, index=False)
    observations.to_parquet(observations_path, index=False, compression="zstd")
    tradebook.to_csv(tradebook_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = {
        "schema_version": "phase7-180min-signal-comparison-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(PROTOCOL_PATH.resolve()), "sha256": _sha256(PROTOCOL_PATH)},
            {"path": str(structure_path.resolve()), "sha256": _sha256(structure_path)},
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(membership_path.resolve()), "sha256": _sha256(membership_path)},
            {"path": str(execution_path.resolve()), "sha256": _sha256(execution_path)},
            {"path": str(observations_path.resolve()), "sha256": _sha256(observations_path)},
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
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
        "--structure-path",
        type=Path,
        default=Path("audit/phase2_defined_risk_structure_paths.parquet"),
    )
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
        "--membership", type=Path, default=Path("audit/phase7_180min_membership.csv")
    )
    parser.add_argument(
        "--executions", type=Path, default=Path("audit/phase7_180min_executions.csv")
    )
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase7_180min_observations.parquet"),
    )
    parser.add_argument(
        "--tradebook", type=Path, default=Path("audit/phase7_180min_tradebook.csv")
    )
    parser.add_argument(
        "--summary", type=Path, default=Path("audit/phase7_180min_summary.json")
    )
    parser.add_argument(
        "--manifest", type=Path, default=Path("audit/phase7_180min_manifest.json")
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        gold_root=args.gold_root,
        structure_path=args.structure_path,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        membership_path=args.membership,
        execution_path=args.executions,
        observations_path=args.observations,
        tradebook_path=args.tradebook,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
