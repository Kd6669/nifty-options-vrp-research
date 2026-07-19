"""Close Phase 2 around one frozen VRP-level-and-direction hypothesis.

The closeout recomputes the causal next-minute zero-crossing evidence from the
frozen-contract 60-minute structure panel, collects the decisive data-boundary
and curve diagnostics, and writes one machine-readable final hypothesis file.
It does not apply costs, slippage, or SPAN capital and therefore cannot promote
the hypothesis into a trading edge.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


CONTRACT_SCHEMA = "phase2-final-hypothesis/v1"
ENTRY_CUTOFF = "14:15"

STRUCTURE_COLUMNS = [
    "entry_ts",
    "trade_date",
    "entry_time",
    "vrp_crossing",
    "vrp_tod_percentile",
    "signal_vrp_var_act365",
    "short_iron_condor__pnl_points",
    "short_iron_condor__return_on_max_loss",
    "long_iron_condor__pnl_points",
    "long_iron_condor__return_on_max_loss",
]


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_contract(path: Path) -> dict[str, Any]:
    contract = _load_json(path)
    if contract.get("schema_version") != CONTRACT_SCHEMA:
        raise ValueError(
            f"unsupported hypothesis schema {contract.get('schema_version')!r}; "
            f"expected {CONTRACT_SCHEMA!r}"
        )
    required = ("hypothesis_id", "claim", "null", "alternative", "limitations")
    missing = [key for key in required if not contract.get(key)]
    if missing:
        raise ValueError(f"hypothesis contract is missing: {', '.join(missing)}")
    return contract


def _exact_lead(frame: pd.DataFrame, column: str) -> pd.Series:
    grouped = frame.groupby("trade_date", sort=False)
    leading = grouped[column].shift(-1)
    leading_ts = grouped["entry_ts"].shift(-1)
    elapsed = (leading_ts - frame["entry_ts"]).dt.total_seconds() / 60
    return leading.where(elapsed == 1)


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return {"count": 0}
    tail_count = max(1, int(np.ceil(len(clean) * 0.05)))
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "win_rate": float((clean > 0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p95": float(clean.quantile(0.95)),
        "cvar05": float(clean.nsmallest(tail_count).mean()),
    }


def _bootstrap_mean_ci(
    values: pd.Series,
    *,
    seed: int = 20260718,
    samples: int = 5000,
) -> list[float]:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna().to_numpy(dtype=float)
    if len(clean) < 10:
        return [float("nan"), float("nan")]
    rng = np.random.default_rng(seed)
    means = np.empty(samples, dtype=float)
    for index in range(samples):
        means[index] = rng.choice(clean, size=len(clean), replace=True).mean()
    return [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))]


def build_first_daily_crossings(structure_path: Path) -> pd.DataFrame:
    """Return causal first-daily zero crossings with exact next-minute outcomes."""

    frame = pd.read_parquet(
        structure_path,
        columns=STRUCTURE_COLUMNS,
        filters=[("horizon_minutes", "=", 60)],
    )
    frame["entry_ts"] = pd.to_datetime(frame["entry_ts"], utc=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    frame = frame.sort_values(["trade_date", "entry_ts"]).reset_index(drop=True)

    for column in (
        "entry_ts",
        "entry_time",
        "short_iron_condor__pnl_points",
        "short_iron_condor__return_on_max_loss",
        "long_iron_condor__pnl_points",
        "long_iron_condor__return_on_max_loss",
    ):
        frame[f"next_{column}"] = _exact_lead(frame, column)

    eligible = frame[
        frame["vrp_crossing"].isin(["cross_up", "cross_down"])
        & frame["next_entry_time"].le(ENTRY_CUTOFF)
        & frame["next_short_iron_condor__pnl_points"].notna()
        & frame["next_long_iron_condor__pnl_points"].notna()
    ].copy()
    eligible = eligible.sort_values(["trade_date", "entry_ts"])
    eligible = eligible.drop_duplicates(["trade_date", "vrp_crossing"], keep="first")
    eligible["year"] = eligible["trade_date"].str[:4].astype(int)
    return eligible


def _crossing_summary(events: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    yearly: list[dict[str, Any]] = []
    definitions = (
        ("cross_up", "short", "primary"),
        ("cross_up", "long", "exact_inverse"),
        ("cross_down", "long", "symmetry_diagnostic"),
        ("cross_down", "short", "symmetry_inverse"),
    )
    for crossing, structure, role in definitions:
        group = events[events["vrp_crossing"].eq(crossing)]
        pnl_column = f"next_{structure}_iron_condor__pnl_points"
        return_column = f"next_{structure}_iron_condor__return_on_max_loss"
        rows.append(
            {
                "crossing": crossing,
                "structure": f"{structure}_iron_condor",
                "role": role,
                "dates": int(group["trade_date"].nunique()),
                "median_causal_vrp_percentile": float(group["vrp_tod_percentile"].median()),
                "pnl_points": _distribution(group[pnl_column]),
                "return_on_max_loss": _distribution(group[return_column]),
                "return_mean_bootstrap_95": _bootstrap_mean_ci(group[return_column]),
            }
        )
        for year, year_group in group.groupby("year", sort=True):
            yearly.append(
                {
                    "crossing": crossing,
                    "structure": f"{structure}_iron_condor",
                    "role": role,
                    "year": int(year),
                    "pnl_points": _distribution(year_group[pnl_column]),
                    "return_on_max_loss": _distribution(year_group[return_column]),
                }
            )
    return rows, yearly


def _curve_summary(curve: dict[str, Any]) -> dict[str, Any]:
    paired = [
        row
        for row in curve["paired_session_ladder_comparisons"]
        if row["outcome"] == "return_on_max_loss"
    ]
    acceleration_rows = []
    for row in curve["acceleration_summary"]:
        upper_short = (
            row["direction"] == "up"
            and np.isclose(row["threshold"], 0.90)
            and row["acceleration_bin"] == "q75_100"
            and row["structure"] == "short_iron_condor"
        )
        lower_long = (
            row["direction"] == "down"
            and np.isclose(row["threshold"], 0.10)
            and row["acceleration_bin"] == "q75_100"
            and row["structure"] == "long_iron_condor"
        )
        if upper_short or lower_long:
            acceleration_rows.append(row)
    return {
        "coverage": curve["coverage"],
        "candidate_confidence_ladders": curve["candidate_confidence_ladders"],
        "paired_session_ladder_comparisons": paired,
        "selected_acceleration_diagnostics": acceleration_rows,
        "conclusion": (
            "VRP level and crossing direction remain features; acceleration and "
            "percentile-to-lots leverage are excluded from the final hypothesis."
        ),
    }


def close(
    contract_path: Path,
    structure_path: Path,
    unconditional_path: Path,
    intraday_path: Path,
    matched_variance_path: Path,
    defined_risk_events_path: Path,
    curve_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    contract = _load_contract(contract_path)
    unconditional = _load_json(unconditional_path)
    intraday = _load_json(intraday_path)
    matched = _load_json(matched_variance_path)
    defined_events = _load_json(defined_risk_events_path)
    curve = _load_json(curve_path)

    events = build_first_daily_crossings(structure_path)
    crossing_rows, yearly_rows = _crossing_summary(events)
    primary = next(row for row in crossing_rows if row["role"] == "primary")
    symmetry = next(row for row in crossing_rows if row["role"] == "symmetry_diagnostic")

    structure_60 = next(
        row
        for row in unconditional["structure_matrix"]
        if row["family"] == "iron_condor"
        and row["horizon_minutes"] == 60
        and row["short_offset"] == 1
        and row["wing_offset"] == 3
    )
    matched_60 = next(
        row for row in matched["intraday_act365_horizons"] if row["horizon_minutes"] == 60
    )
    defined_60 = next(
        row for row in defined_events["structure_coverage"] if row["horizon_minutes"] == 60
    )

    delay = (
        pd.to_datetime(events["next_entry_ts"], utc=True)
        - pd.to_datetime(events["entry_ts"], utc=True)
    ).dt.total_seconds() / 60
    inverse_error = float(
        (
            events["next_short_iron_condor__pnl_points"]
            + events["next_long_iron_condor__pnl_points"]
        )
        .abs()
        .max()
    )
    report = {
        "schema_version": "phase2-final-hypothesis-closeout/v1",
        "status": "hypothesis_formulation_closed",
        "hypothesis": contract,
        "decision": {
            "primary_formulation_evidence": (
                "gross_supported"
                if primary["return_mean_bootstrap_95"][0] > 0
                else "gross_inconclusive"
            ),
            "net_edge_status": "not_tested",
            "cross_down_long_condor": (
                "not_supported_as_standalone_alpha"
                if symmetry["return_mean_bootstrap_95"][0] <= 0
                else "gross_supported"
            ),
            "percentile_leverage": "rejected_from_final_hypothesis",
            "acceleration": "diagnostic_only",
            "next_module": "cost_slippage_margin_and_oos_confirmation",
        },
        "data_boundary": {
            "source_dates": unconditional["source_dates"],
            "minute_rows": int(intraday["coverage"]["minute_rows"]),
            "first_date": intraday["coverage"]["first_date"],
            "last_date": intraday["coverage"]["last_date"],
            "atm_iv_rows": int(intraday["coverage"]["atm_iv_rows"]),
            "complete_local_surface_rows": int(
                intraday["coverage"]["complete_wing_surface_rows"]
            ),
            "unconditional_atm_1_3_60m": structure_60,
            "complete_exact_contract_60m_structure_marks": defined_60[
                "structure_complete_counts"
            ],
        },
        "normalization_evidence": {
            "matched_60m_act365": matched_60,
            "interpretation": (
                "Unconditional matched-clock intraday VRP is usually negative; "
                "the hypothesis is conditional on level and direction, not an "
                "unconditional short-volatility claim."
            ),
        },
        "first_daily_next_minute_zero_crossings": crossing_rows,
        "yearly_zero_crossing_summary": yearly_rows,
        "curve_extension": _curve_summary(curve),
        "invariants": {
            "event_rows": int(len(events)),
            "event_dates": int(events["trade_date"].nunique()),
            "minimum_signal_to_entry_minutes": float(delay.min()),
            "maximum_signal_to_entry_minutes": float(delay.max()),
            "maximum_long_short_pnl_inverse_error": inverse_error,
            "duplicate_date_crossing_rows": int(
                events.duplicated(["trade_date", "vrp_crossing"]).sum()
            ),
        },
        "input_artifacts": [
            str(contract_path.resolve()),
            str(structure_path.resolve()),
            str(unconditional_path.resolve()),
            str(intraday_path.resolve()),
            str(matched_variance_path.resolve()),
            str(defined_risk_events_path.resolve()),
            str(curve_path.resolve()),
        ],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--contract", type=Path, default=Path("research/phase2/final_hypothesis.json")
    )
    parser.add_argument(
        "--structures",
        type=Path,
        default=Path("audit/phase2_defined_risk_structure_paths.parquet"),
    )
    parser.add_argument(
        "--unconditional",
        type=Path,
        default=Path("audit/phase2_unconditional_observed_computed.json"),
    )
    parser.add_argument(
        "--intraday", type=Path, default=Path("audit/phase2_intraday_volatility.json")
    )
    parser.add_argument(
        "--matched",
        type=Path,
        default=Path("audit/phase2_matched_realized_variance.json"),
    )
    parser.add_argument(
        "--defined-events",
        type=Path,
        default=Path("audit/phase2_defined_risk_vrp_events.json"),
    )
    parser.add_argument(
        "--curve", type=Path, default=Path("audit/phase2_vrp_curve_crossings.json")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("audit/phase2_final_hypothesis_closeout.json"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    report = close(
        args.contract,
        args.structures,
        args.unconditional,
        args.intraday,
        args.matched,
        args.defined_events,
        args.curve,
        args.output,
    )
    print(json.dumps({"status": report["status"], **report["decision"]}, indent=2))


if __name__ == "__main__":
    main()
