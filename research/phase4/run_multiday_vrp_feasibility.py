"""Audit exact-contract multi-day coverage and complete-case condor P&L."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from nifty_execution import (
    ExecutedLeg,
    estimate_nifty_option_slippage,
    estimate_round_trip_execution_cost,
    groww_fno_rates_for_date,
)
from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase3.run_full_strategy_backtest import _fill_price


SCHEMA_VERSION = "phase4-multiday-vrp-feasibility/v1"
HOLD_SESSIONS = (1, 2, 3, 5)
MINIMUM_SELL_FILL = 0.05
SIGNAL_STRUCTURES = {
    "upper85_up": "short_iron_condor",
    "lower10_down": "long_iron_condor",
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


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return {"count": 0}
    tail_count = max(1, int(math.ceil(0.05 * len(clean))))
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "win_rate": float((clean > 0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p95": float(clean.quantile(0.95)),
        "cvar05": float(clean.nsmallest(tail_count).mean()),
        "sum": float(clean.sum()),
    }


def _build_requests(
    observations: pd.DataFrame,
    *,
    curve_path: Path,
) -> pd.DataFrame:
    entries = observations.loc[observations["horizon_minutes"].eq(60)].copy()
    entries = entries.drop_duplicates(["trade_id", "leg"])
    selected = []
    for signal, structure in SIGNAL_STRUCTURES.items():
        legs = set(STRUCTURES[structure])
        selected.append(
            entries.loc[
                entries["signal_family"].eq(signal) & entries["leg"].isin(legs)
            ].copy()
        )
    entries = pd.concat(selected, ignore_index=True)
    sessions = (
        duckdb.connect()
        .execute(
            "SELECT DISTINCT cast(trade_date AS DATE) trade_date "
            "FROM read_parquet(?) ORDER BY trade_date",
            [str(curve_path)],
        )
        .fetchdf()["trade_date"]
    )
    session_dates = [pd.Timestamp(value).date() for value in sessions]
    session_index = {value: index for index, value in enumerate(session_dates)}
    rows = []
    for row in entries.itertuples(index=False):
        entry_date = pd.Timestamp(row.trade_date).date()
        base_index = session_index.get(entry_date)
        if base_index is None:
            continue
        for hold in HOLD_SESSIONS:
            target_index = base_index + hold
            target_date = (
                session_dates[target_index] if target_index < len(session_dates) else None
            )
            target_ts = pd.NaT
            if target_date is not None:
                day_delta = target_date - entry_date
                target_ts = pd.Timestamp(row.entry_ts) + pd.Timedelta(days=day_delta.days)
            record = row._asdict()
            record["hold_sessions"] = hold
            record["target_trade_date"] = target_date
            record["target_exit_ts"] = target_ts
            rows.append(record)
    return pd.DataFrame(rows)


def _load_exits(
    requests: pd.DataFrame,
    *,
    gold_glob: str,
    surface_path: Path,
) -> pd.DataFrame:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    connection.register("requests", requests)
    frame = connection.execute(
        """
        SELECT
          requests.*,
          cast(source.close AS DOUBLE) AS multiday_exit_close,
          cast(source.volume AS DOUBLE) AS multiday_exit_volume,
          cast(source.open_interest AS DOUBLE) AS multiday_exit_open_interest,
          cast(source.india_vix AS DOUBLE) AS multiday_exit_india_vix,
          cast(source.mte AS DOUBLE) AS multiday_exit_minutes_to_expiry,
          cast(surface.atm_iv AS DOUBLE) AS multiday_exit_atm_iv
        FROM requests
        LEFT JOIN read_parquet(?, hive_partitioning=true) source
          ON source.timestamp_ist = requests.target_exit_ts
          AND source.trade_date = requests.target_trade_date
          AND cast(source.strike AS DOUBLE) = requests.strike
          AND source.option_type = requests.option_type
          AND source.actual_expiry_date = requests.actual_expiry_date
          AND source.expiry_flag = 'WEEK'
          AND source.close IS NOT NULL
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
        LEFT JOIN read_parquet(?) surface
          ON surface.timestamp_ist = requests.target_exit_ts
        """,
        [gold_glob, str(surface_path)],
    ).fetchdf()
    connection.close()
    slippages = []
    for row in frame.itertuples(index=False):
        if pd.isna(row.multiday_exit_close):
            slippages.append(float("nan"))
            continue
        exit_vix = (
            float(row.multiday_exit_india_vix)
            if pd.notna(row.multiday_exit_india_vix)
            else float(row.multiday_exit_atm_iv) * 100.0
        )
        slippages.append(
            estimate_nifty_option_slippage(
                close=float(row.multiday_exit_close),
                volume=float(row.multiday_exit_volume),
                open_interest=float(row.multiday_exit_open_interest),
                minutes_to_expiry=float(row.multiday_exit_minutes_to_expiry),
                india_vix=exit_vix,
            ).slippage_per_unit
        )
    frame["multiday_exit_slippage_per_unit"] = slippages
    return frame


def build_tradebook(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (signal, hold, trade_id), part in frame.groupby(
        ["signal_family", "hold_sessions", "trade_id"], sort=True
    ):
        structure = SIGNAL_STRUCTURES[str(signal)]
        weights = {leg: int(value) for leg, value in STRUCTURES[structure].items()}
        part = part.loc[part["leg"].isin(weights)].copy()
        complete = len(part) == len(weights) and part["multiday_exit_close"].notna().all()
        if not complete:
            continue
        first = part.iloc[0]
        lot_size = int(first.lot_size)
        gross_points = 0.0
        entry_legs = []
        exit_legs = []
        for row in part.itertuples(index=False):
            weight = weights[str(row.leg)]
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
                float(row.multiday_exit_close),
                float(row.multiday_exit_slippage_per_unit),
                exit_side,
                minimum_sell_fill=MINIMUM_SELL_FILL,
            )
            entry_legs.append(
                ExecutedLeg(
                    entry_side,
                    "OPT",
                    entry_fill,
                    quantity,
                    slippage_per_unit=float(row.entry_slippage_per_unit),
                )
            )
            exit_legs.append(
                ExecutedLeg(
                    exit_side,
                    "OPT",
                    exit_fill,
                    quantity,
                    slippage_per_unit=float(row.multiday_exit_slippage_per_unit),
                )
            )
            gross_points += weight * (
                float(row.multiday_exit_close) - float(row.entry_close)
            )
        costs = estimate_round_trip_execution_cost(
            entry_legs=entry_legs,
            exit_legs=exit_legs,
            entry_rates=groww_fno_rates_for_date(pd.Timestamp(first.entry_ts).date()),
            exit_rates=groww_fno_rates_for_date(
                pd.Timestamp(first.target_exit_ts).date()
            ),
        )
        gross_rupees = gross_points * lot_size
        rows.append(
            {
                "trade_id": int(trade_id),
                "signal_family": str(signal),
                "structure": structure,
                "hold_sessions": int(hold),
                "entry_ts": pd.Timestamp(first.entry_ts),
                "exit_ts": pd.Timestamp(first.target_exit_ts),
                "entry_trade_date": str(first.trade_date),
                "exit_trade_date": str(first.target_trade_date),
                "expiry_date": str(first.actual_expiry_date),
                "lot_size": lot_size,
                "gross_pnl_points": gross_points,
                "gross_pnl_rupees": gross_rupees,
                "total_cost_rupees": costs.total,
                "cost_hurdle_points": costs.total / lot_size,
                "net_pnl_rupees": gross_rupees - costs.total,
            }
        )
    return pd.DataFrame(rows)


def summarize(
    frame: pd.DataFrame,
    tradebook: pd.DataFrame,
) -> dict[str, Any]:
    expected = frame.groupby("signal_family")["trade_id"].nunique().to_dict()
    coverage_rows = []
    pnl_rows = []
    complete = (
        frame.assign(leg_available=frame["multiday_exit_close"].notna())
        .groupby(["signal_family", "hold_sessions", "trade_id"])
        .agg(available_legs=("leg_available", "sum"), requested_legs=("leg", "size"))
        .reset_index()
    )
    complete["complete"] = complete["available_legs"].eq(complete["requested_legs"])
    for (signal, hold), part in complete.groupby(
        ["signal_family", "hold_sessions"], sort=True
    ):
        available = int(part["complete"].sum())
        coverage_rows.append(
            {
                "signal_family": str(signal),
                "structure": SIGNAL_STRUCTURES[str(signal)],
                "hold_sessions": int(hold),
                "expected_events": int(expected[str(signal)]),
                "complete_events": available,
                "exact_contract_coverage": float(available / expected[str(signal)]),
            }
        )
    for (signal, hold), part in tradebook.groupby(
        ["signal_family", "hold_sessions"], sort=True
    ):
        pnl_rows.append(
            {
                "signal_family": str(signal),
                "structure": SIGNAL_STRUCTURES[str(signal)],
                "hold_sessions": int(hold),
                "gross_points": _distribution(part["gross_pnl_points"]),
                "cost_hurdle_points": _distribution(part["cost_hurdle_points"]),
                "net_rupees": _distribution(part["net_pnl_rupees"]),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "coverage": coverage_rows,
        "complete_case_pnl": pnl_rows,
        "interpretation_gate": {
            "minimum_coverage_for_diagnostic": 0.80,
            "eligible_cells": [
                row
                for row in coverage_rows
                if row["exact_contract_coverage"] >= 0.80
            ],
        },
        "limitations": [
            "Multi-day exits require the exact entry expiry, strike, option type, and same clock time.",
            "Complete-case P&L below 80% exact-contract coverage is reported only as a biased sensitivity and must not be treated as evidence of edge.",
            "The rolling nearest-expiry ATM +/-10 surface creates non-random attrition as contracts expire or leave the local chain.",
            "No stale last-quote substitution or synthetic repricing is used in this strict feasibility audit.",
            "All cells remain in-sample diagnostics rather than an out-of-sample strategy test.",
        ],
    }


def run(
    *,
    gold_root: Path,
    observations_path: Path,
    curve_path: Path,
    surface_path: Path,
    tradebook_path: Path,
    coverage_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    observations = pd.read_parquet(observations_path)
    requests = _build_requests(observations, curve_path=curve_path)
    frame = _load_exits(
        requests,
        gold_glob=str(gold_root / "year=*" / "month=*" / "part-*.parquet"),
        surface_path=surface_path,
    )
    tradebook = build_tradebook(frame)
    summary = summarize(frame, tradebook)
    coverage = pd.DataFrame(summary["coverage"])
    for path in (tradebook_path, coverage_path, summary_path, manifest_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    tradebook.to_csv(tradebook_path, index=False)
    coverage.to_csv(coverage_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = {
        "schema_version": "phase4-multiday-vrp-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(observations_path.resolve()), "sha256": _sha256(observations_path)},
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
            {"path": str(coverage_path.resolve()), "sha256": _sha256(coverage_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
        ],
        "gold_root": str(gold_root.resolve()),
        "request_rows": int(len(frame)),
        "complete_trade_rows": int(len(tradebook)),
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )
    return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", required=True, type=Path)
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase4_cost_aware_observations.parquet"),
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
        "--tradebook",
        type=Path,
        default=Path("audit/phase4_multiday_tradebook.csv"),
    )
    parser.add_argument(
        "--coverage",
        type=Path,
        default=Path("audit/phase4_multiday_coverage.csv"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase4_multiday_summary.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase4_multiday_manifest.json"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        gold_root=args.gold_root,
        observations_path=args.observations,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        tradebook_path=args.tradebook,
        coverage_path=args.coverage,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
