"""Test causal VRP tail reversals with requested and inverse iron condors."""

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
from research.phase4.run_cost_aware_discovery import (
    _load_observations,
    _margin_for_structure,
    _trade_record,
)


SCHEMA_VERSION = "phase6-vrp-reversal/v1"
PROTOCOL_PATH = Path("docs/research/PHASE6_VRP_REVERSAL_PROTOCOL.md")
HORIZONS = (60, 120, 180)
TAIL_HIGH = 0.90
TAIL_LOW = 0.10
REVERSAL_DISTANCE = 0.10
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


def build_reversal_events(curve: pd.DataFrame) -> pd.DataFrame:
    frame = curve.sort_values(["trade_date", "entry_ts"]).copy()
    frame["entry_ts"] = pd.to_datetime(frame["entry_ts"], utc=True)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    grouped = frame.groupby("trade_date", sort=False)["vrp_q5"]
    frame["prior_running_max_q5"] = grouped.transform(
        lambda values: values.cummax().shift(1)
    )
    frame["prior_running_min_q5"] = grouped.transform(
        lambda values: values.cummin().shift(1)
    )
    top = (
        frame["prior_running_max_q5"].ge(TAIL_HIGH)
        & frame["vrp_q5"].le(frame["prior_running_max_q5"] - REVERSAL_DISTANCE)
        & frame["q_velocity_5m"].lt(0)
        & frame["signal_vrp_var_act365"].gt(0)
    )
    bottom = (
        frame["prior_running_min_q5"].le(TAIL_LOW)
        & frame["vrp_q5"].ge(frame["prior_running_min_q5"] + REVERSAL_DISTANCE)
        & frame["q_velocity_5m"].gt(0)
        & frame["signal_vrp_var_act365"].lt(0)
    )
    events = frame.loc[top | bottom].copy()
    events["reversal_type"] = np.where(top.loc[events.index], "top_to_zero", "bottom_to_zero")
    events = events.sort_values(["trade_date", "entry_ts"]).drop_duplicates(
        "trade_date", keep="first"
    )
    events = events.rename(columns={"entry_ts": "signal_ts"})
    events["entry_ts"] = events["signal_ts"] + pd.Timedelta(minutes=1)
    events["signal_family"] = events["reversal_type"]
    events["trade_id"] = np.arange(1, len(events) + 1, dtype=np.int64)
    return events[
        [
            "trade_id",
            "trade_date",
            "signal_ts",
            "entry_ts",
            "signal_family",
            "reversal_type",
            "signal_vrp_var_act365",
            "vrp_q5",
            "q_velocity_5m",
            "prior_running_max_q5",
            "prior_running_min_q5",
        ]
    ].reset_index(drop=True)


def build_zero_diagnostics(events: pd.DataFrame, curve: pd.DataFrame) -> pd.DataFrame:
    panel = curve.copy()
    panel["entry_ts"] = pd.to_datetime(panel["entry_ts"], utc=True)
    panel["trade_date"] = pd.to_datetime(panel["trade_date"]).dt.date
    by_date = {date: part.set_index("entry_ts") for date, part in panel.groupby("trade_date")}
    rows = []
    for event in events.itertuples(index=False):
        session = by_date[event.trade_date]
        for horizon in HORIZONS:
            exit_ts = pd.Timestamp(event.entry_ts) + pd.Timedelta(minutes=horizon)
            window = session.loc[
                (session.index >= pd.Timestamp(event.entry_ts)) & (session.index <= exit_ts)
            ]
            entry_vrp = (
                float(session.loc[pd.Timestamp(event.entry_ts), "signal_vrp_var_act365"])
                if pd.Timestamp(event.entry_ts) in session.index
                else float("nan")
            )
            exit_vrp = (
                float(session.loc[exit_ts, "signal_vrp_var_act365"])
                if exit_ts in session.index
                else float("nan")
            )
            if event.reversal_type == "top_to_zero":
                touches = window["signal_vrp_var_act365"].le(0)
            else:
                touches = window["signal_vrp_var_act365"].ge(0)
            touched = bool(touches.any())
            minutes_to_zero = float("nan")
            if touched:
                first_touch = window.index[np.flatnonzero(touches.to_numpy())[0]]
                minutes_to_zero = (first_touch - pd.Timestamp(event.entry_ts)).total_seconds() / 60
            rows.append(
                {
                    "trade_id": int(event.trade_id),
                    "trade_date": str(event.trade_date),
                    "reversal_type": event.reversal_type,
                    "horizon_minutes": horizon,
                    "entry_vrp": entry_vrp,
                    "exit_vrp": exit_vrp,
                    "zero_touched": touched,
                    "minutes_to_zero": minutes_to_zero,
                    "distance_to_zero_reduced": bool(abs(exit_vrp) < abs(entry_vrp))
                    if np.isfinite(entry_vrp) and np.isfinite(exit_vrp)
                    else None,
                }
            )
    return pd.DataFrame(rows)


def build_tradebook(observations: pd.DataFrame) -> pd.DataFrame:
    rows = []
    margin_cache: dict[tuple[int, str], float] = {}
    structures = ("short_iron_condor", "long_iron_condor")
    for (trade_id, horizon), full_part in observations.groupby(
        ["trade_id", "horizon_minutes"], sort=True
    ):
        reversal_type = str(full_part.iloc[0].reversal_type)
        requested = "long_iron_condor" if reversal_type == "top_to_zero" else "short_iron_condor"
        for structure in structures:
            weights = {leg: int(value) for leg, value in STRUCTURES[structure].items()}
            part = full_part.loc[full_part["leg"].isin(weights)].copy()
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            key = (int(trade_id), structure)
            if key not in margin_cache:
                margin_cache[key] = _margin_for_structure(part, weights)
            record = _trade_record(
                part,
                structure=structure,
                weights=weights,
                margin=margin_cache[key],
            )
            record["reversal_type"] = reversal_type
            record["mapping"] = "requested" if structure == requested else "inverse"
            rows.append(record)
    return pd.DataFrame(rows).sort_values(
        ["mapping", "horizon_minutes", "trade_date", "structure"]
    )


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


def summarize(
    events: pd.DataFrame,
    zero_diagnostics: pd.DataFrame,
    tradebook: pd.DataFrame,
) -> dict[str, Any]:
    expected = int(len(events))
    cells = []
    for (mapping, horizon), part in tradebook.groupby(["mapping", "horizon_minutes"], sort=True):
        cells.append(
            {
                "mapping": str(mapping),
                "horizon_minutes": int(horizon),
                "trades": int(len(part)),
                "coverage": float(len(part) / expected),
                "gross_pnl_points": _distribution(part["gross_pnl_points"]),
                "gross_pnl_rupees": _distribution(part["gross_pnl_rupees"]),
                "cost_rupees": _distribution(part["total_cost_rupees"]),
                "net_pnl_rupees": _distribution(part["net_pnl_rupees"]),
                "net_return_on_margin": _distribution(part["net_return_on_margin"]),
                "bootstrap_mean_net_95pct_ci": _bootstrap_mean_ci(part),
            }
        )
    subgroups = []
    for (mapping, horizon, reversal), part in tradebook.groupby(
        ["mapping", "horizon_minutes", "reversal_type"], sort=True
    ):
        subgroups.append(
            {
                "mapping": str(mapping),
                "horizon_minutes": int(horizon),
                "reversal_type": str(reversal),
                "trades": int(len(part)),
                "net_pnl_rupees": _distribution(part["net_pnl_rupees"]),
            }
        )
    zero_rows = []
    for (reversal, horizon), part in zero_diagnostics.groupby(
        ["reversal_type", "horizon_minutes"], sort=True
    ):
        zero_rows.append(
            {
                "reversal_type": str(reversal),
                "horizon_minutes": int(horizon),
                "events": int(len(part)),
                "zero_touch_rate": float(part["zero_touched"].mean()),
                "distance_reduced_rate": float(
                    part["distance_to_zero_reduced"].dropna().mean()
                ),
                "median_minutes_to_zero_when_touched": float(
                    part.loc[part["zero_touched"], "minutes_to_zero"].median()
                ),
            }
        )
    primary = tradebook.loc[
        tradebook["mapping"].eq("requested") & tradebook["horizon_minutes"].eq(60)
    ].copy()
    local_ts = pd.to_datetime(primary["entry_ts"], utc=True).dt.tz_convert("Asia/Kolkata")
    primary["month"] = local_ts.dt.tz_localize(None).dt.to_period("M").astype(str)
    monthly = primary.groupby("month")["net_pnl_rupees"].sum()
    subgroup_means = primary.groupby("reversal_type")["net_pnl_rupees"].mean()
    primary_cell = next(
        cell
        for cell in cells
        if cell["mapping"] == "requested" and cell["horizon_minutes"] == 60
    )
    gates = {
        "minimum_100_trades": bool(len(primary) >= 100),
        "positive_mean_net": bool(primary["net_pnl_rupees"].mean() > 0),
        "positive_aggregate_net": bool(primary["net_pnl_rupees"].sum() > 0),
        "bootstrap_lower_above_zero": bool(primary_cell["bootstrap_mean_net_95pct_ci"][0] > 0),
        "both_reversal_subgroups_positive": bool(
            len(subgroup_means) == 2 and subgroup_means.gt(0).all()
        ),
        "coverage_at_least_80pct": bool(primary_cell["coverage"] >= 0.80),
        "positive_month_fraction_at_least_60pct": bool(monthly.gt(0).mean() >= 0.60),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "protocol": str(PROTOCOL_PATH),
        "events": {
            "total": expected,
            "by_type": {
                str(key): int(value)
                for key, value in events["reversal_type"].value_counts().to_dict().items()
            },
            "date_min": str(events["trade_date"].min()),
            "date_max": str(events["trade_date"].max()),
        },
        "zero_reversion": zero_rows,
        "cells": cells,
        "subgroups": subgroups,
        "primary_acceptance": {
            "passed": bool(all(gates.values())),
            "gates": gates,
            "positive_month_fraction": float(monthly.gt(0).mean()),
        },
        "decision": "PASS" if all(gates.values()) else "FAIL_KEEP_PHASE5_CLOSURE",
        "limitations": [
            "This is an explicitly post-hoc test after the original hypothesis family was closed.",
            "The inverse and 120/180-minute cells are diagnostics, not replacement primaries.",
            "Rolling-surface attrition remains horizon dependent.",
        ],
    }


def run(
    *,
    gold_root: Path,
    curve_path: Path,
    surface_path: Path,
    events_path: Path,
    observations_path: Path,
    zero_path: Path,
    tradebook_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    curve = pd.read_parquet(curve_path)
    events = build_reversal_events(curve)
    zero_diagnostics = build_zero_diagnostics(events, curve)
    observations = _load_observations(
        events,
        gold_glob=str(gold_root / "year=*" / "month=*" / "part-*.parquet"),
        surface_path=surface_path,
    )
    tradebook = build_tradebook(observations)
    summary = summarize(events, zero_diagnostics, tradebook)
    for path in (
        events_path,
        observations_path,
        zero_path,
        tradebook_path,
        summary_path,
        manifest_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(events_path, index=False)
    observations.to_parquet(observations_path, index=False, compression="zstd")
    zero_diagnostics.to_csv(zero_path, index=False)
    tradebook.to_csv(tradebook_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    manifest = {
        "schema_version": "phase6-vrp-reversal-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(PROTOCOL_PATH.resolve()), "sha256": _sha256(PROTOCOL_PATH)},
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(events_path.resolve()), "sha256": _sha256(events_path)},
            {"path": str(observations_path.resolve()), "sha256": _sha256(observations_path)},
            {"path": str(zero_path.resolve()), "sha256": _sha256(zero_path)},
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
        "--curve-path",
        type=Path,
        default=Path("audit/phase2_vrp_session_curve_features.parquet"),
    )
    parser.add_argument(
        "--surface-path",
        type=Path,
        default=Path("audit/phase2_intraday_iv_surface.parquet"),
    )
    parser.add_argument("--events", type=Path, default=Path("audit/phase6_reversal_events.csv"))
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase6_reversal_observations.parquet"),
    )
    parser.add_argument(
        "--zero-diagnostics",
        type=Path,
        default=Path("audit/phase6_reversal_zero_diagnostics.csv"),
    )
    parser.add_argument(
        "--tradebook", type=Path, default=Path("audit/phase6_reversal_tradebook.csv")
    )
    parser.add_argument(
        "--summary", type=Path, default=Path("audit/phase6_reversal_summary.json")
    )
    parser.add_argument(
        "--manifest", type=Path, default=Path("audit/phase6_reversal_manifest.json")
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        gold_root=args.gold_root,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        events_path=args.events,
        observations_path=args.observations,
        zero_path=args.zero_diagnostics,
        tradebook_path=args.tradebook,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
