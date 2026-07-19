"""Compare defined-risk VRP structures with corrected costs and capacity impact."""

from __future__ import annotations

import argparse
from dataclasses import asdict
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
    ParticipationImpactParameters,
    estimate_defined_risk_margin,
    estimate_nifty_option_slippage,
    estimate_participation_impact,
    estimate_round_trip_execution_cost,
    groww_fno_rates_for_date,
)
from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase3.run_full_strategy_backtest import (
    _fill_price,
    _reference_ts_sql,
    _selected_slot_sql,
    _slot_case,
    _span_data,
    _theoretical_risk,
)
from research.phase3.run_tail_percentile_backtests import build_tail_events


SCHEMA_VERSION = "phase4-cost-aware-discovery/v1"
HORIZONS = (60, 120, 180)
CAPACITY_LOTS = tuple(range(1, 101))
CAPACITY_IMPACT_PARAMETERS = ParticipationImpactParameters(
    ladder_parity_lots=60.0,
    volume_participation_weight=1.0,
    oi_participation_weight=1.0,
    participation_exponent=0.5,
)
MINIMUM_SELL_FILL = 0.05
LEG_SPECS = {
    "p_m3": (-3, "PUT"),
    "p_m1": (-1, "PUT"),
    "p_0": (0, "PUT"),
    "p_p3": (3, "PUT"),
    "c_m3": (-3, "CALL"),
    "c_0": (0, "CALL"),
    "c_p1": (1, "CALL"),
    "c_p3": (3, "CALL"),
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


def build_discovery_events(curve_path: Path) -> pd.DataFrame:
    events = build_tail_events(curve_path, thresholds=(0.10, 0.85))
    events = events.loc[
        ((events["threshold"] == 0.85) & events["direction"].eq("up"))
        | ((events["threshold"] == 0.10) & events["direction"].eq("down"))
    ].copy()
    events["signal_family"] = np.where(events["threshold"].eq(0.85), "upper85_up", "lower10_down")
    return events


def _load_observations(
    events: pd.DataFrame,
    *,
    gold_glob: str,
    surface_path: Path,
) -> pd.DataFrame:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    connection.register("events", events)
    leg_values = ",\n".join(
        f"('{leg}', {offset}, '{option_type}')" for leg, (offset, option_type) in LEG_SPECS.items()
    )
    entries = connection.execute(
        f"""
        WITH requests AS (
          SELECT events.*, legs.*
          FROM events
          CROSS JOIN (VALUES {leg_values}) AS legs(leg, entry_offset, option_type)
        )
        SELECT
          requests.*,
          cast(source.strike AS DOUBLE) AS strike,
          cast(source.close AS DOUBLE) AS entry_close,
          cast(source.volume AS DOUBLE) AS entry_volume,
          cast(source.open_interest AS DOUBLE) AS entry_open_interest,
          cast(source.india_vix AS DOUBLE) AS entry_india_vix,
          cast(source.mte AS DOUBLE) AS entry_minutes_to_expiry,
          cast(source.dte AS DOUBLE) AS entry_dte,
          cast(source.contract_lot_size AS INTEGER) AS lot_size,
          cast(source.independent_nifty_spot AS DOUBLE) AS entry_spot,
          cast(surface.atm_iv AS DOUBLE) AS entry_atm_iv,
          cast(source.bsm_delta AS DOUBLE) AS entry_delta,
          cast(source.bsm_gamma AS DOUBLE) AS entry_gamma,
          cast(source.bsm_theta_per_day_365 AS DOUBLE) AS entry_theta_day,
          cast(source.bsm_vega_per_100 AS DOUBLE) AS entry_vega_vol_point,
          source.actual_expiry_date,
          {_slot_case('join_status')} AS span_join_status,
          {_selected_slot_sql()} AS span_time_slot,
          {_slot_case('date')} AS span_date,
          {_reference_ts_sql()} AS span_reference_ts_ist,
          ({_selected_slot_sql()}) = 'BOD' AS span_bod_open_time_assumption,
          source.span_slot_publication_times_proven,
          source.span_intraday_asof_join_performed,
          cast({_slot_case('price')} AS DOUBLE) AS span_price,
          cast({_slot_case('s1')} AS DOUBLE) AS span_s1,
          cast({_slot_case('s2')} AS DOUBLE) AS span_s2,
          cast({_slot_case('s3')} AS DOUBLE) AS span_s3,
          cast({_slot_case('s4')} AS DOUBLE) AS span_s4,
          cast({_slot_case('s5')} AS DOUBLE) AS span_s5,
          cast({_slot_case('s6')} AS DOUBLE) AS span_s6,
          cast({_slot_case('s7')} AS DOUBLE) AS span_s7,
          cast({_slot_case('s8')} AS DOUBLE) AS span_s8,
          cast({_slot_case('s9')} AS DOUBLE) AS span_s9,
          cast({_slot_case('s10')} AS DOUBLE) AS span_s10,
          cast({_slot_case('s11')} AS DOUBLE) AS span_s11,
          cast({_slot_case('s12')} AS DOUBLE) AS span_s12,
          cast({_slot_case('s13')} AS DOUBLE) AS span_s13,
          cast({_slot_case('s14')} AS DOUBLE) AS span_s14,
          cast({_slot_case('s15')} AS DOUBLE) AS span_s15,
          cast({_slot_case('s16')} AS DOUBLE) AS span_s16
        FROM requests
        LEFT JOIN read_parquet(?, hive_partitioning=true) source
          ON source.timestamp_ist = requests.entry_ts
          AND source.trade_date = requests.trade_date
          AND try_cast(source.computed_moneyness_offset AS INTEGER) = requests.entry_offset
          AND source.option_type = requests.option_type
        LEFT JOIN read_parquet(?) surface
          ON surface.timestamp_ist = requests.entry_ts
        WHERE source.expiry_flag = 'WEEK'
          AND source.close IS NOT NULL
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
        """,
        [gold_glob, str(surface_path)],
    ).fetchdf()
    connection.register("entries", entries)
    horizon_values = ",".join(f"({value})" for value in HORIZONS)
    exits = connection.execute(
        f"""
        WITH requests AS (
          SELECT entries.*, horizons.horizon_minutes,
            entries.entry_ts + horizons.horizon_minutes * INTERVAL '1 minute' AS horizon_exit_ts
          FROM entries
          CROSS JOIN (VALUES {horizon_values}) horizons(horizon_minutes)
        )
        SELECT
          requests.trade_id,
          requests.leg,
          requests.horizon_minutes,
          requests.horizon_exit_ts,
          cast(source.close AS DOUBLE) AS exit_close,
          cast(source.volume AS DOUBLE) AS exit_volume,
          cast(source.open_interest AS DOUBLE) AS exit_open_interest,
          cast(source.india_vix AS DOUBLE) AS exit_india_vix,
          cast(source.mte AS DOUBLE) AS exit_minutes_to_expiry,
          cast(source.independent_nifty_spot AS DOUBLE) AS exit_spot,
          cast(surface.atm_iv AS DOUBLE) AS exit_atm_iv
        FROM requests
        LEFT JOIN read_parquet(?, hive_partitioning=true) source
          ON source.timestamp_ist = requests.horizon_exit_ts
          AND source.trade_date = requests.trade_date
          AND cast(source.strike AS DOUBLE) = requests.strike
          AND source.option_type = requests.option_type
          AND source.actual_expiry_date = requests.actual_expiry_date
          AND source.expiry_flag = 'WEEK'
          AND source.close IS NOT NULL
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
        LEFT JOIN read_parquet(?) surface
          ON surface.timestamp_ist = requests.horizon_exit_ts
        """,
        [gold_glob, str(surface_path)],
    ).fetchdf()
    connection.close()
    observations = entries.merge(
        exits,
        on=["trade_id", "leg"],
        how="left",
        validate="one_to_many",
    )
    entry_slippage: list[float] = []
    exit_slippage: list[float] = []
    for row in observations.itertuples(index=False):
        entry_vix = (
            float(row.entry_india_vix)
            if pd.notna(row.entry_india_vix)
            else float(row.entry_atm_iv) * 100.0
        )
        entry_slippage.append(
            estimate_nifty_option_slippage(
                close=float(row.entry_close),
                volume=float(row.entry_volume),
                open_interest=float(row.entry_open_interest),
                minutes_to_expiry=float(row.entry_minutes_to_expiry),
                india_vix=entry_vix,
            ).slippage_per_unit
        )
        if pd.isna(row.exit_close):
            exit_slippage.append(float("nan"))
            continue
        exit_vix = (
            float(row.exit_india_vix)
            if pd.notna(row.exit_india_vix)
            else float(row.exit_atm_iv) * 100.0
        )
        exit_slippage.append(
            estimate_nifty_option_slippage(
                close=float(row.exit_close),
                volume=float(row.exit_volume),
                open_interest=float(row.exit_open_interest),
                minutes_to_expiry=float(row.exit_minutes_to_expiry),
                india_vix=exit_vix,
            ).slippage_per_unit
        )
    observations["entry_slippage_per_unit"] = entry_slippage
    observations["exit_slippage_per_unit"] = exit_slippage
    return observations


def _margin_for_structure(part: pd.DataFrame, weights: dict[str, int]) -> float:
    first = part.iloc[0]
    legs = []
    for row in part.itertuples(index=False):
        weight = int(weights[str(row.leg)])
        side = "BUY" if weight > 0 else "SELL"
        fill = _fill_price(
            float(row.entry_close),
            float(row.entry_slippage_per_unit),
            side,
            minimum_sell_fill=MINIMUM_SELL_FILL,
        )
        legs.append(
            {
                "side": side,
                "instrument": "OPT",
                "option_type": "CE" if row.option_type == "CALL" else "PE",
                "strike": float(row.strike),
                "lot_size": int(row.lot_size),
                "qty_ratio": abs(weight),
                "entry_price": fill,
                "expiry": pd.Timestamp(row.actual_expiry_date).date().isoformat(),
            }
        )
    margin = estimate_defined_risk_margin(
        legs=legs,
        span_data=_span_data(part),
        expiry=pd.Timestamp(first.actual_expiry_date).date().isoformat(),
        spot=float(first.entry_spot),
        eval_dt=pd.Timestamp(first.entry_ts).to_pydatetime(),
    )
    return float(margin.margin)


def _trade_record(
    part: pd.DataFrame,
    *,
    structure: str,
    weights: dict[str, int],
    margin: float,
) -> dict[str, Any]:
    first = part.iloc[0]
    lot_size = int(first.lot_size)
    entry_legs: list[ExecutedLeg] = []
    exit_legs: list[ExecutedLeg] = []
    gross_points = 0.0
    entry_value = 0.0
    greek_totals = {"delta": 0.0, "gamma": 0.0, "theta_day": 0.0, "vega": 0.0}
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
                slippage_per_unit=float(row.exit_slippage_per_unit),
            )
        )
        gross_points += weight * (float(row.exit_close) - float(row.entry_close))
        entry_value += weight * float(row.entry_close)
        greek_totals["delta"] += weight * float(row.entry_delta)
        greek_totals["gamma"] += weight * float(row.entry_gamma)
        greek_totals["theta_day"] += weight * float(row.entry_theta_day)
        greek_totals["vega"] += weight * float(row.entry_vega_vol_point)
    costs = estimate_round_trip_execution_cost(
        entry_legs=entry_legs,
        exit_legs=exit_legs,
        entry_rates=groww_fno_rates_for_date(pd.Timestamp(first.entry_ts).date()),
        exit_rates=groww_fno_rates_for_date(pd.Timestamp(first.horizon_exit_ts).date()),
    )
    max_loss, max_profit = _theoretical_risk(part, weights)
    gross_rupees = gross_points * lot_size
    net_rupees = gross_rupees - costs.total
    return {
        "trade_id": int(first.trade_id),
        "trade_date": str(first.trade_date),
        "signal_family": str(first.signal_family),
        "structure": structure,
        "horizon_minutes": int(first.horizon_minutes),
        "entry_ts": pd.Timestamp(first.entry_ts),
        "exit_ts": pd.Timestamp(first.horizon_exit_ts),
        "lot_size": lot_size,
        "entry_dte": float(first.entry_dte),
        "entry_value_points": entry_value,
        "entry_credit_points": max(-entry_value, 0.0),
        "entry_debit_points": max(entry_value, 0.0),
        "max_loss_points": max_loss,
        "max_profit_points": max_profit,
        "net_delta_per_unit": greek_totals["delta"],
        "net_gamma_per_unit": greek_totals["gamma"],
        "net_theta_points_per_day": greek_totals["theta_day"],
        "net_vega_points_per_vol_point": greek_totals["vega"],
        "gross_pnl_points": gross_points,
        "gross_pnl_rupees": gross_rupees,
        "total_cost_rupees": costs.total,
        "cost_hurdle_points": costs.total / lot_size,
        "net_pnl_rupees": net_rupees,
        "margin_rupees": margin,
        "net_return_on_margin": net_rupees / margin,
        "slippage_rupees": costs.total_slippage,
        "charges_rupees": costs.total_charges,
        "brokerage_rupees": costs.entry.charges.brokerage + costs.exit.charges.brokerage,
        "stt_rupees": costs.entry.charges.stt_ctt + costs.exit.charges.stt_ctt,
    }


def build_tradebook(observations: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    margin_cache: dict[tuple[int, str], float] = {}
    for (trade_id, horizon), full_part in observations.groupby(
        ["trade_id", "horizon_minutes"], sort=True
    ):
        for structure, raw_weights in STRUCTURES.items():
            weights = {leg: int(weight) for leg, weight in raw_weights.items()}
            part = full_part.loc[full_part["leg"].isin(weights)].copy()
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            key = (int(trade_id), structure)
            if key not in margin_cache:
                margin_cache[key] = _margin_for_structure(part, weights)
            records.append(
                _trade_record(
                    part,
                    structure=structure,
                    weights=weights,
                    margin=margin_cache[key],
                )
            )
    return pd.DataFrame(records).sort_values(
        ["signal_family", "horizon_minutes", "structure", "trade_date"]
    )


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return {"count": 0}
    tail_count = max(1, int(math.ceil(0.05 * len(clean))))
    return {
        "count": int(len(clean)),
        "sum": float(clean.sum()),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "win_rate": float((clean > 0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p95": float(clean.quantile(0.95)),
        "cvar05": float(clean.nsmallest(tail_count).mean()),
    }


def summarize(tradebook: pd.DataFrame, events: pd.DataFrame) -> dict[str, Any]:
    rows = []
    expected = events.groupby("signal_family")["trade_id"].nunique().to_dict()
    for (signal, horizon, structure), part in tradebook.groupby(
        ["signal_family", "horizon_minutes", "structure"], sort=True
    ):
        rows.append(
            {
                "signal_family": signal,
                "horizon_minutes": int(horizon),
                "structure": structure,
                "trades": int(len(part)),
                "path_coverage": float(len(part) / expected[signal]),
                "gross_points": _distribution(part["gross_pnl_points"]),
                "gross_rupees": _distribution(part["gross_pnl_rupees"]),
                "cost_hurdle_points": _distribution(part["cost_hurdle_points"]),
                "net_rupees": _distribution(part["net_pnl_rupees"]),
                "net_return_on_margin": _distribution(part["net_return_on_margin"]),
                "margin_rupees": _distribution(part["margin_rupees"]),
                "entry_credit_points": _distribution(part["entry_credit_points"]),
                "entry_debit_points": _distribution(part["entry_debit_points"]),
                "net_delta_per_unit": _distribution(part["net_delta_per_unit"]),
                "net_gamma_per_unit": _distribution(part["net_gamma_per_unit"]),
                "net_theta_points_per_day": _distribution(part["net_theta_points_per_day"]),
                "net_vega_points_per_vol_point": _distribution(
                    part["net_vega_points_per_vol_point"]
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "signals": {str(key): int(value) for key, value in expected.items()},
        "cells": rows,
        "limitations": [
            "All structure and horizon cells are post-selected in-sample diagnostics.",
            "The quantity impact overlay is anchored to ladder parity at 60 lots; volume/OI enter as separate square-root participation sensitivities. It is not used in one-lot cell P&L and remains assumption-driven pending fill calibration.",
            "Path coverage falls at longer horizons because fixed contracts leave the rolling surface or the session ends.",
            "Brokerage is a deployable-current assumption; option STT is date-regime correct.",
        ],
    }


def build_capacity_curve(observations: pd.DataFrame) -> pd.DataFrame:
    selected = observations.loc[
        observations["signal_family"].eq("upper85_up")
        & observations["horizon_minutes"].eq(60)
        & observations["leg"].isin(STRUCTURES["short_iron_condor"])
    ].copy()
    weights = {leg: int(weight) for leg, weight in STRUCTURES["short_iron_condor"].items()}
    rows = []
    for lots in CAPACITY_LOTS:
        for trade_id, part in selected.groupby("trade_id", sort=True):
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            first = part.iloc[0]
            lot_size = int(first.lot_size)
            gross_points = 0.0
            entry_legs = []
            exit_legs = []
            base_slippage_total = 0.0
            ladder_impact_total = 0.0
            volume_impact_total = 0.0
            oi_impact_total = 0.0
            impact_total = 0.0
            for row in part.itertuples(index=False):
                weight = weights[str(row.leg)]
                quantity = abs(weight) * lot_size * lots
                entry_side = "BUY" if weight > 0 else "SELL"
                exit_side = "SELL" if weight > 0 else "BUY"
                entry_impact = estimate_participation_impact(
                    base_slippage_per_unit=float(row.entry_slippage_per_unit),
                    quantity=quantity,
                    lot_size=lot_size,
                    volume=float(row.entry_volume),
                    open_interest=float(row.entry_open_interest),
                    parameters=CAPACITY_IMPACT_PARAMETERS,
                )
                exit_impact = estimate_participation_impact(
                    base_slippage_per_unit=float(row.exit_slippage_per_unit),
                    quantity=quantity,
                    lot_size=lot_size,
                    volume=float(row.exit_volume),
                    open_interest=float(row.exit_open_interest),
                    parameters=CAPACITY_IMPACT_PARAMETERS,
                )
                entry_fill = _fill_price(
                    float(row.entry_close),
                    entry_impact.adjusted_slippage_per_unit,
                    entry_side,
                    minimum_sell_fill=MINIMUM_SELL_FILL,
                )
                exit_fill = _fill_price(
                    float(row.exit_close),
                    exit_impact.adjusted_slippage_per_unit,
                    exit_side,
                    minimum_sell_fill=MINIMUM_SELL_FILL,
                )
                entry_legs.append(
                    ExecutedLeg(
                        entry_side,
                        "OPT",
                        entry_fill,
                        quantity,
                        entry_impact.adjusted_slippage_per_unit,
                    )
                )
                exit_legs.append(
                    ExecutedLeg(
                        exit_side,
                        "OPT",
                        exit_fill,
                        quantity,
                        exit_impact.adjusted_slippage_per_unit,
                    )
                )
                base_slippage_total += quantity * (
                    entry_impact.base_slippage_per_unit + exit_impact.base_slippage_per_unit
                )
                ladder_impact_total += quantity * (
                    entry_impact.ladder_impact_per_unit + exit_impact.ladder_impact_per_unit
                )
                volume_impact_total += quantity * (
                    entry_impact.volume_impact_per_unit + exit_impact.volume_impact_per_unit
                )
                oi_impact_total += quantity * (
                    entry_impact.oi_impact_per_unit + exit_impact.oi_impact_per_unit
                )
                impact_total += quantity * (
                    entry_impact.impact_per_unit + exit_impact.impact_per_unit
                )
                gross_points += weight * (float(row.exit_close) - float(row.entry_close))
            costs = estimate_round_trip_execution_cost(
                entry_legs=entry_legs,
                exit_legs=exit_legs,
                entry_rates=groww_fno_rates_for_date(pd.Timestamp(first.entry_ts).date()),
                exit_rates=groww_fno_rates_for_date(pd.Timestamp(first.horizon_exit_ts).date()),
            )
            gross = gross_points * lot_size * lots
            rows.append(
                {
                    "trade_id": int(trade_id),
                    "lots": lots,
                    "gross_pnl_rupees": gross,
                    "base_slippage_rupees": base_slippage_total,
                    "ladder_impact_rupees": ladder_impact_total,
                    "volume_impact_rupees": volume_impact_total,
                    "oi_impact_rupees": oi_impact_total,
                    "impact_cost_rupees": impact_total,
                    "total_cost_rupees": costs.total,
                    "net_pnl_rupees": gross - costs.total,
                }
            )
    frame = pd.DataFrame(rows)
    return (
        frame.groupby("lots", as_index=False)
        .agg(
            trades=("trade_id", "size"),
            mean_gross=("gross_pnl_rupees", "mean"),
            mean_base_slippage=("base_slippage_rupees", "mean"),
            mean_ladder_impact=("ladder_impact_rupees", "mean"),
            mean_volume_impact=("volume_impact_rupees", "mean"),
            mean_oi_impact=("oi_impact_rupees", "mean"),
            mean_impact=("impact_cost_rupees", "mean"),
            mean_cost=("total_cost_rupees", "mean"),
            mean_net=("net_pnl_rupees", "mean"),
            net_win_rate=("net_pnl_rupees", lambda values: float((values > 0).mean())),
        )
        .assign(
            mean_participation_impact=lambda frame: (
                frame["mean_volume_impact"] + frame["mean_oi_impact"]
            ),
            impact_to_base_ratio=lambda frame: frame["mean_impact"] / frame["mean_base_slippage"],
            ladder_share_of_impact=lambda frame: frame["mean_ladder_impact"]
            / frame["mean_impact"].replace(0.0, np.nan),
        )
    )


def run(
    *,
    gold_root: Path,
    curve_path: Path,
    surface_path: Path,
    tradebook_path: Path,
    observation_path: Path,
    capacity_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    events = build_discovery_events(curve_path)
    observations = _load_observations(
        events,
        gold_glob=str(gold_root / "year=*" / "month=*" / "part-*.parquet"),
        surface_path=surface_path,
    )
    tradebook = build_tradebook(observations)
    capacity = build_capacity_curve(observations)
    summary = summarize(tradebook, events)
    for path in (
        tradebook_path,
        observation_path,
        capacity_path,
        summary_path,
        manifest_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    tradebook.to_csv(tradebook_path, index=False)
    observations.to_parquet(observation_path, index=False, compression="zstd")
    capacity.to_csv(capacity_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    impact_source = Path(__file__).resolve().parents[2] / "src" / "nifty_execution" / "slippage.py"
    manifest = {
        "schema_version": "phase4-cost-aware-manifest/v2",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "impact_model": {
            "name": "additive_ladder_participation_v2",
            "parameters": asdict(CAPACITY_IMPACT_PARAMETERS),
            "capacity_lots": [min(CAPACITY_LOTS), max(CAPACITY_LOTS)],
            "source": {"path": str(impact_source), "sha256": _sha256(impact_source)},
        },
        "inputs": [
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
            {"path": str(observation_path.resolve()), "sha256": _sha256(observation_path)},
            {"path": str(capacity_path.resolve()), "sha256": _sha256(capacity_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
        ],
        "gold_root": str(gold_root.resolve()),
        "trade_rows": int(len(tradebook)),
        "observation_rows": int(len(observations)),
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
        "--tradebook",
        type=Path,
        default=Path("audit/phase4_cost_aware_tradebook.csv"),
    )
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase4_cost_aware_observations.parquet"),
    )
    parser.add_argument(
        "--capacity",
        type=Path,
        default=Path("audit/phase4_capacity_curve.csv"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase4_cost_aware_summary.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase4_cost_aware_manifest.json"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        gold_root=args.gold_root,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        tradebook_path=args.tradebook,
        observation_path=args.observations,
        capacity_path=args.capacity,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
