"""Run the frozen VRP-crossing iron-condor strategy after costs and margin.

The script reuses the Phase-2 first-daily zero-crossing construction, freezes
the four entry contracts, retrieves their exact 60-minute exit observations,
and applies the pinned checkpoint-3 Groww cost, NIFTY slippage, and SPAN margin
engines. Outputs are deterministic trade- and leg-level audit artifacts.
"""

from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

import duckdb
import numpy as np
import pandas as pd

from nifty_execution import (
    ExecutedLeg,
    estimate_defined_risk_margin,
    estimate_nifty_option_slippage,
    estimate_round_trip_execution_cost,
)
from nifty_span.span import SpanContract, SpanData
from research.phase2.close_hypothesis_formulation import build_first_daily_crossings


SCHEMA_VERSION = "phase3-full-strategy-backtest/v1"
HOLDING_MINUTES = 60
BOOTSTRAP_SEED = 20260718
SLIPPAGE_STRESS_MULTIPLIER = 1.5
LEG_DEFINITIONS = (
    ("p_m3", -3, "PUT", 1),
    ("p_m1", -1, "PUT", -1),
    ("c_p1", 1, "CALL", -1),
    ("c_p3", 3, "CALL", 1),
)
CHARGE_FIELDS = (
    "brokerage",
    "stt_ctt",
    "stamp_duty",
    "exchange_transaction",
    "sebi_turnover",
    "ipft",
    "gst",
    "physical_delivery_brokerage",
    "margin_api_turnover_reserve",
    "broker_reported",
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
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _primary_events(structure_path: Path) -> pd.DataFrame:
    events = build_first_daily_crossings(structure_path)
    events = events.loc[events["vrp_crossing"].eq("cross_up")].copy()
    events = events.sort_values(["trade_date", "next_entry_ts"]).reset_index(drop=True)
    events["trade_id"] = np.arange(1, len(events) + 1, dtype=np.int64)
    events["signal_ts"] = pd.to_datetime(events["entry_ts"], utc=True)
    events["entry_ts"] = pd.to_datetime(events["next_entry_ts"], utc=True)
    events["exit_ts"] = events["entry_ts"] + pd.Timedelta(minutes=HOLDING_MINUTES)
    return events[
        [
            "trade_id",
            "trade_date",
            "signal_ts",
            "entry_ts",
            "exit_ts",
            "entry_time",
            "next_entry_time",
            "signal_vrp_var_act365",
            "vrp_tod_percentile",
            "next_short_iron_condor__pnl_points",
            "next_short_iron_condor__return_on_max_loss",
            "next_long_iron_condor__pnl_points",
            "next_long_iron_condor__return_on_max_loss",
        ]
    ].rename(columns={"next_entry_time": "execution_entry_time"})


def _slot_case(field: str) -> str:
    return f"""
        CASE
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '14:00:00'
            AND source.span_id3_join_status = 'matched'
            THEN source.span_id3_{field}
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '12:30:00'
            AND source.span_id2_join_status = 'matched'
            THEN source.span_id2_{field}
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '11:00:00'
            AND source.span_id1_join_status = 'matched'
            THEN source.span_id1_{field}
          ELSE source.span_bod_{field}
        END
    """


def _selected_slot_sql() -> str:
    return """
        CASE
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '14:00:00'
            AND source.span_id3_join_status = 'matched' THEN 'ID3'
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '12:30:00'
            AND source.span_id2_join_status = 'matched' THEN 'ID2'
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '11:00:00'
            AND source.span_id1_join_status = 'matched' THEN 'ID1'
          ELSE 'BOD'
        END
    """


def _reference_ts_sql() -> str:
    return """
        CASE
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '14:00:00'
            AND source.span_id3_join_status = 'matched'
            THEN source.span_id3_reference_ts_ist
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '12:30:00'
            AND source.span_id2_join_status = 'matched'
            THEN source.span_id2_reference_ts_ist
          WHEN strftime(source.timestamp_ist, '%H:%M:%S') >= '11:00:00'
            AND source.span_id1_join_status = 'matched'
            THEN source.span_id1_reference_ts_ist
          ELSE date_trunc('day', source.timestamp_ist) + INTERVAL '9 hours 15 minutes'
        END
    """


def _load_legbook(events: pd.DataFrame, gold_glob: str, surface_path: Path) -> pd.DataFrame:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    connection.execute("SET TimeZone='Asia/Kolkata'")
    connection.register("events", events)
    entry_query = f"""
        WITH requests AS (
          SELECT events.*, legs.*
          FROM events
          CROSS JOIN (
            VALUES
              ('p_m3', -3, 'PUT', 1),
              ('p_m1', -1, 'PUT', -1),
              ('c_p1', 1, 'CALL', -1),
              ('c_p3', 3, 'CALL', 1)
          ) AS legs(leg, entry_offset, option_type, short_weight)
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
          source.actual_expiry_date,
          {_slot_case('join_status')} AS span_join_status,
          source.span_release_status AS span_enrichment_status,
          {_selected_slot_sql()} AS span_time_slot,
          {_slot_case('date')} AS span_date,
          {_slot_case('source_status')} AS span_source_status,
          {_slot_case('timing_source')} AS span_timing_source,
          {_slot_case('timing_confidence')} AS span_timing_confidence,
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
        LEFT JOIN read_parquet(?, hive_partitioning=true) AS source
          ON source.timestamp_ist = requests.entry_ts
          AND source.trade_date = requests.trade_date
          AND try_cast(source.computed_moneyness_offset AS INTEGER) = requests.entry_offset
          AND source.option_type = requests.option_type
        LEFT JOIN read_parquet(?) AS surface
          ON surface.timestamp_ist = requests.entry_ts
        WHERE
          source.expiry_flag = 'WEEK'
          AND source.close IS NOT NULL
          AND source.close >= 0
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
        ORDER BY requests.trade_id, requests.entry_offset, requests.option_type
    """
    entries = connection.execute(entry_query, [gold_glob, str(surface_path)]).fetchdf()
    connection.register("entries", entries)
    exit_query = """
        SELECT
          entries.trade_id,
          entries.leg,
          cast(source.close AS DOUBLE) AS exit_close,
          cast(source.volume AS DOUBLE) AS exit_volume,
          cast(source.open_interest AS DOUBLE) AS exit_open_interest,
          cast(source.india_vix AS DOUBLE) AS exit_india_vix,
          cast(source.mte AS DOUBLE) AS exit_minutes_to_expiry,
          cast(source.dte AS DOUBLE) AS exit_dte,
          cast(source.independent_nifty_spot AS DOUBLE) AS exit_spot,
          cast(surface.atm_iv AS DOUBLE) AS exit_atm_iv,
          try_cast(source.computed_moneyness_offset AS INTEGER) AS exit_observed_offset
        FROM entries
        LEFT JOIN read_parquet(?, hive_partitioning=true) AS source
          ON source.timestamp_ist = entries.exit_ts
          AND source.trade_date = entries.trade_date
          AND cast(source.strike AS DOUBLE) = entries.strike
          AND source.option_type = entries.option_type
          AND source.actual_expiry_date = entries.actual_expiry_date
        LEFT JOIN read_parquet(?) AS surface
          ON surface.timestamp_ist = entries.exit_ts
        WHERE
          source.expiry_flag = 'WEEK'
          AND source.close IS NOT NULL
          AND source.close >= 0
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
        ORDER BY entries.trade_id, entries.entry_offset, entries.option_type
    """
    exits = connection.execute(exit_query, [gold_glob, str(surface_path)]).fetchdf()
    connection.close()
    return entries.merge(exits, on=["trade_id", "leg"], how="left", validate="one_to_one")


def _validate_legbook(frame: pd.DataFrame, expected_trades: int) -> dict[str, Any]:
    required = [
        "entry_close",
        "exit_close",
        "entry_volume",
        "exit_volume",
        "entry_open_interest",
        "exit_open_interest",
        "lot_size",
        "strike",
        "entry_atm_iv",
        "exit_atm_iv",
        "span_reference_ts_ist",
    ] + [f"span_s{index}" for index in range(1, 17)]
    duplicate_keys = int(frame.duplicated(["trade_id", "leg"]).sum())
    per_trade = frame.groupby("trade_id").size()
    missing = {column: int(frame[column].isna().sum()) for column in required}
    reference_ts = pd.to_datetime(frame["span_reference_ts_ist"], utc=True)
    entry_ts = pd.to_datetime(frame["entry_ts"], utc=True)
    report = {
        "expected_trades": int(expected_trades),
        "observed_trades": int(frame["trade_id"].nunique()),
        "expected_leg_rows": int(expected_trades * 4),
        "observed_leg_rows": int(len(frame)),
        "duplicate_trade_leg_keys": duplicate_keys,
        "trades_with_exactly_four_legs": int(per_trade.eq(4).sum()),
        "missing_required_fields": missing,
        "span_reference_after_entry_leg_rows": int((reference_ts > entry_ts).sum()),
        "span_selected_join_not_matched_leg_rows": int(
            (~frame["span_join_status"].eq("matched")).sum()
        ),
    }
    if (
        report["observed_trades"] != expected_trades
        or report["observed_leg_rows"] != expected_trades * 4
        or duplicate_keys
        or report["trades_with_exactly_four_legs"] != expected_trades
        or any(missing.values())
        or report["span_reference_after_entry_leg_rows"]
        or report["span_selected_join_not_matched_leg_rows"]
    ):
        raise ValueError(f"legbook completeness failure: {report}")
    return report


def _add_slippage(frame: pd.DataFrame) -> pd.DataFrame:
    output = frame.copy()
    entry_rows: list[dict[str, float | bool]] = []
    exit_rows: list[dict[str, float | bool]] = []
    for row in output.itertuples(index=False):
        entry_vix = (
            float(row.entry_india_vix)
            if pd.notna(row.entry_india_vix)
            else float(row.entry_atm_iv) * 100.0
        )
        exit_vix = (
            float(row.exit_india_vix)
            if pd.notna(row.exit_india_vix)
            else float(row.exit_atm_iv) * 100.0
        )
        entry_rows.append(
            estimate_nifty_option_slippage(
                close=float(row.entry_close),
                volume=float(row.entry_volume),
                open_interest=float(row.entry_open_interest),
                minutes_to_expiry=float(row.entry_minutes_to_expiry),
                india_vix=entry_vix,
            ).to_dict()
        )
        exit_rows.append(
            estimate_nifty_option_slippage(
                close=float(row.exit_close),
                volume=float(row.exit_volume),
                open_interest=float(row.exit_open_interest),
                minutes_to_expiry=float(row.exit_minutes_to_expiry),
                india_vix=exit_vix,
            ).to_dict()
        )
    for prefix, records in (("entry", entry_rows), ("exit", exit_rows)):
        details = pd.DataFrame(records, index=output.index)
        for column in (
            "turnover_ratio",
            "vix_multiplier",
            "time_multiplier",
            "stale_multiplier",
            "depth_multiplier",
            "base_spread",
            "time_penalty",
            "stale_penalty",
            "depth_penalty",
            "slippage_per_unit",
            "is_executable_proxy",
        ):
            output[f"{prefix}_{column}"] = details[column]
    output["entry_vix_fallback"] = output["entry_india_vix"].isna()
    output["exit_vix_fallback"] = output["exit_india_vix"].isna()
    output["entry_volatility_proxy"] = output["entry_india_vix"].fillna(
        output["entry_atm_iv"] * 100.0
    )
    output["exit_volatility_proxy"] = output["exit_india_vix"].fillna(
        output["exit_atm_iv"] * 100.0
    )
    output["entry_volatility_source"] = np.where(
        output["entry_vix_fallback"], "reconstructed_atm_iv", "india_vix"
    )
    output["exit_volatility_source"] = np.where(
        output["exit_vix_fallback"], "reconstructed_atm_iv", "india_vix"
    )
    output["primary_entry_side"] = np.where(output["short_weight"] > 0, "BUY", "SELL")
    output["primary_exit_side"] = np.where(output["short_weight"] > 0, "SELL", "BUY")
    output["inverse_entry_side"] = output["primary_exit_side"]
    output["inverse_exit_side"] = output["primary_entry_side"]
    return output


def _span_data(part: pd.DataFrame) -> SpanData:
    contracts: dict[tuple[str, str, date, float], SpanContract] = {}
    for row in part.itertuples(index=False):
        option_type = "CE" if row.option_type == "CALL" else "PE"
        expiry = pd.Timestamp(row.actual_expiry_date).date()
        risk_array = tuple(float(getattr(row, f"span_s{index}")) for index in range(1, 17))
        contracts[("NIFTY", option_type, expiry, float(row.strike))] = SpanContract(
            risk_array=risk_array,
            price=float(row.span_price),
        )
    span_dates = pd.to_datetime(part["span_date"]).dt.date.unique()
    slots = part["span_time_slot"].dropna().astype(str).unique()
    if len(span_dates) != 1 or len(slots) != 1:
        raise ValueError("one date and SPAN slot are required per trade")
    return SpanData(
        contracts,
        selected_time_slot=str(slots[0]),
        trading_date=span_dates[0],
    )


def _fill_price(
    close: float,
    slippage: float,
    side: str,
    *,
    minimum_sell_fill: float | None = None,
) -> float:
    price = close + slippage if side == "BUY" else close - slippage
    if side == "SELL" and minimum_sell_fill is not None:
        price = max(price, float(minimum_sell_fill))
    if price < 0.0:
        raise ValueError("adverse slippage produces a negative sell fill")
    return price


def _theoretical_risk(part: pd.DataFrame, weights: dict[str, int]) -> tuple[float, float]:
    entry_value = sum(
        weights[str(row.leg)] * float(row.entry_close) for row in part.itertuples(index=False)
    )
    strikes = [float(value) for value in part["strike"]]
    candidate_spots = [0.0, *strikes, max(strikes) * 2.0]
    pnl_values = []
    for terminal_spot in candidate_spots:
        payoff = 0.0
        for row in part.itertuples(index=False):
            intrinsic = (
                max(terminal_spot - float(row.strike), 0.0)
                if row.option_type == "CALL"
                else max(float(row.strike) - terminal_spot, 0.0)
            )
            payoff += weights[str(row.leg)] * intrinsic
        pnl_values.append(payoff - entry_value)
    return -min(pnl_values), max(pnl_values)


def _trade_row(
    part: pd.DataFrame,
    *,
    strategy: str,
    weight_sign: int,
    minimum_sell_fill: float | None = None,
) -> dict[str, Any]:
    first = part.iloc[0]
    weights = {str(row.leg): int(row.short_weight) * weight_sign for row in part.itertuples()}
    gross_points = sum(
        weights[str(row.leg)] * (float(row.exit_close) - float(row.entry_close))
        for row in part.itertuples(index=False)
    )
    lot_size = int(first["lot_size"])
    base: dict[str, Any] = {}
    stress: dict[str, Any] = {}
    for label, multiplier, sink in (
        ("base", 1.0, base),
        ("stress_1_5x", SLIPPAGE_STRESS_MULTIPLIER, stress),
    ):
        entry_legs: list[ExecutedLeg] = []
        exit_legs: list[ExecutedLeg] = []
        margin_legs: list[dict[str, Any]] = []
        for row in part.itertuples(index=False):
            weight = weights[str(row.leg)]
            entry_side = "BUY" if weight > 0 else "SELL"
            exit_side = "SELL" if weight > 0 else "BUY"
            entry_slippage = float(row.entry_slippage_per_unit) * multiplier
            exit_slippage = float(row.exit_slippage_per_unit) * multiplier
            entry_fill = _fill_price(
                float(row.entry_close),
                entry_slippage,
                entry_side,
                minimum_sell_fill=minimum_sell_fill,
            )
            exit_fill = _fill_price(
                float(row.exit_close),
                exit_slippage,
                exit_side,
                minimum_sell_fill=minimum_sell_fill,
            )
            entry_legs.append(
                ExecutedLeg(
                    entry_side,
                    "OPT",
                    entry_fill,
                    lot_size,
                    slippage_per_unit=entry_slippage,
                )
            )
            exit_legs.append(
                ExecutedLeg(
                    exit_side,
                    "OPT",
                    exit_fill,
                    lot_size,
                    slippage_per_unit=exit_slippage,
                )
            )
            margin_legs.append(
                {
                    "side": entry_side,
                    "instrument": "OPT",
                    "option_type": "CE" if row.option_type == "CALL" else "PE",
                    "strike": float(row.strike),
                    "lot_size": lot_size,
                    "entry_price": entry_fill,
                    "expiry": pd.Timestamp(row.actual_expiry_date).date().isoformat(),
                }
            )
        costs = estimate_round_trip_execution_cost(
            entry_legs=entry_legs,
            exit_legs=exit_legs,
        )
        margin = estimate_defined_risk_margin(
            legs=margin_legs,
            span_data=_span_data(part),
            expiry=pd.Timestamp(first["actual_expiry_date"]).date().isoformat(),
            spot=float(first["entry_spot"]),
            eval_dt=pd.Timestamp(first["entry_ts"]).to_pydatetime(),
        )
        gross_rupees = gross_points * lot_size
        net_rupees = gross_rupees - costs.total
        entry_turnover = sum(leg.price * leg.quantity for leg in entry_legs)
        exit_turnover = sum(leg.price * leg.quantity for leg in exit_legs)
        sink.update(
            {
                "gross_pnl_rupees": gross_rupees,
                "entry_turnover_rupees": entry_turnover,
                "exit_turnover_rupees": exit_turnover,
                "total_turnover_rupees": entry_turnover + exit_turnover,
                "entry_slippage_rupees": costs.entry.slippage,
                "exit_slippage_rupees": costs.exit.slippage,
                "total_slippage_rupees": costs.total_slippage,
                "total_charges_rupees": costs.total_charges,
                "total_cost_rupees": costs.total,
                "net_pnl_rupees": net_rupees,
                "margin_rupees": margin.margin,
                "gross_return_on_margin": gross_rupees / margin.margin,
                "net_return_on_margin": net_rupees / margin.margin,
                "cost_bps_of_turnover": costs.total / (entry_turnover + exit_turnover) * 10_000,
                "span_scan_margin": margin.s_net_clamped,
                "span_elm_required": margin.elm_required,
                "span_long_premium": margin.long_premium,
                "span_slot": margin.span_time_slot,
            }
        )
        for field in CHARGE_FIELDS:
            sink[f"charge_{field}_rupees"] = float(
                getattr(costs.entry.charges, field) + getattr(costs.exit.charges, field)
            )
    max_loss_points, max_profit_points = _theoretical_risk(part, weights)
    return {
        "trade_id": int(first["trade_id"]),
        "strategy": strategy,
        "trade_date": str(first["trade_date"]),
        "signal_ts": first["signal_ts"],
        "entry_ts": first["entry_ts"],
        "exit_ts": first["exit_ts"],
        "entry_time": str(first["execution_entry_time"]),
        "expiry_date": pd.Timestamp(first["actual_expiry_date"]).date().isoformat(),
        "entry_dte": float(first["entry_dte"]),
        "lot_size": lot_size,
        "entry_spot": float(first["entry_spot"]),
        "exit_spot": float(first["exit_spot"]),
        "signal_vrp_var_act365": float(first["signal_vrp_var_act365"]),
        "causal_vrp_percentile": float(first["vrp_tod_percentile"]),
        "gross_pnl_points": gross_points,
        "theoretical_max_loss_points": max_loss_points,
        "theoretical_max_profit_points": max_profit_points,
        "gross_return_on_max_loss": gross_points / max_loss_points,
        "entry_vix_fallback_legs": int(part["entry_vix_fallback"].sum()),
        "exit_vix_fallback_legs": int(part["exit_vix_fallback"].sum()),
        "entry_stale_penalty_legs": int((part["entry_stale_multiplier"] > 1.0).sum()),
        "exit_stale_penalty_legs": int((part["exit_stale_multiplier"] > 1.0).sum()),
        "span_publication_time_proven": bool(
            part["span_slot_publication_times_proven"].fillna(False).all()
        ),
        **{f"base_{key}": value for key, value in base.items()},
        **{f"stress_1_5x_{key}": value for key, value in stress.items()},
    }


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return {"count": 0}
    tail_count = max(1, int(math.ceil(len(clean) * 0.05)))
    return {
        "count": int(len(clean)),
        "sum": float(clean.sum()),
        "mean": float(clean.mean()),
        "median": float(clean.median()),
        "std": float(clean.std()),
        "win_rate": float((clean > 0).mean()),
        "p05": float(clean.quantile(0.05)),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "p95": float(clean.quantile(0.95)),
        "cvar05": float(clean.nsmallest(tail_count).mean()),
        "minimum": float(clean.min()),
        "maximum": float(clean.max()),
    }


def _bootstrap_mean_ci(values: pd.Series, samples: int = 10_000) -> list[float]:
    clean = pd.to_numeric(values, errors="coerce").dropna().to_numpy(dtype=float)
    if len(clean) < 10:
        return [float("nan"), float("nan")]
    rng = np.random.default_rng(BOOTSTRAP_SEED)
    means = np.empty(samples, dtype=float)
    for index in range(samples):
        means[index] = rng.choice(clean, size=len(clean), replace=True).mean()
    return [float(np.quantile(means, 0.025)), float(np.quantile(means, 0.975))]


def _max_drawdown(values: pd.Series) -> dict[str, Any]:
    clean = pd.to_numeric(values, errors="coerce").fillna(0.0).to_numpy(dtype=float)
    equity = np.cumsum(clean)
    if not len(equity):
        return {"amount": 0.0, "peak_trade": None, "trough_trade": None}
    running_peak = np.maximum.accumulate(np.concatenate(([0.0], equity)))[:-1]
    drawdown = equity - running_peak
    trough = int(np.argmin(drawdown))
    peak = int(np.argmax(np.concatenate(([0.0], equity[: trough + 1])))) - 1
    return {
        "amount": float(drawdown[trough]),
        "peak_trade": None if peak < 0 else peak + 1,
        "trough_trade": trough + 1,
    }


def _profit_factor(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    gains = float(clean[clean > 0].sum())
    losses = abs(float(clean[clean < 0].sum()))
    return gains / losses if losses > 0.0 else float("inf")


def _period_stability(frame: pd.DataFrame, pnl_column: str, frequency: str) -> dict[str, Any]:
    dated = frame.copy()
    dated["trade_date_dt"] = pd.to_datetime(dated["trade_date"])
    periods = dated.groupby(dated["trade_date_dt"].dt.to_period(frequency))[pnl_column].agg(
        ["sum", "count"]
    )
    return {
        "periods": int(len(periods)),
        "positive_period_share": float((periods["sum"] > 0).mean()),
        "mean_period_pnl": float(periods["sum"].mean()),
        "median_period_pnl": float(periods["sum"].median()),
        "worst_period": str(periods["sum"].idxmin()),
        "worst_period_pnl": float(periods["sum"].min()),
        "best_period": str(periods["sum"].idxmax()),
        "best_period_pnl": float(periods["sum"].max()),
    }


def _strategy_summary(frame: pd.DataFrame, scenario: str) -> dict[str, Any]:
    prefix = f"{scenario}_"
    gross = frame[f"{prefix}gross_pnl_rupees"]
    net = frame[f"{prefix}net_pnl_rupees"]
    rom = frame[f"{prefix}net_return_on_margin"]
    total_cost = frame[f"{prefix}total_cost_rupees"]
    cost_points = total_cost / frame["lot_size"]
    return {
        "trades": int(len(frame)),
        "first_trade": str(frame["trade_date"].min()),
        "last_trade": str(frame["trade_date"].max()),
        "gross_pnl_rupees": _distribution(gross),
        "gross_pnl_points": _distribution(frame["gross_pnl_points"]),
        "net_pnl_rupees": _distribution(net),
        "net_return_on_margin": _distribution(rom),
        "mean_net_return_on_margin_bootstrap_95": _bootstrap_mean_ci(rom),
        "profit_factor_net": _profit_factor(net),
        "trades_gross_above_cost_share": float((gross > total_cost).mean()),
        "aggregate_cost_to_gross_profit_ratio": (
            float(total_cost.sum() / gross.sum()) if gross.sum() > 0.0 else float("nan")
        ),
        "break_even_cost_points": _distribution(cost_points),
        "max_drawdown_net_rupees": _max_drawdown(net),
        "turnover_rupees": _distribution(frame[f"{prefix}total_turnover_rupees"]),
        "margin_rupees": _distribution(frame[f"{prefix}margin_rupees"]),
        "costs": {
            "total_slippage_rupees": float(frame[f"{prefix}total_slippage_rupees"].sum()),
            "total_charges_rupees": float(frame[f"{prefix}total_charges_rupees"].sum()),
            "total_cost_rupees": float(frame[f"{prefix}total_cost_rupees"].sum()),
            "mean_cost_per_trade": float(frame[f"{prefix}total_cost_rupees"].mean()),
            "median_cost_bps_of_turnover": float(
                frame[f"{prefix}cost_bps_of_turnover"].median()
            ),
            "charge_components": {
                field: float(frame[f"{prefix}charge_{field}_rupees"].sum())
                for field in CHARGE_FIELDS
            },
        },
        "weekly_stability": _period_stability(frame, f"{prefix}net_pnl_rupees", "W"),
        "monthly_stability": _period_stability(frame, f"{prefix}net_pnl_rupees", "M"),
    }


def _group_table(frame: pd.DataFrame, group: str) -> list[dict[str, Any]]:
    rows = []
    for value, part in frame.groupby(group, observed=True, sort=True):
        rows.append(
            {
                group: str(value),
                "trades": int(len(part)),
                "gross_pnl": float(part["base_gross_pnl_rupees"].sum()),
                "total_cost": float(part["base_total_cost_rupees"].sum()),
                "net_pnl": float(part["base_net_pnl_rupees"].sum()),
                "net_mean": float(part["base_net_pnl_rupees"].mean()),
                "net_win_rate": float((part["base_net_pnl_rupees"] > 0).mean()),
                "net_rom_mean": float(part["base_net_return_on_margin"].mean()),
            }
        )
    return rows


def run_backtest(
    *,
    gold_root: Path,
    structure_path: Path,
    surface_path: Path,
    tradebook_path: Path,
    legbook_path: Path,
    summary_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    gold_glob = str(gold_root / "year=*" / "month=*" / "part-*.parquet")
    events = _primary_events(structure_path)
    raw_legs = _load_legbook(events, gold_glob, surface_path)
    validation = _validate_legbook(raw_legs, len(events))
    legbook = _add_slippage(raw_legs)
    trade_rows = []
    for _, part in legbook.groupby("trade_id", sort=True):
        trade_rows.append(_trade_row(part, strategy="short_iron_condor", weight_sign=1))
        trade_rows.append(_trade_row(part, strategy="long_iron_condor_inverse", weight_sign=-1))
    tradebook = pd.DataFrame(trade_rows).sort_values(["strategy", "trade_date", "trade_id"])

    primary = tradebook.loc[tradebook["strategy"].eq("short_iron_condor")].copy()
    inverse = tradebook.loc[tradebook["strategy"].eq("long_iron_condor_inverse")].copy()
    primary["year"] = primary["trade_date"].str[:4]
    primary["entry_hour"] = primary["entry_time"].str[:2]
    primary["dte_bucket"] = pd.cut(
        primary["entry_dte"],
        bins=[0, 7, 10, 14, float("inf")],
        labels=["0-7", "7-10", "10-14", "14+"],
        include_lowest=True,
    )
    primary["lot_size_regime"] = primary["lot_size"].astype(str)

    source_vix_missing = int(
        (legbook["entry_vix_fallback"] | legbook["exit_vix_fallback"]).sum()
    )
    reconciliation_error = (
        primary["gross_pnl_points"]
        - events.set_index("trade_id").loc[
            primary["trade_id"], "next_short_iron_condor__pnl_points"
        ].to_numpy()
    ).abs()
    summary = {
        "schema_version": SCHEMA_VERSION,
        "hypothesis_id": "H1_VRP_LEVEL_DIRECTION_SHORT_CONDOR_60M",
        "decision_status": "not_confirmed_no_prospective_oos",
        "economic_result": "fails_cost_inclusive_viability",
        "inputs": {
            "gold_root": str(gold_root.resolve()),
            "structure_path": str(structure_path.resolve()),
            "structure_sha256": _sha256(structure_path),
            "surface_path": str(surface_path.resolve()),
            "surface_sha256": _sha256(surface_path),
            "holding_minutes": HOLDING_MINUTES,
            "slippage_stress_multiplier": SLIPPAGE_STRESS_MULTIPLIER,
            "position_size": "one historical exchange lot per trade; no compounding",
            "span_selection": (
                "latest six-slot research reference at or before entry; BOD assumed "
                "available at 09:15 before ID1"
            ),
            "volatility_proxy": (
                "India VIX when observed, otherwise reconstructed ATM IV multiplied by 100"
            ),
        },
        "coverage": {
            **validation,
            "sample_first_trade": str(primary["trade_date"].min()),
            "sample_last_trade": str(primary["trade_date"].max()),
            "entry_vix_fallback_leg_rows": int(legbook["entry_vix_fallback"].sum()),
            "exit_vix_fallback_leg_rows": int(legbook["exit_vix_fallback"].sum()),
            "any_vix_fallback_leg_rows": source_vix_missing,
            "entry_stale_penalty_leg_rows": int(
                (legbook["entry_stale_multiplier"] > 1.0).sum()
            ),
            "exit_stale_penalty_leg_rows": int(
                (legbook["exit_stale_multiplier"] > 1.0).sum()
            ),
            "span_matched_leg_rows": int(legbook["span_join_status"].eq("matched").sum()),
            "span_intraday_asof_joined_leg_rows": int(
                legbook["span_intraday_asof_join_performed"].fillna(False).sum()
            ),
            "span_reference_schedule_asof_leg_rows": int(
                (
                    pd.to_datetime(legbook["span_reference_ts_ist"], utc=True)
                    <= pd.to_datetime(legbook["entry_ts"], utc=True)
                ).sum()
            ),
            "span_bod_open_time_assumption_leg_rows": int(
                legbook["span_bod_open_time_assumption"].fillna(False).sum()
            ),
            "span_publication_time_proven_trades": int(
                primary["span_publication_time_proven"].sum()
            ),
            "expiry_day_trades": int(
                (pd.to_datetime(primary["trade_date"]).dt.date == pd.to_datetime(primary["expiry_date"]).dt.date).sum()
            ),
            "maximum_gross_reconciliation_error_points": float(reconciliation_error.max()),
        },
        "primary": {
            "base": _strategy_summary(primary, "base"),
            "stress_1_5x": _strategy_summary(primary, "stress_1_5x"),
        },
        "exact_inverse": {
            "base": _strategy_summary(inverse, "base"),
            "stress_1_5x": _strategy_summary(inverse, "stress_1_5x"),
        },
        "primary_breakdowns": {
            "by_year": _group_table(primary, "year"),
            "by_entry_hour": _group_table(primary, "entry_hour"),
            "by_dte_bucket": _group_table(primary, "dte_bucket"),
            "by_lot_size": _group_table(primary, "lot_size_regime"),
        },
        "limitations": [
            "Every observation predates the 2026-07-18 hypothesis freeze; there is no prospective OOS trade.",
            "The Dhan rolling WEEK history is a nearest-listed-expiry proxy and does not prove actual nearest-weekly identity.",
            "Historical bid/ask is unavailable; fills use the pinned volume/OI synthetic slippage model.",
            "The slippage stale multiplier is a low volume/OI-turnover proxy, not elapsed quote age.",
            "SPAN uses the six-slot research reference-price schedule, not proven file-arrival timestamps.",
            "BOD is assumed available at the 09:15 session open until the 11:00 ID1 reference.",
            "No retained primary trade is on expiry day, so expiry-day margin and settlement mechanics are not empirically exercised.",
            "Matched non-crossing and unconditional scheduled controls require a separately frozen matching/schedule rule.",
        ],
    }

    tradebook_path.parent.mkdir(parents=True, exist_ok=True)
    legbook_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tradebook.to_csv(tradebook_path, index=False)
    legbook.to_csv(legbook_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "phase3-full-strategy-manifest/v1",
        "code": {
            "path": str(Path(__file__).resolve()),
            "sha256": _sha256(Path(__file__).resolve()),
        },
        "inputs": [
            {"path": str(structure_path.resolve()), "sha256": _sha256(structure_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
            {"path": str(legbook_path.resolve()), "sha256": _sha256(legbook_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
        ],
        "gold_root": str(gold_root.resolve()),
        "trades": int(len(primary)),
        "leg_rows": int(len(legbook)),
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True),
        encoding="utf-8",
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
        "--surface-path",
        type=Path,
        default=Path("audit/phase2_intraday_iv_surface.parquet"),
    )
    parser.add_argument(
        "--tradebook",
        type=Path,
        default=Path("audit/phase3_full_strategy_tradebook.csv"),
    )
    parser.add_argument(
        "--legbook",
        type=Path,
        default=Path("audit/phase3_full_strategy_legbook.csv"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase3_full_strategy_tearsheet.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase3_full_strategy_manifest.json"),
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    summary = run_backtest(
        gold_root=args.gold_root,
        structure_path=args.structure_path,
        surface_path=args.surface_path,
        tradebook_path=args.tradebook,
        legbook_path=args.legbook,
        summary_path=args.summary,
        manifest_path=args.manifest,
    )
    print(json.dumps(_json_safe(summary["primary"]), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
