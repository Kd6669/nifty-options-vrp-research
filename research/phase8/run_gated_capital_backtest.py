"""Run the ₹10 lakh capital-aware gated upper-tail VRP strategy backtest."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from nifty_execution import (
    ExecutedLeg,
    estimate_participation_impact,
    estimate_round_trip_execution_cost,
    groww_fno_rates_for_date,
)
from research.phase2.analyze_defined_risk_vrp import STRUCTURES
from research.phase3.run_full_strategy_backtest import _fill_price
from research.phase4.run_cost_aware_discovery import (
    CAPACITY_IMPACT_PARAMETERS,
    MINIMUM_SELL_FILL,
)


SCHEMA_VERSION = "phase8-gated-capital-backtest/v1"
INITIAL_CAPITAL = 1_000_000.0
MAX_CAPACITY_LOTS = 100
DISCOVERY_END_YEAR = 2023
VALIDATION_YEAR = 2024
STRUCTURE_NAMES = (
    "short_iron_fly",
    "short_iron_condor",
    "long_put_butterfly",
)


@dataclass(frozen=True)
class SizingPolicy:
    name: str
    margin_fraction: float
    max_loss_fraction: float | None


POLICIES = (
    SizingPolicy("conservative", 0.25, 0.01),
    SizingPolicy("balanced", 0.50, 0.02),
    SizingPolicy("growth", 0.75, 0.03),
    SizingPolicy("margin_only", 1.00, None),
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


def _split_for_year(year: int) -> str:
    if year <= DISCOVERY_END_YEAR:
        return "discovery_2021_2023"
    if year == VALIDATION_YEAR:
        return "validation_2024"
    return "confirmation_2025_2026"


def build_event_features(curve_path: Path, surface_path: Path) -> pd.DataFrame:
    curve = pd.read_parquet(curve_path).sort_values(["trade_date", "entry_ts"]).copy()
    curve["entry_ts"] = pd.to_datetime(curve["entry_ts"], utc=True)
    curve["trade_date"] = pd.to_datetime(curve["trade_date"]).dt.date
    lag_source = curve[["trade_date", "entry_ts", "spot", "atm_iv", "trailing_rv_act365"]].copy()
    for minutes in (5, 15, 30):
        lag = lag_source.copy()
        lag["entry_ts"] = lag["entry_ts"] + pd.Timedelta(minutes=minutes)
        lag = lag.rename(
            columns={
                "spot": f"spot_lag_{minutes}",
                "atm_iv": f"atm_iv_lag_{minutes}",
                "trailing_rv_act365": f"rv_lag_{minutes}",
            }
        )
        curve = curve.merge(lag, on=["trade_date", "entry_ts"], how="left")
        curve[f"spot_return_{minutes}m"] = curve["spot"] / curve[f"spot_lag_{minutes}"] - 1.0
        curve[f"iv_change_{minutes}m"] = curve["atm_iv"] - curve[f"atm_iv_lag_{minutes}"]
        curve[f"rv_change_{minutes}m"] = curve["trailing_rv_act365"] - curve[f"rv_lag_{minutes}"]
    surface = pd.read_parquet(
        surface_path,
        columns=[
            "timestamp_ist",
            "put_skew",
            "call_skew",
            "risk_reversal",
            "smile_curvature",
            "atm_ce_pe_gap",
            "atm_iv_tod_percentile",
        ],
    )
    surface["entry_ts"] = pd.to_datetime(surface.pop("timestamp_ist"), utc=True)
    surface = surface.drop_duplicates("entry_ts")
    curve = curve.merge(surface, on="entry_ts", how="left")
    curve["minute_of_day"] = (
        curve["entry_ts"].dt.tz_convert("Asia/Kolkata").dt.hour * 60
        + curve["entry_ts"].dt.tz_convert("Asia/Kolkata").dt.minute
    )
    return curve


def build_gated_events(
    observations: pd.DataFrame,
    features: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, float]]:
    events = observations.loc[
        observations["signal_family"].eq("upper85_up") & observations["horizon_minutes"].eq(60),
        ["trade_id", "entry_ts", "trade_date", "entry_dte", "span_time_slot"],
    ].drop_duplicates("trade_id")
    events["entry_ts"] = pd.to_datetime(events["entry_ts"], utc=True)
    feature_columns = [
        "entry_ts",
        "spot",
        "atm_iv",
        "trailing_rv_act365",
        "signal_vrp_var_act365",
        "vrp_tod_percentile",
        "vrp_q5",
        "q_velocity_5m",
        "q_acceleration_5m",
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
        "put_skew",
        "call_skew",
        "risk_reversal",
        "smile_curvature",
        "atm_ce_pe_gap",
        "atm_iv_tod_percentile",
        "minute_of_day",
    ]
    events = events.merge(features[feature_columns], on="entry_ts", how="left", validate="1:1")
    events["year"] = events["entry_ts"].dt.year
    events["split"] = events["year"].map(_split_for_year)
    discovery = events.loc[events["year"] <= DISCOVERY_END_YEAR]
    thresholds = {
        "iv_change_5m": float(discovery["iv_change_5m"].quantile(0.25)),
        "iv_change_15m": float(discovery["iv_change_15m"].quantile(0.25)),
        "rv_change_5m": float(discovery["rv_change_5m"].quantile(0.25)),
    }
    gate_columns = list(thresholds)
    events["gate_complete"] = events[gate_columns].notna().all(axis=1)
    events["gate_pass"] = events["gate_complete"]
    for column, threshold in thresholds.items():
        events["gate_pass"] &= events[column] > threshold
    events["gate_fail_reason"] = "pass"
    for column, threshold in thresholds.items():
        failed = events["gate_complete"] & (events[column] <= threshold)
        events.loc[failed, "gate_fail_reason"] = (
            events.loc[failed, "gate_fail_reason"].where(
                events.loc[failed, "gate_fail_reason"].eq("pass"), lambda x: x + "|"
            )
            + column
        )
    events.loc[~events["gate_complete"], "gate_fail_reason"] = "incomplete_gate_features"
    normalized_cushions = []
    for column, threshold in thresholds.items():
        scale = float(discovery[column].std(ddof=0))
        normalized_cushions.append((events[column] - threshold) / max(scale, 1e-12))
    events["gate_cushion"] = pd.concat(normalized_cushions, axis=1).min(axis=1)
    return events.sort_values("entry_ts").reset_index(drop=True), thresholds


def _execution_record(
    part: pd.DataFrame,
    *,
    structure: str,
    lots: int,
    metadata: pd.Series,
) -> dict[str, Any]:
    weights = {leg: int(weight) for leg, weight in STRUCTURES[structure].items()}
    first = part.iloc[0]
    lot_size = int(first.lot_size)
    gross_points = 0.0
    entry_legs: list[ExecutedLeg] = []
    exit_legs: list[ExecutedLeg] = []
    base_slippage = 0.0
    ladder_impact = 0.0
    volume_impact = 0.0
    oi_impact = 0.0
    turnover = 0.0
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
        turnover += quantity * (entry_fill + exit_fill)
        base_slippage += quantity * (
            entry_impact.base_slippage_per_unit + exit_impact.base_slippage_per_unit
        )
        ladder_impact += quantity * (
            entry_impact.ladder_impact_per_unit + exit_impact.ladder_impact_per_unit
        )
        volume_impact += quantity * (
            entry_impact.volume_impact_per_unit + exit_impact.volume_impact_per_unit
        )
        oi_impact += quantity * (entry_impact.oi_impact_per_unit + exit_impact.oi_impact_per_unit)
        gross_points += weight * (float(row.exit_close) - float(row.entry_close))
    costs = estimate_round_trip_execution_cost(
        entry_legs=entry_legs,
        exit_legs=exit_legs,
        entry_rates=groww_fno_rates_for_date(pd.Timestamp(first.entry_ts).date()),
        exit_rates=groww_fno_rates_for_date(pd.Timestamp(first.horizon_exit_ts).date()),
    )
    entry_charges = costs.entry.charges
    exit_charges = costs.exit.charges
    charges = entry_charges.total + exit_charges.total
    impact = ladder_impact + volume_impact + oi_impact
    gross = gross_points * lot_size * lots
    return {
        "trade_id": int(first.trade_id),
        "trade_date": str(first.trade_date),
        "entry_ts": pd.Timestamp(first.entry_ts),
        "exit_ts": pd.Timestamp(first.horizon_exit_ts),
        "split": str(metadata["split"]),
        "structure": structure,
        "lots": lots,
        "lot_size": lot_size,
        "gross_pnl_rupees": gross,
        "turnover_rupees": turnover,
        "base_slippage_rupees": base_slippage,
        "ladder_impact_rupees": ladder_impact,
        "volume_impact_rupees": volume_impact,
        "oi_impact_rupees": oi_impact,
        "impact_rupees": impact,
        "slippage_rupees": costs.total_slippage,
        "charges_rupees": charges,
        "brokerage_rupees": entry_charges.brokerage + exit_charges.brokerage,
        "stt_rupees": entry_charges.stt_ctt + exit_charges.stt_ctt,
        "stamp_duty_rupees": entry_charges.stamp_duty + exit_charges.stamp_duty,
        "exchange_charges_rupees": (
            entry_charges.exchange_transaction + exit_charges.exchange_transaction
        ),
        "sebi_charges_rupees": entry_charges.sebi_turnover + exit_charges.sebi_turnover,
        "ipft_rupees": entry_charges.ipft + exit_charges.ipft,
        "gst_rupees": entry_charges.gst + exit_charges.gst,
        "total_cost_rupees": costs.total,
        "net_pnl_rupees": gross - costs.total,
        "margin_rupees": float(metadata["margin_rupees"]) * lots,
        "max_loss_rupees": float(metadata["max_loss_points"]) * lot_size * lots,
        "entry_dte": float(metadata["entry_dte"]),
        "span_time_slot": str(metadata["span_time_slot"]),
    }


def build_cost_surface(
    observations: pd.DataFrame,
    phase4_tradebook: pd.DataFrame,
    events: pd.DataFrame,
) -> pd.DataFrame:
    gated = events.loc[events["gate_pass"]].set_index("trade_id")
    selected_observations = observations.loc[
        observations["trade_id"].isin(gated.index)
        & observations["signal_family"].eq("upper85_up")
        & observations["horizon_minutes"].eq(60)
    ].copy()
    selected_tradebook = phase4_tradebook.loc[
        phase4_tradebook["trade_id"].isin(gated.index)
        & phase4_tradebook["signal_family"].eq("upper85_up")
        & phase4_tradebook["horizon_minutes"].eq(60)
        & phase4_tradebook["structure"].isin(STRUCTURE_NAMES)
    ].copy()
    metadata = selected_tradebook.set_index(["trade_id", "structure"])
    rows: list[dict[str, Any]] = []
    for trade_id, full_part in selected_observations.groupby("trade_id", sort=True):
        event = gated.loc[int(trade_id)]
        for structure in STRUCTURE_NAMES:
            weights = STRUCTURES[structure]
            part = full_part.loc[full_part["leg"].isin(weights)].copy()
            if len(part) != len(weights) or part["exit_close"].isna().any():
                continue
            meta = metadata.loc[(int(trade_id), structure)].copy()
            meta["split"] = event["split"]
            meta["span_time_slot"] = part.iloc[0]["span_time_slot"]
            for lots in range(1, MAX_CAPACITY_LOTS + 1):
                rows.append(
                    _execution_record(
                        part,
                        structure=structure,
                        lots=lots,
                        metadata=meta,
                    )
                )
    return pd.DataFrame(rows).sort_values(["structure", "trade_id", "lots"])


def summarize_capacity(surface: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    discovery = surface.loc[surface["split"].eq("discovery_2021_2023")]
    summary = (
        discovery.groupby(["structure", "lots"], as_index=False)
        .agg(
            trades=("trade_id", "size"),
            mean_gross=("gross_pnl_rupees", "mean"),
            mean_net=("net_pnl_rupees", "mean"),
            median_net=("net_pnl_rupees", "median"),
            net_win_rate=("net_pnl_rupees", lambda values: float((values > 0).mean())),
            mean_cost=("total_cost_rupees", "mean"),
            mean_impact=("impact_rupees", "mean"),
            mean_margin=("margin_rupees", "mean"),
            mean_max_loss=("max_loss_rupees", "mean"),
        )
        .sort_values(["structure", "lots"])
    )
    caps: dict[str, int] = {}
    for structure, part in summary.groupby("structure", sort=True):
        best = part.loc[part["mean_net"].idxmax()]
        caps[str(structure)] = int(best["lots"]) if float(best["mean_net"]) > 0.0 else 0
    summary["selected_capacity_cap"] = summary.apply(
        lambda row: int(row["lots"]) == caps[str(row["structure"])], axis=1
    )
    return summary, caps


def simulate_portfolios(
    surface: pd.DataFrame,
    events: pd.DataFrame,
    caps: dict[str, int],
    *,
    initial_capital: float,
) -> pd.DataFrame:
    event_features = events.set_index("trade_id")
    lookup = surface.set_index(["structure", "trade_id", "lots"])
    one_lot = surface.loc[surface["lots"].eq(1)].set_index(["structure", "trade_id"])
    rows: list[dict[str, Any]] = []
    for structure in STRUCTURE_NAMES:
        trade_ids = (
            surface.loc[surface["structure"].eq(structure), ["trade_id", "entry_ts"]]
            .drop_duplicates("trade_id")
            .sort_values("entry_ts")["trade_id"]
            .tolist()
        )
        for policy in POLICIES:
            equity = float(initial_capital)
            for trade_id in trade_ids:
                event = event_features.loc[int(trade_id)]
                one = one_lot.loc[(structure, int(trade_id))]
                margin_per_lot = float(one["margin_rupees"])
                max_loss_per_lot = float(one["max_loss_rupees"])
                margin_lots = math.floor(
                    equity * policy.margin_fraction / max(margin_per_lot, 1e-12)
                )
                risk_lots = (
                    MAX_CAPACITY_LOTS
                    if policy.max_loss_fraction is None
                    else math.floor(
                        equity * policy.max_loss_fraction / max(max_loss_per_lot, 1e-12)
                    )
                )
                capacity_lots = int(caps[structure])
                lots = max(min(margin_lots, risk_lots, capacity_lots), 0)
                before = equity
                common = {
                    "trade_id": int(trade_id),
                    "trade_date": str(event["trade_date"]),
                    "entry_ts": pd.Timestamp(event["entry_ts"]),
                    "split": str(event["split"]),
                    "structure": structure,
                    "policy": policy.name,
                    "margin_fraction": policy.margin_fraction,
                    "max_loss_fraction": policy.max_loss_fraction,
                    "capacity_cap_lots": capacity_lots,
                    "margin_cap_lots": margin_lots,
                    "risk_cap_lots": risk_lots,
                    "lots": lots,
                    "equity_before": before,
                }
                if lots <= 0:
                    rows.append(
                        {
                            **common,
                            "executed": False,
                            "skip_reason": "capital_or_risk_budget_below_one_lot",
                            "gross_pnl_rupees": 0.0,
                            "net_pnl_rupees": 0.0,
                            "turnover_rupees": 0.0,
                            "base_slippage_rupees": 0.0,
                            "impact_rupees": 0.0,
                            "slippage_rupees": 0.0,
                            "charges_rupees": 0.0,
                            "total_cost_rupees": 0.0,
                            "margin_rupees": 0.0,
                            "max_loss_rupees": 0.0,
                            "equity_after": before,
                            "margin_utilization": 0.0,
                            "max_loss_utilization": 0.0,
                        }
                    )
                    continue
                selected = lookup.loc[(structure, int(trade_id), lots)]
                equity += float(selected["net_pnl_rupees"])
                rows.append(
                    {
                        **common,
                        "executed": True,
                        "skip_reason": "",
                        **{
                            column: selected[column]
                            for column in (
                                "gross_pnl_rupees",
                                "net_pnl_rupees",
                                "turnover_rupees",
                                "base_slippage_rupees",
                                "ladder_impact_rupees",
                                "volume_impact_rupees",
                                "oi_impact_rupees",
                                "impact_rupees",
                                "slippage_rupees",
                                "charges_rupees",
                                "brokerage_rupees",
                                "stt_rupees",
                                "stamp_duty_rupees",
                                "exchange_charges_rupees",
                                "sebi_charges_rupees",
                                "ipft_rupees",
                                "gst_rupees",
                                "total_cost_rupees",
                                "margin_rupees",
                                "max_loss_rupees",
                                "entry_dte",
                                "span_time_slot",
                            )
                        },
                        "equity_after": equity,
                        "margin_utilization": float(selected["margin_rupees"]) / before,
                        "max_loss_utilization": float(selected["max_loss_rupees"]) / before,
                    }
                )
    return pd.DataFrame(rows).sort_values(["structure", "policy", "entry_ts"])


def _max_losing_streak(values: pd.Series) -> int:
    best = current = 0
    for value in values:
        if value < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _daily_curve(part: pd.DataFrame, initial_capital: float) -> pd.DataFrame:
    executed = part.loc[part["executed"]].copy()
    start = pd.Timestamp(part["entry_ts"].min()).tz_convert("Asia/Kolkata").tz_localize(None).date()
    end = pd.Timestamp(part["entry_ts"].max()).tz_convert("Asia/Kolkata").tz_localize(None).date()
    calendar = pd.DataFrame({"date": pd.bdate_range(start, end)})
    pnl = (
        executed.assign(date=pd.to_datetime(executed["trade_date"]))
        .groupby("date", as_index=False)["net_pnl_rupees"]
        .sum()
    )
    curve = calendar.merge(pnl, on="date", how="left").fillna({"net_pnl_rupees": 0.0})
    curve["equity"] = initial_capital + curve["net_pnl_rupees"].cumsum()
    curve["peak_equity"] = curve["equity"].cummax()
    curve["drawdown_rupees"] = curve["equity"] - curve["peak_equity"]
    curve["drawdown_pct"] = curve["equity"] / curve["peak_equity"] - 1.0
    curve["daily_return"] = curve["equity"].pct_change().fillna(0.0)
    return curve


def _max_drawdown_duration(curve: pd.DataFrame) -> int:
    current = best = 0
    for value in curve["drawdown_rupees"]:
        if value < 0:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _drawdown_episode(curve: pd.DataFrame) -> dict[str, Any]:
    trough_idx = int(curve["drawdown_rupees"].idxmin())
    trough = curve.loc[trough_idx]
    before = curve.loc[:trough_idx]
    peak_idx = int(before["equity"].idxmax())
    peak = curve.loc[peak_idx]
    after = curve.loc[trough_idx + 1 :]
    recovered = after.loc[after["equity"] >= float(peak["equity"])]
    return {
        "peak_date": pd.Timestamp(peak["date"]).date().isoformat(),
        "trough_date": pd.Timestamp(trough["date"]).date().isoformat(),
        "recovery_date": (
            pd.Timestamp(recovered.iloc[0]["date"]).date().isoformat()
            if len(recovered)
            else None
        ),
    }


def _monthly_stability(executed: pd.DataFrame, initial_capital: float) -> dict[str, Any]:
    monthly = (
        executed.assign(month=pd.to_datetime(executed["trade_date"]).dt.to_period("M").astype(str))
        .groupby("month", as_index=False)["net_pnl_rupees"]
        .sum()
    )
    if monthly.empty:
        return {
            "active_months": 0,
            "positive_months": 0,
            "positive_month_rate": 0.0,
            "best_month": None,
            "best_month_pnl": 0.0,
            "worst_month": None,
            "worst_month_pnl": 0.0,
        }
    best = monthly.loc[monthly["net_pnl_rupees"].idxmax()]
    worst = monthly.loc[monthly["net_pnl_rupees"].idxmin()]
    return {
        "active_months": int(len(monthly)),
        "positive_months": int((monthly["net_pnl_rupees"] > 0).sum()),
        "positive_month_rate": float((monthly["net_pnl_rupees"] > 0).mean()),
        "average_month_pnl": float(monthly["net_pnl_rupees"].mean()),
        "average_month_return_on_initial_capital": float(
            monthly["net_pnl_rupees"].mean() / initial_capital
        ),
        "best_month": str(best["month"]),
        "best_month_pnl": float(best["net_pnl_rupees"]),
        "worst_month": str(worst["month"]),
        "worst_month_pnl": float(worst["net_pnl_rupees"]),
    }


def summarize_portfolio(part: pd.DataFrame, initial_capital: float) -> dict[str, Any]:
    executed = part.loc[part["executed"]].copy()
    curve = _daily_curve(part, initial_capital)
    years = max((curve["date"].max() - curve["date"].min()).days / 365.25, 1 / 365.25)
    final_equity = float(curve["equity"].iloc[-1])
    cagr = (final_equity / initial_capital) ** (1.0 / years) - 1.0 if final_equity > 0 else -1.0
    daily = curve["daily_return"]
    volatility = float(daily.std(ddof=1) * math.sqrt(252)) if len(daily) > 1 else 0.0
    sharpe = (
        float(daily.mean() / daily.std(ddof=1) * math.sqrt(252)) if daily.std(ddof=1) > 0 else 0.0
    )
    downside = daily.loc[daily < 0].std(ddof=1)
    sortino = float(daily.mean() / downside * math.sqrt(252)) if downside > 0 else 0.0
    max_drawdown = float(curve["drawdown_rupees"].min())
    max_drawdown_pct = float(curve["drawdown_pct"].min())
    wins = executed.loc[executed["net_pnl_rupees"] > 0, "net_pnl_rupees"]
    losses = executed.loc[executed["net_pnl_rupees"] < 0, "net_pnl_rupees"]
    tail_count = max(1, math.ceil(0.05 * len(executed))) if len(executed) else 0
    worst_tail = executed.nsmallest(tail_count, "net_pnl_rupees") if tail_count else executed
    drawdown_episode = _drawdown_episode(curve)
    monthly = _monthly_stability(executed, initial_capital)
    return {
        "eligible_signals": int(len(part)),
        "executed_trades": int(len(executed)),
        "skipped_trades": int((~part["executed"]).sum()),
        "initial_capital": initial_capital,
        "final_equity": final_equity,
        "net_profit": final_equity - initial_capital,
        "total_return": final_equity / initial_capital - 1.0,
        "cagr": cagr,
        "annualized_volatility": volatility,
        "sharpe_zero_rate": sharpe,
        "sortino_zero_rate": sortino,
        "calmar": cagr / abs(max_drawdown_pct) if max_drawdown_pct < 0 else 0.0,
        "gross_pnl": float(executed["gross_pnl_rupees"].sum()),
        "total_cost": float(executed["total_cost_rupees"].sum()),
        "charges": float(executed["charges_rupees"].sum()),
        "brokerage": float(executed["brokerage_rupees"].sum()),
        "stt": float(executed["stt_rupees"].sum()),
        "stamp_duty": float(executed["stamp_duty_rupees"].sum()),
        "exchange_charges": float(executed["exchange_charges_rupees"].sum()),
        "sebi_charges": float(executed["sebi_charges_rupees"].sum()),
        "ipft": float(executed["ipft_rupees"].sum()),
        "gst": float(executed["gst_rupees"].sum()),
        "slippage": float(executed["slippage_rupees"].sum()),
        "base_slippage": float(executed["base_slippage_rupees"].sum()),
        "ladder_impact": float(executed["ladder_impact_rupees"].sum()),
        "volume_impact": float(executed["volume_impact_rupees"].sum()),
        "oi_impact": float(executed["oi_impact_rupees"].sum()),
        "impact": float(executed["impact_rupees"].sum()),
        "turnover": float(executed["turnover_rupees"].sum()),
        "turnover_on_initial_capital": float(executed["turnover_rupees"].sum())
        / initial_capital,
        "cost_to_gross_abs": float(executed["total_cost_rupees"].sum())
        / max(abs(float(executed["gross_pnl_rupees"].sum())), 1e-12),
        "win_rate": float((executed["net_pnl_rupees"] > 0).mean()) if len(executed) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() < 0 else None,
        "average_net_trade": float(executed["net_pnl_rupees"].mean()),
        "median_net_trade": float(executed["net_pnl_rupees"].median()),
        "best_trade": float(executed["net_pnl_rupees"].max()),
        "worst_trade": float(executed["net_pnl_rupees"].min()),
        "p05_trade": float(executed["net_pnl_rupees"].quantile(0.05)),
        "cvar05_trade": float(worst_tail["net_pnl_rupees"].mean()),
        "maximum_losing_streak": _max_losing_streak(executed["net_pnl_rupees"]),
        "max_drawdown_rupees": max_drawdown,
        "max_drawdown_pct": max_drawdown_pct,
        "max_drawdown_business_days": _max_drawdown_duration(curve),
        "drawdown_peak_date": drawdown_episode["peak_date"],
        "drawdown_trough_date": drawdown_episode["trough_date"],
        "drawdown_recovery_date": drawdown_episode["recovery_date"],
        **monthly,
        "average_lots": float(executed["lots"].mean()),
        "maximum_lots": int(executed["lots"].max()),
        "average_span_margin": float(executed["margin_rupees"].mean()),
        "maximum_span_margin": float(executed["margin_rupees"].max()),
        "average_margin_utilization": float(executed["margin_utilization"].mean()),
        "maximum_margin_utilization": float(executed["margin_utilization"].max()),
        "average_max_loss_utilization": float(executed["max_loss_utilization"].mean()),
        "maximum_max_loss_utilization": float(executed["max_loss_utilization"].max()),
    }


def build_policy_summary(
    portfolios: pd.DataFrame,
    initial_capital: float,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    details: dict[str, Any] = {}
    for (structure, policy), part in portfolios.groupby(["structure", "policy"], sort=True):
        metrics = summarize_portfolio(part, initial_capital)
        details[f"{structure}__{policy}"] = metrics
        rows.append({"structure": structure, "policy": policy, **metrics})
        for split, split_part in part.groupby("split", sort=True):
            executed = split_part.loc[split_part["executed"]]
            rows.append(
                {
                    "structure": structure,
                    "policy": f"{policy}__{split}",
                    "eligible_signals": len(split_part),
                    "executed_trades": len(executed),
                    "gross_pnl": executed["gross_pnl_rupees"].sum(),
                    "total_cost": executed["total_cost_rupees"].sum(),
                    "net_profit": executed["net_pnl_rupees"].sum(),
                    "average_net_trade": executed["net_pnl_rupees"].mean(),
                    "win_rate": (executed["net_pnl_rupees"] > 0).mean(),
                    "turnover": executed["turnover_rupees"].sum(),
                    "average_lots": executed["lots"].mean(),
                }
            )
    return pd.DataFrame(rows), details


def choose_primary_structure(portfolios: pd.DataFrame) -> tuple[str, pd.DataFrame]:
    balanced_discovery = portfolios.loc[
        portfolios["policy"].eq("balanced")
        & portfolios["split"].eq("discovery_2021_2023")
        & portfolios["executed"]
    ]
    ranking = (
        balanced_discovery.groupby("structure", as_index=False)
        .agg(
            discovery_net=("net_pnl_rupees", "sum"),
            discovery_mean_net=("net_pnl_rupees", "mean"),
            discovery_win_rate=("net_pnl_rupees", lambda values: float((values > 0).mean())),
            discovery_turnover=("turnover_rupees", "sum"),
            discovery_trades=("trade_id", "size"),
        )
        .sort_values(["discovery_net", "discovery_mean_net"], ascending=False)
    )
    return str(ranking.iloc[0]["structure"]), ranking


def _bucket_tertiles(
    values: pd.Series,
    low: float,
    high: float,
    labels: tuple[str, str, str],
) -> pd.Series:
    return pd.cut(values, [-np.inf, low, high, np.inf], labels=labels, include_lowest=True)


def build_regime_summary(
    primary: pd.DataFrame,
    events: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, tuple[float, float]]]:
    duplicate_columns = {
        "split",
        "trade_date",
        "entry_ts",
        "entry_dte",
        "span_time_slot",
    }
    executed = primary.loc[primary["executed"]].merge(
        events.drop(columns=list(duplicate_columns), errors="ignore"),
        on="trade_id",
        how="left",
    )
    discovery_events = events.loc[events["split"].eq("discovery_2021_2023") & events["gate_pass"]]
    threshold_columns = ["atm_iv", "trailing_rv_act365", "entry_dte", "gate_cushion"]
    thresholds = {
        column: (
            float(discovery_events[column].quantile(1 / 3)),
            float(discovery_events[column].quantile(2 / 3)),
        )
        for column in threshold_columns
    }
    executed["iv_regime"] = _bucket_tertiles(
        executed["atm_iv"], *thresholds["atm_iv"], ("low_iv", "mid_iv", "high_iv")
    )
    executed["rv_regime"] = _bucket_tertiles(
        executed["trailing_rv_act365"],
        *thresholds["trailing_rv_act365"],
        ("low_rv", "mid_rv", "high_rv"),
    )
    executed["dte_regime"] = _bucket_tertiles(
        executed["entry_dte"], *thresholds["entry_dte"], ("low_dte", "mid_dte", "high_dte")
    )
    executed["gate_cushion_regime"] = _bucket_tertiles(
        executed["gate_cushion"],
        *thresholds["gate_cushion"],
        ("thin_cushion", "mid_cushion", "wide_cushion"),
    )
    executed["vrp_tail_regime"] = pd.cut(
        executed["vrp_q5"],
        [-np.inf, 0.90, 0.95, np.inf],
        labels=("q85_q90", "q90_q95", "q95_plus"),
    )
    executed["entry_time_regime"] = pd.cut(
        executed["minute_of_day"],
        [-np.inf, 11 * 60, 13 * 60, np.inf],
        labels=("before_1100", "1100_1300", "after_1300"),
    )
    executed["spot_trend_regime"] = np.select(
        [executed["spot_return_15m"] < -0.001, executed["spot_return_15m"] > 0.001],
        ["down_10bp_plus", "up_10bp_plus"],
        default="inside_10bp",
    )
    executed["iv_rv_relation"] = np.where(
        executed["atm_iv"] >= executed["trailing_rv_act365"], "iv_ge_rv", "iv_lt_rv"
    )
    dimensions = [
        "split",
        "year",
        "iv_regime",
        "rv_regime",
        "dte_regime",
        "gate_cushion_regime",
        "vrp_tail_regime",
        "entry_time_regime",
        "spot_trend_regime",
        "iv_rv_relation",
        "span_time_slot",
    ]
    rows: list[dict[str, Any]] = []
    for dimension in dimensions:
        for regime, part in executed.groupby(dimension, observed=True, sort=True):
            rows.append(
                {
                    "dimension": dimension,
                    "regime": str(regime),
                    "trades": int(len(part)),
                    "gross_pnl": float(part["gross_pnl_rupees"].sum()),
                    "total_cost": float(part["total_cost_rupees"].sum()),
                    "net_pnl": float(part["net_pnl_rupees"].sum()),
                    "mean_net": float(part["net_pnl_rupees"].mean()),
                    "median_net": float(part["net_pnl_rupees"].median()),
                    "win_rate": float((part["net_pnl_rupees"] > 0).mean()),
                    "turnover": float(part["turnover_rupees"].sum()),
                    "average_lots": float(part["lots"].mean()),
                    "worst_trade": float(part["net_pnl_rupees"].min()),
                }
            )
    return pd.DataFrame(rows), thresholds


def render_report(tearsheet: dict[str, Any]) -> str:
    primary = tearsheet["primary"]
    caps = tearsheet["capacity_caps"]
    thresholds = tearsheet["gate_thresholds"]
    split_rows = tearsheet["primary_split_summary"]
    regimes = tearsheet["primary_regime_highlights"]
    policy_details = tearsheet["policy_details"]
    lines = [
        "# Phase 8 — ₹10 lakh gated VRP capital backtest",
        "",
        "## Decision",
        "",
        f"The discovery-selected candidate is **{tearsheet['primary_structure']}** under the balanced ",
        "50%-of-equity SPAN budget and 2%-of-equity defined maximum-loss budget. This is a ",
        "capital-aware historical diagnostic, not a clean out-of-sample claim: the gate family was ",
        "chosen after inspecting calendar stability even though its numeric thresholds are fitted only ",
        "on 2021–2023.",
        "",
        "## Frozen contract",
        "",
        "- Initial capital: ₹10,00,000.",
        "- Signal: first daily upper-85 VRP percentile upward crossing.",
        "- Gate: all three causal IV/RV conditions must pass.",
        "- Holding period: fixed 60 minutes; nearest weekly expiry; entry legs inside ATM±3.",
        "- Primary sizing: min(discovery capacity cap, 50% SPAN-margin cap, 2% maximum-loss cap).",
        "- Execution: date-aware Groww charges, pinned base slippage, corrected 60-lot-parity ",
        "  ladder, and separate square-root volume/OI impact.",
        "- Margin: timestamp-aware joined SPAN slot at entry; multi-lot margin scales linearly for the ",
        "  identical defined-risk basket.",
        "- No overlapping positions, compounding occurs only after the 60-minute exit, and there is no ",
        "  intratrade stop or mark-to-market margin-call simulation.",
        "",
        "## Gate and capacity",
        "",
        f"- IV change 5m > {thresholds['iv_change_5m'] * 100:.6f} vol points "
        f"(decimal {thresholds['iv_change_5m']:.8f}).",
        f"- IV change 15m > {thresholds['iv_change_15m'] * 100:.6f} vol points "
        f"(decimal {thresholds['iv_change_15m']:.8f}).",
        f"- RV change 5m > {thresholds['rv_change_5m']:.8f}.",
        f"- Discovery capacity caps: fly {caps['short_iron_fly']} lots, condor ",
        f"  {caps['short_iron_condor']} lots, put butterfly {caps['long_put_butterfly']} lots.",
        "",
        "## Primary tear sheet",
        "",
        "| Metric | Result |",
        "|---|---:|",
        f"| Executed trades | {primary['executed_trades']} |",
        f"| Final equity | ₹{primary['final_equity']:,.2f} |",
        f"| Net profit | ₹{primary['net_profit']:,.2f} |",
        f"| Total return | {primary['total_return']:.2%} |",
        f"| CAGR | {primary['cagr']:.2%} |",
        f"| Sharpe, zero rate | {primary['sharpe_zero_rate']:.3f} |",
        f"| Gross P&L | ₹{primary['gross_pnl']:,.2f} |",
        f"| Total costs | ₹{primary['total_cost']:,.2f} |",
        f"| Charges / slippage | ₹{primary['charges']:,.2f} / ₹{primary['slippage']:,.2f} |",
        f"| Base slippage / added impact | ₹{primary['base_slippage']:,.2f} / ₹{primary['impact']:,.2f} |",
        f"| Turnover | ₹{primary['turnover']:,.2f} |",
        f"| Turnover / starting capital | {primary['turnover_on_initial_capital']:.2f}× |",
        f"| Win rate | {primary['win_rate']:.2%} |",
        f"| Profit factor | {primary['profit_factor']:.3f} |",
        f"| Maximum drawdown | −₹{abs(primary['max_drawdown_rupees']):,.2f} "
        f"({primary['max_drawdown_pct']:.2%}) |",
        f"| Drawdown peak → trough | {primary['drawdown_peak_date']} → "
        f"{primary['drawdown_trough_date']} |",
        f"| Drawdown recovery | {primary['drawdown_recovery_date'] or 'not recovered by sample end'} |",
        f"| Positive active months | {primary['positive_months']} / {primary['active_months']} "
        f"({primary['positive_month_rate']:.2%}) |",
        f"| CVaR 5% per trade | ₹{primary['cvar05_trade']:,.2f} |",
        f"| Average lots | {primary['average_lots']:.2f} |",
        f"| Average / maximum entry SPAN | ₹{primary['average_span_margin']:,.2f} / "
        f"₹{primary['maximum_span_margin']:,.2f} |",
        f"| Maximum margin utilization | {primary['maximum_margin_utilization']:.2%} |",
        f"| Maximum defined-loss utilization | {primary['maximum_max_loss_utilization']:.2%} |",
        "",
        "## Calendar slices",
        "",
        "| Split | Trades | Gross | Costs | Net | Mean net | Win rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in split_rows:
        lines.append(
            f"| {row['split']} | {row['trades']} | ₹{row['gross']:,.2f} | "
            f"₹{row['cost']:,.2f} | ₹{row['net']:,.2f} | ₹{row['mean_net']:,.2f} | "
            f"{row['win_rate']:.2%} |"
        )
    lines.extend(
        [
            "",
            "## Capital-policy sensitivity",
            "",
            "| Structure | Policy | Return | Net P&L | Max drawdown | Average lots |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for structure in ("short_iron_fly", "short_iron_condor", "long_put_butterfly"):
        for policy in ("conservative", "balanced", "growth", "margin_only"):
            row = policy_details[f"{structure}__{policy}"]
            lines.append(
                f"| {structure} | {policy} | {row['total_return']:.2%} | "
                f"₹{row['net_profit']:,.2f} | {row['max_drawdown_pct']:.2%} | "
                f"{row['average_lots']:.2f} |"
            )
    lines.extend(
        [
            "",
            "The margin-only rows are leverage stress tests. They are not deployable recommendations because",
            "the archive cannot simulate intratrade SPAN expansion, margin calls, or forced liquidation, and",
            "the gate is not clean OOS evidence.",
            "",
            "## Full cost decomposition",
            "",
            "| Component | Rupees | Share of total cost |",
            "|---|---:|---:|",
            f"| Brokerage | ₹{primary['brokerage']:,.2f} | {primary['brokerage'] / primary['total_cost']:.2%} |",
            f"| STT | ₹{primary['stt']:,.2f} | {primary['stt'] / primary['total_cost']:.2%} |",
            f"| Exchange charges | ₹{primary['exchange_charges']:,.2f} | {primary['exchange_charges'] / primary['total_cost']:.2%} |",
            f"| GST | ₹{primary['gst']:,.2f} | {primary['gst'] / primary['total_cost']:.2%} |",
            f"| Stamp duty | ₹{primary['stamp_duty']:,.2f} | {primary['stamp_duty'] / primary['total_cost']:.2%} |",
            f"| SEBI + IPFT | ₹{primary['sebi_charges'] + primary['ipft']:,.2f} | {(primary['sebi_charges'] + primary['ipft']) / primary['total_cost']:.2%} |",
            f"| Base slippage | ₹{primary['base_slippage']:,.2f} | {primary['base_slippage'] / primary['total_cost']:.2%} |",
            f"| Lot-ladder impact | ₹{primary['ladder_impact']:,.2f} | {primary['ladder_impact'] / primary['total_cost']:.2%} |",
            f"| Volume impact | ₹{primary['volume_impact']:,.2f} | {primary['volume_impact'] / primary['total_cost']:.2%} |",
            f"| OI impact | ₹{primary['oi_impact']:,.2f} | {primary['oi_impact'] / primary['total_cost']:.2%} |",
            "",
            "## Descriptive regime leads",
            "",
            "These are diagnostics, not permission to add another gate without a new holdout.",
            "",
            "| Dimension | Regime | Trades | Net P&L | Mean/trade | Win rate |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for row in regimes:
        lines.append(
            f"| {row['dimension']} | {row['regime']} | {row['trades']} | "
            f"₹{row['net_pnl']:,.2f} | ₹{row['mean_net']:,.2f} | {row['win_rate']:.2%} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation boundaries",
            "",
            "- The 2024 and 2025–2026 labels are calendar holdouts for sizing and numeric thresholds, ",
            "  but not pristine OOS evidence because the gate concept was retained after inspecting them.",
            "- SPAN is evaluated at entry only. Intratrade margin expansion and forced-liquidation paths ",
            "  are not present in the dataset.",
            "- Historical close, volume, and OI replace observed bid/ask and order-book depth; impact is an ",
            "  auditable sensitivity rather than fill-calibrated market truth.",
            "- Regime cells are descriptive and are not additional entry filters.",
            "",
            "## Reproduce",
            "",
            "```powershell",
            "python -m research.phase8.run_gated_capital_backtest",
            "python -m pytest tests/test_phase8_gated_capital.py -q",
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def run(
    *,
    observation_path: Path,
    phase4_tradebook_path: Path,
    curve_path: Path,
    surface_path: Path,
    event_path: Path,
    capacity_surface_path: Path,
    portfolio_path: Path,
    policy_summary_path: Path,
    equity_path: Path,
    regime_path: Path,
    tearsheet_path: Path,
    report_path: Path,
    manifest_path: Path,
    initial_capital: float = INITIAL_CAPITAL,
) -> dict[str, Any]:
    observations = pd.read_parquet(observation_path)
    observations["entry_ts"] = pd.to_datetime(observations["entry_ts"], utc=True)
    phase4_tradebook = pd.read_csv(phase4_tradebook_path)
    features = build_event_features(curve_path, surface_path)
    events, gate_thresholds = build_gated_events(observations, features)
    surface = build_cost_surface(observations, phase4_tradebook, events)
    capacity_summary, caps = summarize_capacity(surface)
    portfolios = simulate_portfolios(surface, events, caps, initial_capital=initial_capital)
    policy_summary, policy_details = build_policy_summary(portfolios, initial_capital)
    primary_structure, ranking = choose_primary_structure(portfolios)
    primary = portfolios.loc[
        portfolios["structure"].eq(primary_structure) & portfolios["policy"].eq("balanced")
    ].copy()
    equity = _daily_curve(primary, initial_capital)
    regimes, regime_thresholds = build_regime_summary(primary, events)
    primary_metrics = policy_details[f"{primary_structure}__balanced"]
    primary_split_summary = []
    split_order = ["discovery_2021_2023", "validation_2024", "confirmation_2025_2026"]
    for split in split_order:
        part = primary.loc[primary["split"].eq(split)]
        executed = part.loc[part["executed"]]
        primary_split_summary.append(
            {
                "split": str(split),
                "trades": int(len(executed)),
                "gross": float(executed["gross_pnl_rupees"].sum()),
                "cost": float(executed["total_cost_rupees"].sum()),
                "net": float(executed["net_pnl_rupees"].sum()),
                "mean_net": float(executed["net_pnl_rupees"].mean()),
                "win_rate": float((executed["net_pnl_rupees"] > 0).mean()),
            }
        )
    highlight_keys = {
        ("gate_cushion_regime", "thin_cushion"),
        ("gate_cushion_regime", "wide_cushion"),
        ("iv_regime", "low_iv"),
        ("iv_regime", "high_iv"),
        ("rv_regime", "low_rv"),
        ("rv_regime", "high_rv"),
        ("dte_regime", "low_dte"),
        ("dte_regime", "high_dte"),
        ("entry_time_regime", "1100_1300"),
        ("entry_time_regime", "after_1300"),
    }
    regime_highlights = [
        row
        for row in regimes.to_dict(orient="records")
        if (row["dimension"], row["regime"]) in highlight_keys
    ]
    tearsheet = {
        "schema_version": SCHEMA_VERSION,
        "initial_capital": initial_capital,
        "gate_thresholds": gate_thresholds,
        "gate_counts": events.groupby(["split", "gate_pass"]).size().to_dict(),
        "capacity_caps": caps,
        "primary_selection_rule": (
            "highest total discovery net P&L among the three structures under the balanced policy"
        ),
        "primary_structure": primary_structure,
        "primary": primary_metrics,
        "primary_split_summary": primary_split_summary,
        "primary_regime_highlights": regime_highlights,
        "discovery_ranking": ranking.to_dict(orient="records"),
        "policy_details": policy_details,
        "policies": [asdict(policy) for policy in POLICIES],
        "regime_thresholds": regime_thresholds,
        "limitations": [
            "Gate family is post-hoc even though numeric thresholds and sizing caps use only 2021-2023.",
            "No intratrade SPAN expansion, margin call, stop, or forced liquidation is simulated.",
            "Impact uses close, minute volume and OI, not observed bid/ask or order-book fills.",
            "Regime analysis is descriptive and not an additional trading rule.",
        ],
    }
    for path in (
        event_path,
        capacity_surface_path,
        portfolio_path,
        policy_summary_path,
        equity_path,
        regime_path,
        tearsheet_path,
        report_path,
        manifest_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    events.to_csv(event_path, index=False)
    capacity_summary.to_csv(capacity_surface_path, index=False)
    portfolios.to_csv(portfolio_path, index=False)
    policy_summary.to_csv(policy_summary_path, index=False)
    equity.to_csv(equity_path, index=False)
    regimes.to_csv(regime_path, index=False)
    tearsheet_path.write_text(
        json.dumps(_json_safe(tearsheet), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    report = "\n".join(line.rstrip() for line in render_report(tearsheet).splitlines()) + "\n"
    report_path.write_text(report, encoding="utf-8", newline="\n")
    outputs = [
        event_path,
        capacity_surface_path,
        portfolio_path,
        policy_summary_path,
        equity_path,
        regime_path,
        tearsheet_path,
        report_path,
    ]
    manifest = {
        "schema_version": "phase8-gated-capital-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(path.resolve()), "sha256": _sha256(path)}
            for path in (observation_path, phase4_tradebook_path, curve_path, surface_path)
        ],
        "outputs": [{"path": str(path.resolve()), "sha256": _sha256(path)} for path in outputs],
        "impact_model": {
            "name": "additive_ladder_participation_v2",
            "parameters": asdict(CAPACITY_IMPACT_PARAMETERS),
        },
        "initial_capital": initial_capital,
        "primary_structure": primary_structure,
    }
    manifest_path.write_text(
        json.dumps(_json_safe(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return tearsheet


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--observations",
        type=Path,
        default=Path("audit/phase4_cost_aware_observations.parquet"),
    )
    parser.add_argument(
        "--phase4-tradebook",
        type=Path,
        default=Path("audit/phase4_cost_aware_tradebook.csv"),
    )
    parser.add_argument(
        "--curve", type=Path, default=Path("audit/phase2_vrp_session_curve_features.parquet")
    )
    parser.add_argument(
        "--surface", type=Path, default=Path("audit/phase2_intraday_iv_surface.parquet")
    )
    parser.add_argument("--events", type=Path, default=Path("audit/phase8_gated_events.csv"))
    parser.add_argument(
        "--capacity",
        type=Path,
        default=Path("audit/phase8_gated_capacity_surface.csv"),
    )
    parser.add_argument(
        "--portfolio",
        type=Path,
        default=Path("audit/phase8_gated_capital_tradebook.csv"),
    )
    parser.add_argument(
        "--policy-summary",
        type=Path,
        default=Path("audit/phase8_gated_policy_summary.csv"),
    )
    parser.add_argument("--equity", type=Path, default=Path("audit/phase8_gated_equity_curve.csv"))
    parser.add_argument(
        "--regimes", type=Path, default=Path("audit/phase8_gated_regime_summary.csv")
    )
    parser.add_argument("--tearsheet", type=Path, default=Path("audit/phase8_gated_tearsheet.json"))
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("docs/research/PHASE8_10L_GATED_CAPITAL_BACKTEST.md"),
    )
    parser.add_argument("--manifest", type=Path, default=Path("audit/phase8_gated_manifest.json"))
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL)
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = run(
        observation_path=args.observations,
        phase4_tradebook_path=args.phase4_tradebook,
        curve_path=args.curve,
        surface_path=args.surface,
        event_path=args.events,
        capacity_surface_path=args.capacity,
        portfolio_path=args.portfolio,
        policy_summary_path=args.policy_summary,
        equity_path=args.equity,
        regime_path=args.regimes,
        tearsheet_path=args.tearsheet,
        report_path=args.report,
        manifest_path=args.manifest,
        initial_capital=args.initial_capital,
    )
    print(json.dumps(_json_safe(result), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
