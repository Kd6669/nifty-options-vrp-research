"""Relate causal intraday VRP states to frozen-contract defined-risk MTM.

The analysis deliberately separates four ideas that are easy to conflate:

* the level/sign of the corrected ACT/365 variance spread;
* a smoothed zero crossing of that spread;
* the causal same-time-of-day percentile of the spread;
* whether the spread has risen or fallen over the preceding 15 minutes.

All option structures are bounded-risk at entry and all legs start inside the
rolling ATM +/-3 surface.  Exit marks are looked up by exact entry strike and
option type, rather than by following the rolling moneyness label.  Results are
frictionless close-to-close research marks; costs, slippage, and margin are not
applied here.

The source ``WEEK`` / ``expiryCode=1`` payload does not expose a contract expiry.
As in the preceding Phase 2 work, the nearest listed NSE expiry is retained as
an explicitly labelled research proxy.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from .analyze_intraday_volatility import _black76_iv, _causal_percentile
except ImportError:  # Support direct execution by file path.
    from analyze_intraday_volatility import _black76_iv, _causal_percentile


ACT365_SCALE = float(np.sqrt((365 * 24 * 60) / (252 * 375)))
HORIZONS = (15, 60, 120, 180)
RATE_CC = 0.10
LOCAL_OFFSETS = tuple(range(-3, 4))

LEGS: dict[str, tuple[str, str, str]] = {
    "p_m3": ("strike_m3", "put_m3", "PUT"),
    "p_m1": ("strike_m1", "put_m1", "PUT"),
    "p_0": ("strike_0", "put_0", "PUT"),
    "p_p3": ("strike_p3", "put_p3", "PUT"),
    "c_m3": ("strike_m3", "call_m3", "CALL"),
    "c_0": ("strike_0", "call_0", "CALL"),
    "c_p1": ("strike_p1", "call_p1", "CALL"),
    "c_p3": ("strike_p3", "call_p3", "CALL"),
}

STRUCTURES: dict[str, dict[str, float]] = {
    "short_iron_condor": {"p_m3": 1, "p_m1": -1, "c_p1": -1, "c_p3": 1},
    "long_iron_condor": {"p_m3": -1, "p_m1": 1, "c_p1": 1, "c_p3": -1},
    "short_iron_fly": {"p_m3": 1, "p_0": -1, "c_0": -1, "c_p3": 1},
    "long_iron_fly": {"p_m3": -1, "p_0": 1, "c_0": 1, "c_p3": -1},
    "bull_call_spread": {"c_p1": 1, "c_p3": -1},
    "bear_put_spread": {"p_m1": 1, "p_m3": -1},
    "long_call_butterfly": {"c_m3": 1, "c_0": -2, "c_p3": 1},
    "long_put_butterfly": {"p_m3": 1, "p_0": -2, "p_p3": 1},
}

STATE_DIMENSIONS = (
    "all",
    "vrp_sign",
    "vrp_tail",
    "vrp_direction",
    "vrp_crossing",
    "sign_x_direction",
    "tail_x_direction",
    "iv_tail",
    "rv_tail",
    "iv_rv_joint",
)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    if clean.empty:
        return {"count": 0}
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "std": float(clean.std()),
        "p01": float(clean.quantile(0.01)),
        "p05": float(clean.quantile(0.05)),
        "p10": float(clean.quantile(0.10)),
        "p25": float(clean.quantile(0.25)),
        "median": float(clean.median()),
        "p75": float(clean.quantile(0.75)),
        "p90": float(clean.quantile(0.90)),
        "p95": float(clean.quantile(0.95)),
        "p99": float(clean.quantile(0.99)),
    }


def _safe_median(values: pd.Series) -> float:
    clean = values.replace([np.inf, -np.inf], np.nan).dropna()
    return float(clean.median()) if not clean.empty else float("nan")


def _tail(percentile: pd.Series) -> pd.Series:
    return pd.Series(
        np.select(
            [percentile <= 0.10, percentile >= 0.90],
            ["lower_10", "upper_10"],
            default="middle_80",
        ),
        index=percentile.index,
    ).mask(percentile.isna(), "unranked")


def _exact_lag(
    frame: pd.DataFrame,
    column: str,
    periods: int,
) -> pd.Series:
    grouped = frame.groupby("trade_date", sort=False)
    lagged = grouped[column].shift(periods)
    lagged_ts = grouped["timestamp_ist"].shift(periods)
    elapsed = (frame["timestamp_ist"] - lagged_ts).dt.total_seconds() / 60
    return lagged.where(elapsed == periods)


def _build_vrp_states(labels_path: Path, output_path: Path) -> pd.DataFrame:
    labels = pd.read_parquet(
        labels_path,
        filters=[("horizon_minutes", "=", 60)],
    )
    labels = labels.sort_values(["trade_date", "timestamp_ist"]).reset_index(drop=True)
    labels["timestamp_ist"] = pd.to_datetime(labels["timestamp_ist"])
    labels["trade_date"] = labels["trade_date"].astype(str)
    labels["trailing_rv_act365"] = labels["trailing_rv"] * ACT365_SCALE
    labels["forward_rv_act365"] = labels["forward_rv"] * ACT365_SCALE
    labels["signal_vrp_var_act365"] = (
        labels["atm_iv"] ** 2 - labels["trailing_rv_act365"] ** 2
    )
    labels["outcome_vrp_var_act365"] = (
        labels["atm_iv"] ** 2 - labels["forward_rv_act365"] ** 2
    )

    dates = labels["trade_date"].to_numpy()
    times = labels["entry_time"].to_numpy()
    for column, output in (
        ("signal_vrp_var_act365", "vrp_tod_percentile"),
        ("atm_iv", "iv_tod_percentile"),
        ("trailing_rv_act365", "rv_tod_percentile"),
    ):
        labels[output] = _causal_percentile(
            labels[column].to_numpy(dtype=float),
            times,
            dates,
        )

    lag15 = _exact_lag(labels, "signal_vrp_var_act365", 15)
    labels["vrp_delta_15m"] = labels["signal_vrp_var_act365"] - lag15
    labels["vrp_delta_tod_percentile"] = _causal_percentile(
        labels["vrp_delta_15m"].to_numpy(dtype=float),
        times,
        dates,
    )

    labels["vrp_smooth_5m"] = (
        labels.groupby("trade_date", sort=False)["signal_vrp_var_act365"]
        .rolling(5, min_periods=5)
        .median()
        .reset_index(level=0, drop=True)
    )
    previous_smooth = _exact_lag(labels, "vrp_smooth_5m", 1)
    labels["vrp_crossing"] = np.select(
        [
            (previous_smooth <= 0) & (labels["vrp_smooth_5m"] > 0),
            (previous_smooth >= 0) & (labels["vrp_smooth_5m"] < 0),
        ],
        ["cross_up", "cross_down"],
        default="no_cross",
    )
    labels["vrp_sign"] = np.where(
        labels["signal_vrp_var_act365"] >= 0,
        "positive",
        "negative",
    )
    labels["vrp_direction"] = np.select(
        [labels["vrp_delta_15m"] > 0, labels["vrp_delta_15m"] < 0],
        ["increasing", "decreasing"],
        default="unavailable_or_flat",
    )
    labels["vrp_tail"] = _tail(labels["vrp_tod_percentile"])
    labels["iv_tail"] = _tail(labels["iv_tod_percentile"])
    labels["rv_tail"] = _tail(labels["rv_tod_percentile"])
    labels["sign_x_direction"] = labels["vrp_sign"] + "__" + labels["vrp_direction"]
    labels["tail_x_direction"] = labels["vrp_tail"] + "__" + labels["vrp_direction"]
    labels["iv_rv_joint"] = labels["iv_tail"] + "__" + labels["rv_tail"]
    labels["entry_id"] = np.arange(len(labels), dtype=np.int64)

    keep = [
        "entry_id",
        "timestamp_ist",
        "trade_date",
        "entry_time",
        "session_status",
        "research_dte",
        "spot",
        "atm_iv",
        "atm_call_iv",
        "atm_put_iv",
        "put_wing_iv",
        "call_wing_iv",
        "put_skew",
        "call_skew",
        "risk_reversal",
        "smile_curvature",
        "trailing_rv_act365",
        "forward_rv_act365",
        "signal_vrp_var_act365",
        "outcome_vrp_var_act365",
        "vrp_delta_15m",
        "vrp_smooth_5m",
        "vrp_tod_percentile",
        "iv_tod_percentile",
        "rv_tod_percentile",
        "vrp_delta_tod_percentile",
        "vrp_sign",
        "vrp_tail",
        "vrp_direction",
        "vrp_crossing",
        "sign_x_direction",
        "tail_x_direction",
        "iv_tail",
        "rv_tail",
        "iv_rv_joint",
    ]
    states = labels[keep].copy()
    states.to_parquet(output_path, index=False)
    return states


def _local_chain_query(
    gold_glob: str,
    surface_path: Path,
    expiry_year: int,
) -> pd.DataFrame:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    query = """
        SELECT
          source.timestamp_ist,
          source.trade_date,
          surface.entry_time,
          surface.nearest_expiry,
          surface.research_t_years,
          surface.synthetic_forward,
          surface.atm_iv,
          try_cast(source.computed_moneyness_offset AS INTEGER) AS entry_offset,
          cast(source.strike AS DOUBLE) AS strike,
          source.option_type,
          cast(source.close AS DOUBLE) AS close,
          cast(source.provider_iv_raw AS DOUBLE) / 100.0 AS provider_iv,
          cast(source.volume AS DOUBLE) AS volume,
          cast(source.open_interest AS DOUBLE) AS open_interest
        FROM read_parquet(?, hive_partitioning=true) AS source
        JOIN read_parquet(?) AS surface USING (timestamp_ist)
        WHERE
          source.expiry_flag = 'WEEK'
          AND try_cast(source.computed_moneyness_offset AS INTEGER) BETWEEN -3 AND 3
          AND source.close IS NOT NULL
          AND source.close >= 0
          AND source.strike_ladder_valid
          AND NOT source.quality_severe_anomaly
          AND NOT source.proven_severe_payload_corruption
          AND year(surface.nearest_expiry) = ?
        ORDER BY surface.nearest_expiry, source.strike, source.option_type,
          source.timestamp_ist
    """
    frame = connection.execute(
        query,
        [gold_glob, str(surface_path), expiry_year],
    ).fetchdf()
    connection.close()
    return frame


def _build_local_chain(
    gold_glob: str,
    surface_path: Path,
    output_path: Path,
) -> None:
    writer: pq.ParquetWriter | None = None
    try:
        for expiry_year in range(2021, 2028):
            frame = _local_chain_query(gold_glob, surface_path, expiry_year)
            if frame.empty:
                continue
            frame["timestamp_ist"] = pd.to_datetime(frame["timestamp_ist"])
            frame["nearest_expiry"] = pd.to_datetime(frame["nearest_expiry"])
            is_call = frame["option_type"].eq("CALL").to_numpy()
            frame["research_iv"] = _black76_iv(
                frame["synthetic_forward"].to_numpy(dtype=float),
                frame["strike"].to_numpy(dtype=float),
                frame["close"].to_numpy(dtype=float),
                frame["research_t_years"].to_numpy(dtype=float),
                is_call,
            )
            frame["relative_iv_to_atm"] = frame["research_iv"] - frame["atm_iv"]
            frame["relative_iv_ratio_to_atm"] = frame["research_iv"] / frame["atm_iv"] - 1
            frame["local_iv_rank"] = frame.groupby("timestamp_ist", sort=False)[
                "research_iv"
            ].rank(method="average", pct=True)
            contract = ["nearest_expiry", "strike", "option_type"]
            frame["contract_lifetime_iv_rank"] = frame.groupby(contract, sort=False)[
                "research_iv"
            ].rank(method="average", pct=True)
            grouped = frame.groupby(contract, sort=False)
            for horizon in (15, 60):
                future_iv = grouped["research_iv"].shift(-horizon)
                future_ts = grouped["timestamp_ist"].shift(-horizon)
                elapsed = (future_ts - frame["timestamp_ist"]).dt.total_seconds() / 60
                frame[f"contract_iv_change_{horizon}m"] = (
                    future_iv - frame["research_iv"]
                ).where(elapsed == horizon)

            keep = [
                "timestamp_ist",
                "trade_date",
                "entry_time",
                "nearest_expiry",
                "entry_offset",
                "strike",
                "option_type",
                "close",
                "research_iv",
                "provider_iv",
                "atm_iv",
                "relative_iv_to_atm",
                "relative_iv_ratio_to_atm",
                "local_iv_rank",
                "contract_lifetime_iv_rank",
                "contract_iv_change_15m",
                "contract_iv_change_60m",
                "volume",
                "open_interest",
            ]
            table = pa.Table.from_pandas(frame[keep], preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()


def _local_chain_summary(path: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    by_leg = connection.execute(
        """
        SELECT
          entry_offset,
          option_type,
          count(research_iv) AS observations,
          count(DISTINCT nearest_expiry || '|' || strike || '|' || option_type)
            AS proxy_contracts,
          quantile_cont(research_iv, 0.05) AS iv_p05,
          quantile_cont(research_iv, 0.50) AS iv_median,
          quantile_cont(research_iv, 0.95) AS iv_p95,
          quantile_cont(relative_iv_to_atm, 0.05) AS relative_iv_p05,
          quantile_cont(relative_iv_to_atm, 0.50) AS relative_iv_median,
          quantile_cont(relative_iv_to_atm, 0.95) AS relative_iv_p95,
          quantile_cont(contract_iv_change_15m, 0.50) AS iv_change_15m_median,
          quantile_cont(abs(contract_iv_change_15m), 0.90)
            AS abs_iv_change_15m_p90,
          quantile_cont(contract_iv_change_60m, 0.50) AS iv_change_60m_median,
          quantile_cont(abs(contract_iv_change_60m), 0.90)
            AS abs_iv_change_60m_p90
        FROM read_parquet(?)
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        [str(path)],
    ).fetchdf()
    by_year = connection.execute(
        """
        SELECT
          year(trade_date) AS year,
          entry_offset,
          option_type,
          count(research_iv) AS observations,
          quantile_cont(research_iv, 0.05) AS iv_p05,
          quantile_cont(research_iv, 0.50) AS iv_median,
          quantile_cont(research_iv, 0.95) AS iv_p95,
          quantile_cont(relative_iv_to_atm, 0.50) AS relative_iv_median
        FROM read_parquet(?)
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """,
        [str(path)],
    ).fetchdf()
    contract_summary = connection.execute(
        """
        WITH contracts AS (
          SELECT
            nearest_expiry,
            strike,
            option_type,
            count(research_iv) AS observations,
            quantile_cont(research_iv, 0.05) AS iv_p05,
            quantile_cont(research_iv, 0.50) AS iv_median,
            quantile_cont(research_iv, 0.95) AS iv_p95,
            quantile_cont(research_iv, 0.95) - quantile_cont(research_iv, 0.05)
              AS lifetime_iv_width_90
          FROM read_parquet(?)
          GROUP BY 1, 2, 3
        )
        SELECT
          count(*) AS proxy_contracts,
          quantile_cont(observations, 0.50) AS median_observations,
          quantile_cont(iv_median, 0.05) AS contract_median_iv_p05,
          quantile_cont(iv_median, 0.50) AS contract_median_iv,
          quantile_cont(iv_median, 0.95) AS contract_median_iv_p95,
          quantile_cont(lifetime_iv_width_90, 0.50) AS median_lifetime_iv_width_90,
          quantile_cont(lifetime_iv_width_90, 0.90) AS p90_lifetime_iv_width_90
        FROM contracts
        """,
        [str(path)],
    ).fetchdf()
    total = connection.execute(
        "SELECT count(*) AS rows, count(research_iv) AS solved_iv_rows FROM read_parquet(?)",
        [str(path)],
    ).fetchdf()
    connection.close()
    return {
        "coverage": _records(total)[0],
        "by_leg": _records(by_leg),
        "by_year_and_leg": _records(by_year),
        "proxy_contract_distribution": _records(contract_summary)[0],
    }


def _exit_panel(
    states: pd.DataFrame,
    gold_glob: str,
    surface_path: Path,
    horizon: int,
) -> pd.DataFrame:
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    connection.register("states", states)
    request_parts = []
    for leg, (strike_column, _, option_type) in LEGS.items():
        request_parts.append(
            f"SELECT entry_id, '{leg}' AS leg, "
            f"entry_ts + INTERVAL '{horizon} minutes' AS exit_ts, "
            f"{strike_column} AS strike, '{option_type}' AS option_type FROM entries"
        )
    requests = " UNION ALL ".join(request_parts)
    exit_pivots = ",\n".join(
        f"max(close) FILTER (WHERE leg = '{leg}') AS exit_{leg}"
        for leg in LEGS
    )
    query = f"""
        WITH entries AS (
          SELECT
            states.*,
            surface.timestamp_ist AS entry_ts,
            surface.nearest_expiry,
            surface.synthetic_forward AS entry_forward,
            surface.research_t_years AS entry_t_years,
            surface.strike_m3,
            surface.strike_m1,
            surface.strike_0,
            surface.strike_p1,
            surface.strike_p3,
            surface.call_m3,
            surface.put_m3,
            surface.call_m1,
            surface.put_m1,
            surface.call_0,
            surface.put_0,
            surface.call_p1,
            surface.put_p1,
            surface.call_p3,
            surface.put_p3
          FROM states
          JOIN read_parquet(?) AS surface USING (timestamp_ist)
          WHERE states.session_status = 'regular_session'
            AND states.signal_vrp_var_act365 IS NOT NULL
            AND states.vrp_tod_percentile IS NOT NULL
        ),
        requests AS ({requests}),
        quotes AS (
          SELECT
            timestamp_ist,
            cast(strike AS DOUBLE) AS strike,
            option_type,
            cast(close AS DOUBLE) AS close
          FROM read_parquet(?, hive_partitioning=true)
          WHERE
            expiry_flag = 'WEEK'
            AND try_cast(computed_moneyness_offset AS INTEGER) BETWEEN -10 AND 10
            AND close IS NOT NULL
            AND close >= 0
            AND strike_ladder_valid
            AND NOT quality_severe_anomaly
            AND NOT proven_severe_payload_corruption
        ),
        exits AS (
          SELECT requests.entry_id, requests.leg, quotes.close
          FROM requests
          LEFT JOIN quotes
            ON quotes.timestamp_ist = requests.exit_ts
            AND quotes.strike = requests.strike
            AND quotes.option_type = requests.option_type
        ),
        exit_wide AS (
          SELECT entry_id, {exit_pivots}
          FROM exits
          GROUP BY entry_id
        )
        SELECT
          entries.*,
          exit_surface.spot AS exit_spot,
          exit_surface.atm_iv AS exit_atm_iv,
          exit_surface.synthetic_forward AS exit_forward,
          exit_surface.research_t_years AS exit_t_years,
          exit_wide.* EXCLUDE (entry_id)
        FROM entries
        LEFT JOIN exit_wide USING (entry_id)
        LEFT JOIN read_parquet(?) AS exit_surface
          ON exit_surface.timestamp_ist = entries.entry_ts + INTERVAL '{horizon} minutes'
        ORDER BY entries.entry_id
    """
    frame = connection.execute(
        query,
        [str(surface_path), gold_glob, str(surface_path)],
    ).fetchdf()
    connection.close()
    frame["horizon_minutes"] = horizon
    return frame


def _structure_risk(
    frame: pd.DataFrame,
    positions: dict[str, float],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    size = len(frame)
    entry_value = np.zeros(size)
    valid = np.ones(size, dtype=bool)
    strikes: dict[str, np.ndarray] = {}
    for leg, weight in positions.items():
        strike_column, price_column, _ = LEGS[leg]
        strike = frame[strike_column].to_numpy(dtype=float)
        price = frame[price_column].to_numpy(dtype=float)
        strikes[leg] = strike
        valid &= np.isfinite(strike) & np.isfinite(price)
        entry_value += weight * price

    candidate_spots = [np.zeros(size)]
    candidate_spots.extend(strikes.values())
    stacked_strikes = np.column_stack(list(strikes.values()))
    max_strike = np.max(np.where(np.isfinite(stacked_strikes), stacked_strikes, 0), axis=1)
    candidate_spots.append(max_strike * 2)
    min_pnl = np.full(size, np.inf)
    max_pnl = np.full(size, -np.inf)
    for terminal_spot in candidate_spots:
        payoff = np.zeros(size)
        for leg, weight in positions.items():
            _, _, option_type = LEGS[leg]
            if option_type == "CALL":
                leg_payoff = np.maximum(terminal_spot - strikes[leg], 0)
            else:
                leg_payoff = np.maximum(strikes[leg] - terminal_spot, 0)
            payoff += weight * leg_payoff
        terminal_pnl = payoff - entry_value
        min_pnl = np.minimum(min_pnl, terminal_pnl)
        max_pnl = np.maximum(max_pnl, terminal_pnl)
    max_loss = -min_pnl
    max_profit = max_pnl
    valid &= (max_loss > 0) & (max_profit > 0)
    entry_value[~valid] = np.nan
    max_loss[~valid] = np.nan
    max_profit[~valid] = np.nan
    return entry_value, max_loss, max_profit


def _add_structure_marks(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    frame = frame.copy()
    frame["atm_iv_change"] = frame["exit_atm_iv"] - frame["atm_iv"]
    frame["spot_return"] = frame["exit_spot"] / frame["spot"] - 1
    for leg, (strike_column, price_column, option_type) in LEGS.items():
        frame[f"contract_iv_change_{leg}"] = np.nan
        if horizon == 60:
            is_call = np.full(len(frame), option_type == "CALL", dtype=bool)
            frame[f"entry_iv_{leg}"] = _black76_iv(
                frame["entry_forward"].to_numpy(dtype=float),
                frame[strike_column].to_numpy(dtype=float),
                frame[price_column].to_numpy(dtype=float),
                frame["entry_t_years"].to_numpy(dtype=float),
                is_call,
            )
            frame[f"exit_iv_{leg}"] = _black76_iv(
                frame["exit_forward"].to_numpy(dtype=float),
                frame[strike_column].to_numpy(dtype=float),
                frame[f"exit_{leg}"].to_numpy(dtype=float),
                frame["exit_t_years"].to_numpy(dtype=float),
                is_call,
            )
            frame.loc[:, f"contract_iv_change_{leg}"] = (
                frame[f"exit_iv_{leg}"] - frame[f"entry_iv_{leg}"]
            )

    for name, positions in STRUCTURES.items():
        entry_value, max_loss, max_profit = _structure_risk(frame, positions)
        pnl = np.zeros(len(frame))
        valid_exit = np.ones(len(frame), dtype=bool)
        for leg, weight in positions.items():
            _, price_column, _ = LEGS[leg]
            entry_price = frame[price_column].to_numpy(dtype=float)
            exit_price = frame[f"exit_{leg}"].to_numpy(dtype=float)
            valid_exit &= np.isfinite(entry_price) & np.isfinite(exit_price)
            pnl += weight * (exit_price - entry_price)
        pnl[~valid_exit | ~np.isfinite(max_loss)] = np.nan
        frame[f"{name}__entry_value"] = entry_value
        frame[f"{name}__max_loss"] = max_loss
        frame[f"{name}__max_profit"] = max_profit
        frame[f"{name}__pnl_points"] = pnl
        frame[f"{name}__return_on_max_loss"] = pnl / max_loss
    return frame


def _group_summary(
    frame: pd.DataFrame,
    horizon: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dimension in STATE_DIMENSIONS:
        if dimension == "all":
            groups = [("all", frame)]
        else:
            groups = frame.groupby(dimension, observed=True, sort=True)
        for state, part in groups:
            for structure in STRUCTURES:
                pnl_column = f"{structure}__pnl_points"
                risk_column = f"{structure}__return_on_max_loss"
                eligible = part.dropna(subset=[pnl_column, risk_column])
                if eligible.empty:
                    continue
                pnl = eligible[pnl_column]
                risk_return = eligible[risk_column]
                rows.append(
                    {
                        "horizon_minutes": horizon,
                        "dimension": dimension,
                        "state": str(state),
                        "structure": structure,
                        "observations": int(len(eligible)),
                        "dates": int(eligible["trade_date"].nunique()),
                        "median_atm_iv": _safe_median(eligible["atm_iv"]),
                        "median_trailing_rv_act365": _safe_median(
                            eligible["trailing_rv_act365"]
                        ),
                        "median_signal_vrp_var_act365": _safe_median(
                            eligible["signal_vrp_var_act365"]
                        ),
                        "median_vrp_delta_15m": _safe_median(
                            eligible["vrp_delta_15m"]
                        ),
                        "median_atm_iv_change": _safe_median(eligible["atm_iv_change"]),
                        "mean_pnl_points": float(pnl.mean()),
                        "median_pnl_points": float(pnl.median()),
                        "p05_pnl_points": float(pnl.quantile(0.05)),
                        "p95_pnl_points": float(pnl.quantile(0.95)),
                        "win_rate": float((pnl > 0).mean()),
                        "mean_return_on_max_loss": float(risk_return.mean()),
                        "median_return_on_max_loss": float(risk_return.median()),
                        "p05_return_on_max_loss": float(risk_return.quantile(0.05)),
                        "p95_return_on_max_loss": float(risk_return.quantile(0.95)),
                    }
                )
    return rows


def _contract_iv_change_summary(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for leg in LEGS:
        column = f"contract_iv_change_{leg}"
        if column not in frame:
            continue
        distribution = _distribution(frame[column])
        rows.append({"leg": leg, **distribution})
    return rows


def _detail_columns(frame: pd.DataFrame) -> list[str]:
    base = [
        "entry_id",
        "entry_ts",
        "trade_date",
        "entry_time",
        "horizon_minutes",
        "research_dte",
        "nearest_expiry",
        "spot",
        "exit_spot",
        "spot_return",
        "atm_iv",
        "exit_atm_iv",
        "atm_iv_change",
        "trailing_rv_act365",
        "forward_rv_act365",
        "signal_vrp_var_act365",
        "outcome_vrp_var_act365",
        "vrp_delta_15m",
        "vrp_tod_percentile",
        "iv_tod_percentile",
        "rv_tod_percentile",
        "vrp_sign",
        "vrp_tail",
        "vrp_direction",
        "vrp_crossing",
        "sign_x_direction",
        "tail_x_direction",
        "iv_tail",
        "rv_tail",
        "iv_rv_joint",
    ]
    structure_columns = [
        column
        for column in frame.columns
        if any(column.startswith(f"{name}__") for name in STRUCTURES)
    ]
    contract_iv_columns = [
        column
        for column in frame.columns
        if column.startswith("contract_iv_change_")
    ]
    return base + structure_columns + contract_iv_columns


def analyze(
    gold_root: Path,
    surface_path: Path,
    labels_path: Path,
    output_dir: Path,
    *,
    reuse_local_chain: bool = True,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    gold_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")
    state_path = output_dir / "phase2_vrp_state_60m.parquet"
    local_chain_path = output_dir / "phase2_local_chain_iv.parquet"
    structure_path = output_dir / "phase2_defined_risk_structure_paths.parquet"

    states = _build_vrp_states(labels_path, state_path)
    if not reuse_local_chain or not local_chain_path.exists():
        _build_local_chain(gold_glob, surface_path, local_chain_path)
    local_chain = _local_chain_summary(local_chain_path)

    structure_writer: pq.ParquetWriter | None = None
    summaries: list[dict[str, Any]] = []
    coverage = []
    contract_iv_changes: list[dict[str, Any]] = []
    try:
        for horizon in HORIZONS:
            panel = _exit_panel(states, gold_glob, surface_path, horizon)
            panel = _add_structure_marks(panel, horizon)
            summaries.extend(_group_summary(panel, horizon))
            if horizon == 60:
                contract_iv_changes = _contract_iv_change_summary(panel)
            complete_counts = {
                structure: int(panel[f"{structure}__pnl_points"].notna().sum())
                for structure in STRUCTURES
            }
            coverage.append(
                {
                    "horizon_minutes": horizon,
                    "eligible_state_entries": int(len(panel)),
                    "entries_with_exit_surface": int(panel["exit_atm_iv"].notna().sum()),
                    "structure_complete_counts": complete_counts,
                }
            )
            detail = pa.Table.from_pandas(
                panel[_detail_columns(panel)],
                preserve_index=False,
            )
            if structure_writer is None:
                structure_writer = pq.ParquetWriter(
                    structure_path,
                    detail.schema,
                    compression="zstd",
                )
            structure_writer.write_table(detail)
    finally:
        if structure_writer is not None:
            structure_writer.close()

    state_distributions = {
        column: _distribution(states[column])
        for column in (
            "atm_iv",
            "trailing_rv_act365",
            "forward_rv_act365",
            "signal_vrp_var_act365",
            "outcome_vrp_var_act365",
            "vrp_delta_15m",
            "put_skew",
            "call_skew",
            "risk_reversal",
            "smile_curvature",
        )
    }
    summary_frame = pd.DataFrame(summaries)
    report = {
        "contract": {
            "expiry_identity": (
                "nearest-listed-expiry research proxy; Dhan WEEK expiryCode=1 does not "
                "return the actual contract expiry"
            ),
            "vrp_signal": "ATM IV^2 - trailing 60m ACT/365 RV^2",
            "vrp_percentiles": (
                "causal, same minute of day, prior dates only, minimum 60 observations"
            ),
            "zero_crossing": "sign change in trailing 5-minute median VRP signal",
            "direction": "current VRP signal minus its exact 15-minute lag",
            "tail_definition": "lower <=10th percentile; upper >=90th percentile",
            "structure_entry": "all legs frozen inside rolling ATM +/-3 at entry",
            "structure_exit": "exact entry strike and option type after horizon",
            "marking": "frictionless 1-minute close-to-close; no costs or slippage",
            "normalization": "MTM points divided by theoretical maximum expiry loss at entry",
            "overlap_warning": "minute-grid windows overlap and are descriptive, not independent trades",
        },
        "artifacts": {
            "vrp_states": str(state_path),
            "local_chain_iv": str(local_chain_path),
            "structure_paths": str(structure_path),
        },
        "state_coverage": {
            "rows": int(len(states)),
            "dates": int(states["trade_date"].nunique()),
            "first_timestamp": str(states["timestamp_ist"].min()),
            "last_timestamp": str(states["timestamp_ist"].max()),
        },
        "state_distributions": state_distributions,
        "state_counts": {
            column: {
                str(key): int(value)
                for key, value in states[column].value_counts(dropna=False).items()
            }
            for column in (
                "vrp_sign",
                "vrp_tail",
                "vrp_direction",
                "vrp_crossing",
                "iv_tail",
                "rv_tail",
            )
        },
        "local_chain_iv": local_chain,
        "structure_coverage": coverage,
        "fixed_contract_iv_change_60m": contract_iv_changes,
        "structure_state_summary": _records(summary_frame),
    }
    report_path = output_dir / "phase2_defined_risk_vrp.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument(
        "--surface",
        type=Path,
        default=Path("audit/phase2_intraday_iv_surface.parquet"),
    )
    parser.add_argument(
        "--labels",
        type=Path,
        default=Path("audit/phase2_intraday_rv_vrp_labels.parquet"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("audit"))
    return parser


def main() -> None:
    args = _parser().parse_args()
    report = analyze(
        args.gold_root.resolve(),
        args.surface.resolve(),
        args.labels.resolve(),
        args.output_dir.resolve(),
    )
    print(json.dumps(report["state_coverage"], indent=2))
    print(json.dumps(report["structure_coverage"], indent=2))


if __name__ == "__main__":
    main()
