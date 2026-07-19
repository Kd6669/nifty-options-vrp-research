"""Build an intraday NIFTY IV, skew, RV, and VRP research surface.

The gold dataset's Dhan ``expiryCode=1`` response does not contain an expiry
identity.  Its audited enrichment maps code 1 to the second eligible weekly
contract, but observed prices and provider IVs are much more consistent with
the nearest actual NIFTY expiry.  This script therefore keeps both clocks:

* the audited mapped expiry and its existing BSM IVs, for discrepancy audit;
* a clearly labelled nearest-expiry research proxy, used for parity-adjusted
  Black-76 IV and intraday volatility research.

Forward RV is an ex-post label.  Trailing RV and all causal percentiles use
only information available at the entry timestamp.
"""

from __future__ import annotations

import argparse
from bisect import bisect_right, insort
from collections import defaultdict
import json
import math
from pathlib import Path
from typing import Any

import duckdb
import numba
import numpy as np
import pandas as pd


HORIZONS = (15, 30, 60, 90, 120, 180)
RATE_CC = 0.10
MIN_HISTORY = 60
ANNUAL_MINUTES = 252 * 375


@numba.vectorize([numba.float64(numba.float64)], nopython=True, cache=True)
def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _json_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _nanmedian_columns(columns: list[np.ndarray]) -> np.ndarray:
    values = np.column_stack(columns)
    with np.errstate(all="ignore"):
        return np.nanmedian(values, axis=1)


def _black76_iv(
    forward: np.ndarray,
    strike: np.ndarray,
    price: np.ndarray,
    time_years: np.ndarray,
    is_call: np.ndarray,
) -> np.ndarray:
    """Bounded vector bisection for Black-76 IV under a supplied forward."""

    fwd = np.asarray(forward, dtype=np.float64)
    k = np.asarray(strike, dtype=np.float64)
    observed = np.asarray(price, dtype=np.float64)
    t = np.asarray(time_years, dtype=np.float64)
    call = np.asarray(is_call, dtype=np.bool_)
    discount = np.exp(-RATE_CC * t)
    intrinsic = discount * np.where(call, np.maximum(fwd - k, 0), np.maximum(k - fwd, 0))
    upper = discount * np.where(call, fwd, k)
    valid = (
        np.isfinite(fwd)
        & np.isfinite(k)
        & np.isfinite(observed)
        & np.isfinite(t)
        & (fwd > 0)
        & (k > 0)
        & (t > 0)
        & (observed >= intrinsic - 1.0e-8)
        & (observed <= upper + 1.0e-8)
    )
    low = np.full(len(fwd), 1.0e-4)
    high = np.full(len(fwd), 5.0)
    safe_t = np.maximum(t, 1.0e-12)
    for _ in range(64):
        sigma = (low + high) / 2.0
        sqrt_t = np.sqrt(safe_t)
        d1 = (np.log(np.maximum(fwd, 1.0e-12) / np.maximum(k, 1.0e-12)) + 0.5 * sigma**2 * safe_t) / (
            sigma * sqrt_t
        )
        d2 = d1 - sigma * sqrt_t
        model = np.where(
            call,
            discount * (fwd * _normal_cdf(d1) - k * _normal_cdf(d2)),
            discount * (k * _normal_cdf(-d2) - fwd * _normal_cdf(-d1)),
        )
        below = model < observed
        low = np.where(below, sigma, low)
        high = np.where(below, high, sigma)
    result = (low + high) / 2.0
    result[~valid] = np.nan
    return result


def _causal_percentile(
    values: np.ndarray,
    groups: np.ndarray,
    dates: np.ndarray,
    *,
    minimum_history: int = MIN_HISTORY,
) -> np.ndarray:
    """Rank each observation only against earlier dates in the same group."""

    output = np.full(len(values), np.nan)
    histories: dict[str, list[float]] = defaultdict(list)
    pending: dict[tuple[str, str], list[float]] = defaultdict(list)
    current_date: str | None = None

    def flush(date_key: str | None) -> None:
        if date_key is None:
            return
        for (pending_date, group), group_values in list(pending.items()):
            if pending_date != date_key:
                continue
            history = histories[group]
            for value in group_values:
                insort(history, value)
            del pending[(pending_date, group)]

    for index, (value, group, date_value) in enumerate(zip(values, groups, dates, strict=True)):
        date_key = str(date_value)
        group_key = str(group)
        if current_date is not None and date_key != current_date:
            flush(current_date)
        current_date = date_key
        history = histories[group_key]
        if np.isfinite(value):
            if len(history) >= minimum_history:
                output[index] = bisect_right(history, float(value)) / len(history)
            pending[(date_key, group_key)].append(float(value))
    flush(current_date)
    return output


def _add_realized_volatility(frame: pd.DataFrame) -> dict[int, pd.DataFrame]:
    """Create trailing and forward RV without crossing missing-minute runs."""

    labels: dict[int, list[pd.DataFrame]] = {horizon: [] for horizon in HORIZONS}
    for _, daily in frame.groupby("trade_date", sort=True):
        daily = daily.sort_values("timestamp_ist").copy()
        timestamps = pd.to_datetime(daily["timestamp_ist"])
        spot = daily["spot"].to_numpy(dtype=float)
        one_minute = timestamps.diff().dt.total_seconds().eq(60).to_numpy()
        log_return = np.full(len(daily), np.nan)
        valid_return = one_minute & np.isfinite(spot) & np.isfinite(np.roll(spot, 1))
        valid_return[0] = False
        log_return[valid_return] = np.log(spot[valid_return] / np.roll(spot, 1)[valid_return])
        run_start = ~one_minute
        run_start[0] = True
        run_id = np.cumsum(run_start)

        for horizon in HORIZONS:
            trailing = np.full(len(daily), np.nan)
            forward = np.full(len(daily), np.nan)
            for current_run in np.unique(run_id):
                positions = np.flatnonzero(run_id == current_run)
                if len(positions) <= horizon:
                    continue
                first = positions[0]
                last = positions[-1]
                returns = np.nan_to_num(log_return[first : last + 1], nan=0.0)
                cumulative = np.concatenate(([0.0], np.cumsum(returns**2)))
                local = np.arange(len(positions))
                trailing_ok = local >= horizon
                trailing_positions = local[trailing_ok]
                trailing_sum = (
                    cumulative[trailing_positions + 1]
                    - cumulative[trailing_positions - horizon + 1]
                )
                trailing[first + trailing_positions] = np.sqrt(
                    trailing_sum / horizon * ANNUAL_MINUTES
                )
                forward_ok = local + horizon < len(positions)
                forward_positions = local[forward_ok]
                forward_sum = (
                    cumulative[forward_positions + horizon + 1]
                    - cumulative[forward_positions + 1]
                )
                forward[first + forward_positions] = np.sqrt(
                    forward_sum / horizon * ANNUAL_MINUTES
                )

            selected = daily[
                [
                    "timestamp_ist",
                    "trade_date",
                    "entry_time",
                    "session_status",
                    "research_dte",
                    "spot",
                    "india_vix",
                    "atm_iv",
                    "atm_call_iv",
                    "atm_put_iv",
                    "put_wing_iv",
                    "call_wing_iv",
                    "put_skew",
                    "call_skew",
                    "risk_reversal",
                    "smile_curvature",
                    "atm_ce_pe_gap",
                    "atm_iv_tod_percentile",
                ]
            ].copy()
            selected["horizon_minutes"] = horizon
            selected["trailing_rv"] = trailing
            selected["forward_rv"] = forward
            selected["vrp_signal_var"] = selected["atm_iv"] ** 2 - trailing**2
            selected["expost_vrp_var"] = selected["atm_iv"] ** 2 - forward**2
            selected["expost_vrp_vol"] = selected["atm_iv"] - forward
            labels[horizon].append(selected)
    return {
        horizon: pd.concat(parts, ignore_index=True)
        for horizon, parts in labels.items()
    }


def _distribution(frame: pd.DataFrame, columns: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for column in columns:
        values = frame[column].dropna()
        if values.empty:
            continue
        rows.append(
            {
                "metric": column,
                "count": int(values.count()),
                "mean": float(values.mean()),
                "p01": float(values.quantile(0.01)),
                "p05": float(values.quantile(0.05)),
                "p25": float(values.quantile(0.25)),
                "median": float(values.median()),
                "p75": float(values.quantile(0.75)),
                "p95": float(values.quantile(0.95)),
                "p99": float(values.quantile(0.99)),
            }
        )
    return rows


def _regime_summary(labels: pd.DataFrame, percentile_column: str) -> pd.DataFrame:
    eligible = labels.dropna(subset=[percentile_column, "forward_rv", "atm_iv"]).copy()
    eligible["quintile"] = np.minimum((eligible[percentile_column] * 5).astype(int) + 1, 5)
    return (
        eligible.groupby("quintile", observed=True)
        .agg(
            observations=("expost_vrp_var", "size"),
            median_atm_iv=("atm_iv", "median"),
            median_forward_rv=("forward_rv", "median"),
            mean_expost_vrp_var=("expost_vrp_var", "mean"),
            median_expost_vrp_var=("expost_vrp_var", "median"),
            positive_vrp_rate=("expost_vrp_var", lambda values: float((values > 0).mean())),
        )
        .reset_index()
    )


def analyze(gold_root: Path, expiry_calendar: Path, output_dir: Path) -> dict[str, Any]:
    parquet_glob = str(gold_root / "**" / "*.parquet").replace("\\", "/")
    output_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    connection.execute("PRAGMA threads=8")
    connection.execute("PRAGMA memory_limit='12GB'")
    query = """
        WITH expiry_calendar AS (
          SELECT
            try_cast(actual_expiry_date AS DATE) AS expiry_date,
            try_cast(actual_expiry_timestamp_ist AS TIMESTAMPTZ) AS expiry_timestamp,
            expiry_type
          FROM read_parquet(?)
          WHERE mapping_status = 'proven'
        ),
        source_dates AS (
          SELECT DISTINCT trade_date
          FROM read_parquet(?, hive_partitioning=true)
          WHERE expiry_flag = 'WEEK'
        ),
        expiry_candidates AS (
          SELECT
            trade_date,
            (SELECT min(expiry_timestamp) FROM expiry_calendar
             WHERE expiry_date >= trade_date) AS nearest_expiry,
            (SELECT min(expiry_timestamp) FROM expiry_calendar
             WHERE expiry_date >= trade_date AND expiry_type = 'weekly') AS nearest_weekly_expiry
          FROM source_dates
        ),
        clean AS (
          SELECT
            source.timestamp_ist,
            source.trade_date,
            source.session_status,
            source.actual_expiry_timestamp_ist AS mapped_expiry,
            candidate.nearest_expiry,
            candidate.nearest_weekly_expiry,
            try_cast(source.computed_moneyness_offset AS INTEGER) AS entry_offset,
            cast(source.strike AS DOUBLE) AS strike,
            source.option_type,
            cast(source.close AS DOUBLE) AS close,
            cast(source.provider_iv_raw AS DOUBLE) AS provider_iv_raw,
            cast(source.bsm_iv_close AS DOUBLE) AS mapped_bsm_iv,
            source.bsm_status,
            cast(source.independent_nifty_spot AS DOUBLE) AS spot,
            cast(source.india_vix AS DOUBLE) AS india_vix,
            count(*) OVER (
              PARTITION BY source.trade_date, source.timestamp_ist,
                source.actual_expiry_date, source.strike, source.option_type
            ) AS identity_rows
          FROM read_parquet(?, hive_partitioning=true) AS source
          JOIN expiry_candidates AS candidate USING (trade_date)
          WHERE
            source.expiry_flag = 'WEEK'
            AND try_cast(source.computed_moneyness_offset AS INTEGER) BETWEEN -3 AND 3
            AND source.close IS NOT NULL
            AND source.close >= 0
            AND source.strike_ladder_valid
            AND NOT source.quality_severe_anomaly
            AND NOT source.proven_severe_payload_corruption
        ),
        unique_rows AS (
          SELECT * EXCLUDE identity_rows
          FROM clean
          WHERE identity_rows = 1
        )
        SELECT
          timestamp_ist,
          trade_date,
          any_value(session_status) AS session_status,
          any_value(mapped_expiry) AS mapped_expiry,
          any_value(nearest_expiry) AS nearest_expiry,
          any_value(nearest_weekly_expiry) AS nearest_weekly_expiry,
          median(spot) AS spot,
          median(india_vix) AS india_vix,
          max(strike) FILTER (WHERE entry_offset = -3) AS strike_m3,
          max(strike) FILTER (WHERE entry_offset = -1) AS strike_m1,
          max(strike) FILTER (WHERE entry_offset = 0) AS strike_0,
          max(strike) FILTER (WHERE entry_offset = 1) AS strike_p1,
          max(strike) FILTER (WHERE entry_offset = 3) AS strike_p3,
          max(close) FILTER (WHERE entry_offset = -3 AND option_type = 'CALL') AS call_m3,
          max(close) FILTER (WHERE entry_offset = -3 AND option_type = 'PUT') AS put_m3,
          max(close) FILTER (WHERE entry_offset = -1 AND option_type = 'CALL') AS call_m1,
          max(close) FILTER (WHERE entry_offset = -1 AND option_type = 'PUT') AS put_m1,
          max(close) FILTER (WHERE entry_offset = 0 AND option_type = 'CALL') AS call_0,
          max(close) FILTER (WHERE entry_offset = 0 AND option_type = 'PUT') AS put_0,
          max(close) FILTER (WHERE entry_offset = 1 AND option_type = 'CALL') AS call_p1,
          max(close) FILTER (WHERE entry_offset = 1 AND option_type = 'PUT') AS put_p1,
          max(close) FILTER (WHERE entry_offset = 3 AND option_type = 'CALL') AS call_p3,
          max(close) FILTER (WHERE entry_offset = 3 AND option_type = 'PUT') AS put_p3,
          max(provider_iv_raw) FILTER (
            WHERE entry_offset = 0 AND option_type = 'CALL'
          ) AS provider_atm_call_iv,
          max(provider_iv_raw) FILTER (
            WHERE entry_offset = 0 AND option_type = 'PUT'
          ) AS provider_atm_put_iv,
          max(mapped_bsm_iv) FILTER (
            WHERE entry_offset = 0 AND option_type = 'CALL' AND bsm_status = 'ok'
          ) AS mapped_bsm_atm_call_iv,
          max(mapped_bsm_iv) FILTER (
            WHERE entry_offset = 0 AND option_type = 'PUT' AND bsm_status = 'ok'
          ) AS mapped_bsm_atm_put_iv
        FROM unique_rows
        GROUP BY 1, 2
        ORDER BY 2, 1
    """
    frame = connection.execute(
        query,
        [str(expiry_calendar), parquet_glob, parquet_glob],
    ).fetchdf()
    connection.close()

    frame["timestamp_ist"] = pd.to_datetime(frame["timestamp_ist"])
    frame["mapped_expiry"] = pd.to_datetime(frame["mapped_expiry"])
    frame["nearest_expiry"] = pd.to_datetime(frame["nearest_expiry"])
    frame["nearest_weekly_expiry"] = pd.to_datetime(frame["nearest_weekly_expiry"])
    frame["entry_time"] = frame["timestamp_ist"].dt.strftime("%H:%M")
    frame["research_t_years"] = (
        frame["nearest_expiry"] - frame["timestamp_ist"]
    ).dt.total_seconds() / (365 * 24 * 60 * 60)
    frame["research_dte"] = frame["research_t_years"] * 365
    frame["mapped_t_years"] = (
        frame["mapped_expiry"] - frame["timestamp_ist"]
    ).dt.total_seconds() / (365 * 24 * 60 * 60)
    frame["nearest_weekly_t_years"] = (
        frame["nearest_weekly_expiry"] - frame["timestamp_ist"]
    ).dt.total_seconds() / (365 * 24 * 60 * 60)

    parity_forwards = []
    for suffix in ("m1", "0", "p1"):
        parity_forwards.append(
            frame[f"strike_{suffix}"].to_numpy(dtype=float)
            + np.exp(RATE_CC * frame["research_t_years"].to_numpy(dtype=float))
            * (
                frame[f"call_{suffix}"].to_numpy(dtype=float)
                - frame[f"put_{suffix}"].to_numpy(dtype=float)
            )
        )
    frame["synthetic_forward"] = _nanmedian_columns(parity_forwards)
    frame["synthetic_forward_basis_bps"] = 10_000 * (
        frame["synthetic_forward"] / frame["spot"] - 1
    )

    forward = frame["synthetic_forward"].to_numpy(dtype=float)
    time_years = frame["research_t_years"].to_numpy(dtype=float)
    for suffix in ("m3", "0", "p3"):
        strike = frame[f"strike_{suffix}"].to_numpy(dtype=float)
        frame[f"iv_call_{suffix}"] = _black76_iv(
            forward,
            strike,
            frame[f"call_{suffix}"].to_numpy(dtype=float),
            time_years,
            np.ones(len(frame), dtype=bool),
        )
        frame[f"iv_put_{suffix}"] = _black76_iv(
            forward,
            strike,
            frame[f"put_{suffix}"].to_numpy(dtype=float),
            time_years,
            np.zeros(len(frame), dtype=bool),
        )

    frame["atm_call_iv"] = frame["iv_call_0"]
    frame["atm_put_iv"] = frame["iv_put_0"]
    frame["atm_iv"] = frame[["atm_call_iv", "atm_put_iv"]].mean(axis=1)
    frame["put_wing_iv"] = frame["iv_put_m3"]
    frame["call_wing_iv"] = frame["iv_call_p3"]
    frame["put_skew"] = frame["put_wing_iv"] - frame["atm_iv"]
    frame["call_skew"] = frame["call_wing_iv"] - frame["atm_iv"]
    frame["risk_reversal"] = frame["put_wing_iv"] - frame["call_wing_iv"]
    frame["smile_curvature"] = (
        (frame["put_wing_iv"] + frame["call_wing_iv"]) / 2 - frame["atm_iv"]
    )
    frame["atm_ce_pe_gap"] = frame["atm_call_iv"] - frame["atm_put_iv"]
    frame["provider_atm_mid_iv"] = (
        frame["provider_atm_call_iv"] + frame["provider_atm_put_iv"]
    ) / 200
    frame["mapped_bsm_atm_mid_iv"] = frame[
        ["mapped_bsm_atm_call_iv", "mapped_bsm_atm_put_iv"]
    ].mean(axis=1)

    ordered = frame.sort_values(["trade_date", "timestamp_ist"]).reset_index(drop=True)
    ordered["atm_iv_tod_percentile"] = _causal_percentile(
        ordered["atm_iv"].to_numpy(dtype=float),
        ordered["entry_time"].to_numpy(),
        ordered["trade_date"].astype(str).to_numpy(),
    )

    labels_by_horizon = _add_realized_volatility(ordered)
    horizon_frames = []
    regime_rows = []
    horizon_summaries = []
    for horizon, labels in labels_by_horizon.items():
        labels = labels.sort_values(["trade_date", "timestamp_ist"]).reset_index(drop=True)
        labels["vrp_signal_tod_percentile"] = _causal_percentile(
            labels["vrp_signal_var"].to_numpy(dtype=float),
            labels["entry_time"].to_numpy(),
            labels["trade_date"].astype(str).to_numpy(),
        )
        horizon_frames.append(labels)
        complete = labels.dropna(subset=["atm_iv", "forward_rv", "expost_vrp_var"])
        horizon_summaries.append(
            {
                "horizon_minutes": horizon,
                "observations": int(len(complete)),
                "dates": int(complete["trade_date"].nunique()),
                "median_atm_iv": float(complete["atm_iv"].median()),
                "median_forward_rv": float(complete["forward_rv"].median()),
                "mean_expost_vrp_var": float(complete["expost_vrp_var"].mean()),
                "median_expost_vrp_var": float(complete["expost_vrp_var"].median()),
                "median_expost_vrp_vol": float(complete["expost_vrp_vol"].median()),
                "positive_vrp_rate": float((complete["expost_vrp_var"] > 0).mean()),
            }
        )
        for percentile_column in ("atm_iv_tod_percentile", "vrp_signal_tod_percentile"):
            summary = _regime_summary(labels, percentile_column)
            summary["horizon_minutes"] = horizon
            summary["ranking_metric"] = percentile_column
            regime_rows.extend(_json_records(summary))

    horizon_frame = pd.concat(horizon_frames, ignore_index=True)
    minute_path = output_dir / "phase2_intraday_iv_surface.parquet"
    horizon_path = output_dir / "phase2_intraday_rv_vrp_labels.parquet"
    ordered.to_parquet(minute_path, index=False)
    horizon_frame.to_parquet(horizon_path, index=False)

    daily_rv_rows = []
    for trade_date, daily in ordered.groupby("trade_date", sort=True):
        daily = daily.sort_values("timestamp_ist")
        timestamps = pd.to_datetime(daily["timestamp_ist"])
        spot = daily["spot"].to_numpy(dtype=float)
        valid = timestamps.diff().dt.total_seconds().eq(60).to_numpy()
        valid[0] = False
        returns = np.full(len(daily), np.nan)
        returns[valid] = np.log(spot[valid] / np.roll(spot, 1)[valid])
        usable = returns[np.isfinite(returns)]
        daily_rv = (
            float(np.sqrt(np.mean(usable**2) * ANNUAL_MINUTES)) if len(usable) else np.nan
        )
        daily_rv_rows.append(
            {
                "trade_date": trade_date,
                "observed_minutes": int(len(daily)),
                "realized_return_minutes": int(len(usable)),
                "daily_intraday_rv": daily_rv,
                "median_atm_iv": float(daily["atm_iv"].median()),
                "median_put_skew": float(daily["put_skew"].median()),
                "median_call_skew": float(daily["call_skew"].median()),
                "median_risk_reversal": float(daily["risk_reversal"].median()),
                "median_smile_curvature": float(daily["smile_curvature"].median()),
                "median_research_dte": float(daily["research_dte"].median()),
                "session_status": str(daily["session_status"].mode().iloc[0]),
            }
        )
    daily_frame = pd.DataFrame(daily_rv_rows).sort_values("trade_date").reset_index(drop=True)
    daily_frame["daily_vrp_var"] = (
        daily_frame["median_atm_iv"] ** 2 - daily_frame["daily_intraday_rv"] ** 2
    )
    for metric in ("median_atm_iv", "daily_intraday_rv", "daily_vrp_var"):
        daily_frame[f"{metric}_causal_percentile"] = _causal_percentile(
            daily_frame[metric].to_numpy(dtype=float),
            np.full(len(daily_frame), "all_dates"),
            daily_frame["trade_date"].astype(str).to_numpy(),
        )
    daily_path = output_dir / "phase2_daily_iv_rv_vrp.parquet"
    daily_frame.to_parquet(daily_path, index=False)

    candidate_validation = pd.DataFrame(
        {
            "provider": ordered["provider_atm_mid_iv"],
            "mapped": ordered["mapped_bsm_atm_mid_iv"],
            "nearest_proxy": ordered["atm_iv"],
        }
    )
    validation_rows = []
    for candidate in ("mapped", "nearest_proxy"):
        pair = candidate_validation[["provider", candidate]].dropna()
        validation_rows.append(
            {
                "candidate": candidate,
                "observations": int(len(pair)),
                "provider_mae": float((pair[candidate] - pair["provider"]).abs().mean()),
                "provider_correlation": float(pair[candidate].corr(pair["provider"])),
                "median_iv": float(pair[candidate].median()),
                "provider_median_iv": float(pair["provider"].median()),
            }
        )

    result = {
        "contract": {
            "underlying": "NIFTY index options",
            "source_expiry_flag": "WEEK",
            "source_expiry_code": 1,
            "source_expiry_identity": "not returned by Dhan rolling endpoint",
            "audited_mapping": "second eligible weekly contract",
            "research_expiry_proxy": "nearest proven NIFTY expiry including monthly expiry",
            "research_expiry_proxy_warning": (
                "price/provider-IV consistency inference; not a provider-returned contract identity"
            ),
            "structure_boundary": "defined risk, all legs inside entry ATM +/-3",
            "horizons_minutes": list(HORIZONS),
            "rv_annualization": "sqrt(mean one-minute squared log return * 252 * 375)",
            "vrp_signal": "entry ATM IV squared minus trailing same-horizon RV squared",
            "vrp_outcome": "entry ATM IV squared minus forward same-horizon RV squared",
            "lookahead_rule": "forward RV is outcome-only; causal percentiles use prior dates only",
        },
        "coverage": {
            "minute_rows": int(len(ordered)),
            "dates": int(ordered["trade_date"].nunique()),
            "first_date": str(ordered["trade_date"].min()),
            "last_date": str(ordered["trade_date"].max()),
            "atm_iv_rows": int(ordered["atm_iv"].notna().sum()),
            "complete_wing_surface_rows": int(
                ordered[["atm_iv", "put_wing_iv", "call_wing_iv"]].notna().all(axis=1).sum()
            ),
            "india_vix_rows": int(ordered["india_vix"].notna().sum()),
        },
        "expiry_candidate_validation": validation_rows,
        "research_dte_distribution": _distribution(ordered, ["research_dte"]),
        "intraday_iv_skew_distribution": _distribution(
            ordered,
            [
                "atm_iv",
                "atm_call_iv",
                "atm_put_iv",
                "put_wing_iv",
                "call_wing_iv",
                "put_skew",
                "call_skew",
                "risk_reversal",
                "smile_curvature",
                "atm_ce_pe_gap",
                "synthetic_forward_basis_bps",
            ],
        ),
        "horizon_summaries": horizon_summaries,
        "causal_percentile_regimes": regime_rows,
        "daily_distribution": _distribution(
            daily_frame,
            [
                "median_atm_iv",
                "daily_intraday_rv",
                "daily_vrp_var",
                "median_put_skew",
                "median_call_skew",
                "median_risk_reversal",
                "median_smile_curvature",
            ],
        ),
        "artifacts": {
            "minute_iv_surface": str(minute_path),
            "horizon_rv_vrp_labels": str(horizon_path),
            "daily_iv_rv_vrp": str(daily_path),
        },
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--gold-root", type=Path, required=True)
    parser.add_argument("--expiry-calendar", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    result = analyze(
        args.gold_root.resolve(),
        args.expiry_calendar.resolve(),
        args.output_dir.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result["coverage"], indent=2, sort_keys=True))
    print(json.dumps(result["expiry_candidate_validation"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
