"""Summarize the Phase 2 intraday IV/RV/VRP surface into research regimes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from .analyze_intraday_volatility import _causal_percentile
except ImportError:  # Support direct execution by file path.
    from analyze_intraday_volatility import _causal_percentile


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _quintile(values: pd.Series) -> pd.Series:
    result = np.floor(values * 5) + 1
    return result.clip(upper=5).astype("Int64")


def _regime(
    frame: pd.DataFrame,
    rank_column: str,
    *,
    horizon: int,
) -> pd.DataFrame:
    eligible = frame.dropna(
        subset=[rank_column, "atm_iv", "forward_rv", "expost_vrp_var", "forward_return"]
    ).copy()
    eligible["quintile"] = _quintile(eligible[rank_column])
    return (
        eligible.groupby("quintile", observed=True)
        .agg(
            observations=("expost_vrp_var", "size"),
            median_atm_iv=("atm_iv", "median"),
            median_forward_rv=("forward_rv", "median"),
            median_expost_vrp_var=("expost_vrp_var", "median"),
            positive_vrp_rate=("expost_vrp_var", lambda values: float((values > 0).mean())),
            forward_return_p05=("forward_return", lambda values: float(values.quantile(0.05))),
            median_forward_return=("forward_return", "median"),
            forward_return_p95=("forward_return", lambda values: float(values.quantile(0.95))),
            downside_1pct_rate=("forward_return", lambda values: float((values <= -0.01).mean())),
            upside_1pct_rate=("forward_return", lambda values: float((values >= 0.01).mean())),
        )
        .reset_index()
        .assign(horizon_minutes=horizon, ranking_metric=rank_column)
    )


def summarize(
    minute_path: Path,
    labels_path: Path,
    daily_path: Path,
    ranked_minute_output: Path,
) -> dict[str, Any]:
    minute = pd.read_parquet(minute_path)
    labels = pd.read_parquet(labels_path)
    daily = pd.read_parquet(daily_path)
    minute["timestamp_ist"] = pd.to_datetime(minute["timestamp_ist"])
    labels["timestamp_ist"] = pd.to_datetime(labels["timestamp_ist"])

    minute = minute.sort_values(["trade_date", "timestamp_ist"]).reset_index(drop=True)
    date_values = minute["trade_date"].astype(str).to_numpy()
    time_values = minute["entry_time"].to_numpy()
    for metric in ("risk_reversal", "smile_curvature", "put_skew"):
        minute[f"{metric}_tod_percentile"] = _causal_percentile(
            minute[metric].to_numpy(dtype=float),
            time_values,
            date_values,
        )
    minute.to_parquet(ranked_minute_output, index=False)

    ranks = minute[
        [
            "trade_date",
            "timestamp_ist",
            "risk_reversal_tod_percentile",
            "smile_curvature_tod_percentile",
            "put_skew_tod_percentile",
        ]
    ]
    labels = labels.merge(ranks, on=["trade_date", "timestamp_ist"], how="left")
    exit_spot = minute[["trade_date", "timestamp_ist", "spot"]].rename(
        columns={"timestamp_ist": "exit_timestamp", "spot": "exit_spot"}
    )
    labels["exit_timestamp"] = labels["timestamp_ist"] + pd.to_timedelta(
        labels["horizon_minutes"], unit="m"
    )
    labels = labels.merge(exit_spot, on=["trade_date", "exit_timestamp"], how="left")
    labels["forward_return"] = np.log(labels["exit_spot"] / labels["spot"])

    regime_rows = []
    rank_columns = (
        "atm_iv_tod_percentile",
        "vrp_signal_tod_percentile",
        "risk_reversal_tod_percentile",
        "smile_curvature_tod_percentile",
        "put_skew_tod_percentile",
    )
    for horizon, horizon_frame in labels.groupby("horizon_minutes", sort=True):
        for rank_column in rank_columns:
            regime_rows.extend(_records(_regime(horizon_frame, rank_column, horizon=int(horizon))))

    regular_minute = minute[minute["session_status"] == "regular_session"].copy()
    session_minute = (
        regular_minute["timestamp_ist"].dt.hour * 60
        + regular_minute["timestamp_ist"].dt.minute
        - 555
    )
    regular_minute["time_bucket"] = (session_minute // 30).clip(lower=0).astype(int)
    regular_minute["time_bucket_start"] = (
        pd.Timestamp("2000-01-01 09:15")
        + pd.to_timedelta(regular_minute["time_bucket"] * 30, unit="m")
    ).dt.strftime("%H:%M")
    minute_time_summary = (
        regular_minute.groupby("time_bucket_start", sort=True)
        .agg(
            iv_observations=("atm_iv", "count"),
            dates=("trade_date", "nunique"),
            median_atm_iv=("atm_iv", "median"),
            median_put_wing_iv=("put_wing_iv", "median"),
            median_call_wing_iv=("call_wing_iv", "median"),
            median_put_skew=("put_skew", "median"),
            median_call_skew=("call_skew", "median"),
            median_risk_reversal=("risk_reversal", "median"),
            median_smile_curvature=("smile_curvature", "median"),
        )
        .reset_index()
    )

    labels_60 = labels[
        (labels["horizon_minutes"] == 60)
        & (labels["session_status"] == "regular_session")
    ].copy()
    label_minutes = (
        labels_60["timestamp_ist"].dt.hour * 60 + labels_60["timestamp_ist"].dt.minute - 555
    )
    labels_60["time_bucket"] = (label_minutes // 30).clip(lower=0).astype(int)
    labels_60["time_bucket_start"] = (
        pd.Timestamp("2000-01-01 09:15")
        + pd.to_timedelta(labels_60["time_bucket"] * 30, unit="m")
    ).dt.strftime("%H:%M")
    rv_time_summary = (
        labels_60.dropna(subset=["forward_rv", "expost_vrp_var"])
        .groupby("time_bucket_start", sort=True)
        .agg(
            rv_observations=("forward_rv", "size"),
            median_forward_rv=("forward_rv", "median"),
            median_expost_vrp_var=("expost_vrp_var", "median"),
            positive_vrp_rate=("expost_vrp_var", lambda values: float((values > 0).mean())),
        )
        .reset_index()
    )
    time_of_day = minute_time_summary.merge(rv_time_summary, on="time_bucket_start", how="left")

    labels["dte_bucket"] = pd.cut(
        labels["research_dte"],
        bins=[-np.inf, 0.5, 1.5, 3.5, np.inf],
        labels=["0-0.5", "0.5-1.5", "1.5-3.5", "3.5-7"],
    )
    dte_summary = (
        labels.dropna(subset=["atm_iv", "forward_rv", "dte_bucket"])
        .groupby(["horizon_minutes", "dte_bucket"], observed=True)
        .agg(
            observations=("expost_vrp_var", "size"),
            dates=("trade_date", "nunique"),
            median_atm_iv=("atm_iv", "median"),
            median_forward_rv=("forward_rv", "median"),
            median_expost_vrp_var=("expost_vrp_var", "median"),
            positive_vrp_rate=("expost_vrp_var", lambda values: float((values > 0).mean())),
        )
        .reset_index()
    )

    labels["year"] = pd.to_datetime(labels["trade_date"]).dt.year
    year_summary = (
        labels.dropna(subset=["atm_iv", "forward_rv"])
        .groupby(["year", "horizon_minutes"])
        .agg(
            observations=("expost_vrp_var", "size"),
            dates=("trade_date", "nunique"),
            median_atm_iv=("atm_iv", "median"),
            median_forward_rv=("forward_rv", "median"),
            median_expost_vrp_var=("expost_vrp_var", "median"),
            positive_vrp_rate=("expost_vrp_var", lambda values: float((values > 0).mean())),
        )
        .reset_index()
    )

    daily["atm_iv_quintile"] = _quintile(daily["median_atm_iv_causal_percentile"])
    daily_regime = (
        daily.dropna(subset=["atm_iv_quintile", "daily_intraday_rv", "daily_vrp_var"])
        .groupby("atm_iv_quintile", observed=True)
        .agg(
            dates=("trade_date", "size"),
            median_atm_iv=("median_atm_iv", "median"),
            median_daily_rv=("daily_intraday_rv", "median"),
            median_daily_vrp_var=("daily_vrp_var", "median"),
            positive_daily_vrp_rate=("daily_vrp_var", lambda values: float((values > 0).mean())),
            median_put_skew=("median_put_skew", "median"),
            median_risk_reversal=("median_risk_reversal", "median"),
        )
        .reset_index()
    )

    return {
        "contract": {
            "rank_definition": "current value versus prior dates at the same minute of day",
            "minimum_history": 60,
            "forward_return": "log spot(t+h)/spot(t), outcome only",
            "primary_structure_scope": "defined risk, entry legs within ATM +/-3",
        },
        "time_of_day_30m": _records(time_of_day),
        "dte_horizon": _records(dte_summary),
        "year_horizon": _records(year_summary),
        "causal_percentile_regimes": regime_rows,
        "daily_atm_iv_percentile_regimes": _records(daily_regime),
        "artifacts": {
            "ranked_minute_surface": str(ranked_minute_output),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minute-surface", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--daily", type=Path, required=True)
    parser.add_argument("--ranked-minute-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    result = summarize(
        args.minute_surface.resolve(),
        args.labels.resolve(),
        args.daily.resolve(),
        args.ranked_minute_output.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({key: len(value) for key, value in result.items() if isinstance(value, list)}))


if __name__ == "__main__":
    main()
