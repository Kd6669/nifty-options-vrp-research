"""Compare NIFTY option IV with horizon- and clock-matched realized variance.

Three views are deliberately separated:

* intraday RV annualized on ACT/365, matching the option-IV clock;
* expiry-matched RV from a fixed 10:15 entry, including overnight gaps;
* standard fixed-time daily RV annualized over 252 trading sessions.

The nearest listed expiry remains a research proxy because Dhan's rolling
response does not return the actual contract expiry.
"""

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


SESSION_ANNUAL_MINUTES = 252 * 375
ACT365_ANNUAL_MINUTES = 365 * 24 * 60
ACT365_SCALE = float(np.sqrt(ACT365_ANNUAL_MINUTES / SESSION_ANNUAL_MINUTES))
HORIZONS = (15, 30, 60, 90, 120, 180)


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", date_format="iso"))


def _distribution(values: pd.Series) -> dict[str, Any]:
    clean = values.dropna()
    return {
        "count": int(len(clean)),
        "mean": float(clean.mean()),
        "p01": float(clean.quantile(0.01)),
        "p05": float(clean.quantile(0.05)),
        "p25": float(clean.quantile(0.25)),
        "median": float(clean.median()),
        "p75": float(clean.quantile(0.75)),
        "p95": float(clean.quantile(0.95)),
        "p99": float(clean.quantile(0.99)),
    }


def _intraday_matched(labels: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    labels = labels.copy()
    labels["forward_rv_act365"] = labels["forward_rv"] * ACT365_SCALE
    labels["trailing_rv_act365"] = labels["trailing_rv"] * ACT365_SCALE
    labels["expost_vrp_var_act365"] = (
        labels["atm_iv"] ** 2 - labels["forward_rv_act365"] ** 2
    )
    labels["signal_vrp_var_act365"] = (
        labels["atm_iv"] ** 2 - labels["trailing_rv_act365"] ** 2
    )
    ranked_parts = []
    summaries = []
    for horizon, part in labels.groupby("horizon_minutes", sort=True):
        part = part.sort_values(["trade_date", "timestamp_ist"]).copy()
        part["signal_vrp_act365_tod_percentile"] = _causal_percentile(
            part["signal_vrp_var_act365"].to_numpy(dtype=float),
            part["entry_time"].to_numpy(),
            part["trade_date"].astype(str).to_numpy(),
        )
        ranked_parts.append(part)
        complete = part.dropna(subset=["atm_iv", "forward_rv_act365"])
        summaries.append(
            {
                "horizon_minutes": int(horizon),
                "observations": int(len(complete)),
                "dates": int(complete["trade_date"].nunique()),
                "median_atm_iv": float(complete["atm_iv"].median()),
                "median_rv_session252": float(complete["forward_rv"].median()),
                "median_rv_act365": float(complete["forward_rv_act365"].median()),
                "mean_vrp_var_act365": float(complete["expost_vrp_var_act365"].mean()),
                "median_vrp_var_act365": float(complete["expost_vrp_var_act365"].median()),
                "positive_vrp_rate_act365": float(
                    (complete["expost_vrp_var_act365"] > 0).mean()
                ),
            }
        )
    return pd.concat(ranked_parts, ignore_index=True), summaries


def _causal_regimes(labels: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    rank_columns = ("atm_iv_tod_percentile", "signal_vrp_act365_tod_percentile")
    for horizon in HORIZONS:
        part = labels[labels["horizon_minutes"] == horizon]
        for rank_column in rank_columns:
            eligible = part.dropna(
                subset=[
                    rank_column,
                    "atm_iv",
                    "forward_rv_act365",
                    "expost_vrp_var_act365",
                ]
            ).copy()
            eligible["quintile"] = np.minimum(
                np.floor(eligible[rank_column] * 5).astype(int) + 1,
                5,
            )
            summary = (
                eligible.groupby("quintile", observed=True)
                .agg(
                    observations=("expost_vrp_var_act365", "size"),
                    median_atm_iv=("atm_iv", "median"),
                    median_rv_act365=("forward_rv_act365", "median"),
                    mean_vrp_var_act365=("expost_vrp_var_act365", "mean"),
                    median_vrp_var_act365=("expost_vrp_var_act365", "median"),
                    positive_vrp_rate_act365=(
                        "expost_vrp_var_act365",
                        lambda values: float((values > 0).mean()),
                    ),
                )
                .reset_index()
            )
            summary["horizon_minutes"] = int(horizon)
            summary["ranking_metric"] = rank_column
            rows.extend(_records(summary))
    return rows


def _time_of_day(labels: pd.DataFrame) -> list[dict[str, Any]]:
    part = labels[
        (labels["horizon_minutes"] == 60)
        & (labels["session_status"] == "regular_session")
    ].copy()
    timestamps = pd.to_datetime(part["timestamp_ist"])
    minutes = timestamps.dt.hour * 60 + timestamps.dt.minute - 555
    part["time_bucket"] = (minutes // 30).clip(lower=0).astype(int)
    part["time_bucket_start"] = (
        pd.Timestamp("2000-01-01 09:15")
        + pd.to_timedelta(part["time_bucket"] * 30, unit="m")
    ).dt.strftime("%H:%M")
    summary = (
        part.dropna(subset=["atm_iv", "forward_rv_act365", "expost_vrp_var_act365"])
        .groupby("time_bucket_start", sort=True)
        .agg(
            observations=("expost_vrp_var_act365", "size"),
            median_atm_iv=("atm_iv", "median"),
            median_rv_act365=("forward_rv_act365", "median"),
            median_vrp_var_act365=("expost_vrp_var_act365", "median"),
            positive_vrp_rate_act365=(
                "expost_vrp_var_act365",
                lambda values: float((values > 0).mean()),
            ),
        )
        .reset_index()
    )
    return _records(summary)


def _expiry_matched(minute: pd.DataFrame) -> pd.DataFrame:
    minute = minute.sort_values("timestamp_ist").reset_index(drop=True).copy()
    minute["timestamp_ist"] = pd.to_datetime(minute["timestamp_ist"])
    minute["nearest_expiry"] = pd.to_datetime(minute["nearest_expiry"])
    minute["trade_date"] = pd.to_datetime(minute["trade_date"]).dt.date
    spot = minute["spot"].to_numpy(dtype=float)
    log_return = np.full(len(minute), np.nan)
    valid = (
        np.isfinite(spot[1:])
        & np.isfinite(spot[:-1])
        & (spot[1:] > 0)
        & (spot[:-1] > 0)
    )
    log_return[1:][valid] = np.log(spot[1:][valid] / spot[:-1][valid])
    squared = np.nan_to_num(log_return, nan=0.0) ** 2
    overnight = np.zeros(len(minute), dtype=bool)
    overnight[1:] = (
        minute["trade_date"].to_numpy()[1:] != minute["trade_date"].to_numpy()[:-1]
    )
    cumulative = np.concatenate(([0.0], np.cumsum(squared)))
    cumulative_overnight = np.concatenate(([0.0], np.cumsum(squared * overnight)))
    timestamp_ns = minute["timestamp_ist"].astype("int64").to_numpy()

    entries = minute[
        (minute["entry_time"] == "10:15")
        & (minute["session_status"] == "regular_session")
        & minute["atm_iv"].notna()
    ].drop_duplicates("trade_date", keep="first")
    entry_indices = entries.index.to_numpy()
    expiry_ns = entries["nearest_expiry"].astype("int64").to_numpy()
    exit_indices = np.searchsorted(timestamp_ns, expiry_ns, side="right") - 1
    valid_exit = (
        (exit_indices > entry_indices)
        & (exit_indices < len(minute))
        & (
            minute["trade_date"].to_numpy()[exit_indices]
            == entries["nearest_expiry"].dt.date.to_numpy()
        )
    )
    entries = entries.loc[valid_exit].copy()
    entry_indices = entry_indices[valid_exit]
    exit_indices = exit_indices[valid_exit]
    total_variance = cumulative[exit_indices + 1] - cumulative[entry_indices + 1]
    overnight_variance = (
        cumulative_overnight[exit_indices + 1]
        - cumulative_overnight[entry_indices + 1]
    )
    calendar_years = (
        entries["nearest_expiry"] - entries["timestamp_ist"]
    ).dt.total_seconds().to_numpy() / (365 * 24 * 60 * 60)
    entries["integrated_realized_variance"] = total_variance
    entries["rv_act365"] = np.sqrt(total_variance / calendar_years)
    entries["intraday_variance_act365"] = (
        total_variance - overnight_variance
    ) / calendar_years
    entries["overnight_variance_act365"] = overnight_variance / calendar_years
    entries["overnight_variance_share"] = np.divide(
        overnight_variance,
        total_variance,
        out=np.zeros_like(total_variance),
        where=total_variance > 0,
    )
    entries["vrp_var_act365"] = entries["atm_iv"] ** 2 - entries["rv_act365"] ** 2
    entries["positive_vrp"] = entries["vrp_var_act365"] > 0
    return entries[
        [
            "timestamp_ist",
            "trade_date",
            "nearest_expiry",
            "research_dte",
            "atm_iv",
            "rv_act365",
            "vrp_var_act365",
            "integrated_realized_variance",
            "intraday_variance_act365",
            "overnight_variance_act365",
            "overnight_variance_share",
            "positive_vrp",
        ]
    ]


def _expiry_summary(expiry_frame: pd.DataFrame) -> dict[str, Any]:
    dte_bucket = pd.cut(
        expiry_frame["research_dte"],
        bins=[-np.inf, 0.5, 1.5, 3.5, np.inf],
        labels=["0-0.5", "0.5-1.5", "1.5-3.5", "3.5-7"],
    )
    bucketed = expiry_frame.assign(dte_bucket=dte_bucket)
    dte_summary = (
        bucketed.groupby("dte_bucket", observed=True)
        .agg(
            observations=("vrp_var_act365", "size"),
            median_atm_iv=("atm_iv", "median"),
            median_rv_act365=("rv_act365", "median"),
            median_vrp_var_act365=("vrp_var_act365", "median"),
            positive_vrp_rate=("positive_vrp", "mean"),
            median_overnight_variance_share=("overnight_variance_share", "median"),
        )
        .reset_index()
    )
    return {
        "observations": int(len(expiry_frame)),
        "first_date": str(expiry_frame["trade_date"].min()),
        "last_date": str(expiry_frame["trade_date"].max()),
        "atm_iv": _distribution(expiry_frame["atm_iv"]),
        "rv_act365": _distribution(expiry_frame["rv_act365"]),
        "vrp_var_act365": _distribution(expiry_frame["vrp_var_act365"]),
        "positive_vrp_rate": float(expiry_frame["positive_vrp"].mean()),
        "median_overnight_variance_share": float(
            expiry_frame["overnight_variance_share"].median()
        ),
        "dte_buckets": _records(dte_summary),
    }


def _daily_horizons(minute: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    marks = minute[
        (minute["entry_time"] == "10:15")
        & (minute["session_status"] == "regular_session")
        & minute["atm_iv"].notna()
    ].drop_duplicates("trade_date", keep="first")
    marks = marks.sort_values("trade_date").reset_index(drop=True).copy()
    marks["mark_return"] = np.log(marks["spot"] / marks["spot"].shift(1))
    squared = marks["mark_return"].pow(2).to_numpy()
    cumulative = np.concatenate(([0.0], np.nancumsum(np.nan_to_num(squared, nan=0.0))))
    details = []
    summaries = []
    positions = np.arange(len(marks))
    for horizon in (1, 2, 3, 5):
        valid = positions + horizon < len(marks)
        rv = np.full(len(marks), np.nan)
        rv[valid] = np.sqrt(
            252
            / horizon
            * (
                cumulative[positions[valid] + horizon + 1]
                - cumulative[positions[valid] + 1]
            )
        )
        part = marks.copy()
        part["horizon_sessions"] = horizon
        part["forward_rv_252"] = rv
        part["vrp_var_252"] = part["atm_iv"] ** 2 - rv**2
        part["maturity_matched"] = (part["research_dte"] - horizon).abs() <= 0.75
        part = part.dropna(subset=["forward_rv_252", "vrp_var_252"])
        details.append(
            part[
                [
                    "timestamp_ist",
                    "trade_date",
                    "horizon_sessions",
                    "research_dte",
                    "atm_iv",
                    "forward_rv_252",
                    "vrp_var_252",
                    "maturity_matched",
                ]
            ]
        )
        matched = part[part["maturity_matched"]]
        summaries.append(
            {
                "horizon_sessions": horizon,
                "observations": int(len(part)),
                "median_atm_iv": float(part["atm_iv"].median()),
                "median_forward_rv_252": float(part["forward_rv_252"].median()),
                "median_vrp_var_252": float(part["vrp_var_252"].median()),
                "positive_vrp_rate_252": float((part["vrp_var_252"] > 0).mean()),
                "maturity_matched_observations": int(len(matched)),
                "maturity_matched_median_atm_iv": float(matched["atm_iv"].median()),
                "maturity_matched_median_rv_252": float(
                    matched["forward_rv_252"].median()
                ),
                "maturity_matched_median_vrp_var_252": float(
                    matched["vrp_var_252"].median()
                ),
                "maturity_matched_positive_vrp_rate_252": float(
                    (matched["vrp_var_252"] > 0).mean()
                ),
            }
        )
    return pd.concat(details, ignore_index=True), summaries


def analyze(
    minute_path: Path,
    labels_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    minute = pd.read_parquet(minute_path)
    labels = pd.read_parquet(labels_path)
    labels["timestamp_ist"] = pd.to_datetime(labels["timestamp_ist"])
    matched_labels, intraday_summary = _intraday_matched(labels)
    expiry_frame = _expiry_matched(minute)
    daily_frame, daily_summary = _daily_horizons(minute)

    output_dir.mkdir(parents=True, exist_ok=True)
    expiry_path = output_dir / "phase2_expiry_matched_vrp_1015.parquet"
    daily_path = output_dir / "phase2_daily_horizon_rv_vrp_1015.parquet"
    expiry_frame.to_parquet(expiry_path, index=False)
    daily_frame.to_parquet(daily_path, index=False)

    return {
        "contract": {
            "iv_measure": "parity-adjusted forward-ATM Black-76 IV at entry",
            "expiry_identity": "nearest-listed-expiry research proxy, not provider-returned",
            "intraday_rv_act365": (
                "sqrt(sum one-minute squared log returns / intraday calendar-year fraction)"
            ),
            "expiry_rv_act365": (
                "sqrt(sum observed squared log returns including overnight gaps / "
                "entry-to-expiry ACT/365 years)"
            ),
            "daily_rv_252": (
                "sqrt(252 / sessions * sum fixed-10:15 daily squared log returns)"
            ),
            "vrp_definition": "entry ATM IV squared minus matched forward RV squared",
            "forward_rv_role": "outcome only",
        },
        "scale_reconciliation": {
            "session_annual_minutes": SESSION_ANNUAL_MINUTES,
            "act365_annual_minutes": ACT365_ANNUAL_MINUTES,
            "rv_act365_multiplier_vs_session252": ACT365_SCALE,
        },
        "intraday_act365_horizons": intraday_summary,
        "intraday_act365_causal_regimes": _causal_regimes(matched_labels),
        "intraday_act365_time_of_day_60m": _time_of_day(matched_labels),
        "expiry_matched_1015": _expiry_summary(expiry_frame),
        "daily_fixed_1015": daily_summary,
        "artifacts": {
            "expiry_matched_rows": str(expiry_path),
            "daily_horizon_rows": str(daily_path),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minute-surface", type=Path, required=True)
    parser.add_argument("--labels", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    result = analyze(
        args.minute_surface.resolve(),
        args.labels.resolve(),
        args.output_dir.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(result["scale_reconciliation"], indent=2, sort_keys=True))
    print(json.dumps(result["expiry_matched_1015"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
