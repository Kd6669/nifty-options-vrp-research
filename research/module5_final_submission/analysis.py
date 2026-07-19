"""Deterministic final metrics and robustness diagnostics.

This module only consumes frozen upstream artifacts.  It does not rerun data extraction,
hypothesis selection, or the Phase 10 policy search.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


SCHEMA_VERSION = "module5-final-submission/v1"
INITIAL_CAPITAL = 1_000_000.0
BREAK_DATE = pd.Timestamp("2024-11-20")
EVENT_WINDOW_BDAYS = 5
RESULT_ROOT = Path("research/module5_final_submission/results")


def _safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def _sha256(path: Path) -> str:
    payload = path.read_bytes()
    if path.suffix.lower() in {".csv", ".json", ".md", ".svg"}:
        payload = payload.replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n")


def _business_distance(left: pd.Timestamp, right: pd.Timestamp) -> int:
    a = np.datetime64(left.date())
    b = np.datetime64(right.date())
    if a <= b:
        return int(np.busday_count(a, b))
    return -int(np.busday_count(b, a))


def _attach_events(trades: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    event_rows = []
    events = events.copy()
    events["event_date"] = pd.to_datetime(events["event_date"])
    for trade_date in pd.to_datetime(trades["trade_date"]):
        distances = events["event_date"].map(lambda date: _business_distance(date, trade_date))
        nearby = events.loc[distances.abs().le(EVENT_WINDOW_BDAYS)].copy()
        if nearby.empty:
            event_rows.append((False, "none", "", np.nan))
            continue
        nearby["distance"] = distances.loc[nearby.index]
        nearest = nearby.loc[nearby["distance"].abs().idxmin()]
        event_rows.append(
            (
                True,
                "|".join(sorted(nearby["event_type"].unique())),
                " | ".join(nearby["event_label"].tolist()),
                int(nearest["distance"]),
            )
        )
    result = trades.copy()
    result[["event_week", "event_type", "event_label", "nearest_event_bday"]] = pd.DataFrame(
        event_rows, index=result.index
    )
    return result


def _group_summary(frame: pd.DataFrame, label: str) -> dict[str, Any]:
    pnl = frame["net_pnl_rupees"].astype(float)
    gross = frame["gross_pnl_rupees"].astype(float)
    cost = frame["total_cost_rupees"].astype(float)
    return {
        "group": label,
        "trades": len(frame),
        "gross_pnl_rupees": gross.sum(),
        "total_cost_rupees": cost.sum(),
        "net_pnl_rupees": pnl.sum(),
        "mean_net_pnl_rupees": pnl.mean() if len(frame) else np.nan,
        "median_net_pnl_rupees": pnl.median() if len(frame) else np.nan,
        "win_rate": pnl.gt(0).mean() if len(frame) else np.nan,
        "average_lots": frame["lots"].mean() if len(frame) else np.nan,
        "cost_drag_pct_gross": cost.sum() / gross.sum() if gross.sum() > 0 else np.nan,
    }


def _permutation_break(pre: np.ndarray, post: np.ndarray, trials: int = 20_000) -> dict[str, float]:
    observed = float(post.mean() - pre.mean())
    pooled = np.concatenate([pre, post])
    n_pre = len(pre)
    rng = np.random.default_rng(20240719)
    permuted = np.empty(trials)
    for idx in range(trials):
        shuffled = rng.permutation(pooled)
        permuted[idx] = shuffled[n_pre:].mean() - shuffled[:n_pre].mean()
    p_value = float((np.abs(permuted) >= abs(observed)).mean())
    boot = np.empty(trials)
    for idx in range(trials):
        boot_pre = rng.choice(pre, size=len(pre), replace=True)
        boot_post = rng.choice(post, size=len(post), replace=True)
        boot[idx] = boot_post.mean() - boot_pre.mean()
    return {
        "mean_difference_post_minus_pre": observed,
        "two_sided_permutation_p_value": p_value,
        "bootstrap_ci_95_lower": float(np.quantile(boot, 0.025)),
        "bootstrap_ci_95_upper": float(np.quantile(boot, 0.975)),
        "permutations": trials,
    }


def _daily_metrics(daily: pd.DataFrame, trades: pd.DataFrame) -> dict[str, Any]:
    returns = daily["daily_return"].astype(float)
    downside = returns.clip(upper=0)
    ann_vol = returns.std(ddof=1) * np.sqrt(252)
    sharpe = returns.mean() / returns.std(ddof=1) * np.sqrt(252)
    sortino = returns.mean() / np.sqrt((downside.pow(2)).mean()) * np.sqrt(252)
    years = max(
        (pd.to_datetime(daily["date"]).iloc[-1] - pd.to_datetime(daily["date"]).iloc[0]).days
        / 365.25,
        1 / 365.25,
    )
    ending = float(daily["equity_rupees"].iloc[-1])
    executed = trades.loc[trades["executed"].astype(bool)].copy()
    pnl = executed["net_pnl_rupees"]
    wins = pnl[pnl > 0]
    losses = pnl[pnl <= 0]
    weekly = (
        daily.assign(date=pd.to_datetime(daily["date"]))
        .set_index("date")["net_pnl_rupees"]
        .resample("W-FRI")
        .sum()
    )
    cvar_n = max(1, int(np.ceil(0.05 * len(executed))))
    return {
        "initial_capital_rupees": INITIAL_CAPITAL,
        "ending_equity_rupees": ending,
        "net_profit_rupees": ending - INITIAL_CAPITAL,
        "total_return": ending / INITIAL_CAPITAL - 1,
        "cagr": (ending / INITIAL_CAPITAL) ** (1 / years) - 1,
        "annualized_volatility": ann_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "maximum_drawdown_rupees": float(daily["drawdown_rupees"].min()),
        "maximum_drawdown_pct": float(daily["drawdown_pct"].min()),
        "executed_trades": len(executed),
        "candidate_signals": len(trades),
        "skipped_signals": int((~trades["executed"].astype(bool)).sum()),
        "hit_rate": pnl.gt(0).mean(),
        "average_win_rupees": wins.mean(),
        "average_loss_rupees": losses.mean(),
        "payoff_ratio": wins.mean() / abs(losses.mean()),
        "profit_factor": wins.sum() / abs(losses.sum()),
        "average_trade_net_rupees": pnl.mean(),
        "worst_trade_rupees": pnl.min(),
        "cvar_5_trade_rupees": pnl.nsmallest(cvar_n).mean(),
        "worst_day_rupees": float(daily["net_pnl_rupees"].min()),
        "worst_week_rupees": float(weekly.min()),
        "turnover_rupees": executed["turnover_rupees"].sum(),
        "gross_pnl_rupees": executed["gross_pnl_rupees"].sum(),
        "total_cost_rupees": executed["total_cost_rupees"].sum(),
        "cost_drag_pct_gross": executed["total_cost_rupees"].sum()
        / executed["gross_pnl_rupees"].sum(),
        "return_on_average_entry_margin": pnl.sum() / executed["margin_rupees"].mean(),
        "average_margin_utilization": executed["margin_utilization"].mean(),
        "maximum_margin_utilization": executed["margin_utilization"].max(),
        "average_cash_risk_utilization": executed["cash_risk_utilization"].mean(),
        "maximum_cash_risk_utilization": executed["cash_risk_utilization"].max(),
    }


def _svg_line(
    path: Path, frame: pd.DataFrame, x: str, y: str, title: str, color: str = "#2563eb"
) -> None:
    width, height = 900, 360
    left, right, top, bottom = 70, 25, 45, 50
    values = frame[y].astype(float).to_numpy()
    if len(values) < 2:
        return
    low, high = float(values.min()), float(values.max())
    span = high - low or 1.0
    xs = np.linspace(left, width - right, len(values))
    ys = top + (high - values) / span * (height - top - bottom)
    points = " ".join(f"{a:.1f},{b:.1f}" for a, b in zip(xs, ys))
    zero_y = top + (high - 0) / span * (height - top - bottom)
    zero = (
        f'<line x1="{left}" y1="{zero_y:.1f}" x2="{width-right}" y2="{zero_y:.1f}" stroke="#cbd5e1"/>'
        if low <= 0 <= high
        else ""
    )
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/><text x="{left}" y="25" font-family="Arial" font-size="18" font-weight="bold" fill="#0f172a">{title}</text>
<line x1="{left}" y1="{top}" x2="{left}" y2="{height-bottom}" stroke="#64748b"/><line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" stroke="#64748b"/>{zero}
<polyline fill="none" stroke="{color}" stroke-width="2.5" points="{points}"/>
<text x="8" y="{top+5}" font-family="Arial" font-size="11" fill="#475569">{high:,.0f}</text><text x="8" y="{height-bottom}" font-family="Arial" font-size="11" fill="#475569">{low:,.0f}</text>
<text x="{left}" y="{height-18}" font-family="Arial" font-size="11" fill="#475569">{frame[x].iloc[0]}</text><text x="{width-right-95}" y="{height-18}" font-family="Arial" font-size="11" fill="#475569">{frame[x].iloc[-1]}</text></svg>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg, encoding="utf-8", newline="\n")


def build_submission(repo_root: Path) -> dict[str, Any]:
    result_root = repo_root / RESULT_ROOT
    result_root.mkdir(parents=True, exist_ok=True)
    module4 = repo_root / "research/module4_sizing_risk_management/results"
    trades = pd.read_csv(module4 / "trades/recommended_trade_sheet.csv")
    features = pd.read_csv(repo_root / "audit/phase9_scored_events.csv")
    feature_columns = [
        column
        for column in features.columns
        if column not in trades.columns or column == "trade_id"
    ]
    trades = trades.merge(
        features[feature_columns], on="trade_id", how="left", validate="one_to_one"
    )
    trades["trade_date"] = pd.to_datetime(trades["trade_date"])
    trades["structural_regime"] = np.where(
        trades["trade_date"].lt(BREAK_DATE), "pre_2024_11_20", "post_2024_11_20"
    )
    events = pd.read_csv(
        repo_root / "research/module5_final_submission/contracts/event_calendar.csv"
    )
    trades = _attach_events(trades, events)
    trades["trade_date"] = trades["trade_date"].dt.strftime("%Y-%m-%d")
    _write_csv(result_root / "trades/final_trade_sheet.csv", trades)

    daily = pd.read_csv(module4 / "curves/recommended_equity_curve.csv")
    metrics = _daily_metrics(daily, trades)
    episodes = pd.read_csv(module4 / "curves/drawdown_episodes.csv")
    metrics["maximum_drawdown_duration_calendar_days"] = int(
        episodes["calendar_days_to_recovery"].max()
    )

    monthly = (
        daily.assign(date=pd.to_datetime(daily["date"]))
        .set_index("date")
        .resample("ME")
        .agg(
            net_pnl_rupees=("net_pnl_rupees", "sum"),
            gross_pnl_rupees=("gross_pnl_rupees", "sum"),
            total_cost_rupees=("total_cost_rupees", "sum"),
            turnover_rupees=("turnover_rupees", "sum"),
            ending_equity_rupees=("equity_rupees", "last"),
        )
        .reset_index()
    )
    monthly["month"] = monthly["date"].dt.strftime("%Y-%m")
    monthly["monthly_return"] = monthly["net_pnl_rupees"] / monthly["ending_equity_rupees"].sub(
        monthly["net_pnl_rupees"]
    )
    _write_csv(result_root / "curves/monthly_returns.csv", monthly.drop(columns="date"))
    weekly = (
        daily.assign(date=pd.to_datetime(daily["date"]))
        .set_index("date")
        .resample("W-FRI")
        .agg(
            net_pnl_rupees=("net_pnl_rupees", "sum"),
            gross_pnl_rupees=("gross_pnl_rupees", "sum"),
            total_cost_rupees=("total_cost_rupees", "sum"),
        )
        .reset_index()
    )
    weekly["week_ending"] = weekly["date"].dt.strftime("%Y-%m-%d")
    _write_csv(result_root / "curves/weekly_returns.csv", weekly.drop(columns="date"))

    executed = trades.loc[trades["executed"].astype(bool)].copy()
    decay_rows = []
    for multiplier in (0.0, 0.5, 1.0, 1.25, 1.5, 2.0, 3.0):
        net = (
            executed["gross_pnl_rupees"]
            - executed["charges_rupees"]
            - multiplier * executed["slippage_rupees"]
        )
        decay_rows.append(
            {
                "slippage_multiplier": multiplier,
                "gross_pnl_rupees": executed["gross_pnl_rupees"].sum(),
                "charges_rupees": executed["charges_rupees"].sum(),
                "slippage_rupees": multiplier * executed["slippage_rupees"].sum(),
                "net_pnl_rupees": net.sum(),
                "mean_net_pnl_rupees": net.mean(),
                "win_rate": net.gt(0).mean(),
            }
        )
    decay = pd.DataFrame(decay_rows)
    break_even_slippage = (
        executed["gross_pnl_rupees"].sum() - executed["charges_rupees"].sum()
    ) / executed["slippage_rupees"].sum()
    decay["break_even_slippage_multiplier"] = break_even_slippage
    _write_csv(result_root / "robustness/execution_decay.csv", decay)

    structural_rows = []
    for label, group in executed.groupby("structural_regime", sort=True):
        row = _group_summary(group, label)
        row["mean_net_per_lot_rupees"] = (group["net_pnl_rupees"] / group["lots"]).mean()
        row["mean_frozen_one_lot_net_rupees"] = group["one_lot_net_pnl"].mean()
        structural_rows.append(row)
    pre = (
        executed.loc[executed["structural_regime"].eq("pre_2024_11_20"), "net_pnl_rupees"]
        / executed.loc[executed["structural_regime"].eq("pre_2024_11_20"), "lots"]
    ).to_numpy(float)
    post = (
        executed.loc[executed["structural_regime"].eq("post_2024_11_20"), "net_pnl_rupees"]
        / executed.loc[executed["structural_regime"].eq("post_2024_11_20"), "lots"]
    ).to_numpy(float)
    break_test = _permutation_break(pre, post)
    structural = pd.DataFrame(structural_rows)
    for key, value in break_test.items():
        structural[key] = value
    structural["break_date"] = BREAK_DATE.strftime("%Y-%m-%d")
    _write_csv(result_root / "robustness/nov2024_structural_break.csv", structural)

    event_rows = []
    for flag, group in executed.groupby("event_week", sort=True):
        event_rows.append(_group_summary(group, "event_week" if flag else "non_event_week"))
    for label, group in executed.loc[executed["event_week"]].groupby("event_type", sort=True):
        event_rows.append(_group_summary(group, f"event_type:{label}"))
    event_summary = pd.DataFrame(event_rows)
    event_summary["event_window_business_days"] = EVENT_WINDOW_BDAYS
    event_summary["inference_status"] = np.where(
        event_summary["trades"].lt(10), "descriptive_only_small_n", "descriptive_not_causal"
    )
    _write_csv(result_root / "robustness/event_conditioning.csv", event_summary)

    surface = pd.read_parquet(repo_root / "audit/phase10_fly_cost_surface.parquet")
    eligible = set(executed["trade_id"])
    capacity = (
        surface.loc[surface["trade_id"].isin(eligible)]
        .groupby("lots", as_index=False)
        .agg(
            trades=("trade_id", "nunique"),
            gross_pnl_rupees=("gross_pnl_rupees", "sum"),
            charges_rupees=("charges_rupees", "sum"),
            base_slippage_rupees=("base_slippage_rupees", "sum"),
            impact_rupees=("impact_rupees", "sum"),
            slippage_rupees=("slippage_rupees", "sum"),
            net_pnl_rupees=("net_pnl_rupees", "sum"),
            average_margin_rupees=("margin_rupees", "mean"),
            maximum_margin_rupees=("margin_rupees", "max"),
        )
    )
    capacity["net_pnl_per_lot_rupees"] = capacity["net_pnl_rupees"] / capacity["lots"]
    capacity["impact_to_base_slippage"] = (
        capacity["impact_rupees"] / capacity["base_slippage_rupees"]
    )
    capacity["slippage_to_gross"] = capacity["slippage_rupees"] / capacity["gross_pnl_rupees"]
    _write_csv(result_root / "robustness/capacity_curve.csv", capacity)

    worst = episodes.sort_values("maximum_drawdown_rupees").iloc[0]
    start = pd.Timestamp(worst["start_date"])
    recovery = pd.Timestamp(worst["recovery_date"])
    autopsy = executed.loc[pd.to_datetime(executed["trade_date"]).between(start, recovery)].copy()
    autopsy.insert(0, "episode_start", start.strftime("%Y-%m-%d"))
    autopsy.insert(1, "episode_recovery", recovery.strftime("%Y-%m-%d"))
    _write_csv(result_root / "risk/worst_drawdown_trade_autopsy.csv", autopsy)

    annual = (
        daily.assign(year=pd.to_datetime(daily["date"]).dt.year)
        .groupby("year", as_index=False)
        .agg(
            gross_pnl_rupees=("gross_pnl_rupees", "sum"),
            total_cost_rupees=("total_cost_rupees", "sum"),
            net_pnl_rupees=("net_pnl_rupees", "sum"),
            turnover_rupees=("turnover_rupees", "sum"),
            maximum_drawdown_pct=("drawdown_pct", "min"),
        )
    )
    _write_csv(result_root / "curves/annual_returns.csv", annual)
    _write_csv(result_root / "curves/equity_curve.csv", daily)
    _write_csv(result_root / "risk/drawdown_episodes.csv", episodes)

    _svg_line(
        result_root / "visualizations/equity_curve.svg",
        daily,
        "date",
        "equity_rupees",
        "₹10 lakh shadow-candidate equity",
    )
    _svg_line(
        result_root / "visualizations/execution_decay.svg",
        decay,
        "slippage_multiplier",
        "net_pnl_rupees",
        "Net P&L versus slippage multiplier",
        "#dc2626",
    )
    _svg_line(
        result_root / "visualizations/capacity_curve.svg",
        capacity,
        "lots",
        "net_pnl_per_lot_rupees",
        "Capacity: net P&L per lot",
        "#059669",
    )

    summary = {
        "schema_version": SCHEMA_VERSION,
        "decision": "SHADOW_ONLY_NOT_LIVE_CAPITAL_APPROVED",
        "hypothesis_result": "STANDALONE_INTRADAY_VRP_RULE_REJECTED_NET_OF_COSTS",
        "post_hoc_candidate": "UPPER_85_SHORT_IRON_FLY_WITH_FROZEN_GATES_AND_SIZING",
        "metrics": metrics,
        "execution_decay": {"break_even_slippage_multiplier": break_even_slippage},
        "nov2024_break": {**break_test, "test_unit": "selected_profile_net_pnl_per_lot"},
        "event_calendar": {
            "window_business_days": EVENT_WINDOW_BDAYS,
            "heavy_results_weeks": "not_tagged_no_reliable_index_specific_calendar",
        },
        "limitations": [
            "rolling nearest-expiry ATM+/-10 archive",
            "fixed-contract labels reliable only through 180 minutes",
            "historical bid/ask unavailable; slippage is modeled",
            "post-hoc candidate and sizing are not clean OOS evidence",
            "event results are descriptive, not causal",
            "SPAN is timestamp-aware where joined; missing India VIX falls back to entry ATM IV",
        ],
    }
    summary_path = result_root / "summary.json"
    summary_path.write_text(
        json.dumps(_safe(summary), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    report = _render_tearsheet(summary, structural, event_summary, decay, capacity, episodes)
    (result_root / "tearsheet.md").write_text(report, encoding="utf-8", newline="\n")
    _write_csv(
        result_root / "headline_metrics.csv",
        pd.DataFrame([{"metric": key, "value": value} for key, value in metrics.items()]),
    )
    payload = {
        "summary": _safe(summary),
        "headline_metrics": _safe(pd.DataFrame([metrics]).to_dict("records")),
        "structural_break": _safe(structural.to_dict("records")),
        "event_conditioning": _safe(event_summary.to_dict("records")),
        "execution_decay": _safe(decay.to_dict("records")),
        "capacity": _safe(capacity.to_dict("records")),
        "annual": _safe(annual.to_dict("records")),
        "monthly": _safe(monthly.drop(columns="date").to_dict("records")),
        "drawdowns": _safe(episodes.to_dict("records")),
        "trades": _safe(trades.to_dict("records")),
        "equity": _safe(daily.to_dict("records")),
    }
    (result_root / "workbook_payload.json").write_text(
        json.dumps(payload, separators=(",", ":")), encoding="utf-8"
    )

    tracked = sorted(
        path for path in result_root.rglob("*") if path.is_file() and path.name != "manifest.json"
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "members": [
            {
                "path": path.relative_to(repo_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in tracked
        ],
    }
    (result_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return _safe(
        {
            "status": "built",
            "decision": summary["decision"],
            "metrics": metrics,
            "files": len(tracked),
        }
    )


def _render_tearsheet(
    summary: dict[str, Any],
    structural: pd.DataFrame,
    events: pd.DataFrame,
    decay: pd.DataFrame,
    capacity: pd.DataFrame,
    episodes: pd.DataFrame,
) -> str:
    m = summary["metrics"]
    return f"""# Final NIFTY VRP Research Tear Sheet

## Decision

**Shadow only; no live capital approval.** The preregistered standalone intraday VRP rules fail net
of one-lot costs. The later upper-85 short-iron-fly result is a post-hoc forward-shadow candidate,
not a clean out-of-sample discovery.

## Headline economics

| Metric | Result |
|---|---:|
| Starting capital | ₹{m['initial_capital_rupees']:,.0f} |
| Ending equity | ₹{m['ending_equity_rupees']:,.0f} |
| Net profit | ₹{m['net_profit_rupees']:,.0f} |
| Total return | {m['total_return']:.2%} |
| CAGR | {m['cagr']:.2%} |
| Annualized volatility | {m['annualized_volatility']:.2%} |
| Sharpe / Sortino | {m['sharpe']:.2f} / {m['sortino']:.2f} |
| Maximum drawdown | ₹{abs(m['maximum_drawdown_rupees']):,.0f} ({abs(m['maximum_drawdown_pct']):.2%}) |
| Max recovery duration | {m['maximum_drawdown_duration_calendar_days']} calendar days |
| Signals / executed / skipped | {m['candidate_signals']} / {m['executed_trades']} / {m['skipped_signals']} |
| Hit rate | {m['hit_rate']:.2%} |
| Average win / loss | ₹{m['average_win_rupees']:,.0f} / ₹{m['average_loss_rupees']:,.0f} |
| Worst trade / day / week | ₹{m['worst_trade_rupees']:,.0f} / ₹{m['worst_day_rupees']:,.0f} / ₹{m['worst_week_rupees']:,.0f} |
| Trade CVaR 5% | ₹{m['cvar_5_trade_rupees']:,.0f} |
| Gross / costs / net | ₹{m['gross_pnl_rupees']:,.0f} / ₹{m['total_cost_rupees']:,.0f} / ₹{m['net_profit_rupees']:,.0f} |
| Cost drag / gross | {m['cost_drag_pct_gross']:.2%} |
| Turnover | ₹{m['turnover_rupees']:,.0f} |

## Mandatory robustness extensions

- Execution decay: aggregate net P&L reaches zero at approximately
  **{summary['execution_decay']['break_even_slippage_multiplier']:.2f}×** modeled slippage.
- November 2024 break: post-minus-pre mean difference is
  **₹{summary['nov2024_break']['mean_difference_post_minus_pre']:,.0f} per selected lot**
  (95% bootstrap CI ₹{summary['nov2024_break']['bootstrap_ci_95_lower']:,.0f} to
  ₹{summary['nov2024_break']['bootstrap_ci_95_upper']:,.0f}); the two-sided permutation p-value is
  **{summary['nov2024_break']['two_sided_permutation_p_value']:.3f}**.
- Event conditioning uses a frozen ±{EVENT_WINDOW_BDAYS}-business-day window around RBI MPC,
  Union Budget and 2024 national-election dates. Small cells are descriptive only.
- Capacity is an execution diagnostic over 1–100 equal lots, not an authorization to breach the
  ₹10 lakh margin and cash-risk controls.

## Why this might be fake

The positive curve was discovered after the base VRP hypothesis failed; the same history influenced
the gate, structure and sizing work. Calendar 2024 is weak, Phase 9's holdout rank-correlation
bootstrap gate fails, quotes are model-filled rather than historical bid/ask, and the rolling chain
cannot validate holds beyond 180 minutes. Sparse event and post-break samples cannot rescue those
identification problems.

## Live decision and kill-switch

Allocate **₹0 live** today. Run the exact rule in shadow at one-lot telemetry, without silently
retuning. Promotion requires at least 100 new non-overlapping trades over 12 months, positive net
P&L after observed costs in both six-month halves, stable concentration, and a positive resampled
score/P&L relationship. Stop new entries immediately for stale/missing quotes, absent point-in-time
SPAN, modeled-cost breach, margin above 35%, cash risk above 4%, daily loss above 0.75%, or total
drawdown above 1.5%.
"""


def verify_submission(repo_root: Path) -> list[str]:
    root = repo_root / RESULT_ROOT
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        return ["missing results/manifest.json"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    failures = []
    for member in manifest.get("members", []):
        path = repo_root / member["path"]
        if not path.exists():
            failures.append(f"missing {member['path']}")
        elif _sha256(path) != member["sha256"]:
            failures.append(f"hash mismatch {member['path']}")
    trade_path = root / "trades/final_trade_sheet.csv"
    if trade_path.exists():
        trades = pd.read_csv(trade_path)
        if len(trades) != 132 or int(trades["executed"].sum()) != 86:
            failures.append("final trade sheet must contain 132 signals and 86 executions")
    return failures
