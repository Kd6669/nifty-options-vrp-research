"""Build the deterministic Module 4 sizing and risk-management closeout packet."""

from __future__ import annotations

import gzip
import hashlib
import html
import io
import json
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd


SCHEMA_VERSION = "module4-sizing-risk-closeout/v1"
DECISION = "FORWARD_SHADOW_CANDIDATE_NOT_DEPLOYMENT_APPROVED"
PROFILE = "max_margin_efficiency_dd_1.5%"
CONFIG_ID = 5628
INITIAL_CAPITAL = 1_000_000.0
RESULT_ROOT = Path("research/module4_sizing_risk_management/results")

SOURCE_FILES = [
    "audit/phase8_gated_events.csv",
    "audit/phase8_gated_capacity_surface.csv",
    "audit/phase8_gated_capital_tradebook.csv",
    "audit/phase8_gated_policy_summary.csv",
    "audit/phase8_gated_equity_curve.csv",
    "audit/phase8_gated_regime_summary.csv",
    "audit/phase8_gated_tearsheet.json",
    "audit/phase8_gated_manifest.json",
    "audit/phase9_scored_events.csv",
    "audit/phase9_rank_correlation.csv",
    "audit/phase9_score_quintiles.csv",
    "audit/phase9_confidence_tradebook.csv",
    "audit/phase9_economic_summary.csv",
    "audit/phase9_equity_curve.csv",
    "audit/phase9_tearsheet.json",
    "audit/phase9_manifest.json",
    "audit/phase10_fly_cost_surface.parquet",
    "audit/phase10_cost_reserve.csv",
    "audit/phase10_sizing_grid.csv",
    "audit/phase10_selected_profiles.csv",
    "audit/phase10_dimension_summary.csv",
    "audit/phase10_selected_profile_tradebook.csv",
    "audit/phase10_tearsheet.json",
    "audit/phase10_manifest.json",
]

IMPLEMENTATIONS = [
    "src/nifty_execution/costs.py",
    "src/nifty_execution/margin.py",
    "src/nifty_execution/provenance.py",
    "src/nifty_execution/slippage.py",
    "research/phase8/run_gated_capital_backtest.py",
    "research/phase9/run_confidence_sizing.py",
    "research/phase10/run_sizing_exploration.py",
    "research/module4_sizing_risk_management/__init__.py",
    "research/module4_sizing_risk_management/closeout.py",
    "research/module4_sizing_risk_management/run.py",
]

DOCUMENTS = [
    ".gitattributes",
    ".gitignore",
    "README.md",
    "pyproject.toml",
    "docs/research/CHECKPOINT3_EXECUTION_CAPITAL_MODELS.md",
    "docs/research/PHASE8_10L_GATED_CAPITAL_BACKTEST.md",
    "docs/research/PHASE9_CONFIDENCE_SIZING.md",
    "docs/research/PHASE10_SIZING_EXPLORATION.md",
    "research/module4_sizing_risk_management/README.md",
    "research/module4_sizing_risk_management/MODULE_MANIFEST.md",
    "research/module4_sizing_risk_management/module.yaml",
    "research/module4_sizing_risk_management/contracts/module.yaml",
    "research/module4_sizing_risk_management/contracts/strategy.json",
    "research/module4_sizing_risk_management/docs/module.yaml",
    "research/module4_sizing_risk_management/docs/architecture.md",
    "research/module4_sizing_risk_management/docs/runbook.md",
    "research/module4_sizing_risk_management/docs/research_note.md",
    "research/module4_sizing_risk_management/results/module.yaml",
    "research/module4_sizing_risk_management/scripts/module.yaml",
    "research/module4_sizing_risk_management/scripts/run_closeout.ps1",
]

RESULT_FILES = [
    RESULT_ROOT / "closeout.json",
    RESULT_ROOT / "closeout_report.md",
    RESULT_ROOT / "trades/recommended_trade_sheet.csv",
    RESULT_ROOT / "curves/recommended_equity_curve.csv",
    RESULT_ROOT / "curves/recommended_monthly_returns.csv",
    RESULT_ROOT / "curves/drawdown_episodes.csv",
    RESULT_ROOT / "diagnostics/profile_comparison.csv",
    RESULT_ROOT / "diagnostics/cost_breakdown.csv",
    RESULT_ROOT / "diagnostics/rank_correlation.csv",
    RESULT_ROOT / "diagnostics/score_quintiles.csv",
    RESULT_ROOT / "diagnostics/regime_summary.csv",
    RESULT_ROOT / "diagnostics/sizing_dimension_summary.csv",
    RESULT_ROOT / "exploration/selected_profiles.csv",
    RESULT_ROOT / "exploration/sizing_grid.csv.gz",
    RESULT_ROOT / "visualizations/equity_drawdown.svg",
    RESULT_ROOT / "visualizations/profile_frontier.svg",
    RESULT_ROOT / "visualizations/sizing_neighborhoods.svg",
]

TEXT_SUFFIXES = {".csv", ".json", ".md", ".py", ".yaml", ".yml", ".toml", ".ps1", ".svg"}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, float):
        return None if not math.isfinite(value) else value
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


def _sha256(path: Path) -> str:
    payload = path.read_bytes()
    if path.suffix.lower() in TEXT_SUFFIXES:
        payload = payload.replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _member(repo_root: Path, relative_path: str | Path, category: str) -> dict[str, Any]:
    relative = Path(relative_path)
    path = repo_root / relative
    return {
        "path": relative.as_posix(),
        "category": category,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n")


def _write_deterministic_csv_gz(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="") as text:
                frame.to_csv(text, index=False, lineterminator="\n")


def _recommended_profile(profiles: pd.DataFrame) -> pd.Series:
    selected = profiles.loc[
        profiles["profile"].eq(PROFILE) & profiles["config_id"].eq(CONFIG_ID)
    ]
    if len(selected) != 1:
        raise ValueError(f"Expected one {PROFILE!r} row for config {CONFIG_ID}; got {len(selected)}")
    row = selected.iloc[0]
    frozen = {
        "margin_fraction": 0.35,
        "max_risk_fraction": 0.04,
        "score_floor": 0.40,
        "score_power": 0.0,
    }
    for column, expected in frozen.items():
        if not math.isclose(float(row[column]), expected, rel_tol=0.0, abs_tol=1e-12):
            raise ValueError(f"Frozen {column} changed: {row[column]} != {expected}")
    return row


def _build_trade_sheet(repo_root: Path) -> pd.DataFrame:
    selected = pd.read_csv(repo_root / "audit/phase10_selected_profile_tradebook.csv")
    selected = selected.loc[
        selected["profile"].eq(PROFILE) & selected["config_id"].eq(CONFIG_ID)
    ].copy()
    if len(selected) != 132 or selected["trade_id"].duplicated().any():
        raise ValueError("Recommended profile must contain 132 unique candidate signals")

    surface = pd.read_parquet(repo_root / "audit/phase10_fly_cost_surface.parquet")
    surface_columns = [
        "trade_id",
        "lots",
        "exit_ts",
        "structure",
        "lot_size",
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
        "max_loss_rupees",
        "entry_dte",
        "span_time_slot",
    ]
    sheet = selected.merge(surface[surface_columns], on=["trade_id", "lots"], how="left")
    executed = sheet["lots"].gt(0)
    if sheet.loc[executed, "exit_ts"].isna().any():
        raise ValueError("An executed recommended trade has no exact-lot execution row")

    reserve = pd.read_csv(repo_root / "audit/phase10_cost_reserve.csv").rename(
        columns={"discovery_q95_total_cost_reserve": "q95_cost_reserve_rupees"}
    )
    sheet = sheet.merge(
        reserve[["lots", "q95_cost_reserve_rupees"]], on="lots", how="left"
    )
    sheet["executed"] = executed
    numeric_execution = [
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
        "max_loss_rupees",
        "q95_cost_reserve_rupees",
    ]
    sheet[numeric_execution] = sheet[numeric_execution].fillna(0.0)
    sheet["cash_risk_reserved_rupees"] = (
        sheet["max_loss_rupees"] + sheet["q95_cost_reserve_rupees"]
    )
    sheet["cash_risk_utilization"] = np.where(
        executed,
        sheet["cash_risk_reserved_rupees"] / sheet["equity_before"],
        0.0,
    )
    sheet["position_net_return_on_margin"] = np.where(
        executed & sheet["margin_rupees"].gt(0),
        sheet["net_pnl_rupees"] / sheet["margin_rupees"],
        0.0,
    )
    sheet["quality_switch_pass"] = sheet["confidence_score"].gt(0.40)

    def binding(row: pd.Series) -> str:
        if int(row["lots"]) == 0:
            return "quality_switch"
        margin_cap = int(row["margin_cap_lots"])
        risk_cap = int(row["risk_cap_lots"])
        if int(row["lots"]) == 76:
            return "capacity"
        if margin_cap < risk_cap:
            return "margin"
        if risk_cap < margin_cap:
            return "cash_risk"
        return "margin_and_cash_risk"

    sheet["binding_constraint"] = sheet.apply(binding, axis=1)
    sheet["skip_reason"] = np.where(executed, "", "confidence_score_at_or_below_0.40")
    running_peak = sheet["equity_after"].cummax().clip(lower=INITIAL_CAPITAL)
    sheet["drawdown_after_rupees"] = sheet["equity_after"] - running_peak
    sheet["drawdown_after_pct"] = sheet["equity_after"] / running_peak - 1.0

    ordered = [
        "trade_id",
        "trade_date",
        "entry_ts",
        "exit_ts",
        "split",
        "structure",
        "span_time_slot",
        "entry_dte",
        "confidence_score",
        "quality_switch_pass",
        "skip_reason",
        "lots",
        "lot_size",
        "margin_cap_lots",
        "risk_cap_lots",
        "binding_constraint",
        "equity_before",
        "margin_rupees",
        "margin_utilization",
        "max_loss_rupees",
        "q95_cost_reserve_rupees",
        "cash_risk_reserved_rupees",
        "cash_risk_utilization",
        "gross_pnl_rupees",
        "base_slippage_rupees",
        "ladder_impact_rupees",
        "volume_impact_rupees",
        "oi_impact_rupees",
        "impact_rupees",
        "slippage_rupees",
        "brokerage_rupees",
        "stt_rupees",
        "stamp_duty_rupees",
        "exchange_charges_rupees",
        "sebi_charges_rupees",
        "ipft_rupees",
        "gst_rupees",
        "charges_rupees",
        "total_cost_rupees",
        "net_pnl_rupees",
        "turnover_rupees",
        "position_net_return_on_margin",
        "equity_after",
        "drawdown_after_rupees",
        "drawdown_after_pct",
        "executed",
        "profile",
        "config_id",
    ]
    return sheet[ordered].sort_values(["entry_ts", "trade_id"]).reset_index(drop=True)


def _build_equity_curve(trades: pd.DataFrame) -> pd.DataFrame:
    frame = trades.loc[trades["executed"]].copy()
    frame["date"] = pd.to_datetime(frame["trade_date"])
    by_day = frame.groupby("date", as_index=True).agg(
        signals=("trade_id", "count"),
        lots=("lots", "sum"),
        gross_pnl_rupees=("gross_pnl_rupees", "sum"),
        total_cost_rupees=("total_cost_rupees", "sum"),
        net_pnl_rupees=("net_pnl_rupees", "sum"),
        turnover_rupees=("turnover_rupees", "sum"),
        maximum_margin_rupees=("margin_rupees", "max"),
    )
    calendar = pd.date_range(frame["date"].min(), frame["date"].max(), freq="B")
    daily = by_day.reindex(calendar, fill_value=0.0).rename_axis("date").reset_index()
    daily["signals"] = daily["signals"].astype(int)
    daily["lots"] = daily["lots"].astype(int)
    daily["equity_rupees"] = INITIAL_CAPITAL + daily["net_pnl_rupees"].cumsum()
    daily["peak_equity_rupees"] = daily["equity_rupees"].cummax().clip(lower=INITIAL_CAPITAL)
    daily["drawdown_rupees"] = daily["equity_rupees"] - daily["peak_equity_rupees"]
    daily["drawdown_pct"] = daily["equity_rupees"] / daily["peak_equity_rupees"] - 1.0
    daily["daily_return"] = daily["equity_rupees"].pct_change().fillna(
        daily["net_pnl_rupees"] / INITIAL_CAPITAL
    )
    daily["date"] = daily["date"].dt.date.astype(str)
    return daily


def _build_monthly_returns(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame["month"] = frame["date"].dt.to_period("M").astype(str)
    grouped = frame.groupby("month", as_index=False).agg(
        end_equity_rupees=("equity_rupees", "last"),
        signals=("signals", "sum"),
        lots=("lots", "sum"),
        gross_pnl_rupees=("gross_pnl_rupees", "sum"),
        total_cost_rupees=("total_cost_rupees", "sum"),
        net_pnl_rupees=("net_pnl_rupees", "sum"),
        turnover_rupees=("turnover_rupees", "sum"),
        worst_drawdown_pct=("drawdown_pct", "min"),
    )
    grouped.insert(
        1,
        "start_equity_rupees",
        grouped["end_equity_rupees"].shift(fill_value=INITIAL_CAPITAL),
    )
    grouped["monthly_return"] = (
        grouped["end_equity_rupees"] / grouped["start_equity_rupees"] - 1.0
    )
    return grouped


def _build_drawdown_episodes(daily: pd.DataFrame) -> pd.DataFrame:
    frame = daily.copy()
    underwater = frame["drawdown_rupees"].lt(-1e-9).to_numpy()
    rows: list[dict[str, Any]] = []
    start: int | None = None
    for index, active in enumerate(underwater):
        if active and start is None:
            start = index
        if start is not None and (not active or index == len(frame) - 1):
            end = index - 1 if not active else index
            segment = frame.iloc[start : end + 1]
            trough_index = int(segment["drawdown_rupees"].idxmin())
            recovered = not active
            rows.append(
                {
                    "start_date": frame.loc[start, "date"],
                    "trough_date": frame.loc[trough_index, "date"],
                    "recovery_date": frame.loc[index, "date"] if recovered else "",
                    "calendar_days_to_trough": (
                        pd.Timestamp(frame.loc[trough_index, "date"])
                        - pd.Timestamp(frame.loc[start, "date"])
                    ).days,
                    "calendar_days_to_recovery": (
                        (pd.Timestamp(frame.loc[index, "date"]) - pd.Timestamp(frame.loc[start, "date"])).days
                        if recovered
                        else None
                    ),
                    "maximum_drawdown_rupees": float(frame.loc[trough_index, "drawdown_rupees"]),
                    "maximum_drawdown_pct": float(frame.loc[trough_index, "drawdown_pct"]),
                    "recovered": recovered,
                }
            )
            start = None
    return pd.DataFrame(rows).sort_values("maximum_drawdown_rupees").reset_index(drop=True)


def _build_cost_breakdown(trades: pd.DataFrame) -> pd.DataFrame:
    executed = trades.loc[trades["executed"]].copy()
    components = [
        "base_slippage_rupees",
        "ladder_impact_rupees",
        "volume_impact_rupees",
        "oi_impact_rupees",
        "impact_rupees",
        "slippage_rupees",
        "brokerage_rupees",
        "stt_rupees",
        "stamp_duty_rupees",
        "exchange_charges_rupees",
        "sebi_charges_rupees",
        "ipft_rupees",
        "gst_rupees",
        "charges_rupees",
        "total_cost_rupees",
    ]
    rows: list[dict[str, Any]] = []
    groups: list[tuple[str, pd.DataFrame]] = [("full_sample", executed)]
    groups.extend((str(split), group) for split, group in executed.groupby("split"))
    for split, group in groups:
        total = float(group["total_cost_rupees"].sum())
        for component in components:
            amount = float(group[component].sum())
            rows.append(
                {
                    "split": split,
                    "component": component,
                    "trades": len(group),
                    "rupees": amount,
                    "rupees_per_trade": amount / len(group),
                    "share_of_total_cost": amount / total if total else 0.0,
                }
            )
    return pd.DataFrame(rows)


def _profile_record(row: pd.Series) -> dict[str, Any]:
    fields = [
        "profile",
        "config_id",
        "margin_fraction",
        "max_risk_fraction",
        "score_floor",
        "score_power",
        "full_trades",
        "full_average_lots",
        "full_maximum_lots",
        "full_average_margin_utilization",
        "full_maximum_margin_utilization",
        "full_gross",
        "full_cost",
        "full_net",
        "full_turnover",
        "full_return",
        "full_cagr",
        "full_max_drawdown",
        "full_max_drawdown_pct",
        "full_cvar05_trade",
        "full_win_rate",
        "discovery_net",
        "validation_net",
        "confirmation_net",
        "holdout_net",
    ]
    return _json_safe({field: row[field] for field in fields})


def build_closeout(repo_root: Path) -> dict[str, Any]:
    """Build the machine-readable decision packet from frozen Phase 8-10 outputs."""
    profiles = pd.read_csv(repo_root / "audit/phase10_selected_profiles.csv")
    candidate = _recommended_profile(profiles)
    trades = _build_trade_sheet(repo_root)
    executed = trades.loc[trades["executed"]]
    rank = pd.read_csv(repo_root / "audit/phase9_rank_correlation.csv")
    holdout_rank = rank.loc[
        rank["score_type"].eq("regime_composite") & rank["split"].eq("holdout_2024_2026")
    ].iloc[0]
    phase9 = _read_json(repo_root / "audit/phase9_tearsheet.json")
    phase10 = _read_json(repo_root / "audit/phase10_tearsheet.json")

    recomputed = {
        "signals": int(len(trades)),
        "executed_trades": int(len(executed)),
        "average_lots": float(executed["lots"].mean()),
        "maximum_lots": int(executed["lots"].max()),
        "gross_pnl_rupees": float(executed["gross_pnl_rupees"].sum()),
        "total_cost_rupees": float(executed["total_cost_rupees"].sum()),
        "net_pnl_rupees": float(executed["net_pnl_rupees"].sum()),
        "turnover_rupees": float(executed["turnover_rupees"].sum()),
        "win_rate": float(executed["net_pnl_rupees"].gt(0).mean()),
        "profit_factor": float(
            executed.loc[executed["net_pnl_rupees"].gt(0), "net_pnl_rupees"].sum()
            / abs(executed.loc[executed["net_pnl_rupees"].lt(0), "net_pnl_rupees"].sum())
        ),
        "average_margin_utilization": float(executed["margin_utilization"].mean()),
        "maximum_margin_utilization": float(executed["margin_utilization"].max()),
        "average_cash_risk_utilization": float(executed["cash_risk_utilization"].mean()),
        "maximum_cash_risk_utilization": float(executed["cash_risk_utilization"].max()),
        "ending_equity_rupees": float(INITIAL_CAPITAL + executed["net_pnl_rupees"].sum()),
    }
    checks = {
        "trades_match": recomputed["executed_trades"] == int(candidate["full_trades"]),
        "gross_matches": math.isclose(
            recomputed["gross_pnl_rupees"], float(candidate["full_gross"]), abs_tol=0.01
        ),
        "cost_matches": math.isclose(
            recomputed["total_cost_rupees"], float(candidate["full_cost"]), abs_tol=0.01
        ),
        "net_matches": math.isclose(
            recomputed["net_pnl_rupees"], float(candidate["full_net"]), abs_tol=0.01
        ),
        "turnover_matches": math.isclose(
            recomputed["turnover_rupees"], float(candidate["full_turnover"]), abs_tol=0.01
        ),
        "margin_cap_respected": recomputed["maximum_margin_utilization"] <= 0.35 + 1e-12,
        "cash_risk_cap_respected": recomputed["maximum_cash_risk_utilization"] <= 0.04 + 1e-12,
    }
    if not all(checks.values()):
        raise ValueError(f"Module 4 reconciliation failed: {checks}")

    return {
        "schema_version": SCHEMA_VERSION,
        "decision": DECISION,
        "strategy_id": "upper85_gated_short_iron_fly_60m",
        "initial_capital_rupees": INITIAL_CAPITAL,
        "research_lineage": {
            "module3_decision": "standalone_short_horizon_VRP_hypothesis_rejected",
            "phase8": "gated_10_lakh_capital_diagnostic",
            "phase9": "confidence_rank_gate_failed",
            "phase10": "post_hoc_discovery_only_sizing_exploration",
        },
        "recommended_candidate": _profile_record(candidate),
        "recomputed_trade_sheet": recomputed,
        "reconciliation": checks,
        "confidence_rank_gate": {
            "verdict": phase9["pass_evaluation"]["verdict"],
            "combined_holdout_spearman": float(holdout_rank["rho_one_lot_net_pnl"]),
            "bootstrap_95_ci": [
                float(holdout_rank["rho_net_bootstrap_ci_low"]),
                float(holdout_rank["rho_net_bootstrap_ci_high"]),
            ],
            "one_sided_permutation_p": float(
                holdout_rank["net_permutation_p_one_sided"]
            ),
            "failed_gate": "combined_holdout_bootstrap_ci_low_above_zero",
        },
        "sizing_grid_robustness": phase10["robustness"],
        "interpretation": {
            "historical_result": "positive_after_exact_costs_and_entry_time_span",
            "allowed": "freeze_and_forward_shadow_test",
            "not_allowed": "deployment_or_clean_out_of_sample_claim",
        },
        "limitations": [
            "The confidence score did not pass its strict combined-holdout bootstrap gate.",
            "The score and regime directions are post-hoc to the available archive.",
            "The Phase 10 policy grid creates multiple-testing risk despite discovery-only selection.",
            "Drawdown and brakes use fixed-horizon end-of-trade equity, not intratrade MTM paths.",
            "SPAN is observed at entry; intratrade margin expansion and liquidation paths are unavailable.",
            "The rolling nearest-expiry ATM±10 archive does not support multi-day or multi-expiry claims.",
        ],
    }


def _fmt_rupees(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}₹{abs(value):,.0f}"


def _render_report(summary: dict[str, Any], episodes: pd.DataFrame) -> str:
    candidate = summary["recommended_candidate"]
    recomputed = summary["recomputed_trade_sheet"]
    robustness = summary["sizing_grid_robustness"]
    rank = summary["confidence_rank_gate"]
    deepest = episodes.iloc[0] if len(episodes) else None
    deepest_line = (
        f"{deepest['trough_date']} at {_fmt_rupees(float(deepest['maximum_drawdown_rupees']))} "
        f"({float(deepest['maximum_drawdown_pct']):.2%})"
        if deepest is not None
        else "none"
    )
    return f"""# Module 4 — sizing and risk-management closeout

## Decision

**Forward-shadow candidate; not deployment approved.** Module 3 rejected normalized intraday VRP
as a standalone 60–180 minute defined-risk entry rule. Module 4 preserves one post-hoc gated
upper-tail short-iron-fly candidate, tests whether confidence can rank its one-lot outcomes, and
then freezes a capital-efficient sizing rule. The historical economics are presentable, but the
Phase 9 rank bootstrap gate failed and the score is not clean out-of-sample evidence.

## Frozen strategy contract

- NIFTY nearest-weekly proxy; legs inside ATM±3 at entry.
- Upper-85 normalized intraday VRP crossing upward; 60-minute fixed exit.
- Short iron fly; exact integer lots and date-aware Groww charges.
- Base depth/staleness slippage plus quantity-aware ladder, volume, and OI impact.
- Timestamp-aware joined SPAN at entry; ATM IV substitutes for missing India VIX.
- Entry gates: IV 5m > −0.046817 vol points, IV 15m > −0.114873 vol points, and normalized RV 5m > −0.02651714.
- Frozen quality switch: confidence score strictly above 40%; hard switch (power 0).
- Sizing: at most 35% of current equity in entry SPAN and 4% in defined max loss plus the
  discovery q95 exact round-trip cost reserve; 76-lot capacity ceiling.
- No fitted drawdown or losing-streak brake.

## Recommended historical profile

| Metric | Result |
|---|---:|
| Candidate signals | {recomputed['signals']:,} |
| Executed trades | {recomputed['executed_trades']:,} |
| Average / maximum lots | {recomputed['average_lots']:.2f} / {recomputed['maximum_lots']} |
| Gross P&L | {_fmt_rupees(recomputed['gross_pnl_rupees'])} |
| Total costs | {_fmt_rupees(recomputed['total_cost_rupees'])} |
| Net P&L | {_fmt_rupees(recomputed['net_pnl_rupees'])} |
| Ending equity | {_fmt_rupees(recomputed['ending_equity_rupees'])} |
| Total return / CAGR | {float(candidate['full_return']):.2%} / {float(candidate['full_cagr']):.2%} |
| Win rate | {recomputed['win_rate']:.2%} |
| Profit factor | {recomputed['profit_factor']:.3f} |
| Maximum drawdown | {_fmt_rupees(float(candidate['full_max_drawdown']))} ({float(candidate['full_max_drawdown_pct']):.2%}) |
| 5% trade CVaR | {_fmt_rupees(float(candidate['full_cvar05_trade']))} |
| Turnover | {_fmt_rupees(recomputed['turnover_rupees'])} |
| Average / maximum margin use | {recomputed['average_margin_utilization']:.2%} / {recomputed['maximum_margin_utilization']:.2%} |
| Average / maximum cost-reserved risk | {recomputed['average_cash_risk_utilization']:.2%} / {recomputed['maximum_cash_risk_utilization']:.2%} |

Calendar net P&L: 2021–23 discovery {_fmt_rupees(float(candidate['discovery_net']))}; 2024
validation {_fmt_rupees(float(candidate['validation_net']))}; 2025–26 confirmation
{_fmt_rupees(float(candidate['confirmation_net']))}; combined later period
{_fmt_rupees(float(candidate['holdout_net']))}. The deepest daily-close drawdown episode bottoms on
{deepest_line}.

## What passed and what did not

The 17,640-policy sizing grid had {int(robustness['eligible_policy_count']):,} discovery-eligible
policies. {float(robustness['positive_holdout_rate']):.1%} were positive in the combined later
period, {float(robustness['positive_both_later_splits_rate']):.1%} were positive in both later
slices, and discovery-versus-holdout policy net rank correlation was
{float(robustness['discovery_holdout_net_spearman']):.3f}. The score-floor-40% / low-margin region
forms a useful neighborhood rather than a single isolated optimizer cell.

However, the frozen composite score's combined-holdout one-lot net-P&L Spearman rho was
{float(rank['combined_holdout_spearman']):.3f}, with bootstrap 95% interval
[{float(rank['bootstrap_95_ci'][0]):.3f}, {float(rank['bootstrap_95_ci'][1]):.3f}]. Because the lower
bound crosses zero, the sizing score did **not** pass its preregistered confidence gate. Positive
historical capital results therefore support only a frozen forward shadow test.

## Preserved evidence

- `trades/recommended_trade_sheet.csv`: all 132 signals, including 46 explicit skips, exact lots,
  binding cap, margin, cost-reserved loss, complete charge/slippage attribution, P&L, and equity.
- `curves/`: business-day equity, monthly returns, and drawdown episodes.
- `diagnostics/`: candidate profiles, cost breakdown, rank tests, score quintiles, regime results,
  and grid-neighborhood summaries.
- `exploration/sizing_grid.csv.gz`: the complete deterministic 17,640-policy grid.
- `visualizations/`: equity/drawdown, selected-profile frontier, and sizing-neighborhood figures.
- `manifest.json`: SHA-256 lineage over the contracts, implementations, source evidence, and results.

## Research boundary

These results do not observe intratrade MTM drawdown, SPAN expansion, forced liquidation, or
stop-loss performance. They cannot validate multi-day, multi-expiry, or horizons beyond the
rolling-chain coverage. Do not re-optimize the quality score or choose a new profile using the
same later-period outcomes. Freeze config 5628 and acquire untouched forward observations.

## Reproduce and verify

```powershell
python -m research.phase8.run_gated_capital_backtest
python -m research.phase9.run_confidence_sizing
python -m research.phase10.run_sizing_exploration
python -m research.module4_sizing_risk_management.run build
python -m research.module4_sizing_risk_management.run verify
python -m pytest tests/test_phase8_gated_capital.py tests/test_phase9_confidence_sizing.py tests/test_phase10_sizing_exploration.py tests/test_module4_sizing_risk_management.py -q
```

The Phase 8–10 reruns require the local gold observation Parquets documented in the runbook.
Module 4 itself rebuilds from the preserved compact audit outputs and exact-lot cost surface.
"""


def _svg_polyline(values: Iterable[float], x0: float, y0: float, width: float, height: float) -> str:
    data = np.asarray(list(values), dtype=float)
    if len(data) == 0:
        return ""
    low, high = float(np.nanmin(data)), float(np.nanmax(data))
    span = high - low if high > low else 1.0
    xs = np.linspace(x0, x0 + width, len(data))
    ys = y0 + height - (data - low) / span * height
    return " ".join(f"{x:.2f},{y:.2f}" for x, y in zip(xs, ys, strict=True))


def _write_equity_svg(path: Path, daily: pd.DataFrame) -> None:
    equity = daily["equity_rupees"].to_numpy()
    drawdown = daily["drawdown_pct"].to_numpy() * 100.0
    eq_points = _svg_polyline(equity, 95, 90, 1010, 280)
    dd_points = _svg_polyline(drawdown, 95, 455, 1010, 150)
    text = f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="680" viewBox="0 0 1200 680" role="img" aria-labelledby="title desc">
<title id="title">Recommended Module 4 equity and drawdown</title>
<desc id="desc">Business-day equity rises from one million rupees to {equity[-1]:.0f}; maximum daily-close drawdown is {drawdown.min():.2f} percent.</desc>
<rect width="1200" height="680" fill="#fbfaf7"/>
<text x="60" y="42" font-family="Arial,sans-serif" font-size="24" font-weight="500" fill="#20242a">Recommended profile: equity and underwater curve</text>
<text x="60" y="67" font-family="Arial,sans-serif" font-size="14" fill="#606770">₹10 lakh initial capital · fixed 60-minute exits · executed trades only</text>
<line x1="95" y1="90" x2="95" y2="370" stroke="#c7cbd1"/><line x1="95" y1="370" x2="1105" y2="370" stroke="#c7cbd1"/>
<polyline points="{eq_points}" fill="none" stroke="#126a62" stroke-width="3"/>
<text x="22" y="100" font-family="Arial,sans-serif" font-size="13" fill="#606770">₹{equity.max()/1e5:.2f}L</text>
<text x="22" y="370" font-family="Arial,sans-serif" font-size="13" fill="#606770">₹{equity.min()/1e5:.2f}L</text>
<text x="985" y="112" font-family="Arial,sans-serif" font-size="15" font-weight="500" fill="#20242a">End ₹{equity[-1]:,.0f}</text>
<line x1="95" y1="455" x2="95" y2="605" stroke="#c7cbd1"/><line x1="95" y1="455" x2="1105" y2="455" stroke="#c7cbd1"/>
<polyline points="{dd_points}" fill="none" stroke="#b14b4b" stroke-width="2.5"/>
<text x="26" y="464" font-family="Arial,sans-serif" font-size="13" fill="#606770">0%</text>
<text x="17" y="605" font-family="Arial,sans-serif" font-size="13" fill="#606770">{drawdown.min():.2f}%</text>
<text x="95" y="640" font-family="Arial,sans-serif" font-size="13" fill="#606770">{daily.iloc[0]['date']}</text>
<text x="1015" y="640" font-family="Arial,sans-serif" font-size="13" fill="#606770">{daily.iloc[-1]['date']}</text>
</svg>\n"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_frontier_svg(path: Path, profiles: pd.DataFrame) -> None:
    width, height = 1200, 680
    x0, y0, plot_w, plot_h = 105.0, 95.0, 980.0, 480.0
    returns = profiles["full_return"].to_numpy(dtype=float) * 100.0
    drawdowns = -profiles["full_max_drawdown_pct"].to_numpy(dtype=float) * 100.0
    x_min, x_max = 0.0, max(14.0, float(returns.max()) * 1.08)
    y_min, y_max = 0.0, max(1.6, float(drawdowns.max()) * 1.15)
    points: list[str] = []
    for row, ret, dd in zip(profiles.itertuples(index=False), returns, drawdowns, strict=True):
        x = x0 + (ret - x_min) / (x_max - x_min) * plot_w
        y = y0 + plot_h - (dd - y_min) / (y_max - y_min) * plot_h
        selected = row.profile == PROFILE
        fill = "#126a62" if selected else "#7c8794"
        radius = 9 if selected else 6
        label = "Recommended" if selected else ""
        points.append(
            f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius}" fill="{fill}"><title>{html.escape(str(row.profile))}: return {ret:.2f}%, drawdown {dd:.2f}%</title></circle>'
        )
        if label:
            points.append(
                f'<text x="{x + 14:.1f}" y="{y - 10:.1f}" font-family="Arial,sans-serif" font-size="14" font-weight="500" fill="#20242a">{label}: {ret:.2f}% / {dd:.2f}% DD</text>'
            )
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">
<title id="title">Selected-profile return and drawdown frontier</title>
<desc id="desc">Nine discovery-selected sizing profiles plotted by full-sample return and maximum drawdown; the recommended low-margin profile is highlighted.</desc>
<rect width="1200" height="680" fill="#fbfaf7"/>
<text x="60" y="42" font-family="Arial,sans-serif" font-size="24" font-weight="500" fill="#20242a">Selected sizing profiles: return versus drawdown</text>
<text x="60" y="68" font-family="Arial,sans-serif" font-size="14" fill="#606770">Higher return is right; lower drawdown is better. Historical profile comparison, not an efficient-frontier claim.</text>
<line x1="{x0}" y1="{y0}" x2="{x0}" y2="{y0 + plot_h}" stroke="#c7cbd1"/><line x1="{x0}" y1="{y0 + plot_h}" x2="{x0 + plot_w}" y2="{y0 + plot_h}" stroke="#c7cbd1"/>
{''.join(points)}
<text x="500" y="635" font-family="Arial,sans-serif" font-size="14" fill="#606770">Full-sample total return (%)</text>
<text x="18" y="360" transform="rotate(-90 18 360)" font-family="Arial,sans-serif" font-size="14" fill="#606770">Maximum drawdown magnitude (%)</text>
<text x="{x0}" y="603" font-family="Arial,sans-serif" font-size="12" fill="#606770">0%</text><text x="{x0 + plot_w - 25}" y="603" font-family="Arial,sans-serif" font-size="12" fill="#606770">{x_max:.0f}%</text>
<text x="65" y="{y0 + plot_h}" font-family="Arial,sans-serif" font-size="12" fill="#606770">0%</text><text x="55" y="{y0 + 8}" font-family="Arial,sans-serif" font-size="12" fill="#606770">{y_max:.1f}%</text>
</svg>\n"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg, encoding="utf-8")


def _write_neighborhood_svg(path: Path, dimensions: pd.DataFrame) -> None:
    wanted = dimensions.loc[
        dimensions["dimension"].isin(["score_floor", "margin_fraction"])
    ].copy()
    score = wanted.loc[wanted["dimension"].eq("score_floor")].sort_values("value")
    margin = wanted.loc[wanted["dimension"].eq("margin_fraction")].sort_values("value")

    def bars(frame: pd.DataFrame, x_start: float, panel_width: float) -> str:
        items: list[str] = []
        bar_width = panel_width / max(len(frame), 1) * 0.58
        gap = panel_width / max(len(frame), 1)
        for index, row in enumerate(frame.itertuples(index=False)):
            rate = float(row.positive_both_later_rate)
            h = rate * 360
            x = x_start + index * gap + gap * 0.2
            y = 540 - h
            fill = "#126a62" if math.isclose(float(row.value), 0.4) else "#718096"
            items.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{h:.1f}" fill="{fill}"/>')
            items.append(f'<text x="{x + bar_width/2:.1f}" y="{y - 9:.1f}" text-anchor="middle" font-family="Arial,sans-serif" font-size="13" fill="#20242a">{rate:.1%}</text>')
            items.append(f'<text x="{x + bar_width/2:.1f}" y="565" text-anchor="middle" font-family="Arial,sans-serif" font-size="12" fill="#606770">{float(row.value):.0%}</text>')
        return "".join(items)

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="680" viewBox="0 0 1200 680" role="img" aria-labelledby="title desc">
<title id="title">Sizing parameter neighborhood stability</title>
<desc id="desc">Share of discovery-eligible policies profitable in both later time slices, grouped by score floor and margin ceiling.</desc>
<rect width="1200" height="680" fill="#fbfaf7"/>
<text x="60" y="42" font-family="Arial,sans-serif" font-size="24" font-weight="500" fill="#20242a">Sizing neighborhoods: positive in both later periods</text>
<text x="60" y="68" font-family="Arial,sans-serif" font-size="14" fill="#606770">The 40% quality floor and lower margin ceilings form the more stable historical region.</text>
<text x="105" y="115" font-family="Arial,sans-serif" font-size="17" font-weight="500" fill="#20242a">Confidence score floor</text>
<text x="665" y="115" font-family="Arial,sans-serif" font-size="17" font-weight="500" fill="#20242a">Margin ceiling</text>
<line x1="105" y1="540" x2="545" y2="540" stroke="#c7cbd1"/><line x1="665" y1="540" x2="1105" y2="540" stroke="#c7cbd1"/>
{bars(score, 105, 440)}
{bars(margin, 665, 440)}
<text x="440" y="625" font-family="Arial,sans-serif" font-size="13" fill="#606770">Bars show eligible-policy rate, not strategy return.</text>
</svg>\n"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(svg, encoding="utf-8")


def _write_manifest(repo_root: Path) -> dict[str, Any]:
    members: list[dict[str, Any]] = []
    for relative in SOURCE_FILES:
        members.append(_member(repo_root, relative, "source_evidence"))
    for relative in IMPLEMENTATIONS:
        members.append(_member(repo_root, relative, "implementation"))
    for relative in DOCUMENTS:
        members.append(_member(repo_root, relative, "document_or_contract"))
    for relative in RESULT_FILES:
        members.append(_member(repo_root, relative, "generated_result"))
    manifest = {
        "schema_version": "module4-integrity-manifest/v1",
        "hashing": "sha256; CRLF normalized to LF for declared text suffixes",
        "member_count": len(members),
        "members": members,
    }
    path = repo_root / RESULT_ROOT / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def write_closeout(repo_root: Path) -> dict[str, Any]:
    """Write every Module 4 result artifact and its integrity manifest."""
    result_root = repo_root / RESULT_ROOT
    result_root.mkdir(parents=True, exist_ok=True)
    summary = build_closeout(repo_root)
    trades = _build_trade_sheet(repo_root)
    daily = _build_equity_curve(trades)
    monthly = _build_monthly_returns(daily)
    episodes = _build_drawdown_episodes(daily)
    profiles = pd.read_csv(repo_root / "audit/phase10_selected_profiles.csv")
    dimensions = pd.read_csv(repo_root / "audit/phase10_dimension_summary.csv")
    costs = _build_cost_breakdown(trades)

    _write_csv(repo_root / RESULT_ROOT / "trades/recommended_trade_sheet.csv", trades)
    _write_csv(repo_root / RESULT_ROOT / "curves/recommended_equity_curve.csv", daily)
    _write_csv(repo_root / RESULT_ROOT / "curves/recommended_monthly_returns.csv", monthly)
    _write_csv(repo_root / RESULT_ROOT / "curves/drawdown_episodes.csv", episodes)
    _write_csv(repo_root / RESULT_ROOT / "diagnostics/profile_comparison.csv", profiles)
    _write_csv(repo_root / RESULT_ROOT / "diagnostics/cost_breakdown.csv", costs)
    _write_csv(
        repo_root / RESULT_ROOT / "diagnostics/rank_correlation.csv",
        pd.read_csv(repo_root / "audit/phase9_rank_correlation.csv"),
    )
    _write_csv(
        repo_root / RESULT_ROOT / "diagnostics/score_quintiles.csv",
        pd.read_csv(repo_root / "audit/phase9_score_quintiles.csv"),
    )
    _write_csv(
        repo_root / RESULT_ROOT / "diagnostics/regime_summary.csv",
        pd.read_csv(repo_root / "audit/phase8_gated_regime_summary.csv"),
    )
    _write_csv(
        repo_root / RESULT_ROOT / "diagnostics/sizing_dimension_summary.csv", dimensions
    )
    _write_csv(repo_root / RESULT_ROOT / "exploration/selected_profiles.csv", profiles)
    _write_deterministic_csv_gz(
        repo_root / RESULT_ROOT / "exploration/sizing_grid.csv.gz",
        pd.read_csv(repo_root / "audit/phase10_sizing_grid.csv"),
    )
    _write_equity_svg(repo_root / RESULT_ROOT / "visualizations/equity_drawdown.svg", daily)
    _write_frontier_svg(
        repo_root / RESULT_ROOT / "visualizations/profile_frontier.svg", profiles
    )
    _write_neighborhood_svg(
        repo_root / RESULT_ROOT / "visualizations/sizing_neighborhoods.svg", dimensions
    )
    (repo_root / RESULT_ROOT / "closeout.json").write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (repo_root / RESULT_ROOT / "closeout_report.md").write_text(
        _render_report(summary, episodes), encoding="utf-8"
    )
    _write_manifest(repo_root)
    return summary


def verify_manifest(repo_root: Path) -> list[str]:
    """Return integrity failures for the saved Module 4 manifest."""
    path = repo_root / RESULT_ROOT / "manifest.json"
    if not path.exists():
        return [f"missing manifest: {path}"]
    manifest = _read_json(path)
    failures: list[str] = []
    for member in manifest.get("members", []):
        target = repo_root / member["path"]
        if not target.exists():
            failures.append(f"missing: {member['path']}")
            continue
        actual = _sha256(target)
        if actual != member["sha256"]:
            failures.append(
                f"hash mismatch: {member['path']} expected {member['sha256']} got {actual}"
            )
    if manifest.get("member_count") != len(manifest.get("members", [])):
        failures.append("manifest member_count does not match members")
    return failures
