"""Run 12 cost-inclusive upper-tail VRP-percentile crossing variants.

The experiment crosses the trailing five-minute median of the causal,
same-minute-of-day VRP percentile through 70/75/80/85/90/95 percent in both
directions.  Only the first event per session, threshold, and direction is
retained.  Every event enters the same ATM +/-3 short iron condor at the next
exact minute and exits the frozen contracts 60 minutes later.

This is a post-hypothesis diagnostic.  Threshold cells overlap and must not be
summed into a portfolio or interpreted as independent strategies.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from research.phase3.run_full_strategy_backtest import (
    CHARGE_FIELDS,
    HOLDING_MINUTES,
    SLIPPAGE_STRESS_MULTIPLIER,
    _add_slippage,
    _json_safe,
    _load_legbook,
    _strategy_summary,
    _trade_row,
    _validate_legbook,
)


SCHEMA_VERSION = "phase3-tail-percentile-backtest/v1"
THRESHOLDS = (0.70, 0.75, 0.80, 0.85, 0.90, 0.95)
ENTRY_CUTOFF = "14:15"
MINIMUM_SELL_FILL = 0.05


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_tail_events(
    curve_path: Path,
    *,
    thresholds: Iterable[float] = THRESHOLDS,
) -> pd.DataFrame:
    """Build the first daily crossing for each upper-tail threshold/direction."""

    columns = [
        "entry_ts",
        "trade_date",
        "entry_time",
        "signal_vrp_var_act365",
        "vrp_tod_percentile",
        "vrp_q5",
        "next_entry_ts",
        "next_entry_time",
        "next_short_pnl_points",
        "next_short_return_on_max_loss",
        "next_long_pnl_points",
        "next_long_return_on_max_loss",
    ]
    frame = pd.read_parquet(curve_path, columns=columns)
    frame["entry_ts"] = pd.to_datetime(frame["entry_ts"], utc=True)
    frame["next_entry_ts"] = pd.to_datetime(frame["next_entry_ts"], utc=True)
    frame["trade_date"] = frame["trade_date"].astype(str)
    frame = frame.sort_values(["trade_date", "entry_ts"]).reset_index(drop=True)

    grouped = frame.groupby("trade_date", sort=False)
    previous_q5 = grouped["vrp_q5"].shift(1)
    previous_ts = grouped["entry_ts"].shift(1)
    exact_previous = (frame["entry_ts"] - previous_ts).dt.total_seconds().eq(60)
    previous_q5 = previous_q5.where(exact_previous)

    event_frames: list[pd.DataFrame] = []
    for threshold in thresholds:
        threshold_value = float(threshold)
        directions = {
            "up": (previous_q5 < threshold_value) & (frame["vrp_q5"] >= threshold_value),
            "down": (previous_q5 > threshold_value) & (frame["vrp_q5"] <= threshold_value),
        }
        for direction, mask in directions.items():
            selected = frame.loc[mask].copy()
            selected["threshold"] = threshold_value
            selected["direction"] = direction
            event_frames.append(selected)

    if not event_frames:
        raise ValueError("no percentile thresholds supplied")
    events = pd.concat(event_frames, ignore_index=True)
    next_delay = (events["next_entry_ts"] - events["entry_ts"]).dt.total_seconds() / 60.0
    events = events.loc[
        next_delay.eq(1.0)
        & events["next_entry_time"].le(ENTRY_CUTOFF)
        & events["next_short_pnl_points"].notna()
        & events["next_long_pnl_points"].notna()
    ].copy()
    events = events.sort_values(["trade_date", "threshold", "direction", "entry_ts"])
    events = events.drop_duplicates(["trade_date", "threshold", "direction"], keep="first")
    events = events.sort_values(["threshold", "direction", "trade_date"]).reset_index(drop=True)
    events["trade_id"] = np.arange(1, len(events) + 1, dtype=np.int64)
    events["signal_ts"] = events["entry_ts"]
    events["entry_ts"] = events["next_entry_ts"]
    events["exit_ts"] = events["entry_ts"] + pd.Timedelta(minutes=HOLDING_MINUTES)
    events["execution_entry_time"] = events["next_entry_time"].astype(str)
    events["signal_vrp_percentile_raw"] = events["vrp_tod_percentile"]
    events["signal_vrp_percentile_q5"] = events["vrp_q5"]

    # Compatibility names used by the frozen Phase-3 leg loader/reconciler.
    events["vrp_tod_percentile"] = events["signal_vrp_percentile_q5"]
    events["next_short_iron_condor__pnl_points"] = events["next_short_pnl_points"]
    events["next_short_iron_condor__return_on_max_loss"] = events[
        "next_short_return_on_max_loss"
    ]
    events["next_long_iron_condor__pnl_points"] = events["next_long_pnl_points"]
    events["next_long_iron_condor__return_on_max_loss"] = events[
        "next_long_return_on_max_loss"
    ]
    return events[
        [
            "trade_id",
            "threshold",
            "direction",
            "trade_date",
            "signal_ts",
            "entry_ts",
            "exit_ts",
            "entry_time",
            "execution_entry_time",
            "signal_vrp_var_act365",
            "signal_vrp_percentile_raw",
            "signal_vrp_percentile_q5",
            "vrp_tod_percentile",
            "next_short_iron_condor__pnl_points",
            "next_short_iron_condor__return_on_max_loss",
            "next_long_iron_condor__pnl_points",
            "next_long_iron_condor__return_on_max_loss",
        ]
    ]


def _variant_id(threshold: float, direction: str) -> str:
    return f"q{int(round(float(threshold) * 100)):02d}_{direction}"


def _variant_table(tradebook: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (threshold, direction), part in tradebook.groupby(
        ["threshold", "direction"], sort=True
    ):
        base = _strategy_summary(part, "base")
        stress = _strategy_summary(part, "stress_1_5x")
        rows.append(
            {
                "variant": _variant_id(float(threshold), str(direction)),
                "threshold": float(threshold),
                "direction": str(direction),
                "base": base,
                "stress_1_5x": stress,
                "base_positive_net_mean": bool(base["net_pnl_rupees"]["mean"] > 0.0),
                "base_mean_rom_ci_excludes_zero_positive": bool(
                    base["mean_net_return_on_margin_bootstrap_95"][0] > 0.0
                ),
            }
        )
    return rows


def _render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Phase 3 upper-tail percentile crossing tear sheet",
        "",
        "## Decision",
        "",
        str(summary["decision"]),
        "",
        "This is a post-hypothesis, in-sample diagnostic. Each row trades one historical exchange "
        "lot, with no compounding, capital pool, leverage rule, or portfolio risk overlay. Cells "
        "overlap across thresholds and must not be summed as independent strategies.",
        "",
        "## Frozen experiment",
        "",
        "- Signal curve: trailing five-minute median of the causal same-minute-of-day normalized-VRP percentile.",
        "- Thresholds: 70%, 75%, 80%, 85%, 90%, and 95%.",
        "- Directions: first daily strict crossing upward and first daily strict crossing downward.",
        "- Execution: next exact minute, no later than 14:15 IST.",
        "- Structure: the same ATM +/-3 short iron condor for both directions.",
        "- Exit: fixed contracts after exactly 60 minutes.",
        "- Frictions: pinned Groww charges, volume/OI slippage, ATM-IV fallback for missing India VIX, and timestamp-scheduled SPAN margin.",
        "",
        "## Base one-lot results",
        "",
        "| Variant | Trades | Gross mean pts | Gross total | Mean cost | Net total | Net mean | Win rate | Mean net ROM | ROM 95% CI | Net CVaR 5% | Positive months |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summary["variants"]:
        base = row["base"]
        ci = base["mean_net_return_on_margin_bootstrap_95"]
        lines.append(
            "| {variant} | {trades:,} | {gross_points:+.3f} | Rs {gross_total:,.2f} | "
            "Rs {mean_cost:,.2f} | Rs {net_total:,.2f} | Rs {net_mean:,.2f} | "
            "{win:.2%} | {rom:+.4%} | [{ci0:+.4%}, {ci1:+.4%}] | "
            "Rs {cvar:,.2f} | {months:.2%} |".format(
                variant=row["variant"],
                trades=base["trades"],
                gross_points=base["gross_pnl_points"]["mean"],
                gross_total=base["gross_pnl_rupees"]["sum"],
                mean_cost=base["costs"]["mean_cost_per_trade"],
                net_total=base["net_pnl_rupees"]["sum"],
                net_mean=base["net_pnl_rupees"]["mean"],
                win=base["net_pnl_rupees"]["win_rate"],
                rom=base["net_return_on_margin"]["mean"],
                ci0=ci[0],
                ci1=ci[1],
                cvar=base["net_pnl_rupees"]["cvar05"],
                months=base["monthly_stability"]["positive_period_share"],
            )
        )
    lines.extend(
        [
            "",
            "## Interpretation rules",
            "",
            "A variant is not promoted merely because its sample net mean is positive. Promotion "
            "requires a positive cost-inclusive mean, a bootstrap mean-ROM interval above zero, "
            "reasonable tail loss and concentration, stability across years/months, and genuinely "
            "prospective out-of-sample confirmation. The best row in this table is post-selected.",
            "",
            "## Model limitations",
            "",
        ]
    )
    lines.extend(f"- {item}" for item in summary["limitations"])
    lines.extend(
        [
            "",
            "## Reproducibility",
            "",
            "The CSV tradebook and legbook retain the exact contracts, entry/exit observations, "
            "cost components, slippage components, historical lot size, and SPAN slot. The JSON "
            "summary and manifest contain the machine-readable results and SHA-256 evidence.",
            "",
        ]
    )
    return "\n".join(lines)


def run_backtest(
    *,
    gold_root: Path,
    curve_path: Path,
    surface_path: Path,
    tradebook_path: Path,
    legbook_path: Path,
    summary_path: Path,
    manifest_path: Path,
    report_path: Path,
) -> dict[str, Any]:
    gold_glob = str(gold_root / "year=*" / "month=*" / "part-*.parquet")
    events = build_tail_events(curve_path)
    raw_legs = _load_legbook(events, gold_glob, surface_path)
    validation = _validate_legbook(raw_legs, len(events))
    legbook = _add_slippage(raw_legs)

    rows: list[dict[str, Any]] = []
    for trade_id, part in legbook.groupby("trade_id", sort=True):
        trade = _trade_row(
            part,
            strategy="short_iron_condor",
            weight_sign=1,
            minimum_sell_fill=MINIMUM_SELL_FILL,
        )
        event = events.loc[events["trade_id"].eq(trade_id)].iloc[0]
        trade["threshold"] = float(event["threshold"])
        trade["direction"] = str(event["direction"])
        trade["variant"] = _variant_id(trade["threshold"], trade["direction"])
        trade["signal_vrp_percentile_raw"] = float(event["signal_vrp_percentile_raw"])
        trade["signal_vrp_percentile_q5"] = float(event["signal_vrp_percentile_q5"])
        rows.append(trade)
    tradebook = pd.DataFrame(rows).sort_values(["threshold", "direction", "trade_date"])

    expected = events.set_index("trade_id")["next_short_iron_condor__pnl_points"]
    reconciliation = (
        tradebook["gross_pnl_points"]
        - expected.loc[tradebook["trade_id"]].to_numpy(dtype=float)
    ).abs()
    entry_sell_floor = (
        legbook["primary_entry_side"].eq("SELL")
        & (
            legbook["entry_close"] - legbook["entry_slippage_per_unit"]
            < MINIMUM_SELL_FILL
        )
    )
    exit_sell_floor = (
        legbook["primary_exit_side"].eq("SELL")
        & (
            legbook["exit_close"] - legbook["exit_slippage_per_unit"]
            < MINIMUM_SELL_FILL
        )
    )
    stress_entry_sell_floor = (
        legbook["primary_entry_side"].eq("SELL")
        & (
            legbook["entry_close"]
            - SLIPPAGE_STRESS_MULTIPLIER * legbook["entry_slippage_per_unit"]
            < MINIMUM_SELL_FILL
        )
    )
    stress_exit_sell_floor = (
        legbook["primary_exit_side"].eq("SELL")
        & (
            legbook["exit_close"]
            - SLIPPAGE_STRESS_MULTIPLIER * legbook["exit_slippage_per_unit"]
            < MINIMUM_SELL_FILL
        )
    )
    variants = _variant_table(tradebook)
    positive = [row["variant"] for row in variants if row["base_positive_net_mean"]]
    confirmed = [
        row["variant"]
        for row in variants
        if row["base_mean_rom_ci_excludes_zero_positive"]
    ]
    ranked = sorted(
        variants,
        key=lambda row: row["base"]["net_pnl_rupees"]["mean"],
        reverse=True,
    )
    decision = (
        "No variant has a positive cost-inclusive sample mean."
        if not positive
        else (
            f"Positive sample net mean: {', '.join(positive)}. "
            "These post-selected cells require prospective confirmation."
        )
    )
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "research_status": "post_hypothesis_in_sample_diagnostic",
        "decision": decision,
        "experiment": {
            "thresholds": list(THRESHOLDS),
            "directions": ["up", "down"],
            "variants": len(THRESHOLDS) * 2,
            "signal_curve": (
                "trailing five-minute median of the causal same-minute-of-day "
                "normalized-VRP percentile"
            ),
            "crossing_rule": (
                "up: q5(t-1)<threshold and q5(t)>=threshold; down: "
                "q5(t-1)>threshold and q5(t)<=threshold"
            ),
            "daily_rule": "first event per trade_date, threshold, and direction",
            "structure": "same ATM +/-3 short iron condor for both directions",
            "entry": "next exact minute, no later than 14:15 IST",
            "holding_minutes": HOLDING_MINUTES,
            "position_size": "one historical exchange lot per event",
            "slippage_stress_multiplier": SLIPPAGE_STRESS_MULTIPLIER,
            "minimum_sell_fill": MINIMUM_SELL_FILL,
        },
        "coverage": {
            **validation,
            "variants_observed": int(
                tradebook[["threshold", "direction"]].drop_duplicates().shape[0]
            ),
            "unique_trade_dates": int(tradebook["trade_date"].nunique()),
            "first_trade": str(tradebook["trade_date"].min()),
            "last_trade": str(tradebook["trade_date"].max()),
            "maximum_gross_reconciliation_error_points": float(reconciliation.max()),
            "entry_vix_fallback_leg_rows": int(legbook["entry_vix_fallback"].sum()),
            "exit_vix_fallback_leg_rows": int(legbook["exit_vix_fallback"].sum()),
            "entry_stale_penalty_leg_rows": int(
                (legbook["entry_stale_multiplier"] > 1.0).sum()
            ),
            "exit_stale_penalty_leg_rows": int(
                (legbook["exit_stale_multiplier"] > 1.0).sum()
            ),
            "base_entry_minimum_sell_fill_leg_rows": int(entry_sell_floor.sum()),
            "base_exit_minimum_sell_fill_leg_rows": int(exit_sell_floor.sum()),
            "base_trades_with_minimum_sell_fill": int(
                legbook.loc[entry_sell_floor | exit_sell_floor, "trade_id"].nunique()
            ),
            "stress_entry_minimum_sell_fill_leg_rows": int(
                stress_entry_sell_floor.sum()
            ),
            "stress_exit_minimum_sell_fill_leg_rows": int(stress_exit_sell_floor.sum()),
            "stress_trades_with_minimum_sell_fill": int(
                legbook.loc[
                    stress_entry_sell_floor | stress_exit_sell_floor, "trade_id"
                ].nunique()
            ),
            "span_bod_open_time_assumption_leg_rows": int(
                legbook["span_bod_open_time_assumption"].fillna(False).sum()
            ),
        },
        "variants": variants,
        "ranking_by_base_mean_net_pnl": [row["variant"] for row in ranked],
        "positive_sample_net_mean_variants": positive,
        "positive_mean_rom_ci_variants": confirmed,
        "cost_components": list(CHARGE_FIELDS),
        "limitations": [
            "All threshold choices and observations are in-sample and post-hypothesis; no row is prospective OOS evidence.",
            "Threshold variants overlap in dates and sometimes entry times; they are dependent diagnostics, not an additive portfolio.",
            "Historical bid/ask is unavailable; fills use the pinned volume/OI synthetic slippage model.",
            "The pinned slippage function has no submitted-order participation input, so the present experiment is restricted to one lot.",
            "When modeled slippage would put a sell fill below the model's minimum option tick, the fill is conservatively floored at Rs 0.05 and explicitly counted.",
            "The slippage stale multiplier is a low volume/OI-turnover proxy, not elapsed quote age.",
            "SPAN uses the six-slot research reference schedule; BOD is assumed available at 09:15 until ID1.",
            "The Dhan rolling WEEK history is a nearest-listed-expiry proxy and does not prove actual nearest-weekly identity.",
            "Missing exact-contract exits are not imputed; the event curve only retains complete next-minute 60-minute paths.",
        ],
    }

    for path in (
        tradebook_path,
        legbook_path,
        summary_path,
        manifest_path,
        report_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    tradebook.to_csv(tradebook_path, index=False)
    legbook.to_csv(legbook_path, index=False)
    summary_path.write_text(
        json.dumps(_json_safe(summary), indent=2, sort_keys=True), encoding="utf-8"
    )
    report_path.write_text(_render_markdown(summary), encoding="utf-8")
    manifest = {
        "schema_version": "phase3-tail-percentile-manifest/v1",
        "code": {"path": str(Path(__file__).resolve()), "sha256": _sha256(Path(__file__))},
        "inputs": [
            {"path": str(curve_path.resolve()), "sha256": _sha256(curve_path)},
            {"path": str(surface_path.resolve()), "sha256": _sha256(surface_path)},
        ],
        "outputs": [
            {"path": str(tradebook_path.resolve()), "sha256": _sha256(tradebook_path)},
            {"path": str(legbook_path.resolve()), "sha256": _sha256(legbook_path)},
            {"path": str(summary_path.resolve()), "sha256": _sha256(summary_path)},
            {"path": str(report_path.resolve()), "sha256": _sha256(report_path)},
        ],
        "gold_root": str(gold_root.resolve()),
        "event_trades": int(len(tradebook)),
        "leg_rows": int(len(legbook)),
        "variant_count": int(len(variants)),
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
        default=Path("audit/phase3_tail_percentile_tradebook.csv"),
    )
    parser.add_argument(
        "--legbook",
        type=Path,
        default=Path("audit/phase3_tail_percentile_legbook.csv"),
    )
    parser.add_argument(
        "--summary",
        type=Path,
        default=Path("audit/phase3_tail_percentile_tearsheet.json"),
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("audit/phase3_tail_percentile_manifest.json"),
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("docs/research/PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md"),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    summary = run_backtest(
        gold_root=args.gold_root,
        curve_path=args.curve_path,
        surface_path=args.surface_path,
        tradebook_path=args.tradebook,
        legbook_path=args.legbook,
        summary_path=args.summary,
        manifest_path=args.manifest,
        report_path=args.report,
    )
    print(json.dumps(_json_safe(summary), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
