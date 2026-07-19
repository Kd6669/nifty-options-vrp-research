"""Build the deterministic Module 3 VRP hypothesis-testing closeout packet."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "module3-vrp-hypothesis-closeout/v1"
DECISION = "REJECT_INTRADAY_VRP_AS_STANDALONE_DEFINED_RISK_ENTRY_RULE_FOR_CURRENT_DATASET"

SOURCE_JSONS = {
    "curve_dynamics": "audit/phase2_vrp_curve_crossings.json",
    "zero_crossing": "audit/phase3_full_strategy_tearsheet.json",
    "tail_crossing": "audit/phase3_tail_percentile_tearsheet.json",
    "structure_horizon": "audit/phase4_cost_aware_summary.json",
    "multiday_feasibility": "audit/phase4_multiday_summary.json",
    "feature_rescue": "audit/phase5_final_attempt_summary.json",
    "tail_reversal": "audit/phase6_reversal_summary.json",
    "unified_180min": "audit/phase7_180min_summary.json",
}

SOURCE_MANIFESTS = [
    "audit/phase2_hypothesis_evidence_manifest.json",
    "audit/phase3_full_strategy_manifest.json",
    "audit/phase3_tail_percentile_manifest.json",
    "audit/phase4_cost_aware_manifest.json",
    "audit/phase4_multiday_manifest.json",
    "audit/phase5_final_attempt_dataset_manifest.json",
    "audit/phase5_final_attempt_manifest.json",
    "audit/phase6_reversal_manifest.json",
    "audit/phase7_180min_manifest.json",
]

IMPLEMENTATIONS = [
    "src/nifty_execution/__init__.py",
    "src/nifty_execution/costs.py",
    "src/nifty_execution/margin.py",
    "src/nifty_execution/provenance.py",
    "src/nifty_execution/slippage.py",
    "research/phase3/run_full_strategy_backtest.py",
    "research/phase3/run_tail_percentile_backtests.py",
    "research/phase4/run_cost_aware_discovery.py",
    "research/phase4/run_multiday_vrp_feasibility.py",
    "research/phase5/build_final_attempt_dataset.py",
    "research/phase5/run_final_attempt_strategy.py",
    "research/phase6/run_vrp_reversal_test.py",
    "research/phase7/run_180min_signal_comparison.py",
    "research/module3_hypothesis_testing/__init__.py",
    "research/module3_hypothesis_testing/closeout.py",
    "research/module3_hypothesis_testing/run.py",
]

DOCUMENTS = [
    ".gitattributes",
    "research/module3_hypothesis_testing/README.md",
    "research/module3_hypothesis_testing/MODULE_MANIFEST.md",
    "research/module3_hypothesis_testing/module.yaml",
    "research/module3_hypothesis_testing/contracts/module.yaml",
    "research/module3_hypothesis_testing/contracts/hypotheses.json",
    "research/module3_hypothesis_testing/docs/module.yaml",
    "research/module3_hypothesis_testing/docs/architecture.md",
    "research/module3_hypothesis_testing/docs/runbook.md",
    "research/module3_hypothesis_testing/results/module.yaml",
    "research/module3_hypothesis_testing/scripts/module.yaml",
    "research/module3_hypothesis_testing/scripts/run_closeout.ps1",
    "docs/research/FINAL_HYPOTHESIS.md",
    "docs/research/PHASE3_FULL_STRATEGY_TEAR_SHEET.md",
    "docs/research/PHASE3_TAIL_PERCENTILE_TEAR_SHEET.md",
    "docs/research/PHASE4_COST_AWARE_VRP_DISCOVERY.md",
    "docs/research/PHASE5_FINAL_ATTEMPT_PROTOCOL.md",
    "docs/research/PHASE5_FINAL_ATTEMPT_RESULTS.md",
    "docs/research/PHASE6_VRP_REVERSAL_PROTOCOL.md",
    "docs/research/PHASE6_VRP_REVERSAL_RESULTS.md",
    "docs/research/PHASE7_180MIN_COMPARISON_PROTOCOL.md",
    "docs/research/PHASE7_180MIN_COMPARISON_RESULTS.md",
]


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    # Every Module 3 manifest member is text. Canonical LF hashing makes the top-level
    # integrity packet portable across Git checkouts with different core.autocrlf settings.
    payload = path.read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _phase3_economics(block: dict[str, Any]) -> dict[str, Any]:
    return {
        "trades": int(block["trades"]),
        "gross_mean_rupees": float(block["gross_pnl_rupees"]["mean"]),
        "cost_mean_rupees": float(block["costs"]["mean_cost_per_trade"]),
        "net_mean_rupees": float(block["net_pnl_rupees"]["mean"]),
        "net_total_rupees": float(block["net_pnl_rupees"]["sum"]),
        "net_win_rate": float(block["net_pnl_rupees"]["win_rate"]),
        "mean_net_return_on_margin": float(block["net_return_on_margin"]["mean"]),
        "mean_net_rom_bootstrap_95": block["mean_net_return_on_margin_bootstrap_95"],
    }


def _cell_economics(cell: dict[str, Any]) -> dict[str, Any]:
    net_key = "net_pnl_rupees" if "net_pnl_rupees" in cell else "net_rupees"
    gross_key = "gross_pnl_rupees" if "gross_pnl_rupees" in cell else "gross_rupees"
    result = {
        "trades": int(cell["trades"]),
        "gross_mean_rupees": float(cell[gross_key]["mean"]),
        "net_mean_rupees": float(cell[net_key]["mean"]),
        "net_win_rate": float(cell[net_key]["win_rate"]),
        "mean_net_return_on_margin": float(cell["net_return_on_margin"]["mean"]),
    }
    if "cost_rupees" in cell:
        result["cost_mean_rupees"] = float(cell["cost_rupees"]["mean"])
    if "coverage" in cell:
        result["coverage"] = float(cell["coverage"])
    if "bootstrap_mean_net_95pct_ci" in cell:
        result["bootstrap_mean_net_95pct_ci"] = cell["bootstrap_mean_net_95pct_ci"]
    return result


def _phase6_cells(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "mapping": cell["mapping"],
            "horizon_minutes": int(cell["horizon_minutes"]),
            **_cell_economics(cell),
        }
        for cell in cells
    ]


def _phase7_cells(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "mapping": cell["mapping"],
            "signal_group": cell["signal_group"],
            "signal_name": cell["signal_name"],
            "expected_events": int(cell["expected_events"]),
            "unevaluated_events": int(
                cell.get("unevaluated_events", cell["expected_events"] - cell["trades"])
            ),
            **_cell_economics(cell),
        }
        for cell in cells
    ]


def build_closeout(repo_root: Path) -> dict[str, Any]:
    """Read frozen phase evidence and return the closed Module 3 decision packet."""
    evidence = {
        name: _load(repo_root / relative_path) for name, relative_path in SOURCE_JSONS.items()
    }

    phase3 = evidence["zero_crossing"]
    zero_crossing = {
        "hypothesis_id": phase3["hypothesis_id"],
        "status": phase3["economic_result"],
        "requested_short_condor": _phase3_economics(phase3["primary"]["base"]),
        "inverse_long_condor": _phase3_economics(phase3["exact_inverse"]["base"]),
    }

    tail_cells = []
    for variant in evidence["tail_crossing"]["variants"]:
        tail_cells.append(
            {
                "signal_name": variant["variant"],
                "threshold": float(variant["threshold"]),
                "direction": variant["direction"],
                **_phase3_economics(variant["base"]),
            }
        )

    selected_features = {
        "vrp_tod_percentile",
        "vrp_q5",
        "q_velocity_5m",
        "q_acceleration_5m",
        "vrp_velocity_5m",
        "vrp_acceleration_5m",
    }
    dynamics = [
        {
            "structure": row["structure"],
            "feature": row["feature"],
            "observations": int(row["observations"]),
            "spearman": float(row["spearman"]),
            "pearson": float(row["pearson"]),
        }
        for row in evidence["curve_dynamics"]["minute_grid_correlations"]
        if row["state"] == "all" and row["feature"] in selected_features
    ]

    phase4_cells = evidence["structure_horizon"]["cells"]
    phase4_ranked = sorted(phase4_cells, key=lambda row: row["net_rupees"]["mean"], reverse=True)
    phase4 = {
        "cells_tested": len(phase4_cells),
        "structures": sorted({row["structure"] for row in phase4_cells}),
        "horizons_minutes": sorted({int(row["horizon_minutes"]) for row in phase4_cells}),
        "positive_net_mean_cells": sum(row["net_rupees"]["mean"] > 0 for row in phase4_cells),
        "best_cell": {
            "signal_family": phase4_ranked[0]["signal_family"],
            "structure": phase4_ranked[0]["structure"],
            "horizon_minutes": int(phase4_ranked[0]["horizon_minutes"]),
            **_cell_economics(phase4_ranked[0]),
        },
        "corrected_upper85_short_condor_60m": next(
            {
                "signal_family": row["signal_family"],
                "structure": row["structure"],
                "horizon_minutes": int(row["horizon_minutes"]),
                **_cell_economics(row),
            }
            for row in phase4_cells
            if row["signal_family"] == "upper85_up"
            and row["structure"] == "short_iron_condor"
            and row["horizon_minutes"] == 60
        ),
        "multiday_interpretation_gate": evidence["multiday_feasibility"][
            "interpretation_gate"
        ],
    }

    phase5 = evidence["feature_rescue"]
    phase5_result = {
        "decision": phase5["decision"],
        "confirmation": phase5["confirmation"],
        "acceptance": phase5["acceptance"],
    }

    phase6 = evidence["tail_reversal"]
    reversal = {
        "decision": phase6["decision"],
        "events": phase6["events"],
        "cells": _phase6_cells(phase6["cells"]),
        "subgroups": phase6["subgroups"],
        "primary_acceptance": phase6["primary_acceptance"],
    }

    phase7 = evidence["unified_180min"]
    unified_cells = _phase7_cells(phase7["cells"])
    material_gross = sorted(
        [cell for cell in unified_cells if cell["gross_mean_rupees"] >= 65.0],
        key=lambda cell: cell["gross_mean_rupees"],
        reverse=True,
    )
    unified = {
        "decision": phase7["decision"],
        "horizon_minutes": int(phase7["horizon_minutes"]),
        "membership_rows": int(phase7["membership_rows"]),
        "unique_entry_timestamps": int(phase7["unique_entry_timestamps"]),
        "credible_positive_cell_count": len(phase7["credible_positive_cells"]),
        "cells": unified_cells,
        "positive_gross_cells_at_least_65_rupees": material_gross,
    }

    return {
        "schema_version": SCHEMA_VERSION,
        "module_id": "module3_vrp_hypothesis_testing",
        "status": "closed",
        "decision": DECISION,
        "research_universe": {
            "underlying": "NIFTY nearest-weekly options",
            "entry_moneyness": "ATM plus or minus 3",
            "position_size": "one historical exchange lot per completed trade",
            "primary_horizon_minutes": 60,
            "maximum_diagnostic_horizon_minutes": 180,
            "structures": "defined-risk only",
        },
        "tests": {
            "zero_crossing": zero_crossing,
            "tail_crossing_level_direction": {
                "decision": evidence["tail_crossing"]["decision"],
                "cells": tail_cells,
            },
            "velocity_acceleration": {
                "coverage": evidence["curve_dynamics"]["coverage"],
                "minute_grid_correlations": dynamics,
                "paired_ladder_comparisons": evidence["curve_dynamics"][
                    "paired_session_ladder_comparisons"
                ],
            },
            "alternative_structures_horizons": phase4,
            "causal_feature_rescue": phase5_result,
            "tail_mean_reversion": reversal,
            "unified_180min": unified,
        },
        "economic_interpretation": {
            "observed": (
                "Several 180-minute cells reach positive gross means above Rs65 per trade, "
                "showing that selected payoffs become materially larger at the longest observable "
                "horizon. They remain inconsistent, mostly low-coverage, and insufficient to clear "
                "roughly Rs249-Rs311 average round-trip execution costs."
            ),
            "closed_claim": (
                "Within the observable intraday window, standalone VRP signals do not produce a "
                "defined-risk payoff that matures or is realized consistently enough to cover "
                "one-lot per-trade execution costs."
            ),
            "causal_caution": (
                "The tests reject economic viability for these rules; they do not prove that VRP "
                "has no slower-horizon information."
            ),
        },
        "data_coverage_boundary": {
            "conclusion": (
                "Research beyond 180 minutes or across sessions is outside the current defensible "
                "scope because frozen strikes increasingly leave the rolling ATM plus or minus 10 "
                "surface and missingness becomes non-random."
            ),
            "required_next_data": (
                "Full fixed-contract, actual-expiry chain history with sufficiently long intraday "
                "and multi-session paths."
            ),
            "no_extrapolation": True,
            "top_reversal_180min": {
                "signals": 191,
                "evaluated_trades": 96,
                "unevaluated_events": 95,
                "coverage": 96 / 191,
                "treatment": "unevaluated events are neither wins, losses, nor imputed trades",
            },
        },
        "source_artifacts": SOURCE_JSONS,
    }


def _money(value: float) -> str:
    sign = "+" if value >= 0 else "−"
    return f"{sign}₹{abs(value):,.2f}"


def render_report(summary: dict[str, Any]) -> str:
    """Render a concise human closeout from the machine packet."""
    zero = summary["tests"]["zero_crossing"]
    tail = summary["tests"]["tail_crossing_level_direction"]["cells"]
    tail_best = max(tail, key=lambda row: row["net_mean_rupees"])
    phase4 = summary["tests"]["alternative_structures_horizons"]
    phase5 = summary["tests"]["causal_feature_rescue"]
    reversal = summary["tests"]["tail_mean_reversion"]
    unified = summary["tests"]["unified_180min"]

    lines = [
        "# Module 3 — VRP hypothesis-testing closeout",
        "",
        "## Decision",
        "",
        "**REJECT the tested intraday standalone-VRP defined-risk strategy family for the current "
        "dataset.**",
        "",
        "This closes the economic testing layer. IV and RV were independently normalized onto the "
        "intraday clock before constructing VRP; provider chain IV was not accepted at face value. "
        "Every economic result below is one historical exchange lot per completed trade, with dated "
        "charges, volume/OI slippage, ATM-IV fallback when India VIX is absent, conservative fills, "
        "and timestamp-aware SPAN margin.",
        "",
        "## Test sequence",
        "",
        "| Test | Main evidence | Decision |",
        "|---|---|---|",
        (
            "| Zero crossing | 60m short condor: "
            f"{_money(zero['requested_short_condor']['gross_mean_rupees'])} gross, "
            f"{_money(zero['requested_short_condor']['net_mean_rupees'])} net over "
            f"{zero['requested_short_condor']['trades']} trades | Rejected after costs |"
        ),
        (
            "| Tail level and direction | Best of 12 at 60m: "
            f"{tail_best['signal_name']} at {_money(tail_best['net_mean_rupees'])} net/trade; "
            "dated-STT rerun −₹169.32 | All 12 rejected |"
        ),
        "| Velocity and acceleration | Rank correlations are near zero and deeper crossings fail "
        "paired-session ordering | Rejected as confidence/leverage rule |",
        (
            f"| Structures and horizons | {phase4['positive_net_mean_cells']} positive-net cells "
            f"out of {phase4['cells_tested']} | No structure/horizon rescue |"
        ),
        (
            f"| Causal feature rescue | {phase5['confirmation']['trades']} locked-confirmation "
            f"trades, {_money(phase5['confirmation']['mean_net_pnl_rupees'])} net/trade | Rejected |"
        ),
        (
            f"| Tail mean reversion | {reversal['events']['total']} frozen events; requested and "
            "aggregate inverse mappings fail | Rejected |"
        ),
        (
            f"| Unified 180m | {unified['credible_positive_cell_count']} credible positive cells "
            "out of 32 requested/inverse cells | No credible edge |"
        ),
        "",
        "## The material 180-minute gross results",
        "",
        "The longest observable horizon does produce several materially larger gross means:",
        "",
        "| Mapping | Signal | Signals | Evaluated | Unevaluated | Coverage | Gross/trade | "
        "Cost/trade | Net/trade |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for cell in unified["positive_gross_cells_at_least_65_rupees"]:
        lines.append(
            f"| {cell['mapping']} | {cell['signal_name']} | {cell['expected_events']} | "
            f"{cell['trades']} | {cell['unevaluated_events']} | "
            f"{100 * cell['coverage']:.2f}% | {_money(cell['gross_mean_rupees'])} | "
            f"₹{cell['cost_mean_rupees']:,.2f} | {_money(cell['net_mean_rupees'])} |"
        )
    lines.extend(
        [
            "",
            "This is important evidence of slower payoff maturation, but it is not a profitable "
            "strategy result. The only positive-net cell is the inverse top-reversal short condor "
            "at +₹31.18 across 96 evaluated trades with 50.26% coverage and a confidence interval "
            "spanning −₹222.31 to +₹263.74. The other 95 of 191 signals are unevaluated—not wins, "
            "losses, or imputed trades. It fails every robustness condition needed for promotion.",
            "",
            "## Economic conclusion",
            "",
            "At 60 minutes, the original zero-crossing gross edge is only +₹22.60 against ₹271.14 "
            "average cost. Tail selection, direction, velocity, acceleration, alternative defined-"
            "risk structures, and a causal feature model do not bridge the hurdle. At 180 minutes, "
            "selected gross means rise above ₹65 and sometimes much further, but coverage falls and "
            "the payoff remains too inconsistent to clear approximately ₹249–₹311 per-trade cost.",
            "",
            "The supported conclusion is therefore economic rather than metaphysical: **within the "
            "observable intraday window, these standalone VRP rules do not mature or become realized "
            "in defined-risk option prices consistently enough to cover one-lot trading costs.**",
            "",
            "## Data boundary",
            "",
            "The rolling nearest-weekly ATM±10 archive cannot support an unbiased test beyond 180 "
            "minutes. For the apparent top-reversal candidate, 95 of 191 signals have no evaluated "
            "180-minute outcome. Frozen strikes progressively leave the observed surface, exact-"
            "contract coverage falls non-randomly, and multi-session contract tracking is incomplete. A "
            "longer-horizon or multi-day VRP hypothesis remains open only for a future full fixed-"
            "contract chain dataset; no profitability is extrapolated from this module.",
            "",
            "## Reproduce and verify",
            "",
            "```powershell",
            "python -m research.module3_hypothesis_testing.run build",
            "python -m research.module3_hypothesis_testing.run verify",
            "python -m pytest -q",
            "```",
            "",
            "The detailed phase reports and full CSV trade/leg books remain preserved under "
            "`docs/research/` and `audit/`. `results/manifest.json` hashes the calculation code, "
            "contracts, source summaries, documentation, and generated closeout outputs.",
            "",
        ]
    )
    return "\n".join(lines)


def _manifest_rows(repo_root: Path, paths: list[str] | dict[str, str]) -> list[dict[str, str]]:
    relative_paths = list(paths.values()) if isinstance(paths, dict) else paths
    return [
        {"path": relative_path, "sha256": _sha256(repo_root / relative_path)}
        for relative_path in relative_paths
    ]


def write_closeout(repo_root: Path) -> dict[str, Any]:
    """Write closeout JSON, report, and a hash manifest."""
    results_root = repo_root / "research/module3_hypothesis_testing/results"
    results_root.mkdir(parents=True, exist_ok=True)
    summary_path = results_root / "closeout.json"
    report_path = results_root / "closeout_report.md"
    manifest_path = results_root / "manifest.json"

    summary = build_closeout(repo_root)
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report_path.write_text(render_report(summary), encoding="utf-8")
    manifest = {
        "schema_version": "module3-vrp-hypothesis-manifest/v1",
        "hash_contract": "sha256 after CRLF-to-LF canonicalization",
        "module_id": summary["module_id"],
        "decision": summary["decision"],
        "sources": _manifest_rows(repo_root, SOURCE_JSONS)
        + _manifest_rows(repo_root, SOURCE_MANIFESTS),
        "implementations": _manifest_rows(repo_root, IMPLEMENTATIONS),
        "documents": _manifest_rows(repo_root, DOCUMENTS),
        "outputs": _manifest_rows(
            repo_root,
            [
                "research/module3_hypothesis_testing/results/closeout.json",
                "research/module3_hypothesis_testing/results/closeout_report.md",
            ],
        ),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def verify_manifest(repo_root: Path) -> list[str]:
    """Return manifest hash failures; an empty list means complete verification."""
    manifest_path = repo_root / "research/module3_hypothesis_testing/results/manifest.json"
    manifest = _load(manifest_path)
    failures = []
    for section in ("sources", "implementations", "documents", "outputs"):
        for row in manifest[section]:
            path = repo_root / row["path"]
            if not path.exists() or _sha256(path) != row["sha256"]:
                failures.append(row["path"])
    return failures
